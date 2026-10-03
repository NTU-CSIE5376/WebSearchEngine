"""Safety guards: preflight refusal, runtime verdicts and the self-cancel watchdog.

Every probe fails closed when configured to: a probe that cannot answer is
treated as "unsafe", never as "fine".
"""

from __future__ import annotations

import contextlib
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from idxsel.config import Config
from idxsel.log import EventLog
from idxsel.manifest import dir_size_bytes
from idxsel.pg import SessionHolder
from idxsel.schedule import blackout_reason
from idxsel.throttle import Verdict

GB = 1_000_000_000


class GuardAbort(RuntimeError):
    """Raised when a hard limit is hit; the current stage must stop."""


class PreflightFailed(RuntimeError):
    """Raised when a stage must not start."""


@dataclass(frozen=True)
class Finding:
    """A guard outcome; `crawlerdb_only` ones matter only to stages that read crawlerdb."""

    verdict: Verdict
    reason: str
    crawlerdb_only: bool = False


# One pass over pg_stat_activity. Parameters in order: own app-name pattern,
# foreign patterns, long-query seconds, own pattern, own pattern.
_DB_HEALTH_SQL = (
    "SELECT (SELECT max(age(datfrozenxid)) FROM pg_database), "
    "count(*) FILTER (WHERE backend_type = 'autovacuum worker' "
    "AND query LIKE '%%to prevent wraparound%%'), "
    "count(*) FILTER (WHERE state = 'active' AND backend_type = 'client backend' "
    "AND pid <> pg_backend_pid() AND application_name NOT LIKE %s "
    "AND (application_name LIKE ANY(%s) OR now() - query_start > make_interval(secs => %s))), "
    "current_setting('max_connections')::int - count(*), "
    "count(*) FILTER (WHERE wait_event = 'DataFileRead' AND application_name NOT LIKE %s), "
    "count(*) FILTER (WHERE wait_event_type = 'LWLock' AND application_name NOT LIKE %s) "
    "FROM pg_stat_activity"
)
# pg_blocking_pids() takes the lock manager's partition locks, so it is only
# evaluated for backends that actually wait on a heavyweight lock.
_BLOCKED_BY_SQL = (
    "SELECT count(*) FROM pg_stat_activity WHERE CASE WHEN wait_event_type = 'Lock' "
    "THEN %s = ANY(pg_blocking_pids(pid)) ELSE false END"
)


class DbProbe:
    """Reads crawlerdb health from statistics views only (no table data)."""

    def __init__(self, holder: SessionHolder, cfg: Config) -> None:
        self._holder = holder
        self._cfg = cfg

    def findings(self, *, starting: bool) -> list[Finding]:
        g = self._cfg.guard
        own = f"{self._cfg.run.app_name_prefix}%"
        out: list[Finding] = []
        try:
            age, wrap, foreign, free_conns, io_wait, lw_wait = self._holder.session.fetchone(
                _DB_HEALTH_SQL,
                (own, list(g.foreign_app_name_patterns), g.foreign_long_query_s, own, own))
        except psycopg.Error as exc:
            with contextlib.suppress(psycopg.Error):
                self._holder.reset()
            verdict = Verdict.ABORT if starting else Verdict.PAUSE
            return [Finding(verdict, f"db probe failed: {str(exc)[:200]}")]
        if age is not None and age >= g.max_datfrozenxid_age:
            out.append(Finding(Verdict.ABORT,
                               f"datfrozenxid age {age} >= {g.max_datfrozenxid_age}"))
        if wrap >= g.max_wraparound_vacuum_workers:
            out.append(Finding(Verdict.PAUSE, f"{wrap} anti-wraparound vacuums running"))
        if foreign > 0:
            out.append(Finding(Verdict.PAUSE, f"{foreign} heavy foreign sessions active"))
        if free_conns < g.min_free_connections:
            out.append(Finding(Verdict.PAUSE, f"only {free_conns} free connections"))
        if io_wait >= g.db_io_wait_backends_hard:
            out.append(Finding(Verdict.PAUSE, f"{io_wait} backends waiting on DataFileRead"))
        elif io_wait >= g.db_io_wait_backends_soft:
            out.append(Finding(Verdict.SLOW, f"{io_wait} backends waiting on DataFileRead"))
        if lw_wait >= g.db_lwlock_wait_backends_soft:
            out.append(Finding(Verdict.SLOW, f"{lw_wait} backends waiting on LWLocks"))
        if starting:
            out = [Finding(Verdict.ABORT, f.reason) if f.verdict >= Verdict.PAUSE else f
                   for f in out]
        return out


class LocalProbe:
    """Checks the compute host: work dir free space and size, memory.

    Walking the work dir touches every file, so between chunks the measured
    size is reused for `guard.work_dir_scan_interval_s`; preflight always rescans.
    """

    def __init__(self, cfg: Config, meminfo_path: Path = Path("/proc/meminfo"),
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._cfg = cfg
        self._work = Path(cfg.run.work_dir)
        self._meminfo = meminfo_path
        self._clock = clock
        self._used: tuple[float, float] | None = None

    def _used_gb(self, starting: bool) -> float:
        now = self._clock()
        stale = (self._used is None
                 or now - self._used[0] >= self._cfg.guard.work_dir_scan_interval_s)
        if starting or stale:
            self._used = (now, dir_size_bytes(self._work) / GB)
        return self._used[1]

    def findings(self, *, starting: bool, reserve_gb: float = 0.0) -> list[Finding]:
        """Checks the floor after `reserve_gb`, the space the stage itself will still use."""
        g = self._cfg.guard
        out: list[Finding] = []
        self._work.mkdir(parents=True, exist_ok=True)
        free_gb = shutil.disk_usage(self._work).free / GB
        if free_gb - reserve_gb < g.work_dir_min_free_gb:
            out.append(Finding(Verdict.ABORT, f"work fs free {free_gb:.0f} GB - reserve "
                                              f"{reserve_gb:.0f} GB < floor"))
        used_gb = self._used_gb(starting)
        if used_gb > g.work_dir_max_gb:
            out.append(Finding(Verdict.ABORT, f"work dir {used_gb:.0f} GB > cap"))
        if g.host_meminfo_enabled:
            try:
                avail_gb = _mem_available_gb(self._meminfo)
            except OSError as exc:
                if g.fail_closed_on_probe_error:
                    out.append(Finding(Verdict.ABORT, f"meminfo probe failed: {exc}"))
            else:
                limit = (g.host_mem_available_min_gb_start if starting
                         else g.host_mem_available_min_gb_run)
                if avail_gb < limit:
                    out.append(Finding(Verdict.ABORT, f"MemAvailable {avail_gb:.0f} GB < {limit}"))
        return out


def _mem_available_gb(path: Path) -> float:
    for line in path.read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024 / GB
    raise OSError("MemAvailable not found")


class RemoteProbe:
    """Reads Host1 disk space and IO pressure through a forced-command SSH key.

    Expected output: `df -B1 --output=target,avail` lines, then the content of
    `/proc/pressure/io` (a line starting with `some avg10=...`).
    """

    def __init__(self, cfg: Config,
                 runner: Callable[[list[str], float], str] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self._cfg = cfg
        self._runner = runner or _run_command
        self._clock = clock
        self._last: tuple[float, list[Finding], list[Finding]] | None = None

    def findings(self, *, starting: bool) -> list[Finding]:
        r = self._cfg.guard.remote
        if not r.enabled:
            return []
        now = self._clock()
        if self._last is None or now - self._last[0] >= r.interval_s:
            self._last = (now, *self._probe())
        return self._last[1] if starting else self._last[2]

    def _probe(self) -> tuple[list[Finding], list[Finding]]:
        g = self._cfg.guard
        r = g.remote
        try:
            text = self._runner(list(r.command), r.timeout_s)
            free, psi60 = parse_remote_probe(text)
            data_gb = free[r.data_mount] / GB
            root_gb = free[r.root_mount] / GB
        except Exception as exc:
            if not g.fail_closed_on_probe_error:
                return [], []
            bad = [Finding(Verdict.ABORT, f"remote probe failed: {exc}")]
            return bad, bad
        start: list[Finding] = []
        run: list[Finding] = []
        # Only crawlerdb lives on the data mount; our selectdb is on the root mount.
        data_low = Finding(Verdict.ABORT, f"remote {r.data_mount} free {data_gb:.0f} GB",
                           crawlerdb_only=True)
        if data_gb < g.remote_data_min_free_gb_start:
            start.append(data_low)
        if data_gb < g.remote_data_min_free_gb_run:
            run.append(data_low)
        if root_gb < g.remote_root_min_free_gb:
            msg = Finding(Verdict.ABORT, f"remote {r.root_mount} free {root_gb:.0f} GB")
            start.append(msg)
            run.append(msg)
        if psi60 >= g.remote_io_psi_some_avg60_hard:
            start.append(Finding(Verdict.ABORT, f"remote IO PSI avg60 {psi60}"))
            run.append(Finding(Verdict.PAUSE, f"remote IO PSI avg60 {psi60}"))
        elif psi60 >= g.remote_io_psi_some_avg60_soft:
            run.append(Finding(Verdict.SLOW, f"remote IO PSI avg60 {psi60}"))
        return start, run


def _run_command(cmd: list[str], timeout_s: float) -> str:
    done = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s, check=True)
    return done.stdout


def parse_remote_probe(text: str) -> tuple[dict[str, int], float]:
    """Parses `df -B1 --output=target,avail` plus `/proc/pressure/io` output."""
    free: dict[str, int] = {}
    psi60: float | None = None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0].startswith("/") and parts[1].isdigit():
            free[parts[0]] = int(parts[1])
        elif parts and parts[0] == "some":
            fields = dict(p.split("=", 1) for p in parts[1:])
            psi60 = float(fields["avg60"])
    if psi60 is None:
        raise ValueError("no 'some avg60=' line in remote probe output")
    return free, psi60


class Guard:
    """Combines schedule, DB, local and remote probes."""

    def __init__(self, cfg: Config, db: DbProbe | None, local: LocalProbe,
                 remote: RemoteProbe, events: EventLog,
                 now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._cfg = cfg
        self._db = db
        self._local = local
        self._remote = remote
        self._events = events
        self._now = now

    def _all(self, starting: bool, reserve_gb: float = 0.0, host1: bool = True
             ) -> list[Finding]:
        found: list[Finding] = []
        reason = blackout_reason(self._now(), self._cfg.schedule)
        if reason:
            found.append(Finding(Verdict.ABORT if starting else Verdict.PAUSE, reason,
                                 crawlerdb_only=True))
        if self._db is not None and self._cfg.guard.db_checks_enabled:
            found += self._db.findings(starting=starting)
        found += self._local.findings(starting=starting, reserve_gb=reserve_gb)
        if host1:
            found += self._remote.findings(starting=starting)
        return found

    def preflight(self, stage: str, *, reads_crawlerdb: bool = True,
                  reserve_gb: float = 0.0, host1: bool = True) -> None:
        """Raises PreflightFailed if any check forbids starting `stage`.

        Args:
            stage: Name for the event log.
            reads_crawlerdb: False skips blackouts and the crawlerdb disk, which only
                constrain crawlerdb reads.
            reserve_gb: Space on the work fs the stage will still fill after it starts.
            host1: False skips the Host1 probe, for stages that touch Host1 not at all.
        """
        bad = [f.reason for f in self._all(starting=True, reserve_gb=reserve_gb, host1=host1)
               if f.verdict >= Verdict.PAUSE and (reads_crawlerdb or not f.crawlerdb_only)]
        self._events.emit("guard.preflight", stage=stage, ok=not bad, reasons=bad)
        if bad:
            raise PreflightFailed(f"{stage}: " + "; ".join(bad))

    def check(self) -> Finding:
        """Returns the most severe runtime finding (OK if none)."""
        found = self._all(starting=False)
        if not found:
            return Finding(Verdict.OK, "ok")
        worst = max(found, key=lambda f: f.verdict)
        self._events.emit("guard.verdict", verdict=worst.verdict.name,
                          reasons=[f.reason for f in found])
        return worst


class Watchdog:
    """Cancels our own query whenever it blocks another backend.

    Our ACCESS SHARE lock can make a queued ACCESS EXCLUSIVE request (e.g. a
    partition DROP) wait, and every later writer then queues behind that
    request. Cancelling ourselves first keeps the crawler's writers moving.
    If its own connection fails, the watchdog reconnects and reports itself
    unhealthy until a check succeeds again; callers must not start new reads
    while it is unhealthy.
    """

    def __init__(self, holder: SessionHolder, interval_s: float, events: EventLog) -> None:
        self._holder = holder
        self._interval = interval_s
        self._events = events
        self._target: psycopg.Connection | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.cancels = 0
        self.healthy = True
        self._thread = threading.Thread(target=self._loop, name="idxsel-watchdog", daemon=True)

    def __enter__(self) -> Watchdog:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval * 5)

    def set_target(self, conn: psycopg.Connection | None) -> None:
        with self._lock:
            self._target = conn

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            with self._lock:
                target = self._target
            pid = target.info.backend_pid if target is not None and not target.closed else None
            if pid is None and self.healthy:
                continue
            try:
                # With no target, pid 0 turns the check into a cheap health ping.
                (blocked,) = self._holder.session.fetchone(_BLOCKED_BY_SQL, (pid or 0,))
            except psycopg.Error as exc:
                self.healthy = False
                self._events.emit("watchdog.error", error=str(exc)[:300])
                try:
                    self._holder.reset()
                except psycopg.Error as reconnect_exc:
                    self._events.emit("watchdog.reconnect_failed", error=str(reconnect_exc)[:300])
                continue
            self.healthy = True
            if pid is not None and blocked:
                target.cancel_safe(timeout=self._interval * 5)
                self.cancels += 1
                self._events.emit("watchdog.self_cancel", pid=pid, blocked=blocked)
