"""PostgreSQL sessions with a read-only SQL gate, read windows and statement pacing.

crawlerdb's role is a superuser, so the database cannot stop an accidental
write. Two independent guards apply to every session: the server-side
`default_transaction_read_only=on` option, and a client-side gate that lexes
each statement and rejects anything that is not a single read.
"""

from __future__ import annotations

import logging
import os
import re
import statistics
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from isel.config import Config, GuardCfg, hhmm_minutes, hourly_open_s, timeout_seconds

log = logging.getLogger(__name__)

_READ_KEYWORDS = ("SELECT", "WITH", "SHOW", "EXPLAIN", "COPY")
_WRITE_WORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|DROP|ALTER|CREATE|GRANT|REVOKE|VACUUM|ANALYZE"
    r"|CLUSTER|REINDEX|LOCK|CALL|DO|REFRESH|COMMENT|SECURITY|IMPORT|NOTIFY|LISTEN|INTO)\b",
    re.I,
)
# A superuser can change server state from a plain SELECT through these functions,
# and the last group runs a query string the gate cannot see.
_WRITE_FUNCTIONS = re.compile(
    r"\b(set_config|pg_terminate_backend|pg_cancel_backend|pg_reload_conf|pg_rotate_logfile"
    r"|pg_switch_wal|pg_promote|pg_create_restore_point|pg_backup_\w+|pg_wal_replay_\w+"
    r"|pg_\w*replication_slot\w*|pg_replication_origin_\w+|pg_logical_emit_message"
    r"|pg_stat_\w*reset\w*|pg_(?:try_)?advisory\w*|pg_file_\w+|pg_log_backend_memory_contexts"
    r"|pg_notify|pg_sleep\w*|pg_prewarm|lo_\w+|dblink\w*|nextval|setval|txid_current"
    r"|pg_current_xact_id|brin_summarize_\w+|brin_desummarize_range|gin_clean_pending_list"
    r"|query_to_xml\w*|cursor_to_xml\w*|ts_stat)\s*\(",
    re.I,
)
# PostgreSQL identifiers may hold any non-ASCII letter, and `$` after the first character.
_IDENT_CHAR = re.compile(r"[\w$]")
_DOLLAR_TAG = re.compile(r"\$(?:[^\W\d]\w*)?\$")
# The only COPY allowed: `COPY (query) TO STDOUT [[WITH] (options)]`, after the query.
_COPY_TAIL = re.compile(r"\s*TO\s+STDOUT\s*(?:(?:WITH\s*)?\([\w\s,'=]*\))?\s*", re.I)
# Read windows repeat at most monthly, so a search this long finds one if any exists.
_WINDOW_HORIZON = timedelta(days=32)

# Our parallel export sessions share one application_name; their own IO waits are
# not load, while other backends slowed by our reads are.
_IO_WAITERS_SQL = ("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' "
                   "AND wait_event_type = 'IO' "
                   "AND application_name <> current_setting('application_name')")


class ReadOnlyViolation(RuntimeError):
    """Raised when code tries to send a non-read statement."""


def require_env(name: str) -> str:
    """Returns env var `name`, or the content of the file named by `<name>_FILE`.

    DSNs never live in config. Containers pass them as secret files under
    `/run/secrets/`, so they do not show up in `docker inspect`.

    Raises:
        RuntimeError: If neither is set, both are set, or the value is empty.
    """
    value = os.environ.get(name, "")
    path = os.environ.get(f"{name}_FILE", "")
    if value and path:
        raise RuntimeError(f"set only one of {name} and {name}_FILE")
    if path:
        value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"environment variable {name} (or {name}_FILE) is not set")
    return value


def _options(session: Mapping[str, str]) -> str:
    parts = []
    for key, value in session.items():
        escaped = str(value).replace("\\", "\\\\").replace(" ", "\\ ")
        parts.append(f"-c {key}={escaped}")
    return " ".join(parts)


def connect(dsn: str, *, app_name: str, session: Mapping[str, str],
            connect_timeout_s: int) -> psycopg.Connection:
    """Opens an autocommit connection with session GUCs set at startup."""
    return psycopg.connect(
        dsn,
        autocommit=True,
        application_name=app_name,
        options=_options(session),
        connect_timeout=connect_timeout_s,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )


def _end_of_string(text: str, start: int, backslash: bool) -> int:
    """Index after the string constant whose opening quote is at `start`."""
    i = start + 1
    while i < len(text):
        if backslash and text[i] == "\\":
            i += 2
        elif text[i] == "'":
            if text.startswith("''", i):
                i += 2
            else:
                return i + 1
        else:
            i += 1
    raise ReadOnlyViolation("unterminated string constant")


def _end_of_block_comment(text: str, start: int) -> int:
    """Index after the block comment opened at `start`; PostgreSQL nests them."""
    depth, i = 0, start
    while i < len(text):
        if text.startswith("/*", i):
            depth, i = depth + 1, i + 2
        elif text.startswith("*/", i):
            depth, i = depth - 1, i + 2
            if depth == 0:
                return i
        else:
            i += 1
    raise ReadOnlyViolation("unterminated block comment")


def _end_of_identifier(text: str, start: int) -> tuple[int, str]:
    """(index after, bare name) of the quoted identifier opened at `start`."""
    i, name = start + 1, []
    while i < len(text):
        if text[i] == '"':
            if text.startswith('""', i):
                name.append('"')
                i += 2
                continue
            return i + 1, re.sub(r"\W", "_", "".join(name))
        name.append(text[i])
        i += 1
    raise ReadOnlyViolation("unterminated quoted identifier")


def normalize_sql(statement: str) -> str:
    """Lexes `statement` the way PostgreSQL does and keeps only bare SQL tokens.

    String constants (plain, `E''`, `U&''` and dollar-quoted) become `''` and
    comments become a space, so nothing can hide inside them. A quoted
    identifier becomes `"name"` with every non-word character replaced, so the
    only double quotes left delimit identifiers. Unicode-escaped identifiers
    (`U&"..."`) are rejected outright.

    Raises:
        ReadOnlyViolation: On an unterminated literal, comment or identifier.
    """
    out: list[str] = []
    i = 0
    while i < len(statement):
        c = statement[i]
        prev = statement[i - 1] if i else " "
        if statement.startswith("--", i):
            end = statement.find("\n", i)
            i = len(statement) if end < 0 else end
            out.append(" ")
        elif statement.startswith("/*", i):
            i = _end_of_block_comment(statement, i)
            out.append(" ")
        elif c == "'":
            before = statement[i - 2] if i >= 2 else " "
            escape = prev in "eE" and not _IDENT_CHAR.match(before)
            i = _end_of_string(statement, i, backslash=escape)
            out.append("''")
        elif c == '"':
            if prev == "&":
                raise ReadOnlyViolation("unicode-escaped identifiers are not allowed")
            i, name = _end_of_identifier(statement, i)
            out.append(f'"{name}"')
        elif c == "$" and not _IDENT_CHAR.match(prev) and (m := _DOLLAR_TAG.match(statement, i)):
            end = statement.find(m.group(0), m.end())
            if end < 0:
                raise ReadOnlyViolation("unterminated dollar-quoted string")
            i = end + len(m.group(0))
            out.append("''")
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _copy_query(text: str) -> str:
    """The query of `COPY (query) TO STDOUT [[WITH] (options)]`, the only COPY allowed.

    Raises:
        ReadOnlyViolation: On any other COPY shape, such as a server-side file target.
    """
    m = re.match(r"COPY\s*\(", text, re.I)
    depth, i = 1, m.end() if m else len(text)
    while i < len(text) and depth:
        depth += {"(": 1, ")": -1}.get(text[i], 0)
        i += 1
    if not m or depth or not _COPY_TAIL.fullmatch(text, i):
        raise ReadOnlyViolation("COPY must be COPY (query) TO STDOUT [(options)]")
    return text[m.end():i - 1].strip()


def assert_read_only_sql(statement: str) -> None:
    """Raises ReadOnlyViolation unless `statement` is one read-only statement.

    Allowed: SELECT/WITH/SHOW/EXPLAIN (without ANALYZE) and `COPY (query) TO STDOUT`.
    Keyword and function checks always cover the whole statement.
    """
    normalized = normalize_sql(statement).strip().rstrip(";").strip()
    if ";" in normalized:
        raise ReadOnlyViolation("multiple statements are not allowed")
    # psycopg sends %s as $1 after this check, so `$q%s` would become a dollar quote.
    if re.search(r"[\w$]%s", normalized):
        raise ReadOnlyViolation("a parameter must not follow an identifier or `$`")
    # Keywords never come quoted, so quoted identifiers drop out of the keyword
    # checks; a function can be called as "name", so they count as bare names there.
    text = re.sub(r'"\w*"', " _ ", normalized)
    calls = normalized.replace('"', "")
    head = text.split(None, 1)[0].upper() if text else ""
    if head not in _READ_KEYWORDS:
        raise ReadOnlyViolation(f"statement type {head!r} is not allowed")
    if head == "COPY":
        inner = _copy_query(text)
        if (inner.split(None, 1)[0].upper() if inner else "") not in ("SELECT", "WITH"):
            raise ReadOnlyViolation("COPY must wrap a SELECT or WITH query")
    upper = text.upper()
    if head == "EXPLAIN" and re.search(r"\bANALY[SZ]E\b", upper):
        raise ReadOnlyViolation("EXPLAIN ANALYZE executes the query")
    if re.search(r"\bFOR\s+(UPDATE|SHARE|NO\s+KEY|KEY)\b", upper):
        raise ReadOnlyViolation("row locks are not allowed")
    if _WRITE_WORDS.search(text):
        raise ReadOnlyViolation("write keyword found in a read-only statement")
    if _WRITE_FUNCTIONS.search(calls):
        raise ReadOnlyViolation("state-changing function found in a read-only statement")


class Pacer:
    """Keeps at least `min_interval_s` between the starts of consecutive statements."""

    def __init__(self, min_interval_s: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self._interval = min_interval_s
        self._clock = clock
        self._sleep = sleep
        self._last: float | None = None

    def wait(self) -> None:
        now = self._clock()
        if self._last is not None:
            remaining = self._interval - (now - self._last)
            if remaining > 0:
                self._sleep(remaining)
                now = self._clock()
        self._last = now


class ByteRate:
    """Spaces reads so that all sessions together read at most `bytes_per_s` on average.

    Each call reserves the next free slot of `n_bytes / bytes_per_s` seconds and
    sleeps until it starts. Idle time earns no credit, so a pause never ends in a burst.
    """

    def __init__(self, bytes_per_s: float, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        if bytes_per_s <= 0:
            raise ValueError(f"bytes_per_s must be > 0, got {bytes_per_s!r}")
        self._rate = bytes_per_s
        self._clock = clock
        self._sleep = sleep
        self._next: float | None = None
        self._lock = threading.Lock()

    def wait(self, n_bytes: float) -> None:
        with self._lock:
            now = self._clock()
            start = now if self._next is None else max(now, self._next)
            self._next = start + n_bytes / self._rate
        if start > now:
            self._sleep(start - now)


def _forbidden_windows(start: datetime, end: datetime,
                       g: GuardCfg) -> list[tuple[datetime, datetime]]:
    """Every forbidden [from, to) interval that can overlap [start, end]."""
    out = []
    day = start.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    monthly = [timedelta(minutes=hhmm_minutes(v)) for v in g.month_window]
    while day <= end:
        if day.day in g.month_days:
            out.append((day + monthly[0], day + monthly[1]))
        day += timedelta(days=1)
    first, last = g.hourly_window
    span = timedelta(minutes=(last - first) % 60)
    hour = start.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    while hour <= end:
        begin = hour + timedelta(minutes=first)
        out.append((begin, begin + span))
        hour += timedelta(hours=1)
    return out


def next_allowed_start(now: datetime, duration_s: float, g: GuardCfg) -> datetime:
    """Earliest start >= `now` whose [start, start + duration] avoids every forbidden window.

    Raises:
        ValueError: If `now` is naive or `duration_s` cannot fit the hourly open gap.
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if duration_s >= hourly_open_s(g):
        raise ValueError(f"a {duration_s:.0f} s statement cannot fit the hourly read window")
    start = now.astimezone(UTC)
    duration = timedelta(seconds=duration_s)
    while start < now + _WINDOW_HORIZON:
        end = start + duration
        blocking = [b for a, b in _forbidden_windows(start, end, g) if a < end and start < b]
        if not blocking:
            return start
        start = max(blocking)
    raise RuntimeError("no allowed read window within a month; check the guard config")


def next_forbidden_window(now: datetime, g: GuardCfg) -> tuple[datetime, datetime]:
    """The forbidden window in progress at `now`, or the next one, with overlaps merged."""
    start = now.astimezone(UTC)
    day = timedelta(days=1)
    merged: list[list[datetime]] = []
    for a, b in sorted(_forbidden_windows(start - day, start + 2 * day, g)):
        if merged and a <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    a, b = next(w for w in merged if w[1] > start)
    return a, b


class ReadGuard:
    """Holds each statement until it fits a read window and the server's IO load is low."""

    def __init__(self, g: GuardCfg, io_waiters: Callable[[], int],
                 now: Callable[[], datetime] = lambda: datetime.now(UTC),
                 sleep: Callable[[float], None] = time.sleep,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self._g = g
        self._io_waiters = io_waiters
        self._now = now
        self._sleep = sleep
        self._monotonic = monotonic
        self._reading: tuple[float, float] | None = None

    def _load(self) -> float:
        if self._reading and self._monotonic() - self._reading[0] < self._g.probe_ttl_s:
            return self._reading[1]
        samples = []
        for i in range(self._g.probes):
            if i:
                self._sleep(self._g.probe_interval_s)
            samples.append(self._io_waiters())
        self._reading = (self._monotonic(), statistics.median(samples))
        return self._reading[1]

    def _sleep_until_window(self, timeout_s: float) -> bool:
        """Sleeps until a window opens; True if it had to sleep."""
        now = self._now()
        start = next_allowed_start(now, timeout_s, self._g)
        if start <= now:
            return False
        log.info("read window closed; sleeping until %s", start.isoformat())
        self._sleep((start - now).total_seconds())
        return True

    def wait(self, timeout_s: float) -> None:
        """Blocks until a statement that may run `timeout_s` seconds is allowed to start.

        The window is checked again after the load probe, which takes time itself,
        so the statement starts right after a passing window check.
        """
        while True:
            if self._sleep_until_window(timeout_s):
                continue
            load = self._load()
            if load > self._g.max_io_waiters:
                log.info("%s backends wait on IO (max %d); pausing %.0f s",
                         load, self._g.max_io_waiters, self._g.pause_s)
                self._sleep(self._g.pause_s)
                continue
            if not self._sleep_until_window(timeout_s):
                return


class ReadOnlySession:
    """A connection wrapper that only lets paced read statements through."""

    def __init__(self, conn: psycopg.Connection, pacer: Pacer | None = None,
                 timeout_s: int | None = None) -> None:
        self.conn = conn
        self._pacer = pacer or Pacer(0.0)
        self.timeout_s = timeout_s
        # Set after construction: the guard probes load through this session.
        self.guard: ReadGuard | None = None

    def _admit(self) -> None:
        self._pacer.wait()
        if self.guard is not None:
            self.guard.wait(self.timeout_s or 0)

    def io_waiters(self) -> int:
        """Active backends of other applications waiting on IO (not guarded)."""
        assert_read_only_sql(_IO_WAITERS_SQL)
        with self.conn.cursor() as cur:
            cur.execute(_IO_WAITERS_SQL)
            return int(cur.fetchall()[0][0])

    def set_statement_timeout(self, seconds: int) -> None:
        """Sets the session `statement_timeout`; the only statement that skips the gate.

        Raises:
            ValueError: If `seconds` is not a positive int.
        """
        if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds <= 0:
            raise ValueError(f"statement timeout must be a positive int, got {seconds!r}")
        with self.conn.cursor() as cur:
            cur.execute("SELECT set_config('statement_timeout', %s, false)", (f"{seconds}s",))
        self.timeout_s = seconds

    def _text(self, query: str | sql.Composable) -> str:
        return query if isinstance(query, str) else query.as_string(self.conn)

    def fetchall(self, query: str | sql.Composable, params: Sequence[Any] | None = None) -> list:
        text = self._text(query)
        assert_read_only_sql(text)
        self._admit()
        with self.conn.cursor() as cur:
            cur.execute(text, params)
            return cur.fetchall()

    def fetchone(self, query: str | sql.Composable, params: Sequence[Any] | None = None) -> Any:
        rows = self.fetchall(query, params)
        return rows[0] if rows else None

    def copy_bytes(self, query: str | sql.Composable) -> bytearray:
        """Returns the whole output of a `COPY ... TO STDOUT` statement."""
        text = self._text(query)
        assert_read_only_sql(text)
        self._admit()
        out = bytearray()
        with self.conn.cursor() as cur, cur.copy(text) as copy:
            for block in copy:
                out += block
        return out

    def close(self) -> None:
        self.conn.close()


def _open(cfg: Config, dsn_env: str, app_suffix: str, session: Mapping[str, str],
          connect_timeout_s: int, min_interval_s: float) -> ReadOnlySession:
    conn = connect(require_env(dsn_env), app_name=f"{cfg.run.app_name}_{app_suffix}",
                   session=session, connect_timeout_s=connect_timeout_s)
    read_only = ReadOnlySession(conn, Pacer(min_interval_s), timeout_s=timeout_seconds(session))
    read_only.guard = ReadGuard(cfg.guard, read_only.io_waiters)
    return read_only


def open_crawlerdb(cfg: Config) -> ReadOnlySession:
    c = cfg.crawlerdb
    return _open(cfg, c.dsn_env, "crawler", c.session, c.connect_timeout_s,
                 cfg.pull.min_interval_s)


def open_metricdb(cfg: Config) -> ReadOnlySession:
    """Opens metricdb under the same read windows, so a golden pull never sees half a batch."""
    m = cfg.metricdb
    return _open(cfg, m.dsn_env, "metric", m.session, m.connect_timeout_s, 0.0)
