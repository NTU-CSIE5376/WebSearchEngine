import shutil
from datetime import UTC, datetime

import psycopg
import pytest
from helpers import example_config, with_changes

from idxsel.guards import (
    DbProbe,
    Guard,
    LocalProbe,
    PreflightFailed,
    RemoteProbe,
    parse_remote_probe,
)
from idxsel.log import EventLog
from idxsel.throttle import Pacer, Verdict

PROBE_OK = """Mounted on        Avail
/   700000000000
/data 900000000000
some avg10=1.00 avg60=12.50 avg300=10.00 total=1
full avg10=0.00 avg60=0.00 avg300=0.00 total=1
"""


def test_pacer_sleeps_to_hold_rate():
    slept = []
    pacer = Pacer(example_config().throttle, sleep=slept.append)
    assert pacer.after_chunk(nbytes=120_000_000, elapsed_s=1.0) == pytest.approx(1.0)
    assert slept == [pytest.approx(1.0)]
    assert pacer.after_chunk(nbytes=1_000_000, elapsed_s=5.0) == 0.0


def test_pacer_aimd_and_pause_ladder():
    cfg = example_config().throttle
    slept = []
    pacer = Pacer(cfg, sleep=slept.append)
    for _ in range(cfg.increase_every_healthy_chunks):
        pacer.on_verdict(Verdict.OK)
    assert pacer.rate_mb_s == cfg.rate_mb_s_initial + cfg.increase_mb_s
    for _ in range(10):
        pacer.on_verdict(Verdict.SLOW)
    assert pacer.rate_mb_s == cfg.rate_mb_s_min
    for _ in range(6):
        pacer.on_verdict(Verdict.PAUSE)
    assert slept == [60, 120, 300, 900, 900, 900]


def test_parse_remote_probe():
    free, psi = parse_remote_probe(PROBE_OK)
    assert free == {"/": 700000000000, "/data": 900000000000}
    assert psi == 12.5


def _remote(text=None, error=None, **guard):
    cfg = with_changes(example_config(), **{f"guard__{k}": v for k, v in guard.items()})
    calls = []

    def runner(cmd, timeout):
        calls.append(cmd)
        if error:
            raise error
        return text

    return RemoteProbe(cfg, runner=runner, clock=lambda: 0.0), calls


def test_remote_probe_ok_and_cached():
    probe, calls = _remote(PROBE_OK)
    assert probe.findings(starting=True) == []
    assert probe.findings(starting=False) == []
    assert len(calls) == 1


def test_remote_probe_low_disk_aborts():
    probe, _ = _remote(PROBE_OK.replace("900000000000", "480000000000"))
    assert [f.verdict for f in probe.findings(starting=True)] == [Verdict.ABORT]
    assert probe.findings(starting=False) == []


def test_remote_probe_pressure_slows():
    probe, _ = _remote(PROBE_OK.replace("avg60=12.50", "avg60=40.00"))
    assert [f.verdict for f in probe.findings(starting=False)] == [Verdict.SLOW]


def test_remote_probe_fails_closed():
    probe, _ = _remote(error=OSError("ssh down"))
    assert probe.findings(starting=False)[0].verdict == Verdict.ABORT
    probe, _ = _remote(error=OSError("ssh down"), fail_closed_on_probe_error=False)
    assert probe.findings(starting=False) == []


def test_local_probe_memory_and_disk(tmp_path):
    meminfo = tmp_path / "meminfo"
    meminfo.write_text("MemTotal: 1 kB\nMemAvailable: 250000000 kB\n")
    cfg = with_changes(example_config(), run__work_dir=str(tmp_path / "w"),
                       guard__work_dir_min_free_gb=0.0, guard__work_dir_max_gb=1.0)
    probe = LocalProbe(cfg, meminfo_path=meminfo)
    start = probe.findings(starting=True)
    assert [f.verdict for f in start] == [Verdict.ABORT]
    assert "MemAvailable" in start[0].reason
    assert probe.findings(starting=False) == []


def test_local_probe_reuses_the_work_dir_size_between_scans(tmp_path):
    work = tmp_path / "w"
    work.mkdir()
    now = [0.0]
    cfg = with_changes(example_config(), run__work_dir=str(work),
                       guard__host_meminfo_enabled=False, guard__work_dir_min_free_gb=0.0,
                       guard__work_dir_max_gb=0.000001, guard__work_dir_scan_interval_s=60.0)
    probe = LocalProbe(cfg, clock=lambda: now[0])
    assert probe.findings(starting=False) == []
    (work / "big").write_bytes(b"x" * 2000)
    now[0] = 59.0
    assert probe.findings(starting=False) == []  # cached size
    assert [f.verdict for f in probe.findings(starting=True)] == [Verdict.ABORT]
    (work / "big").unlink()
    now[0] = 70.0  # the preflight at t=59 refreshed the cache
    assert [f.verdict for f in probe.findings(starting=False)] == [Verdict.ABORT]
    now[0] = 119.0
    assert probe.findings(starting=False) == []


def test_guard_preflight_refuses_in_blackout(tmp_path):
    cfg = with_changes(example_config(), run__work_dir=str(tmp_path / "w"),
                       guard__work_dir_min_free_gb=0.0, guard__host_meminfo_enabled=False,
                       guard__remote__enabled=False)
    guard = Guard(cfg, None, LocalProbe(cfg), RemoteProbe(cfg), EventLog(None),
                  now=lambda: datetime(2026, 10, 16, 6, tzinfo=UTC))
    with pytest.raises(PreflightFailed, match="measurement-day blackout"):
        guard.preflight("export_full")
    guard = Guard(cfg, None, LocalProbe(cfg), RemoteProbe(cfg), EventLog(None),
                  now=lambda: datetime(2026, 10, 15, 6, tzinfo=UTC))
    guard.preflight("export_full")
    assert guard.check().verdict == Verdict.OK


def test_build_preflight_skips_crawlerdb_limits_but_not_host1_root(tmp_path):
    cfg = with_changes(example_config(), run__work_dir=str(tmp_path / "w"),
                       guard__work_dir_min_free_gb=0.0, guard__host_meminfo_enabled=False)
    measure_day_blackout = datetime(2026, 10, 16, 6, tzinfo=UTC)

    def guard(probe_text):
        remote = RemoteProbe(cfg, runner=lambda cmd, timeout: probe_text, clock=lambda: 0.0)
        return Guard(cfg, None, LocalProbe(cfg), remote, EventLog(None),
                     now=lambda: measure_day_blackout)

    low_data = guard(PROBE_OK.replace("900000000000", "100000000000"))
    with pytest.raises(PreflightFailed, match=r"blackout.*remote /data"):
        low_data.preflight("export_delta")
    low_data.preflight("build", reads_crawlerdb=False)  # the build writes neither
    with pytest.raises(PreflightFailed, match="remote / free"):
        guard(PROBE_OK.replace("700000000000", "100000000000")).preflight(
            "build", reads_crawlerdb=False)
    # Stages that never touch Host1 (backtest, forward eval) skip its probe entirely.
    calls = []

    def failing(cmd, timeout):
        calls.append(cmd)
        raise OSError("no route to host1")

    offline = Guard(cfg, None, LocalProbe(cfg), RemoteProbe(cfg, runner=failing,
                                                            clock=lambda: 0.0),
                    EventLog(None), now=lambda: measure_day_blackout)
    offline.preflight("backtest", reads_crawlerdb=False, host1=False)
    assert calls == []
    with pytest.raises(PreflightFailed, match="remote probe failed"):
        offline.preflight("build", reads_crawlerdb=False)


def test_reserve_counts_against_the_work_fs_floor(tmp_path):
    free_gb = shutil.disk_usage(tmp_path).free / 1e9
    cfg = with_changes(example_config(), run__work_dir=str(tmp_path / "w"),
                       guard__work_dir_min_free_gb=free_gb / 2, guard__host_meminfo_enabled=False)
    probe = LocalProbe(cfg)
    assert probe.findings(starting=True) == []
    assert [f.verdict for f in probe.findings(starting=True, reserve_gb=free_gb)] == [Verdict.ABORT]


class _BrokenHolder:
    """A session holder whose database is gone."""

    def __init__(self):
        self.resets = 0
        self.session = self

    def fetchone(self, query, params=None):
        raise psycopg.OperationalError("server closed the connection")

    def reset(self):
        self.resets += 1
        raise psycopg.OperationalError("still down")


def test_db_probe_error_pauses_running_and_refuses_starting():
    holder = _BrokenHolder()
    probe = DbProbe(holder, example_config())
    assert [f.verdict for f in probe.findings(starting=False)] == [Verdict.PAUSE]
    assert [f.verdict for f in probe.findings(starting=True)] == [Verdict.ABORT]
    assert holder.resets == 2
