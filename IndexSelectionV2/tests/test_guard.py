from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from conftest import FakeConn

from isel.pg import (
    ByteRate,
    ReadGuard,
    ReadOnlySession,
    next_allowed_start,
    next_forbidden_window,
)


def t(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


@pytest.mark.parametrize("start, duration_s, expected", [
    ("2026-10-08 05:20", 300, "2026-10-08 05:20"),
    ("2026-10-08 05:53", 300, "2026-10-08 05:53"),   # ends exactly at :58
    ("2026-10-08 05:54", 300, "2026-10-08 06:10"),   # would run into :58
    ("2026-10-08 06:05", 60, "2026-10-08 06:10"),    # inside :58-:10
    ("2026-10-08 03:20", 300, "2026-10-08 03:20"),   # no daily window since 2026-10-11
    ("2026-10-16 10:00", 60, "2026-10-16 20:10"),    # measurement day, then :58-:10
    ("2026-10-15 23:58", 60, "2026-10-16 20:10"),    # chained hourly, day 16, hourly
    ("2026-10-02 10:20", 60, "2026-10-02 10:20"),    # day 2 is not a measurement day
])
def test_next_allowed_start(base_cfg, start, duration_s, expected):
    assert next_allowed_start(t(start), duration_s, base_cfg.guard) == t(expected)


def test_next_allowed_start_needs_aware_time(base_cfg):
    with pytest.raises(ValueError):
        next_allowed_start(datetime(2026, 10, 8, 5, 20), 60, base_cfg.guard)


def test_a_statement_longer_than_the_hourly_gap_is_refused(base_cfg):
    with pytest.raises(ValueError, match="cannot fit"):
        next_allowed_start(t("2026-10-08 05:20"), 48 * 60, base_cfg.guard)


@pytest.mark.parametrize("now, expected", [
    ("2026-10-08 05:20", ("2026-10-08 05:58", "2026-10-08 06:10")),
    ("2026-10-08 06:00", ("2026-10-08 05:58", "2026-10-08 06:10")),   # in progress
    ("2026-10-08 02:56", ("2026-10-08 02:58", "2026-10-08 03:10")),   # only the hourly one
    ("2026-10-16 10:00", ("2026-10-15 23:58", "2026-10-16 20:10")),   # chained on day 16
])
def test_next_forbidden_window_merges_overlaps(base_cfg, now, expected):
    assert next_forbidden_window(t(now), base_cfg.guard) == (t(expected[0]), t(expected[1]))


class FakeClock:
    def __init__(self, start: datetime):
        self.now = start
        self.mono = 0.0
        self.slept: list[float] = []

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += timedelta(seconds=seconds)
        self.mono += seconds


def _guard(cfg, clock: FakeClock, readings: list[int]) -> tuple[ReadGuard, list[int]]:
    calls: list[int] = []

    def io_waiters() -> int:
        calls.append(1)
        return readings.pop(0)

    guard = ReadGuard(cfg.guard, io_waiters, now=lambda: clock.now,
                      sleep=clock.sleep, monotonic=lambda: clock.mono)
    return guard, calls


def test_guard_waits_for_the_window_then_probes_load(base_cfg):
    clock = FakeClock(t("2026-10-08 06:05"))
    guard, calls = _guard(base_cfg, clock, [1, 2, 1])
    guard.wait(60)
    assert clock.now >= t("2026-10-08 06:10")
    assert clock.slept[0] == 300
    assert len(calls) == base_cfg.guard.probes


def test_guard_pauses_while_io_waiters_are_high(base_cfg):
    g = base_cfg.guard
    high, low = g.max_io_waiters + 5, 0
    clock = FakeClock(t("2026-10-08 05:20"))
    guard, _ = _guard(base_cfg, clock, [high] * g.probes + [low] * g.probes)
    guard.wait(60)
    assert g.pause_s in clock.slept


def test_guard_uses_the_median_probe(base_cfg):
    g = base_cfg.guard
    clock = FakeClock(t("2026-10-08 05:20"))
    spike = [g.max_io_waiters + 50] + [0] * (g.probes - 1)
    guard, _ = _guard(base_cfg, clock, spike)
    guard.wait(60)
    assert g.pause_s not in clock.slept


def test_session_runs_statements_only_inside_read_windows(base_cfg):
    clock = FakeClock(t("2026-10-08 06:05"))
    conn = FakeConn(rows={"pg_stat_activity": [(0,)], "SELECT 1": [(1,)]},
                    clock=lambda: clock.now)
    session = ReadOnlySession(conn, timeout_s=120)
    session.guard = ReadGuard(base_cfg.guard, session.io_waiters,
                              now=lambda: clock.now, sleep=clock.sleep,
                              monotonic=lambda: clock.mono)
    assert session.fetchall("SELECT 1") == [(1,)]
    main = [at for (text, _), at in zip(conn.executed, conn.executed_at, strict=True)
            if text == "SELECT 1"]
    assert main == [t("2026-10-08 06:10") + timedelta(seconds=2)]
    probes = [text for text, _ in conn.executed if "pg_stat_activity" in text]
    assert len(probes) == base_cfg.guard.probes
    # Our other connections share the application name; their reads are not load.
    assert all("application_name <> current_setting('application_name')" in text
               for text in probes)


def test_window_is_checked_again_after_the_load_probe(base_cfg):
    # 05:56:00 + 120 s just fits before :58, but the probe itself takes 2 s.
    clock = FakeClock(t("2026-10-08 05:56"))
    conn = FakeConn(rows={"pg_stat_activity": [(0,)], "SELECT 1": [(1,)]},
                    clock=lambda: clock.now)
    session = ReadOnlySession(conn, timeout_s=120)
    session.guard = ReadGuard(base_cfg.guard, session.io_waiters,
                              now=lambda: clock.now, sleep=clock.sleep,
                              monotonic=lambda: clock.mono)
    session.fetchall("SELECT 1")
    (started,) = [at for (text, _), at in zip(conn.executed, conn.executed_at, strict=True)
                  if text == "SELECT 1"]
    assert started >= t("2026-10-08 06:10")


def test_guard_reuses_a_fresh_load_reading(base_cfg):
    g = base_cfg.guard
    clock = FakeClock(t("2026-10-08 05:20"))
    guard, calls = _guard(base_cfg, clock, [0] * (2 * g.probes))
    guard.wait(60)
    guard.wait(60)
    assert len(calls) == g.probes


def test_byte_rate_spaces_reads_without_saving_idle_credit():
    clock = FakeClock(t("2026-10-08 05:20"))
    rate = ByteRate(100.0, clock=lambda: clock.mono, sleep=clock.sleep)
    starts = []
    for n_bytes, idle_s in [(100, 0), (100, 0), (50, 0), (100, 10), (100, 0)]:
        clock.sleep(idle_s)
        rate.wait(n_bytes)
        starts.append(clock.mono)
    # 100 B/s: back-to-back reservations wait for the previous slot; ten idle
    # seconds earn no burst, so the read after them waits again.
    assert [s - starts[0] for s in starts] == [0.0, 1.0, 2.0, 12.0, 13.0]
