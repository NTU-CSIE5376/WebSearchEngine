from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from conftest import replace

from isel.schedule import Window, cycle_at, golden_days, min_gap_days, swap_allowed, window


def _at(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=UTC)


@pytest.mark.parametrize("now, allowed", [
    (_at(2026, 10, 16, 11, 29), True),
    (_at(2026, 10, 16, 11, 30), False),    # random set at 12:00
    (_at(2026, 10, 16, 19, 59), False),    # head set at 18:00
    (_at(2026, 10, 16, 20, 0), True),
    (_at(2026, 10, 15, 15, 0), True),
    # 2026-10-17 01:00 in Taipei is 10-16 17:00 UTC: inside the freeze.
    (datetime(2026, 10, 17, 1, 0, tzinfo=timezone(timedelta(hours=8))), False),
])
def test_swaps_wait_while_the_metric_reads_the_list(base_cfg, now, allowed):
    assert swap_allowed(now, base_cfg.schedule) is allowed


def test_naive_datetimes_are_refused(base_cfg):
    naive = datetime(2026, 12, 1, 12, 0)
    with pytest.raises(ValueError, match="naive"):
        swap_allowed(naive, base_cfg.schedule)
    with pytest.raises(ValueError, match="naive"):
        cycle_at(naive, base_cfg)
    w = window(date(2026, 12, 1), base_cfg)
    with pytest.raises(ValueError, match="naive"):
        Window(w.golden_day, naive, w.swap_by, w.freeze_start, w.freeze_end, w.ends_at)


def test_a_window_out_of_order_is_refused(base_cfg):
    w = window(date(2026, 12, 1), base_cfg)
    with pytest.raises(ValueError, match="out of order"):
        Window(w.golden_day, w.start_at, w.freeze_start, w.freeze_start, w.freeze_end,
               w.ends_at)


def test_day_31_is_a_golden_day_only_in_months_that_have_it():
    days = []
    for day in golden_days(date(2027, 1, 1), [1, 16, 31]):
        if day.year > 2027:
            break
        days.append(day)
    assert [d.month for d in days if d.day == 31] == [1, 3, 5, 7, 8, 10, 12]
    assert len([d for d in days if d.day in (1, 16)]) == 24


@pytest.mark.parametrize("days, gap", [([1, 16], 13), ([15], 28), ([31], 31), ([30], 30)])
def test_the_shortest_gap_between_golden_days(days, gap):
    # [1, 16]: Feb 16 to Mar 1 in a common year. [31]: Jul 31 to Aug 31.
    assert min_gap_days(days) == gap


def test_the_shortest_gap_needs_a_golden_day():
    with pytest.raises(ValueError):
        min_gap_days([])


def test_the_window_of_a_golden_day(base_cfg):
    assert window(date(2026, 12, 1), base_cfg) == Window(
        date(2026, 12, 1), start_at=_at(2026, 11, 29, 2, 15), swap_by=_at(2026, 12, 1, 11, 0),
        freeze_start=_at(2026, 12, 1, 11, 30), freeze_end=_at(2026, 12, 1, 20, 0),
        ends_at=_at(2026, 12, 14, 2, 15))


def test_the_window_of_a_16th(base_cfg):
    w = window(date(2026, 12, 16), base_cfg)
    assert (w.start_at, w.swap_by) == (_at(2026, 12, 14, 2, 15), _at(2026, 12, 16, 11, 0))


def test_a_cycle_spans_the_new_year(base_cfg):
    w = window(date(2027, 1, 1), base_cfg)
    assert (w.start_at, w.ends_at) == (_at(2026, 12, 30, 2, 15), _at(2027, 1, 14, 2, 15))
    assert window(date(2026, 12, 16), base_cfg).ends_at == w.start_at


@pytest.mark.parametrize("golden_day, ends_at", [
    (date(2027, 2, 16), _at(2027, 2, 27, 2, 15)),    # March 1 - 2 days, common year
    (date(2028, 2, 16), _at(2028, 2, 28, 2, 15)),    # leap year: Feb 29 exists
])
def test_a_february_cycle_ends_two_days_before_march_1(base_cfg, golden_day, ends_at):
    assert window(golden_day, base_cfg).ends_at == ends_at


def test_day_31_skips_short_months(base_cfg):
    cfg = replace(base_cfg, "schedule", swap_freeze_days=[31])
    assert window(date(2027, 1, 31), cfg).ends_at == _at(2027, 3, 29, 2, 15)
    with pytest.raises(ValueError, match=r"not a schedule\.swap_freeze_days day"):
        window(date(2027, 4, 30), cfg)


def test_a_freeze_until_midnight_ends_the_next_day(base_cfg):
    cfg = replace(base_cfg, "schedule", swap_freeze_window=["11:30", "24:00"])
    w = window(date(2026, 12, 1), cfg)
    assert w.freeze_end == _at(2026, 12, 2) < w.ends_at


@pytest.mark.parametrize("now, golden_day", [
    (_at(2026, 11, 29, 2, 15), date(2026, 12, 1)),                         # start_at itself
    (_at(2026, 11, 29, 2, 15) - timedelta(microseconds=1), date(2026, 11, 16)),
    (_at(2026, 12, 6), date(2026, 12, 1)),                                 # after G, same cycle
    (_at(2026, 11, 17), date(2026, 11, 16)),                               # a late deploy
])
def test_the_cycle_that_owns_an_instant(base_cfg, now, golden_day):
    assert cycle_at(now, base_cfg).golden_day == golden_day


def test_cycles_tile_time_without_gaps_or_overlaps(base_cfg):
    now, end = _at(2027, 1, 1), _at(2029, 1, 1)
    while now < end:
        w = cycle_at(now, base_cfg)
        assert w.start_at <= now < w.ends_at
        following = cycle_at(w.ends_at, base_cfg)
        assert following.start_at == w.ends_at and following.golden_day > w.golden_day
        now += timedelta(hours=7)
