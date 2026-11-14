"""Golden days, the swap freeze, and the refresh cycle of each golden day (UTC).

The metric creates a golden batch on each `schedule.swap_freeze_days` day and reads
`selected_urls_current` inside `schedule.swap_freeze_window`. Those days are the golden
days G. This module is the only code that turns them into datetimes: `publish.swap`,
`recrawl.sync` and `refresh` all ask it, so they agree on when the metric reads.

A day number counts in every month that has it: 31 is absent from 30-day months. The
calendar walks day by day, so it never builds an invalid date.

Every instant belongs to exactly one cycle, the one whose `start_at` came last. A
cycle's start is its golden day shifted by a constant, so start times rise with G.

Standard library only (`isel.config` is too), so the long-lived refresh parent imports
it without DuckDB. Every function refuses a naive datetime: `astimezone` would read it
as the host's local time.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

from isel.config import hhmm_minutes

if TYPE_CHECKING:
    from isel.config import Config, ScheduleCfg

# Four consecutive years hold every pattern of month lengths, a leap February included.
_GAP_SPAN = (date(2027, 1, 1), date(2031, 1, 1))
# A day number recurs within 62 days: 31 skips from Mar 31 to May 31 (61).
_MAX_GAP_DAYS = 62


def utc(at: datetime) -> datetime:
    """`at` in UTC.

    Raises:
        ValueError: If `at` is naive.
    """
    if at.tzinfo is None or at.utcoffset() is None:
        raise ValueError(f"naive datetime {at.isoformat()}; pass an aware datetime")
    return at.astimezone(UTC)


@dataclass(frozen=True)
class Window:
    """The refresh cycle of golden day G: it owns `[start_at, ends_at)`.

    Raises:
        ValueError: On construction, if a field is naive or the fields do not satisfy
            `start_at < swap_by < freeze_start < freeze_end <= ends_at`.
    """

    golden_day: date
    start_at: datetime      # G - start_days_before at start_time: the cycle may begin
    swap_by: datetime       # freeze_start - swap_margin_min: no build step runs past it
    freeze_start: datetime  # the metric reads the list from here ...
    freeze_end: datetime    # ... to here
    ends_at: datetime       # the next golden day's start_at

    def __post_init__(self) -> None:
        for f in fields(self):
            if f.name != "golden_day":
                utc(getattr(self, f.name))
        if not (self.start_at < self.swap_by < self.freeze_start < self.freeze_end
                <= self.ends_at):
            raise ValueError(f"the window of {self.golden_day} is out of order")


def swap_allowed(now: datetime, cfg: ScheduleCfg) -> bool:
    """False while the metric may read the list (a golden day, inside the freeze window).

    Raises:
        ValueError: If `now` is naive.
    """
    now = utc(now)
    if now.day not in cfg.swap_freeze_days:
        return True
    start, end = (hhmm_minutes(v) for v in cfg.swap_freeze_window)
    return not start <= now.hour * 60 + now.minute < end


def golden_days(start: date, days: Sequence[int]) -> Iterator[date]:
    """Golden days on or after `start`, ascending, without end.

    `days` must be non-empty and within 1-31 (`config` checks both).
    """
    wanted = frozenset(days)
    day = start
    while True:
        if day.day in wanted:
            yield day
        day += timedelta(days=1)


def min_gap_days(days: Sequence[int]) -> int:
    """The fewest days between two consecutive golden days in any year.

    [1, 16] gives 13: Feb 16 to Mar 1 in a common year.

    Raises:
        ValueError: If `days` is empty.
    """
    if not days:
        raise ValueError("no golden days")
    hits = []
    for day in golden_days(_GAP_SPAN[0], days):
        if day >= _GAP_SPAN[1]:
            break
        hits.append(day)
    return min((b - a).days for a, b in itertools.pairwise(hits))


def _at(day: date, hhmm: str) -> datetime:
    """`day` at HH:MM UTC; "24:00" is the next midnight."""
    return datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(
        minutes=hhmm_minutes(hhmm))


def _start_at(golden_day: date, cfg: Config) -> datetime:
    return _at(golden_day - timedelta(days=cfg.refresh.start_days_before),
               cfg.refresh.start_time)


def window(golden_day: date, cfg: Config) -> Window:
    """The cycle of `golden_day`.

    Raises:
        ValueError: If `golden_day` is not a golden day.
    """
    s = cfg.schedule
    if golden_day.day not in s.swap_freeze_days:
        raise ValueError(f"{golden_day} is not a schedule.swap_freeze_days day")
    freeze_start, freeze_end = (_at(golden_day, v) for v in s.swap_freeze_window)
    following = next(golden_days(golden_day + timedelta(days=1), s.swap_freeze_days))
    return Window(golden_day, _start_at(golden_day, cfg),
                  freeze_start - timedelta(minutes=cfg.refresh.swap_margin_min),
                  freeze_start, freeze_end, _start_at(following, cfg))


def cycle_at(now: datetime, cfg: Config) -> Window:
    """The cycle that owns `now`: `start_at <= now < ends_at`.

    Raises:
        ValueError: If `now` is naive.
    """
    now = utc(now)
    first = now.date() - timedelta(days=cfg.refresh.start_days_before + _MAX_GAP_DAYS)
    owner = None
    for golden_day in golden_days(first, cfg.schedule.swap_freeze_days):
        if _start_at(golden_day, cfg) > now:
            break
        owner = golden_day
    assert owner is not None, "the search starts more than one gap before now"
    return window(owner, cfg)
