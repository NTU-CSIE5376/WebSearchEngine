"""Time windows (UTC): blackouts, swap freezes and the per-cycle plan.

All functions are pure: they take `now` and return a decision, so they are
easy to test across midnight and month boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any

from idxsel.config import ScheduleCfg


class Action(StrEnum):
    """What the daemon should do on this tick."""

    WAIT = "wait"
    EXPORT_FULL = "export_full"
    EXPORT_DELTA = "export_delta"
    BUILD = "build"


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str
    measurement: datetime | None = None


def _minutes(hhmm: str) -> int:
    hours, minutes = hhmm.split(":")
    h, m = int(hours), int(minutes)
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError(f"bad time {hhmm!r}")
    return h * 60 + m


def in_window(now: datetime, window: str) -> bool:
    """True if `now` (UTC) falls in `HH:MM-HH:MM`; windows may wrap midnight."""
    start_s, end_s = window.split("-")
    start, end = _minutes(start_s), _minutes(end_s)
    minute = now.astimezone(UTC).hour * 60 + now.astimezone(UTC).minute
    if start <= end:
        return start <= minute < end
    return minute >= start or minute < end


def is_measure_day(now: datetime, cfg: ScheduleCfg) -> bool:
    return now.astimezone(UTC).day in cfg.measure_days


def blackout_reason(now: datetime, cfg: ScheduleCfg) -> str | None:
    """Returns why DB reads are forbidden now, or None if they are allowed."""
    if not cfg.enabled:
        return None
    for window in cfg.blackout_daily_utc:
        if in_window(now, window):
            return f"daily blackout {window}"
    if is_measure_day(now, cfg):
        for window in cfg.blackout_measure_day_utc:
            if in_window(now, window):
                return f"measurement-day blackout {window}"
    return None


def swap_allowed(now: datetime, cfg: ScheduleCfg) -> bool:
    """False during the measurement-day freeze, so head and random see one list."""
    if not cfg.enabled or not is_measure_day(now, cfg):
        return True
    return not any(in_window(now, w) for w in cfg.swap_freeze_measure_day_utc)


def next_measurement(now: datetime, cfg: ScheduleCfg) -> datetime:
    """Returns the earliest measurement time strictly after `now`."""
    now = now.astimezone(UTC)
    minute = _minutes(cfg.measure_time_utc)
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    for _ in range(0, 64):
        if day.day in cfg.measure_days:
            candidate = day + timedelta(minutes=minute)
            if candidate > now:
                return candidate
        day += timedelta(days=1)
    raise ValueError("schedule.measure_days yields no measurement in the next 64 days")


def plan_next_action(now: datetime, state: dict[str, Any], cfg: ScheduleCfg,
                     full_refresh_due: bool, has_base: bool) -> Decision:
    """Chooses the daemon's next action.

    Args:
        now: Current time.
        state: Schedule state with optional keys `built_for`, `full_done_for`,
            `last_delta_at` (ISO timestamps).
        cfg: Schedule config.
        full_refresh_due: True if this cycle needs a full export.
        has_base: True if a completed base export exists.

    Returns:
        The decision and the measurement time it targets.
    """
    measurement = next_measurement(now, cfg)
    hours_left = (measurement - now).total_seconds() / 3600
    reason = blackout_reason(now, cfg)
    if reason:
        return Decision(Action.WAIT, reason, measurement)
    target = measurement.isoformat()
    if (
        (full_refresh_due or not has_base)
        and state.get("full_done_for") != target
        and cfg.full_export_deadline_h_before_measure
        <= hours_left
        <= cfg.full_export_start_h_before_measure
    ):
        return Decision(Action.EXPORT_FULL, f"full export window, {hours_left:.1f}h left",
                        measurement)
    # A build that starts inside the swap freeze cannot go live before this measurement.
    if (has_base and state.get("built_for") != target
            and hours_left <= cfg.build_start_h_before_measure and swap_allowed(now, cfg)):
        return Decision(Action.BUILD, f"build window, {hours_left:.1f}h left", measurement)
    last = state.get("last_delta_at")
    hours_since = (
        (now - datetime.fromisoformat(last)).total_seconds() / 3600 if last else float("inf")
    )
    today_delta = now.astimezone(UTC).replace(second=0, microsecond=0)
    past_delta_time = (today_delta.hour * 60 + today_delta.minute) >= _minutes(cfg.daily_delta_utc)
    if has_base and past_delta_time and hours_since >= cfg.min_hours_between_deltas:
        return Decision(Action.EXPORT_DELTA, "daily delta", measurement)
    return Decision(Action.WAIT, f"nothing due, {hours_left:.1f}h to measurement", measurement)
