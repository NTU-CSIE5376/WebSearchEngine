from datetime import UTC, datetime

from helpers import example_config

from idxsel.schedule import (
    Action,
    blackout_reason,
    in_window,
    next_measurement,
    plan_next_action,
    swap_allowed,
)

S = example_config().schedule


def t(day, hour, minute=0, month=10):
    return datetime(2026, month, day, hour, minute, tzinfo=UTC)


def test_window_without_and_with_midnight_wrap():
    assert in_window(t(5, 3), "02:50-04:40")
    assert not in_window(t(5, 4, 40), "02:50-04:40")
    assert in_window(t(5, 23, 30), "23:00-01:00")
    assert in_window(t(5, 0, 30), "23:00-01:00")
    assert not in_window(t(5, 1, 0), "23:00-01:00")


def test_blackouts():
    assert blackout_reason(t(5, 3), S) == "daily blackout 02:50-04:40"
    assert "measurement-day" in blackout_reason(t(16, 6), S)
    assert blackout_reason(t(15, 6), S) is None


def test_swap_freeze_only_on_measure_days():
    assert not swap_allowed(t(16, 12), S)
    assert swap_allowed(t(16, 20, 1), S)
    assert swap_allowed(t(15, 12), S)


def test_next_measurement_crosses_month_end():
    assert next_measurement(t(31, 15), S) == datetime(2026, 11, 1, 12, tzinfo=UTC)
    assert next_measurement(t(16, 11), S) == t(16, 12)
    assert next_measurement(t(16, 12), S) == t(1, 12, month=11)


def test_plan_prefers_full_export_in_its_window():
    d = plan_next_action(t(14, 6), {}, S, full_refresh_due=True, has_base=True)
    assert d.action == Action.EXPORT_FULL
    assert d.measurement == t(16, 12)


def test_plan_builds_inside_build_window():
    d = plan_next_action(t(15, 18), {"last_delta_at": t(15, 5).isoformat()}, S,
                         full_refresh_due=False, has_base=True)
    assert d.action == Action.BUILD


def test_plan_builds_from_thirty_hours_before_but_not_inside_the_swap_freeze():
    done = {"last_delta_at": t(15, 5).isoformat()}
    assert plan_next_action(t(15, 5, 59), {}, S, False, True).action == Action.EXPORT_DELTA
    assert plan_next_action(t(15, 6), done, S, False, True).action == Action.BUILD
    assert plan_next_action(t(16, 11), done, S, False, True).action == Action.BUILD
    # Started inside the freeze, a build could only go live after both measurements.
    assert plan_next_action(t(16, 11, 45), done, S, False, True).action != Action.BUILD


def test_plan_runs_daily_delta_then_waits():
    d = plan_next_action(t(10, 6), {}, S, full_refresh_due=False, has_base=True)
    assert d.action == Action.EXPORT_DELTA
    d = plan_next_action(t(10, 6), {"last_delta_at": t(10, 5).isoformat()}, S,
                         full_refresh_due=False, has_base=True)
    assert d.action == Action.WAIT


def test_plan_waits_in_blackout_even_when_work_is_due():
    d = plan_next_action(t(15, 3), {}, S, full_refresh_due=False, has_base=True)
    assert d.action == Action.WAIT
    assert "blackout" in d.reason
