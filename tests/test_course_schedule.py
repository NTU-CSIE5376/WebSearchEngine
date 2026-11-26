"""The December 2026 course schedule replaces the default golden jobs and keeps its state.

Nothing here spends a search: the runner's phase logic is pure, and `--preview` returns
before any request.
"""
import importlib.util
import re
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

DEPLOY = Path(__file__).resolve().parents[1] / "Metric" / "deploy"
RUNNER = DEPLOY / "course" / "run_golden.py"
TAIPEI = ZoneInfo("Asia/Taipei")


def _jobs(name):
    lines = (DEPLOY / name).read_text().splitlines()
    return [line for line in lines if line.strip() and not line.lstrip().startswith("#")]


def _runner(monkeypatch, state):
    monkeypatch.setenv("GOLDEN_COURSE_STATE", str(state))
    spec = importlib.util.spec_from_file_location("run_golden", RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _ticks(job):
    minute, hour, day, month, _ = job.split()[:5]

    def expand(field):
        out = set()
        for part in field.split(","):
            lo, _, hi = part.partition("-")
            out.update(range(int(lo), int(hi or lo) + 1))
        return out

    return expand(minute), expand(hour), expand(day), expand(month)


def test_the_course_crontab_has_no_default_golden_job():
    jobs = _jobs("crontab.course-202612")
    assert not [j for j in jobs if "--create" in j]
    runners = [j for j in jobs if "run_golden.py" in j]
    assert len(runners) == 1
    assert "/root/WebSearchEngine/Metric/deploy/course/run_golden.py" in runners[0]
    # The hourly status and export stay.
    assert [j for j in jobs if "--measure status" in j]
    assert [j for j in jobs if "migrate.py" in j]


def test_the_default_crontab_keeps_its_two_golden_jobs():
    jobs = _jobs("crontab")
    assert len([j for j in jobs if "--create" in j]) == 2
    assert not [j for j in jobs if "run_golden.py" in j]


def test_every_phase_start_has_a_cron_tick(monkeypatch, tmp_path):
    run = _runner(monkeypatch, tmp_path)
    (job,) = [j for j in _jobs("crontab.course-202612") if "run_golden.py" in j]
    minutes, hours, days, months = _ticks(job)
    for day, steps in run.DATES.items():
        for _, start, _ in steps:
            utc = datetime.combine(day, start, TAIPEI).astimezone(ZoneInfo("UTC"))
            assert utc.minute in minutes and utc.hour in hours
            assert utc.day in days and utc.month in months


def test_the_phases_run_in_order_once_each(monkeypatch, tmp_path):
    run = _runner(monkeypatch, tmp_path)
    day = datetime(2026, 12, 1, tzinfo=TAIPEI)

    def at(hhmm, state):
        h, m = map(int, hhmm.split(":"))
        return run.scheduled(day + timedelta(hours=h, minutes=m), state)

    assert at("08:04", {}) is None
    assert at("08:05", {}) == ("seed", 10)
    assert at("08:30", {"seed": "running"}) is None
    assert at("08:30", {"seed": "done"}) is None
    assert at("09:00", {"seed": "done"}) == ("random", 1000)
    assert at("09:00", {"seed": "failed"}) is None
    assert at("15:00", {"seed": "done", "random": "done"}) == ("head", 1000)
    assert at("19:45", {"seed": "done", "random": "done", "head": "done"}) is None
    dec8 = datetime(2026, 12, 8, 9, tzinfo=TAIPEI)
    assert run.scheduled(dec8, {"seed": "done"}) == ("head", 950)


def test_dec_8_shrinks_to_keep_dec_15_whole(monkeypatch, tmp_path):
    run = _runner(monkeypatch, tmp_path)
    dec8, dec1 = datetime(2026, 12, 8).date(), datetime(2026, 12, 1).date()
    assert run.allowed_target(dec8, "head", 950, left=2500, hourly=5000) == 490
    assert run.allowed_target(dec8, "head", 950, left=2010, hourly=5000) is None
    # Dec 1 head keeps 2,010 for Dec 15 and does not shrink.
    assert run.allowed_target(dec1, "head", 1000, left=3010, hourly=5000) == 1000
    assert run.allowed_target(dec1, "head", 1000, left=3009, hourly=5000) is None


def test_the_state_lives_where_the_env_says(monkeypatch, tmp_path):
    run = _runner(monkeypatch, tmp_path)
    assert run.STATE == tmp_path


@pytest.mark.parametrize("name", ["crontab", "crontab.course-202612"])
def test_the_compose_and_image_know_each_crontab(name):
    root = DEPLOY.parents[1]
    assert re.search(r"Metric/deploy/crontab Metric/deploy/crontab\.course-202612",
                     (root / "Dockerfile").read_text())
    assert "METRIC_CRONTAB" in (root / "docker-compose.yml").read_text()
    assert (DEPLOY / name).exists()
