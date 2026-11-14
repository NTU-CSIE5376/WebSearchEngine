"""The refresh cycle without Docker: plan, evidence on disk, outcome, tick, lock, CLI.

selectdb is replaced by `FakeDb` at `refresh._read_db`, the one function that reads it.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import replace as dc_replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from conftest import BASE, replace

from isel import refresh
from isel.cli import COMMANDS, build_parser
from isel.cli import main as cli_main
from isel.config import load_config
from isel.io import LockBusy, atomic_write_json, file_lock
from isel.refresh import (
    BUILD,
    STEPS,
    Completed,
    DbRun,
    Deferred,
    Evidence,
    ExportState,
    Failed,
    Finish,
    GateVerdict,
    Idle,
    Job,
    Killed,
    NotYet,
    Outcome,
    Run,
    ScoredState,
    Status,
    StepName,
    TagCoverage,
    VerifyRecord,
    execute,
    judge,
    plan,
    read_outcome,
    record,
    run_in_child,
    spawn_runner,
    status,
    tick,
    utcnow,
    work_lock,
    work_lock_holder,
    write_outcome,
)
from isel.schedule import window

G = date(2026, 12, 1)
EXPORT_ID = "2026-11-29T03:00:00+00:00"
CUTOFF = datetime.fromisoformat(EXPORT_ID)
OLD = DbRun(1, "r0", "live", datetime(2026, 11, 14, 3, tzinfo=UTC))
NEW_LIVE = DbRun(2, "r1", "live", CUTOFF)
PASSED = GateVerdict("r1", "r0", 9, 0.5, {"head": TagCoverage(0.5, 0.4, 1.25)}, True)
S = StepName


@pytest.fixture
def cfg(tmp_path):
    return replace(load_config([BASE]), "run", work_dir=str(tmp_path / "work"))


@pytest.fixture
def w(cfg):
    return window(G, cfg)


def _at(month, day, hh, mm=0):
    return datetime(2026, month, day, hh, mm, tzinfo=UTC)


# ---------------------------------------------------------------- plan


def evidence_through(step: StepName | None, **over) -> Evidence:
    """Evidence with every build step up to `step` done; None leaves the old export."""
    order = [s.name for s in BUILD]
    done = set(order[:order.index(step) + 1]) if step else set()
    swapped = S.SWAP in done
    fields = {
        "export": (ExportState.FINISHED if S.EXPORT in done
                   else ExportState.ABSENT if S.CLEAR in done else ExportState.STALE),
        "export_id": EXPORT_ID if S.EXPORT in done else None,
        "golden": S.GOLDEN in done, "dims": S.DIMS in done,
        "sample": S.SAMPLE in done, "positives": S.POSITIVES in done,
        "model": Path("/work/models/m1") if S.MODEL in done else None,
        "scored": ScoredState.FINISHED if S.SCORED in done else ScoredState.ABSENT,
        "run": "r1" if S.SELECTION in done else None,
        "db_run": (NEW_LIVE if swapped else DbRun(2, "r1", "validated", CUTOFF))
        if S.PUBLISH in done else None,
        "live": NEW_LIVE if swapped else OLD,
        "prev": "r0" if swapped else None,
        "selection": frozenset({"r0"} | ({"r1"} if S.SELECTION in done else set())),
        "selectdb_bytes": 1,
    }
    return Evidence(**{**fields, **over})


def outcome_through(step: StepName | None, **over) -> Outcome:
    gated = step is not None and [s.name for s in BUILD].index(step) >= [
        s.name for s in BUILD].index(S.GATES)
    return Outcome(G, gates=PASSED if gated else None, **over)


@pytest.mark.parametrize("i", range(len(BUILD)))
def test_plan_runs_the_first_build_step_not_done(cfg, w, i):
    before = BUILD[i - 1].name if i else None
    decision = plan(w, evidence_through(before), outcome_through(before), _at(11, 29, 3),
                    cfg.refresh)
    assert decision == Run(BUILD[i].name, w.swap_by)


def test_plan_at_the_cycle_start_clears_the_previous_export(cfg, w):
    assert plan(w, evidence_through(None), Outcome(G), w.start_at, cfg.refresh) == Run(
        S.CLEAR, w.swap_by)


@pytest.mark.parametrize("terminal", [s for s in Status if s is not Status.RUNNING])
def test_plan_leaves_a_finished_cycle_alone(cfg, w, terminal):
    decision = plan(w, evidence_through(None), Outcome(G, status=terminal), _at(11, 29, 3),
                    cfg.refresh)
    assert isinstance(decision, Idle)


def test_plan_skips_a_cycle_that_missed_swap_by(cfg, w):
    decision = plan(w, evidence_through(S.MODEL), Outcome(G), w.swap_by, cfg.refresh)
    assert decision == Finish(Status.SKIPPED_LATE, S.SCORED)


def test_plan_follows_a_live_run_of_this_cycle_after_swap_by(cfg, w):
    # A swap that committed before a crash: the frontier must still follow.
    decision = plan(w, evidence_through(S.SWAP), Outcome(G), _at(12, 1, 11, 10), cfg.refresh)
    assert decision == Run(S.VERIFY, w.ends_at)


def test_plan_reads_a_live_row_without_cutoff_as_an_earlier_cycle(cfg, w):
    ev = evidence_through(S.SELECTION, live=dc_replace(OLD, cutoff_at=None))
    assert plan(w, ev, Outcome(G), w.swap_by, cfg.refresh).status is Status.SKIPPED_LATE


@pytest.mark.parametrize("taken_off", ["rolled_back", "abandoned", "retired"])
def test_plan_stops_a_cycle_whose_run_was_taken_off(cfg, w, taken_off):
    ev = evidence_through(S.PUBLISH, db_run=DbRun(2, "r1", taken_off, CUTOFF))
    decision = plan(w, ev, outcome_through(S.PUBLISH), _at(12, 1, 9), cfg.refresh)
    assert decision == Finish(Status.WITHDRAWN)


def test_plan_stops_at_a_rejected_gate(cfg, w):
    rejected = dc_replace(PASSED, passed=False)
    decision = plan(w, evidence_through(S.SELECTION), Outcome(G, gates=rejected),
                    _at(11, 30, 3), cfg.refresh)
    assert decision == Finish(Status.SKIPPED_GATE, S.GATES)


def test_plan_gates_again_when_the_verdict_was_for_another_run(cfg, w):
    other = dc_replace(PASSED, run_label="r9", passed=False)
    decision = plan(w, evidence_through(S.SELECTION), Outcome(G, gates=other), _at(11, 30, 3),
                    cfg.refresh)
    assert decision == Run(S.GATES, w.swap_by)


def test_plan_fails_a_step_after_max_attempts(cfg, w):
    n = cfg.refresh.max_attempts
    ev = evidence_through(S.POSITIVES)
    assert plan(w, ev, Outcome(G, failures={S.MODEL: n - 1}), _at(11, 30, 3),
                cfg.refresh) == Run(S.MODEL, w.swap_by)
    assert plan(w, ev, Outcome(G, failures={S.MODEL: n}), _at(11, 30, 3),
                cfg.refresh) == Finish(Status.FAILED, S.MODEL)


def test_plan_counts_failures_per_step(cfg, w):
    out = Outcome(G, failures={S.EXPORT: cfg.refresh.max_attempts})
    assert plan(w, evidence_through(S.POSITIVES), out, _at(11, 30, 3), cfg.refresh) == Run(
        S.MODEL, w.swap_by)


VERIFIED = outcome_through(S.SWAP, verify=VerifyRecord("r1", 9, {"head": 0.5}))


@pytest.mark.parametrize("now, decision", [
    # The budget is 2 h, so a sync may still start at 09:30 before the 11:30 freeze.
    (_at(12, 1, 9, 30), "run_until_freeze"),
    (_at(12, 1, 9, 31), "idle"),
    (_at(12, 1, 15), "idle"),
    (_at(12, 1, 20), "run_until_end"),
    (_at(11, 29, 4), "run_until_freeze"),
])
def test_plan_starts_a_sync_only_with_its_budget_outside_the_freeze(cfg, w, now, decision):
    got = plan(w, evidence_through(S.SWAP), VERIFIED, now, cfg.refresh)
    expected = {"run_until_freeze": Run(S.SYNC, w.freeze_start),
                "run_until_end": Run(S.SYNC, w.ends_at)}.get(decision)
    assert got == expected if expected else isinstance(got, Idle)


def test_plan_verifies_inside_the_freeze(cfg, w):
    # verify reads 15k URLs as the metric does; only the sync waits for the freeze.
    assert plan(w, evidence_through(S.SWAP), outcome_through(S.SWAP), _at(12, 1, 12),
                cfg.refresh) == Run(S.VERIFY, w.ends_at)


def test_plan_cleans_up_then_finishes(cfg, w):
    synced = dc_replace(VERIFIED, synced_run_id=NEW_LIVE.id)
    stray = evidence_through(S.SWAP, selection=frozenset({"r0", "r1", "r9"}))
    assert plan(w, stray, synced, _at(12, 1, 21), cfg.refresh) == Run(S.CLEANUP, w.ends_at)
    assert plan(w, evidence_through(S.SWAP), synced, _at(12, 1, 21),
                cfg.refresh) == Finish(Status.DONE)


def test_plan_never_builds_after_a_swap_even_without_a_gate_record(cfg, w):
    decision = plan(w, evidence_through(S.SWAP), Outcome(G), _at(11, 30, 3), cfg.refresh)
    assert decision == Run(S.VERIFY, w.ends_at)


def test_plan_syncs_again_when_the_live_run_changed(cfg, w):
    # A sync recorded for another run id does not count for this live run.
    stale = dc_replace(VERIFIED, synced_run_id=OLD.id)
    assert plan(w, evidence_through(S.SWAP), stale, _at(12, 1, 21), cfg.refresh) == Run(
        S.SYNC, w.ends_at)


@pytest.mark.parametrize("over", [
    {"sample": True},                                   # a sample without the export
    {"model": Path("/work/models/m1")},                 # a model without its inputs
    {"run": "r1"},                                      # a run without finished scores
])
def test_evidence_refuses_a_chain_with_a_missing_link(over):
    with pytest.raises(ValueError, match="inconsistent evidence"):
        evidence_through(S.CLEAR, **over)


def test_evidence_refuses_a_row_of_another_run():
    with pytest.raises(ValueError, match="inconsistent evidence"):
        evidence_through(S.PUBLISH, db_run=DbRun(5, "r5", "validated", CUTOFF))


def test_judge_needs_both_gates():
    rc = load_config([BASE]).refresh                    # overlap 0.3, coverage 0.9
    good = {"head": TagCoverage(0.5, 0.5, 1.0), "random": TagCoverage(0.2, 0.0, None)}
    assert judge(0.3, good, rc) is True                 # at the threshold; no live hit passes
    assert judge(0.29, good, rc) is False
    assert judge(0.9, {**good, "head": TagCoverage(0.44, 0.5, 0.88)}, rc) is False


# ---------------------------------------------------------------- evidence on disk


def _touch(path: Path, at: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    os.utime(path, (at.timestamp(), at.timestamp()))


def build_disk(work: Path, through: StepName, started: str = EXPORT_ID, k: int = 64_000_000):
    """Writes the stamps the libraries leave, up to `through` (a derived build step)."""
    order = [s.name for s in BUILD]
    done = set(order[:order.index(through) + 1])
    if S.GOLDEN in done:
        _touch(work / "golden" / "summary.json", CUTOFF)
    if S.DIMS in done:
        _touch(work / "dims" / "split_hosts.parquet", CUTOFF)
    if S.EXPORT in done:
        atomic_write_json(work / "full" / "export" / "_meta.json",
                          {"started_at": started, "finished_at": started})
    if S.SAMPLE in done:
        atomic_write_json(work / "sample" / "_meta.json",
                          {"export_started_at": started, "finished_at": started})
    if S.POSITIVES in done:
        atomic_write_json(work / "positives" / "_meta.json", {"export_started_at": started})
    model = work / "models" / "20261129T080000000000Z"
    if S.MODEL in done:
        atomic_write_json(model / "lgbm_full" / "meta.json",
                          {"sample_fingerprint": {"export_started_at": started}})
    if S.SCORED in done:
        atomic_write_json(work / "full" / "scored" / "_meta.json", {
            "model_dir": str(model), "export_started_at": started, "k": k,
            "finished_at": started, "kept_check": {"passed": True}})
    if S.SELECTION in done:
        atomic_write_json(work / "full" / "selection" / "run=r1" / "_MANIFEST.json",
                          {"scope": "full", "model_dir": str(model), "k": k, "rows": k})
    return model


class FakeDb:
    """Stands in for `_read_db`: the live row, rows by label, prev."""

    def __init__(self, live=OLD, rows=None, prev=None, error=None):
        self.live, self.rows, self.prev, self.error = live, rows or {}, prev, error
        self.calls = 0

    def __call__(self, cfg, ev):
        self.calls += 1
        if self.error:
            raise self.error
        return dc_replace(ev, live=self.live, db_run=self.rows.get(ev.run) if ev.run else None,
                          prev=self.prev, selectdb_bytes=1)


@pytest.fixture
def db(monkeypatch):
    fake = FakeDb()
    monkeypatch.setattr(refresh, "_read_db", fake)
    return fake


def next_step(cfg):
    w = window(G, cfg)
    return plan(w, refresh.observe(cfg, w), read_outcome(cfg, G), _at(11, 29, 9), cfg.refresh)


def test_disk_an_export_from_before_the_cycle_is_cleared(cfg, db):
    build_disk(cfg.work_dir, S.SELECTION, started="2026-11-14T03:00:00+00:00")
    assert next_step(cfg).step is S.CLEAR


def test_disk_parts_of_an_export_killed_before_its_meta_are_cleared(cfg, db):
    _touch(cfg.work_dir / "full" / "export" / "shard=000" / "part-0-1.parquet", CUTOFF)
    assert next_step(cfg).step is S.CLEAR


def test_disk_an_empty_work_dir_starts_with_golden(cfg, db):
    assert next_step(cfg).step is S.GOLDEN


def test_disk_golden_and_dims_count_from_the_cycle_start(cfg, db, w):
    _touch(cfg.work_dir / "golden" / "summary.json", w.start_at - timedelta(seconds=1))
    assert next_step(cfg).step is S.GOLDEN
    _touch(cfg.work_dir / "golden" / "summary.json", w.start_at)
    assert next_step(cfg).step is S.DIMS


def test_disk_an_unfinished_export_resumes(cfg, db):
    build_disk(cfg.work_dir, S.DIMS)
    atomic_write_json(cfg.work_dir / "full" / "export" / "_meta.json",
                      {"started_at": EXPORT_ID, "shards_done": [0]})
    assert next_step(cfg).step is S.EXPORT


def test_disk_a_sample_of_another_export_is_derived_again(cfg, db):
    build_disk(cfg.work_dir, S.POSITIVES)
    atomic_write_json(cfg.work_dir / "sample" / "_meta.json", {
        "export_started_at": "2026-11-14T03:00:00+00:00", "finished_at": EXPORT_ID})
    assert next_step(cfg).step is S.SAMPLE


def test_disk_positives_without_meta_are_derived_again(cfg, db):
    build_disk(cfg.work_dir, S.SAMPLE)
    _touch(cfg.work_dir / "positives" / "batch=1.parquet", CUTOFF)
    assert next_step(cfg).step is S.POSITIVES


def test_disk_the_newest_model_of_this_export_is_scored(cfg, db):
    stamped = build_disk(cfg.work_dir, S.SCORED)
    models = cfg.work_dir / "models"
    atomic_write_json(models / "20261129T070000000000Z" / "lgbm_full" / "meta.json",
                      {"sample_fingerprint": {"export_started_at": EXPORT_ID}})
    (models / "20261129T090000000000Z" / "lgbm_full").mkdir(parents=True)   # killed in save
    assert next_step(cfg).step is S.SELECTION
    newer = models / "20261129T085000000000Z"
    atomic_write_json(newer / "lgbm_full" / "meta.json",
                      {"sample_fingerprint": {"export_started_at": EXPORT_ID}})
    assert next_step(cfg).step is S.SCORED      # the scores belong to the older model
    assert refresh.observe(cfg, window(G, cfg)).scored is ScoredState.FOREIGN
    assert stamped != newer


@pytest.mark.parametrize("meta", [
    {"finished_at": None},                      # unfinished
    {"failed": "kept share z = 9.0"},
    {"k": 1000},                                # another K
])
def test_disk_scores_that_are_not_finished_for_this_model_are_scored(cfg, db, meta):
    build_disk(cfg.work_dir, S.SCORED)
    path = cfg.work_dir / "full" / "scored" / "_meta.json"
    data = json.loads(path.read_text())
    if meta.get("finished_at", 0) is None:
        del data["finished_at"]
    else:
        data.update(meta)
    atomic_write_json(path, data)
    assert next_step(cfg).step is S.SCORED


def test_disk_a_run_without_its_manifest_file_is_selected_again_and_cleaned(cfg, db):
    build_disk(cfg.work_dir, S.SCORED)
    (cfg.work_dir / "full" / "selection" / "run=r1" / "manifest").mkdir(parents=True)
    ev = refresh.observe(cfg, window(G, cfg))
    assert ev.run is None and ev.selection == {"r1"}
    assert next_step(cfg).step is S.SELECTION


def test_disk_a_finished_run_goes_to_the_gates(cfg, db):
    build_disk(cfg.work_dir, S.SELECTION)
    assert next_step(cfg).step is S.GATES


# ---------------------------------------------------------------- outcome


def test_record_counts_failures_per_step_and_keeps_only_the_kind():
    gates = refresh.STEP[S.GATES]
    out = record(Outcome(G), gates, Failed("OperationalError"))
    out = record(out, refresh.STEP[S.EXPORT], Failed("QueryCanceled"))
    assert out.failures == {S.GATES: 1, S.EXPORT: 1}
    assert out.error == refresh.StepError(S.EXPORT, "QueryCanceled")
    killed = record(out, gates, Killed())
    assert killed.failures == out.failures and killed.error.kind == "deadline"
    assert record(out, gates, Deferred()) == out
    assert record(out, refresh.STEP[S.SAMPLE], Completed({"rows": 1})) == out
    assert record(out, gates, Completed(PASSED)).gates == PASSED


def test_outcome_round_trips_through_its_file(cfg):
    out = Outcome(
        G, status=Status.DONE, failures={S.EXPORT: 2}, error=refresh.StepError(None, "x"),
        pending=S.SYNC, gates=PASSED, verify=VerifyRecord("r1", 9, {"head": 0.25}),
        synced_run_id=7, audit=refresh.Audit("r1", 2, "r1", 10, "dev", _at(12, 1, 21)))
    write_outcome(cfg, out)
    assert read_outcome(cfg, G) == out


def _boom(job):
    raise RuntimeError("password=hunter2 postgres://u:p@db.example/selectdb")


def test_a_failure_stores_the_exception_class_never_its_message(cfg, db):
    result = tick(cfg, lambda: _at(11, 29, 3), lambda job, step, deadline: execute(_boom, job))
    assert result.result == Failed("RuntimeError")
    text = refresh.outcome_path(cfg, G).read_text()
    assert "RuntimeError" in text and "hunter2" not in text and "postgres://" not in text


def test_execute_maps_how_an_act_ended(cfg, w):
    job = Job(cfg, w, evidence_through(None), utcnow)

    def not_yet(job):
        raise NotYet("12 shards missing")

    def interrupted(job):
        raise KeyboardInterrupt

    assert execute(lambda job: 5, job) == Completed(5)
    assert execute(not_yet, job) == Deferred()
    assert execute(_boom, job) == Failed("RuntimeError")
    with pytest.raises(KeyboardInterrupt):
        execute(interrupted, job)


# ---------------------------------------------------------------- tick


def _never(job, step, deadline):
    pytest.fail(f"no step may run, got {step}")


def test_tick_runs_nothing_while_a_command_holds_the_lock(cfg, db):
    with work_lock(cfg, exclusive=False):
        result = tick(cfg, lambda: _at(11, 29, 3), _never)
    assert isinstance(result.decision, Idle)
    assert not refresh.outcome_path(cfg, G).exists()


def test_a_second_tick_runs_nothing_while_one_holds_the_lock(cfg, db):
    with work_lock(cfg, exclusive=True):
        assert isinstance(tick(cfg, lambda: _at(11, 29, 3), _never).decision, Idle)


def test_tick_on_a_finished_cycle_reads_nothing(cfg, monkeypatch):
    write_outcome(cfg, Outcome(G, status=Status.DONE))
    fake = FakeDb(error=AssertionError("selectdb read"))
    monkeypatch.setattr(refresh, "_read_db", fake)
    with work_lock(cfg, exclusive=True):              # not even the lock is needed
        result = tick(cfg, lambda: _at(11, 30, 3), _never)
    assert isinstance(result.decision, Idle) and fake.calls == 0


def test_tick_fails_a_step_that_completed_without_its_evidence(cfg, db):
    result = tick(cfg, lambda: _at(11, 29, 3), lambda job, step, deadline: Completed(None))
    assert result.result == Failed("no_evidence")
    out = read_outcome(cfg, G)
    assert out.failures == {S.GOLDEN: 1} and out.error.kind == "no_evidence"


def test_tick_records_nothing_for_a_step_that_completed_with_its_evidence(cfg, db):
    def pull(job, step, deadline):
        _touch(job.cfg.work_dir / "golden" / "summary.json", _at(11, 29, 3))
        return Completed({"batches": 9})

    result = tick(cfg, lambda: _at(11, 29, 3), pull)
    assert result.again and read_outcome(cfg, G).failures == {}
    assert next_step(cfg).step is S.DIMS


def test_tick_runs_each_step_with_an_empty_scratch_dir(cfg, db):
    scratch = cfg.work_dir / "duckdb_tmp"
    (scratch / "duckdb_temp_storage-0.tmp").parent.mkdir(parents=True)
    (scratch / "duckdb_temp_storage-0.tmp").write_bytes(b"x" * 100)
    seen = []
    tick(cfg, lambda: _at(11, 29, 3),
         lambda job, step, deadline: seen.append(list(scratch.glob("*"))) or Deferred())
    assert seen == [[]]


def test_tick_gives_the_step_its_deadline_and_the_injected_clock(cfg, db, w):
    seen = []
    clock = lambda: _at(11, 29, 3)  # noqa: E731
    tick(cfg, clock, lambda job, step, deadline: seen.append(
        (step, deadline, job.clock is clock, job.window == w)) or Deferred())
    assert seen == [(S.GOLDEN, w.swap_by, True, True)]


def test_tick_ends_an_unfinished_earlier_cycle(cfg, db):
    write_outcome(cfg, Outcome(date(2026, 11, 16), failures={S.SYNC: 1}))
    write_outcome(cfg, Outcome(date(2026, 11, 1), status=Status.DONE))
    tick(cfg, lambda: _at(11, 29, 3), lambda job, step, deadline: Deferred())
    earlier = read_outcome(cfg, date(2026, 11, 16))
    assert earlier.status is Status.FAILED and earlier.error.kind == "window_ended"
    assert read_outcome(cfg, date(2026, 11, 1)).status is Status.DONE


def test_tick_writes_the_terminal_outcome_once(cfg, db):
    # 2026-12-01 11:00 is swap_by and nothing was built.
    result = tick(cfg, lambda: _at(12, 1, 11), _never)
    out = read_outcome(cfg, G)
    assert result.decision == Finish(Status.SKIPPED_LATE, S.GOLDEN)
    assert out.status is Status.SKIPPED_LATE and out.pending is S.GOLDEN
    assert out.audit.live_label == "r0"


def test_run_loop_ticks_again_only_after_a_completed_step(cfg, monkeypatch, capsys):
    results = iter([
        refresh.TickResult(G, Run(S.GOLDEN, _at(12, 1, 11)), Completed(None)),
        refresh.TickResult(G, Idle("waiting")),
        RuntimeError("selectdb is down"),
        refresh.TickResult(G, Run(S.DIMS, _at(12, 1, 11)), Deferred()),
    ])

    def fake_tick(cfg, clock, runner):
        item = next(results)
        if isinstance(item, Exception):
            raise item
        return item

    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 3:
            raise SystemExit

    monkeypatch.setattr(refresh, "tick", fake_tick)
    monkeypatch.setattr(signal, "signal", lambda *a: None)
    with pytest.raises(SystemExit):
        refresh.run_loop(cfg, sleep=sleep)
    assert sleeps == [cfg.refresh.poll_s] * 3     # none after the completed step
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["decision"] for line in lines] == [
        {"run": "golden", "deadline": _at(12, 1, 11).isoformat()}, {"idle": "waiting"},
        {"run": "dims", "deadline": _at(12, 1, 11).isoformat()}]


# ---------------------------------------------------------------- status


def test_status_changes_nothing(cfg, db):
    build_disk(cfg.work_dir, S.SELECTION)

    def tree():
        return sorted((str(p), p.stat().st_mtime_ns) for p in cfg.work_dir.rglob("*"))

    before = tree()
    report = status(cfg, lambda: _at(11, 30, 3))
    assert tree() == before
    assert report["next"] == {"run": "gates", "deadline": _at(12, 1, 11).isoformat()}
    assert report["work_lock"] == "free"
    assert [s["done"] for s in report["steps"]] == [True] * 9 + [False] * 6


def test_status_names_the_error_when_selectdb_cannot_be_read(cfg, monkeypatch):
    monkeypatch.setattr(refresh, "_read_db", FakeDb(error=ConnectionRefusedError("x")))
    report = status(cfg, lambda: _at(11, 30, 3))
    assert report["observe_error"] == "ConnectionRefusedError"
    assert report["cycle"]["golden_day"] == G.isoformat() and "next" not in report


def test_status_names_the_lock_holder(cfg):
    assert work_lock_holder(cfg) == "free"
    with work_lock(cfg, exclusive=False):
        assert work_lock_holder(cfg) == "commands"
    with work_lock(cfg, exclusive=True):
        assert work_lock_holder(cfg) == "refresh"
    assert work_lock_holder(cfg) == "free"


# ---------------------------------------------------------------- lock and CLI


def test_file_lock_shared_and_exclusive_holders(tmp_path):
    path = tmp_path / "lock"
    with file_lock(path, shared=True), file_lock(path, shared=True), pytest.raises(LockBusy), \
            file_lock(path):
        pass
    with file_lock(path), pytest.raises(LockBusy), file_lock(path, shared=True):
        pass
    with file_lock(path):
        pass


def test_file_lock_dies_with_its_holder(tmp_path):
    path = tmp_path / "lock"
    holder = subprocess.Popen(
        [sys.executable, "-c", "import fcntl, sys, time; f = open(sys.argv[1], 'a'); "
         "fcntl.flock(f, fcntl.LOCK_EX); print('held', flush=True); time.sleep(60)",
         str(path)], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        with pytest.raises(LockBusy), file_lock(path, shared=True):
            pass
    finally:
        holder.kill()
        holder.wait()
    with file_lock(path):
        pass


@pytest.fixture
def cli_config(tmp_path):
    overlay = tmp_path / "overlay.toml"
    overlay.write_text(f'[run]\nwork_dir = "{tmp_path / "work"}"\n')
    return ["--config", str(BASE), "--config", str(overlay)]


def test_work_commands_refuse_to_run_beside_a_refresh_step(tmp_path, cli_config, capsys):
    cfg = load_config([BASE, tmp_path / "overlay.toml"])
    with work_lock(cfg, exclusive=True):
        assert cli_main([*cli_config, "publish", "--run-dir", "/nowhere"]) == 3
        assert "refresh is running a step" in capsys.readouterr().err
        assert cli_main([*cli_config, "check-config"]) == 0


def test_work_commands_run_beside_each_other(tmp_path, cli_config):
    cfg = load_config([BASE, tmp_path / "overlay.toml"])
    # The command's own error: it got past the lock into its code.
    with work_lock(cfg, exclusive=False), pytest.raises(
            RuntimeError, match="the full export is unfinished"):
        cli_main([*cli_config, "derive-sample"])


def test_every_command_decides_whether_it_needs_the_work_lock():
    subcommands = next(a for a in build_parser()._actions if a.dest == "command").choices
    assert set(subcommands) == set(COMMANDS)
    assert {name for name, c in COMMANDS.items() if not c.work} == {
        "check-config", "report", "verify-live", "recrawl-plan", "recrawl-archive",
        "text-index-build", "text-index-eval", "validate-manifest", "refresh"}


def test_refresh_needs_exactly_one_mode(cli_config, capsys):
    with pytest.raises(SystemExit):
        cli_main([*cli_config, "refresh"])
    with pytest.raises(SystemExit):
        cli_main([*cli_config, "refresh", "--once", "--status"])


def test_the_refresh_parent_never_loads_duckdb(tmp_path):
    # The parent idles for days on a 144-core host: DuckDB would start a thread per core.
    code = (
        "import sys\n"
        "from datetime import UTC, datetime\n"
        "from pathlib import Path\n"
        "import dataclasses\n"
        "import isel.cli, isel.refresh as r, isel.schedule\n"
        "from isel.config import load_config\n"
        "cfg = load_config([sys.argv[1]])\n"
        "cfg = dataclasses.replace(cfg, run=dataclasses.replace(cfg.run, work_dir=sys.argv[2]))\n"
        "now = datetime(2026, 11, 29, 3, tzinfo=UTC)\n"
        "w = isel.schedule.cycle_at(now, cfg)\n"
        "ev = r._read_disk(cfg.work_dir, w, cfg.eval.k_headline)\n"
        "out = r.record(r.read_outcome(cfg, w.golden_day), r.STEP[r.plan(w, ev, "
        "r.read_outcome(cfg, w.golden_day), now, cfg.refresh).step], r.Failed('x'))\n"
        "r.write_outcome(cfg, out)\n"
        "r.work_lock_holder(cfg)\n"
        "print('duckdb' in sys.modules)\n")
    out = subprocess.run([sys.executable, "-c", code, str(BASE), str(tmp_path)],
                         capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "False"


# ---------------------------------------------------------------- a step in its own process


def _child_pid(job):
    return os.getpid()


def _sleep_with_a_grandchild(job):
    grandchild = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    (job.cfg.work_dir / "pids").write_text(f"{os.getpid()} {grandchild.pid}")
    time.sleep(60)


def _gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def test_a_step_runs_in_its_own_process(cfg, w):
    job = Job(replace(cfg, "refresh", kill_grace_s=1.0), w, evidence_through(None), utcnow)
    result = run_in_child(_child_pid, job, utcnow() + timedelta(seconds=60))
    assert isinstance(result, Completed) and result.value != os.getpid()


def test_a_step_past_its_deadline_is_killed_with_its_children(cfg, w):
    cfg.work_dir.mkdir(parents=True)
    job = Job(replace(cfg, "refresh", kill_grace_s=0.5), w, evidence_through(None), utcnow)
    started = time.monotonic()
    result = run_in_child(_sleep_with_a_grandchild, job, utcnow() + timedelta(seconds=3))
    assert result == Killed() and time.monotonic() - started < 15
    pids = [int(p) for p in (cfg.work_dir / "pids").read_text().split()]
    deadline = time.monotonic() + 5
    while not all(_gone(p) for p in pids) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert all(_gone(p) for p in pids)


def test_the_cleanup_step_crosses_into_its_process(cfg, w):
    selection = cfg.work_dir / "full" / "selection"
    for label in ("r0", "r1", "r9"):
        atomic_write_json(selection / f"run={label}" / "_MANIFEST.json", {})
    ev = evidence_through(S.SWAP, selection=frozenset({"r0", "r1", "r9"}))
    result = spawn_runner(Job(cfg, w, ev, utcnow), S.CLEANUP, utcnow() + timedelta(minutes=1))
    assert result == Completed({"removed": ["r9"]})
    assert sorted(p.name for p in selection.iterdir()) == ["run=r0", "run=r1"]


def test_the_steps_table_matches_the_cycle():
    assert [s.name for s in STEPS] == list(StepName)
    assert [s.name for s in refresh.FOLLOW] == [S.VERIFY, S.SYNC, S.CLEANUP]
    assert BUILD[-1].name is S.SWAP
    assert [s.name for s in BUILD].index(S.GATES) < [s.name for s in BUILD].index(S.PUBLISH)


def test_the_export_step_waits_for_missing_shards(cfg, w, monkeypatch):
    import isel.pg
    import isel.pull

    opened, calls = [], []

    class Session:
        def close(self):
            opened.remove(self)

    def open_crawlerdb(cfg):
        opened.append(Session())
        return opened[-1]

    def pull_full(cfg, sessions, out_dir, now=None):
        calls.append((len(sessions), now))
        return {"shards_missing": 3 if len(calls) == 1 else 0}

    monkeypatch.setattr(isel.pg, "open_crawlerdb", open_crawlerdb)
    monkeypatch.setattr(isel.pull, "pull_full", pull_full)
    job = Job(cfg, w, evidence_through(S.DIMS), lambda: _at(11, 29, 3))
    act = refresh.STEP[S.EXPORT].act
    assert execute(act, job) == Deferred()
    assert execute(act, job) == Completed({"shards_missing": 0})
    # The cycle's clock names the export; every session is closed again.
    assert calls == [(cfg.full.connections, _at(11, 29, 3))] * 2 and opened == []


@pytest.mark.parametrize("answer, result", [
    ({"swapped": True, "attempts": 1}, Completed({"swapped": True, "attempts": 1})),
    ({"swapped": False, "reason": "lock timeout", "retry": True}, Deferred()),
    ({"swapped": False, "reason": "run has no loaded table", "retry": False},
     Failed("SwapRefused")),
])
def test_the_swap_step_retries_only_what_swap_retries(cfg, w, monkeypatch, answer, result):
    import contextlib

    import isel.publish
    import isel.selectdb

    seen = []
    monkeypatch.setattr(isel.selectdb, "admin", lambda cfg: contextlib.nullcontext("conn"))
    monkeypatch.setattr(isel.publish, "swap", lambda cfg, conn, run_id, now: (
        seen.append((conn, run_id, now())) or answer))
    job = Job(cfg, w, evidence_through(S.PUBLISH), lambda: _at(12, 1, 9))
    assert execute(refresh.STEP[S.SWAP].act, job) == result
    assert seen == [("conn", 2, _at(12, 1, 9))]
