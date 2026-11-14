"""Whole refresh cycles against a throwaway `postgres:16` selectdb.

Golden, dims and the export are stubbed with `tests/synth.py` data stamped by an
injected clock; every other step is the real act, run in this process. Run with
`uv run pytest -m integration`.
"""

# The selectdb fixtures come from test_integration_pg; parameters reuse their names.
# ruff: noqa: F811

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from conftest import replace, small_cfg
from synth import crawl_rows, make_world, write_export
from test_integration_pg import (  # noqa: F401
    _admin,
    _selection,
    _setup,
    cfg,
    dsn,
    selectdb,
)

from isel.golden import golden_table, summarize
from isel.io import atomic_write_json, write_parquet_atomic
from isel.publish import load_selection, rollback, swap
from isel.recrawl import pg_doc_id, sync
from isel.refresh import (
    STEP,
    Idle,
    Killed,
    Status,
    StepName,
    execute,
    read_outcome,
    tick,
)

pytestmark = pytest.mark.integration

G = date(2026, 12, 1)
START = datetime(2026, 11, 29, 3, 0, tzinfo=UTC)
OPEN = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
S = StepName


class Clock:
    def __init__(self, at: datetime):
        self.at = at

    def __call__(self) -> datetime:
        return self.at


class Crash(Exception):
    """The refresh process dies: nothing after the act is recorded."""


@pytest.fixture(scope="module")
def world():
    return make_world()


def _touch(path, at):
    os.utime(path, (at.timestamp(), at.timestamp()))


def _stubs(world, clock):
    def golden(job):
        work = job.cfg.work_dir
        table = golden_table(world.golden_rows)
        write_parquet_atomic(table, work / "golden" / "golden.parquet")
        atomic_write_json(work / "golden" / "summary.json", summarize(table, job.cfg.golden.tags))
        _touch(work / "golden" / "summary.json", clock())

    def dims(job):
        path = job.cfg.work_dir / "dims" / "split_hosts.parquet"
        write_parquet_atomic(pa.table({"host": pa.array([], pa.string()), "migrated_at": pa.array(
            [], pa.timestamp("us", tz="UTC"))}), path)
        _touch(path, clock())

    def export(job):
        write_export(world, job.cfg.work_dir, started_at=clock())
        return {"shards_missing": 0}

    return {S.GOLDEN: golden, S.DIMS: dims, S.EXPORT: export}


def _runner(world, clock, crash_after=None, before=None):
    """Runs each step in this process; `before[step]` runs once before the act.

    `before` is shared with the caller: an entry is gone once it has run.
    """
    stubs, before = _stubs(world, clock), before if before is not None else {}

    def run(job, step, deadline):
        if clock() >= deadline:
            return Killed()
        if step in before:
            before.pop(step)(job)
        result = execute(stubs.get(step, STEP[step].act), job)
        if step is crash_after:
            raise Crash(step)
        return result

    return run


def _until_finished(cfg, clock, runner, limit=40):
    for _ in range(limit):
        if read_outcome(cfg, G).status is not Status.RUNNING:
            return read_outcome(cfg, G)
        result = tick(cfg, clock, runner)
        assert not isinstance(result.decision, Idle), result
        clock.at += timedelta(minutes=1)
    pytest.fail(f"the cycle did not finish: {read_outcome(cfg, G)}")


def _cycle_cfg(tmp_path, **refresh):
    cfg = replace(small_cfg(tmp_path), "full", sample_percent=100.0)
    # The synthetic world's lists are not comparable to a production list.
    return replace(cfg, "refresh", **{"min_overlap_share": 0.0, "min_coverage_ratio": 0.0,
                                      **refresh})


R0_URLS = 500


def _live_r0(cfg, world, urls=None):
    """The previous cycle's end: r0 is live and the frontier follows it.

    Its export is still on disk, and so is a run that was never published.
    """
    _setup(cfg)
    write_export(world, cfg.work_dir, started_at=START - timedelta(days=15))
    _selection(cfg, cfg.work_dir, "s1", [p["url"] for p in crawl_rows(world)[:10]])
    urls = urls or [p["url"] for p in crawl_rows(world)[:R0_URLS]]
    admin = _admin(cfg)
    try:
        loaded = load_selection(cfg, admin, _selection(cfg, cfg.work_dir, "r0", urls),
                                cfg.work_dir)
        assert swap(cfg, admin, loaded["db_run_id"], now=lambda: OPEN)["swapped"]
        sync(cfg, admin, now=lambda: OPEN)
    finally:
        admin.close()
    return urls


def _db(cfg, query, *params):
    admin = _admin(cfg)
    try:
        return admin.execute(query, params).fetchall()
    finally:
        admin.close()


def _assert_refreshed(cfg, r0_urls):
    """The new list is live, r0 is prev, the frontier and the work dir follow."""
    ((live_id, live),) = _db(cfg, "SELECT id, run_label FROM model_runs WHERE status = 'live'")
    assert live != "r0"
    assert _db(cfg, "SELECT DISTINCT m.run_label FROM selected_urls_prev p "
                    "JOIN model_runs m ON m.id = p.run_id") == [("r0",)]
    assert _db(cfg, "SELECT DISTINCT model_run_id FROM recrawl.doc "
                    "WHERE rank IS NOT NULL") == [(live_id,)]
    listed = {u for (u,) in _db(cfg, "SELECT url FROM selected_urls_current")}
    off = next(u for u in r0_urls if u not in listed)
    assert _db(cfg, "SELECT rank FROM recrawl.doc WHERE doc_id = %s", pg_doc_id(off)) == [
        (None,)]
    selection = cfg.work_dir / "full" / "selection"
    assert sorted(p.name for p in selection.iterdir()) == sorted([f"run={live}", "run=r0"])
    out = read_outcome(cfg, G)
    assert out.synced_run_id == live_id and out.verify.run_label == live
    return out


def test_a_cycle_makes_a_new_list_live_and_the_frontier_follows(selectdb, tmp_path, world):
    cfg = _cycle_cfg(tmp_path)
    r0_urls = _live_r0(cfg, world)
    clock = Clock(START)
    out = _until_finished(cfg, clock, _runner(world, clock))
    assert out.status is Status.DONE and out.failures == {}
    out = _assert_refreshed(cfg, r0_urls)
    # publish loaded exactly the manifest the gate measured.
    assert out.verify.batch == out.gates.batch
    assert out.verify.rates == {tag: c.new for tag, c in out.gates.coverage.items()}
    assert out.gates.passed and out.gates.live_label == "r0"


@pytest.mark.parametrize("step", list(StepName))
def test_a_crash_after_any_step_still_ends_done(selectdb, tmp_path, world, step):
    cfg = _cycle_cfg(tmp_path)
    r0_urls = _live_r0(cfg, world)
    clock = Clock(START)
    with pytest.raises(Crash):
        _until_finished(cfg, clock, _runner(world, clock, crash_after=step))
    out = _until_finished(cfg, clock, _runner(world, clock))
    # No step failed: derive never met its own meta, publish never met run_label UNIQUE.
    assert out.status is Status.DONE and out.failures == {}, out
    _assert_refreshed(cfg, r0_urls)
    assert len(_db(cfg, "SELECT 1 FROM model_runs")) == 2


def test_leftovers_of_killed_steps_do_not_stop_the_cycle(selectdb, tmp_path, world):
    cfg = _cycle_cfg(tmp_path)
    r0_urls = _live_r0(cfg, world)
    work = cfg.work_dir

    def killed_in(step, leftover):
        def plant(job):
            leftover(job)
            raise Crash(step)
        return plant

    def stray_parquet(rel):
        return lambda job: write_parquet_atomic(pa.table({"x": [1]}), work / rel)

    before = {
        S.SAMPLE: killed_in(S.SAMPLE, stray_parquet("sample/shard=000.parquet")),
        S.POSITIVES: killed_in(S.POSITIVES, stray_parquet("positives/batch=1.parquet")),
        S.SCORED: killed_in(S.SCORED, lambda job: atomic_write_json(
            work / "full" / "scored" / "_meta.json", {"model_dir": "/elsewhere", "k": 1})),
        S.SELECTION: killed_in(S.SELECTION, stray_parquet(
            "full/selection/run=20261129T000000000000Z/manifest/index_shard=00.parquet")),
    }
    clock = Clock(START)
    killed = []
    while before:
        with pytest.raises(Crash) as crash:
            _until_finished(cfg, clock, _runner(world, clock, before=before))
        killed.append(crash.value.args[0])
    assert killed == [S.SAMPLE, S.POSITIVES, S.SCORED, S.SELECTION]
    out = _until_finished(cfg, clock, _runner(world, clock))
    assert out.status is Status.DONE and out.failures == {}, out
    _assert_refreshed(cfg, r0_urls)


def test_a_rejected_gate_leaves_selectdb_untouched(selectdb, tmp_path, world):
    cfg = _cycle_cfg(tmp_path, min_overlap_share=1.0)
    _live_r0(cfg, world, urls=[f"https://elsewhere{i}.example/" for i in range(R0_URLS)])
    clock = Clock(START)
    out = _until_finished(cfg, clock, _runner(world, clock))
    assert out.status is Status.SKIPPED_GATE and out.pending is S.GATES
    assert out.gates.overlap_share == 0.0
    assert _db(cfg, "SELECT run_label, status FROM model_runs") == [("r0", "live")]


def test_a_crash_after_the_swap_still_syncs_after_swap_by(selectdb, tmp_path, world):
    cfg = _cycle_cfg(tmp_path)
    r0_urls = _live_r0(cfg, world)
    clock = Clock(START)
    with pytest.raises(Crash):
        _until_finished(cfg, clock, _runner(world, clock, crash_after=S.SWAP))
    runner = _runner(world, clock)
    clock.at = datetime(2026, 12, 1, 11, 10, tzinfo=UTC)       # past swap_by (11:00)
    assert tick(cfg, clock, runner).decision.step is S.VERIFY
    assert isinstance(tick(cfg, clock, runner).decision, Idle)  # the sync waits for 20:00
    clock.at = datetime(2026, 12, 1, 20, 5, tzinfo=UTC)
    out = _until_finished(cfg, clock, runner)
    assert out.status is Status.DONE
    _assert_refreshed(cfg, r0_urls)


def test_a_rollback_withdraws_the_cycle(selectdb, tmp_path, world):
    cfg = _cycle_cfg(tmp_path)
    _live_r0(cfg, world)
    clock = Clock(START)
    with pytest.raises(Crash):
        _until_finished(cfg, clock, _runner(world, clock, crash_after=S.SWAP))
    admin = _admin(cfg)
    try:
        rollback(cfg, admin)
    finally:
        admin.close()
    out = _until_finished(cfg, clock, _runner(world, clock))
    assert out.status is Status.WITHDRAWN
    assert _db(cfg, "SELECT run_label FROM model_runs WHERE status = 'live'") == [("r0",)]
