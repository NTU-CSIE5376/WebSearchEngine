"""Daemon state machine and catalog-only CLI commands against local databases."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pyarrow.parquet as pq
import pytest
from fixture_db import add_batch, drop_batch
from helpers import EXAMPLE
from test_e2e import _cli

from idxsel import guards as guards_mod
from idxsel import pipeline
from idxsel.config import load_config
from idxsel.export import NeedFullRefresh
from idxsel.guards import Finding, GuardAbort
from idxsel.manifest import WorkDirBusy, atomic_write_json, load_state, read_json, work_lock
from idxsel.schedule import Action, Decision
from idxsel.throttle import Verdict

pytestmark = pytest.mark.integration
TARGET = datetime(2026, 10, 16, 12, tzinfo=UTC)


def _runtime(overlay, extra=""):
    sched = overlay.parent / "schedule.toml"
    sched.write_text("[schedule]\nenabled = true\ntick_s = 0\nblackout_daily_utc = []\n"
                     "blackout_measure_day_utc = []\nswap_freeze_measure_day_utc = []\n" + extra)
    return pipeline.Runtime(load_config([EXAMPLE, overlay, sched]))


def _decide(monkeypatch, *actions):
    decisions = iter([Decision(a, "test", TARGET) for a in actions])
    monkeypatch.setattr(pipeline, "plan_next_action", lambda *a, **k: next(decisions))


def _events(rt):
    return (rt.layout.root / "logs" / "events.jsonl").read_text()


def _live(servers):
    with psycopg.connect(servers["env"]["IDXSEL_TEST_SELECTDB_ADMIN_DSN"]) as conn:
        live = conn.execute("SELECT id FROM model_runs WHERE status = 'live'").fetchone()
        (rows,) = conn.execute("SELECT count(*) FROM selected_urls_current").fetchone()
        (runs,) = conn.execute("SELECT count(*) FROM model_runs").fetchone()
    return (live[0] if live else None), rows, runs


def _boom(*args, **kwargs):
    raise psycopg.OperationalError("simulated outage")


def test_export_dry_run_reads_catalog_only(overlay):
    plan = _cli(overlay, "export", "--mode", "full", "--dry-run")
    assert plan["shards"] == 4 and plan["heap_bytes"] > 0
    assert not list((overlay.parent / "work" / "exports").glob("base_*"))
    assert _cli(overlay, "probe")["verdict"] == "OK"
    assert _cli(overlay, "preflight") == {"preflight": "ok"}


def test_daemon_runs_full_export_then_build(overlay, servers, fresh_selectdb, monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    _decide(monkeypatch, Action.EXPORT_FULL, Action.BUILD, Action.WAIT)
    pipeline.daemon(rt, max_ticks=2)
    sched_state = read_json(rt.layout.schedule_file)
    assert sched_state["full_done_for"] == TARGET.isoformat()
    assert sched_state["built_for"] == TARGET.isoformat()
    assert not sched_state.get("pending_swap")
    live, rows, _ = _live(servers)
    assert rows == rt.cfg.run.k
    assert _events(rt).count('"daemon.tick"') == 2
    assert _events(rt).count('"verify_live.done"') == 1
    assert load_state(rt.layout)["cycles_since_full"] == 1
    # gc ran inside the build right after features: base and catch-up are gone already.
    assert [p.name.split("_")[0] for p in rt.layout.exports.iterdir()] == ["dims"]
    assert not list(rt.layout.root.glob("runs/*/load.csv"))
    assert not (rt.layout.duckdb_tmp / "load.csv").exists()

    # A pending swap of a run that is already live is cleared and verified again.
    pending = {"db_run_id": live, "run_id": rt.pointers()["run"]}
    atomic_write_json(rt.layout.schedule_file, {**sched_state, "pending_swap": pending})
    pipeline.daemon(rt, max_ticks=1)
    assert read_json(rt.layout.schedule_file)["pending_swap"] is None
    assert _events(rt).count('"verify_live.done"') == 2
    assert '"daemon.error"' not in _events(rt)


def test_daemon_skips_busy_ticks_and_refuses_a_second_daemon(overlay, servers):
    rt = _runtime(overlay)
    with work_lock(rt.layout.lock_file, "a manual export"):
        pipeline.daemon(rt, max_ticks=1)
    assert '"daemon.busy"' in _events(rt) and '"daemon.tick"' not in _events(rt)
    with work_lock(rt.layout.daemon_lock_file, "the first daemon"), \
            pytest.raises(WorkDirBusy, match="the first daemon"):
        pipeline.daemon(rt, max_ticks=1)


def test_build_reads_no_crawlerdb_and_survives_a_metricdb_outage(overlay, servers,
                                                                  fresh_selectdb, monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    _decide(monkeypatch, Action.EXPORT_FULL, Action.BUILD, Action.BUILD)
    pipeline.daemon(rt, max_ticks=1)
    monkeypatch.setattr(pipeline, "stage_export", _boom)  # crawlerdb is unreachable
    pipeline.daemon(rt, max_ticks=1)
    assert read_json(rt.layout.schedule_file)["built_for"] == TARGET.isoformat()
    first, rows, _ = _live(servers)
    assert rows == rt.cfg.run.k
    assert '"daemon.error"' not in _events(rt)

    # metricdb is down too: the next build reuses the previous golden set.
    monkeypatch.setattr(pipeline, "stage_golden", _boom)
    pipeline.daemon(rt, max_ticks=1)
    second, rows, runs = _live(servers)
    assert second != first and rows == rt.cfg.run.k and runs == 2
    assert '"daemon.golden_fallback"' in _events(rt)
    assert load_state(rt.layout)["cycles_since_full"] == 2


def test_paused_daily_delta_stops_at_its_deadline_and_the_build_runs(
        overlay, servers, fresh_selectdb, monkeypatch):
    rt = _runtime(overlay, "delta_max_h = 0.0003\n")  # about one second
    _cli(overlay, "init-selectdb")
    _decide(monkeypatch, Action.EXPORT_FULL, Action.EXPORT_DELTA, Action.BUILD)
    pipeline.daemon(rt, max_ticks=1)
    # From now on every runtime check says PAUSE, e.g. anti-wraparound vacuums all day.
    monkeypatch.setattr(guards_mod.Guard, "check",
                        lambda self: Finding(Verdict.PAUSE, "2 anti-wraparound vacuums running"))
    pipeline.daemon(rt, max_ticks=1)
    assert "deadline passed" in _events(rt) and '"daemon.deferred"' in _events(rt)
    assert "delta" in load_state(rt.layout)["in_progress"]  # resumes on the next delta
    pipeline.daemon(rt, max_ticks=1)
    assert read_json(rt.layout.schedule_file)["built_for"] == TARGET.isoformat()
    assert _live(servers)[1] == rt.cfg.run.k


def test_failures_after_the_load_never_reload(overlay, servers, fresh_selectdb, monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    monkeypatch.setattr(pipeline, "stage_verify_live", _boom)
    monkeypatch.setattr(pipeline, "stage_eval", _boom)
    _decide(monkeypatch, Action.EXPORT_FULL, Action.BUILD, Action.WAIT)
    pipeline.daemon(rt, max_ticks=3)
    sched = read_json(rt.layout.schedule_file)
    assert sched["built_for"] == TARGET.isoformat() and sched["pending_swap"] is None
    events = _events(rt)
    assert '"verify_live.failed"' in events and '"eval.failed"' in events
    assert '"daemon.error"' not in events
    live, rows, runs = _live(servers)
    assert live is not None and rows == rt.cfg.run.k and runs == 1
    assert load_state(rt.layout)["cycles_since_full"] == 1


def test_frozen_swap_waits_then_swaps_and_verifies(overlay, servers, fresh_selectdb,
                                                   monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    frozen = [True]
    monkeypatch.setattr(pipeline, "swap_allowed", lambda now, cfg: not frozen[0])
    _decide(monkeypatch, Action.EXPORT_FULL, Action.BUILD, Action.WAIT)
    pipeline.daemon(rt, max_ticks=2)
    sched = read_json(rt.layout.schedule_file)
    pending = sched["pending_swap"]
    assert pending["run_id"] == rt.pointers()["run"]
    assert _live(servers)[0] is None
    # Some later error put the daemon into cooldown; the ready list still goes live.
    later = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    atomic_write_json(rt.layout.schedule_file, {**sched, "cooldown_until": later})
    frozen[0] = False
    pipeline.daemon(rt, max_ticks=1)
    assert read_json(rt.layout.schedule_file)["pending_swap"] is None
    assert _live(servers)[0] == pending["db_run_id"]
    assert '"verify_live.done"' in _events(rt) and '"daemon.cooldown"' in _events(rt)


def test_failed_select_leaves_no_scored_dir(overlay, servers, monkeypatch):
    rt = _runtime(overlay)
    pipeline.stage_export(rt, "full")
    pipeline.stage_golden(rt)
    pipeline.stage_features(rt)
    monkeypatch.setattr(pipeline, "select_top_k", _boom)
    with pytest.raises(psycopg.OperationalError):
        pipeline.stage_select(rt)
    (run_dir,) = list((rt.layout.root / "runs").iterdir())
    assert not (run_dir / "scored").exists()
    pipeline.gc_storage(rt)  # an unfinished run directory is garbage
    assert not run_dir.exists()


def test_daemon_error_enters_cooldown_instead_of_crashing(overlay, servers, fresh_selectdb,
                                                          monkeypatch):
    rt = _runtime(overlay, "error_cooldown_s = 3600\n")
    _cli(overlay, "init-selectdb")
    pipeline.stage_export(rt, "full")
    monkeypatch.setattr(pipeline, "stage_select", _boom)
    _decide(monkeypatch, Action.BUILD)
    pipeline.daemon(rt, max_ticks=2)
    sched = read_json(rt.layout.schedule_file)
    assert datetime.fromisoformat(sched["cooldown_until"]) > datetime.now(UTC)
    events = _events(rt)
    assert events.count('"daemon.error"') == 1
    assert events.count('"daemon.cooldown"') == 1


def test_build_without_room_is_refused_before_any_work(overlay, servers, fresh_selectdb,
                                                       monkeypatch):
    rt = _runtime(overlay, "[guard]\nbuild_reserve_gb = 1000000000.0\n")
    _cli(overlay, "init-selectdb")
    pipeline.stage_export(rt, "full")  # exports do not count the build's reserve
    _decide(monkeypatch, Action.BUILD)
    pipeline.daemon(rt, max_ticks=1)
    events = _events(rt)
    assert '"daemon.deferred"' in events and '"action": "build"' in events
    assert "reserve" in events and '"daemon.error"' not in events
    sched = read_json(rt.layout.schedule_file)
    assert "cooldown_until" not in sched and "built_for" not in sched
    for sub in ("golden", "features", "runs"):
        assert not (rt.layout.root / sub).exists()
    assert _live(servers) == (None, 0, 0)


def test_lost_compacted_shard_asks_for_a_full_export(overlay, servers, fresh_selectdb,
                                                     monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    _decide(monkeypatch, Action.EXPORT_FULL, Action.BUILD)
    pipeline.daemon(rt, max_ticks=2)
    compacted = rt.layout.features_dir(load_state(rt.layout)["compacted"]["features"])
    (compacted / "shard=001" / "part.parquet").unlink()
    seen = []

    def plan(now, sched, cfg, full_refresh_due, has_base):
        seen.append(full_refresh_due)
        return Decision(Action.BUILD, "test", TARGET + timedelta(days=15))

    monkeypatch.setattr(pipeline, "plan_next_action", plan)
    before = sorted(p.name for p in rt.layout.root.glob("*/*"))
    pipeline.daemon(rt, max_ticks=1)
    assert seen == [True]  # the next full-export window will rebuild it
    assert "lost shards [1]" in _events(rt) and '"daemon.need_full"' in _events(rt)
    assert load_state(rt.layout)["force_full"] is True
    assert "cooldown_until" not in read_json(rt.layout.schedule_file)
    assert sorted(p.name for p in rt.layout.root.glob("*/*")) == before  # no delta, no golden
    # Any completed full export clears the request, also a manual one.
    pipeline.stage_export(rt, "full")
    state = load_state(rt.layout)
    assert "force_full" not in state and "compacted" not in state


def test_build_fails_fast_when_selectdb_is_down(overlay, servers, monkeypatch):
    rt = _runtime(overlay)
    pipeline.stage_export(rt, "full")
    monkeypatch.setattr(rt, "admin_conn", _boom)
    _decide(monkeypatch, Action.BUILD)
    pipeline.daemon(rt, max_ticks=1)
    assert '"daemon.error"' in _events(rt) and "simulated outage" in _events(rt)
    assert not (rt.layout.root / "features").exists()  # no hour of work before failing


def test_full_export_progress_survives_a_delta(overlay, servers):
    rt = _runtime(overlay)
    first = pipeline.stage_export(rt, "full")
    with pytest.raises(GuardAbort, match="deadline"):
        pipeline.stage_export(rt, "full", deadline=datetime.now(UTC) - timedelta(minutes=1))
    pending = load_state(rt.layout)["in_progress"]["full"]["id"]
    assert pending != first["export_id"]

    delta = pipeline.stage_export(rt, "delta")
    state = load_state(rt.layout)
    assert state["in_progress"] == {"full": state["in_progress"]["full"]}
    assert state["in_progress"]["full"]["id"] == pending
    assert state["deltas"][-1]["id"] == delta["export_id"]
    # A build compacts the old base and the deltas while the new full is pending.
    pipeline.stage_features(rt)
    pipeline.gc_storage(rt)
    assert not rt.layout.base_dir(first["export_id"]).exists()
    assert not rt.layout.delta_dir(delta["export_id"]).exists()
    assert rt.layout.base_dir(pending).exists()

    resumed = pipeline.stage_export(rt, "full")
    assert resumed["export_id"] == pending
    state = load_state(rt.layout)
    assert state["in_progress"] == {} and "compacted" not in state
    assert state["base"]["id"] == pending


def test_full_export_past_history_retention_starts_over(overlay, servers, monkeypatch):
    rt = _runtime(overlay)

    def pruned(*args, **kwargs):
        raise NeedFullRefresh("shard 0: history starts above the base watermark")

    monkeypatch.setattr(pipeline, "export_delta", pruned)
    with pytest.raises(NeedFullRefresh):
        pipeline.stage_export(rt, "full")
    (stale,) = list(rt.layout.exports.glob("base_*"))
    assert load_state(rt.layout)["in_progress"] == {}
    pipeline.gc_storage(rt)
    assert not stale.exists()


def _measured(monkeypatch, at):
    """The next tick plans a forward evaluation of the measurement at `at`."""
    monkeypatch.setattr(pipeline, "plan_next_action",
                        lambda *a, **k: Decision(Action.FORWARD_EVAL, "test", at))


def _built(rt, monkeypatch):
    _decide(monkeypatch, Action.EXPORT_FULL, Action.BUILD)
    pipeline.daemon(rt, max_ticks=2)
    return datetime.now(UTC)


def test_daemon_forward_eval_records_no_new_batch_then_labels(overlay, servers, fresh_selectdb,
                                                              monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    first = _built(rt, monkeypatch)
    _measured(monkeypatch, first)
    pipeline.daemon(rt, max_ticks=1)
    assert '"forward_eval.no_new_batch"' in _events(rt)  # the metric reused its batch
    assert read_json(rt.layout.schedule_file)["forward_eval_for"] == first.isoformat()
    assert not rt.layout.labels.exists()

    live = pq.read_table(rt.layout.run_dir(rt.pointers()["run"]) / "selected.parquet")[
        "url"].to_pylist()
    metric_dsn = servers["crawler"].dsn(user="metric", password="metric", db="metricdb")
    second = datetime.now(UTC)
    add_batch(metric_dsn, 4, second, [
        (f"kw4-{q}", ["head", "random"], [*sorted(live)[2 * q:2 * q + 2],
                                          f"https://never.example/{q}"]) for q in range(5)])
    runs_before = sorted(p.name for p in (rt.layout.root / "runs").iterdir())
    try:
        # The evaluation needs neither crawlerdb nor selectdb admin, and never re-selects.
        monkeypatch.setattr(pipeline, "stage_export", _boom)
        monkeypatch.setattr(pipeline, "select_into", _boom)
        _measured(monkeypatch, datetime.now(UTC))
        pipeline.daemon(rt, max_ticks=1)
    finally:
        drop_batch(metric_dsn, 4)
    events = _events(rt)
    assert '"forward_eval.done"' in events and '"daemon.error"' not in events
    meta = read_json(rt.layout.labels / "4" / "meta.json")
    assert meta["forward"] is True and meta["in_sample"] is False
    (done,) = list((rt.layout.root / "forward_evals").iterdir())
    summary = read_json(done / "forward_eval.json")
    assert summary["batches"] == [4] and summary["labels"] == {"4": "written"}
    assert summary["eval"]["batch4_head"]["indexed_exact_rate"] == pytest.approx(10 / 15)
    assert sorted(p.name for p in (rt.layout.root / "runs").iterdir()) == runs_before


def test_forward_eval_without_live_run_is_recorded(overlay, servers, fresh_selectdb,
                                                   monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    at = datetime.now(UTC)
    _measured(monkeypatch, at)
    pipeline.daemon(rt, max_ticks=1)
    assert '"forward_eval.no_live_run"' in _events(rt)
    assert read_json(rt.layout.schedule_file)["forward_eval_for"] == at.isoformat()


def test_forward_eval_skips_when_live_changed_after_measurement(overlay, servers,
                                                                fresh_selectdb, monkeypatch):
    rt = _runtime(overlay)
    _cli(overlay, "init-selectdb")
    before = datetime.now(UTC) - timedelta(minutes=5)  # the list went live after this
    _built(rt, monkeypatch)
    _measured(monkeypatch, before)
    pipeline.daemon(rt, max_ticks=1)
    assert '"forward_eval.ambiguous_live"' in _events(rt)
    assert not rt.layout.labels.exists()


def test_forward_eval_failure_enters_cooldown_and_retries(overlay, servers, fresh_selectdb,
                                                          monkeypatch):
    rt = _runtime(overlay, "error_cooldown_s = 3600\n")
    _cli(overlay, "init-selectdb")
    at = _built(rt, monkeypatch)
    monkeypatch.setattr(pipeline, "golden_into", _boom)  # metricdb is down
    _measured(monkeypatch, at)
    pipeline.daemon(rt, max_ticks=1)
    sched = read_json(rt.layout.schedule_file)
    assert "forward_eval_for" not in sched and "cooldown_until" in sched
    assert '"daemon.error"' in _events(rt)
