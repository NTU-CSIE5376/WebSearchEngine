"""Stage orchestration: export -> golden -> features -> select -> load -> swap -> eval.

The core functions (`golden_into`, `select_into`, `eval_run`) take explicit
paths. Production stages wrap them and publish their outputs in
`pointers.json`; a backtest calls them on its own directory and leaves the
production state alone. One process at a time changes the work directory: the
CLI and the daemon take `manifest.work_lock` at their entry points.
"""

from __future__ import annotations

import dataclasses
import os
import shutil
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import psycopg

import idxsel
from idxsel.config import Config
from idxsel.duck import open_duckdb
from idxsel.evaluate import evaluate, verify_live
from idxsel.export import (
    ExportContext,
    NeedFullRefresh,
    export_base,
    export_delta,
    export_dims,
    plan_base,
)
from idxsel.features import PriorKind, build_features
from idxsel.golden import build_golden
from idxsel.guards import DbProbe, Guard, GuardAbort, LocalProbe, PreflightFailed, RemoteProbe
from idxsel.load import ensure_schema, load_selection, swap, update_eval_metrics
from idxsel.log import EventLog
from idxsel.manifest import (
    Layout,
    WorkDirBusy,
    atomic_write_json,
    load_state,
    read_json,
    save_state,
    utc_id,
    work_lock,
)
from idxsel.pg import ReadOnlySession, SessionHolder, connect, require_env
from idxsel.schedule import Action, next_measurement, plan_next_action, swap_allowed
from idxsel.scoring import build_scored
from idxsel.select_topk import select_top_k
from idxsel.throttle import Pacer


class Runtime:
    """Holds config, paths and connection factories for one process."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self.layout = Layout(cfg.run.work_dir)
        self.events = EventLog(self.layout.root / "logs" / "events.jsonl")

    def app(self, purpose: str) -> str:
        return f"{self.cfg.run.app_name_prefix}_{purpose}"

    def crawler_session(self, purpose: str) -> ReadOnlySession:
        c = self.cfg.crawlerdb
        return ReadOnlySession(connect(require_env(c.dsn_env), app_name=self.app(purpose),
                                       session=c.session, connect_timeout_s=c.connect_timeout_s))

    def metric_session(self) -> ReadOnlySession:
        m = self.cfg.metricdb
        return ReadOnlySession(connect(require_env(m.dsn_env), app_name=self.app("golden"),
                                       session=m.session, connect_timeout_s=m.connect_timeout_s))

    def reader_session(self) -> ReadOnlySession:
        s = self.cfg.selectdb
        return ReadOnlySession(connect(
            require_env(s.reader_dsn_env), app_name=self.app("verify"),
            session={"default_transaction_read_only": "on"},
            connect_timeout_s=s.connect_timeout_s))

    def admin_conn(self) -> psycopg.Connection:
        s = self.cfg.selectdb
        return connect(require_env(s.admin_dsn_env), app_name=self.app("load"), session={},
                       connect_timeout_s=s.connect_timeout_s)

    def duck(self, spill: bool = True) -> duckdb.DuckDBPyConnection:
        """Opens DuckDB; only the work-lock holder may spill into the shared temp dir."""
        return open_duckdb(self.cfg, self.layout.duckdb_tmp if spill else None)

    def guard(self, db: SessionHolder | None) -> Guard:
        probe = DbProbe(db, self.cfg) if db is not None else None
        return Guard(self.cfg, probe, LocalProbe(self.cfg), RemoteProbe(self.cfg), self.events)

    def pointers(self) -> dict[str, Any]:
        return read_json(self.layout.root / "state" / "pointers.json", {})

    def set_pointer(self, **kwargs: Any) -> None:
        ptrs = {**self.pointers(), **kwargs}
        atomic_write_json(self.layout.root / "state" / "pointers.json", ptrs)


def parse_shards(spec: str | None, num_shards: int) -> list[int]:
    """Parses `0-3,7,10-12`; None means all shards."""
    if not spec:
        return list(range(num_shards))
    out: set[int] = set()
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        out.update(range(int(lo), int(hi or lo) + 1))
    bad = [s for s in out if not 0 <= s < num_shards]
    if bad:
        raise ValueError(f"shards out of range: {sorted(bad)}")
    return sorted(out)


def _best_effort(rt: Runtime, name: str, fn: Callable[[], Any]) -> Any:
    """Runs a step whose failure must not undo work already delivered."""
    try:
        return fn()
    except Exception as exc:
        error = f"{type(exc).__name__}: {str(exc)[:500]}"
        rt.events.emit(f"{name}.failed", error=error)
        return {"error": error}


def _latest_as_of(state: dict[str, Any]) -> datetime:
    stamps = [state["base"]["as_of"]] + [d["as_of"] for d in state["deltas"]]
    if state.get("compacted"):
        stamps.append(state["compacted"]["as_of"])
    return max(datetime.fromisoformat(str(s)) for s in stamps)


def stage_export(rt: Runtime, mode: str, shards: Sequence[int] | None = None,
                 dry_run: bool = False, deadline: datetime | None = None) -> dict[str, Any]:
    """Runs a full (base + dims + catch-up delta) or delta export.

    Each kind keeps its own resume slot in `state.in_progress`, so a daily delta
    never discards an unfinished full export.
    """
    cfg, layout = rt.cfg, rt.layout
    state = load_state(layout)
    pending = state.setdefault("in_progress", {})
    guard_holder = SessionHolder(lambda: rt.crawler_session("guard"))
    try:
        guard = rt.guard(guard_holder)
        ctx = ExportContext(cfg, layout, rt.events, guard, Pacer(cfg.throttle),
                           rt.crawler_session, deadline=deadline)
        wanted = list(shards) if shards is not None else state.get("shards") or list(
            range(cfg.crawlerdb.num_shards))
        if dry_run:
            return plan_base(ctx, wanted)
        guard.preflight(f"export_{mode}")
        if mode == "full":
            job = pending.get("full") or {"id": utc_id(), "shards": wanted}
            pending["full"] = job
            save_state(layout, state)
            export_id = job["id"]
            base = export_base(ctx, export_id, job["shards"])
            export_dims(ctx, export_id)
            try:
                catch_up = export_delta(ctx, f"{export_id}c", job["shards"], base["watermarks"])
            except NeedFullRefresh:
                # History was pruned past this base's watermarks while the job was
                # deferred: it can never be caught up, so the next full starts over.
                pending.pop("full")
                save_state(layout, state)
                raise
            state = {"base": {"id": export_id, "as_of": base["finished_at"]},
                     "dims": export_id, "shards": job["shards"],
                     "deltas": [{"id": f"{export_id}c", "as_of": catch_up["finished_at"]}],
                     "watermarks": catch_up["watermarks"], "cycles_since_full": 0,
                     "in_progress": {}}
            save_state(layout, state)
            rt.events.emit("export.full_done", export_id=export_id, rows=base["rows"],
                           catch_up_rows=catch_up["rows"])
            return {"mode": "full", "export_id": export_id, "rows": base["rows"],
                    "catch_up_rows": catch_up["rows"]}
        if not state.get("base"):
            raise NeedFullRefresh("no base export yet; run a full export first")
        job = pending.get("delta") or {"id": utc_id()}
        pending["delta"] = job
        save_state(layout, state)
        delta = export_delta(ctx, job["id"], state["shards"], state["watermarks"])
        state["deltas"].append({"id": job["id"], "as_of": delta["finished_at"]})
        state["watermarks"].update(delta["watermarks"])
        pending.pop("delta")
        save_state(layout, state)
        return {"mode": "delta", "export_id": job["id"], "rows": delta["rows"]}
    finally:
        guard_holder.close()


def golden_into(rt: Runtime, out: Path, as_of: datetime) -> dict[str, Any]:
    """Reads the golden set from metricdb into `out`; returns its meta."""
    state = load_state(rt.layout)
    if not state.get("dims"):
        raise RuntimeError("no dims export yet; run a full export first")
    metric = rt.metric_session()
    con = rt.duck()
    try:
        return build_golden(rt.cfg, metric, rt.layout.dims_dir(state["dims"]), out, con, as_of)
    finally:
        metric.close()
        con.close()


def stage_golden(rt: Runtime) -> dict[str, Any]:
    golden_id = utc_id()
    meta = golden_into(rt, rt.layout.golden_dir(golden_id), _latest_as_of(load_state(rt.layout)))
    rt.set_pointer(golden=golden_id)
    rt.events.emit("golden.done", golden_id=golden_id, **{k: meta[k] for k in (
        "rows", "past_golden_urls", "prior_domains", "target_batch")})
    return {"golden_id": golden_id, **meta}


def _merge_inputs(rt: Runtime, state: dict[str, Any]) -> tuple[tuple[PriorKind, Path], list[str]]:
    """Returns the merge prior (compacted feature set, else the base) and new delta ids.

    Raises:
        NeedFullRefresh: If there is no base, or the compacted set lost a file. Its
            inputs are deleted, and a missing shard would silently drop that shard's
            URLs from every later list, so only a full export can rebuild it.
    """
    if not state.get("base"):
        raise NeedFullRefresh("no base export yet; run a full export first")
    deltas = [d["id"] for d in state["deltas"]]
    compacted = state.get("compacted")
    if not compacted:
        return ("base", rt.layout.base_dir(state["base"]["id"])), deltas
    prior = rt.layout.features_dir(compacted["features"])
    done = read_json(prior / "_SUCCESS.json")
    if done is None or not (prior / "meta.json").exists():
        raise NeedFullRefresh(f"compacted feature set {prior.name} is incomplete")
    lost = sorted(int(s) for s in done["per_shard"]
                  if not (prior / f"shard={int(s):03d}" / "part.parquet").exists())
    if lost:
        raise NeedFullRefresh(f"compacted feature set {prior.name} lost shards {lost}")
    return ("merged", prior), deltas


def _full_export_due(rt: Runtime, state: dict[str, Any]) -> bool:
    """True if the next export must be full.

    That is: the periodic refresh, a pending or requested full export, or merge
    inputs only a full export can rebuild. Checking the inputs on every tick
    finds a lost compacted set in time for the next full-export window.
    """
    if (state.get("cycles_since_full", 0) >= rt.cfg.export.full_refresh_every_n_cycles
            or state.get("force_full") or (state.get("in_progress") or {}).get("full")):
        return True
    try:
        _merge_inputs(rt, state)
    except NeedFullRefresh:
        return True
    return False


def _request_full(rt: Runtime, reason: str) -> None:
    """Records that only a full export can help; any completed full export clears it."""
    state = load_state(rt.layout)
    state["force_full"] = True
    save_state(rt.layout, state)
    rt.events.emit("daemon.need_full", reason=reason)


def stage_features(rt: Runtime) -> dict[str, Any]:
    """Merges the prior with the new deltas and makes the result the next prior.

    Merged deltas leave the state (gc deletes them), so storage and merge time
    stay bounded by one cycle of deltas instead of growing until the next full.
    """
    state = load_state(rt.layout)
    (kind, prior), deltas = _merge_inputs(rt, state)
    if kind == "merged" and not deltas:
        rt.set_pointer(features=prior.name)
        rt.events.emit("features.reused", feature_id=prior.name)
        return {"feature_id": prior.name, "rows": read_json(prior / "_SUCCESS.json")["rows"],
                "as_of": state["compacted"]["as_of"], "reused": True}
    feature_id = utc_id()
    out = rt.layout.features_dir(feature_id)
    con = rt.duck()
    try:
        summary = build_features(rt.cfg, con, (kind, prior),
                                 [rt.layout.delta_dir(d) for d in deltas], out, state["shards"])
    finally:
        con.close()
    as_of = _latest_as_of(state)
    atomic_write_json(out / "meta.json", {
        "as_of": as_of, "base": state["base"]["id"], "prior": {"kind": kind, "id": prior.name},
        "deltas": deltas})
    state["compacted"] = {"features": feature_id, "as_of": as_of}
    state["deltas"] = []  # every listed delta is now inside the compacted set
    save_state(rt.layout, state)
    rt.set_pointer(features=feature_id)
    rt.events.emit("features.done", feature_id=feature_id, rows=summary["rows"],
                   merged_deltas=len(deltas))
    return {"feature_id": feature_id, "rows": summary["rows"], "as_of": as_of}


def select_into(rt: Runtime, features_dir: Path, golden_dir: Path, run_dir: Path
                ) -> dict[str, Any]:
    """Scores the feature set and writes exactly K URLs to `run_dir/selected.parquet`."""
    as_of = datetime.fromisoformat(str(read_json(features_dir / "meta.json")["as_of"]))
    con = rt.duck()
    try:
        scored = build_scored(rt.cfg, con, features_dir, golden_dir, run_dir, as_of)
        stats = select_top_k(con, str(run_dir / "scored" / "**" / "*.parquet"), rt.cfg.run.k,
                             rt.cfg.selection.exact_sort_max_rows,
                             rt.cfg.selection.max_dedupe_rounds, run_dir / "selected.parquet")
    finally:
        con.close()
        shutil.rmtree(run_dir / "scored", ignore_errors=True)
    manifest = {"run_id": run_dir.name, "as_of": as_of, "features": features_dir.name,
                "golden": golden_dir.name, "scored": scored, "selection": stats,
                "config_sha256": rt.cfg.sha256()}
    atomic_write_json(run_dir / "manifest.json", manifest)
    return manifest


def stage_select(rt: Runtime, golden_id: str | None = None,
                 features_id: str | None = None) -> dict[str, Any]:
    ptrs = rt.pointers()
    run_id = utc_id()
    manifest = select_into(rt, rt.layout.features_dir(features_id or ptrs["features"]),
                           rt.layout.golden_dir(golden_id or ptrs["golden"]),
                           rt.layout.run_dir(run_id))
    rt.set_pointer(run=run_id)
    stats = manifest["selection"]
    rt.events.emit("select.done", run_id=run_id, k=stats["k"], candidates=stats["candidates"],
                   tier_counts=stats["tier_counts"])
    return manifest


def _run(rt: Runtime, run_id: str | None) -> tuple[Path, dict[str, Any]]:
    run_dir = rt.layout.run_dir(run_id or rt.pointers()["run"])
    return run_dir, read_json(run_dir / "manifest.json")


def stage_load(rt: Runtime, run_id: str | None = None) -> dict[str, Any]:
    run_dir, manifest = _run(rt, run_id)
    lineage = read_json(rt.layout.features_dir(manifest["features"]) / "meta.json", {})
    profile = rt.cfg.active_profile
    meta = {
        "run_label": manifest["run_id"],
        "cutoff_datetime": manifest["as_of"],
        "model_path": f"rule:{rt.cfg.selection.profile}",
        "train_metrics": {"profile": rt.cfg.selection.profile, "tiers": profile.tiers,
                          "weights": dataclasses.asdict(profile.weights)},
        "selection_metrics": {"scored": manifest["scored"], "selection": manifest["selection"]},
        "source_paths": {"features": manifest["features"], "golden": manifest["golden"],
                         **{k: lineage.get(k) for k in ("base", "prior", "deltas")}},
        "config_sha256": manifest["config_sha256"],
        "code_version": idxsel.__version__,
    }
    # Select may have run for hours since the build's preflight: check Host1 again.
    rt.guard(None).preflight("load", reads_crawlerdb=False)
    admin = rt.admin_conn()
    con = rt.duck()
    try:
        ensure_schema(admin, rt.cfg.selectdb.reader_role)
        # The CSV goes to the temp dir, which gc empties, so a killed load leaves nothing.
        db_run_id = load_selection(rt.cfg, admin, con, run_dir / "selected.parquet",
                                   rt.layout.duckdb_tmp, meta, rt.events)
    finally:
        admin.close()
        con.close()
    manifest["db_run_id"] = db_run_id
    atomic_write_json(run_dir / "manifest.json", manifest)
    return {"run_id": manifest["run_id"], "db_run_id": db_run_id}


def stage_swap(rt: Runtime, db_run_id: int | None = None) -> dict[str, Any]:
    if db_run_id is None:
        db_run_id = _run(rt, None)[1]["db_run_id"]
    admin = rt.admin_conn()
    try:
        return {"db_run_id": db_run_id, **swap(rt.cfg, admin, db_run_id, rt.events)}
    finally:
        admin.close()


def eval_run(rt: Runtime, run_dir: Path, features_dir: Path, golden_dir: Path,
             db_run_id: int | None) -> dict[str, Any]:
    """Evaluates a run's own selection; records it on its model_runs row if it was loaded."""
    con = rt.duck()
    try:
        report = evaluate(rt.cfg, con, run_dir / "selected.parquet", golden_dir, features_dir,
                          read_json(golden_dir / "meta.json"), run_dir)
    finally:
        con.close()
    if db_run_id is not None:
        admin = rt.admin_conn()
        try:
            update_eval_metrics(admin, db_run_id, report)
        finally:
            admin.close()
    return report


def stage_eval(rt: Runtime, run_id: str | None = None) -> dict[str, Any]:
    run_dir, manifest = _run(rt, run_id)
    report = eval_run(rt, run_dir, rt.layout.features_dir(manifest["features"]),
                      rt.layout.golden_dir(manifest["golden"]), manifest.get("db_run_id"))
    rt.events.emit("eval.done", run_id=manifest["run_id"], results=report["results"])
    return report


def stage_verify_live(rt: Runtime, run_id: str | None = None) -> dict[str, Any]:
    """Checks, as the metric's role, that a loaded run is live and complete.

    It runs without the work lock, so its DuckDB never spills to the shared temp dir.
    """
    run_dir, manifest = _run(rt, run_id)
    if manifest.get("db_run_id") is None:
        raise RuntimeError(f"run {manifest['run_id']} was never loaded")
    reader = rt.reader_session()
    con = rt.duck(spill=False)
    try:
        result = verify_live(rt.cfg, con, reader, run_dir / "selected.parquet",
                             manifest["db_run_id"])
    finally:
        reader.close()
        con.close()
    rt.events.emit("verify_live.done", run_id=manifest["run_id"], **result)
    return result


def _preflight_build(rt: Runtime) -> None:
    """Refuses a build that would crowd Host2's or Host1's disks or memory.

    A build reads no crawlerdb, so blackouts and the crawlerdb disk do not apply.
    The reserve covers the new feature set and DuckDB spill it writes after this.
    """
    rt.guard(None).preflight("build", reads_crawlerdb=False,
                             reserve_gb=rt.cfg.guard.build_reserve_gb)


def _build_and_load(rt: Runtime, golden_id: str) -> dict[str, Any]:
    """Features -> select -> load; the cycle counts once its list is in selectdb."""
    admin = rt.admin_conn()  # an unreachable selectdb fails now, not after an hour of work
    try:
        ensure_schema(admin, rt.cfg.selectdb.reader_role)
    finally:
        admin.close()
    features = stage_features(rt)
    # The previous compacted set and the merged deltas are garbage now: free them
    # before scoring, so the build never holds two feature sets and the deltas at once.
    _best_effort(rt, "gc", lambda: gc_storage(rt))
    selection = stage_select(rt, golden_id=golden_id, features_id=features["feature_id"])
    loaded = stage_load(rt, selection["run_id"])
    state = load_state(rt.layout)
    state["cycles_since_full"] = state.get("cycles_since_full", 0) + 1
    save_state(rt.layout, state)
    return {"features": features, "selection": selection["selection"], **loaded}


def run_once(rt: Runtime, *, full: bool = False,
             shards: Sequence[int] | None = None) -> dict[str, Any]:
    """Runs one production cycle end to end and makes its list live."""
    need_full = full or _full_export_due(rt, load_state(rt.layout))
    try:
        export = stage_export(rt, "full" if need_full else "delta", shards)
    except NeedFullRefresh as exc:
        rt.events.emit("export.need_full", reason=str(exc))
        export = stage_export(rt, "full", shards)
    _preflight_build(rt)
    golden = stage_golden(rt)
    built = _build_and_load(rt, golden["golden_id"])
    summary: dict[str, Any] = {
        "export": export,
        "golden": {k: golden[k] for k in ("golden_id", "target_batch", "past_golden_urls")},
        **built, "swap": stage_swap(rt, built["db_run_id"])}
    if summary["swap"]["swapped"]:
        summary["verify_live"] = _best_effort(
            rt, "verify_live", lambda: stage_verify_live(rt, built["run_id"]))
    summary["eval"] = _best_effort(rt, "eval", lambda: stage_eval(rt, built["run_id"])["results"])
    summary["gc"] = gc_storage(rt)
    rt.events.emit("cycle.done", run_id=built["run_id"], swapped=summary["swap"]["swapped"])
    return summary


def deadline_before_build(rt: Runtime, what: str) -> datetime | None:
    """Returns when a long manual command must stop to leave the build its window.

    None means no build is pending (or the schedule is off).

    Raises:
        WorkDirBusy: Inside the pre-measurement build window before the build ran.
    """
    s = rt.cfg.schedule
    if not s.enabled:
        return None
    now = datetime.now(UTC)
    target = next_measurement(now, s)
    if read_json(rt.layout.schedule_file, {}).get("built_for") == target.isoformat():
        return None
    start = target - timedelta(hours=s.build_start_h_before_measure)
    if now >= start:
        raise WorkDirBusy(f"the pre-measurement build has not run yet; {what} after it")
    return start


def backtest(rt: Runtime) -> dict[str, Any]:
    """Golden -> select -> eval on the latest feature set for `golden.target_batch`.

    Reads metricdb only. crawlerdb, selectdb, the export state and the pointers
    stay untouched; the outputs go to `backtests/<id>/`.

    Run after a measurement and before the next build, with `target_batch =
    "next"`, it scores the live list on the batch that measurement created: the
    only out-of-sample number (see `evaluate`).

    Raises:
        WorkDirBusy: Inside the pre-measurement build window, which must not wait.
    """
    deadline_before_build(rt, "backtest")
    features_id = rt.pointers().get("features")
    if not features_id:
        raise RuntimeError("no feature set yet; run `features` first")
    features_dir = rt.layout.features_dir(features_id)
    as_of = datetime.fromisoformat(str(read_json(features_dir / "meta.json")["as_of"]))
    root = rt.layout.backtest_dir(utc_id())
    golden = golden_into(rt, root / "golden", as_of)
    manifest = select_into(rt, features_dir, root / "golden", root / "run")
    report = eval_run(rt, root / "run", features_dir, root / "golden", None)
    summary = {"backtest_dir": str(root), "features": features_id,
               "target_batch": golden["target_batch"], "selection": manifest["selection"],
               "eval": report["results"]}
    atomic_write_json(root / "backtest.json", summary)
    rt.events.emit("backtest.done", backtest_dir=str(root), results=report["results"])
    return summary


def _dirs_by_completion(root: Path, marker: str) -> tuple[list[Path], list[Path]]:
    dirs = sorted(p for p in root.glob("*") if p.is_dir()) if root.exists() else []
    return ([p for p in dirs if (p / marker).exists()],
            [p for p in dirs if not (p / marker).exists()])


def gc_storage(rt: Runtime) -> dict[str, int]:
    """Deletes exports, golden sets, feature sets, runs and backtests nobody needs.

    It runs only under the work lock, so a directory without its completion
    marker is a failed attempt, and no DuckDB or load is using the temp dir.
    Exports are resumable and follow the export state instead; a feature set
    lives only while the pointer or the compacted state names it.
    """
    cfg, layout = rt.cfg, rt.layout
    state = load_state(layout)
    pending = state.get("in_progress") or {}
    full_job = pending.get("full")
    keep_deltas = {d["id"] for d in state.get("deltas", [])} | {j["id"] for j in pending.values()}
    keep_bases, keep_dims = set(), set()
    if state.get("base") and not state.get("compacted"):
        keep_bases.add(f"base_{state['base']['id']}")
    if state.get("dims"):
        keep_dims.add(f"dims_{state['dims']}")
    if full_job:
        keep_deltas.add(f"{full_job['id']}c")
        keep_bases.add(f"base_{full_job['id']}")
        keep_dims.add(f"dims_{full_job['id']}")
    doomed = [p for p in layout.exports.glob("base_*") if p.name not in keep_bases]
    doomed += [p for p in layout.exports.glob("delta_*")
               if p.name.removeprefix("delta_") not in keep_deltas]
    doomed += [p for p in layout.exports.glob("dims_*") if p.name not in keep_dims]
    ptrs = rt.pointers()
    protected = {ptrs.get("golden"), ptrs.get("features"), ptrs.get("run"),
                 (state.get("compacted") or {}).get("features")}
    for root, marker, keep in ((layout.root / "golden", "meta.json", cfg.storage.keep_runs),
                               (layout.root / "runs", "manifest.json", cfg.storage.keep_runs),
                               (layout.root / "backtests", "backtest.json",
                                cfg.storage.keep_runs)):
        complete, unfinished = _dirs_by_completion(root, marker)
        doomed += [p for p in unfinished + complete[:-keep] if p.name not in protected]
    # A feature set nothing points at can never be merged again: its deltas are gone.
    doomed += [p for p in sorted((layout.root / "features").glob("*"))
               if p.is_dir() and p.name not in protected]
    for p in doomed:
        shutil.rmtree(p)
    shutil.rmtree(layout.duckdb_tmp, ignore_errors=True)
    return {"removed_dirs": len(doomed)}


def _try_pending_swap(rt: Runtime, sched: dict[str, Any], now: datetime) -> None:
    """Swaps the pending run in when allowed, then verifies it; failures keep it pending."""
    pending = sched.get("pending_swap")
    if not pending or not swap_allowed(now, rt.cfg.schedule):
        return
    try:
        result = stage_swap(rt, pending["db_run_id"])
    except Exception as exc:
        rt.events.emit("swap.failed", db_run_id=pending["db_run_id"],
                       error=f"{type(exc).__name__}: {str(exc)[:500]}")
        return
    if result["swapped"]:
        _best_effort(rt, "verify_live", lambda: stage_verify_live(rt, pending["run_id"]))
    if result["swapped"] or not result.get("retry", True):
        sched["pending_swap"] = None
        atomic_write_json(rt.layout.schedule_file, sched)


def _build(rt: Runtime, sched: dict[str, Any], target: str) -> None:
    """Pre-measurement build from the exports on disk; it reads no crawlerdb.

    The daily delta feeds it, so a stuck or refused crawlerdb never blocks it.
    An unreachable metricdb only makes the golden set staler. Once the list is
    loaded the cycle is recorded at once, so a later failure never loads it again.
    """
    _merge_inputs(rt, load_state(rt.layout))  # cheapest check, and the most specific error
    _preflight_build(rt)
    try:
        golden_id = stage_golden(rt)["golden_id"]
    except Exception as exc:
        golden_id = rt.pointers().get("golden")
        if golden_id is None:
            raise
        rt.events.emit("daemon.golden_fallback", golden_id=golden_id,
                       reason=f"{type(exc).__name__}: {exc}"[:500])
    built = _build_and_load(rt, golden_id)
    sched.update(built_for=target,
                 pending_swap={"db_run_id": built["db_run_id"], "run_id": built["run_id"]})
    atomic_write_json(rt.layout.schedule_file, sched)
    _try_pending_swap(rt, sched, datetime.now(UTC))
    _best_effort(rt, "eval", lambda: stage_eval(rt, built["run_id"]))


def _tick(rt: Runtime, sched: dict[str, Any], now: datetime) -> None:
    """One daemon step; every outcome is recorded in `sched`."""
    cfg = rt.cfg
    # A loaded list goes live as soon as allowed, even while errors cool down.
    _best_effort(rt, "pending_swap", lambda: _try_pending_swap(rt, sched, now))
    cooldown = sched.get("cooldown_until")
    if cooldown and now < datetime.fromisoformat(cooldown):
        rt.events.emit("daemon.cooldown", until=cooldown)
        return
    decision = None
    try:
        _best_effort(rt, "gc", lambda: gc_storage(rt))
        state = load_state(rt.layout)
        decision = plan_next_action(now, sched, cfg.schedule, _full_export_due(rt, state),
                                    bool(state.get("base")))
        rt.events.emit("daemon.tick", action=decision.action.value, reason=decision.reason)
        target = decision.measurement.isoformat() if decision.measurement else None
        if decision.action == Action.EXPORT_FULL:
            deadline = decision.measurement - timedelta(
                hours=cfg.schedule.full_export_deadline_h_before_measure)
            stage_export(rt, "full", deadline=deadline)
            sched.update(full_done_for=target, last_delta_at=now.isoformat())
        elif decision.action == Action.EXPORT_DELTA:
            stage_export(rt, "delta", deadline=now + timedelta(hours=cfg.schedule.delta_max_h))
            sched["last_delta_at"] = now.isoformat()
        elif decision.action == Action.BUILD:
            _build(rt, sched, target)
    except NeedFullRefresh as exc:
        _request_full(rt, str(exc))
    except (PreflightFailed, GuardAbort) as exc:
        hours_left = (round((decision.measurement - now).total_seconds() / 3600, 1)
                      if decision and decision.measurement else None)
        rt.events.emit("daemon.deferred", reason=str(exc), hours_left=hours_left,
                       action=decision.action.value if decision else None)
    except Exception as exc:
        until = now + timedelta(seconds=cfg.schedule.error_cooldown_s)
        sched["cooldown_until"] = until.isoformat()
        rt.events.emit("daemon.error", error=f"{type(exc).__name__}: {str(exc)[:500]}",
                       cooldown_until=until)


def daemon(rt: Runtime, max_ticks: int | None = None) -> None:
    """Follows the schedule: daily deltas, full refreshes and pre-measurement builds.

    A lifetime lock keeps a second daemon out. Each tick holds the work lock and
    releases it while sleeping, so manual commands can run between ticks.

    Raises:
        WorkDirBusy: If another daemon is running.
    """
    cfg = rt.cfg
    if not cfg.schedule.enabled:
        raise RuntimeError("daemon needs schedule.enabled = true")
    with work_lock(rt.layout.daemon_lock_file, "daemon"):
        ticks = 0
        while max_ticks is None or ticks < max_ticks:
            ticks += 1
            try:
                with work_lock(rt.layout.lock_file, "daemon tick"):
                    sched = read_json(rt.layout.schedule_file, {})
                    _tick(rt, sched, datetime.now(UTC))
                    atomic_write_json(rt.layout.schedule_file, sched)
            except WorkDirBusy as exc:
                rt.events.emit("daemon.busy", reason=str(exc))
            if max_ticks is None or ticks < max_ticks:
                time.sleep(cfg.schedule.tick_s)


def status(rt: Runtime) -> dict[str, Any]:
    return {"state": load_state(rt.layout), "pointers": rt.pointers(),
            "schedule": read_json(rt.layout.schedule_file, {}),
            "work_dir": str(Path(rt.cfg.run.work_dir).resolve()),
            "pid": os.getpid()}
