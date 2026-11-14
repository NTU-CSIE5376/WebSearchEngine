"""A new list for every golden day, unattended: `isel refresh --loop`.

One cycle per golden day G (`isel.schedule.Window`). From `start_at` the cycle clears
the old export, pulls golden and dims, exports crawlerdb, derives, trains, scores,
selects, gates, publishes and swaps, all before `swap_by`. Once the live list is this
cycle's, it verifies it, syncs the recrawl frontier and drops old selection runs, even
after `swap_by`. The text index stays manual.

Shape: observe, plan, act.

- `observe` runs in the parent and reads only JSON stamps, mtimes, directory names and
  `model_runs` rows, so it needs no DuckDB. Every artifact is stamped with what it was
  made from: sample, positives and model with the export's `started_at`; scores and
  selection runs with the model dir; the selectdb row with the run label. The live
  row's `cutoff_datetime` is the export's `started_at` again. So `Evidence` says "this
  cycle's sample exists", not "a sample exists".
- `plan` is a pure function of (window, evidence, outcome, now). It names the next
  step, idles, or finishes the cycle.
- The step runs in a fresh spawned process, in its own process group, until it returns
  or its deadline passes. The parent then records the result in the cycle's `Outcome`
  and checks that the step's evidence now holds.

Done-ness is never stored for a step whose rerun after an unrecorded success would fail
or redo hours of work (derive refuses an existing meta, publish hits `run_label`
UNIQUE). Those are `DerivedStep`s: their `done` takes `Evidence` only, so it cannot read
the outcome. The outcome keeps results only for `RecordedStep`s (gates, verify, sync),
which are safe to rerun.

Single writer: only a tick holding the work lock exclusively runs a step or writes an
outcome file. CLI commands that touch the work dir hold it shared, so they still run
beside each other but never beside a refresh step.

This module and everything it imports at module level must not load DuckDB.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import logging
import multiprocessing
import os
import shutil
import signal
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, ExitStack, closing
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, NoReturn, Protocol

from isel.config import Config, RefreshCfg
from isel.io import (
    LockBusy,
    atomic_write_json,
    code_version,
    configure_logging,
    file_lock,
    read_json,
)
from isel.schedule import Window, cycle_at

log = logging.getLogger(__name__)

REFRESH_DIR = "refresh"            # under work_dir: `lock` and `outcomes/<G>.json`
SCRATCH_DIR = "duckdb_tmp"         # DuckDB spill and staging CSVs of every step
MANIFEST_FILE = "_MANIFEST.json"   # written last by manifest.write_run: no file, no run
# model_runs statuses of a run someone took off: the cycle stops and never swaps it in.
WITHDRAWN_STATUSES = frozenset({"abandoned", "rolled_back", "retired"})

Clock = Callable[[], datetime]
# Waits for a step's result in slices: epoll and kqueue refuse a timeout past 24.8 days.
_WAIT_SLICE_S = 3600.0


def utcnow() -> datetime:
    """The real clock; module level, so a spawned step process can unpickle it."""
    return datetime.now(UTC)


# ---------------------------------------------------------------- evidence


class ExportState(enum.StrEnum):
    ABSENT = "absent"        # no export
    STALE = "stale"          # started before start_at, or part files without _meta.json
    PARTIAL = "partial"      # started at or after start_at, shards missing
    FINISHED = "finished"    # started at or after start_at, every shard done


class ScoredState(enum.StrEnum):
    ABSENT = "absent"
    FOREIGN = "foreign"      # stamped with another model, export or K
    PARTIAL = "partial"      # this model's, unfinished or failed a check
    FINISHED = "finished"    # this model's, finished, kept check passed


@dataclass(frozen=True)
class DbRun:
    """One `public.model_runs` row."""

    id: int
    label: str
    status: str
    cutoff_at: datetime | None   # the export's started_at; NULL on rows made by hand


@dataclass(frozen=True)
class Evidence:
    """What this cycle has built so far, resolved link by link from stamps.

    A link is looked up only when the links it was made from are set, so `Evidence`
    never claims a later artifact without the earlier ones.

    Raises:
        ValueError: On construction, if the links contradict each other.
    """

    export: ExportState
    export_id: str | None        # this cycle's export: its _meta.json started_at
    golden: bool                 # golden/summary.json written at or after start_at
    dims: bool                   # dims/split_hosts.parquet written at or after start_at
    sample: bool                 # sample/_meta.json finished and stamped export_id
    positives: bool              # positives/_meta.json stamped export_id
    model: Path | None           # newest models/<id> with a meta.json stamped export_id
    scored: ScoredState          # full/scored against model, export_id and eval.k_headline
    run: str | None              # newest complete full/selection run of model and K
    db_run: DbRun | None         # model_runs row of `run`
    live: DbRun | None           # the live model_runs row, of any cycle
    prev: str | None             # run label of selected_urls_prev's rows
    selection: frozenset[str]    # every run=<label> dir under full/selection
    selectdb_bytes: int | None

    def __post_init__(self) -> None:
        finished = self.export is ExportState.FINISHED
        rules = (
            ((self.export_id is not None)
             == (self.export in (ExportState.PARTIAL, ExportState.FINISHED)),
             "export_id is set exactly when the export is this cycle's"),
            (finished or not (self.sample or self.positives),
             "sample and positives need a finished export"),
            (self.model is None or (self.sample and self.positives and self.golden
                                    and self.dims),
             "a model needs the sample, positives, golden and dims"),
            (self.model is not None or self.scored is ScoredState.ABSENT,
             "scores are judged only against a model"),
            (self.run is None or self.scored is ScoredState.FINISHED,
             "a run needs finished scores"),
            (self.db_run is None or self.db_run.label == self.run,
             "db_run is the row of run"),
        )
        for ok, rule in rules:
            if not ok:
                raise ValueError(f"inconsistent evidence: {rule}")

    def live_in_cycle(self, w: Window) -> bool:
        """The live list was cut from this cycle's export.

        Provenance, not insertion time: `cutoff_datetime` is the export's started_at,
        which the export step takes from the injected clock.
        """
        return (self.live is not None and self.live.cutoff_at is not None
                and self.live.cutoff_at >= w.start_at)


def observe(cfg: Config, w: Window) -> Evidence:
    """The evidence for `w`: stamps under work_dir, then selectdb as the reader role.

    Raises:
        Exception: If selectdb cannot be read (psycopg.Error, a missing DSN); nothing
            is decided then.
    """
    return _read_db(cfg, _read_disk(cfg.work_dir, w, cfg.eval.k_headline))


def _fresh(path: Path, since: datetime) -> bool:
    return path.exists() and datetime.fromtimestamp(path.stat().st_mtime, UTC) >= since


def _export_state(export: Path, since: datetime) -> tuple[ExportState, str | None]:
    meta = read_json(export / "_meta.json")
    if meta is None:
        parts = any(export.glob("shard=*/part-*.parquet"))
        return (ExportState.STALE if parts else ExportState.ABSENT), None
    started = meta.get("started_at")
    if started is None or datetime.fromisoformat(started) < since:
        return ExportState.STALE, None
    return (ExportState.FINISHED if "finished_at" in meta else ExportState.PARTIAL), started


def _stamped_model(models: Path, export_id: str) -> Path | None:
    for model in sorted((p for p in models.glob("*") if p.is_dir()), reverse=True):
        for meta_path in model.glob("*/meta.json"):
            fingerprint = (read_json(meta_path) or {}).get("sample_fingerprint") or {}
            if fingerprint.get("export_started_at") == export_id:
                return model
    return None


def _scored_state(meta: dict[str, Any] | None, model: Path, export_id: str,
                  k: int) -> ScoredState:
    if meta is None:
        return ScoredState.ABSENT
    if (meta.get("model_dir"), meta.get("export_started_at"), meta.get("k")) != (
            str(model), export_id, k):
        return ScoredState.FOREIGN
    done = ("finished_at" in meta and not meta.get("failed")
            and (meta.get("kept_check") or {}).get("passed"))
    return ScoredState.FINISHED if done else ScoredState.PARTIAL


def _model_run(selection: Path, model: Path, k: int) -> str | None:
    for run in sorted(selection.glob("run=*"), reverse=True):
        meta = read_json(run / MANIFEST_FILE) or {}
        if (meta.get("scope"), meta.get("model_dir"), meta.get("k"), meta.get("rows")) == (
                "full", str(model), k, k):
            return run.name.removeprefix("run=")
    return None


def _read_disk(work_dir: Path, w: Window, k: int) -> Evidence:
    """The disk half of the evidence; the selectdb fields are None."""
    export, export_id = _export_state(work_dir / "full" / "export", w.start_at)
    finished = export is ExportState.FINISHED
    golden = _fresh(work_dir / "golden" / "summary.json", w.start_at)
    dims = _fresh(work_dir / "dims" / "split_hosts.parquet", w.start_at)
    sample_meta = read_json(work_dir / "sample" / "_meta.json") or {}
    sample = (finished and "finished_at" in sample_meta
              and sample_meta.get("export_started_at") == export_id)
    positives_meta = read_json(work_dir / "positives" / "_meta.json") or {}
    positives = finished and positives_meta.get("export_started_at") == export_id
    model = None
    if sample and positives and golden and dims:
        model = _stamped_model(work_dir / "models", export_id)
    scored = ScoredState.ABSENT
    if model is not None:
        scored = _scored_state(read_json(work_dir / "full" / "scored" / "_meta.json"),
                               model, export_id, k)
    selection_dir = work_dir / "full" / "selection"
    run = _model_run(selection_dir, model, k) if scored is ScoredState.FINISHED else None
    selection = frozenset(p.name.removeprefix("run=") for p in selection_dir.glob("run=*")
                          if p.is_dir())
    return Evidence(export, export_id, golden, dims, sample, positives, model, scored, run,
                    db_run=None, live=None, prev=None, selection=selection,
                    selectdb_bytes=None)


_RUN_ROW = "SELECT id, run_label, status, cutoff_datetime FROM public.model_runs"


def _read_db(cfg: Config, ev: Evidence) -> Evidence:
    """Adds the selectdb half: the live row, `run`'s row, prev's label, the db size."""
    from isel.selectdb import connect

    conn = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"{_RUN_ROW} WHERE status = 'live'")
            live = cur.fetchone()
            db_run = None
            if ev.run is not None:
                cur.execute(f"{_RUN_ROW} WHERE run_label = %s", (ev.run,))
                db_run = cur.fetchone()
            prev = None
            cur.execute("SELECT to_regclass('public.selected_urls_prev') IS NOT NULL")
            if cur.fetchone()[0]:
                # One row names the run; never DISTINCT over 64M rows.
                cur.execute("SELECT run_label FROM public.model_runs WHERE id = "
                            "(SELECT run_id FROM public.selected_urls_prev LIMIT 1)")
                row = cur.fetchone()
                prev = row[0] if row else None
            cur.execute("SELECT pg_database_size(current_database())")
            (size,) = cur.fetchone()
    finally:
        conn.close()
    return replace(ev, live=live and DbRun(*live), db_run=db_run and DbRun(*db_run),
                   prev=prev, selectdb_bytes=int(size))


# ---------------------------------------------------------------- outcome


class Status(enum.StrEnum):
    RUNNING = "running"
    DONE = "done"
    SKIPPED_LATE = "skipped_late"
    SKIPPED_GATE = "skipped_gate"
    WITHDRAWN = "withdrawn"
    FAILED = "failed"


class StepName(enum.StrEnum):
    CLEAR = "clear"
    GOLDEN = "golden"
    DIMS = "dims"
    EXPORT = "export"
    SAMPLE = "sample"
    POSITIVES = "positives"
    MODEL = "model"
    SCORED = "scored"
    SELECTION = "selection"
    GATES = "gates"
    PUBLISH = "publish"
    SWAP = "swap"
    VERIFY = "verify"
    SYNC = "sync"
    CLEANUP = "cleanup"


@dataclass(frozen=True)
class StepError:
    """The last failure: never its message.

    A message can hold a DSN, and `atomic_write_json` would then refuse the outcome on
    every tick. `kind` is an exception class name or one of the code-defined tokens
    `deadline`, `no_evidence`, `exit:<code>` and `window_ended`.
    """

    step: StepName | None
    kind: str


@dataclass(frozen=True)
class TagCoverage:
    new: float              # metric rate of the target batch under the new run's URLs
    live: float             # ... under the live run's URLs
    ratio: float | None     # new / live; None when the live run found none (passes)


@dataclass(frozen=True)
class GateVerdict:
    """Both gates on one run, before anything of it reaches selectdb."""

    run_label: str
    live_label: str
    batch: int                          # newest golden batch not in golden.exclude_batches
    overlap_share: float                # the new run's doc_ids on the live run's manifest
    coverage: Mapping[str, TagCoverage]
    passed: bool


@dataclass(frozen=True)
class VerifyRecord:
    run_label: str                      # the live run verify_live read
    batch: int
    rates: Mapping[str, float]          # indexed_metric_rate per tag


@dataclass(frozen=True)
class Audit:
    """A snapshot at the terminal decision, for people; `plan` never reads it."""

    run_label: str | None
    db_run_id: int | None
    live_label: str | None
    selectdb_bytes: int | None
    code_version: str
    finished_at: datetime


@dataclass(frozen=True)
class Outcome:
    """One cycle's result: `work_dir/refresh/outcomes/<G>.json`.

    Only a tick holding the work lock exclusively writes it. It holds no "step N is
    done" marks: the evidence says that.
    """

    golden_day: date
    status: Status = Status.RUNNING
    failures: Mapping[StepName, int] = field(default_factory=dict)
    error: StepError | None = None
    pending: StepName | None = None         # the step a finished cycle stopped at
    gates: GateVerdict | None = None
    verify: VerifyRecord | None = None
    synced_run_id: int | None = None        # model_run_id that recrawl-sync loaded
    audit: Audit | None = None

    def to_json(self) -> dict[str, Any]:
        def plain(value: Any) -> Any:
            return None if value is None else dataclasses.asdict(value)

        audit = plain(self.audit)
        if audit is not None:
            audit["finished_at"] = self.audit.finished_at.isoformat()
        return {"golden_day": self.golden_day.isoformat(), "status": self.status.value,
                "failures": {s.value: n for s, n in self.failures.items()},
                "error": plain(self.error), "pending": self.pending,
                "gates": plain(self.gates), "verify": plain(self.verify),
                "synced_run_id": self.synced_run_id, "audit": audit}

    @staticmethod
    def from_json(data: Mapping[str, Any]) -> Outcome:
        error, gates, verify, audit = (data[k] for k in ("error", "gates", "verify", "audit"))
        return Outcome(
            golden_day=date.fromisoformat(data["golden_day"]),
            status=Status(data["status"]),
            failures={StepName(s): int(n) for s, n in data["failures"].items()},
            error=error and StepError(error["step"] and StepName(error["step"]),
                                      error["kind"]),
            pending=data["pending"] and StepName(data["pending"]),
            gates=gates and GateVerdict(**{**gates, "coverage": {
                tag: TagCoverage(**c) for tag, c in gates["coverage"].items()}}),
            verify=verify and VerifyRecord(**verify),
            synced_run_id=data["synced_run_id"],
            audit=audit and Audit(**{**audit, "finished_at": datetime.fromisoformat(
                audit["finished_at"])}))


def outcome_path(cfg: Config, golden_day: date) -> Path:
    return cfg.work_dir / REFRESH_DIR / "outcomes" / f"{golden_day.isoformat()}.json"


def read_outcome(cfg: Config, golden_day: date) -> Outcome:
    """The stored outcome of `golden_day`, or a fresh running one."""
    data = read_json(outcome_path(cfg, golden_day))
    return Outcome(golden_day) if data is None else Outcome.from_json(data)


def write_outcome(cfg: Config, out: Outcome) -> None:
    atomic_write_json(outcome_path(cfg, out.golden_day), out.to_json())


# ---------------------------------------------------------------- steps


class Phase(enum.StrEnum):
    BUILD = "build"      # before swap_by; deadline swap_by
    FOLLOW = "follow"    # once the live run is this cycle's, even after swap_by


class NotYet(Exception):
    """Raised by a step that made progress but is not finished.

    The export still misses shards, or the swap timed out on its lock. No failure is
    counted.
    """


class GateInputMissing(RuntimeError):
    """Raised when the gates have nothing to compare with: no live run or its manifest."""


class SwapRefused(RuntimeError):
    """Raised when `publish.swap` refuses the run for good (no loaded table)."""


@dataclass(frozen=True)
class Job:
    """Everything a step needs. Picklable: it crosses into the spawned step process."""

    cfg: Config
    window: Window
    evidence: Evidence
    clock: Clock


Act = Callable[[Job], Any]


@dataclass(frozen=True)
class DerivedStep:
    """A step whose done-ness comes from evidence alone: `done` cannot see the outcome."""

    name: StepName
    phase: Phase
    done: Callable[[Evidence], bool]
    act: Act


@dataclass(frozen=True)
class RecordedStep:
    """A step safe to rerun; the outcome keeps its result.

    `record` stores what `act` returned; `done` matches that record against the
    evidence (the gated run label, the live label or id).
    """

    name: StepName
    phase: Phase
    done: Callable[[Evidence, Outcome], bool]
    act: Act
    record: Callable[[Outcome, Any], Outcome]
    budgeted: bool = False      # starts only with refresh.sync_budget_min before a freeze


Step = DerivedStep | RecordedStep


def step_done(step: Step, ev: Evidence, out: Outcome) -> bool:
    return step.done(ev) if isinstance(step, DerivedStep) else step.done(ev, out)


def _remove(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)


# Acts run in the step process and import heavy modules there. Each act first removes
# what the evidence ruled out at its own path, so a library's "already holds ..."
# refusal never fires on a killed step's leftovers.


def _clear(job: Job) -> Any:
    """Deletes the stale export and everything derived from it.

    The export goes last: a half-cleared work dir still reads STALE.
    """
    work = job.cfg.work_dir
    for rel in ("full/scored", "sample", "positives", "datasets", "full/export"):
        _remove(work / rel)
    return None


def _pull_golden(job: Job) -> Any:
    from isel.golden import pull_golden
    from isel.pg import open_metricdb

    cfg = job.cfg
    with closing(open_metricdb(cfg)) as session:
        summary = pull_golden(session, cfg.work_dir / "golden", cfg.golden.tags)
    return {"batches": len(summary)}


def _pull_dims(job: Job) -> Any:
    from isel.pg import open_crawlerdb
    from isel.pull import pull_dims

    cfg = job.cfg
    with closing(open_crawlerdb(cfg)) as session:
        return pull_dims(cfg, session, cfg.work_dir / "dims")


def _export(job: Job) -> Any:
    """One `pull_full` pass; raises NotYet while shards are missing."""
    from isel.pg import open_crawlerdb
    from isel.pull import pull_full

    cfg = job.cfg
    with ExitStack() as stack:
        sessions = [stack.enter_context(closing(open_crawlerdb(cfg)))
                    for _ in range(cfg.full.connections)]
        # `now` becomes started_at only on the first pass: the cycle's export identity.
        report = pull_full(cfg, sessions, cfg.work_dir / "full" / "export", now=job.clock())
    if report["shards_missing"]:
        raise NotYet(f"{report['shards_missing']} shards missing")
    return report


def _derive_sample(job: Job) -> Any:
    from isel.derive import derive_sample

    _remove(job.cfg.work_dir / "sample")
    return derive_sample(job.cfg, job.cfg.work_dir)


def _derive_positives(job: Job) -> Any:
    from isel.derive import derive_positives

    _remove(job.cfg.work_dir / "positives")
    return derive_positives(job.cfg, job.cfg.work_dir)


def _train(job: Job) -> Any:
    from isel.backtest import train_frozen

    # No baselines: eval.baselines holds v1_features, which would train a second model.
    return {"model_dir": str(train_frozen(job.cfg, job.cfg.work_dir, []))}


def _score(job: Job) -> Any:
    from isel.score import score_full

    cfg, ev = job.cfg, job.evidence
    if ev.scored is ScoredState.FOREIGN:
        _remove(cfg.work_dir / "full" / "scored")
    return score_full(cfg, cfg.work_dir, ev.model, cfg.eval.k_headline)


def _select(job: Job) -> Any:
    from isel.score import select_full

    meta = select_full(job.cfg, job.cfg.work_dir, job.cfg.eval.k_headline)
    return {k: v for k, v in meta.items() if k != "files"}


def target_batch(golden_rows: list[Mapping[str, Any]], exclude: list[int]) -> int:
    """The newest golden batch not excluded: the batch both the gates and verify read.

    Raises:
        RuntimeError: If every batch is excluded or golden is empty.
    """
    batches = {int(r["batch_id"]) for r in golden_rows} - set(exclude)
    if not batches:
        raise RuntimeError("golden has no batch outside golden.exclude_batches")
    return max(batches)


def judge(overlap_share: float, coverage: Mapping[str, TagCoverage], rc: RefreshCfg) -> bool:
    """Both gates: overlap >= min_overlap_share and, per tag, ratio >= min_coverage_ratio.

    A tag whose ratio is None passes: the live list found none of its golden URLs.
    """
    return overlap_share >= rc.min_overlap_share and all(
        c.ratio is None or c.ratio >= rc.min_coverage_ratio for c in coverage.values())


def _gates(job: Job) -> GateVerdict:
    """Gate (a) overlap and gate (b) coverage, from the two manifests on disk.

    Reads no selectdb table and writes nothing.

    Raises:
        GateInputMissing: If there is no live run or its manifest is not on disk.
    """
    import pyarrow as pa

    from isel.evaluate import golden_by_tag, metric_rate
    from isel.golden import read_golden
    from isel.io import open_duckdb, sql_str

    cfg, ev = job.cfg, job.evidence
    if ev.live is None:
        raise GateInputMissing("selectdb has no live run to compare with")
    selection = cfg.work_dir / "full" / "selection"
    if not (selection / f"run={ev.live.label}" / MANIFEST_FILE).exists():
        raise GateInputMissing(f"the live run {ev.live.label} has no manifest in the work dir")
    new, live = (sql_str(str(selection / f"run={label}" / "manifest" / "*.parquet"))
                 for label in (ev.run, ev.live.label))
    con = open_duckdb(cfg, cfg.work_dir / SCRATCH_DIR)
    try:
        (share,) = con.execute(
            "SELECT count(l.doc_id)::DOUBLE / count(*) "
            f"FROM (SELECT DISTINCT doc_id FROM read_parquet({new})) n "
            f"LEFT JOIN (SELECT DISTINCT doc_id FROM read_parquet({live})) l "
            "USING (doc_id)").fetchone()
        rows = read_golden(cfg.work_dir).to_pylist()
        batch = target_batch(rows, cfg.golden.exclude_batches)
        coverage = {}
        for tag, g in golden_by_tag(rows, batch, cfg.golden.tags).items():
            # The spellings verify_live looks up, so the rate under the new run equals
            # what verify reads once publish has loaded exactly this manifest.
            con.register("spellings", pa.table(
                {"url": sorted(set(g.urls) | set(g.canonical.values()))}))
            rates = []
            for manifest in (new, live):
                listed = {u for (u,) in con.execute(
                    f"SELECT DISTINCT url FROM read_parquet({manifest}) "
                    "SEMI JOIN spellings USING (url)").fetchall()}
                rates.append(metric_rate(g, listed))
            con.unregister("spellings")
            coverage[tag] = TagCoverage(rates[0], rates[1],
                                        rates[0] / rates[1] if rates[1] else None)
    finally:
        con.close()
    share = float(share or 0.0)
    return GateVerdict(ev.run, ev.live.label, batch, share, coverage,
                       judge(share, coverage, cfg.refresh))


def _verdict(ev: Evidence, out: Outcome) -> GateVerdict | None:
    """The recorded verdict if it judged this run against the current live run."""
    g = out.gates
    if g is None or ev.live is None or (g.run_label, g.live_label) != (ev.run, ev.live.label):
        return None
    return g


def _publish(job: Job) -> Any:
    from isel.publish import load_selection
    from isel.selectdb import admin

    cfg = job.cfg
    run_dir = cfg.work_dir / "full" / "selection" / f"run={job.evidence.run}"
    with admin(cfg) as conn:
        return load_selection(cfg, conn, run_dir, cfg.work_dir)


def _swap(job: Job) -> Any:
    """Raises NotYet when the swap asks for a retry (lock timeout or freeze)."""
    from isel.publish import swap
    from isel.selectdb import admin

    with admin(job.cfg) as conn:
        result = swap(job.cfg, conn, job.evidence.db_run.id, now=job.clock)
    if result["swapped"]:
        return result
    if result.get("retry"):
        raise NotYet(result["reason"])
    raise SwapRefused(result["reason"])


def _verify(job: Job) -> VerifyRecord:
    from isel.golden import read_golden
    from isel.publish import verify_live

    cfg = job.cfg
    batch = target_batch(read_golden(cfg.work_dir).to_pylist(), cfg.golden.exclude_batches)
    report = verify_live(cfg, cfg.work_dir, batch)
    (live,) = report["live_runs"]
    return VerifyRecord(live["run_label"], batch,
                        {tag: r["indexed_metric_rate"] for tag, r in report["tags"].items()})


def _sync(job: Job) -> Any:
    from isel.recrawl import sync
    from isel.selectdb import admin

    with admin(job.cfg, ready=True) as conn:
        return sync(job.cfg, conn, now=job.clock)


def _kept(ev: Evidence) -> set[str]:
    """The selection runs cleanup keeps: live, and prev for a rollback."""
    return {ev.live.label} | ({ev.prev} if ev.prev else set())


def _cleanup(job: Job) -> Any:
    """Deletes every selection run but the live and prev ones.

    `_MANIFEST.json` goes first, so a half-deleted run reads incomplete to
    `manifest.latest_run` and to `observe`.
    """
    ev = job.evidence
    selection = job.cfg.work_dir / "full" / "selection"
    removed = sorted(ev.selection - _kept(ev))
    for label in removed:
        run = selection / f"run={label}"
        (run / MANIFEST_FILE).unlink(missing_ok=True)
        _remove(run)
    return {"removed": removed}


STEPS: tuple[Step, ...] = (
    DerivedStep(StepName.CLEAR, Phase.BUILD,
                lambda e: e.export is not ExportState.STALE, _clear),
    DerivedStep(StepName.GOLDEN, Phase.BUILD, lambda e: e.golden, _pull_golden),
    DerivedStep(StepName.DIMS, Phase.BUILD, lambda e: e.dims, _pull_dims),
    DerivedStep(StepName.EXPORT, Phase.BUILD,
                lambda e: e.export is ExportState.FINISHED, _export),
    DerivedStep(StepName.SAMPLE, Phase.BUILD, lambda e: e.sample, _derive_sample),
    DerivedStep(StepName.POSITIVES, Phase.BUILD, lambda e: e.positives, _derive_positives),
    DerivedStep(StepName.MODEL, Phase.BUILD, lambda e: e.model is not None, _train),
    DerivedStep(StepName.SCORED, Phase.BUILD,
                lambda e: e.scored is ScoredState.FINISHED, _score),
    DerivedStep(StepName.SELECTION, Phase.BUILD, lambda e: e.run is not None, _select),
    RecordedStep(StepName.GATES, Phase.BUILD,
                 lambda e, o: _verdict(e, o) is not None, _gates,
                 lambda o, verdict: replace(o, gates=verdict)),
    DerivedStep(StepName.PUBLISH, Phase.BUILD, lambda e: e.db_run is not None, _publish),
    DerivedStep(StepName.SWAP, Phase.BUILD,
                lambda e: e.live is not None and e.live.label == e.run, _swap),
    RecordedStep(StepName.VERIFY, Phase.FOLLOW,
                 lambda e, o: (e.live is not None and o.verify is not None
                               and o.verify.run_label == e.live.label),
                 _verify, lambda o, rec: replace(o, verify=rec)),
    RecordedStep(StepName.SYNC, Phase.FOLLOW,
                 lambda e, o: e.live is not None and o.synced_run_id == e.live.id,
                 _sync, lambda o, summary: replace(o, synced_run_id=int(summary["model_run_id"])),
                 budgeted=True),
    DerivedStep(StepName.CLEANUP, Phase.FOLLOW,
                lambda e: e.live is not None and e.selection <= _kept(e), _cleanup),
)
STEP: Mapping[StepName, Step] = {s.name: s for s in STEPS}
BUILD = tuple(s for s in STEPS if s.phase is Phase.BUILD)
FOLLOW = tuple(s for s in STEPS if s.phase is Phase.FOLLOW)
if tuple(s.name for s in STEPS) != tuple(StepName):
    raise ImportError("refresh.STEPS and refresh.StepName must list the same steps in order")


# ---------------------------------------------------------------- plan (pure)


@dataclass(frozen=True)
class Idle:
    reason: str


@dataclass(frozen=True)
class Finish:
    status: Status
    step: StepName | None = None     # becomes Outcome.pending


@dataclass(frozen=True)
class Run:
    step: StepName
    deadline: datetime


Decision = Idle | Finish | Run


def _first_pending(steps: tuple[Step, ...], ev: Evidence, out: Outcome) -> Step | None:
    return next((s for s in steps if not step_done(s, ev, out)), None)


def _exhausted(step: Step, out: Outcome, rc: RefreshCfg) -> Finish | None:
    if out.failures.get(step.name, 0) >= rc.max_attempts:
        return Finish(Status.FAILED, step.name)
    return None


def _follow(step: Step, w: Window, now: datetime, rc: RefreshCfg) -> Decision:
    if not (isinstance(step, RecordedStep) and step.budgeted):
        return Run(step.name, w.ends_at)
    if now + timedelta(minutes=rc.sync_budget_min) <= w.freeze_start:
        return Run(step.name, w.freeze_start)
    if now >= w.freeze_end:
        return Run(step.name, w.ends_at)
    return Idle(f"{step.name} waits for the freeze to end at {w.freeze_end.isoformat()}")


def plan(w: Window, ev: Evidence, out: Outcome, now: datetime, rc: RefreshCfg) -> Decision:
    """The next move of cycle `w`. Pure: no clock, no I/O.

    Rules, in order:
    1. A finished outcome idles.
    2. The live run is this cycle's: the first FOLLOW step not done, even after
       swap_by; none left is DONE. A budgeted step starts only with
       `rc.sync_budget_min` before the freeze (deadline freeze_start) or after the
       freeze (deadline ends_at); the others run with deadline ends_at.
    3. This cycle's run was taken off by hand (`WITHDRAWN_STATUSES`): WITHDRAWN.
    4. `now >= swap_by`: SKIPPED_LATE, pending the first BUILD step not done.
    5. The gates rejected this run: SKIPPED_GATE.
    6. The first BUILD step not done, deadline swap_by.
    A step that failed `rc.max_attempts` times finishes the cycle FAILED instead.
    """
    if out.status is not Status.RUNNING:
        return Idle(f"the cycle is {out.status}")
    if ev.live_in_cycle(w):
        step = _first_pending(FOLLOW, ev, out)
        if step is None:
            return Finish(Status.DONE)
        return _exhausted(step, out, rc) or _follow(step, w, now, rc)
    if ev.db_run is not None and ev.db_run.status in WITHDRAWN_STATUSES:
        return Finish(Status.WITHDRAWN)
    if now >= w.swap_by:
        step = _first_pending(BUILD, ev, out)
        return Finish(Status.SKIPPED_LATE, step and step.name)
    verdict = _verdict(ev, out)
    if verdict is not None and not verdict.passed:
        return Finish(Status.SKIPPED_GATE, StepName.GATES)
    step = _first_pending(BUILD, ev, out)
    if step is None:
        return Idle("every build step is done, yet the live run is not this cycle's")
    return _exhausted(step, out, rc) or Run(step.name, w.swap_by)


# ---------------------------------------------------------------- running a step


@dataclass(frozen=True)
class Completed:
    value: Any                 # the act's return; a RecordedStep stores it


@dataclass(frozen=True)
class Deferred:
    pass                       # the act raised NotYet


@dataclass(frozen=True)
class Failed:
    kind: str                  # exception class name, "exit:<code>" or "no_evidence"


@dataclass(frozen=True)
class Killed:
    pass                       # the deadline passed; the process group was ended


RunResult = Completed | Deferred | Failed | Killed


class Runner(Protocol):
    def __call__(self, job: Job, step: StepName, deadline: datetime) -> RunResult: ...


def record(out: Outcome, step: Step, result: RunResult) -> Outcome:
    """The outcome after one run of `step`.

    Completed: a RecordedStep stores its value; a DerivedStep changes nothing.
    Failed: one more failure of `step`, and the error. Killed: the error "deadline",
    no failure counted (the deadline itself ends the phase). Deferred: unchanged.
    """
    if isinstance(result, Completed):
        return step.record(out, result.value) if isinstance(step, RecordedStep) else out
    if isinstance(result, Failed):
        return replace(out, failures={**out.failures,
                                      step.name: out.failures.get(step.name, 0) + 1},
                       error=StepError(step.name, result.kind))
    if isinstance(result, Killed):
        return replace(out, error=StepError(step.name, "deadline"))
    return out


def finish(out: Outcome, d: Finish, ev: Evidence, at: datetime) -> Outcome:
    """The terminal outcome: status, pending step and the audit snapshot."""
    return replace(out, status=d.status, pending=d.step, audit=Audit(
        run_label=ev.run, db_run_id=ev.db_run and ev.db_run.id,
        live_label=ev.live and ev.live.label, selectdb_bytes=ev.selectdb_bytes,
        code_version=code_version(), finished_at=at))


def execute(act: Act, job: Job) -> RunResult:
    """Runs `act` and maps how it ended; the step process and in-process tests share it.

    The traceback goes to the log; only the exception class leaves this function.
    KeyboardInterrupt and SystemExit propagate.
    """
    try:
        value = act(job)
    except NotYet as exc:
        log.info("not finished yet: %s", exc)
        return Deferred()
    except Exception as exc:
        log.exception("refresh step failed")
        return Failed(type(exc).__name__)
    return Completed(value)


def _child_main(act: Act, job: Job, send: Connection) -> None:
    os.setpgid(0, 0)   # its own group: a kill reaches select_full's workers too
    configure_logging()
    send.send(execute(act, job))
    send.close()


def _signal_group(proc: multiprocessing.process.BaseProcess, sig: int) -> None:
    try:
        os.killpg(proc.pid, sig)
    except ProcessLookupError:
        if proc.is_alive():    # the child has not called setpgid yet
            os.kill(proc.pid, sig)


def _end_group(proc: multiprocessing.process.BaseProcess, grace_s: float) -> None:
    """SIGTERM to the step's process group, then SIGKILL after `grace_s`."""
    _signal_group(proc, signal.SIGTERM)
    proc.join(grace_s)
    _signal_group(proc, signal.SIGKILL)   # also a grandchild that outlived the child
    proc.join()


def _result_by(recv: Connection, deadline: datetime, clock: Clock) -> bool:
    """Waits until the step process sends its result or dies (True), or `deadline` (False)."""
    while (left := (deadline - clock()).total_seconds()) > 0:
        if recv.poll(min(left, _WAIT_SLICE_S)):
            return True
    return False


def run_in_child(act: Act, job: Job, deadline: datetime) -> RunResult:
    """Runs `act(job)` in a fresh spawned, non-daemon process until `deadline`.

    Not a daemon: select_full starts its own worker processes. Not fork: the parent
    holds pyarrow threads. On the deadline, or on any exception in the parent (SIGTERM
    from `docker compose stop`), the child's whole process group gets SIGTERM, then
    SIGKILL after `refresh.kill_grace_s`.
    """
    grace = job.cfg.refresh.kill_grace_s
    if deadline <= job.clock():
        return Killed()
    ctx = multiprocessing.get_context("spawn")
    recv, send = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=_child_main, args=(act, job, send),
                       name=f"isel-refresh-{getattr(act, '__name__', 'step')}")
    proc.start()
    send.close()
    try:
        if not _result_by(recv, deadline, job.clock):
            _end_group(proc, grace)
            return Killed()
        try:
            result = recv.recv()
        except EOFError:
            result = None
        proc.join(grace)
        if proc.is_alive():
            _end_group(proc, grace)
        return Failed(f"exit:{proc.exitcode}") if result is None else result
    except BaseException:
        _end_group(proc, grace)
        raise
    finally:
        recv.close()


def spawn_runner(job: Job, step: StepName, deadline: datetime) -> RunResult:
    """The production `Runner`: one step in its own process (`run_in_child`)."""
    return run_in_child(STEP[step].act, job, deadline)


# ---------------------------------------------------------------- tick, loop, status


def _decision_json(d: Decision) -> dict[str, Any]:
    if isinstance(d, Idle):
        return {"idle": d.reason}
    if isinstance(d, Finish):
        return {"finish": d.status.value, "step": d.step}
    return {"run": d.step.value, "deadline": d.deadline.isoformat()}


def _result_json(r: RunResult | None) -> dict[str, Any] | None:
    if r is None:
        return None
    if isinstance(r, Completed):
        value = r.value
        return {"completed": dataclasses.asdict(value)
                if dataclasses.is_dataclass(value) else value}
    if isinstance(r, Failed):
        return {"failed": r.kind}
    if isinstance(r, Killed):
        return {"killed": "deadline"}
    return {"deferred": True}


@dataclass(frozen=True)
class TickResult:
    golden_day: date
    decision: Decision
    result: RunResult | None = None

    @property
    def again(self) -> bool:
        """A step completed: the loop plans the next one at once."""
        return isinstance(self.result, Completed)

    def as_json(self) -> dict[str, Any]:
        return {"golden_day": self.golden_day.isoformat(),
                "decision": _decision_json(self.decision),
                "result": _result_json(self.result)}


def _lock_path(cfg: Config) -> Path:
    return cfg.work_dir / REFRESH_DIR / "lock"


def work_lock(cfg: Config, *, exclusive: bool) -> AbstractContextManager[None]:
    """The work dir lock, without waiting.

    A refresh tick holds it exclusively. A CLI command that reads or writes what
    refresh rebuilds, or uses `duckdb_tmp`, holds it shared.

    Raises:
        LockBusy: If another holder conflicts.
    """
    return file_lock(_lock_path(cfg), shared=not exclusive)


def work_lock_holder(cfg: Config) -> str:
    """"free", "commands" or "refresh", from two non-blocking probes; creates nothing."""
    path = _lock_path(cfg)
    if not path.exists():
        return "free"
    try:
        with file_lock(path, shared=True):
            pass
    except LockBusy:
        return "refresh"
    try:
        with file_lock(path):
            pass
    except LockBusy:
        return "commands"
    return "free"


def _expire_earlier(cfg: Config, golden_day: date) -> None:
    """Fails every running outcome of an earlier cycle: its window has ended."""
    for path in sorted((cfg.work_dir / REFRESH_DIR / "outcomes").glob("*.json")):
        day = date.fromisoformat(path.stem)
        if day < golden_day:
            out = read_outcome(cfg, day)
            if out.status is Status.RUNNING:
                write_outcome(cfg, replace(out, status=Status.FAILED,
                                           error=StepError(None, "window_ended")))


def tick(cfg: Config, clock: Clock = utcnow, runner: Runner = spawn_runner) -> TickResult:
    """One observe-plan-act round for the cycle that owns `clock()`.

    A finished cycle idles without the lock, selectdb or a write. Otherwise, under the
    exclusive work lock: fail unfinished outcomes of earlier cycles, observe, plan, and
    either finish (write the outcome) or run one step. A step runs with an empty
    `duckdb_tmp`; after a Completed run the evidence is observed again, and if the
    step's evidence does not hold the run counts as Failed("no_evidence").

    Returns an Idle decision, without waiting, if a CLI command holds the lock.

    Raises:
        Exception: From observe (selectdb down) or the outcome write; nothing is
            recorded then and the next tick starts over.
    """
    now = clock()
    w = cycle_at(now, cfg)
    stored = read_outcome(cfg, w.golden_day)
    if stored.status is not Status.RUNNING:
        return TickResult(w.golden_day, Idle(f"the cycle is {stored.status}"))
    with ExitStack() as stack:
        try:
            stack.enter_context(work_lock(cfg, exclusive=True))
        except LockBusy:
            return TickResult(w.golden_day, Idle("a command holds the work dir lock"))
        _expire_earlier(cfg, w.golden_day)
        out = read_outcome(cfg, w.golden_day)
        ev = observe(cfg, w)
        decision = plan(w, ev, out, now, cfg.refresh)
        if isinstance(decision, Idle):
            return TickResult(w.golden_day, decision)
        if isinstance(decision, Finish):
            write_outcome(cfg, finish(out, decision, ev, now))
            return TickResult(w.golden_day, decision)
        step = STEP[decision.step]
        _remove(cfg.work_dir / SCRATCH_DIR)     # a killed step leaves up to 100 GB here
        result = runner(Job(cfg, w, ev, clock), decision.step, decision.deadline)
        new = record(out, step, result)
        if isinstance(result, Completed) and not step_done(step, observe(cfg, w), new):
            result = Failed("no_evidence")
            new = record(out, step, result)
        write_outcome(cfg, new)
        return TickResult(w.golden_day, decision, result)


def _exit_on_sigterm(signum: int, frame: Any) -> None:
    raise SystemExit(0)


def run_loop(cfg: Config, clock: Clock = utcnow, runner: Runner = spawn_runner,
             sleep: Callable[[float], None] = time.sleep) -> NoReturn:
    """Ticks forever: again at once after a completed step, else after `poll_s`.

    SIGTERM becomes SystemExit, so the runner ends the step's process group and the
    lock goes with the process. Any other exception in a tick is logged and retried
    after `poll_s`. Prints one JSON line per tick.
    """
    signal.signal(signal.SIGTERM, _exit_on_sigterm)
    while True:
        try:
            result = tick(cfg, clock, runner)
        except Exception:
            log.exception("refresh tick failed")
            sleep(cfg.refresh.poll_s)
            continue
        print(json.dumps(result.as_json(), sort_keys=True, default=str), flush=True)
        if not result.again:
            sleep(cfg.refresh.poll_s)


def _window_json(w: Window) -> dict[str, Any]:
    return {f.name: getattr(w, f.name).isoformat() for f in dataclasses.fields(w)}


def _evidence_json(ev: Evidence) -> dict[str, Any]:
    return {"export": ev.export.value, "export_id": ev.export_id,
            "model": ev.model and str(ev.model), "scored": ev.scored.value, "run": ev.run,
            "db_run": ev.db_run and dataclasses.asdict(ev.db_run),
            "live": ev.live and dataclasses.asdict(ev.live), "prev": ev.prev,
            "selection": sorted(ev.selection), "selectdb_bytes": ev.selectdb_bytes}


def _verify_matches_gate(out: Outcome) -> bool | None:
    """Verify read what the gate measured; None until both judged the same run."""
    if out.verify is None or out.gates is None or out.verify.run_label != out.gates.run_label:
        return None
    return out.verify.batch == out.gates.batch and dict(out.verify.rates) == {
        tag: c.new for tag, c in out.gates.coverage.items()}


def status(cfg: Config, clock: Clock = utcnow) -> dict[str, Any]:
    """The cycle, its outcome, the evidence per step and the next decision.

    Read-only: it creates or writes no file and holds no lock. If selectdb cannot be
    read, the result names the exception class under "observe_error" and omits the
    evidence, the steps and the next decision.
    """
    now = clock()
    w = cycle_at(now, cfg)
    out = read_outcome(cfg, w.golden_day)
    report: dict[str, Any] = {"now": now.isoformat(), "cycle": _window_json(w),
                              "work_lock": work_lock_holder(cfg), "outcome": out.to_json(),
                              "verify_matches_gate": _verify_matches_gate(out)}
    try:
        ev = observe(cfg, w)
    except Exception as exc:
        log.exception("could not observe the cycle")
        return {**report, "observe_error": type(exc).__name__}
    return {**report, "evidence": _evidence_json(ev),
            "steps": [{"step": s.name.value, "done": step_done(s, ev, out)} for s in STEPS],
            "next": _decision_json(plan(w, ev, out, now, cfg.refresh))}
