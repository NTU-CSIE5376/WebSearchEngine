"""Full-pool scoring with a frozen model, and the exact top-K list.

The domain aggregates come from the sample, as in training. Each exported
file replaces the sample rows in the FeatureStore and goes through the same
frame SQL, which counts a row outside the sample into its own domain
statistics, so a row gets the score it would get as a sample row. One file
at a time bounds the memory: the largest shard does not fit at once.

Only rows scoring at least tau are kept. tau is the sample's score at rank
round(K·p·(1 + tau_margin)). When at least K distinct URLs score >= tau, the
K-th best score is >= tau, so every row of the exact top-K was kept.

Two checks stop a run whose features depend on sample membership:
- every sample row in the export must score exactly as in the threshold frame;
- the kept rows must match the sample's estimate, shard by shard, within
  `full.kept_share_max_z` standard errors.
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from isel.backtest import frozen_model
from isel.config import Config
from isel.features import FeatureStore, open_store
from isel.io import (
    atomic_write_json,
    code_version,
    open_duckdb,
    read_json,
    sql_str,
    utc_id,
    write_parquet_atomic,
)
from isel.manifest import RANK_ORDER, write_run
from isel.model import TrainedModel

SCORED_SCHEMA = pa.schema([("url", pa.string()), ("shard", pa.int16()), ("h", pa.uint64()),
                           ("score", pa.float64())])

# Model facts copied into the run, and from there into selectdb's model_runs.
MODEL_META = ("best_iteration", "train_batches", "valid_batch", "rows", "positives",
              "trained_at", "code_version")

log = logging.getLogger(__name__)


class NotEnoughCandidates(RuntimeError):
    """Raised when fewer than K distinct URLs scored at or above tau."""


def _scored(model: TrainedModel, frame: pa.Table) -> tuple[pa.Table, np.ndarray]:
    """Rows with a domain key and their raw scores; NaN scores sort last."""
    frame = frame.filter(pc.is_valid(frame.column("dkey")))
    return frame, np.nan_to_num(model.raw_score(frame), nan=-np.inf)


def sample_scores(store: FeatureStore, model: TrainedModel, batch_id: int) -> pa.Table:
    """(url, shard, h, score) of every sample row that `score_full` would score."""
    frame = store.frame(batch_id, "eval")
    frame, scores = _scored(model, frame.filter(frame.column("is_sample")))
    return pa.table({"url": frame.column("url"), "shard": frame.column("shard"),
                     "h": frame.column("h"), "score": pa.array(scores)}, schema=SCORED_SCHEMA)


def threshold(cfg: Config, store: FeatureStore, members: pa.Table, k: int) -> dict[str, Any]:
    """The score floor tau from the sample, under the (score desc, URL hash asc) order."""
    scores = members.column("score").to_numpy()
    h = members.column("h").to_numpy(zero_copy_only=False).astype(np.uint64)
    ordered = scores[np.lexsort((h, -scores))]
    rank = max(1, round(k * store.rate * (1 + cfg.full.tau_margin)))
    boundary = max(1, round(k * store.rate))
    return {"tau": float(ordered[rank - 1]) if rank <= len(ordered) else float("-inf"),
            "tau_rank": rank, "sample_rows": len(ordered),
            "sample_boundary_score": (float(ordered[boundary - 1])
                                      if boundary <= len(ordered) else None)}


def _sample_share(members: pa.Table, tau: float) -> dict[int, tuple[int, int]]:
    """Per shard: (sample rows, sample rows scoring >= tau)."""
    shards = members.column("shard").to_numpy()
    hits = members.column("score").to_numpy() >= tau
    return {int(s): (int((shards == s).sum()), int(hits[shards == s].sum()))
            for s in np.unique(shards)}


def _expected(scored: int, rows: int, hits: int, rate: float) -> dict[str, float]:
    """Kept rows the sample predicts for a shard, and the variance of that estimate.

    The sample is a without-replacement draw from the shard, hence the (1 - rate) factor.
    """
    if not rows:
        return {"expected": 0.0, "var": 0.0}
    p = hits / rows
    return {"expected": scored * hits / rows,
            "var": scored * scored * p * (1 - p) / rows * (1 - rate)}


def _kept_check(cfg: Config, shards: dict[str, dict[str, float]]) -> dict[str, Any]:
    kept = sum(v["kept"] for v in shards.values())
    expected = sum(v["expected"] for v in shards.values())
    sd = math.sqrt(sum(v["var"] for v in shards.values()))
    diff = kept - expected
    # A 100 % sample predicts the count exactly; allow for float rounding only.
    z = diff / sd if sd > 0 else (0.0 if abs(diff) < 0.5 else math.copysign(math.inf, diff))
    return {"kept": kept, "expected": expected, "sd": sd, "z": z,
            "passed": abs(z) <= cfg.full.kept_share_max_z}


def _member_mismatches(store: FeatureStore, frame: pa.Table, scores: np.ndarray) -> int:
    """Sample rows of a file whose score differs from the threshold frame's."""
    member = frame.column("is_sample").to_numpy(zero_copy_only=False)
    rows = pa.table({"url": frame.column("url").filter(member),
                     "shard": frame.column("shard").filter(member),
                     "score": pa.array(scores[member])})
    store.con.register("chunk_members", rows)
    try:
        (bad,) = store.con.execute(
            "SELECT count(*) FROM chunk_members c LEFT JOIN sample_scores s "
            "ON s.url = c.url AND s.shard = c.shard "
            "WHERE s.score IS DISTINCT FROM c.score").fetchone()
    finally:
        store.con.unregister("chunk_members")
    return int(bad)


def score_full(cfg: Config, work_dir: Path, model_dir: Path, k: int) -> dict[str, Any]:
    """Scores every exported shard and writes `full/scored/shard=NNN.parquet`, resumably.

    Raises:
        RuntimeError: If the export is unfinished, the model belongs to another
            sample, a sample key has export copies with different features,
            `full/scored` holds scores of another model, K or export or
            failed a check, or a check fails now.
    """
    export = work_dir / "full" / "export"
    exported = read_json(export / "_meta.json") or {}
    if "finished_at" not in exported:
        raise RuntimeError("the full export is unfinished; rerun pull-full until none is missing")
    store = open_store(cfg, work_dir, next_batch=True)
    model = frozen_model(cfg, store, model_dir)
    sample = read_json(work_dir / "sample" / "_meta.json")
    if sample["differing_duplicates"]:
        raise RuntimeError(f"{sample['differing_duplicates']} sample keys have export copies "
                           "with different features; the sample row check would fail")
    target = store.batches[-1].batch_id
    members = sample_scores(store, model, target)
    store.con.register("sample_scores", members)
    t = threshold(cfg, store, members, k)
    share = _sample_share(members, t["tau"])
    out = work_dir / "full" / "scored"
    meta_path = out / "_meta.json"
    meta = read_json(meta_path) or {}
    header = {"model_dir": str(model_dir), "k": k, "tau": t["tau"],
              "snapshot_at": store.snapshot_at.isoformat(), "target_batch": target,
              "export_started_at": exported["started_at"]}
    if meta and any(meta.get(key) != value for key, value in header.items()):
        raise RuntimeError(f"{out} holds scores of another model, K or export; move it away")
    if meta.get("failed"):
        raise RuntimeError(f"{out} failed a check earlier ({meta['failed']}); move it away")
    meta.update(header, threshold=t, config_sha256=cfg.sha256(), code_version=code_version(),
                model_meta={key: model.meta.get(key) for key in MODEL_META})
    shards: dict[str, dict[str, int]] = meta.setdefault("shards", {})
    todo = [s for s in sorted(exported["shards_done"]) if str(s) not in shards]
    for i, shard in enumerate(todo, start=1):
        kept, scored = [SCORED_SCHEMA.empty_table()], 0
        for path in sorted((export / f"shard={shard:03d}").glob("part-*.parquet")):
            store.load_chunk(path)
            frame, scores = _scored(model, store.frame(target, "score"))
            bad = _member_mismatches(store, frame, scores)
            if bad:
                meta["failed"] = f"{bad} sample rows scored differently in {path.name}"
                atomic_write_json(meta_path, meta)
                raise RuntimeError(f"{bad} sample rows scored differently in {path}; "
                                   "a feature depends on sample membership")
            kept.append(pa.table({"url": frame.column("url"), "shard": frame.column("shard"),
                                  "h": frame.column("h"), "score": pa.array(scores)},
                                 schema=SCORED_SCHEMA).filter(pa.array(scores >= t["tau"])))
            scored += frame.num_rows
        table = pa.concat_tables(kept)
        write_parquet_atomic(table, out / f"shard={shard:03d}.parquet")
        shards[str(shard)] = {"scored": scored, "kept": table.num_rows,
                              **_expected(scored, *share.get(shard, (0, 0)), store.rate)}
        check = meta["kept_check"] = _kept_check(cfg, shards)
        if not check["passed"]:
            meta["failed"] = f"kept share z = {check['z']:.1f}"
        atomic_write_json(meta_path, meta)
        log.info("shard %03d: kept %d of %d rows (%d/%d); kept so far %d, expected %.0f, "
                 "z = %.2f", shard, table.num_rows, scored, i, len(todo), check["kept"],
                 check["expected"], check["z"])
        if not check["passed"]:
            raise RuntimeError(f"the kept share left the sample's estimate after shard "
                               f"{shard}: {check}")
    meta["kept_check"] = _kept_check(cfg, shards)
    meta.setdefault("finished_at", datetime.now(UTC).isoformat())
    atomic_write_json(meta_path, meta)
    return {"shards": len(shards), "scored": sum(v["scored"] for v in shards.values()),
            "kept": sum(v["kept"] for v in shards.values()), "tau": t["tau"],
            "tau_rank": t["tau_rank"]}


def select_full(cfg: Config, work_dir: Path, k: int) -> dict[str, Any]:
    """Writes the exact top-K distinct URLs as a manifest run under `full/selection/`.

    Order: `RANK_ORDER`, the same order as the backtest boundary. A URL found
    in two shards keeps its best score and that row's shard. The crawler's
    `domain_id` comes from the export shard the row was scored in, so a
    consumer can route the page to the crawler process that owns the domain.

    Raises:
        NotEnoughCandidates: If fewer than K distinct URLs were kept.
    """
    scored = work_dir / "full" / "scored"
    meta = read_json(scored / "_meta.json") or {}
    if "finished_at" not in meta:
        raise RuntimeError("scoring is unfinished; rerun score-full")
    if meta.get("failed") or not meta.get("kept_check", {}).get("passed"):
        raise RuntimeError(f"scoring failed its checks ({meta.get('failed')}); "
                           "fix the cause and rerun score-full")
    if k > meta["k"]:
        raise ValueError(f"tau was set for K = {meta['k']}; rerun score-full for K = {k}")
    con = open_duckdb(cfg, work_dir / "duckdb_tmp")
    best = (f"SELECT url, score, shard, h "
            f"FROM read_parquet({sql_str(str(scored / 'shard=*.parquet'))}) "
            "QUALIFY row_number() OVER (PARTITION BY url ORDER BY score DESC, shard ASC) = 1")
    (candidates,) = con.execute(f"SELECT count(*) FROM ({best})").fetchone()
    if candidates < k:
        raise NotEnoughCandidates(f"only {candidates} URLs scored >= tau, need {k}; "
                                  "raise full.tau_margin and rerun score-full")
    con.execute(f"CREATE TEMP TABLE top AS SELECT * FROM ({best}) "
                f"ORDER BY {RANK_ORDER} LIMIT {int(k)}")
    con.execute("CREATE TEMP TABLE top_routed (url VARCHAR, crawl_shard SMALLINT, "
                "domain_id BIGINT, score DOUBLE)")
    export = work_dir / "full" / "export"
    for (shard,) in con.execute("SELECT DISTINCT shard FROM top ORDER BY 1").fetchall():
        files = sql_str(str(export / f"shard={shard:03d}" / "part-*.parquet"))
        con.execute(
            "INSERT INTO top_routed SELECT t.url, t.shard, e.domain_id, t.score "
            f"FROM (SELECT * FROM top WHERE shard = {int(shard)}) t LEFT JOIN "
            f"(SELECT url, min(domain_id) AS domain_id FROM read_parquet({files}, "
            "hive_partitioning = false) GROUP BY url) e USING (url)")
    con.execute("DROP TABLE top")
    run_id = utc_id()
    return write_run(
        cfg, con, "SELECT url, crawl_shard, domain_id, score, NULL::VARCHAR[] AS reason "
                  "FROM top_routed",
        work_dir / "full" / "selection", run_id,
        {"scope": "full", "k": k, "candidates": candidates, "tau": meta["tau"],
         "model_dir": meta["model_dir"], "snapshot_at": meta["snapshot_at"],
         "target_batch": meta["target_batch"], "model_meta": meta["model_meta"],
         "code_version": code_version(), "selected_at": datetime.now(UTC).isoformat()})
