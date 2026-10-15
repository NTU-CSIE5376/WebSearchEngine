"""The training sample and the positives, derived from the full export.

Every row that trains, evaluates or gets scored then comes from one read. A row
joins the sample when a salted hash of (url, shard) falls under the sample
rate, so each row's membership is independent of every other row's. The old
`TABLESAMPLE SYSTEM` sample took whole heap pages; rows on one page often share
a domain, so a sampled row found its page neighbours in its own domain
statistics and an unsampled row never did (docs/DESIGN.md §3).
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from isel.config import Config
from isel.features import FORBIDDEN, ROW_COLUMNS, SAMPLE_METHOD
from isel.golden import read_golden
from isel.io import atomic_write_json, copy_atomic, open_duckdb, read_json, sql_str
from isel.pull import FULL_SCHEMA

_PPM = 1_000_000
_COLUMNS = FULL_SCHEMA.names
# Copies that differ here get different features, and so different scores.
_FEATURE_INPUTS = [c for c in ROW_COLUMNS if c not in ("url", "shard") and c not in FORBIDDEN]


def _export(work_dir: Path) -> tuple[Path, dict[str, Any]]:
    export = work_dir / "full" / "export"
    meta = read_json(export / "_meta.json") or {}
    if "finished_at" not in meta:
        raise RuntimeError("the full export is unfinished; rerun pull-full until none is missing")
    return export, meta


def _read(files: list[Path]) -> str:
    names = ", ".join(sql_str(str(f)) for f in files)
    return f"read_parquet([{names}], hive_partitioning = false)"


def _one_per(keys: str, rows_sql: str) -> str:
    """One row per `keys`; a row moved during the export can be read twice.

    The order covers every column, so the same copy wins on every run.
    """
    order = ", ".join(f"{c} DESC NULLS LAST" for c in _COLUMNS if c not in ("url", "shard"))
    return (f"SELECT * FROM ({rows_sql}) "
            f"QUALIFY row_number() OVER (PARTITION BY {keys} ORDER BY {order}) = 1")


def derive_sample(cfg: Config, work_dir: Path) -> dict[str, Any]:
    """Writes `sample/shard=NNN.parquet` and `_meta.json` from the full export.

    `in_agg` splits the sample by a second salted hash: that half only feeds the
    domain statistics, and the other half gives negatives, the eval pool and tau.
    The meta counts sample keys read more than once, and those whose copies
    differ in a feature input; `score_full` refuses a sample with any of the latter.

    Raises:
        RuntimeError: If the export is unfinished or `sample/` already holds a sample.
    """
    export, exported = _export(work_dir)
    out = work_dir / "sample"
    if (out / "_meta.json").exists():
        raise RuntimeError(f"{out} already holds a sample; move it away first")
    ppm = round(cfg.full.sample_percent * _PPM / 100)
    member = (f"md5_number_lower(url || ':' || shard::VARCHAR || "
              f"{sql_str(':' + cfg.full.sample_salt)}) % {_PPM} < {ppm}")
    in_agg = (f"md5_number_lower(url || ':' || shard::VARCHAR || "
              f"{sql_str(':' + cfg.full.agg_salt)}) % {_PPM} < {round(cfg.full.agg_share * _PPM)}")
    cols = ", ".join(_COLUMNS)
    con = open_duckdb(cfg, work_dir / "duckdb_tmp")
    rows: dict[str, int] = {}
    latest = []
    duplicates, differing = 0, 0
    for shard in sorted(exported["shards_done"]):
        files = sorted((export / f"shard={shard:03d}").glob("part-*.parquet"))
        if not files:
            continue
        source = _read(files)
        rows[str(shard)] = copy_atomic(
            con, f"SELECT *, {in_agg} AS in_agg FROM ("
            + _one_per("url, shard", f"SELECT {cols} FROM {source} WHERE {member}")
            + ") ORDER BY url", out / f"shard={shard:03d}.parquet")
        latest.append(con.execute(f"SELECT max(last_fetch_ok) FROM {source}").fetchone()[0])
        copies, apart = con.execute(
            "SELECT count(*) FILTER (WHERE n > 1), count(*) FILTER (WHERE d > 1) FROM ("
            f"SELECT count(*) AS n, count(DISTINCT row({', '.join(_FEATURE_INPUTS)})) AS d "
            f"FROM {source} WHERE {member} GROUP BY url, shard)").fetchone()
        duplicates += copies
        differing += apart
    newest = max((t for t in latest if t is not None), default=None)
    meta = {"method": SAMPLE_METHOD, "sample_percent": cfg.full.sample_percent,
            "sample_salt": cfg.full.sample_salt, "agg_share": cfg.full.agg_share,
            "agg_salt": cfg.full.agg_salt, "snapshot_at": exported["started_at"],
            "export_started_at": exported["started_at"],
            "frozen_probe": exported.get("frozen_probe"), "rows": rows,
            "shards_done": sorted(int(s) for s in rows),
            "max_last_fetch_ok": newest.isoformat() if newest else None,
            "duplicate_keys": duplicates, "differing_duplicates": differing,
            "finished_at": datetime.now(UTC).isoformat()}
    atomic_write_json(out / "_meta.json", meta)
    return {"rows": sum(rows.values()), "shards": len(rows),
            "max_last_fetch_ok": meta["max_last_fetch_ok"],
            "differing_duplicates": differing}


def derive_positives(cfg: Config, work_dir: Path,
                     batches: Iterable[int] | None = None) -> dict[str, int]:
    """Writes `positives/batch=<id>.parquet`: export rows whose URL is a golden spelling.

    The join covers every shard, so a URL stored in two shards keeps both rows.

    Raises:
        RuntimeError: If the export is unfinished or `positives/` came from another export.
        ValueError: If a requested batch is not in the golden set of this config.
    """
    export, exported = _export(work_dir)
    out = work_dir / "positives"
    meta_path = out / "_meta.json"
    meta = read_json(meta_path) or {}
    if not meta and any(out.glob("batch=*.parquet")):
        raise RuntimeError(f"{out} holds positives with no _meta.json; move it away first")
    if meta and meta.get("export_started_at") != exported["started_at"]:
        raise RuntimeError(f"{out} holds positives from another export; move it away first")
    golden = read_golden(work_dir, include_pseudo=cfg.golden.use_pseudo)
    ids = golden.column("batch_id").to_pylist()
    wanted = set(batches) if batches else set(ids)
    unknown = sorted(wanted - set(ids))
    if unknown:
        raise ValueError(f"batches {unknown} are not in the golden set; pseudo batches "
                         "need config/pseudo.toml")
    pairs = {(b, u) for b, raw, canon in zip(ids, golden.column("raw_url").to_pylist(),
                                             golden.column("canonical_url").to_pylist(),
                                             strict=True)
             if b in wanted for u in (raw, canon)}
    con = open_duckdb(cfg, work_dir / "duckdb_tmp")
    con.register("spellings", pa.table({"batch_id": pa.array([b for b, _ in pairs], pa.int64()),
                                        "url": pa.array([u for _, u in pairs], pa.string())}))
    cols = ", ".join(f"e.{c}" for c in _COLUMNS)
    files = sorted(export.glob("shard=*/part-*.parquet"))
    con.execute("CREATE TEMP TABLE hits AS " + _one_per(
        "batch_id, url, shard",
        f"SELECT s.batch_id, {cols} FROM {_read(files)} e JOIN spellings s ON s.url = e.url"))
    report = {}
    for b in sorted(wanted):
        report[str(b)] = copy_atomic(
            con, f"SELECT {', '.join(_COLUMNS)} FROM hits WHERE batch_id = {int(b)} "
                 "ORDER BY url, shard", out / f"batch={b}.parquet")
    atomic_write_json(meta_path, {"export_started_at": exported["started_at"],
                                  "batches": sorted(set(meta.get("batches", [])) | wanted)})
    return report
