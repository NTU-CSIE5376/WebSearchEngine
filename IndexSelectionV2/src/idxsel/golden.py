"""Golden set: past-golden URLs and a time-decayed golden prior per domain.

Only batches strictly before the target batch feed the prior and the
past-golden tier, so a backtest on batch N never sees batch N.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from idxsel.config import Config
from idxsel.duck import copy_atomic, sql_str
from idxsel.manifest import atomic_write_json, write_parquet_atomic
from idxsel.pg import ReadOnlySession
from idxsel.urlnorm import DomainKeyer, canonicalize

GOLDEN_SQL = (
    "SELECT b.id, b.created_at, q.id, q.tags, u.url "
    "FROM metric_url u JOIN metric_queries q ON q.id = u.query_id "
    "JOIN metric_batches b ON b.id = q.batch_id WHERE u.url IS NOT NULL"
)


@dataclass(frozen=True)
class Target:
    batch_id: int | None
    as_of: datetime


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def resolve_target(cfg: Config, batches: dict[int, datetime], as_of_hint: datetime) -> Target:
    """Returns the batch being predicted and the reference time for decay."""
    tb = cfg.golden.target_batch
    if tb == "next":
        return Target(None, _utc(as_of_hint))
    if tb not in batches:
        raise ValueError(f"golden.target_batch {tb} is not in metricdb")
    return Target(int(tb), batches[int(tb)])


def build_golden(cfg: Config, metric: ReadOnlySession, dims_dir: Path, out_dir: Path,
                 con: duckdb.DuckDBPyConnection, as_of_hint: datetime) -> dict[str, Any]:
    """Reads the golden set, maps URLs to crawler domain ids, writes priors.

    Outputs in `out_dir`: `golden.parquet` (all golden rows), `past_golden.parquet`
    (canonical URLs before the target, with the newest raw spelling),
    `domain_prior.parquet`, `meta.json`.
    """
    rows = metric.fetchall(GOLDEN_SQL)
    split_hosts = pq.read_table(dims_dir / "split_hosts.parquet")["host"].to_pylist()
    keyer = DomainKeyer(split_hosts, offline=cfg.golden.tldextract_offline)
    batch_created = {int(r[0]): _utc(r[1]) for r in rows}
    target = resolve_target(cfg, batch_created, as_of_hint)
    golden = pa.table({
        "batch_id": pa.array([int(r[0]) for r in rows], pa.int64()),
        "batch_created_at": pa.array([_utc(r[1]) for r in rows], pa.timestamp("us", tz="UTC")),
        "query_id": pa.array([int(r[2]) for r in rows], pa.int64()),
        "tags": pa.array([list(r[3] or []) for r in rows], pa.list_(pa.string())),
        "raw_url": pa.array([r[4] for r in rows], pa.string()),
        "canonical_url": pa.array([canonicalize(r[4]) for r in rows], pa.string()),
        "domain_key": pa.array([keyer.key(r[4]) for r in rows], pa.string()),
    })
    raw_path = out_dir / "golden_raw.parquet"
    write_parquet_atomic(golden, raw_path)
    domains = sql_str(str(dims_dir / "domain_state.parquet"))
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE golden AS SELECT g.*, d.domain_id "
        f"FROM read_parquet({sql_str(str(raw_path))}) g "
        f"LEFT JOIN read_parquet({domains}) d ON d.domain = g.domain_key"
    )
    before = (
        f"batch_id < {target.batch_id}" if target.batch_id is not None else "TRUE"
    ) + f" AND batch_created_at <= {sql_str(target.as_of.isoformat())}::TIMESTAMPTZ"
    half_life = float(cfg.golden.prior_half_life_days)
    copy_atomic(con, "SELECT * FROM golden", out_dir / "golden.parquet")
    raw_path.unlink()
    copy_atomic(
        con,
        f"SELECT canonical_url, first(raw_url ORDER BY batch_created_at DESC, raw_url) AS raw_url "
        f"FROM golden WHERE {before} GROUP BY canonical_url",
        out_dir / "past_golden.parquet")
    prior = out_dir / "domain_prior.parquet"
    copy_atomic(
        con,
        f"SELECT domain_id, sum(n * pow(0.5, age_days / {half_life})) AS prior FROM ("
        f"  SELECT domain_id, batch_id, count(DISTINCT canonical_url) AS n,"
        f"    date_diff('second', any_value(batch_created_at),"
        f"              {sql_str(target.as_of.isoformat())}::TIMESTAMPTZ) / 86400.0 AS age_days"
        f"  FROM golden WHERE {before} AND domain_id IS NOT NULL GROUP BY domain_id, batch_id"
        f") GROUP BY domain_id",
        prior)
    stats = con.execute(
        f"SELECT count(*), count(DISTINCT batch_id), "
        f"count(*) FILTER (WHERE domain_id IS NULL), "
        f"count(DISTINCT canonical_url) FILTER (WHERE {before}) FROM golden"
    ).fetchone()
    meta = {
        "target_batch": target.batch_id,
        "as_of": target.as_of,
        "rows": stats[0],
        "batches": stats[1],
        "rows_without_domain": stats[2],
        "past_golden_urls": stats[3],
        "prior_domains": pq.ParquetFile(prior).metadata.num_rows,
        "batches_used": sorted(b for b, t in batch_created.items()
                               if (target.batch_id is None or b < target.batch_id)
                               and t <= target.as_of),
    }
    atomic_write_json(out_dir / "meta.json", meta)
    return meta
