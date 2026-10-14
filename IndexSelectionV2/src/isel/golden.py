"""Golden set: every batch, query and organic URL the official metric measures.

Official rows come from metricdb. Pseudo rows (`label_source = "pseudo"`) are
built by `pseudo.py` from our own SERP fetches; they train models and feed
features but are never evaluated. `available_at` is when a row could first be
known: the batch time for official rows, the SERP fetch time for pseudo rows.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from isel.io import atomic_write_json, write_parquet_atomic
from isel.pg import ReadOnlySession
from isel.urlnorm import canonicalize, host_of

GOLDEN_SQL = (
    "SELECT b.id, b.created_at, q.id, q.keyword, q.frequency, q.tags, u.rank, u.url "
    "FROM metric_url u JOIN metric_queries q ON q.id = u.query_id "
    "JOIN metric_batches b ON b.id = q.batch_id WHERE u.url IS NOT NULL"
)

POOL_SQL = (
    "SELECT b.id, b.created_at, q.id, q.keyword, q.frequency, q.tags "
    "FROM metric_queries q JOIN metric_batches b ON b.id = q.batch_id"
)
OFFICIAL = "official"
PSEUDO = "pseudo"
_TS = pa.timestamp("us", tz="UTC")

GOLDEN_SCHEMA = pa.schema([
    ("batch_id", pa.int64()),
    ("batch_created_at", _TS),
    ("query_id", pa.int64()),
    ("keyword", pa.string()),
    ("frequency", pa.int64()),
    ("tags", pa.list_(pa.string())),
    ("rank", pa.int32()),
    ("raw_url", pa.string()),
    ("canonical_url", pa.string()),
    ("host", pa.string()),
    ("available_at", _TS),
    ("label_source", pa.string()),
])

# The trending pool has the query columns of the golden set, one row per query.
POOL_SCHEMA = pa.schema([GOLDEN_SCHEMA.field(n) for n in (
    "batch_id", "batch_created_at", "query_id", "keyword", "frequency", "tags")])


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def _query_columns(rows: list[tuple]) -> dict[str, list[Any]]:
    """The `POOL_SCHEMA` columns of rows that start (batch_id, created_at, query_id, ...)."""
    return {
        "batch_id": [int(r[0]) for r in rows],
        "batch_created_at": [_utc(r[1]) for r in rows],
        "query_id": [int(r[2]) for r in rows],
        "keyword": [r[3] for r in rows],
        "frequency": [None if r[4] is None else int(r[4]) for r in rows],
        "tags": [list(r[5] or []) for r in rows],
    }


def golden_table(rows: list[tuple], label_source: str = OFFICIAL,
                 available_at: datetime | None = None) -> pa.Table:
    """Builds the golden table from `GOLDEN_SQL`-shaped rows.

    Args:
        rows: (batch_id, created_at, query_id, keyword, frequency, tags, rank, url).
        label_source: `official` or `pseudo`.
        available_at: When pseudo rows were fetched; official rows use the batch time.
    """
    if (label_source == PSEUDO) != (available_at is not None):
        raise ValueError("pseudo rows need available_at; official rows must not set it")
    return pa.table({
        **_query_columns(rows),
        "rank": [None if r[6] is None else int(r[6]) for r in rows],
        "raw_url": [r[7] for r in rows],
        "canonical_url": [canonicalize(r[7]) for r in rows],
        "host": [host_of(r[7]) for r in rows],
        "available_at": [_utc(available_at or r[1]) for r in rows],
        "label_source": [label_source] * len(rows),
    }, schema=GOLDEN_SCHEMA)


def pool_table(rows: list[tuple]) -> pa.Table:
    """Builds the trending-query pool from `POOL_SQL` rows (selected or not)."""
    return pa.table(_query_columns(rows), schema=POOL_SCHEMA)


def summarize(golden: pa.Table, tags: list[str]) -> dict[str, Any]:
    """Per (batch, tag) counts, spelling drift and URL recurrence across batches."""
    rows = golden.to_pylist()
    created = {r["batch_id"]: r["batch_created_at"] for r in rows}
    canon_by_batch: dict[int, set[str]] = defaultdict(set)
    for r in rows:
        canon_by_batch[r["batch_id"]].add(r["canonical_url"])
    out: dict[str, Any] = {}
    seen: set[str] = set()
    for batch in sorted(created):
        current = canon_by_batch[batch]
        entry: dict[str, Any] = {
            "created_at": created[batch],
            "distinct_canonical": len(current),
            "recurring_canonical_rate": len(current & seen) / len(current) if current else None,
        }
        for tag in tags:
            sel = [r for r in rows if r["batch_id"] == batch and tag in r["tags"]]
            raw = {r["raw_url"]: r["canonical_url"] for r in sel}
            entry[tag] = {
                "queries": len({r["query_id"] for r in sel}),
                "distinct_raw_urls": len(raw),
                "raw_ne_canonical_rate": (sum(u != c for u, c in raw.items()) / len(raw)
                                          if raw else None),
            }
        out[str(batch)] = entry
        seen |= current
    return out


def pull_golden(metric: ReadOnlySession, out_dir: Path, tags: list[str]) -> dict[str, Any]:
    """Writes `golden.parquet`, `pool.parquet` and `summary.json`; returns the summary."""
    table = golden_table(metric.fetchall(GOLDEN_SQL))
    write_parquet_atomic(table, out_dir / "golden.parquet")
    write_parquet_atomic(pool_table(metric.fetchall(POOL_SQL)), out_dir / "pool.parquet")
    summary = summarize(table, tags)
    atomic_write_json(out_dir / "summary.json", summary)
    return summary


def pseudo_dir(work_dir: Path) -> Path:
    return work_dir / "golden" / "pseudo"


def pseudo_batch_path(work_dir: Path, batch_id: int) -> Path:
    """`golden/pseudo/batch=<id>.parquet`; its metadata sits next to it as `.json`."""
    return pseudo_dir(work_dir) / f"batch={batch_id}.parquet"


def golden_files(work_dir: Path, include_pseudo: bool) -> list[Path]:
    """The official golden file, then every pseudo batch file if requested."""
    files = [work_dir / "golden" / "golden.parquet"]
    if include_pseudo:
        files += sorted(pseudo_dir(work_dir).glob("batch=*.parquet"))
    return files


def read_golden(work_dir: Path, include_pseudo: bool = False) -> pa.Table:
    """Golden rows; pseudo rows only when `include_pseudo` (never for evaluation)."""
    return pa.concat_tables([pq.read_table(f, schema=GOLDEN_SCHEMA)
                             for f in golden_files(work_dir, include_pseudo)])
