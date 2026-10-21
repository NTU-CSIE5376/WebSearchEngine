"""Golden set: every batch, query and organic URL the official metric measures."""

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

GOLDEN_SCHEMA = pa.schema([
    ("batch_id", pa.int64()),
    ("batch_created_at", pa.timestamp("us", tz="UTC")),
    ("query_id", pa.int64()),
    ("keyword", pa.string()),
    ("frequency", pa.int64()),
    ("tags", pa.list_(pa.string())),
    ("rank", pa.int32()),
    ("raw_url", pa.string()),
    ("canonical_url", pa.string()),
    ("host", pa.string()),
])


def _utc(ts: datetime) -> datetime:
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def golden_table(rows: list[tuple]) -> pa.Table:
    """Builds the golden table from `GOLDEN_SQL` rows."""
    return pa.table({
        "batch_id": [int(r[0]) for r in rows],
        "batch_created_at": [_utc(r[1]) for r in rows],
        "query_id": [int(r[2]) for r in rows],
        "keyword": [r[3] for r in rows],
        "frequency": [None if r[4] is None else int(r[4]) for r in rows],
        "tags": [list(r[5] or []) for r in rows],
        "rank": [None if r[6] is None else int(r[6]) for r in rows],
        "raw_url": [r[7] for r in rows],
        "canonical_url": [canonicalize(r[7]) for r in rows],
        "host": [host_of(r[7]) for r in rows],
    }, schema=GOLDEN_SCHEMA)


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
    """Writes `golden.parquet` and `summary.json` under `out_dir`; returns the summary."""
    table = golden_table(metric.fetchall(GOLDEN_SQL))
    write_parquet_atomic(table, out_dir / "golden.parquet")
    summary = summarize(table, tags)
    atomic_write_json(out_dir / "summary.json", summary)
    return summary


def golden_path(work_dir: Path) -> Path:
    return work_dir / "golden" / "golden.parquet"


def read_golden(work_dir: Path) -> pa.Table:
    # The schema drops columns that older pulls wrote and the code no longer reads.
    return pq.read_table(golden_path(work_dir), schema=GOLDEN_SCHEMA)
