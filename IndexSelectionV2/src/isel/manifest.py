"""Selection manifest: the contract between index selection and its consumers.

Layout of one run (`<selection_dir>/run=<id>/`):

* `manifest/index_shard=NN.parquet`: one row per selected URL, `MANIFEST_SCHEMA`,
  ordered by rank.
* `deltas/added.parquet`, `deltas/removed.parquet`: doc ids against the parent run.
* `_MANIFEST.json`: schema version, parent run, row counts and sha256 per file,
  plus the caller's run facts; written last, so a run without it is incomplete.

`url` is the spelling the metric matches. `doc_id` is the low 64 bits of MD5
of the canonical URL, so it stays the same for a page across runs and several
spellings of one page share it.
`host`, `domain_id` and `crawl_shard` route a page to the crawler process that
owns its domain.

Rows go through DuckDB so a 128M-row run never sits in Python lists. Only
`canonicalize` runs in Python, in a process pool, and only the spellings it
changes come back.
"""

from __future__ import annotations

import hashlib
import multiprocessing
import os
from collections import deque
from collections.abc import Iterator, Mapping
from concurrent.futures import Future, ProcessPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from isel.config import Config
from isel.io import atomic_write_json, copy_atomic, read_json, sql_str
from isel.urlnorm import HOST_RE, canonicalize

MANIFEST_SCHEMA = pa.schema([
    ("doc_id", pa.uint64()),
    ("url", pa.string()),
    ("canonical_url", pa.string()),
    ("host", pa.string()),
    ("domain_id", pa.int64()),
    ("crawl_shard", pa.int16()),
    ("index_shard", pa.int16()),
    ("score", pa.float64()),
    ("rank", pa.int64()),
    ("reason", pa.list_(pa.string())),
    ("content_hash", pa.string()),
    ("simhash", pa.uint64()),
    ("lang", pa.string()),
    ("text_len", pa.int32()),
    ("run_id", pa.string()),
    ("schema_version", pa.int32()),
])
DELTA_SCHEMA = pa.schema([("doc_id", pa.uint64()), ("url", pa.string()),
                          ("canonical_url", pa.string())])
# The one ranking order: score, then the URL hash `h = md5_number_lower(url)`, then the URL.
RANK_ORDER = "score DESC, h ASC, url ASC"
ROW_COLUMNS = ("url", "crawl_shard", "domain_id", "score", "reason")


class ManifestError(ValueError):
    """Raised when a manifest is inconsistent or fails validation."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def to_pg_bigint(doc_ids: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Reinterprets uint64 doc ids as the two's-complement BIGINT that selectdb stores."""
    return pc.cast(doc_ids, pa.int64(), safe=False)


def latest_run(selection_dir: Path) -> Path | None:
    """The newest complete run (one with `_MANIFEST.json`)."""
    runs = sorted(p for p in selection_dir.glob("run=*") if (p / "_MANIFEST.json").exists())
    return runs[-1] if runs else None


def _respelled(urls: list[str]) -> list[tuple[str, str]]:
    """(url, canonical_url) for the URLs that `canonicalize` changes."""
    return [(u, c) for u in urls if (c := canonicalize(u)) != u]


@contextmanager
def _worker_env() -> Iterator[None]:
    """Starts worker processes with one OpenBLAS thread; restores the parent's env after.

    A spawned worker imports numpy, and OpenBLAS starts up to 64 threads in it.
    Sixteen such workers broke the container's 512-pid limit on Host2.
    """
    saved = os.environ.get("OPENBLAS_NUM_THREADS")
    os.environ["OPENBLAS_NUM_THREADS"] = "1"
    try:
        yield
    finally:
        if saved is None:
            del os.environ["OPENBLAS_NUM_THREADS"]
        else:
            os.environ["OPENBLAS_NUM_THREADS"] = saved


def _canonical_pairs(cfg: Config, con: duckdb.DuckDBPyConnection, rows_sql: str) -> pa.Table:
    """Runs `canonicalize` over every URL, at most two batches per worker in flight."""
    m = cfg.manifest
    reader = con.execute(f"SELECT url FROM ({rows_sql})").to_arrow_reader(
        m.canonical_batch_rows)
    batches = (b.column(0).to_pylist() for b in reader)
    pairs: list[tuple[str, str]] = []
    if m.canonical_workers == 1:
        for urls in batches:
            pairs += _respelled(urls)
    else:
        # spawn: the parent holds DuckDB threads, which fork would copy mid-flight.
        context = multiprocessing.get_context("spawn")
        with _worker_env(), ProcessPoolExecutor(m.canonical_workers,
                                                mp_context=context) as pool:
            pending: deque[Future] = deque()
            for urls in batches:
                pending.append(pool.submit(_respelled, urls))
                if len(pending) >= 2 * m.canonical_workers:
                    pairs += pending.popleft().result()
            while pending:
                pairs += pending.popleft().result()
    return pa.table({"url": pa.array([p[0] for p in pairs], pa.string()),
                     "canonical_url": pa.array([p[1] for p in pairs], pa.string())})


def _build(cfg: Config, con: duckdb.DuckDBPyConnection, rows_sql: str, run_id: str) -> int:
    """Fills the temp table `manifest_rows` and checks it; returns its row count."""
    con.register("canon_pairs", _canonical_pairs(cfg, con, rows_sql))
    try:
        con.execute(f"""
CREATE OR REPLACE TEMP TABLE manifest_rows AS
WITH r AS (
  SELECT s.url, coalesce(p.canonical_url, s.url) AS canonical_url, s.crawl_shard,
         s.domain_id, s.score, s.reason, md5_number_lower(s.url) AS h
  FROM ({rows_sql}) s LEFT JOIN canon_pairs p ON p.url = s.url
)
SELECT md5_number_lower(canonical_url) AS doc_id, url, canonical_url,
       lower(regexp_extract(canonical_url, {sql_str(HOST_RE)}, 1)) AS host,
       domain_id::BIGINT AS domain_id, crawl_shard::SMALLINT AS crawl_shard,
       (md5_number_lower(canonical_url) % {int(cfg.manifest.index_shards)})::SMALLINT
           AS index_shard,
       score::DOUBLE AS score, row_number() OVER (ORDER BY {RANK_ORDER}) AS rank,
       reason::VARCHAR[] AS reason, NULL::VARCHAR AS content_hash, NULL::UBIGINT AS simhash,
       NULL::VARCHAR AS lang, NULL::INTEGER AS text_len, {sql_str(run_id)} AS run_id,
       {int(cfg.manifest.schema_version)}::INTEGER AS schema_version
FROM r""")
    finally:
        con.unregister("canon_pairs")
    names = [row[0] for row in con.execute("DESCRIBE manifest_rows").fetchall()]
    if names != MANIFEST_SCHEMA.names:
        raise ManifestError(f"manifest columns {names} differ from MANIFEST_SCHEMA")
    rows, urls, missing = con.execute(
        "SELECT count(*), count(DISTINCT url), count(*) FILTER (WHERE domain_id IS NULL "
        "OR crawl_shard IS NULL OR host = '') FROM manifest_rows").fetchone()
    if urls != rows:
        raise ManifestError(f"{rows - urls} duplicate URLs in the selection")
    if missing:
        raise ManifestError(f"{missing} rows lack a domain_id, crawl_shard or host")
    (collisions,) = con.execute(
        "SELECT count(*) FROM (SELECT doc_id FROM manifest_rows GROUP BY doc_id "
        "HAVING count(DISTINCT canonical_url) > 1)").fetchone()
    if collisions:
        raise ManifestError(f"{collisions} doc_ids are shared by different canonical URLs")
    return int(rows)


def _delta(side: str, other: str) -> str:
    """Docs of `side` missing from `other`, one spelling per doc (the best ranked)."""
    return (f"SELECT doc_id, arg_min(url, rank) AS url, arg_min(canonical_url, rank) "
            f"AS canonical_url FROM {side} WHERE doc_id NOT IN (SELECT doc_id FROM {other}) "
            "GROUP BY doc_id ORDER BY doc_id")


def write_run(cfg: Config, con: duckdb.DuckDBPyConnection, rows_sql: str, selection_dir: Path,
              run_id: str, extra_meta: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Writes one run plus deltas against the newest complete run; returns its meta.

    Args:
        cfg: Config; `manifest` sets the shards, the version and the canonicalize pool.
        con: DuckDB connection that can evaluate `rows_sql`.
        rows_sql: A query with `ROW_COLUMNS`, one row per selected URL. The
            writer ranks the rows by `RANK_ORDER`.
        selection_dir: Directory holding this scope's runs.
        run_id: The new run's id.
        extra_meta: Run facts copied into `_MANIFEST.json`.

    Raises:
        ManifestError: If the run exists, URLs repeat, a row lacks routing
            columns, or two canonical URLs share a doc_id.
    """
    parent = latest_run(selection_dir)
    out = selection_dir / f"run={run_id}"
    if out.exists():
        raise ManifestError(f"{out} already exists")
    rows = _build(cfg, con, rows_sql, run_id)
    files: dict[str, dict[str, Any]] = {}

    def put(rel: str, query: str) -> None:
        path = out / rel
        files[rel] = {"rows": copy_atomic(con, query, path), "sha256": _sha256(path)}

    for shard in range(cfg.manifest.index_shards):
        put(f"manifest/index_shard={shard:02d}.parquet",
            f"SELECT * FROM manifest_rows WHERE index_shard = {shard} ORDER BY rank")
    if parent is None:
        con.execute("CREATE OR REPLACE TEMP VIEW parent_rows AS SELECT * FROM manifest_rows "
                    "WHERE false")
    else:
        con.execute("CREATE OR REPLACE TEMP VIEW parent_rows AS SELECT * FROM read_parquet("
                    f"{sql_str(str(parent / 'manifest' / '*.parquet'))})")
    put("deltas/added.parquet", _delta("manifest_rows", "parent_rows"))
    put("deltas/removed.parquet", _delta("parent_rows", "manifest_rows"))
    (boundary,) = con.execute("SELECT min(score) FROM manifest_rows").fetchone()
    meta = {
        "schema_version": cfg.manifest.schema_version,
        "run_id": run_id,
        "parent_run_id": None if parent is None else parent.name.removeprefix("run="),
        "rows": rows,
        "docs": con.execute("SELECT count(DISTINCT doc_id) FROM manifest_rows").fetchone()[0],
        "boundary_score": boundary,
        "index_shards": cfg.manifest.index_shards,
        "config_sha256": cfg.sha256(),
        "files": files,
        **(extra_meta or {}),
    }
    con.execute("DROP VIEW parent_rows")
    con.execute("DROP TABLE manifest_rows")
    atomic_write_json(out / "_MANIFEST.json", meta)
    return meta


def validate_run(run_dir: Path) -> dict[str, Any]:
    """Checks every file's checksum and row count against `_MANIFEST.json`."""
    meta = read_json(run_dir / "_MANIFEST.json")
    if meta is None:
        raise ManifestError(f"{run_dir} has no _MANIFEST.json")
    total = 0
    for rel, info in meta["files"].items():
        path = run_dir / rel
        if _sha256(path) != info["sha256"]:
            raise ManifestError(f"checksum mismatch: {rel}")
        if pq.ParquetFile(path).metadata.num_rows != info["rows"]:
            raise ManifestError(f"row count mismatch: {rel}")
        if rel.startswith("manifest/"):
            total += info["rows"]
    if total != meta["rows"]:
        raise ManifestError("manifest partitions do not add up to the run's row count")
    return meta
