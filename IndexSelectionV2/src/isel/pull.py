"""Read-only pulls from crawlerdb: the split table and the full pool.

Every statement is a single read through `ReadOnlySession`, paced by
`pull.min_interval_s` and held by the read guard. The training sample and the
positives are derived from the full export (`isel.derive`), so they come from
the same read as every scored row. The full export reads each shard in key
ranges, one short `COPY ... (FORMAT CSV)` per range; pyarrow parses the CSV in
C++ and a worker thread writes it while the next range streams. No statement
sorts or aggregates on the server.

The key space depends on `crawlerdb.engine`. On PostgreSQL it is the heap: a
ctid block range is a TID range scan that reads only those blocks. YugabyteDB
has no ctid; a shard table is hash-sharded on `url`, so its key space is
`yb_hash_code(url)` in [0, 65536) and a range is a primary-key scan that only
asks the tablets holding it. Both write the same rows into the same files.
"""

from __future__ import annotations

import logging
import math
import queue
import shutil
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from psycopg import errors

from isel.config import YB_HASH_SPACE, Config
from isel.io import AtomicParquetWriter, atomic_write_json, read_json, write_parquet_atomic
from isel.pg import ByteRate, ReadOnlySession

BOOL, INT2, INT4, INT8, TEXT, FLOAT8, TSTZ = 16, 21, 23, 20, 25, 701, 1184
_ARROW = {BOOL: pa.bool_(), INT2: pa.int16(), INT4: pa.int32(), INT8: pa.int64(),
          TEXT: pa.string(), FLOAT8: pa.float64(), TSTZ: pa.timestamp("us", tz="UTC")}

# (column, SQL expression, type OID). `url_score` and `source` are pulled only
# for the forward-only baselines; `features.FORBIDDEN` keeps both out of models.
CRAWL_COLUMNS: tuple[tuple[str, str, int], ...] = (
    ("url", "url::text", TEXT),
    ("domain_id", "domain_id::int8", INT8),
    ("last_fetch_ok", "last_fetch_ok::timestamptz", TSTZ),
    ("title_len", "char_length(title)::int4", INT4),
    ("has_anchor", "(anchor_text IS NOT NULL)", BOOL),
    ("discovery_source_type", "discovery_source_type::int2", INT2),
    ("robots_bits", "robots_bits::int2", INT2),
    ("is_redirect", "is_redirect::bool", BOOL),
    ("redirect_hop_count", "redirect_hop_count::int4", INT4),
    ("has_json_ld", "has_json_ld::bool", BOOL),
    ("hreflang_count", "hreflang_count::int4", INT4),
    ("has_last_modified", "(last_modified IS NOT NULL)", BOOL),
    ("has_cache_control", "(cache_control IS NOT NULL)", BOOL),
    ("inlink_count_approx", "inlink_count_approx::int8", INT8),
    ("inlink_count_external", "inlink_count_external::int8", INT8),
    ("url_score", "url_score::float8", FLOAT8),
    ("source", "source::int2", INT2),
)
# The full export reads CSV: pyarrow parses it about 15x faster than psycopg decodes
# binary rows, and the server writes CSV no slower. A naive UTC timestamp keeps the text zone-free.
_CSV_EXPRESSIONS = {"last_fetch_ok": "(last_fetch_ok AT TIME ZONE 'UTC')"}
FULL_COLUMNS = tuple((name, _CSV_EXPRESSIONS.get(name, expr), oid)
                     for name, expr, oid in CRAWL_COLUMNS)
FULL_SCHEMA = pa.schema(
    [(name, _ARROW[oid]) for name, _, oid in FULL_COLUMNS] + [("shard", pa.int16())]
)
_CSV_NAMES = [name for name, _, _ in FULL_COLUMNS]
# PostgreSQL CSV: NULL is an unquoted empty field and an empty string is "".
_CSV_CONVERT = pcsv.ConvertOptions(
    column_types={name: (pa.timestamp("us") if oid == TSTZ else _ARROW[oid])
                  for name, _, oid in FULL_COLUMNS},
    true_values=["t"], false_values=["f"], null_values=[""],
    strings_can_be_null=True, quoted_strings_can_be_null=False)
_CSV_PARSE = pcsv.ParseOptions(newlines_in_values=True)
_IO_SQL = ("SELECT w.wal_bytes, w.wal_fpi, (SELECT coalesce(sum(writes), 0) FROM pg_stat_io "
           "WHERE backend_type = 'client backend') FROM pg_stat_wal w")
PAGE_BYTES = 8192
# A shard that hits its timeout or lock_timeout, or keeps failing serialization
# (a YugabyteDB read restart), is retried on the next run.
_RETRYABLE = (errors.QueryCanceled, errors.LockNotAvailable, errors.SerializationFailure)
_sleep = time.sleep

log = logging.getLogger(__name__)


def _shards(cfg: Config, shards: Iterable[int] | None) -> list[int]:
    chosen = list(shards) if shards else list(cfg.pull.shards)
    return chosen or list(range(cfg.crawlerdb.num_shards))


def _table_name(cfg: Config, shard: int) -> str:
    return cfg.crawlerdb.url_table_fmt.format(shard)


def _select_list(cfg: Config, columns: Sequence[tuple[str, str, int]] = CRAWL_COLUMNS) -> str:
    return ", ".join(f"{expr} AS {name}" for name, expr, _ in columns)


def _yugabyte(cfg: Config) -> bool:
    return cfg.crawlerdb.engine == "yugabyte"


def _chunk_span(cfg: Config) -> int:
    """Keys per COPY: heap blocks on PostgreSQL, hash codes on YugabyteDB."""
    return YB_HASH_SPACE // cfg.full.hash_chunks if _yugabyte(cfg) else cfg.full.chunk_blocks


def _key_end(cfg: Config, session: ReadOnlySession, shard: int) -> int:
    """End of a shard's key space; only the PostgreSQL heap grows while it is read."""
    return YB_HASH_SPACE if _yugabyte(cfg) else _relation_blocks(cfg, session, shard)


def _heap_bytes(cfg: Config, lo: int, hi: int) -> int | None:
    """Heap bytes of a key range, known before the read only on PostgreSQL."""
    return None if _yugabyte(cfg) else (hi - lo) * PAGE_BYTES


def _timeout_for_bytes(cfg: Config, heap_bytes: float) -> int:
    g = cfg.guard
    seconds = math.ceil(heap_bytes / (g.min_mb_s * 1_000_000))
    return min(max(seconds, g.timeout_floor_s), g.timeout_cap_s)


def probe_frozen(cfg: Config, session: ReadOnlySession, now: datetime) -> dict[str, Any]:
    """Server start time and cumulative row writes on the URL tables.

    Two equal probes mean no row of `url_state_current_*` was inserted, updated or
    deleted in between. YugabyteDB counts writes per node only, so there the
    count is not recorded.
    """
    if _yugabyte(cfg):
        (started,) = session.fetchone("SELECT pg_postmaster_start_time()")
        return {"postmaster_start": started.isoformat(), "tuple_changes": None,
                "probed_at": now.isoformat()}
    prefix = cfg.crawlerdb.url_table_fmt.split("{", 1)[0]
    started, changes = session.fetchone(
        "SELECT pg_postmaster_start_time(), "
        "coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::int8 "
        "FROM pg_stat_user_tables WHERE starts_with(relname, %s)", (prefix,))
    return {"postmaster_start": started.isoformat(), "tuple_changes": int(changes),
            "probed_at": now.isoformat()}


def pull_dims(cfg: Config, session: ReadOnlySession, out_dir: Path) -> dict[str, Any]:
    """Writes the split-subdomain whitelist (host, migrated_at)."""
    rows = session.fetchall(
        f"SELECT host::text, migrated_at::timestamptz FROM {cfg.crawlerdb.split_table}")
    table = pa.table({"host": [r[0] for r in rows], "migrated_at": [r[1] for r in rows]},
                     schema=pa.schema([("host", pa.string()),
                                       ("migrated_at", pa.timestamp("us", tz="UTC"))]))
    write_parquet_atomic(table, out_dir / "split_hosts.parquet")
    return {"split_hosts": table.num_rows,
            "pending": sum(r[1] is None for r in rows)}


def _relation_blocks(cfg: Config, session: ReadOnlySession, shard: int) -> int:
    """Current heap size of a shard table in blocks (not the stale `relpages`); 0 if missing."""
    (blocks,) = session.fetchone(
        "SELECT coalesce(pg_relation_size(to_regclass(%s)), 0) "
        "/ current_setting('block_size')::int8", (_table_name(cfg, shard),))
    return int(blocks)


def full_sql(cfg: Config, shard: int, lo: int, hi: int) -> str:
    """One COPY over keys [lo, hi) of a shard: heap blocks, or hash codes on YugabyteDB."""
    if _yugabyte(cfg):
        keys = f"yb_hash_code(url) >= {int(lo)} AND yb_hash_code(url) < {int(hi)}"
    else:
        keys = f"ctid >= '({int(lo)},0)'::tid AND ctid < '({int(hi)},0)'::tid"
    return (f"COPY (SELECT {_select_list(cfg, FULL_COLUMNS)} FROM {_table_name(cfg, shard)} "
            f"WHERE {keys} AND last_fetch_ok IS NOT NULL) TO STDOUT (FORMAT CSV)")


def _csv_table(data: bytes | bytearray, shard: int) -> pa.Table:
    """Parses one chunk of `full_sql` output into `FULL_SCHEMA` rows of `shard`."""
    if not data:
        return FULL_SCHEMA.empty_table()
    table = pcsv.read_csv(pa.BufferReader(data), pcsv.ReadOptions(column_names=_CSV_NAMES),
                          _CSV_PARSE, _CSV_CONVERT)
    table = table.append_column("shard", pa.repeat(pa.scalar(shard, pa.int16()),
                                                   table.num_rows))
    return table.cast(FULL_SCHEMA)


def _io_counters(cfg: Config, session: ReadOnlySession) -> dict[str, int]:
    """Cluster-wide WAL and client-backend block writes, to compare a shard with idle time.

    YugabyteDB has no `pg_stat_io` and writes no PostgreSQL WAL, so nothing is counted there.
    """
    if _yugabyte(cfg):
        return {}
    wal_bytes, wal_fpi, writes = session.fetchone(_IO_SQL)
    return {"wal_bytes": int(wal_bytes), "wal_fpi": int(wal_fpi),
            "client_backend_writes": int(writes)}


def estimate_full(cfg: Config, session: ReadOnlySession,
                  shards: Iterable[int] | None = None) -> dict[str, Any]:
    """Reads only catalog sizes: heap bytes, COPY statements and worst-case hours.

    YugabyteDB reports no heap size, so there it counts the COPY statements and
    the tservers the sessions can spread over.
    """
    if _yugabyte(cfg):
        chosen = _shards(cfg, shards)
        (nodes,) = session.fetchone("SELECT count(*) FROM yb_servers()")
        return {"shards": len(chosen), "chunks": len(chosen) * cfg.full.hash_chunks,
                "chunk_timeout_s": cfg.guard.timeout_cap_s, "tservers": int(nodes)}
    blocks = {s: _relation_blocks(cfg, session, s) for s in _shards(cfg, shards)}
    heap = sum(blocks.values()) * PAGE_BYTES
    return {
        "shards": len(blocks), "heap_bytes": heap,
        "chunks": sum(math.ceil(b / cfg.full.chunk_blocks) for b in blocks.values()),
        "chunk_timeout_s": _timeout_for_bytes(cfg, cfg.full.chunk_blocks * PAGE_BYTES),
        "hours_at_min_mb_s": round(heap / (cfg.guard.min_mb_s * 1_000_000) / 3600, 1),
        "largest_shard": max(blocks, key=blocks.get) if blocks else None,
        "blocks": {str(s): b for s, b in blocks.items()},
    }


def _check_disk(cfg: Config, path: Path) -> None:
    free = shutil.disk_usage(path).free
    if free < cfg.full.min_free_gb * 1e9:
        raise RuntimeError(f"only {free / 1e9:.0f} GB free under {path}; "
                           f"full.min_free_gb is {cfg.full.min_free_gb:.0f}")


def _copy_chunk(cfg: Config, session: ReadOnlySession, sql: str, shard: int) -> bytearray:
    """Reads one chunk; a serialization failure discards the partial output and rereads it."""
    for attempt in range(cfg.full.chunk_retries + 1):
        try:
            return session.copy_bytes(sql)
        except errors.SerializationFailure:
            if attempt == cfg.full.chunk_retries:
                raise
            log.warning("shard %03d: serialization failure, rereading the chunk (%d of %d)",
                        shard, attempt + 1, cfg.full.chunk_retries)
            _sleep(2 ** attempt)
    raise AssertionError("unreachable")


def _export_file(cfg: Config, session: ReadOnlySession, final: Path, shard: int,
                 lo: int, hi: int, rate: ByteRate) -> int:
    """Streams keys [lo, hi) of one shard into `final`, one COPY per chunk.

    A worker thread parses and writes each chunk while the next COPY streams;
    pyarrow releases the GIL, so the server rarely waits for this client. On
    PostgreSQL a chunk reserves its heap bytes from `rate` before it reads; on
    YugabyteDB, where the size is unknown, the CSV bytes it returned are charged
    after it.
    """
    span = _chunk_span(cfg)
    start, read = time.monotonic(), 0
    with AtomicParquetWriter(final, FULL_SCHEMA) as writer, ThreadPoolExecutor(1) as pool:
        pending = None
        for c_lo in range(lo, hi, span):
            c_hi = min(c_lo + span, hi)
            heap = _heap_bytes(cfg, c_lo, c_hi)
            if heap is None:
                session.set_statement_timeout(cfg.guard.timeout_cap_s)
            else:
                session.set_statement_timeout(_timeout_for_bytes(cfg, heap))
                rate.wait(heap)
            data = _copy_chunk(cfg, session, full_sql(cfg, shard, c_lo, c_hi), shard)
            read += len(data) if heap is None else heap
            if heap is None:
                rate.wait(len(data))
            if pending is not None:
                pending.result()
            pending = pool.submit(lambda d=data: writer.write(_csv_table(d, shard)))
        if pending is not None:
            pending.result()
    seconds = time.monotonic() - start
    unit = "CSV" if _yugabyte(cfg) else "heap"
    log.info("shard %03d keys %d-%d: %d rows in %.0f s, %.0f MB/s of %s", shard, lo, hi,
             writer.rows, seconds, read / 1e6 / max(seconds, 1e-9), unit)
    return writer.rows


def _export_shard(cfg: Config, session: ReadOnlySession, out_dir: Path, shard: int,
                  extents: list[list[int]], grow: Callable[[int, int], None],
                  rate: ByteRate) -> int:
    """Exports every extent of a shard as files of `chunks_per_file` chunks; skips done files.

    Pages appended while the export runs become one more extent, so rows that
    moved to the end of the heap are still read.
    """
    span = _chunk_span(cfg) * cfg.full.chunks_per_file
    shard_dir = out_dir / f"shard={shard:03d}"
    rows, i = 0, 0
    while i < len(extents):
        lo, hi = extents[i]
        for f_lo in range(lo, hi, span):
            f_hi = min(f_lo + span, hi)
            final = shard_dir / f"part-{f_lo:010d}-{f_hi:010d}.parquet"
            if final.exists():
                rows += pq.ParquetFile(final).metadata.num_rows
                continue
            _check_disk(cfg, out_dir)
            rows += _export_file(cfg, session, final, shard, f_lo, f_hi, rate)
        i += 1
        if i == len(extents) and not _yugabyte(cfg):
            grown = _relation_blocks(cfg, session, shard)
            if grown > extents[-1][1]:
                grow(extents[-1][1], grown)
    return rows


def pull_full(cfg: Config, sessions: Sequence[ReadOnlySession], out_dir: Path,
              shards: Iterable[int] | None = None, now: datetime | None = None,
              rate: ByteRate | None = None) -> dict[str, Any]:
    """Exports every crawled row into `shard=NNN/part-<lo>-<hi>.parquet`, resumably.

    Each session takes whole shards from a shared queue and keeps its own read
    guard; one `ByteRate` caps the bytes all of them read per second. The first
    visit of a shard records its key space in `_meta.json` as an extent; a
    resumed run reuses it, so file ranges never move. The engine is part of the
    layout, so an export is finished on the engine that started it. A shard
    whose COPY hits its timeout or lock_timeout, or fails serialization after
    `full.chunk_retries` rereads, is recorded in `shards_failed` and resumed by
    the next run at file granularity. Any other error stops every
    session after its current shard and is raised. `finished_at` is set once
    every configured shard is done. The pool filter `last_fetch_ok < as_of`
    drops rows fetched after the sample's S0, so a later export still scores
    consistently.
    """
    now = now or datetime.now(UTC)
    first = sessions[0]
    rate = rate or ByteRate(cfg.full.max_heap_mb_s * 1e6)
    out_dir.mkdir(parents=True, exist_ok=True)
    meta_path = out_dir / "_meta.json"
    meta = read_json(meta_path) or {}
    span_key = "hash_chunks" if _yugabyte(cfg) else "chunk_blocks"
    layout = {"engine": cfg.crawlerdb.engine, span_key: getattr(cfg.full, span_key),
              "chunks_per_file": cfg.full.chunks_per_file}
    if meta and any(meta.get(k) != v for k, v in layout.items()):
        raise RuntimeError(f"the existing export used a different crawlerdb.engine, "
                           f"full.{span_key} or full.chunks_per_file; "
                           "finish it with the same config")
    if "started_at" not in meta:
        meta.update(layout, started_at=now.isoformat(),
                    frozen_probe=probe_frozen(cfg, first, now))
    extents: dict[str, list[list[int]]] = meta.setdefault("extents", {})
    done = set(meta.get("shards_done", []))
    failed: dict[str, str] = meta.setdefault("shards_failed", {})
    meta.setdefault("shards_done", sorted(done))
    todo: queue.Queue[int] = queue.Queue()
    for shard in _shards(cfg, shards):
        if shard not in done:
            todo.put(shard)
    total = todo.qsize()
    lock = threading.Lock()   # guards `meta` and its file
    stop = threading.Event()

    def save() -> None:
        with lock:
            atomic_write_json(meta_path, meta)

    def export(session: ReadOnlySession, shard: int) -> None:
        key = str(shard)
        if key not in extents:
            end = _key_end(cfg, session, shard)
            with lock:
                extents[key] = [[0, end]]
            save()

        def grow(lo: int, hi: int) -> None:
            with lock:
                extents[key].append([lo, hi])
            save()

        before, start = _io_counters(cfg, session), time.monotonic()
        try:
            rows = _export_shard(cfg, session, out_dir, shard, extents[key], grow, rate)
        except _RETRYABLE as exc:
            with lock:
                failed[key] = type(exc).__name__
            log.warning("shard %03d failed (%s); it resumes on the next run",
                        shard, type(exc).__name__)
        else:
            after = _io_counters(cfg, session)
            io = {"seconds": round(time.monotonic() - start),
                  **{k: after[k] - before[k] for k in after}}
            with lock:
                done.add(shard)
                failed.pop(key, None)
                meta.setdefault("rows", {})[key] = rows
                meta.setdefault("io", {})[key] = io
                meta["shards_done"] = sorted(done)
                finished = len(done)
            log.info("shard %03d: %d crawled rows (%d done in total, %d queued this run); "
                     "cluster-wide during it: %s", shard, rows, finished, total, io)
        save()

    def work(session: ReadOnlySession) -> None:
        try:
            while not stop.is_set():
                try:
                    shard = todo.get_nowait()
                except queue.Empty:
                    return
                export(session, shard)
        except BaseException:
            stop.set()
            raise

    pa.set_cpu_count(cfg.full.parse_threads)
    with ThreadPoolExecutor(len(sessions), thread_name_prefix="pull-full") as pool:
        futures = [pool.submit(work, session) for session in sessions]
    for future in futures:
        future.result()
    missing = sorted(set(_shards(cfg, None)) - done)
    if missing:
        meta.pop("finished_at", None)
    elif "finished_at" not in meta:
        finished_at = datetime.now(UTC)
        meta.update(finished_at=finished_at.isoformat(),
                    end_probe=probe_frozen(cfg, first, finished_at))
    save()
    return {"shards_done": len(done), "shards_missing": len(missing),
            "shards_failed": sorted(int(s) for s in failed),
            "rows": sum(meta.get("rows", {}).values())}
