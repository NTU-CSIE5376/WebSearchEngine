"""Read-only, paced export of crawlerdb into Parquet on the compute host.

- Base export: `url_state_current_NNN` in ctid block ranges. Each chunk is one
  short autocommit `COPY ... TO STDOUT`, so no snapshot or lock outlives a chunk.
- Delta export: `url_state_history_NNN` by `snapshot_id` range. The ingestor
  appends the full post-upsert row there on every fetch, so the delta catches
  everything fetched since the previous watermark without a full scan.
- Dims export: `domain_state` and the split-subdomain whitelist.

Nothing here sorts, aggregates or writes on the server. Each chunk becomes one
Parquet row group, so memory stays bounded by one chunk. The first visit of a
shard records its plan in `_shards/NNN.start.json`; a resumed export reuses it,
so file ranges and history watermarks do not move between attempts.
"""

from __future__ import annotations

import contextlib
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql

from idxsel.config import Config
from idxsel.guards import Finding, Guard, GuardAbort, Watchdog
from idxsel.log import EventLog
from idxsel.manifest import (
    AtomicParquetWriter,
    Layout,
    atomic_write_json,
    parquet_ok,
    read_json,
    remove_tmp_files,
    write_parquet_atomic,
)
from idxsel.pg import ReadOnlySession, SessionHolder
from idxsel.throttle import Pacer, Verdict

# Columns copied as-is from url_state_current / url_state_history.
PLAIN_COLUMNS: tuple[str, ...] = (
    "url",
    "domain_id",
    "first_seen",
    "last_fetch_ok",
    "num_fetch_ok_90d",
    "num_consecutive_fail",
    "inlink_count_approx",
    "inlink_count_external",
    "parent_page_score",
    "url_score",
    "source",
    "discovery_source_type",
    "robots_bits",
    "is_redirect",
    "redirect_hop_count",
    "has_json_ld",
    "hreflang_count",
)
BOOL_OID, INT2_OID, INT4_OID, INT8_OID = 16, 21, 23, 20
TEXT_OID, VARCHAR_OID, FLOAT4_OID, FLOAT8_OID = 25, 1043, 700, 701
TS_OID, TSTZ_OID = 1114, 1184
# Derived columns: (name, SQL expression, result type OID).
DERIVED_COLUMNS: tuple[tuple[str, str, int], ...] = (
    ("has_anchor", "(anchor_text IS NOT NULL)", BOOL_OID),
    ("title_null", "(title IS NULL)", BOOL_OID),
    ("content_hash64", "hashtextextended(content_hash, 0)", INT8_OID),
)
# The crawler's daily accounting job (WebCrawler
# containers/scheduler_control/accounting/service.py) appends history rows with
# only 17 columns; inlink counts, robots_bits, title and the rest take their
# column defaults. Every ingestor fetch row of a crawled URL carries robots_bits
# 1 or 2, so this predicate keeps complete rows only.
COMPLETE_HISTORY_ROW = "robots_bits <> 0"
_ARROW_TYPES: dict[int, pa.DataType] = {
    BOOL_OID: pa.bool_(),
    INT2_OID: pa.int16(),
    INT4_OID: pa.int32(),
    INT8_OID: pa.int64(),
    TEXT_OID: pa.string(),
    VARCHAR_OID: pa.string(),
    FLOAT4_OID: pa.float32(),
    FLOAT8_OID: pa.float64(),
    TS_OID: pa.timestamp("us"),
    TSTZ_OID: pa.timestamp("us", tz="UTC"),
}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# One chunk: (COPY statement, heap bytes it reads for pacing, log labels).
Chunk = tuple[sql.Composable, int, dict[str, Any]]


class SchemaDrift(RuntimeError):
    """Raised when shards disagree on exported column types."""


class NeedFullRefresh(RuntimeError):
    """Raised when history no longer covers the gap since the last watermark."""


@dataclass
class ExportContext:
    cfg: Config
    layout: Layout
    events: EventLog
    guard: Guard
    pacer: Pacer
    open_session: Callable[[str], ReadOnlySession]
    sleep: Callable[[float], None] = time.sleep
    deadline: datetime | None = None


@dataclass
class _Run:
    """Per-export state shared by the shard bodies."""

    ctx: ExportContext
    holder: SessionHolder
    wd: Watchdog
    out: Path
    schema: pa.Schema
    oids: list[int]


def order_shards(shards: Iterable[int], cfg: Config) -> list[int]:
    """Orders shards; round-robin spreads consecutive reads across ingestors."""
    wanted = set(shards)
    if cfg.export.shard_order == "sequential":
        return sorted(wanted)
    per = cfg.export.shards_per_ingestor
    groups = cfg.crawlerdb.num_shards // per
    order = [g * per + j for j in range(per) for g in range(groups)]
    return [s for s in order if s in wanted]


def column_types(session: ReadOnlySession, tables: Sequence[str]) -> dict[str, int]:
    """Returns exported column type OIDs, verifying every table agrees.

    Raises:
        SchemaDrift: If a table lacks a column or uses a different type.
    """
    rows = session.fetchall(
        "SELECT c.relname, a.attname, a.atttypid::int FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname = ANY(%s) AND a.attname = ANY(%s) "
        "AND a.attnum > 0 AND NOT a.attisdropped",
        (list(tables), list(PLAIN_COLUMNS)),
    )
    per_table: dict[str, dict[str, int]] = {}
    for rel, att, oid in rows:
        per_table.setdefault(rel, {})[att] = oid
    reference: dict[str, int] | None = None
    problems = []
    for t in tables:
        cols = per_table.get(t, {})
        missing = [c for c in PLAIN_COLUMNS if c not in cols]
        if missing:
            problems.append(f"{t} lacks {missing}")
            continue
        if reference is None:
            reference = cols
        elif cols != reference:
            diff = sorted(c for c in PLAIN_COLUMNS if cols[c] != reference[c])
            problems.append(f"{t} type mismatch on {diff}")
    if problems or reference is None:
        raise SchemaDrift("; ".join(problems) or "no tables found")
    unknown = sorted({o for o in reference.values() if o not in _ARROW_TYPES})
    if unknown:
        raise SchemaDrift(f"unsupported type oids {unknown}")
    return reference


def _arrow_schema(types: dict[str, int], extra: Sequence[tuple[str, int]]) -> pa.Schema:
    fields = [pa.field(c, _ARROW_TYPES[types[c]]) for c in PLAIN_COLUMNS]
    fields += [pa.field(name, _ARROW_TYPES[oid]) for name, oid in extra]
    fields.append(pa.field("shard", pa.int16()))
    return pa.schema(fields)


def _oids(types: dict[str, int], extra: Sequence[int]) -> list[int]:
    return [types[c] for c in PLAIN_COLUMNS] + [o for _, _, o in DERIVED_COLUMNS] + list(extra)


def _to_table(rows: list[tuple], schema: pa.Schema, shard: int | None) -> pa.Table:
    """Builds one row group; `shard` (if given) fills the trailing shard column."""
    fields = list(schema)[:-1] if shard is not None else list(schema)
    columns = list(zip(*rows, strict=True)) if rows else [()] * len(fields)
    arrays = [pa.array(col, type=f.type) for f, col in zip(fields, columns, strict=True)]
    if shard is None:
        return pa.Table.from_arrays(arrays, schema=schema)
    arrays.append(pa.array([shard] * len(rows), type=pa.int16()))
    return pa.Table.from_arrays(arrays, schema=schema).sort_by("url")


def _block_size(session: ReadOnlySession) -> int:
    (size,) = session.fetchone("SELECT current_setting('block_size')::int")
    return int(size)


def _relation_blocks(session: ReadOnlySession, table: str) -> int:
    (n,) = session.fetchone(
        "SELECT pg_relation_size(%s::regclass) / current_setting('block_size')::int", (table,)
    )
    return int(n)


def history_watermark(session: ReadOnlySession, table: str) -> tuple[int | None, datetime | None]:
    """Returns the newest (snapshot_id, snapshot_at) in a history table."""
    row = session.fetchone(
        sql.SQL("SELECT snapshot_id, snapshot_at FROM {} ORDER BY snapshot_id DESC LIMIT 1")
        .format(sql.Identifier(table))
        .as_string(session.conn)
    )
    return (row[0], row[1]) if row else (None, None)


def _run_chunk(ctx: ExportContext, holder: SessionHolder, wd: Watchdog, chunk: Chunk,
               oids: Sequence[int]) -> list[tuple]:
    """Runs one COPY with guard checks, pacing, self-cancel and retries."""
    query, heap_bytes, label = chunk
    attempt = 0
    backoff = ctx.cfg.export.retry_backoff_s
    while True:
        if ctx.deadline and datetime.now(UTC) > ctx.deadline:
            raise GuardAbort("deadline passed")
        finding = ctx.guard.check()
        if finding.verdict < Verdict.PAUSE and not wd.healthy:
            finding = Finding(Verdict.PAUSE, "watchdog connection is down")
        if finding.verdict == Verdict.ABORT:
            raise GuardAbort(finding.reason)
        ctx.pacer.on_verdict(finding.verdict)
        if finding.verdict == Verdict.PAUSE:
            continue
        session = holder.session
        text = query.as_string(session.conn)
        started = time.monotonic()
        wd.set_target(session.conn)
        try:
            rows = list(session.copy_rows(text, oids))
        except (psycopg.errors.QueryCanceled, psycopg.OperationalError) as exc:
            attempt += 1
            ctx.events.emit("export.chunk_retry", attempt=attempt, error=str(exc)[:300], **label)
            if attempt > ctx.cfg.export.max_chunk_retries:
                raise
            ctx.sleep(backoff[min(attempt - 1, len(backoff) - 1)])
            if session.conn.closed or isinstance(exc, psycopg.OperationalError):
                holder.reset()
            continue
        finally:
            wd.set_target(None)
        elapsed = time.monotonic() - started
        slept = ctx.pacer.after_chunk(heap_bytes, elapsed)
        ctx.events.emit("export.chunk_done", rows=len(rows), heap_bytes=heap_bytes,
                        secs=round(elapsed, 3), slept=round(slept, 3),
                        rate_mb_s=ctx.pacer.rate_mb_s, **label)
        return rows


def _export_file(ctx: ExportContext, holder: SessionHolder, wd: Watchdog, final: Path,
                 schema: pa.Schema, shard: int | None, chunks: Iterable[Chunk],
                 oids: Sequence[int]) -> int:
    """Streams every chunk into its own row group of `final`; returns the row count."""
    e = ctx.cfg.export
    with AtomicParquetWriter(final, schema, compression=e.parquet_compression,
                             compression_level=e.parquet_compression_level) as writer:
        for chunk in chunks:
            writer.write(_to_table(_run_chunk(ctx, holder, wd, chunk, oids), schema, shard))
    return writer.rows


def _export_ranges(run: _Run, shard: int, tag: str, width: int, lo: int, hi: int,
                   chunk_span: int, chunk: Callable[[int, int], Chunk]) -> tuple[int, list[str]]:
    """Exports [lo, hi) as files of `chunks_per_file` chunks; skips committed files."""
    file_span = chunk_span * run.ctx.cfg.export.chunks_per_file
    shard_dir = run.out / f"shard={shard:03d}"
    rows, files = 0, []
    for f_lo in range(lo, hi, file_span):
        f_hi = min(f_lo + file_span, hi)
        final = shard_dir / f"part-{tag}{f_lo:0{width}d}-{tag}{f_hi:0{width}d}.parquet"
        if parquet_ok(final):
            rows += pq.ParquetFile(final).metadata.num_rows
        else:
            chunks = (chunk(c, min(c + chunk_span, f_hi)) for c in range(f_lo, f_hi, chunk_span))
            rows += _export_file(run.ctx, run.holder, run.wd, final, run.schema, shard, chunks,
                                 run.oids)
        files.append(final.name)
    return rows, files


def _shard_start(out: Path, shard: int, first_visit: Callable[[], dict[str, Any]]
                 ) -> tuple[Path, dict[str, Any]]:
    """Returns the shard's recorded plan, creating it on the first visit."""
    path = out / "_shards" / f"{shard:03d}.start.json"
    start = read_json(path)
    if start is None:
        start = first_visit()
        atomic_write_json(path, start)
    return path, start


def _select_list() -> sql.Composable:
    cols = [sql.Identifier(c) for c in PLAIN_COLUMNS]
    derived = [sql.SQL("{} AS {}").format(sql.SQL(expr), sql.Identifier(name))
               for name, expr, _ in DERIVED_COLUMNS]
    return sql.SQL(", ").join(cols + derived)


def _base_query(table: str, lo: int, hi: int) -> sql.Composable:
    return sql.SQL(
        "COPY (SELECT {cols}, statement_timestamp() AS observed_at FROM {table} "
        "WHERE ctid >= {lo}::tid AND ctid < {hi}::tid AND last_fetch_ok IS NOT NULL) "
        "TO STDOUT (FORMAT BINARY)"
    ).format(cols=_select_list(), table=sql.Identifier(table), lo=sql.Literal(f"({lo},0)"),
             hi=sql.Literal(f"({hi},0)"))


def _delta_query(table: str, lo: int, hi: int, prune_lo: datetime) -> sql.Composable:
    return sql.SQL(
        "COPY (SELECT {cols}, snapshot_at AS observed_at, snapshot_id FROM {table} "
        "WHERE snapshot_id > {lo} AND snapshot_id <= {hi} AND snapshot_at >= {prune} "
        "AND last_fetch_ok IS NOT NULL AND {complete}) TO STDOUT (FORMAT BINARY)"
    ).format(cols=_select_list(), table=sql.Identifier(table), lo=sql.Literal(lo),
             hi=sql.Literal(hi), prune=sql.Literal(prune_lo),
             complete=sql.SQL(COMPLETE_HISTORY_ROW))


def plan_base(ctx: ExportContext, shards: Sequence[int]) -> dict[str, Any]:
    """Dry run: sizes each shard from the catalog only and estimates duration."""
    holder = SessionHolder(lambda: ctx.open_session("export"))
    try:
        block_size = _block_size(holder.session)
        per_shard = {}
        for shard in order_shards(shards, ctx.cfg):
            table = ctx.cfg.crawlerdb.url_table_fmt.format(shard)
            per_shard[shard] = _relation_blocks(holder.session, table)
    finally:
        holder.close()
    heap = sum(per_shard.values()) * block_size
    hours = heap / (ctx.cfg.throttle.rate_mb_s_initial * 1_000_000) / 3600
    plan = {"shards": len(per_shard), "heap_bytes": heap, "blocks": per_shard,
            "hours_at_initial_rate": round(hours, 2)}
    ctx.events.emit("export.plan", shards=len(per_shard), heap_bytes=heap,
                    hours_at_initial_rate=round(hours, 2))
    return plan


@contextlib.contextmanager
def _export_sessions(ctx: ExportContext) -> Iterator[tuple[SessionHolder, Watchdog]]:
    """Yields the export session and a running watchdog on its own session."""
    holder = SessionHolder(lambda: ctx.open_session("export"))
    try:
        wd_holder = SessionHolder(lambda: ctx.open_session("watchdog"))
        try:
            with Watchdog(wd_holder, ctx.cfg.guard.watchdog_interval_s, ctx.events) as wd:
                yield holder, wd
        finally:
            wd_holder.close()
    finally:
        holder.close()


def _export_shards(ctx: ExportContext, out: Path, kind: str, export_id: str,
                   shards: Sequence[int], table_fmt: str, extra: Sequence[tuple[str, int]],
                   body: Callable[[_Run, int], dict[str, Any]]) -> dict[str, Any]:
    """Runs `body` for every shard without a done-marker, then writes `_SUCCESS.json`."""
    remove_tmp_files(out)
    ordered = order_shards(shards, ctx.cfg)
    started_at = datetime.now(UTC)
    with _export_sessions(ctx) as (holder, wd):
        types = column_types(holder.session, [table_fmt.format(s) for s in ordered])
        schema = _arrow_schema(types, [(n, o) for n, _, o in DERIVED_COLUMNS] + list(extra))
        oids = _oids(types, [o for _, o in extra])
        ctx.events.emit(f"export.{kind}_start", export_id=export_id, shards=len(ordered))
        run = _Run(ctx, holder, wd, out, schema, oids)
        for shard in ordered:
            marker = out / "_shards" / f"{shard:03d}.json"
            if marker.exists():
                continue
            result = body(run, shard)
            atomic_write_json(marker, {"shard": shard, **result,
                                       "finished_at": datetime.now(UTC)})
            ctx.events.emit("export.shard_done", kind=kind, shard=shard, rows=result["rows"])
    summary = _summarize(out, ordered, export_id, kind, started_at)
    atomic_write_json(out / "_SUCCESS.json", summary)
    return summary


def export_base(ctx: ExportContext, export_id: str, shards: Sequence[int]) -> dict[str, Any]:
    """Exports crawled rows of the given shards; resumable at file granularity."""
    cfg = ctx.cfg
    span = cfg.export.chunk_pages

    def body(run: _Run, shard: int) -> dict[str, Any]:
        table = cfg.crawlerdb.url_table_fmt.format(shard)
        hist = cfg.crawlerdb.history_table_fmt.format(shard)

        def first_visit() -> dict[str, Any]:
            s = run.holder.session
            wm, wm_at = history_watermark(s, hist)
            return {"wm": wm, "wm_at": wm_at, "started_at": datetime.now(UTC),
                    "extents": [[0, _relation_blocks(s, table)]]}

        path, start = _shard_start(run.out, shard, first_visit)
        block_size = _block_size(run.holder.session)

        def chunk(lo: int, hi: int) -> Chunk:
            return (_base_query(table, lo, hi), (hi - lo) * block_size,
                    {"kind": "base", "shard": shard, "blk_lo": lo, "blk_hi": hi})

        rows, files, done = 0, [], 0
        while True:
            for lo, hi in start["extents"][done:]:
                r, f = _export_ranges(run, shard, "b", 10, lo, hi, span, chunk)
                rows, files = rows + r, files + f
            done = len(start["extents"])
            last = start["extents"][-1][1]
            grown = _relation_blocks(run.holder.session, table)
            if grown <= last:
                break
            start["extents"].append([last, grown])
            atomic_write_json(path, start)
        return {"blocks": last, "rows": rows, "files": files, "history_wm": start["wm"],
                "history_wm_at": start["wm_at"], "started_at": start["started_at"]}

    return _export_shards(ctx, ctx.layout.base_dir(export_id), "base", export_id, shards,
                          cfg.crawlerdb.url_table_fmt, [("observed_at", TSTZ_OID)], body)


def export_delta(ctx: ExportContext, export_id: str, shards: Sequence[int],
                 watermarks: dict[str, Any]) -> dict[str, Any]:
    """Exports complete history rows above each shard's watermark.

    Raises:
        NeedFullRefresh: If history was pruned past a shard's watermark.
    """
    cfg = ctx.cfg
    e = cfg.export

    def body(run: _Run, shard: int) -> dict[str, Any]:
        hist = cfg.crawlerdb.history_table_fmt.format(shard)

        def first_visit() -> dict[str, Any]:
            prev = watermarks.get(str(shard))
            if prev is None:
                raise NeedFullRefresh(f"shard {shard} has no watermark")
            prev_id, prev_at = prev.get("wm"), prev.get("wm_at")
            s = run.holder.session
            (min_id,) = s.fetchone(sql.SQL("SELECT min(snapshot_id) FROM {}")
                                   .format(sql.Identifier(hist)).as_string(s.conn))
            if prev_id is not None and min_id is not None and min_id > prev_id:
                raise NeedFullRefresh(f"shard {shard}: history starts at {min_id} > {prev_id}")
            hi, hi_at = history_watermark(s, hist)
            lo = max(0, (prev_id or 0) - e.history_overlap_ids)
            prune_lo = (datetime.fromisoformat(str(prev_at))
                        - timedelta(hours=e.history_prune_slack_hours) if prev_at else _EPOCH)
            return {"lo": lo, "hi": hi if hi is not None else lo, "prune_lo": prune_lo,
                    "wm": hi if hi is not None else prev_id,
                    "wm_at": hi_at if hi is not None else prev_at}

        _, start = _shard_start(run.out, shard, first_visit)
        prune_lo = datetime.fromisoformat(str(start["prune_lo"]))

        def chunk(lo: int, hi: int) -> Chunk:
            return (_delta_query(hist, lo, hi, prune_lo), (hi - lo) * e.history_row_bytes_estimate,
                    {"kind": "delta", "shard": shard, "id_lo": lo, "id_hi": hi})

        rows, files = _export_ranges(run, shard, "s", 19, start["lo"], start["hi"],
                                     e.history_chunk_ids, chunk)
        return {"lo": start["lo"], "hi": start["hi"], "rows": rows, "files": files,
                "history_wm": start["wm"], "history_wm_at": start["wm_at"]}

    return _export_shards(ctx, ctx.layout.delta_dir(export_id), "delta", export_id, shards,
                          cfg.crawlerdb.history_table_fmt,
                          [("observed_at", TSTZ_OID), ("snapshot_id", INT8_OID)], body)


def export_dims(ctx: ExportContext, export_id: str) -> dict[str, Any]:
    """Exports domain_state (domain_id, domain, shard_id) and the split whitelist."""
    cfg = ctx.cfg
    out = ctx.layout.dims_dir(export_id)
    remove_tmp_files(out)
    domain_schema = pa.schema([("domain_id", pa.int64()), ("domain", pa.string()),
                               ("shard_id", pa.int32())])
    with _export_sessions(ctx) as (holder, wd):
        final = out / "domain_state.parquet"
        if not parquet_ok(final):
            table = cfg.crawlerdb.domain_table
            block_size = _block_size(holder.session)
            nblocks = _relation_blocks(holder.session, table)
            step = cfg.export.chunk_pages

            def chunk(lo: int, hi: int) -> Chunk:
                query = sql.SQL(
                    "COPY (SELECT domain_id::int8, domain::text, shard_id::int4 FROM {} "
                    "WHERE ctid >= {}::tid AND ctid < {}::tid) TO STDOUT (FORMAT BINARY)"
                ).format(sql.Identifier(table), sql.Literal(f"({lo},0)"),
                         sql.Literal(f"({hi},0)"))
                return query, (hi - lo) * block_size, {"kind": "dims", "blk_lo": lo,
                                                       "blk_hi": hi}

            chunks = (chunk(lo, min(lo + step, nblocks)) for lo in range(0, nblocks, step))
            _export_file(ctx, holder, wd, final, domain_schema, None, chunks,
                         [INT8_OID, TEXT_OID, INT4_OID])
        split_sql = sql.SQL("SELECT host::text FROM {}").format(
            sql.Identifier(cfg.crawlerdb.split_table))
        hosts = [r[0] for r in holder.session.fetchall(split_sql)]
        write_parquet_atomic(pa.table({"host": pa.array(hosts, type=pa.string())}),
                             out / "split_hosts.parquet")
    summary = {"export_id": export_id, "kind": "dims", "finished_at": datetime.now(UTC),
               "domains": pq.ParquetFile(out / "domain_state.parquet").metadata.num_rows,
               "split_hosts": len(hosts)}
    atomic_write_json(out / "_SUCCESS.json", summary)
    return summary


def _summarize(out: Path, shards: Sequence[int], export_id: str, kind: str,
               started_at: datetime) -> dict[str, Any]:
    markers = {s: read_json(out / "_shards" / f"{s:03d}.json") for s in shards}
    return {
        "export_id": export_id,
        "kind": kind,
        "started_at": started_at,
        "finished_at": datetime.now(UTC),
        "rows": sum(m["rows"] for m in markers.values()),
        "watermarks": {str(s): {"wm": m["history_wm"], "wm_at": m["history_wm_at"]}
                       for s, m in markers.items()},
    }
