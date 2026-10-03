"""Load, validate, swap and roll back the selection in OUR selectdb.

Schema matches PR #4 (`IndexSelection/selectdb_common.py`) and the metric's
query `SELECT url FROM public.selected_urls_current WHERE url = ANY(:urls)`.

- Load (transaction 1): create `selected_urls_r<id>`, COPY FREEZE, add the
  primary key, ANALYZE and validate in ONE transaction. With `wal_level=minimal`
  a table created and filled in the same transaction skips WAL.
- Swap (transaction 2): rename tables under a short `lock_timeout`, so readers
  never see a half-loaded list and are blocked for milliseconds at most.
- A new load or a rollback supersedes every loaded-but-unswapped run: their
  tables are dropped and the runs are marked `abandoned`, so selectdb never
  fills up and a pending swap cannot undo a rollback.
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import psycopg
from psycopg import sql

from idxsel.config import Config
from idxsel.duck import copy_atomic, sql_str
from idxsel.log import EventLog
from idxsel.schedule import swap_allowed

CURRENT = "selected_urls_current"
PREV = "selected_urls_prev"
RUN_TABLE_RE = r"^selected_urls_r[0-9]+$"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.model_runs (
    id BIGSERIAL PRIMARY KEY,
    run_label TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cutoff_datetime TIMESTAMPTZ,
    model_path TEXT,
    train_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    eval_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    selection_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_paths JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'loading',
    activated_at TIMESTAMPTZ,
    config_sha256 TEXT,
    code_version TEXT
);
CREATE TABLE IF NOT EXISTS public.selected_urls_current (
    url TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    first_seen TIMESTAMPTZ,
    run_id BIGINT NOT NULL REFERENCES public.model_runs(id) ON DELETE CASCADE,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_SECRET_RE = re.compile(r"(password|passwd|secret|token)|://[^/\s]*:[^@/\s]*@", re.I)


class SecretLeak(ValueError):
    """Raised when audit metadata would store something that looks like a secret."""


def assert_no_secrets(payload: Any) -> None:
    """Rejects metadata containing credentials (PR #4 stored a DSN this way)."""
    text = json.dumps(payload, default=str)
    if _SECRET_RE.search(text):
        raise SecretLeak("audit metadata contains a credential-like value")


def ensure_schema(admin: psycopg.Connection, reader_role: str) -> None:
    """Creates missing tables; fails if `init-selectdb` never created the reader role."""
    with admin.transaction(), admin.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (reader_role,))
        if cur.fetchone() is None:
            raise RuntimeError(f"role {reader_role!r} is missing; run `idxsel init-selectdb` once")


def init_selectdb(admin: psycopg.Connection, reader_role: str, reader_password: str) -> None:
    """One-time setup: tables plus the read-only role the metric connects as. Idempotent."""
    with admin.transaction(), admin.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (reader_role,))
        verb = "ALTER" if cur.fetchone() else "CREATE"
        cur.execute(sql.SQL("{} ROLE {} LOGIN PASSWORD {}").format(
            sql.SQL(verb), sql.Identifier(reader_role), sql.Literal(reader_password)))
        cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(admin.info.dbname), sql.Identifier(reader_role)))
        cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
            sql.Identifier(reader_role)))
        cur.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(
            sql.Identifier(reader_role)))
        cur.execute(sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}").format(
            sql.Identifier(reader_role)))


def _table_exists(cur: psycopg.Cursor, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{name}",))
    return bool(cur.fetchone()[0])


def _drop_unswapped(admin: psycopg.Connection, events: EventLog) -> list[int]:
    """Drops tables of runs that were loaded but never swapped in; returns their ids."""
    dropped = []
    with admin.transaction(), admin.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                    "AND tablename ~ %s", (RUN_TABLE_RE,))
        for (name,) in cur.fetchall():
            run_id = int(name.removeprefix("selected_urls_r"))
            cur.execute(sql.SQL("DROP TABLE public.{}").format(sql.Identifier(name)))
            cur.execute("UPDATE public.model_runs SET status = 'abandoned' WHERE id = %s",
                        (run_id,))
            events.emit("load.dropped_unswapped", run_id=run_id)
            dropped.append(run_id)
    return dropped


def _ensure_space(cfg: Config, admin: psycopg.Connection, k: int, events: EventLog) -> None:
    need = k * cfg.selectdb.est_bytes_per_row
    with admin.cursor() as cur:
        cur.execute("SELECT pg_database_size(current_database())")
        (size,) = cur.fetchone()
        if size + need <= cfg.selectdb.max_db_bytes:
            return
        if _table_exists(cur, PREV):
            with admin.transaction():
                cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(PREV)))
            events.emit("load.dropped_prev_for_space", db_bytes=size, need=need)
            cur.execute("SELECT pg_database_size(current_database())")
            (size,) = cur.fetchone()
    if size + need > cfg.selectdb.max_db_bytes:
        raise RuntimeError(f"selectdb would exceed max_db_bytes ({size} + {need})")


def _load_validated(cfg: Config, admin: psycopg.Connection, csv_path: Path,
                    meta: dict[str, Any]) -> int:
    """Transaction 1: model_runs row, new table, COPY FREEZE, keys, ANALYZE, row check."""
    k = cfg.run.k
    with admin.transaction(), admin.cursor() as cur:
        cur.execute(
            "INSERT INTO public.model_runs (run_label, cutoff_datetime, model_path, train_metrics,"
            " eval_metrics, selection_metrics, source_paths, status, config_sha256, code_version)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, 'loading', %s, %s) RETURNING id",
            (meta["run_label"], meta["cutoff_datetime"], meta["model_path"],
             json.dumps(meta.get("train_metrics", {}), default=str),
             json.dumps(meta.get("eval_metrics", {}), default=str),
             json.dumps(meta.get("selection_metrics", {}), default=str),
             json.dumps(meta.get("source_paths", {}), default=str),
             meta["config_sha256"], meta["code_version"]),
        )
        (run_id,) = cur.fetchone()
        table = sql.Identifier(f"selected_urls_r{run_id}")
        cur.execute(sql.SQL(
            "CREATE TABLE public.{} (url TEXT NOT NULL, score DOUBLE PRECISION NOT NULL, "
            "first_seen TIMESTAMPTZ, run_id BIGINT NOT NULL DEFAULT {}, "
            "selected_at TIMESTAMPTZ NOT NULL DEFAULT now())").format(table, sql.Literal(run_id)))
        # FREEZE (allowed because the table was created in this transaction) writes
        # rows frozen and all-visible: readers never dirty pages with hint bits.
        with cur.copy(sql.SQL("COPY public.{} (url, score, first_seen) FROM STDIN "
                              "(FORMAT csv, FREEZE)").format(table)) as copy, \
                csv_path.open("rb") as fh:
            while block := fh.read(cfg.selectdb.copy_block_bytes):
                copy.write(block)
        cur.execute(sql.SQL("SET LOCAL maintenance_work_mem = {}").format(
            sql.Literal(cfg.selectdb.maintenance_work_mem)))
        cur.execute(sql.SQL("ALTER TABLE public.{} ADD CONSTRAINT {} PRIMARY KEY (url)").format(
            table, sql.Identifier(f"selected_urls_r{run_id}_pkey")))
        cur.execute(sql.SQL(
            "ALTER TABLE public.{} ADD CONSTRAINT {} FOREIGN KEY (run_id) "
            "REFERENCES public.model_runs(id) ON DELETE CASCADE").format(
            table, sql.Identifier(f"selected_urls_r{run_id}_run_fk")))
        cur.execute(sql.SQL("ANALYZE public.{}").format(table))
        cur.execute(sql.SQL("SELECT count(*) FROM public.{}").format(table))
        (rows,) = cur.fetchone()
        if rows != k:
            raise RuntimeError(f"loaded {rows} rows, expected {k}")
        cur.execute("UPDATE public.model_runs SET status = 'validated' WHERE id = %s", (run_id,))
    return run_id


def load_selection(cfg: Config, admin: psycopg.Connection, con: duckdb.DuckDBPyConnection,
                   selected: Path, work: Path, meta: dict[str, Any], events: EventLog) -> int:
    """Loads `selected` into a new table; returns the model_runs id.

    The table is validated (row count == K, unique URLs via the primary key)
    before commit. Nothing becomes visible to readers until `swap`.
    """
    k = cfg.run.k
    for key in ("train_metrics", "eval_metrics", "selection_metrics", "source_paths"):
        assert_no_secrets(meta.get(key, {}))
    _drop_unswapped(admin, events)
    _ensure_space(cfg, admin, k, events)
    csv_path = work / "load.csv"
    try:
        copy_atomic(
            con,
            f"SELECT url, score, strftime(first_seen, '%Y-%m-%d %H:%M:%S.%f+00') AS first_seen "
            f"FROM read_parquet({sql_str(str(selected))})",
            csv_path, "FORMAT csv, HEADER false")
        started = time.monotonic()
        run_id = _load_validated(cfg, admin, csv_path, meta)
    finally:
        csv_path.unlink(missing_ok=True)
    events.emit("load.validated", run_id=run_id, rows=k, secs=round(time.monotonic() - started, 2))
    return run_id


def swap(cfg: Config, admin: psycopg.Connection, run_id: int, events: EventLog,
         now: Callable[[], datetime] = lambda: datetime.now(UTC),
         sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Makes run `run_id` live. Retries on lock timeouts; refuses during the freeze.

    Idempotent: a run that is already live reports success. `retry` tells the
    caller whether trying again later can help.
    """
    s = cfg.selectdb
    with admin.cursor() as cur:
        cur.execute("SELECT status FROM public.model_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if row and row[0] == "live":
            return {"swapped": True, "already_live": True}
        if not _table_exists(cur, f"selected_urls_r{run_id}"):
            events.emit("swap.missing_table", run_id=run_id, status=row[0] if row else None)
            return {"swapped": False, "reason": "run has no loaded table", "retry": False}
    for attempt in range(1, s.swap_retry_attempts + 1):
        if not swap_allowed(now(), cfg.schedule):
            events.emit("swap.frozen", run_id=run_id)
            return {"swapped": False, "reason": "swap freeze window", "retry": True}
        try:
            with admin.transaction(), admin.cursor() as cur:
                cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(
                    sql.Literal(s.swap_lock_timeout)))
                started = time.monotonic()
                if _table_exists(cur, CURRENT):
                    cur.execute(sql.SQL("LOCK TABLE public.{} IN ACCESS EXCLUSIVE MODE").format(
                        sql.Identifier(CURRENT)))
                lock_ms = round((time.monotonic() - started) * 1000, 1)
                cur.execute(sql.SQL("DROP TABLE IF EXISTS public.{}").format(sql.Identifier(PREV)))
                cur.execute(sql.SQL("ALTER TABLE IF EXISTS public.{} RENAME TO {}").format(
                    sql.Identifier(CURRENT), sql.Identifier(PREV)))
                cur.execute(sql.SQL("ALTER TABLE public.{} RENAME TO {}").format(
                    sql.Identifier(f"selected_urls_r{run_id}"), sql.Identifier(CURRENT)))
                cur.execute("UPDATE public.model_runs SET status = 'retired' WHERE status = 'live'")
                cur.execute("UPDATE public.model_runs SET status = 'live', activated_at = now() "
                            "WHERE id = %s", (run_id,))
                if not s.keep_prev:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS public.{}").format(
                        sql.Identifier(PREV)))
        except psycopg.errors.LockNotAvailable:
            events.emit("swap.lock_timeout", run_id=run_id, attempt=attempt)
            if attempt < s.swap_retry_attempts:
                sleep(s.swap_retry_backoff_s)
            continue
        events.emit("swap.done", run_id=run_id, lock_wait_ms=lock_ms, attempt=attempt)
        return {"swapped": True, "lock_wait_ms": lock_ms, "attempts": attempt}
    return {"swapped": False, "reason": "lock timeout", "retry": True}


def rollback(cfg: Config, admin: psycopg.Connection, events: EventLog) -> dict[str, Any]:
    """Swaps `selected_urls_current` and `selected_urls_prev` back.

    Loaded-but-unswapped runs are dropped too, so a swap still pending in the
    daemon cannot undo the rollback (it finds no table and gives up).
    """
    tmp = "selected_urls_swap_tmp"
    with admin.transaction(), admin.cursor() as cur:
        if not (_table_exists(cur, CURRENT) and _table_exists(cur, PREV)):
            raise RuntimeError("rollback needs both current and prev tables")
        cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(
            sql.Literal(cfg.selectdb.swap_lock_timeout)))
        cur.execute(sql.SQL("LOCK TABLE public.{}, public.{} IN ACCESS EXCLUSIVE MODE").format(
            sql.Identifier(CURRENT), sql.Identifier(PREV)))
        cur.execute(sql.SQL("SELECT run_id FROM public.{} LIMIT 1").format(sql.Identifier(PREV)))
        prev_row = cur.fetchone()
        for a, b in ((CURRENT, tmp), (PREV, CURRENT), (tmp, PREV)):
            cur.execute(sql.SQL("ALTER TABLE public.{} RENAME TO {}").format(
                sql.Identifier(a), sql.Identifier(b)))
        cur.execute("UPDATE public.model_runs SET status = 'rolled_back' WHERE status = 'live'")
        if prev_row:
            cur.execute("UPDATE public.model_runs SET status = 'live', activated_at = now() "
                        "WHERE id = %s", (prev_row[0],))
    events.emit("swap.rollback", live_run=prev_row[0] if prev_row else None)
    return {"live_run": prev_row[0] if prev_row else None,
            "dropped_unswapped": _drop_unswapped(admin, events)}


def update_eval_metrics(admin: psycopg.Connection, run_id: int, metrics: dict[str, Any]) -> None:
    assert_no_secrets(metrics)
    with admin.transaction(), admin.cursor() as cur:
        cur.execute("UPDATE public.model_runs SET eval_metrics = %s WHERE id = %s",
                    (json.dumps(metrics, default=str), run_id))
