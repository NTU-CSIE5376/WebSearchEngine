"""Load, swap and roll back the selection in OUR selectdb; read it as the metric does.

Connections come from `isel.selectdb.connect`, the only way to write a
database. The tables follow `isel.schema`; a load refuses a database that
`migrate-selectdb` has not brought to the newest revision.

- Load (transaction 1): create `selected_urls_r<id>` LIKE the live table, COPY
  FREEZE, add the primary and foreign keys, ANALYZE and check the row count.
  With `wal_level=minimal`, a table created and filled in one transaction skips WAL.
- Swap (transaction 2): rename tables, and their `<table>_pkey` and
  `<table>_run_id_fkey` constraints, under a short `lock_timeout`, so readers
  never see a half-loaded list and wait milliseconds at most.
- A new load or a rollback drops every loaded-but-unswapped run, so a pending
  swap cannot undo a rollback.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from isel.config import Config, ScheduleCfg, hhmm_minutes
from isel.evaluate import bootstrap, golden_by_tag, metric_rate, rates
from isel.golden import read_golden
from isel.io import assert_no_secrets, open_duckdb, sql_str
from isel.manifest import validate_run
from isel.schema import LIST_TABLE
from isel.selectdb import assert_ready, connect

CURRENT = LIST_TABLE
PREV = "selected_urls_prev"
SWAP_TMP = "selected_urls_swap_tmp"
RUN_TABLE_RE = r"^selected_urls_r[0-9]+$"


def swap_allowed(now: datetime, cfg: ScheduleCfg) -> bool:
    """False while the metric may read the list (measurement days, freeze window, UTC)."""
    now = now.astimezone(UTC)
    if now.day not in cfg.swap_freeze_days:
        return True
    start, end = (hhmm_minutes(v) for v in cfg.swap_freeze_window)
    return not start <= now.hour * 60 + now.minute < end


def _table_exists(cur: psycopg.Cursor, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{name}",))
    return bool(cur.fetchone()[0])


def _rename(cur: psycopg.Cursor, old: str, new: str) -> None:
    """Renames a list table and the two constraints named after it."""
    cur.execute(sql.SQL("ALTER TABLE public.{} RENAME TO {}").format(
        sql.Identifier(old), sql.Identifier(new)))
    for suffix in ("_pkey", "_run_id_fkey"):
        cur.execute(sql.SQL("ALTER TABLE public.{} RENAME CONSTRAINT {} TO {}").format(
            sql.Identifier(new), sql.Identifier(old + suffix), sql.Identifier(new + suffix)))


def _drop_unswapped(admin: psycopg.Connection) -> list[int]:
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
            dropped.append(run_id)
    return dropped


def _ensure_space(cfg: Config, admin: psycopg.Connection, k: int) -> None:
    s = cfg.selectdb
    need = k * s.est_bytes_per_row
    with admin.cursor() as cur:
        cur.execute("SELECT pg_database_size(current_database())")
        (size,) = cur.fetchone()
        if size + need > s.max_db_bytes and _table_exists(cur, PREV):
            with admin.transaction():
                cur.execute(sql.SQL("DROP TABLE public.{}").format(sql.Identifier(PREV)))
            cur.execute("SELECT pg_database_size(current_database())")
            (size,) = cur.fetchone()
    if size + need > s.max_db_bytes:
        raise RuntimeError(f"selectdb would exceed selectdb.max_db_bytes ({size} + {need})")


def _model_runs_row(run: dict[str, Any]) -> dict[str, Any]:
    row = {
        "run_label": run["run_id"], "cutoff_datetime": run["snapshot_at"],
        "model_path": run["model_dir"], "train_metrics": run["model_meta"],
        "selection_metrics": {k: run[k] for k in
                              ("k", "rows", "docs", "candidates", "boundary_score", "tau",
                               "target_batch")},
        "source_paths": {"selection_run": run["run_id"]},
        "config_sha256": run["config_sha256"], "code_version": run["code_version"],
    }
    assert_no_secrets(row)
    return row


def _load_validated(cfg: Config, admin: psycopg.Connection, csv_path: Path,
                    row: dict[str, Any], k: int) -> int:
    """Transaction 1: model_runs row, new table, COPY FREEZE, keys, ANALYZE, row check."""
    s = cfg.selectdb
    with admin.transaction(), admin.cursor() as cur:
        cur.execute(
            "INSERT INTO public.model_runs (run_label, cutoff_datetime, model_path, "
            "train_metrics, selection_metrics, source_paths, status, config_sha256, "
            "code_version) VALUES (%s, %s, %s, %s, %s, %s, 'loading', %s, %s) RETURNING id",
            (row["run_label"], row["cutoff_datetime"], row["model_path"],
             json.dumps(row["train_metrics"], default=str),
             json.dumps(row["selection_metrics"], default=str),
             json.dumps(row["source_paths"]), row["config_sha256"], row["code_version"]))
        (run_id,) = cur.fetchone()
        name = f"selected_urls_r{run_id}"
        table = sql.Identifier(name)
        # The live table is the column definition; its keys are added after the COPY.
        cur.execute(sql.SQL("CREATE TABLE public.{} (LIKE public.{} INCLUDING DEFAULTS "
                            "INCLUDING CONSTRAINTS)").format(table, sql.Identifier(CURRENT)))
        # run_id is known only now; a default fills it during the COPY and is then dropped.
        cur.execute(sql.SQL("ALTER TABLE public.{} ALTER COLUMN run_id SET DEFAULT {}").format(
            table, sql.Literal(run_id)))
        # FREEZE is allowed because the table was created in this transaction. Rows are
        # written frozen and all-visible, so readers never dirty pages with hint bits.
        with cur.copy(sql.SQL("COPY public.{} (url, score) FROM STDIN (FORMAT csv, FREEZE)")
                      .format(table)) as copy, csv_path.open("rb") as fh:
            while block := fh.read(s.copy_block_bytes):
                copy.write(block)
        cur.execute(sql.SQL("ALTER TABLE public.{} ALTER COLUMN run_id DROP DEFAULT").format(
            table))
        cur.execute(sql.SQL("SET LOCAL maintenance_work_mem = {}").format(
            sql.Literal(s.maintenance_work_mem)))
        cur.execute(sql.SQL("ALTER TABLE public.{} ADD CONSTRAINT {} PRIMARY KEY (url)").format(
            table, sql.Identifier(f"{name}_pkey")))
        cur.execute(sql.SQL(
            "ALTER TABLE public.{} ADD CONSTRAINT {} FOREIGN KEY (run_id) "
            "REFERENCES public.model_runs(id) ON DELETE RESTRICT").format(
            table, sql.Identifier(f"{name}_run_id_fkey")))
        cur.execute(sql.SQL("ANALYZE public.{}").format(table))
        cur.execute(sql.SQL("SELECT count(*) FROM public.{}").format(table))
        (rows,) = cur.fetchone()
        if rows != k:
            raise RuntimeError(f"loaded {rows} rows, expected {k}")
        cur.execute("UPDATE public.model_runs SET status = 'validated' WHERE id = %s", (run_id,))
    return run_id


def load_selection(cfg: Config, admin: psycopg.Connection, run_dir: Path,
                   work_dir: Path) -> dict[str, Any]:
    """Loads a `select-full` manifest run into a new, not yet visible table.

    The manifest's checksums are verified first. The table is validated (row
    count == K, unique URLs through the primary key) before commit. Nothing
    becomes visible to readers until `swap`.
    """
    run = validate_run(run_dir)
    if run.get("scope") != "full":
        raise RuntimeError(f"{run_dir} is a {run.get('scope')} run; publish a select-full run")
    if run["rows"] != run["k"]:
        raise RuntimeError(f"the run holds {run['rows']} rows, not K = {run['k']}")
    row = _model_runs_row(run)
    assert_ready(cfg, admin)
    dropped = _drop_unswapped(admin)
    _ensure_space(cfg, admin, run["k"])
    csv_path = work_dir / "duckdb_tmp" / "load.csv"
    con = open_duckdb(cfg, work_dir / "duckdb_tmp")
    started = time.monotonic()
    try:
        con.execute(f"COPY (SELECT url, score FROM read_parquet("
                    f"{sql_str(str(run_dir / 'manifest' / '*.parquet'))})) "
                    f"TO {sql_str(str(csv_path))} (FORMAT csv, HEADER false)")
        db_run_id = _load_validated(cfg, admin, csv_path, row, run["k"])
    finally:
        csv_path.unlink(missing_ok=True)
    return {"db_run_id": db_run_id, "rows": run["k"], "dropped_unswapped": dropped,
            "secs": round(time.monotonic() - started, 1)}


def swap(cfg: Config, admin: psycopg.Connection, run_id: int,
         now: Callable[[], datetime] = lambda: datetime.now(UTC),
         sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Makes run `run_id` live. Retries on lock timeouts; refuses during the freeze.

    Idempotent: a run that is already live reports success.
    """
    s = cfg.selectdb
    with admin.cursor() as cur:
        cur.execute("SELECT status FROM public.model_runs WHERE id = %s", (run_id,))
        row = cur.fetchone()
        if row and row[0] == "live":
            return {"swapped": True, "already_live": True}
        if not _table_exists(cur, f"selected_urls_r{run_id}"):
            return {"swapped": False, "reason": "run has no loaded table", "retry": False}
    for attempt in range(1, s.swap_retry_attempts + 1):
        if not swap_allowed(now(), cfg.schedule):
            return {"swapped": False, "reason": "swap freeze window", "retry": True}
        try:
            with admin.transaction(), admin.cursor() as cur:
                cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(
                    sql.Literal(s.swap_lock_timeout)))
                started = time.monotonic()
                cur.execute(sql.SQL("LOCK TABLE public.{} IN ACCESS EXCLUSIVE MODE").format(
                    sql.Identifier(CURRENT)))
                lock_ms = round((time.monotonic() - started) * 1000, 1)
                cur.execute(sql.SQL("DROP TABLE IF EXISTS public.{}").format(sql.Identifier(PREV)))
                _rename(cur, CURRENT, PREV)
                _rename(cur, f"selected_urls_r{run_id}", CURRENT)
                cur.execute("UPDATE public.model_runs SET status = 'retired' WHERE status = 'live'")
                cur.execute("UPDATE public.model_runs SET status = 'live', activated_at = now() "
                            "WHERE id = %s", (run_id,))
                if not s.keep_prev:
                    cur.execute(sql.SQL("DROP TABLE public.{}").format(sql.Identifier(PREV)))
        except psycopg.errors.LockNotAvailable:
            if attempt < s.swap_retry_attempts:
                sleep(s.swap_retry_backoff_s)
            continue
        return {"swapped": True, "lock_wait_ms": lock_ms, "attempts": attempt}
    return {"swapped": False, "reason": "lock timeout", "retry": True}


def rollback(cfg: Config, admin: psycopg.Connection) -> dict[str, Any]:
    """Swaps `selected_urls_current` and `selected_urls_prev` back."""
    with admin.transaction(), admin.cursor() as cur:
        if not (_table_exists(cur, CURRENT) and _table_exists(cur, PREV)):
            raise RuntimeError("rollback needs both current and prev tables")
        cur.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(
            sql.Literal(cfg.selectdb.swap_lock_timeout)))
        cur.execute(sql.SQL("LOCK TABLE public.{}, public.{} IN ACCESS EXCLUSIVE MODE").format(
            sql.Identifier(CURRENT), sql.Identifier(PREV)))
        cur.execute(sql.SQL("SELECT run_id FROM public.{} LIMIT 1").format(sql.Identifier(PREV)))
        prev_row = cur.fetchone()
        for a, b in ((CURRENT, SWAP_TMP), (PREV, CURRENT), (SWAP_TMP, PREV)):
            _rename(cur, a, b)
        cur.execute("UPDATE public.model_runs SET status = 'rolled_back' WHERE status = 'live'")
        if prev_row:
            cur.execute("UPDATE public.model_runs SET status = 'live', activated_at = now() "
                        "WHERE id = %s", (prev_row[0],))
    return {"live_run": prev_row[0] if prev_row else None,
            "dropped_unswapped": _drop_unswapped(admin)}


def verify_live(cfg: Config, work_dir: Path, batch: int) -> dict[str, Any]:
    """The indexed rate of `batch` under the live list, read as the official metric reads it.

    Connects as the metric's reader role and sends the metric's own query in
    chunks of `selectdb.verify_chunk` URLs. The metric rate equals the metric's
    `indexed_rate` (canonical golden URLs found / canonical golden URLs); the
    exact and canonical rates are the backtest's, for comparison.
    """
    golden = golden_by_tag(read_golden(work_dir).to_pylist(), batch, cfg.golden.tags)
    if not golden:
        raise RuntimeError(f"batch {batch} has no golden rows; run pull-golden first")
    e, chunk = cfg.eval, cfg.selectdb.verify_chunk
    conn = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, run_label, activated_at FROM public.model_runs "
                        "WHERE status = 'live'")
            live = cur.fetchall()
            tags = {}
            for tag, g in golden.items():
                spellings = sorted(set(g.urls) | set(g.canonical.values()))
                listed: set[str] = set()
                for i in range(0, len(spellings), chunk):
                    cur.execute("SELECT url FROM public.selected_urls_current "
                                "WHERE url = ANY(%s)", (spellings[i:i + chunk],))
                    listed.update(u for (u,) in cur.fetchall())
                exact, r_exact, r_canonical = rates(g, listed)
                tags[tag] = {"golden_urls": len(g.urls), "indexed": len(exact),
                             "indexed_exact_rate": r_exact,
                             "indexed_canonical_rate": r_canonical,
                             "indexed_metric_rate": metric_rate(g, listed),
                             "ci95": list(bootstrap(g.queries, exact, e.bootstrap_samples,
                                                    e.bootstrap_seed))}
    finally:
        conn.close()
    return {"batch": batch,
            "live_runs": [{"id": r[0], "run_label": r[1], "activated_at": r[2]} for r in live],
            "tags": tags}
