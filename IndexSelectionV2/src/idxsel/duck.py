"""DuckDB connections bounded by the config's memory, thread and spill limits."""

from __future__ import annotations

from pathlib import Path

import duckdb

from idxsel.config import Config
from idxsel.manifest import TMP_SUFFIX, commit_file


def open_duckdb(cfg: Config, temp_dir: Path | None) -> duckdb.DuckDBPyConnection:
    """Opens an in-memory DuckDB that spills to `temp_dir` within the configured cap.

    DuckDB names spill files by size class and index, so two processes sharing
    a temp directory overwrite each other's files. Only the work-lock holder
    may spill; a caller without the lock passes None and gets no spilling.
    """
    con = duckdb.connect(database=":memory:")
    con.execute("SET TimeZone = 'UTC'")
    d = cfg.duckdb
    con.execute(f"SET threads = {int(d.threads)}")
    con.execute("SET memory_limit = ?", [d.memory_limit])
    if temp_dir is None:
        con.execute("SET temp_directory = ''")
    else:
        temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute("SET temp_directory = ?", [str(temp_dir)])
        con.execute("SET max_temp_directory_size = ?", [d.max_temp_directory_size])
    con.execute(f"SET preserve_insertion_order = {str(d.preserve_insertion_order).lower()}")
    return con


def sql_str(value: str) -> str:
    """Quotes a string literal for DuckDB SQL (paths and timestamps only)."""
    return "'" + value.replace("'", "''") + "'"


def sql_list(paths: list[Path]) -> str:
    return "[" + ", ".join(sql_str(str(p)) for p in paths) + "]"


def copy_atomic(con: duckdb.DuckDBPyConnection, query: str, final: Path,
                options: str = "FORMAT parquet, COMPRESSION zstd") -> int:
    """Runs `COPY (query) TO final` through a committed tmp file; returns the row count."""
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(final.name + TMP_SUFFIX)
    try:
        (rows,) = con.execute(f"COPY ({query}) TO {sql_str(str(tmp))} ({options})").fetchone()
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    commit_file(tmp, final)
    return int(rows)
