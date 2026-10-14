"""Crash-safe file writes and DuckDB connections.

A data file is committed only by renaming `<name>.tmp` to `<name>` after fsync,
so a reader never sees a half-written file and a re-run redoes exactly the
unfinished work.
"""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from isel.config import Config

TMP_SUFFIX = ".tmp"
_SECRET_RE = re.compile(r"\b(password|passwd|secret|token)\b|://[^/\s]*:[^@/\s]*@", re.I)


class SecretLeak(ValueError):
    """Raised when metadata would store something that looks like a credential."""


def code_version() -> str:
    """The git revision baked into the image (`ISEL_CODE_VERSION`), or `dev` outside it."""
    return os.environ.get("ISEL_CODE_VERSION", "dev")


def utc_id(now: datetime | None = None) -> str:
    """Returns a sortable, microsecond-unique UTC id such as `20261001T050000123456Z`."""
    return (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%S%fZ")


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def commit_file(tmp: Path, final: Path) -> None:
    """Durably renames `tmp` to `final`."""
    with tmp.open("rb") as fh:
        os.fsync(fh.fileno())
    os.replace(tmp, final)
    _fsync_dir(final.parent)


def assert_no_secrets(payload: Any) -> None:
    """Rejects metadata containing credentials (PR #4 stored a DSN this way)."""
    if _SECRET_RE.search(json.dumps(payload, default=str)):
        raise SecretLeak("metadata contains a credential-like value")


def atomic_write_json(path: Path, obj: Any) -> None:
    """Writes JSON so readers see either the old or the new content."""
    assert_no_secrets(obj)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + TMP_SUFFIX)
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, sort_keys=True, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def write_parquet_atomic(table: pa.Table, final: Path, **options: Any) -> int:
    """Writes `table` to `final` through a committed tmp file; returns the row count."""
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(final.name + TMP_SUFFIX)
    pq.write_table(table, tmp, compression="zstd", **options)
    commit_file(tmp, final)
    return table.num_rows


class AtomicParquetWriter:
    """Streams row groups into `final`; the file appears only after a clean close."""

    def __init__(self, final: Path, schema: pa.Schema) -> None:
        final.parent.mkdir(parents=True, exist_ok=True)
        self._final = final
        self._tmp = final.with_name(final.name + TMP_SUFFIX)
        self._writer = pq.ParquetWriter(self._tmp, schema, compression="zstd")
        self.rows = 0

    def write(self, table: pa.Table) -> None:
        if table.num_rows:
            self._writer.write_table(table)
            self.rows += table.num_rows

    def __enter__(self) -> AtomicParquetWriter:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, *exc: object) -> None:
        self._writer.close()
        if exc_type is None:
            commit_file(self._tmp, self._final)
        else:
            self._tmp.unlink(missing_ok=True)


def open_duckdb(cfg: Config, temp_dir: Path) -> duckdb.DuckDBPyConnection:
    """Opens an in-memory DuckDB bounded by the configured memory, threads and spill."""
    con = duckdb.connect(database=":memory:")
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"SET threads = {int(cfg.duckdb.threads)}")
    con.execute("SET memory_limit = ?", [cfg.duckdb.memory_limit])
    temp_dir.mkdir(parents=True, exist_ok=True)
    con.execute("SET temp_directory = ?", [str(temp_dir)])
    con.execute("SET max_temp_directory_size = ?", [cfg.duckdb.max_temp_directory_size])
    return con


def sql_str(value: str) -> str:
    """Quotes a string literal for DuckDB SQL (paths, regexes and timestamps only)."""
    return "'" + value.replace("'", "''") + "'"


def copy_atomic(con: duckdb.DuckDBPyConnection, query: str, final: Path) -> int:
    """Runs `COPY (query) TO final` through a committed tmp file; returns the row count."""
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(final.name + TMP_SUFFIX)
    try:
        (rows,) = con.execute(
            f"COPY ({query}) TO {sql_str(str(tmp))} (FORMAT parquet, COMPRESSION zstd)"
        ).fetchone()
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    commit_file(tmp, final)
    return int(rows)
