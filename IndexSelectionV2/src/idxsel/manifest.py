"""Crash-safe file writes and the on-disk layout of exports, runs and state.

A data file is committed only by renaming `<name>.tmp` to `<name>` after fsync.
Anything still named `.tmp` after a crash is garbage and gets deleted, so a
re-run redoes exactly the unfinished work. One process at a time may change the
work directory; `work_lock` enforces that.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

TMP_SUFFIX = ".tmp"


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


def atomic_write_json(path: Path, obj: Any) -> None:
    """Writes JSON so readers see either the old or the new content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + TMP_SUFFIX)
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2, sort_keys=True, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def write_parquet_atomic(table: pa.Table, final: Path, **options: Any) -> int:
    """Writes `table` to `final` through a committed tmp file; returns the row count."""
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(final.name + TMP_SUFFIX)
    pq.write_table(table, tmp, **options)
    commit_file(tmp, final)
    return table.num_rows


class AtomicParquetWriter:
    """Streams row groups into `final`; the file appears only after a clean close."""

    def __init__(self, final: Path, schema: pa.Schema, **options: Any) -> None:
        final.parent.mkdir(parents=True, exist_ok=True)
        self._final = final
        self._tmp = final.with_name(final.name + TMP_SUFFIX)
        self._writer = pq.ParquetWriter(self._tmp, schema, **options)
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


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def remove_tmp_files(root: Path) -> int:
    """Deletes leftover `.tmp` files under `root`; returns how many."""
    if not root.exists():
        return 0
    count = 0
    for p in root.rglob(f"*{TMP_SUFFIX}"):
        p.unlink(missing_ok=True)
        count += 1
    return count


def parquet_ok(path: Path) -> bool:
    """True if `path` is a readable Parquet file with a valid footer."""
    if not path.exists():
        return False
    try:
        pq.ParquetFile(path).metadata  # noqa: B018 - reading the footer is the check
    except Exception:
        return False
    return True


def dir_size_bytes(root: Path) -> int:
    if not root.exists():
        return 0
    total = 0
    for p in root.rglob("*"):
        with contextlib.suppress(FileNotFoundError):
            if p.is_file():
                total += p.stat().st_size
    return total


class WorkDirBusy(RuntimeError):
    """Raised when another process holds a work-directory lock."""


@contextlib.contextmanager
def work_lock(lock_file: Path, holder: str) -> Iterator[None]:
    """Holds an exclusive, non-blocking flock on `lock_file`.

    flock belongs to the open file, so a second acquisition inside the same
    process conflicts too: take it only at process entry points.

    Raises:
        WorkDirBusy: If another holder has the lock; its note is in the message.
    """
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_file, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            note = os.pread(fd, 512, 0).decode(errors="replace")
            raise WorkDirBusy(f"{lock_file.name} is held by {note or 'another process'}") from None
        os.ftruncate(fd, 0)
        os.pwrite(fd, json.dumps({"pid": os.getpid(), "holder": holder}).encode(), 0)
        yield
    finally:
        os.close(fd)


class Layout:
    """Paths under the work directory."""

    def __init__(self, work_dir: str | Path) -> None:
        self.root = Path(work_dir)

    @property
    def exports(self) -> Path:
        return self.root / "exports"

    @property
    def state_file(self) -> Path:
        return self.root / "state" / "exports.json"

    @property
    def schedule_file(self) -> Path:
        return self.root / "state" / "schedule.json"

    def base_dir(self, export_id: str) -> Path:
        return self.exports / f"base_{export_id}"

    def delta_dir(self, export_id: str) -> Path:
        return self.exports / f"delta_{export_id}"

    def dims_dir(self, export_id: str) -> Path:
        return self.exports / f"dims_{export_id}"

    def golden_dir(self, golden_id: str) -> Path:
        return self.root / "golden" / golden_id

    def features_dir(self, feature_id: str) -> Path:
        return self.root / "features" / feature_id

    def run_dir(self, run_id: str) -> Path:
        return self.root / "runs" / run_id

    def backtest_dir(self, backtest_id: str) -> Path:
        return self.root / "backtests" / backtest_id

    @property
    def lock_file(self) -> Path:
        return self.root / "state" / "lock"

    @property
    def daemon_lock_file(self) -> Path:
        return self.root / "state" / "daemon.lock"

    @property
    def duckdb_tmp(self) -> Path:
        return self.root / "duckdb_tmp"


def load_state(layout: Layout) -> dict[str, Any]:
    """Returns the export state: latest base, deltas, per-shard history watermarks."""
    return read_json(
        layout.state_file,
        {"base": None, "deltas": [], "dims": None, "watermarks": {}, "cycles_since_full": 0,
         "in_progress": {}},
    )


def save_state(layout: Layout, state: dict[str, Any]) -> None:
    atomic_write_json(layout.state_file, state)
