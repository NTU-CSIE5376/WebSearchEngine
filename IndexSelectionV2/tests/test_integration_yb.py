"""The crawlerdb read path against a throwaway local YugabyteDB, checked against Postgres.

Run with `uv run pytest -m yugabyte`. It starts the production YugabyteDB version
and the `postgres:16` container of `test_integration_pg`, loads the same rows into
both, and requires the two full exports to be equal row for row. Nothing here
touches a course server: both containers listen on 127.0.0.1 only and are removed
when the module ends.
"""

from __future__ import annotations

import subprocess
import time
import uuid

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import BASE, ROOT, replace
from test_integration_pg import (  # noqa: F401 - `dsn` is the Postgres fixture
    ALLOWED,
    SHARDS,
    _free_port,
    _load,
    _urls,
    dsn,
)

from isel.config import load_config
from isel.pg import ReadGuard, open_crawlerdb
from isel.pull import FULL_SCHEMA, estimate_full, full_sql, pull_dims, pull_full

pytestmark = pytest.mark.yugabyte

YB_IMAGE = "yugabytedb/yugabyte:2026.1.2.0-b137"   # the course cluster's version
FOLLOWER_STALENESS_S = 30                           # yb_follower_read_staleness_ms default


@pytest.fixture(scope="module")
def yb_dsn():
    name = f"isel-yb-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    subprocess.run(["docker", "run", "-d", "--rm", "--name", name,
                    "-p", f"127.0.0.1:{port}:5433", YB_IMAGE,
                    "bin/yugabyted", "start", "--background=false"],
                   check=True, capture_output=True)
    url = f"postgresql://yugabyte@127.0.0.1:{port}/yugabyte"
    try:
        deadline = time.monotonic() + 180
        while True:
            try:
                psycopg.connect(url, connect_timeout=2).close()
                break
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        _load(url)
        # Follower reads see the data as of yb_follower_read_staleness_ms (30 s) ago,
        # so rows written just now are not visible yet.
        time.sleep(FOLLOWER_STALENESS_S + 5)
        yield url
    finally:
        subprocess.run(["docker", "stop", name], check=False, capture_output=True)


def _cfg(tmp_path, monkeypatch, url, engine):
    overlays = [ROOT / "config/crawlerdb/yugabyte.toml"] if engine == "yugabyte" else []
    cfg = load_config([BASE, *overlays])
    monkeypatch.setenv(cfg.crawlerdb.dsn_env, url)
    cfg = replace(cfg, "run", work_dir=str(tmp_path))
    cfg = replace(cfg, "crawlerdb", num_shards=SHARDS)
    cfg = replace(cfg, "routing", overrides={})
    cfg = replace(cfg, "pull", min_interval_s=0.0)
    return replace(cfg, "full", chunk_blocks=1, hash_chunks=8, chunks_per_file=3,
                   min_free_gb=0.0)


@pytest.fixture
def yb_cfg(tmp_path, monkeypatch, yb_dsn):
    return _cfg(tmp_path, monkeypatch, yb_dsn, "yugabyte")


def _session(cfg):
    session = open_crawlerdb(cfg)
    session.guard = ReadGuard(cfg.guard, session.io_waiters, now=lambda: ALLOWED)
    return session


def _export(cfg, out, connections=2):
    sessions = [_session(cfg) for _ in range(connections)]
    try:
        summary = pull_full(cfg, sessions, out, now=ALLOWED)
    finally:
        for session in sessions:
            session.close()
    table = pa.concat_tables([pq.read_table(p) for p in sorted(out.glob("shard=*/part-*.parquet"))])
    return summary, table.cast(FULL_SCHEMA).sort_by("url")


def test_every_session_setting_applies_on_yugabyte(yb_cfg, tmp_path):
    session = _session(yb_cfg)
    try:
        for name in yb_cfg.crawlerdb.session:
            assert session.fetchone(f"SHOW {name}") is not None, name
        assert session.fetchone("SHOW default_transaction_read_only") == ("on",)
        assert session.fetchone("SHOW yb_read_from_followers") == ("on",)
        assert session.io_waiters() >= 0
        assert pull_dims(yb_cfg, session, tmp_path / "dims")["split_hosts"] == 1
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            session.conn.execute("CREATE TABLE should_not_exist (a int)")
    finally:
        session.close()


def test_a_hash_range_reads_only_its_keys_through_the_primary_key(yb_cfg):
    session = _session(yb_cfg)
    try:
        assert estimate_full(yb_cfg, session)["tservers"] == 1
        copy = full_sql(yb_cfg, 0, 8192, 16384)
        select = copy.removeprefix("COPY (").removesuffix(") TO STDOUT (FORMAT CSV)")
        plan = "\n".join(row[0] for row in session.fetchall(f"EXPLAIN {select}"))
    finally:
        session.close()
    assert "Index Cond" in plan and "yb_hash_code(url)" in plan, plan


def test_yugabyte_and_postgres_exports_are_equal_row_for_row(
        tmp_path, monkeypatch, yb_dsn, dsn):  # noqa: F811
    yb_cfg = _cfg(tmp_path / "yb", monkeypatch, yb_dsn, "yugabyte")
    pg_cfg = _cfg(tmp_path / "pg", monkeypatch, dsn, "postgres")
    yb_out, yb_rows = _export(yb_cfg, tmp_path / "yb" / "export", connections=3)
    pg_out, pg_rows = _export(pg_cfg, tmp_path / "pg" / "export")
    crawled = sorted(u for n, u in enumerate(_urls()) if n % 10 != 0)
    assert yb_out["shards_missing"] == pg_out["shards_missing"] == 0
    assert yb_rows.column("url").to_pylist() == crawled
    assert yb_rows.equals(pg_rows)
