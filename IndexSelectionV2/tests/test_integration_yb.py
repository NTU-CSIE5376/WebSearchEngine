"""The crawlerdb read path against a throwaway local YugabyteDB.

Run with `uv run pytest -m yugabyte`. It starts the course cluster's YugabyteDB
version, loads the rows of `test_integration_pg`, and checks the full export
against psycopg's own decoding of the same rows. Nothing here touches a course
server: the container listens on 127.0.0.1 only and is removed when the module ends.
"""

from __future__ import annotations

import subprocess
import time
import uuid

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import replace
from test_integration_pg import ALLOWED, SHARDS, _free_port, _load, _urls

from isel.pg import ReadGuard, ReadOnlyViolation, open_crawlerdb
from isel.pull import CRAWL_COLUMNS, FULL_SCHEMA, estimate_full, full_sql, pull_dims, pull_full

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


@pytest.fixture
def cfg(base_cfg, tmp_path, monkeypatch, yb_dsn):
    monkeypatch.setenv(base_cfg.crawlerdb.dsn_env, yb_dsn)
    cfg = replace(base_cfg, "run", work_dir=str(tmp_path))
    cfg = replace(cfg, "crawlerdb", num_shards=SHARDS)
    cfg = replace(cfg, "routing", overrides={})
    cfg = replace(cfg, "pull", min_interval_s=0.0)
    return replace(cfg, "full", hash_chunks=8, chunks_per_file=3, min_free_gb=0.0)


def _session(cfg):
    session = open_crawlerdb(cfg)
    session.guard = ReadGuard(cfg.guard, session.io_waiters, now=lambda: ALLOWED)
    return session


def _export(cfg, out, connections):
    sessions = [_session(cfg) for _ in range(connections)]
    try:
        summary = pull_full(cfg, sessions, out, now=ALLOWED)
    finally:
        for session in sessions:
            session.close()
    table = pa.concat_tables([pq.read_table(p) for p in sorted(out.glob("shard=*/part-*.parquet"))])
    return summary, table.cast(FULL_SCHEMA).sort_by("url")


def test_every_session_setting_applies_and_the_gate_holds(cfg, tmp_path):
    session = _session(cfg)
    try:
        for name in cfg.crawlerdb.session:
            assert session.fetchone(f"SHOW {name}") is not None, name
        assert session.fetchone("SHOW default_transaction_read_only") == ("on",)
        assert session.fetchone("SHOW yb_read_from_followers") == ("on",)
        assert session.io_waiters() >= 0
        assert pull_dims(cfg, session, tmp_path / "dims")["split_hosts"] == 1
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            session.conn.execute("CREATE TABLE should_not_exist (a int)")
        with pytest.raises(ReadOnlyViolation):
            session.fetchall("SELECT \"set_config\"('default_transaction_read_only', 'off', false)")
    finally:
        session.close()


def test_a_hash_range_reads_only_its_keys_through_the_primary_key(cfg):
    session = _session(cfg)
    try:
        assert estimate_full(cfg, session)["tservers"] == 1
        copy = full_sql(cfg, 0, 8192, 16384)
        select = copy.removeprefix("COPY (").removesuffix(") TO STDOUT (FORMAT CSV)")
        plan = "\n".join(row[0] for row in session.fetchall(f"EXPLAIN {select}"))
    finally:
        session.close()
    assert "Index Cond" in plan and "yb_hash_code(url)" in plan, plan


def test_the_full_export_reads_every_crawled_row_once_as_psycopg_decodes_it(cfg, tmp_path,
                                                                           yb_dsn):
    summary, exported = _export(cfg, tmp_path / "export", connections=3)
    crawled = sorted(u for n, u in enumerate(_urls()) if n % 10 != 0)
    assert summary["shards_missing"] == 0 and summary["rows"] == len(crawled)
    assert exported.column("url").to_pylist() == crawled
    columns = ", ".join(f"{expr} AS {name}" for name, expr, _ in CRAWL_COLUMNS)
    rows = []
    with psycopg.connect(yb_dsn) as conn:
        for shard in range(SHARDS):
            cur = conn.execute(f"SELECT {columns} FROM url_state_current_{shard:03d} "
                               "WHERE last_fetch_ok IS NOT NULL")
            names = [d.name for d in cur.description]
            rows += [{**dict(zip(names, r, strict=True)), "shard": shard} for r in cur]
    decoded = pa.Table.from_pylist(rows, schema=FULL_SCHEMA).sort_by("url")
    assert exported.equals(decoded)
    assert sum("\n" in u for u in exported.column("url").to_pylist()) == 9
