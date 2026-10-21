"""The read and selectdb paths against a throwaway local `postgres:16` container.

Run with `uv run pytest -m integration`. Nothing here touches a course server:
the container listens on 127.0.0.1 only and is removed when the module ends.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import socket
import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from alembic import command
from conftest import replace
from recrawl_fakes import ApiStub, FakeCrawler, Page, crawler_of

from isel.cli import main as cli_main
from isel.golden import golden_table, pull_golden
from isel.io import read_json, write_parquet_atomic
from isel.manifest import write_run
from isel.pg import ReadGuard, ReadOnlyViolation, open_crawlerdb, open_metricdb
from isel.publish import load_selection, rollback, swap, verify_live
from isel.pull import CRAWL_COLUMNS, FULL_SCHEMA, estimate_full, pull_dims, pull_full
from isel.recrawl import (
    CrawlerApi,
    archive_cycle,
    capacity_report,
    host_id,
    pg_doc_id,
    plan_cycle,
    sync,
)
from isel.selectdb import (
    ForbiddenTarget,
    NeedsBaselineStamp,
    _alembic_config,
    connect,
    init_role,
    migrate,
)
from isel.urlnorm import DomainKeyer, ShardRouter

pytestmark = pytest.mark.integration

SHARDS = 4
ALLOWED = datetime(2026, 10, 8, 5, 20, tzinfo=UTC)   # inside a read window
FETCHED = datetime(2026, 9, 1, tzinfo=UTC)
URL_TABLE = """CREATE TABLE url_state_current_{:03d} (
    url text PRIMARY KEY, domain_id bigint, last_fetch_ok timestamptz, title text,
    anchor_text text, discovery_source_type smallint, robots_bits smallint,
    is_redirect boolean, redirect_hop_count integer, has_json_ld boolean,
    hreflang_count integer, last_modified text, cache_control text,
    inlink_count_approx bigint, inlink_count_external bigint, url_score double precision,
    source smallint, num_fetch_ok_90d integer)"""


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _router() -> ShardRouter:
    return ShardRouter(DomainKeyer([], offline=True), SHARDS, {})


def _urls() -> list[str]:
    plain = [f"https://site{d}.com/p{i}" for d in range(40) for i in range(25)]
    # Quotes, commas, newlines, backslashes and CJK, so the CSV full export is checked
    # against psycopg's own decoding; ten of them keep len(_urls()) a multiple of 10.
    tricky = [f'https://tricky{i}.example/a?q="x,{i}"\n\\p/中文/NULL' for i in range(10)]
    return plain + tricky


def _load(dsn: str) -> None:
    router = _router()
    with psycopg.connect(dsn, autocommit=True) as conn:
        for shard in range(SHARDS):
            conn.execute(URL_TABLE.format(shard))
        for n, url in enumerate(_urls()):
            shard = router.shard_for_key(router.keyer.key(url))
            fetched = None if n % 10 == 0 else FETCHED + timedelta(minutes=n, microseconds=7 * n)
            conn.execute(
                f"INSERT INTO url_state_current_{shard:03d} VALUES "
                "(%s, %s, %s, %s, %s, %s, 1, %s, 0, true, 0, NULL, 'max-age=60', %s, %s, %s, "
                "0, 1)", (url, n // 25, fetched, "t" * 500, "a" if n % 2 else None,
                          None if n % 4 == 0 else n % 5, None if n % 5 == 0 else n % 3 == 0,
                          n % 7, n % 3, n / 3))
        conn.execute("CREATE TABLE shard_split_subdomain (host text, migrated_at timestamptz)")
        conn.execute("INSERT INTO shard_split_subdomain VALUES ('news.site1.com', NULL)")
        conn.execute("CREATE TABLE metric_batches (id bigint, created_at timestamptz)")
        conn.execute("CREATE TABLE metric_queries (id bigint, batch_id bigint, keyword text, "
                     "frequency bigint, tags text[])")
        conn.execute("CREATE TABLE metric_url (query_id bigint, rank integer, url text)")
        conn.execute("INSERT INTO metric_batches VALUES (1, '2026-09-16 12:00+00')")
        conn.execute("INSERT INTO metric_queries VALUES (1, 1, 'q1', 900, '{head}'), "
                     "(2, 1, 'q2', 50, '{}')")
        conn.execute("INSERT INTO metric_url VALUES (1, 1, 'https://site1.com/p1'), "
                     "(1, 2, 'https://nowhere.example/')")
        conn.execute("ANALYZE")


@pytest.fixture(scope="module")
def dsn():
    name = f"isel-it-{uuid.uuid4().hex[:8]}"
    port = _free_port()
    password = uuid.uuid4().hex
    subprocess.run(["docker", "run", "-d", "--rm", "--name", name, "-e",
                    f"POSTGRES_PASSWORD={password}", "-p", f"127.0.0.1:{port}:5432",
                    "postgres:16"], check=True, capture_output=True)
    url = f"postgresql://postgres:{password}@127.0.0.1:{port}/postgres"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                psycopg.connect(url, connect_timeout=2).close()
                break
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)
        _load(url)
        yield url
    finally:
        subprocess.run(["docker", "stop", name], check=False, capture_output=True)


@pytest.fixture
def cfg(base_cfg, tmp_path, dsn, monkeypatch):
    monkeypatch.setenv(base_cfg.crawlerdb.dsn_env, dsn)
    monkeypatch.setenv(base_cfg.metricdb.dsn_env, dsn)
    cfg = replace(base_cfg, "run", work_dir=str(tmp_path))
    cfg = replace(cfg, "crawlerdb", num_shards=SHARDS)
    cfg = replace(cfg, "routing", overrides={})
    return replace(cfg, "pull", min_interval_s=0.0)


def _session(cfg, opener=open_crawlerdb):
    session = opener(cfg)
    session.guard = ReadGuard(cfg.guard, session.io_waiters, now=lambda: ALLOWED)
    return session


def test_dims_read_real_rows(cfg, tmp_path):
    session = _session(cfg)
    try:
        assert session.io_waiters() >= 0
        assert pull_dims(cfg, session, tmp_path / "dims")["split_hosts"] == 1
    finally:
        session.close()


def test_the_server_refuses_writes_and_the_gate_stops_bypasses(cfg):
    session = _session(cfg)
    try:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            session.conn.execute("CREATE TABLE should_not_exist (a int)")
        with pytest.raises(ReadOnlyViolation):
            session.fetchall("SELECT \"set_config\"('default_transaction_read_only', 'off', false)")
        session.set_statement_timeout(300)
        assert session.fetchone("SHOW statement_timeout") == ("5min",)
        assert session.fetchone("SHOW default_transaction_read_only") == ("on",)
    finally:
        session.close()


def test_golden_pull_reads_metricdb_tables(cfg, tmp_path):
    session = _session(cfg, open_metricdb)
    try:
        summary = pull_golden(session, tmp_path / "golden", cfg.golden.tags)
    finally:
        session.close()
    assert summary["1"]["head"]["distinct_raw_urls"] == 2


def _files(root):
    return pa.concat_tables([pq.read_table(p) for p in sorted(root.glob("shard=*/part-*.parquet"))])


def test_full_export_reads_every_crawled_row_once(cfg, tmp_path, dsn):
    cfg = replace(cfg, "full", chunk_blocks=1, chunks_per_file=3, min_free_gb=0.0)
    sessions = [_session(cfg), _session(cfg)]
    try:
        assert estimate_full(cfg, sessions[0])["chunks"] > SHARDS
        out = pull_full(cfg, sessions, tmp_path / "export", now=ALLOWED)
    finally:
        for session in sessions:
            session.close()
    exported = _files(tmp_path / "export")
    crawled = [u for n, u in enumerate(_urls()) if n % 10 != 0]
    assert out["shards_missing"] == 0 and out["rows"] == exported.num_rows == len(crawled)
    assert sorted(exported.column("url").to_pylist()) == sorted(crawled)
    assert exported.schema.equals(FULL_SCHEMA, check_metadata=False)
    meta = read_json(tmp_path / "export" / "_meta.json")
    assert "finished_at" in meta and "end_probe" in meta
    assert set(meta["io"]) == {str(s) for s in range(SHARDS)}


def test_the_csv_full_export_equals_psycopg_decoding(cfg, tmp_path, dsn):
    cfg = replace(cfg, "full", chunk_blocks=1, chunks_per_file=3, min_free_gb=0.0)
    session = _session(cfg)
    try:
        pull_full(cfg, [session], tmp_path / "export", now=ALLOWED)
    finally:
        session.close()
    columns = ", ".join(f"{expr} AS {name}" for name, expr, _ in CRAWL_COLUMNS)
    rows = []
    with psycopg.connect(dsn) as conn:
        for shard in range(SHARDS):
            cur = conn.execute(f"SELECT {columns} FROM url_state_current_{shard:03d} "
                               "WHERE last_fetch_ok IS NOT NULL")
            names = [d.name for d in cur.description]
            rows += [{**dict(zip(names, r, strict=True)), "shard": shard} for r in cur]
    decoded = pa.Table.from_pylist(rows, schema=FULL_SCHEMA)
    csv = _files(tmp_path / "export").cast(FULL_SCHEMA)
    assert csv.sort_by("url").equals(decoded.sort_by("url"))
    assert sum("\n" in u for u in csv.column("url").to_pylist()) == 9


OPEN = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
FROZEN = datetime(2026, 10, 16, 12, 0, tzinfo=UTC)


@pytest.fixture
def selectdb(cfg, dsn, monkeypatch):
    """A fresh database in the throwaway container standing in for our selectdb."""
    name = f"selectdb_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')
    server = dsn.rsplit("/", 1)[0]
    password = uuid.uuid4().hex
    monkeypatch.setenv(cfg.selectdb.admin_dsn_env, f"{server}/{name}")
    monkeypatch.setenv(cfg.selectdb.reader_password_env, password)
    host = server.split("@")[1]
    monkeypatch.setenv(cfg.selectdb.reader_dsn_env, f"postgresql://select:{password}@{host}/{name}")
    return cfg


# The tables `init-selectdb` made before Alembic, and the state a load and a swap left
# on the live selectdb (2026-10-12): current carries run 1's names and default.
LEGACY = """
CREATE TABLE model_runs (id BIGSERIAL PRIMARY KEY, run_label TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), cutoff_datetime TIMESTAMPTZ, model_path TEXT,
    train_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    eval_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    selection_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_paths JSONB NOT NULL DEFAULT '{}'::jsonb, status TEXT NOT NULL DEFAULT 'loading',
    activated_at TIMESTAMPTZ, config_sha256 TEXT, code_version TEXT);
CREATE TABLE selected_urls_prev (url TEXT PRIMARY KEY, score DOUBLE PRECISION NOT NULL,
    first_seen TIMESTAMPTZ,
    run_id BIGINT NOT NULL REFERENCES model_runs(id) ON DELETE CASCADE,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
ALTER TABLE selected_urls_prev RENAME CONSTRAINT selected_urls_prev_pkey
    TO selected_urls_current_pkey;
ALTER TABLE selected_urls_prev RENAME CONSTRAINT selected_urls_prev_run_id_fkey
    TO selected_urls_current_run_id_fkey;
INSERT INTO model_runs (run_label, status, activated_at) VALUES ('v2', 'live', now());
CREATE TABLE selected_urls_current (url TEXT NOT NULL, score DOUBLE PRECISION NOT NULL,
    first_seen TIMESTAMPTZ, run_id BIGINT NOT NULL DEFAULT 1,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT now());
INSERT INTO selected_urls_current (url, score) VALUES ('https://v2.example/', 1.0);
ALTER TABLE selected_urls_current ADD CONSTRAINT selected_urls_r1_pkey PRIMARY KEY (url);
ALTER TABLE selected_urls_current ADD CONSTRAINT selected_urls_r1_run_fk FOREIGN KEY (run_id)
    REFERENCES model_runs(id) ON DELETE CASCADE;
"""


def _admin(cfg):
    return connect(cfg, cfg.selectdb.admin_dsn_env, read_only=False)


def _setup(cfg):
    """migrate-selectdb as the CLI runs it on an empty selectdb."""
    result = migrate(_admin(cfg))
    admin = _admin(cfg)
    try:
        init_role(cfg, admin)
    finally:
        admin.close()
    return result


def _constraints(conn, table):
    cur = conn.execute("SELECT conname, contype, confdeltype FROM pg_constraint "
                       "WHERE conrelid = %s::regclass ORDER BY conname", (f"public.{table}",))
    return [(n, t, d if t == "f" else None) for n, t, d in cur.fetchall()]


def _list_constraints(table):
    return [(f"{table}_pkey", "p", None), (f"{table}_run_id_fkey", "f", "r")]


def test_migrate_adopts_the_pre_alembic_selectdb(selectdb):
    cfg = selectdb
    admin = _admin(cfg)
    admin.execute(LEGACY)
    admin.close()
    with pytest.raises(NeedsBaselineStamp):
        migrate(_admin(cfg))
    first = migrate(_admin(cfg), stamp_baseline=True)
    assert first == {"revision_before": None, "revision": "0003", "stamped_baseline": True}
    assert migrate(_admin(cfg))["revision_before"] == "0003"     # a rerun changes nothing
    with pytest.raises(RuntimeError, match="only fits a pre-Alembic"):
        migrate(_admin(cfg), stamp_baseline=True)
    admin = _admin(cfg)
    try:
        for table in ("selected_urls_current", "selected_urls_prev"):
            assert _constraints(admin, table) == _list_constraints(table)
            (default,) = admin.execute(
                "SELECT column_default FROM information_schema.columns WHERE table_name = %s "
                "AND column_name = 'run_id'", (table,)).fetchone()
            assert default is None
        assert admin.execute("SELECT count(*) FROM selected_urls_current").fetchone() == (1,)
        with pytest.raises(psycopg.errors.CheckViolation):
            admin.execute("UPDATE model_runs SET status = 'liv'")
        with pytest.raises(psycopg.errors.UniqueViolation):
            admin.execute("INSERT INTO model_runs (run_label, status) VALUES ('x', 'live')")
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            admin.execute("DELETE FROM model_runs")              # RESTRICT keeps the list
    finally:
        admin.close()


def test_migrate_builds_an_empty_selectdb_and_downgrades(selectdb):
    cfg = selectdb
    assert _setup(cfg)["revision"] == "0003"
    dsn = os.environ[cfg.selectdb.admin_dsn_env]
    engine = sa.create_engine(dsn.replace("postgresql://", "postgresql+psycopg://"))
    acfg = _alembic_config()
    with engine.begin() as conn:
        acfg.attributes["connection"] = conn
        command.downgrade(acfg, "base")
        assert sa.inspect(conn).get_table_names() == ["alembic_version"]
        assert "recrawl" not in sa.inspect(conn).get_schema_names()
        command.upgrade(acfg, "head")
        command.check(acfg)
    engine.dispose()


def test_the_reader_role_can_only_read(selectdb):
    cfg = selectdb
    _setup(cfg)
    reader = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=False)
    try:
        assert reader.execute("SELECT count(*) FROM selected_urls_current").fetchone() == (0,)
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("INSERT INTO model_runs (run_label) VALUES ('x')")
    finally:
        reader.close()


def test_the_reader_role_cannot_see_the_recrawl_schema(selectdb):
    cfg = selectdb
    _setup(cfg)
    reader = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=True)
    try:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM recrawl.doc")
    finally:
        reader.close()


def test_publish_refuses_a_selectdb_behind_the_newest_revision(selectdb, tmp_path):
    cfg = selectdb
    admin = _admin(cfg)
    admin.execute(LEGACY)
    try:
        with pytest.raises(RuntimeError, match="run `isel migrate-selectdb`"):
            load_selection(cfg, admin, _selection(cfg, tmp_path, "r1", ["https://a.example/"]),
                           tmp_path)
    finally:
        admin.close()


def _selection(cfg, work, run_id, urls, shards=None):
    con = duckdb.connect()
    con.register("rows", pa.table({
        "url": urls, "crawl_shard": pa.array(shards or [0] * len(urls), pa.int16()),
        "domain_id": pa.array([1] * len(urls), pa.int64()),
        "score": [float(len(urls) - i) for i in range(len(urls))],
        "reason": pa.nulls(len(urls), pa.list_(pa.string()))}))
    sel = work / "full" / "selection"
    write_run(cfg, con, "SELECT * FROM rows", sel, run_id, {
        "scope": "full", "k": len(urls), "candidates": len(urls), "tau": 0.0,
        "model_dir": "/work/models/m0", "snapshot_at": "2026-10-09T05:39:22+00:00",
        "target_batch": 20, "model_meta": {"best_iteration": 7}, "code_version": "test"})
    return sel / f"run={run_id}"


def _count(conn, table="selected_urls_current"):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


def test_publish_swap_freeze_rollback_and_the_metric_query(selectdb, tmp_path):
    cfg = selectdb
    first_urls = ["https://site1.com/p1", "https://a.example/x?q=1,2", 'https://b.example/"q"']
    _setup(cfg)
    admin = _admin(cfg)
    try:
        first = load_selection(cfg, admin, _selection(cfg, tmp_path, "r1", first_urls), tmp_path)
        assert _count(admin) == 0                       # loaded, not visible yet
        assert swap(cfg, admin, first["db_run_id"], now=lambda: OPEN)["swapped"]
        assert _count(admin) == 3
        second = load_selection(cfg, admin, _selection(
            cfg, tmp_path, "r2", ["https://site2.com/p2", "https://site1.com/p1"]), tmp_path)
        frozen = swap(cfg, admin, second["db_run_id"], now=lambda: FROZEN)
        assert frozen == {"swapped": False, "reason": "swap freeze window", "retry": True}
        assert swap(cfg, admin, second["db_run_id"], now=lambda: OPEN)["swapped"]
        assert _count(admin) == 2
        assert admin.execute("SELECT DISTINCT run_id FROM selected_urls_current").fetchall() == [
            (second["db_run_id"],)]
        for table in ("selected_urls_current", "selected_urls_prev"):
            assert _constraints(admin, table) == _list_constraints(table)
        assert rollback(cfg, admin)["live_run"] == first["db_run_id"]
        assert _count(admin) == 3
        for table in ("selected_urls_current", "selected_urls_prev"):
            assert _constraints(admin, table) == _list_constraints(table)
        (default,) = admin.execute(
            "SELECT column_default FROM information_schema.columns WHERE table_name = "
            "'selected_urls_current' AND column_name = 'run_id'").fetchone()
        assert default is None
    finally:
        admin.close()
    write_parquet_atomic(golden_table([
        (20, OPEN, 1, "q", 1, ["head"], 1, "https://site1.com/p1"),
        (20, OPEN, 1, "q", 1, ["head"], 2, "https://a.example/x?q=1,2"),
        (20, OPEN, 2, "r", 1, ["head"], 1, "https://nowhere.example/")]),
        tmp_path / "golden" / "golden.parquet")
    result = verify_live(cfg, tmp_path, 20)
    head = result["tags"]["head"]
    assert head["golden_urls"] == 3 and head["indexed"] == 2
    assert head["indexed_exact_rate"] == pytest.approx(2 / 3)
    # The metric looks up only q=1%2C2, while the list holds the raw q=1,2.
    assert head["indexed_metric_rate"] == pytest.approx(1 / 3)
    assert [r["run_label"] for r in result["live_runs"]] == ["r1"]
    reader = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=True)
    try:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            reader.execute("DELETE FROM selected_urls_current")
    finally:
        reader.close()


def test_writes_refuse_a_database_with_crawler_tables(selectdb, dsn, monkeypatch):
    monkeypatch.setenv(selectdb.selectdb.admin_dsn_env, dsn)   # holds url_state_current_000
    with pytest.raises(ForbiddenTarget, match="crawler or metric tables"):
        connect(selectdb, selectdb.selectdb.admin_dsn_env, read_only=False)


# ---------------------------------------------------------------- recrawl

A1, A2, A3, B1 = ("https://a.example/1", "https://a.example/2", "https://a.example/3",
                  "https://b.example/1")
C1 = "https://c.example/1"
T0 = OPEN
ARTICLE = Page(body=("<html><head><title>颱風假</title><meta name=description content='停班停課'>"
                     "</head><body><nav>首頁 新聞</nav><main><p>台北市與新北市宣布明天停止上班上課，"
                     "氣象署表示颱風將在今晚登陸東部，民眾應注意強風豪雨並做好防颱準備。</p></main>"
                     "</body></html>").encode())
ENGLISH = Page(body=b"<html><head><title>News</title></head><body><main><p>The city council "
               b"approved the new budget on Tuesday after a long debate about schools, roads and "
               b"public transport.</p></main></body></html>")


@pytest.fixture
def stub(tmp_path):
    s = ApiStub(tmp_path / "manual_bulk_crawls")
    yield s
    s.close()


def _recrawl_cfg(cfg, tmp_path, stub, **kw):
    root = tmp_path / "recrawl"
    values = {"api_url": stub.url, "result_root": str(stub.results),
              "text_root": str(root / "text"), "log_root": str(root / "log"), "job_size": 4,
              "host_docs_per_job": 2, "max_jobs_inflight": 3, "plan_max_jobs": 2,
              "min_free_gb": 0, "archive_workers": 1, "status_poll_s": 0}
    return replace(cfg, "recrawl", **{**values, **kw})


def _api(stub, timeout_s=2.0):
    return CrawlerApi(stub.url, timeout_s)


def _poll(cfg, admin, stub, at):
    """A plan cycle that only asks the API about its jobs: the text disk looks full."""
    return plan_cycle(replace(cfg, "recrawl", min_free_gb=10**9), admin, at, _api(stub))


def _go_live(cfg, admin, work, run_id, urls):
    loaded = load_selection(cfg, admin, _selection(cfg, work, run_id, urls), work)
    assert swap(cfg, admin, loaded["db_run_id"], now=lambda: OPEN)["swapped"]


def _live(cfg, admin, work, urls):
    _go_live(cfg, admin, work, "r1", urls)
    sync(cfg, admin, now=lambda: OPEN)


def _doc(admin, url, *cols):
    return admin.execute(f"SELECT {', '.join(cols)} FROM recrawl.doc WHERE doc_id = %s",
                         (pg_doc_id(url),)).fetchone()


def _host(admin, host):
    return admin.execute("SELECT batch_id, next_at FROM recrawl.host WHERE host_id = %s",
                         (host_id(host),)).fetchone()


def _jobs(admin):
    return admin.execute("SELECT batch_id, job_id, state, doc_ids FROM recrawl.job "
                         "ORDER BY batch_id").fetchall()


def test_recrawl_sync_plan_archive_and_revisit(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    r = cfg.recrawl
    api = _api(stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        # A2 twice: the second spelling canonicalizes onto the same doc.
        _go_live(cfg, admin, tmp_path, "r1", [A1, A2, "https://a.example/x/../2", A3, B1, C1])
        synced = sync(cfg, admin, now=lambda: OPEN)
        assert (synced["docs"], synced["dropped"], synced["hosts_due"]) == (5, 0, 3)
        assert capacity_report(cfg, admin)["docs"] == 5

        first = plan_cycle(cfg, admin, T0, api)
        assert (first["jobs"], first["docs"], first["refused"], first["unknown"]) == (1, 4, 0, 0)
        # Hosts fill the job in rank order, at most two docs each; their URLs alternate.
        assert stub.posts == [[A1, B1, C1, A2]]
        ((batch, job_id, state, doc_ids),) = _jobs(admin)
        assert (state, doc_ids) == ("submitted", [pg_doc_id(u) for u in (A1, B1, C1, A2)])
        assert stub.jobs() == [job_id]
        assert _host(admin, "a.example") == (batch, T0 + timedelta(seconds=2 * r.min_gap_s))
        assert _host(admin, "b.example") == (batch, T0 + timedelta(seconds=r.min_gap_s))
        assert plan_cycle(cfg, admin, T0, api)["jobs"] == 0        # A3's host is in a job

        done = T0 + timedelta(minutes=1)
        crawler = FakeCrawler(stub, {A1: ARTICLE, A2: Page(status=503), B1: ENGLISH,
                                     C1: Page(status=None, error="IgnoreRequest")})
        crawler.take(done)
        out = stub.results / job_id
        before = sorted(str(f.relative_to(out)) for f in out.rglob("*"))
        assert {f.parent.name for f in out.rglob("0000*.json")} == {"crawler_00", "crawler_01"}
        # The results dir exists from the POST on; only the API's status says it is done.
        assert archive_cycle(cfg, admin, done)["jobs"] == 0
        assert _poll(cfg, admin, stub, done)["completed"] == 1
        assert archive_cycle(cfg, admin, done) == {
            "jobs": 1, "records": 4, "applied": 4, "texts": 2, "errors": 0, "io_error": 0}
        text_file = f"fetch_date=20261015/job-{batch}.parquet"
        state, stored, lang, due, sha = _doc(admin, A1, "state", "text_file", "lang",
                                             "due_at", "payload_sha1")
        assert (state, stored, lang, due) == (
            "fetched", text_file, "zh", done + timedelta(days=r.stale_days))
        assert sha == hashlib.sha1(ARTICLE.body).hexdigest()
        rows = {x["url"]: x for x in pq.read_table(Path(r.text_root) / text_file).to_pylist()}
        manifest = pq.read_table(tmp_path / "full" / "selection" / "run=r1" / "manifest"
                                 ).to_pylist()
        (a1_id,) = {m["doc_id"] for m in manifest if m["canonical_url"] == A1}
        assert sorted(rows) == [A1, B1] and rows[A1]["doc_id"] == a1_id
        assert (rows[A1]["title"], rows[A1]["description"], rows[A1]["lang"]) == (
            "颱風假", "停班停課", "zh")
        assert "防颱準備" in rows[A1]["body"] and "首頁" not in rows[A1]["body"]
        assert rows[B1]["fetched_at"] == done + timedelta(seconds=1)   # its position in the job
        assert _doc(admin, A2, "state", "attempts", "fail_reason") == ("failed", 1, "http_503")
        assert _doc(admin, C1, "state", "fail_reason", "due_at") == (
            "failed", "ignore_request", done + timedelta(days=r.robots_retry_days))
        assert _host(admin, "a.example") == (None, done + timedelta(seconds=r.retry_base_s))
        assert sorted(str(f.relative_to(out)) for f in out.rglob("*")) == before  # files stay
        with gzip.open(Path(r.log_root) / "20261015" / f"job-{batch}.jsonl.gz", "rt") as fh:
            log = [json.loads(line) for line in fh]
        assert [(x["url"], x["index"], x["outcome"]) for x in log] == [
            (A1, 0, "fetched"), (B1, 1, "fetched"), (C1, 2, "robots"), (A2, 3, "throttled")]
        assert _jobs(admin)[0][2] == "applied"
        assert archive_cycle(cfg, admin, done)["jobs"] == 0      # applied once only

        # The next plan deletes the applied job's results at the API. The throttled host
        # waits, then gets its retry and the doc it had no room for.
        later = done + timedelta(seconds=r.retry_base_s)
        got = plan_cycle(cfg, admin, later, api)
        assert (got["deleted"], got["jobs"]) == (1, 1) and not out.exists()
        assert stub.ttls == [r.result_ttl_days] * 2 and stub.posts[-1] == [A2, A3]
        crawler.web.update({A2: ENGLISH, A3: ENGLISH})
        crawler.take(later)
        _poll(cfg, admin, stub, later)
        assert archive_cycle(cfg, admin, later)["texts"] == 2

        # Fetched docs come due again after stale_days and go out first by rank.
        stale = later + timedelta(days=r.stale_days, seconds=1)
        revisit = plan_cycle(cfg, admin, stale, api)
        assert revisit["swept"] == 4 and stub.posts[-1] == [A1, B1, C1, A2]
    finally:
        admin.close()


def test_recrawl_sync_takes_docs_off_the_list_and_plan_skips_them(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        _go_live(cfg, admin, tmp_path, "r2", [A1])
        assert sync(cfg, admin, now=lambda: OPEN)["dropped"] == 1
        assert _doc(admin, B1, "rank", "state") == (None, "pending")
        assert plan_cycle(cfg, admin, T0, _api(stub))["jobs"] == 1
        assert stub.posts == [[A1]]
        with pytest.raises(RuntimeError, match="freeze window"):
            sync(cfg, admin, now=lambda: FROZEN)
    finally:
        admin.close()


def test_recrawl_plan_caps_open_jobs_and_spaces_a_hosts_jobs(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, job_size=2, host_docs_per_job=1)
    D1, E1 = "https://d.example/1", "https://e.example/1"
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, A2, B1, C1, D1, E1])
        api = _api(stub)
        got = plan_cycle(cfg, admin, T0, api)
        assert (got["jobs"], got["open"]) == (2, 0)                 # plan_max_jobs
        assert stub.posts == [[A1, B1], [C1, D1]]
        assert plan_cycle(cfg, admin, T0, api)["jobs"] == 1         # max_jobs_inflight
        assert stub.posts[-1] == [E1]
        full = plan_cycle(cfg, admin, T0, api)
        assert (full["jobs"], full["open"]) == (0, 3)
        FakeCrawler(stub, {}).finish(_jobs(admin)[0][1], T0)
        _poll(cfg, admin, stub, T0)
        archive_cycle(cfg, admin, T0 + timedelta(seconds=1))
        # A's next job waits min_gap_s per doc it was sent.
        assert plan_cycle(cfg, admin, T0 + timedelta(seconds=2), api)["jobs"] == 0
        assert plan_cycle(cfg, admin, T0 + timedelta(seconds=5), api)["jobs"] == 1
        assert stub.posts[-1] == [A2]
    finally:
        admin.close()


def test_recrawl_plan_drops_an_invalid_url_and_sends_the_rest(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1, C1])
        stub.reject = {B1}
        got = plan_cycle(cfg, admin, T0, _api(stub))
        assert (got["jobs"], got["invalid"], got["docs"]) == (1, 1, 2)
        assert stub.posts == [[A1, C1]]
        assert _doc(admin, B1, "state", "fail_reason") == ("gone", "invalid_url")
        ((_, _, state, doc_ids),) = _jobs(admin)
        assert (state, doc_ids) == ("submitted", [pg_doc_id(A1), pg_doc_id(C1)])
        assert _host(admin, "b.example")[0] is None          # nothing of B left in the job
    finally:
        admin.close()


@pytest.mark.parametrize("answer", [
    (400, "Invalid request body"),
    (415, "Content-Type must be application/json"),
    (500, "Submission failed; no URLs accepted"),
    (503, "Submission unavailable; no URLs accepted"),
])
def test_recrawl_plan_releases_jobs_the_api_refused_and_pauses(selectdb, tmp_path, stub,
                                                               answer):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, job_size=1, host_docs_per_job=1)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        api = _api(stub)
        stub.forced = [answer]
        got = plan_cycle(cfg, admin, T0, api)
        assert (got["jobs"], got["refused"]) == (0, 2) and str(answer[0]) in got["api_error"]
        assert [s for _, _, s, _ in _jobs(admin)] == ["lost", "lost"]
        assert _doc(admin, A1, "state") == ("pending",) and _host(admin, "a.example")[0] is None
        assert plan_cycle(cfg, admin, T0 + timedelta(seconds=30), api)["paused"]
        resumed = T0 + timedelta(seconds=cfg.recrawl.api_backoff_s + 1)
        assert plan_cycle(cfg, admin, resumed, api)["jobs"] == 2
    finally:
        admin.close()


def test_recrawl_plan_keeps_a_job_whose_reply_was_lost(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, job_size=1, host_docs_per_job=1)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        api = _api(stub)
        stub.lose_reply_s = 1.0
        got = plan_cycle(cfg, admin, T0, _api(stub, timeout_s=0.3))
        stub.lose_reply_s = 0.0
        assert (got["jobs"], got["unknown"], got["refused"]) == (0, 1, 1)
        assert [s for _, _, s, _ in _jobs(admin)] == ["claimed", "lost"]
        assert len(stub.jobs()) == 1                          # the API did accept it
        # The claimed job is not sent again until claim_timeout_s says it is lost.
        assert plan_cycle(cfg, admin, T0 + timedelta(seconds=60), api)["jobs"] == 1
        assert stub.posts[-1] == [B1] and _doc(admin, A1, "state") == ("dispatched",)
        late = plan_cycle(cfg, admin, T0 + timedelta(seconds=cfg.recrawl.claim_timeout_s + 1),
                          api)
        assert (late["lost"], late["jobs"]) == (1, 1) and stub.posts[-1] == [A1]
    finally:
        admin.close()


def test_recrawl_plan_sends_nothing_when_the_text_disk_is_short(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1])
        short = replace(cfg, "recrawl", min_free_gb=10**9)
        assert plan_cycle(short, admin, T0, _api(stub))["disk_full"]
        assert _doc(admin, A1, "state") == ("pending",) and not stub.posts
    finally:
        admin.close()


def test_recrawl_a_job_past_its_ttl_is_lost_and_sent_again(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, status_poll_s=60)
    r = cfg.recrawl
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        api = _api(stub)
        plan_cycle(cfg, admin, T0, api)
        check = T0 + timedelta(seconds=r.status_poll_s + 1)
        got = plan_cycle(cfg, admin, check, api)
        assert (got["checked"], got["lost"], got["jobs"]) == (1, 0, 0)
        assert plan_cycle(cfg, admin, check + timedelta(seconds=1), api)["checked"] == 0

        # The crawler never got to it, and the API stops it once its TTL passes.
        now = datetime.now(UTC)
        stub.clock = lambda: now + timedelta(days=r.result_ttl_days, seconds=1)
        again = check + timedelta(seconds=r.status_poll_s + 1)
        got = plan_cycle(cfg, admin, again, api)
        assert (got["lost"], got["jobs"]) == (1, 1)            # lost, and sent again at once
        assert stub.posts == [[A1, B1], [A1, B1]]
        crawler = FakeCrawler(stub, {A1: ENGLISH, B1: ENGLISH})
        assert crawler.take(again) == 2                       # only the new job runs
        _poll(cfg, admin, stub, again + timedelta(seconds=r.status_poll_s))
        assert archive_cycle(cfg, admin, again)["texts"] == 2
        assert [s for _, _, s, _ in _jobs(admin)] == ["lost", "applied"]
        assert stub.collect_expired() == 1 and stub.jobs() == [_jobs(admin)[1][1]]
    finally:
        admin.close()


def test_recrawl_plan_deletes_applied_results_and_retries_a_failed_delete(selectdb, tmp_path,
                                                                          stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1])
        api = _api(stub)
        plan_cycle(cfg, admin, T0, api)
        ((batch, job_id, _, _),) = _jobs(admin)
        FakeCrawler(stub, {A1: ENGLISH}).take(T0)
        _poll(cfg, admin, stub, T0)
        archive_cycle(cfg, admin, T0)
        stub.delete_codes = [503]
        got = plan_cycle(cfg, admin, T0 + timedelta(seconds=30), api)
        assert got["deleted"] == 0 and job_id in got["api_error"]
        assert (stub.results / job_id).is_dir()
        later = T0 + timedelta(seconds=60)
        assert plan_cycle(cfg, admin, later, api)["deleted"] == 1
        assert not (stub.results / job_id).exists() and stub.deletes == [job_id]
        assert admin.execute("SELECT deleted_at FROM recrawl.job WHERE batch_id = %s",
                             (batch,)).fetchone() == (later,)
        assert plan_cycle(cfg, admin, later, api)["deleted"] == 0
    finally:
        admin.close()


def test_recrawl_plan_loses_a_forgotten_job_and_flags_a_missing_mount(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, job_size=1, host_docs_per_job=1)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        plan_cycle(cfg, admin, T0, _api(stub))
        (_, first, _, _), (_, second, _, _) = _jobs(admin)
        stub.status_codes[first] = 404
        FakeCrawler(stub, {}).finish(second, T0)
        unmounted = replace(cfg, "recrawl", result_root=str(tmp_path / "not-mounted"))
        got = plan_cycle(unmounted, admin, T0 + timedelta(seconds=10), _api(stub))
        assert got["lost"] == 1 and second in got["config_error"]
        # A1 is due again, but no job goes out while results cannot be read.
        assert got["unmounted"] and got["jobs"] == 0
        assert [s for _, _, s, _ in _jobs(admin)] == ["lost", "submitted"]
    finally:
        admin.close()


def test_recrawl_archive_maps_rewritten_and_deduplicated_urls(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    raw, canon = "https://q.example/s?b=2&a=1", "https://q.example/s?a=1&b=2"
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1])
        # Two docs the API turns into one URL: our canonical form can still differ from
        # the API's (another w3lib version), so `raw` stands in for such a doc.
        (run,) = admin.execute("SELECT id FROM model_runs WHERE status = 'live'").fetchone()
        admin.execute("INSERT INTO recrawl.host (host_id, host, best_rank) VALUES (%s, %s, 2)",
                      (host_id("q.example"), "q.example"))
        admin.execute(
            "INSERT INTO recrawl.doc (doc_id, url, host_id, rank, model_run_id, due_at) "
            "VALUES (%s, %s, %s, 2, %s, %s), (%s, %s, %s, 3, %s, %s)",
            (pg_doc_id(raw), raw, host_id("q.example"), run, T0,
             pg_doc_id(canon), canon, host_id("q.example"), run, T0))
        api = _api(stub)
        plan_cycle(cfg, admin, T0, api)
        assert stub.posts == [[A1, raw, canon]]
        crawler = FakeCrawler(stub, {A1: ENGLISH, canon: ENGLISH})
        crawler.take(T0)
        _poll(cfg, admin, stub, T0)
        got = archive_cycle(cfg, admin, T0)
        assert (got["jobs"], got["texts"], got["errors"]) == (1, 2, 0)
        assert _doc(admin, canon, "state") == ("fetched",)
        assert _doc(admin, raw, "state", "fail_reason") == ("failed", "unmatched")

        # Alone in its next job, the rewritten URL pairs by position.
        retry = T0 + timedelta(seconds=cfg.recrawl.retry_base_s)
        plan_cycle(cfg, admin, retry, api)
        assert stub.posts[-1] == [raw]
        crawler.take(retry)
        _poll(cfg, admin, stub, retry)
        assert archive_cycle(cfg, admin, retry)["texts"] == 1
        assert _doc(admin, raw, "state", "final_url") == ("fetched", canon)
    finally:
        admin.close()


def _reverse(job_dir):
    """Rewrites a finished job as an API that reordered its URLs would have written it.

    Each record stays in its crawler's partition under its new index.
    """
    request = json.loads((job_dir / "request.json").read_text())
    parts = [job_dir / f"crawler_{c:02d}" for c in request["crawler_ids"]]
    n = len(request["urls"])
    items = [json.loads((parts[i] / f"{i:05d}.json").read_text()) for i in range(n)]
    pages = [(parts[i] / f"{i:05d}.html").read_bytes() for i in range(n)]
    for i in range(n):
        (parts[i] / f"{i:05d}.json").unlink()
        (parts[i] / f"{i:05d}.html").unlink()
    for key in ("urls", "crawler_ids"):
        request[key].reverse()
    (job_dir / "request.json").write_text(json.dumps(request))
    parts.reverse()
    for i, (item, page) in enumerate(zip(reversed(items), reversed(pages), strict=True)):
        item["html_file"] = f"{i:05d}.html"
        (parts[i] / f"{i:05d}.json").write_text(json.dumps(item))
        (parts[i] / f"{i:05d}.html").write_bytes(page)


def test_recrawl_archive_halts_the_planner_on_results_out_of_order(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, max_jobs_inflight=1)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1, C1])
        api = _api(stub)
        plan_cycle(cfg, admin, T0, api)
        assert stub.posts == [[A1, B1, C1]]
        ((_, job_id, _, _),) = _jobs(admin)
        FakeCrawler(stub, {A1: ARTICLE, B1: ENGLISH, C1: ENGLISH}).take(T0)
        _poll(cfg, admin, stub, T0)
        _reverse(stub.results / job_id)
        got = archive_cycle(cfg, admin, T0)
        assert got["errors"] == 1 and "not in the order sent" in got["last_error"]
        assert _jobs(admin)[0][2] == "error" and _doc(admin, A1, "state") == ("dispatched",)
        assert not list(Path(cfg.recrawl.text_root).rglob("*.parquet"))
        halted = plan_cycle(cfg, admin, T0 + timedelta(hours=1), api)
        assert halted["halted"] and halted["jobs"] == 0

        # Once the cause is fixed, the next cycle applies the job and planning resumes.
        _reverse(stub.results / job_id)
        assert archive_cycle(cfg, admin, T0 + timedelta(hours=2))["jobs"] == 1
        assert _doc(admin, A1, "state", "lang") == ("fetched", "zh")
        assert not plan_cycle(cfg, admin, T0 + timedelta(hours=3), api)["halted"]
    finally:
        admin.close()


def test_recrawl_an_error_job_left_unfixed_is_lost_when_its_results_expire(selectdb, tmp_path,
                                                                           stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, max_jobs_inflight=1)
    r = cfg.recrawl
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        api = _api(stub)
        plan_cycle(cfg, admin, T0, api)
        ((_, job_id, _, _),) = _jobs(admin)
        FakeCrawler(stub, {A1: ENGLISH, B1: ENGLISH}).take(T0)
        _poll(cfg, admin, stub, T0)
        _reverse(stub.results / job_id)
        assert archive_cycle(cfg, admin, T0)["errors"] == 1
        assert plan_cycle(cfg, admin, T0 + timedelta(hours=1), api)["halted"]

        now = datetime.now(UTC)
        stub.clock = lambda: now + timedelta(days=r.result_ttl_days, seconds=1)
        assert stub.collect_expired() == 1
        got = plan_cycle(cfg, admin, T0 + timedelta(hours=2), api)
        assert (got["lost"], got["halted"], got["jobs"]) == (1, False, 1)
        assert [s for _, _, s, _ in _jobs(admin)] == ["lost", "submitted"]
        assert stub.posts[-1] == [A1, B1]
    finally:
        admin.close()


def test_recrawl_archive_reads_without_writing_and_retries_until_it_can(selectdb, tmp_path,
                                                                        stub, monkeypatch):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        plan_cycle(cfg, admin, T0, _api(stub))
        ((_, job_id, _, _),) = _jobs(admin)
        FakeCrawler(stub, {A1: ENGLISH, B1: ENGLISH}).take(T0)
        _poll(cfg, admin, stub, T0)
        out = stub.results / job_id
        html = out / f"crawler_{crawler_of(A1):02d}" / "00000.html"

        # A completed job missing a page breaks the contract.
        html.rename(tmp_path / "held.html")
        got = archive_cycle(cfg, admin, T0)
        assert got["errors"] == 1 and "missing 00000.html" in got["last_error"]
        (tmp_path / "held.html").rename(html)

        # An unreadable page loses nothing: the job waits until it can be read.
        html.chmod(0o000)
        try:
            got = archive_cycle(cfg, admin, T0)
        finally:
            html.chmod(0o644)
        assert (got["io_error"], got["jobs"]) == (1, 0) and "Permission" in got["last_error"]

        # A crash after the files are written and before the DB commit is replayed whole,
        # from a results dir the archive cannot write to.
        monkeypatch.setattr("isel.recrawl._apply", lambda *a, **k: pytest.fail("crash"))
        with pytest.raises(pytest.fail.Exception):
            archive_cycle(cfg, admin, T0)
        monkeypatch.undo()
        for f in out.rglob("*"):
            f.chmod(0o555 if f.is_dir() else 0o444)
        out.chmod(0o555)
        try:
            assert archive_cycle(cfg, admin, T0)["jobs"] == 1
        finally:
            for d in [out, *out.glob("crawler_*")]:
                d.chmod(0o755)
        assert _doc(admin, A1, "state", "lang") == ("fetched", "en")
        assert len(list(Path(cfg.recrawl.text_root).rglob("*.parquet"))) == 1
        assert not list(Path(cfg.recrawl.text_root).rglob("*.tmp"))
    finally:
        admin.close()


def test_recrawl_a_job_completes_only_when_every_crawler_is_done(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        api = _api(stub)
        plan_cycle(cfg, admin, T0, api)
        ((batch, job_id, _, _),) = _jobs(admin)
        crawler = FakeCrawler(stub, {A1: ENGLISH, B1: ENGLISH})
        assert crawler_of(A1) != crawler_of(B1)
        crawler.finish(job_id, T0, crawlers=[crawler_of(A1)])
        got = plan_cycle(cfg, admin, T0 + timedelta(seconds=1), api)
        assert (got["checked"], got["completed"]) == (1, 0)
        assert archive_cycle(cfg, admin, T0)["jobs"] == 0      # one part is still running

        # Completed, the job keeps its docs and hosts until the archive applies it.
        done = T0 + timedelta(minutes=5)
        crawler.take(done)
        got = plan_cycle(cfg, admin, done, api)
        assert (got["completed"], got["orphans"], got["jobs"]) == (1, 0, 0)
        assert admin.execute("SELECT state, completed_at FROM recrawl.job WHERE batch_id = %s",
                             (batch,)).fetchone() == ("completed", done)
        assert _doc(admin, A1, "state") == ("dispatched",)
        assert _host(admin, "a.example")[0] == batch
        assert archive_cycle(cfg, admin, done + timedelta(hours=1))["texts"] == 2
        assert _doc(admin, B1, "state", "lang") == ("fetched", "en")
    finally:
        admin.close()


def test_recrawl_plan_stops_while_completed_jobs_wait_for_the_archive(selectdb, tmp_path,
                                                                     stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, job_size=1, host_docs_per_job=1,
                       max_jobs_inflight=1, plan_max_jobs=1)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        api = _api(stub)
        assert plan_cycle(cfg, admin, T0, api)["jobs"] == 1
        FakeCrawler(stub, {A1: ENGLISH}).take(T0)
        got = plan_cycle(cfg, admin, T0 + timedelta(seconds=10), api)
        assert (got["completed"], got["open"], got["archive_lag"], got["jobs"]) == (1, 0, True, 0)
        assert archive_cycle(cfg, admin, T0 + timedelta(seconds=10))["jobs"] == 1
        assert plan_cycle(cfg, admin, T0 + timedelta(seconds=20), api)["jobs"] == 1
        assert stub.posts[-1] == [B1]
    finally:
        admin.close()


def test_recrawl_a_completed_job_the_api_forgot_is_lost(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1])
        api = _api(stub)
        plan_cycle(cfg, admin, T0, api)
        ((_, job_id, _, _),) = _jobs(admin)
        FakeCrawler(stub, {A1: ENGLISH}).take(T0)
        assert _poll(cfg, admin, stub, T0)["completed"] == 1
        stub.status_codes[job_id] = 404                       # expired before the archive ran
        got = plan_cycle(cfg, admin, T0 + timedelta(seconds=1), api)
        assert got["lost"] == 1 and _jobs(admin)[0][2] == "lost"
        assert _doc(admin, A1, "state") == ("pending",) and _host(admin, "a.example")[0] is None
        assert archive_cycle(cfg, admin, T0 + timedelta(seconds=1))["jobs"] == 0
    finally:
        admin.close()


def test_recrawl_a_job_the_api_cannot_answer_for_holds_back_no_other(selectdb, tmp_path,
                                                                     stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub, job_size=1, host_docs_per_job=1)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        assert plan_cycle(cfg, admin, T0, _api(stub))["jobs"] == 2
        (_, first, _, _), (second_batch, _, _, _) = _jobs(admin)
        FakeCrawler(stub, {A1: ENGLISH, B1: ENGLISH}).take(T0)
        stub.status_codes[first] = 500
        got = _poll(cfg, admin, stub, T0 + timedelta(seconds=1))
        assert got["completed"] == 1 and first in got["api_error"]
        assert [s for _, _, s, _ in _jobs(admin)] == ["submitted", "completed"]
        dead = CrawlerApi("http://127.0.0.1:9", 0.5)        # nothing listens
        got = plan_cycle(replace(cfg, "recrawl", min_free_gb=10**9), admin,
                         T0 + timedelta(seconds=2), dead)
        assert got["checked"] == 0 and "api_error" in got
        assert admin.execute("SELECT checked_at FROM recrawl.job WHERE batch_id = %s",
                             (second_batch,)).fetchone() == (T0 + timedelta(seconds=1),)
    finally:
        admin.close()


def test_recrawl_sync_wakes_a_host_waiting_on_docs_in_backoff(selectdb, tmp_path, stub):
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, B1])
        # A1 waits 30 days after a robots refusal; B is paused 600 s after a 429.
        month, pause = T0 + timedelta(days=30), T0 + timedelta(seconds=600)
        admin.execute("UPDATE recrawl.doc SET state = 'failed', due_at = %s WHERE doc_id = %s",
                      (month, pg_doc_id(A1)))
        admin.execute("UPDATE recrawl.host SET next_at = %s WHERE host_id = %s",
                      (month, host_id("a.example")))
        admin.execute("UPDATE recrawl.host SET next_at = %s WHERE host_id = %s",
                      (pause, host_id("b.example")))
        _go_live(cfg, admin, tmp_path, "r2", [A2, A1, B1])
        sync(cfg, admin, now=lambda: OPEN)
        assert plan_cycle(cfg, admin, T0, _api(stub))["jobs"] == 1
        assert stub.posts == [[A2]]
        assert _host(admin, "b.example") == (None, pause)
    finally:
        admin.close()


def test_recrawl_end_to_end_through_the_cli(selectdb, tmp_path, stub, capsys):
    """Sync, then the real `isel recrawl-plan` / `recrawl-archive` commands against the
    API stub and the fake crawler, until every doc has a final state."""
    cfg = _recrawl_cfg(selectdb, tmp_path, stub)
    moved, bad = "https://d.example/old", "https://e.example/bad"
    web = {A1: ARTICLE, A2: ENGLISH, A3: Page(status=404),
           B1: Page(body=b"%PDF-1.7", content_type="application/pdf"),
           C1: Page(status=None, error="IgnoreRequest"),
           moved: Page(body=ENGLISH.body, final_url="https://d.example/new")}
    stub.reject = {bad}
    _setup(cfg)
    admin = _admin(cfg)
    try:
        _live(cfg, admin, tmp_path, [A1, A2, A3, B1, C1, moved, bad])
        overlay = tmp_path / "e2e.toml"
        r = cfg.recrawl
        overlay.write_text(f"""
[run]
work_dir = "{tmp_path}"
[recrawl]
api_url = "{stub.url}"
result_root = "{r.result_root}"
text_root = "{r.text_root}"
log_root = "{r.log_root}"
job_size = 3
host_docs_per_job = 2
plan_max_jobs = 2
min_free_gb = 0
archive_workers = 2
min_gap_s = 0.001
status_poll_s = 0
""")
        cli = ["--config", str(Path(__file__).resolve().parents[1] / "config" / "isel.toml"),
               "--config", str(overlay)]
        crawler = FakeCrawler(stub, web)
        for _ in range(3):
            assert cli_main([*cli, "recrawl-plan", "--once"]) == 0
            crawler.take(datetime.now(UTC))
            assert cli_main([*cli, "recrawl-archive", "--once"]) == 0
        # The status of the last jobs comes one plan later, their deletion one more.
        assert cli_main([*cli, "recrawl-plan", "--once"]) == 0
        assert cli_main([*cli, "recrawl-archive", "--once"]) == 0
        assert cli_main([*cli, "recrawl-plan", "--once"]) == 0
        assert not list(stub.results.iterdir())
        capsys.readouterr()
        assert cli_main([*cli, "recrawl-plan", "--dry-run"]) == 0
        report = json.loads(capsys.readouterr().out)
        states = dict(admin.execute("SELECT url, state || ':' || coalesce(fail_reason, '') "
                                    "FROM recrawl.doc").fetchall())
        assert states == {A1: "fetched:", A2: "fetched:", A3: "gone:http_404",
                          B1: "gone:non_html", C1: "failed:ignore_request", moved: "fetched:",
                          bad: "gone:invalid_url"}
        assert report["states"] == {"fetched": 3, "gone": 3, "failed": 1}
        texts = pq.read_table(Path(r.text_root)).to_pylist()
        assert {t["url"]: (t["final_url"], t["lang"]) for t in texts} == {
            A1: (A1, "zh"), A2: (A2, "en"), moved: ("https://d.example/new", "en")}
        jobs = _jobs(admin)
        assert {s for _, _, s, _ in jobs} == {"applied"} and len(stub.posts) == len(jobs)
        assert not stub.jobs()
    finally:
        admin.close()
