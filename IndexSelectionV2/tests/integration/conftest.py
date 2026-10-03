from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fixture_db import (
    build_universe,
    docker_available,
    seed_crawler_server,
    start_postgres,
    start_selectdb,
)
from helpers import EXAMPLE

from idxsel.config import load_config

NUM_SHARDS = 4
K = 1000
ENV = {
    "crawler": "IDXSEL_TEST_CRAWLERDB_DSN",
    "metric": "IDXSEL_TEST_METRICDB_DSN",
    "admin": "IDXSEL_TEST_SELECTDB_ADMIN_DSN",
    "reader": "IDXSEL_TEST_SELECTDB_READER_DSN",
    "reader_pw": "IDXSEL_TEST_SELECTDB_READER_PASSWORD",
}

OVERLAY = """
[run]
work_dir = "{work}"
k = {k}

[crawlerdb]
dsn_env = "{crawler}"
num_shards = {shards}

[metricdb]
dsn_env = "{metric}"

[selectdb]
admin_dsn_env = "{admin}"
reader_dsn_env = "{reader}"
reader_password_env = "{reader_pw}"
max_db_bytes = 2_000_000_000
swap_retry_attempts = 3
swap_retry_backoff_s = 0.2
swap_lock_timeout = "1s"

[export]
chunk_pages = 4
chunks_per_file = 3
shards_per_ingestor = 2
history_chunk_ids = 100
history_overlap_ids = 50
retry_backoff_s = [0.1]

[throttle]
rate_mb_s_initial = 500
rate_mb_s_min = 100
rate_mb_s_max = 1000
pause_backoff_s = [0.2]

[guard]
host_meminfo_enabled = false
work_dir_max_gb = 5
work_dir_min_free_gb = 1
build_reserve_gb = 0
db_io_wait_backends_soft = 1000
db_io_wait_backends_hard = 2000
min_free_connections = 5

[guard.remote]
enabled = false

[schedule]
enabled = false

[duckdb]
threads = 4
memory_limit = "2GB"
max_temp_directory_size = "5GB"

[golden]
target_batch = 3

[selection]
exact_sort_max_rows = 50

[eval]
bootstrap_samples = 200
live_check_sample_size = 100

[labels]
sample_mod = 1
"""


@pytest.fixture(scope="session")
def servers():
    if not docker_available():
        pytest.skip("docker with the postgres:16 image is required")
    crawler = start_postgres([])
    selectdb = start_selectdb()
    universe = build_universe(NUM_SHARDS, urls_per_domain=150, seed=7)
    try:
        seed_crawler_server(crawler, universe)
        env = {
            ENV["crawler"]: crawler.dsn("crawler", "crawler", "crawlerdb"),
            ENV["metric"]: crawler.dsn("metric", "metric", "metricdb"),
            ENV["admin"]: selectdb.dsn("idxsel_admin", "adminpw", "selectdb"),
            ENV["reader"]: selectdb.dsn("select", "select", "selectdb"),
            ENV["reader_pw"]: "select",
        }
        os.environ.update(env)
        yield {"crawler": crawler, "selectdb": selectdb, "universe": universe, "env": env}
    finally:
        crawler.stop()
        selectdb.stop()


def write_overlay(tmp: Path, **extra: str) -> Path:
    text = OVERLAY.format(work=tmp / "work", k=K, shards=NUM_SHARDS, **ENV)
    for section, body in extra.items():
        text += f"\n[{section}]\n{body}\n"
    path = tmp / "overlay.toml"
    path.write_text(text)
    return path


@pytest.fixture
def overlay(tmp_path, servers):
    return write_overlay(tmp_path)


@pytest.fixture
def fresh_selectdb(servers):
    """Drops and recreates our selectdb so each test starts with no runs."""
    import psycopg

    pg = servers["selectdb"]
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                     "WHERE datname = 'selectdb' AND pid <> pg_backend_pid()")
        conn.execute("DROP DATABASE IF EXISTS selectdb")
        conn.execute("CREATE DATABASE selectdb OWNER idxsel_admin")
    return pg


@pytest.fixture
def cfg(overlay):
    return load_config([EXAMPLE, overlay])
