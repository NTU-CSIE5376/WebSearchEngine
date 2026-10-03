import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from idxsel.duck import copy_atomic
from idxsel.guards import _BLOCKED_BY_SQL, _DB_HEALTH_SQL
from idxsel.load import SecretLeak, assert_no_secrets
from idxsel.manifest import (
    AtomicParquetWriter,
    WorkDirBusy,
    atomic_write_json,
    parquet_ok,
    read_json,
    remove_tmp_files,
    utc_id,
    work_lock,
    write_parquet_atomic,
)
from idxsel.pg import ReadOnlyViolation, assert_read_only_sql


@pytest.mark.parametrize("stmt", [
    "SELECT 1",
    "  with x as (select 1) select * from x;",
    "SHOW statement_timeout",
    "EXPLAIN SELECT * FROM t",
    "COPY (SELECT url FROM t WHERE ctid >= '(0,0)'::tid) TO STDOUT (FORMAT BINARY)",
    "SELECT 'drop table x; delete' AS s",
    "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%to prevent wraparound%'",
    _DB_HEALTH_SQL,
    _BLOCKED_BY_SQL,
])
def test_read_only_sql_allowed(stmt):
    assert_read_only_sql(stmt)


@pytest.mark.parametrize("stmt", [
    "INSERT INTO t VALUES (1)",
    "update t set a = 1",
    "SELECT 1; DELETE FROM t",
    "COPY t FROM STDIN",
    "EXPLAIN ANALYZE SELECT 1",
    "SELECT * FROM t FOR UPDATE SKIP LOCKED",
    "CREATE TEMP TABLE x AS SELECT 1",
    "WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d",
    "VACUUM t",
    "COPY (DELETE FROM t RETURNING *) TO STDOUT",
])
def test_read_only_sql_rejected(stmt):
    with pytest.raises(ReadOnlyViolation):
        assert_read_only_sql(stmt)


def test_parquet_writers_commit_atomically(tmp_path):
    table = pa.table({"a": [1, 2, 3]})
    assert write_parquet_atomic(table, tmp_path / "x" / "w.parquet") == 3
    with AtomicParquetWriter(tmp_path / "s.parquet", table.schema) as writer:
        writer.write(table)
        writer.write(table.slice(0, 0))
        writer.write(table)
    assert writer.rows == 6 and pq.ParquetFile(tmp_path / "s.parquet").metadata.num_row_groups == 2
    with pytest.raises(RuntimeError), AtomicParquetWriter(tmp_path / "f.parquet",
                                                          table.schema) as writer:
        writer.write(table)
        raise RuntimeError("crash")
    assert not list(tmp_path.glob("f.parquet*"))
    rows = copy_atomic(duckdb.connect(), "SELECT * FROM range(5)", tmp_path / "c.parquet")
    assert rows == 5 and pq.read_table(tmp_path / "c.parquet").num_rows == 5


def test_failed_copy_leaves_no_tmp(tmp_path):
    con = duckdb.connect()
    with pytest.raises(duckdb.Error):
        copy_atomic(con, "SELECT error('boom') AS x", tmp_path / "out.parquet")
    assert list(tmp_path.iterdir()) == []


def test_work_lock_is_exclusive_even_within_one_process(tmp_path):
    lock = tmp_path / "state" / "lock"
    with work_lock(lock, "export"), pytest.raises(WorkDirBusy, match="export"), \
            work_lock(lock, "gc"):
        pass
    with work_lock(lock, "gc"):
        pass


def test_atomic_json_and_tmp_cleanup(tmp_path):
    path = tmp_path / "a" / "state.json"
    atomic_write_json(path, {"x": 1})
    assert read_json(path) == {"x": 1}
    (tmp_path / "a" / "junk.parquet.tmp").write_text("x")
    assert remove_tmp_files(tmp_path) == 1
    assert read_json(tmp_path / "missing.json", {"d": 1}) == {"d": 1}


def test_parquet_ok(tmp_path):
    good = tmp_path / "g.parquet"
    pq.write_table(pa.table({"a": [1]}), good)
    bad = tmp_path / "b.parquet"
    bad.write_bytes(good.read_bytes()[:-10])
    assert parquet_ok(good)
    assert not parquet_ok(bad)
    assert not parquet_ok(tmp_path / "none.parquet")


def test_utc_id_is_sortable():
    ids = [utc_id() for _ in range(200)]
    assert all(len(i) == 22 and i.endswith("Z") for i in ids)
    assert ids == sorted(ids)


def test_secrets_are_rejected():
    assert_no_secrets({"profile": "baseline_v1", "weights": {"prior": 2.0}})
    with pytest.raises(SecretLeak):
        assert_no_secrets({"crawler_db_url": "postgresql://user:pw@db.example/x"})
    with pytest.raises(SecretLeak):
        assert_no_secrets(json.loads('{"password": "x"}'))
