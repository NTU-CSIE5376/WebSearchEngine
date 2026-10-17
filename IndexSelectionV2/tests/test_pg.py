from __future__ import annotations

import pytest
from conftest import FakeConn

from isel.cli import _ids
from isel.pg import (
    Pacer,
    ReadOnlySession,
    ReadOnlyViolation,
    assert_read_only_sql,
    require_env,
)
from isel.pull import full_sql


@pytest.mark.parametrize("sql", [
    "SELECT 1",
    "WITH a AS (SELECT 1) SELECT * FROM a",
    "COPY (SELECT url FROM t TABLESAMPLE SYSTEM (1.0) REPEATABLE (7)) TO STDOUT (FORMAT BINARY)",
    "SELECT 'drop table x' AS s",
    "SELECT $$; set_config($$ AS s, $tag$ it's $$ $tag$ AS t",
    "SELECT E'it\\'s; delete' AS s, 'a''b' AS t",
    "SELECT 1 /* outer /* nested; update */ still comment */",
    'SELECT "update" FROM "my table" WHERE url = ANY(%s)',
    "SELECT a$b FROM t WHERE x = $1",
    "SELECT 'café' AS s, \"名稱\" FROM t WHERE x = %s",
    "COPY (SELECT a FROM t WHERE a SIMILAR TO stdout) TO STDOUT",
    "COPY (SELECT url FROM t WHERE f(url) IN (SELECT 1)) TO STDOUT WITH (FORMAT csv, HEADER true)",
])
def test_read_only_gate_allows_reads(sql):
    assert_read_only_sql(sql)


@pytest.mark.parametrize("sql", [
    "UPDATE t SET a = 1",
    "SELECT 1; DELETE FROM t",
    "COPY t FROM STDIN",
    "SELECT * FROM t FOR UPDATE",
    "EXPLAIN ANALYZE SELECT 1",
    "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x",
    "SELECT * INTO new_table FROM t",
    # COPY must be exactly COPY (query) TO STDOUT; a server-side file is a write.
    "COPY (SELECT url FROM t WHERE url SIMILAR TO stdout) TO '/tmp/x.csv'",
    "COPY (SELECT 1) TO STDOUT (FORMAT csv), pg_terminate_backend(1)",
    "COPY (SELECT 1) TO STDOUT (FORMAT csv) TO '/tmp/x'",
    "COPY t TO STDOUT",
    "COPY (DELETE FROM t RETURNING *) TO STDOUT",
    "COPY (SELECT 1) TO PROGRAM 'rm -rf /'",
])
def test_read_only_gate_rejects_writes(sql):
    with pytest.raises(ReadOnlyViolation):
        assert_read_only_sql(sql)


@pytest.mark.parametrize("sql", [
    "SELECT set_config('default_transaction_read_only', 'off', false)",
    "SELECT pg_terminate_backend(123)",
    "SELECT pg_cancel_backend(123)",
    "SELECT lo_export(1, '/tmp/x')",
    "SELECT lo_import('/etc/passwd')",
    "SELECT pg_reload_conf()",
    "SELECT pg_switch_wal()",
    "SELECT pg_promote()",
    "SELECT SET_CONFIG ('statement_timeout', '0', false)",
    "SELECT pg_create_physical_replication_slot('s')",
    "SELECT pg_stat_reset()",
    "SELECT pg_advisory_lock(1)",
    "SELECT query_to_xml('select 1', true, false, '')",
    # Spellings that slipped past the regex gate before it lexed SQL.
    'SELECT "set_config"(\'default_transaction_read_only\', \'off\', false)',
    "SELECT '--', set_config('default_transaction_read_only', 'off', false)",
    "SELECT E'\\'', set_config('default_transaction_read_only', 'off', false), ''",
    'SELECT pg_catalog."pg_terminate_backend"(1)',
    "SELECT set_config/* gap */('statement_timeout', '0', false)",
    "SELECT pg_stat_statements_reset()",
    "SELECT brin_summarize_new_values('i')",
    "SELECT gin_clean_pending_list('i')",
    "SELECT pg_prewarm('url_state_current_000')",
    # Non-ASCII letters are identifier characters, so `$q$` here is not a quote.
    "SELECT 1 AS x\u00e9$q$, set_config('a', 'b', false), $q$ AS z",
    "SELECT x\u00e9e'\\', set_config('a', 'b', false), ''",
])
def test_read_only_gate_rejects_state_changing_functions(sql):
    with pytest.raises(ReadOnlyViolation):
        assert_read_only_sql(sql)


@pytest.mark.parametrize("sql", [
    "SELECT 'open",
    "SELECT 1 /* open",
    'SELECT "open',
    "SELECT $q$ open",
    'SELECT U&"d\\0061t" FROM t',
    "SELECT $q%s FROM t",   # psycopg would send `$q$1`
])
def test_read_only_gate_rejects_unlexable_sql(sql):
    with pytest.raises(ReadOnlyViolation):
        assert_read_only_sql(sql)


def test_statement_timeout_is_sent_as_a_parameter():
    conn = FakeConn()
    session = ReadOnlySession(conn)
    session.set_statement_timeout(300)
    assert session.timeout_s == 300
    (text, params), = conn.executed
    assert "statement_timeout" in text and params == ("300s",)


@pytest.mark.parametrize("seconds", [0, -5, 1.5, "60"])
def test_statement_timeout_rejects_bad_values(seconds):
    with pytest.raises(ValueError):
        ReadOnlySession(FakeConn()).set_statement_timeout(seconds)


def test_full_export_statement_passes_the_gate(base_cfg):
    text = full_sql(base_cfg, 7, 131072, 262144)
    assert_read_only_sql(text)
    assert "ctid >= '(131072,0)'::tid AND ctid < '(262144,0)'::tid" in text
    assert text.endswith("TO STDOUT (FORMAT CSV)")


def test_require_env_reads_a_secret_file(tmp_path, monkeypatch):
    secret = tmp_path / "dsn"
    secret.write_text("postgresql://u@h:5445/selectdb\n")
    monkeypatch.delenv("ISEL_TEST_DSN", raising=False)
    monkeypatch.setenv("ISEL_TEST_DSN_FILE", str(secret))
    assert require_env("ISEL_TEST_DSN") == "postgresql://u@h:5445/selectdb"
    monkeypatch.setenv("ISEL_TEST_DSN", "postgresql://other")
    with pytest.raises(RuntimeError, match="only one"):
        require_env("ISEL_TEST_DSN")
    monkeypatch.delenv("ISEL_TEST_DSN")
    monkeypatch.delenv("ISEL_TEST_DSN_FILE")
    with pytest.raises(RuntimeError, match="is not set"):
        require_env("ISEL_TEST_DSN")


def test_pacer_spaces_statements():
    now = [0.0]
    slept = []
    pacer = Pacer(1.0, clock=lambda: now[0], sleep=lambda s: (slept.append(s),
                                                               now.__setitem__(0, now[0] + s)))
    pacer.wait()
    now[0] += 0.25
    pacer.wait()
    assert slept == [0.75]


def test_id_ranges():
    assert _ids("0-2,7,5-5") == [0, 1, 2, 5, 7]
    assert _ids(None) is None
