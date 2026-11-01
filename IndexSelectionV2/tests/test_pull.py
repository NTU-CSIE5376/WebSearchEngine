from __future__ import annotations

from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest
from conftest import replace
from psycopg import errors

from isel import pull
from isel.io import read_json
from isel.pg import assert_read_only_sql
from isel.pull import FULL_SCHEMA, estimate_full, pull_full

T0 = datetime(2026, 10, 8, 5, 20, tzinfo=UTC)
T1 = datetime(2026, 10, 14, 20, 10, tzinfo=UTC)
QUARTERS = [(0, 16384), (16384, 32768), (32768, 49152), (49152, 65536)]


class FakeSession:
    """Answers the catalog query `pull` sends; COPY returns `csv` for every chunk."""

    def __init__(self, failing: set[str] = frozenset(), csv: bytes = b"",
                 broken: set[str] = frozenset(), restarts: dict[str, int] | None = None):
        self.failing = set(failing)
        self.broken = set(broken)   # tables whose COPY raises a non-retryable error
        self.restarts = dict(restarts or {})   # serialization failures left per table
        self.queries: list[str] = []
        self.csv = csv
        self.timeout_s = 120
        self.copies: list[tuple[str, int]] = []

    def fetchone(self, query, params=None):
        assert_read_only_sql(query)
        self.queries.append(query)
        if "yb_servers" in query:
            return (3,)
        raise AssertionError(query)

    def set_statement_timeout(self, seconds):
        self.timeout_s = seconds

    def copy_bytes(self, query):
        assert_read_only_sql(query)
        self.copies.append((query, self.timeout_s))
        if any(f"FROM {table} " in query for table in self.failing):
            raise errors.QueryCanceled("canceling statement due to statement timeout")
        if any(f"FROM {table} " in query for table in self.broken):
            raise RuntimeError("server closed the connection unexpectedly")
        for table, left in self.restarts.items():
            if f"FROM {table} " in query and left > 0:
                self.restarts[table] = left - 1
                raise errors.SerializationFailure("Restart read required")
        return bytearray(self.csv)


class FakeRate:
    """Records the bytes each chunk charges instead of sleeping."""

    def __init__(self):
        self.reserved: list[int] = []

    def wait(self, n_bytes):
        self.reserved.append(n_bytes)


def _cfg(base_cfg, **full):
    cfg = replace(base_cfg, "crawlerdb", num_shards=2)
    return replace(cfg, "full", **{"hash_chunks": 4, "chunks_per_file": 2, "min_free_gb": 0.0,
                                   **full})


def _ranges(session):
    """(table, lo, hi) of every COPY in order."""
    out = []
    for query, _ in session.copies:
        table = query.split("FROM ")[1].split()[0]
        lo, hi = (int(part.split()[1]) for part in query.split("yb_hash_code(url) ")[1:3])
        out.append((table, lo, hi))
    return out


def test_full_export_reads_the_hash_space_of_each_shard_once_and_resumes(tmp_path, base_cfg):
    session = FakeSession()
    out = pull_full(_cfg(base_cfg), [session], tmp_path, now=T0)
    assert out["shards_missing"] == 0
    assert _ranges(session) == [(f"url_state_current_{s:03d}", lo, hi)
                                for s in range(2) for lo, hi in QUARTERS]
    files = sorted(p.name for p in (tmp_path / "shard=000").iterdir())
    assert files == ["part-0000000000-0000032768.parquet", "part-0000032768-0000065536.parquet"]
    assert pq.read_schema(tmp_path / "shard=000" / files[0]).names == FULL_SCHEMA.names
    meta = read_json(tmp_path / "_meta.json")
    assert meta["hash_chunks"] == 4 and meta["chunks_per_file"] == 2 and "finished_at" in meta
    assert set(meta["seconds"]) == {"0", "1"}
    # Only the COPY statements: no heap size, WAL or pg_stat_io reads.
    assert session.queries == []
    again = FakeSession()
    pull_full(_cfg(base_cfg), [again], tmp_path, now=T1)
    assert again.copies == []


def test_full_export_resumes_a_failed_shard_from_its_unfinished_files(tmp_path, base_cfg):
    cfg = _cfg(base_cfg, chunks_per_file=1)
    out = pull_full(cfg, [FakeSession(failing={"url_state_current_001"})], tmp_path, now=T0)
    assert out["shards_failed"] == [1] and "finished_at" not in read_json(tmp_path / "_meta.json")
    (tmp_path / "shard=001").mkdir(exist_ok=True)
    pq.write_table(FULL_SCHEMA.empty_table(),
                   tmp_path / "shard=001" / "part-0000000000-0000016384.parquet")
    retry = FakeSession()
    out = pull_full(cfg, [retry], tmp_path, now=T1)
    assert _ranges(retry) == [("url_state_current_001", lo, hi) for lo, hi in QUARTERS[1:]]
    assert out["shards_failed"] == [] and "finished_at" in read_json(tmp_path / "_meta.json")


# YSQL `COPY ... (FORMAT CSV)` output, as in PostgreSQL: NULL is an unquoted empty field, an
# empty string is "", quotes inside a quoted field are doubled.
CSV_CHUNK = (
    b'"https://a.com/x?q=""a,b""\nnext",7,2026-10-09 05:39:22.423515,12,t,1,2,f,0,t,3,f,t,'
    b'100,5,0.30000000000000004,0\n'
    b'https://b.com/NULL,8,2026-09-01 00:01:00,,f,,,,,,,f,f,,,-Infinity,\n'
)


def test_full_export_parses_the_csv_copy_output(tmp_path, base_cfg):
    session = FakeSession(csv=CSV_CHUNK)
    pull_full(_cfg(base_cfg, chunks_per_file=4), [session], tmp_path, shards=[0], now=T0)
    assert "TO STDOUT (FORMAT CSV)" in session.copies[0][0]
    got = pq.read_table(tmp_path / "shard=000" / "part-0000000000-0000065536.parquet")
    assert got.schema.equals(FULL_SCHEMA, check_metadata=False)
    unset = dict.fromkeys(("title_len", "discovery_source_type", "robots_bits", "is_redirect",
                           "redirect_hop_count", "has_json_ld", "hreflang_count",
                           "inlink_count_approx", "inlink_count_external", "source"))
    assert got.to_pylist()[:2] == [
        {"url": 'https://a.com/x?q="a,b"\nnext', "domain_id": 7,
         "last_fetch_ok": datetime(2026, 10, 9, 5, 39, 22, 423515, tzinfo=UTC),
         "title_len": 12, "has_anchor": True, "discovery_source_type": 1, "robots_bits": 2,
         "is_redirect": False, "redirect_hop_count": 0, "has_json_ld": True,
         "hreflang_count": 3, "has_last_modified": False, "has_cache_control": True,
         "inlink_count_approx": 100, "inlink_count_external": 5,
         "url_score": 0.30000000000000004, "source": 0, "shard": 0},
        {**unset, "url": "https://b.com/NULL", "domain_id": 8,
         "last_fetch_ok": datetime(2026, 9, 1, 0, 1, tzinfo=UTC), "has_anchor": False,
         "has_last_modified": False, "has_cache_control": False, "url_score": float("-inf"),
         "shard": 0},
    ]


def test_full_export_spreads_shards_over_sessions_and_reads_each_range_once(tmp_path,
                                                                           base_cfg):
    cfg = replace(_cfg(base_cfg), "crawlerdb", num_shards=6)
    sessions = [FakeSession() for _ in range(3)]
    out = pull_full(cfg, sessions, tmp_path, now=T0)
    ranges = [r for s in sessions for r in _ranges(s)]
    expected = {(f"url_state_current_{s:03d}", lo, hi) for s in range(6) for lo, hi in QUARTERS}
    assert len(ranges) == len(set(ranges)) and set(ranges) == expected
    meta = read_json(tmp_path / "_meta.json")
    assert out["shards_missing"] == 0 and meta["shards_done"] == list(range(6))
    assert "finished_at" in meta


def test_a_broken_session_stops_the_export_and_raises(tmp_path, base_cfg):
    cfg = replace(_cfg(base_cfg), "crawlerdb", num_shards=4)
    broken = FakeSession(broken={f"url_state_current_{s:03d}" for s in range(4)})
    with pytest.raises(RuntimeError, match="closed the connection"):
        pull_full(cfg, [broken, FakeSession()], tmp_path, now=T0)
    assert "finished_at" not in read_json(tmp_path / "_meta.json")


def test_every_chunk_charges_its_csv_bytes_to_the_shared_rate(tmp_path, base_cfg):
    rate = FakeRate()
    cfg = _cfg(base_cfg)
    row = b"https://a.example/,1,2026-10-01 00:00:00,5,t,0,0,f,0,f,0,f,f,1,1,0.5,0\n"
    session = FakeSession(csv=row)
    out = pull_full(cfg, [session], tmp_path, shards=[0], now=T0, rate=rate)
    assert out["rows"] == 4 and rate.reserved == [len(row)] * 4
    assert {timeout for _, timeout in session.copies} == {cfg.guard.timeout_cap_s}


def test_full_export_refuses_another_layout_or_a_full_disk(tmp_path, base_cfg):
    pull_full(_cfg(base_cfg), [FakeSession()], tmp_path, shards=[0], now=T0)
    with pytest.raises(RuntimeError, match=r"full\.hash_chunks"):
        pull_full(_cfg(base_cfg, hash_chunks=8), [FakeSession()], tmp_path, now=T1)
    with pytest.raises(RuntimeError, match="GB free"):
        pull_full(_cfg(base_cfg, min_free_gb=1e9), [FakeSession()], tmp_path, now=T1)


def test_full_export_refuses_a_postgres_export_dir(tmp_path, base_cfg):
    (tmp_path / "_meta.json").write_text(
        '{"engine": "postgres", "chunk_blocks": 65536, "chunks_per_file": 16}')
    with pytest.raises(RuntimeError, match=r"full\.hash_chunks"):
        pull_full(_cfg(base_cfg), [FakeSession()], tmp_path, now=T1)


def test_a_read_restart_rereads_only_that_chunk(tmp_path, base_cfg, monkeypatch):
    waits = []
    monkeypatch.setattr(pull, "_sleep", waits.append)
    session = FakeSession(restarts={"url_state_current_000": 2})
    out = pull_full(_cfg(base_cfg), [session], tmp_path, shards=[0], now=T0)
    assert out["shards_failed"] == [] and waits == [1, 2]
    assert [r[1:] for r in _ranges(session)][:3] == [(0, 16384)] * 3


def test_a_shard_that_keeps_restarting_resumes_on_the_next_run(tmp_path, base_cfg, monkeypatch):
    monkeypatch.setattr(pull, "_sleep", lambda s: None)
    cfg = _cfg(base_cfg, chunk_retries=1)
    session = FakeSession(restarts={"url_state_current_000": 2})
    out = pull_full(cfg, [session], tmp_path, now=T0)
    assert out["shards_failed"] == [0] and out["shards_missing"] == 1
    assert read_json(tmp_path / "_meta.json")["shards_failed"] == {"0": "SerializationFailure"}
    pull_full(cfg, [FakeSession()], tmp_path, now=T1)
    assert "finished_at" in read_json(tmp_path / "_meta.json")


def test_full_estimate_counts_chunks_and_tservers(base_cfg):
    est = estimate_full(_cfg(base_cfg), FakeSession())
    assert est == {"shards": 2, "chunks": 8, "chunk_timeout_s": base_cfg.guard.timeout_cap_s,
                   "tservers": 3}
