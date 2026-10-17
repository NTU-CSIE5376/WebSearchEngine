from __future__ import annotations

from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest
from conftest import replace
from psycopg import errors

from isel.io import read_json
from isel.pg import assert_read_only_sql
from isel.pull import FULL_SCHEMA, estimate_full, pull_full

T0 = datetime(2026, 10, 8, 5, 20, tzinfo=UTC)
T1 = datetime(2026, 10, 14, 20, 10, tzinfo=UTC)


class FakeSession:
    """Answers the catalog and probe queries `pull` sends; COPY returns no rows."""

    def __init__(self, probe: tuple, failing: set[str] = frozenset(),
                 sizes: dict[str, list[int]] | None = None, csv: bytes = b"",
                 io: list[tuple[int, int, int]] | None = None, broken: set[str] = frozenset()):
        self.probe = probe
        self.failing = set(failing)
        self.broken = set(broken)   # tables whose COPY raises a non-retryable error
        self.sizes = {t: list(v) for t, v in (sizes or {}).items()}   # successive heap sizes
        self.csv = csv
        self.io = list(io or [(0, 0, 0)])   # successive (wal_bytes, wal_fpi, writes) readings
        self.timeout_s = 120
        self.copies: list[tuple[str, int]] = []

    def fetchone(self, query, params=None):
        assert_read_only_sql(query)
        if "pg_stat_wal" in query:
            return self.io.pop(0) if len(self.io) > 1 else self.io[0]
        if "pg_relation_size" in query:
            sizes = self.sizes.get(params[0], [0])
            return (sizes.pop(0) if len(sizes) > 1 else sizes[0],)
        if "pg_stat_user_tables" in query:
            return self.probe
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
        return bytearray(self.csv)


class FakeRate:
    """Records the heap bytes each chunk reserves instead of sleeping."""

    def __init__(self):
        self.reserved: list[int] = []

    def wait(self, n_bytes):
        self.reserved.append(n_bytes)


PROBE = (datetime(2026, 10, 6, 8, 2, 16, tzinfo=UTC), 123_456)


def _full_cfg(base_cfg, **full):
    cfg = replace(base_cfg, "crawlerdb", num_shards=2)
    return replace(cfg, "full", **{"chunk_blocks": 10, "chunks_per_file": 2, "min_free_gb": 0.0,
                                   **full})


def _ranges(session):
    """(table, lo, hi) of every COPY in order."""
    out = []
    for query, _ in session.copies:
        table = query.split("FROM ")[1].split()[0]
        lo, hi = (int(part.split("'(")[1].split(",")[0]) for part in query.split("ctid ")[1:3])
        out.append((table, lo, hi))
    return out


def test_full_export_reads_every_block_range_once_and_resumes(tmp_path, base_cfg):
    cfg = _full_cfg(base_cfg)
    sizes = {"url_state_current_000": [35], "url_state_current_001": [0]}
    session = FakeSession(PROBE, sizes=sizes)
    out = pull_full(cfg, [session], tmp_path, now=T0)
    assert out["shards_missing"] == 0
    assert _ranges(session) == [("url_state_current_000", lo, hi)
                                for lo, hi in ((0, 10), (10, 20), (20, 30), (30, 35))]
    files = sorted(p.name for p in (tmp_path / "shard=000").iterdir())
    assert files == ["part-0000000000-0000000020.parquet", "part-0000000020-0000000035.parquet"]
    assert pq.read_schema(tmp_path / "shard=000" / files[0]).names == FULL_SCHEMA.names
    meta = read_json(tmp_path / "_meta.json")
    assert meta["extents"] == {"0": [[0, 35]], "1": [[0, 0]]} and "finished_at" in meta
    again = FakeSession(PROBE, sizes=sizes)
    pull_full(cfg, [again], tmp_path, now=T1)
    assert again.copies == []


def test_full_export_resumes_a_failed_shard_from_its_unfinished_files(tmp_path, base_cfg):
    cfg = _full_cfg(base_cfg, chunks_per_file=1)
    sizes = {"url_state_current_000": [20], "url_state_current_001": [20]}
    failing = FakeSession(PROBE, failing={"url_state_current_001"}, sizes=sizes)
    out = pull_full(cfg, [failing], tmp_path, now=T0)
    assert out["shards_failed"] == [1] and "finished_at" not in read_json(tmp_path / "_meta.json")
    (tmp_path / "shard=001").mkdir(exist_ok=True)
    pq.write_table(FULL_SCHEMA.empty_table(),
                   tmp_path / "shard=001" / "part-0000000000-0000000010.parquet")
    retry = FakeSession(PROBE, sizes=sizes)
    out = pull_full(cfg, [retry], tmp_path, now=T1)
    assert _ranges(retry) == [("url_state_current_001", 10, 20)]
    assert out["shards_failed"] == [] and "finished_at" in read_json(tmp_path / "_meta.json")


# PostgreSQL `COPY ... (FORMAT CSV)` output: NULL is an unquoted empty field, an
# empty string is "", quotes inside a quoted field are doubled.
CSV_CHUNK = (
    b'"https://a.com/x?q=""a,b""\nnext",7,2026-10-09 05:39:22.423515,12,t,1,2,f,0,t,3,f,t,'
    b'100,5,0.30000000000000004,0\n'
    b'https://b.com/NULL,8,2026-09-01 00:01:00,,f,,,,,,,f,f,,,-Infinity,\n'
)


def test_full_export_parses_the_csv_copy_output(tmp_path, base_cfg):
    session = FakeSession(PROBE, sizes={"url_state_current_000": [10]}, csv=CSV_CHUNK)
    pull_full(_full_cfg(base_cfg), [session], tmp_path, shards=[0], now=T0)
    assert "TO STDOUT (FORMAT CSV)" in session.copies[0][0]
    got = pq.read_table(tmp_path / "shard=000" / "part-0000000000-0000000010.parquet")
    assert got.schema.equals(FULL_SCHEMA, check_metadata=False)
    unset = dict.fromkeys(("title_len", "discovery_source_type", "robots_bits", "is_redirect",
                           "redirect_hop_count", "has_json_ld", "hreflang_count",
                           "inlink_count_approx", "inlink_count_external", "source"))
    assert got.to_pylist() == [
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


def test_full_export_records_cluster_wal_and_writes_per_shard(tmp_path, base_cfg):
    session = FakeSession(PROBE, sizes={"url_state_current_000": [10]},
                          io=[(1_000, 10, 50), (4_000, 12, 80)])
    pull_full(_full_cfg(base_cfg), [session], tmp_path, shards=[0], now=T0)
    io = read_json(tmp_path / "_meta.json")["io"]["0"]
    assert {k: io[k] for k in ("wal_bytes", "wal_fpi", "client_backend_writes")} == {
        "wal_bytes": 3_000, "wal_fpi": 2, "client_backend_writes": 30}
    assert io["seconds"] >= 0


def test_full_export_spreads_shards_over_sessions_and_reads_each_range_once(tmp_path,
                                                                           base_cfg):
    cfg = replace(_full_cfg(base_cfg), "crawlerdb", num_shards=6)
    sizes = {f"url_state_current_{s:03d}": [10 * (s + 1)] for s in range(6)}
    sessions = [FakeSession(PROBE, sizes=sizes) for _ in range(3)]
    out = pull_full(cfg, sessions, tmp_path, now=T0)
    ranges = [r for s in sessions for r in _ranges(s)]
    expected = {(f"url_state_current_{s:03d}", lo, lo + 10) for s in range(6)
                for lo in range(0, 10 * (s + 1), 10)}
    assert len(ranges) == len(set(ranges)) and set(ranges) == expected
    meta = read_json(tmp_path / "_meta.json")
    assert out["shards_missing"] == 0 and meta["shards_done"] == list(range(6))
    assert set(meta["io"]) == {str(s) for s in range(6)} and "finished_at" in meta


def test_a_broken_session_stops_the_export_and_raises(tmp_path, base_cfg):
    cfg = replace(_full_cfg(base_cfg), "crawlerdb", num_shards=4)
    sizes = {f"url_state_current_{s:03d}": [10] for s in range(4)}
    broken = FakeSession(PROBE, sizes=sizes, broken={f"url_state_current_{s:03d}"
                                                          for s in range(4)})
    with pytest.raises(RuntimeError, match="closed the connection"):
        pull_full(cfg, [broken, FakeSession(PROBE, sizes=sizes)], tmp_path, now=T0)
    assert "finished_at" not in read_json(tmp_path / "_meta.json")


def test_every_chunk_reserves_its_heap_bytes_from_the_shared_rate(tmp_path, base_cfg):
    rate = FakeRate()
    session = FakeSession(PROBE, sizes={"url_state_current_000": [35]})
    pull_full(_full_cfg(base_cfg), [session], tmp_path, shards=[0], now=T0, rate=rate)
    assert rate.reserved == [10 * 8192, 10 * 8192, 10 * 8192, 5 * 8192]


def test_full_export_reads_pages_appended_during_the_export(tmp_path, base_cfg):
    cfg = _full_cfg(base_cfg)
    session = FakeSession(PROBE, sizes={"url_state_current_000": [20, 26, 26]})
    pull_full(cfg, [session], tmp_path, shards=[0], now=T0)
    assert [r[1:] for r in _ranges(session)] == [(0, 10), (10, 20), (20, 26)]
    assert read_json(tmp_path / "_meta.json")["extents"]["0"] == [[0, 20], [20, 26]]


def test_full_export_refuses_another_layout_or_a_full_disk(tmp_path, base_cfg):
    sizes = {"url_state_current_000": [10], "url_state_current_001": [10]}
    pull_full(_full_cfg(base_cfg), [FakeSession(PROBE, sizes=sizes)], tmp_path, shards=[0],
              now=T0)
    with pytest.raises(RuntimeError, match=r"different full\.chunk_blocks"):
        pull_full(_full_cfg(base_cfg, chunk_blocks=5), [FakeSession(PROBE, sizes=sizes)],
                  tmp_path, now=T1)
    with pytest.raises(RuntimeError, match="GB free"):
        pull_full(_full_cfg(base_cfg, min_free_gb=1e9), [FakeSession(PROBE, sizes=sizes)],
                  tmp_path, now=T1)


def test_full_estimate_counts_chunks_and_worst_case_hours(base_cfg):
    cfg = _full_cfg(base_cfg, chunk_blocks=131072)
    sizes = {"url_state_current_000": [131072 * 3 + 1], "url_state_current_001": [0]}
    est = estimate_full(cfg, FakeSession(PROBE, sizes=sizes))
    assert est["chunks"] == 4 and est["largest_shard"] == 0
    assert est["chunk_timeout_s"] == 215     # 1 GiB at 5 MB/s
