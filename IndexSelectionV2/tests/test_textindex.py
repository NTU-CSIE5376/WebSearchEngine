from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pytest
from conftest import ROOT, replace, small_cfg
from opensearch_fakes import OpenSearchStub

from isel.golden import GOLDEN_SCHEMA, golden_path
from isel.io import write_parquet_atomic
from isel.recrawl import TEXT_SCHEMA
from isel.textindex import (
    DOC_FIELDS,
    OpenSearch,
    TextIndexError,
    build,
    bulk_payloads,
    evaluate_golden,
    latest_docs,
    query_body,
)
from isel.urlnorm import md5_number_lower

OLD = datetime(2026, 10, 1, tzinfo=UTC)
NEW = datetime(2026, 10, 20, tzinfo=UTC)


def _row(doc_id: int, fetched_at: datetime = NEW, title: str | None = "t", body: str = "b"):
    return {"doc_id": doc_id, "batch_id": 1, "url": f"https://h{doc_id}.example/",
            "final_url": None, "fetched_at": fetched_at, "http_status": 200, "lang": "en",
            "title": title, "description": None, "body": body, "text_len": len(body),
            "payload_sha1": "x"}


def _write(path: Path, rows: list[dict]) -> None:
    write_parquet_atomic(pa.Table.from_pylist(rows, schema=TEXT_SCHEMA), path)


@pytest.fixture
def stub():
    s = OpenSearchStub()
    yield s
    s.close()


@pytest.fixture
def cfg(tmp_path):
    c = small_cfg(tmp_path / "work")
    return replace(c, "text_index", settings_file=str(ROOT / "config/text_index/icu.json"),
                   lease_wait_s=1.0, backoff_s=0.0)


def _client(stub: OpenSearchStub, retries: int = 2) -> OpenSearch:
    return OpenSearch(stub.url, 5.0, retries, 0.0, sleep=lambda s: None)


def _lines(payload: bytes) -> list[dict]:
    return [json.loads(line) for line in payload.decode().splitlines()]


def test_bulk_payloads_split_by_bytes_and_keep_u64_ids():
    big_id = (1 << 64) - 1
    rows = [_row(big_id), _row(1), _row(2, body="x" * 300), _row(3)]
    batch = pa.RecordBatch.from_pylist(rows, schema=TEXT_SCHEMA)
    sizes = [len(p) for p in bulk_payloads([batch], max_bytes=1)]   # one document each
    cap = sizes[0] + sizes[1]
    payloads = list(bulk_payloads([batch], max_bytes=cap))
    assert all(p.endswith(b"\n") for p in payloads)
    assert [len(p.splitlines()) for p in payloads] == [4, 2, 2]     # the long body alone
    assert len(payloads[0]) == cap and len(payloads[1]) > cap
    assert payloads[0].startswith(b'{"index":{"_id":"18446744073709551615"}}\n')
    doc = _lines(payloads[0])[1]
    assert set(doc) == {"doc_id", "url", "lang", "title", "body"}  # no null field
    assert doc["doc_id"] == big_id


def test_latest_docs_keeps_the_newest_row_of_each_doc(cfg, tmp_path):
    root = tmp_path / "text"
    _write(root / "fetch_date=20261001" / "job-1.parquet", [_row(1, OLD, "old"), _row(2, NEW, "a")])
    _write(root / "fetch_date=20261020" / "job-2.parquet", [_row(1, NEW, "new"), _row(2, NEW, "b")])
    _write(root / "fetch_date=20261021" / "job-3.parquet", [_row(3, NEW, "c")])
    (root / "fetch_date=20261021" / "job-3.parquet").rename(
        root / "fetch_date=20261021" / "job-3.parquet.tmp")
    from isel.io import open_duckdb

    con = open_duckdb(cfg, tmp_path / "tmp")
    rows, reader = latest_docs(con, root, 10)
    table = reader.read_all()
    assert rows == 2
    assert table.column_names == ["doc_id", *DOC_FIELDS]
    # doc 2 ties on fetched_at: the larger file name (job-2) wins.
    assert sorted(zip(table["doc_id"].to_pylist(), table["title"].to_pylist(),
                      strict=True)) == [(1, "new"), (2, "b")]


def test_latest_docs_needs_text(cfg, tmp_path):
    from isel.io import open_duckdb

    with pytest.raises(TextIndexError, match="no text Parquet"):
        latest_docs(open_duckdb(cfg, tmp_path / "tmp"), tmp_path / "empty", 10)


def test_bulk_resends_on_429_and_raises_on_other_item_errors(stub):
    client = _client(stub)
    stub.indices["i"] = {"docs": {}, "body": None, "settings": {}}
    payload = b'{"index":{"_id":"1"}}\n{"url":"u"}\n{"index":{"_id":"2"}}\n{"url":"v"}\n'
    stub.bulk_http = [429]
    assert client.bulk("i", payload) == 2
    stub.item_status = [201, 429]
    assert client.bulk("i", payload) == 2
    assert len(stub.paths("POST")) == 4
    stub.item_status = [400]
    with pytest.raises(TextIndexError, match="forced"):
        client.bulk("i", payload)
    stub.bulk_http = [429, 429, 429]
    with pytest.raises(TextIndexError, match="HTTP 429"):
        client.bulk("i", payload)


def test_build_swaps_the_alias_in_one_call_and_keeps_one_previous(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1), _row(2)])
    stub.indices["isel-text-v2-20261001t000000000000z"] = {"docs": {}, "body": None,
                                                           "settings": {}}
    client = _client(stub)
    times = [datetime(2026, 10, 23, h, tzinfo=UTC) for h in (1, 2, 3)]
    first, second, third = (build(cfg, client, root, now) for now in times)
    assert re.fullmatch(r"isel-text-\d{8}t\d{12}z", first["index"])
    assert first["docs"] == 2 and first["previous"] == []
    assert second["previous"] == [first["index"]] and second["deleted"] == []
    assert third["deleted"] == [first["index"]]
    assert stub.aliases["isel-text"] == {third["index"]}
    # The other alias's index is not one of our builds.
    assert set(stub.indices) == {second["index"], third["index"],
                                 "isel-text-v2-20261001t000000000000z"}
    swaps = [b for m, p, b in stub.requests if p == "/_aliases"]
    assert swaps[-1]["actions"] == [{"remove": {"index": second["index"], "alias": "isel-text"}},
                                    {"add": {"index": third["index"], "alias": "isel-text"}}]
    calls = [p for _, p, _ in stub.requests]
    merge = calls.index(f"/{third['index']}/_forcemerge?max_num_segments=1"
                        "&wait_for_completion=false")
    # A commit after the lease wait lets the merge drop the copies; searches then move to
    # the merged segment before writes are blocked and the alias moves.
    assert calls[merge - 1] == f"/{third['index']}/_flush?force=true"
    assert calls[merge + 1] == "/_tasks/node:1"
    assert calls[merge + 2] == f"/{third['index']}/_refresh"
    assert calls[merge + 4] == f"/{third['index']}/_settings"
    created = stub.indices[third["index"]]
    assert created["body"]["settings"]["number_of_replicas"] == 0
    assert created["body"]["settings"]["refresh_interval"] == "-1"
    assert created["settings"]["blocks.write"] is True
    manifest = json.loads((cfg.work_dir / "text_index" / "build"
                           / f"{third['index']}.json").read_text())
    assert manifest["rows"] == 2 and manifest["recovery_source_pruned"] is True
    # Sizes come from the live segments' files, not from the store.
    assert manifest["segment_bytes"] == 900 and manifest["bytes_per_doc"] == 450


def test_build_without_keep_prev_deletes_the_previous_index(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    cfg = replace(cfg, "text_index", keep_prev=False)
    client = _client(stub)
    first = build(cfg, client, root, datetime(2026, 10, 23, 1, tzinfo=UTC))
    second = build(cfg, client, root, datetime(2026, 10, 23, 2, tzinfo=UTC))
    assert second["deleted"] == [first["index"]]
    assert set(stub.indices) == {second["index"]}


def test_build_drops_the_new_index_when_a_doc_is_missing(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1), _row(2)])
    stub.count_offset = -1
    with pytest.raises(TextIndexError, match="holds 1 docs, expected 2"):
        build(cfg, _client(stub), root)
    assert stub.indices == {} and stub.aliases == {}
    assert "/_aliases" not in stub.paths("POST")


def test_build_drops_the_new_index_when_the_merge_fails(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    stub.merge_failed = 1
    with pytest.raises(TextIndexError, match=r"force merge of isel-text-\S+ failed"):
        build(cfg, _client(stub), root)
    assert stub.indices == {} and "/_aliases" not in stub.paths("POST")


@pytest.mark.parametrize("query_type, key", [
    ("combined_fields", "combined_fields"), ("most_fields", "multi_match"),
    ("best_fields", "multi_match")])
def test_query_body(base_cfg, query_type, key):
    t = replace(base_cfg, "text_index", query_type=query_type).text_index
    body = query_body(t, 'a "b" OR c:d')
    assert body["query"][key]["query"] == 'a "b" OR c:d'
    assert body["size"] == 10 and body["_source"] is False
    assert body["sort"] == [{"_score": "desc"}, {"doc_id": "asc"}]
    if key == "multi_match":
        assert body["query"][key]["type"] == query_type


def test_evaluate_golden_counts_pairs_by_doc_id(cfg, stub, tmp_path):
    urls = ["https://a.example/", "https://b.example/", "https://c.example/"]
    ids = [str(md5_number_lower(u)) for u in urls]
    golden = [
        {"batch_id": 20, "batch_created_at": NEW, "query_id": 1, "keyword": "alpha",
         "frequency": 1, "tags": ["head"], "rank": 1, "raw_url": urls[0],
         "canonical_url": urls[0], "host": "a.example"},
        {"batch_id": 20, "batch_created_at": NEW, "query_id": 1, "keyword": "alpha",
         "frequency": 1, "tags": ["head"], "rank": 2, "raw_url": urls[1],
         "canonical_url": urls[1], "host": "b.example"},
        {"batch_id": 20, "batch_created_at": NEW, "query_id": 2, "keyword": "beta",
         "frequency": 1, "tags": ["random"], "rank": 1, "raw_url": urls[2],
         "canonical_url": urls[2], "host": "c.example"},
    ]
    write_parquet_atomic(pa.Table.from_pylist(golden, schema=GOLDEN_SCHEMA),
                         golden_path(cfg.work_dir))
    stub.indices["isel-text-x"] = {"docs": {ids[0]: {}, ids[1]: {}}, "body": None,
                                   "settings": {}}
    stub.aliases["isel-text"] = {"isel-text-x"}
    stub.hits = {"alpha": [ids[1], "999"], "beta": []}
    out = evaluate_golden(cfg, _client(stub), 20)
    assert out["all"]["pairs"] == 3 and out["all"]["recall_pairs"] == pytest.approx(1 / 3)
    assert out["all"]["presence"] == pytest.approx(2 / 3)
    assert out["all"]["recall_given_present"] == pytest.approx(1 / 2)
    assert out["head"]["recall_pairs"] == pytest.approx(1 / 2)
    assert out["random"]["zero_hit_rate"] == 1.0
    assert out["index"] == ["isel-text-x"]
