from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
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


def test_latest_docs_lets_a_row_without_text_lose_to_any_row_with_text(cfg, tmp_path):
    from isel.io import open_duckdb

    root = tmp_path / "text"
    challenge = {"title": None, "body": ""}     # a WAF page that an older archive stored
    _write(root / "fetch_date=20261001" / "job-1.parquet", [
        _row(1, OLD, "real", "text"), _row(2, OLD, None, "only a body"), _row(3, OLD, **challenge)])
    _write(root / "fetch_date=20261020" / "job-2.parquet", [
        _row(1, NEW, **challenge), _row(2, NEW, **challenge), _row(3, NEW, **challenge)])
    _, reader = latest_docs(open_duckdb(cfg, tmp_path / "tmp"), root, 10)
    table = reader.read_all().sort_by("doc_id")
    assert table["body"].to_pylist() == ["text", "only a body", ""]
    assert table["title"].to_pylist()[0] == "real"
    # With no text at all the doc stays: its URL's words can still find it.
    assert table["doc_id"].to_pylist()[2] == 3


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
    spec = json.loads((ROOT / "config/text_index/icu.json").read_text())
    stub.indices["isel-text-x"] = {"docs": {ids[0]: {}, ids[1]: {}}, "body": spec,
                                   "settings": {}}
    stub.aliases["isel-text"] = {"isel-text-x"}
    stub.hits = {"alpha": [ids[1], "999"], "beta": []}
    out = evaluate_golden(cfg, _client(stub), 20)
    saved = pq.read_table(next((cfg.work_dir / "text_index" / "eval").glob("*.queries.parquet")))
    by_query = {r["query_id"]: r for r in saved.to_pylist()}
    assert by_query[1]["present"] == 2 and by_query[1]["ranked"] == [ids[1]]
    assert by_query[2]["present"] == 0 and by_query[2]["ranked"] == []
    assert out["all"]["recall_given_present_ci95"][0] <= out["all"]["recall_given_present"]
    assert out["all"]["pairs"] == 3 and out["all"]["recall_pairs"] == pytest.approx(1 / 3)
    assert out["all"]["presence"] == pytest.approx(2 / 3)
    assert out["all"]["recall_given_present"] == pytest.approx(1 / 2)
    assert out["head"]["recall_pairs"] == pytest.approx(1 / 2)
    assert out["random"]["zero_hit_rate"] == 1.0
    assert out["index"] == ["isel-text-x"]


def test_bulk_resends_items_refused_with_503(stub):
    stub.indices["i"] = {"docs": {}, "body": None, "settings": {}}
    payload = b'{"index":{"_id":"1"}}\n{"url":"u"}\n'
    stub.item_status = [503]
    assert _client(stub).bulk("i", payload) == 1
    assert len(stub.paths("POST")) == 2


def test_lost_replies_to_idempotent_requests_are_resent(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1), _row(2)])
    stub.drop = ["/_bulk", "/_tasks/", "/_count", "/_stats"]
    out = build(cfg, _client(stub), root)
    assert out["docs"] == 2 and stub.aliases["isel-text"] == {out["index"]}
    assert stub.drop == []


def test_a_lost_swap_reply_keeps_the_aliased_index(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    client = _client(stub)
    first = build(cfg, client, root, datetime(2026, 10, 23, 1, tzinfo=UTC))
    stub.drop = ["/_aliases"]
    second = build(cfg, client, root, datetime(2026, 10, 23, 2, tzinfo=UTC))
    assert stub.aliases["isel-text"] == {second["index"]}
    assert set(stub.indices) == {first["index"], second["index"]}


def test_a_refused_swap_drops_the_new_index(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    client = _client(stub)
    first = build(cfg, client, root, datetime(2026, 10, 23, 1, tzinfo=UTC))
    stub.fail = ["/_aliases"]
    with pytest.raises(TextIndexError, match="HTTP 500"):
        build(cfg, client, root, datetime(2026, 10, 23, 2, tzinfo=UTC))
    assert stub.aliases["isel-text"] == {first["index"]} and set(stub.indices) == {first["index"]}


def test_build_stops_before_the_merge_when_the_leases_lag(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    stub.lease_lag = 1
    with pytest.raises(TextIndexError, match="retention leases"):
        build(replace(cfg, "text_index", lease_wait_s=0.05), _client(stub), root)
    assert stub.indices == {}
    assert not any("_forcemerge" in p for p in stub.paths("POST"))


def test_build_refuses_to_load_past_the_flood_stage(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1), _row(2)])
    stub.fs = {"total_in_bytes": 100_000, "available_in_bytes": 8_000}
    cfg = replace(cfg, "text_index", expected_bytes_per_doc=1_000)   # needs 4,000
    with pytest.raises(TextIndexError, match="flood stage"):
        build(cfg, _client(stub), root)
    assert stub.paths("PUT") == []


def test_build_refuses_a_merge_past_the_flood_stage(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    # The load needs 2 bytes, the merge as much as the store holds (1,000 in the stub).
    stub.fs = {"total_in_bytes": 100_000, "available_in_bytes": 5_900}
    cfg = replace(cfg, "text_index", expected_bytes_per_doc=1)
    with pytest.raises(TextIndexError, match=r"merging isel-text-\S+ needs 1000 bytes"):
        build(cfg, _client(stub), root)
    assert stub.indices == {}
    assert not any("_forcemerge" in p for p in stub.paths("POST"))


def test_every_merge_poll_refreshes(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    stub.merge_polls = 2
    out = build(cfg, _client(stub), root)
    calls = [p for _, p, _ in stub.requests]
    merge = next(n for n, p in enumerate(calls) if "_forcemerge" in p)
    assert calls[merge + 1:merge + 7] == ["/_tasks/node:1", f"/{out['index']}/_refresh"] * 3


def test_build_keeps_a_newer_index_another_build_is_loading(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    newer = "isel-text-20991231t000000000000z"
    stub.indices[newer] = {"docs": {}, "body": None, "settings": {}}
    out = build(replace(cfg, "text_index", keep_prev=False), _client(stub), root)
    assert set(stub.indices) == {out["index"], newer} and out["deleted"] == []


def test_build_drops_a_killed_build_before_it_loads(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    stray = "isel-text-20261001t000000000000z"
    stub.indices[stray] = {"docs": {}, "body": None, "settings": {}}
    out = build(cfg, _client(stub), root, datetime(2026, 10, 23, 1, tzinfo=UTC))
    calls = [(m, p) for m, p, _ in stub.requests]
    assert calls.index(("DELETE", f"/{stray}")) < calls.index(("PUT", f"/{out['index']}"))
    assert out["strays"] == [stray] and set(stub.indices) == {out["index"]}


def test_a_second_build_waits_for_no_lock(cfg, stub, tmp_path):
    import fcntl

    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    lock = cfg.work_dir / "text_index" / "build.lock"
    lock.parent.mkdir(parents=True)
    with lock.open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        with pytest.raises(TextIndexError, match="another text-index-build"):
            build(cfg, _client(stub), root)
    assert stub.paths("PUT") == []


def test_build_and_eval_reject_a_field_the_index_lacks(cfg, stub, tmp_path):
    root = tmp_path / "text"
    _write(root / "a" / "job-1.parquet", [_row(1)])
    bad = replace(cfg, "text_index", fields=["titel^2", "body"])
    with pytest.raises(TextIndexError, match="titel is not a text field"):
        build(bad, _client(stub), root)
    build(cfg, _client(stub), root)
    with pytest.raises(TextIndexError, match="titel is not a text field"):
        evaluate_golden(bad, _client(stub), 20)
    with pytest.raises(TextIndexError, match="one analyzer"):
        build(replace(cfg, "text_index", query_type="combined_fields",
                      fields=["title", "url.text"]), _client(stub), root)


def test_a_failed_shard_fails_the_eval(cfg, stub, tmp_path):
    url = "https://a.example/"
    golden = [{"batch_id": 20, "batch_created_at": NEW, "query_id": 1, "keyword": "alpha",
               "frequency": 1, "tags": ["head"], "rank": 1, "raw_url": url,
               "canonical_url": url, "host": "a.example"}]
    write_parquet_atomic(pa.Table.from_pylist(golden, schema=GOLDEN_SCHEMA),
                         golden_path(cfg.work_dir))
    spec = json.loads((ROOT / "config/text_index/icu.json").read_text())
    stub.indices["isel-text-x"] = {"docs": {str(md5_number_lower(url)): {}}, "body": spec,
                                   "settings": {}}
    stub.aliases["isel-text"] = {"isel-text-x"}
    stub.hits = {"alpha": [str(md5_number_lower(url))]}
    # A partial search would answer 200 with no hits, a recall of 0 and no error.
    stub.failed_shards = 1
    with pytest.raises(TextIndexError, match="HTTP 503"):
        evaluate_golden(cfg, _client(stub), 20)
