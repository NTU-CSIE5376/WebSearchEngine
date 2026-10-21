"""Recrawl pieces that need no database: ids, outcomes, result mapping and the API client."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import duckdb
import pytest
from conftest import BASE, replace
from recrawl_fakes import ApiStub, FakeCrawler, Page

from isel.config import ConfigError, load_config
from isel.recrawl import (
    CrawlerApi,
    RecrawlError,
    Reply,
    classify,
    host_id,
    map_results,
    pg_doc_id,
    read_item,
    run_loop,
)
from isel.urlnorm import md5_number_lower

URL = "https://a.example/x"


def item(**kw):
    rec = {"url": URL, "final_url": URL, "status_code": 200, "error": None,
           "html_file": "00000.html", "fetched_at": "2026-10-15T12:00:01+00:00"}
    rec.update(kw)
    return rec


def test_ids_match_duckdb_and_fit_postgres():
    urls = [f"https://h{i}.example/p" for i in range(64)]
    assert any(md5_number_lower(u) >= 1 << 63 for u in urls)   # both branches covered
    con = duckdb.connect()
    for u in urls:
        signed, hid = con.execute(
            "SELECT CASE WHEN md5_number_lower(?) > 9223372036854775807 THEN "
            "(md5_number_lower(?) - 9223372036854775808)::BIGINT - 9223372036854775807 - 1 "
            "ELSE md5_number_lower(?)::BIGINT END, "
            "(md5_number_lower(?) & 9223372036854775807)::BIGINT", [u] * 4).fetchone()
        assert signed == pg_doc_id(u) and -(1 << 63) <= signed < 1 << 63
        assert (signed + (1 << 64)) % (1 << 64) == md5_number_lower(u)
        assert hid == host_id(u) and 0 <= hid < 1 << 63


@pytest.mark.parametrize(("kw", "expected"), [
    ({}, ("fetched", None)),
    ({"error": "NonHTML content-type", "html_file": None}, ("gone", "non_html")),
    ({"status_code": 404, "error": "HTTP 404"}, ("gone", "http_404")),
    ({"status_code": 410, "error": "HTTP 410", "html_file": None}, ("gone", "http_410")),
    ({"status_code": None, "error": "IgnoreRequest", "html_file": None},
     ("robots", "ignore_request")),
    ({"status_code": 429, "error": "HTTP 429"}, ("throttled", "http_429")),
    ({"status_code": 503, "error": "HTTP 503", "html_file": None}, ("throttled", "http_503")),
    ({"status_code": None, "error": "TimeoutError", "html_file": None},
     ("retry", "TimeoutError")),
    ({"status_code": 500, "error": "HTTP 500"}, ("retry", "http_500")),
    ({"status_code": 301, "error": "HTTP 301", "html_file": None}, ("retry", "http_301")),
])
def test_classify(kw, expected):
    assert classify(item(**kw)) == expected


def test_map_results_pairs_by_position_and_catches_a_new_order():
    a, b, c = "https://a.example/1", "https://a.example/2", "https://b.example/1"
    rewritten = "https://a.example/H%C3%A9"
    sent = [a, "https://a.example/H%25C3%25A9", c]
    assert map_results(sent, [a, rewritten, c]) == [(0, 0), (1, 1), (2, 2)]
    assert map_results([a, b, c], [a, b, c]) == [(0, 0), (1, 1), (2, 2)]
    with pytest.raises(RecrawlError, match="not in the order sent"):
        map_results([a, b, c], [b, a, c])
    with pytest.raises(RecrawlError, match="not in the order sent"):
        map_results([a, b, c], sorted([c, a, b], reverse=True))
    # A duplicate was dropped: only unchanged URLs pair, by string.
    assert map_results([a, b, "https://a.example/2?"], [a, b]) == [(0, 0), (1, 1)]
    assert map_results([b, a, c, "https://x.example/%7E"], [b, a, c]) == [
        (0, 0), (1, 1), (2, 2)]
    with pytest.raises(RecrawlError, match="we sent 1"):
        map_results([a], [a, b])


def _item_file(tmp_path, **kw):
    (tmp_path / "00000.json").write_text(json.dumps(item(**kw)))


@pytest.mark.parametrize(("kw", "message"), [
    ({"url": "https://other.example/"}, "another URL"),
    ({"status_code": "200"}, "status_code"),
    ({"status_code": 99999}, "status_code"),
    ({"error": 5}, "error must be"),
    ({"final_url": 5}, "final_url must be"),
    ({"html_file": "../../etc/passwd"}, "html_file"),
    ({"html_file": "00001.html"}, "html_file"),
    ({"fetched_at": None}, "ISO 8601"),
    ({"fetched_at": "2026-10-15T12:00:01"}, "no UTC offset"),
])
def test_read_item_enforces_the_contract(tmp_path, kw, message):
    _item_file(tmp_path, **kw)
    with pytest.raises(ValueError, match=message):
        read_item(tmp_path, 0, URL)


def test_read_item_ignores_new_keys_and_parses_the_time(tmp_path):
    _item_file(tmp_path, content_type="text/html")
    got = read_item(tmp_path, 0, URL)
    assert got["fetched_at"] == datetime(2026, 10, 15, 12, 0, 1, tzinfo=UTC)
    (tmp_path / "00000.json").write_text(json.dumps({"url": URL}))
    with pytest.raises(ValueError, match="lacks a result key"):
        read_item(tmp_path, 0, URL)


def test_run_loop_once_returns_the_first_cycle():
    calls = []
    assert run_loop(lambda: calls.append(1) or {"n": len(calls)}, 5.0, once=True,
                    sleep=pytest.fail) == {"n": 1}


def test_recrawl_config_is_strict(tmp_path, base_cfg):
    path = tmp_path / "isel.toml"
    cases = [
        (("sweep_rows = 100000", ""), "sweep_rows"),
        (('api_url = "http://127.0.0.1:4000"', 'api_url = "http://127.0.0.1:4000/"'), "api_url"),
        (('"work/recrawl/log"', '"work/recrawl/text"'), "must differ"),
        (("job_size = 3000 ", "job_size = 10001 "), "max_urls"),
        (("host_docs_per_job = 120 ", "host_docs_per_job = 3001 "), "host_docs_per_job"),
        (("claim_timeout_s = 600 ", "claim_timeout_s = 5 "), "claim_timeout_s"),
        (("status_poll_s = 60 ", "status_poll_s = 259200 "), "status_poll_s"),
        (("status_poll_s = 60 ", "status_poll_s = -1 "), "status_poll_s"),
        (("result_ttl_days = 3 ", "result_ttl_days = 0 "), "result_ttl_days"),
        (("result_ttl_days = 3 ", "result_ttl_days = 61 "), "result_ttl_days"),
        (("archive_workers = 8", "archive_workers = 0"), "must be > 0"),
        (("max_resend = 20 ", "max_resend = -1 "), "max_resend"),
    ]
    for (old, new), message in cases:
        assert old in BASE.read_text(), old
        path.write_text(BASE.read_text().replace(old, new))
        with pytest.raises(ConfigError, match=message):
            load_config([path])
    assert replace(base_cfg, "recrawl", job_size=1).recrawl.job_size == 1
    assert replace(base_cfg, "recrawl", status_poll_s=0).recrawl.status_poll_s == 0


@pytest.fixture
def stub(tmp_path):
    s = ApiStub(tmp_path / "api")
    yield s
    s.close()


def test_the_api_client_reads_every_documented_answer(stub):
    api = CrawlerApi(stub.url, timeout_s=2.0)
    reply = api.submit([URL, "https://a.example/q?b=2&a=1", URL], 3)
    assert reply.status == 200 and len(reply.job_id) == 32
    pending = json.loads((stub.pending / f"{reply.job_id}.json").read_text())
    assert pending["urls"] == [URL, "https://a.example/q?a=1&b=2"]  # canonical, deduplicated
    assert pending["ttl_days"] == 3
    assert api.status(reply.job_id) == ("pending", None)
    done = datetime.now(UTC)
    FakeCrawler(stub, {URL: Page(body=b"<p>x</p>")}).take(done)
    assert api.status(reply.job_id) == ("completed", done.isoformat())
    assert api.status("0" * 32) == ("missing", None)
    stub.status_codes["1" * 32] = 503
    assert api.status("1" * 32) == (None, None)
    assert api.submit([URL, "http://localhost/"], 3) == Reply(
        400, error="Invalid HTTP(S) URL at index 1")
    assert api.submit([URL], 61) == Reply(
        400, error='"ttl_days" must be an integer between 1 and 60')
    stub.forced = [(503, "Submission unavailable; no URLs accepted")]
    assert api.submit([URL], 3) == Reply(503, error="Submission unavailable; no URLs accepted")
    gone = CrawlerApi("http://127.0.0.1:9", 0.5)        # nothing listens
    assert gone.submit([URL], 3) == Reply(None)
    with pytest.raises(OSError):
        gone.status("0" * 32)
    assert gone.delete("0" * 32) is False


def test_the_api_client_deletes_and_sees_expired_jobs(stub):
    api = CrawlerApi(stub.url, timeout_s=2.0)
    done, live = api.submit([URL], 1).job_id, api.submit(["https://b.example/"], 3).job_id
    FakeCrawler(stub, {}).take(datetime.now(UTC), hold={live})
    stub.delete_codes = [503]
    assert api.delete(done) is False and (stub.results / done).is_dir()
    assert api.delete(done) is True and not (stub.results / done).exists()
    assert api.delete(done) is True                     # 404: nothing left to delete
    now = datetime.now(UTC)
    stub.clock = lambda: now + timedelta(days=3)
    assert api.status(live) == ("missing", None)        # past its TTL
    assert stub.collect_expired() == 1 and stub.jobs() == []
