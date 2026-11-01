"""The recrawl client against a fake crawler API v2: no database, files only."""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import BASE, replace
from recrawl_fakes import TOKEN, FakeCrawlApi, Page

from isel.config import ConfigError, load_config
from isel.recrawl import (
    CrawlApi,
    LiveRun,
    Puller,
    RecrawlError,
    apply_segments,
    classify,
    compact,
    compute_due,
    layout,
    push_cycle,
    read_record,
    run_loop,
)
from isel.textindex import latest_docs
from isel.urlnorm import md5_number_lower

T0 = datetime(2026, 10, 20, 12, 30, tzinfo=UTC)     # outside every guard window
GOOD = "<html><head><title>T</title></head><body><p>" + "alpha beta gamma " * 20 + "</p></body></html>"


@pytest.fixture
def api():
    fake = FakeCrawlApi()
    fake.clock = lambda: T0 + timedelta(minutes=1)
    yield fake
    fake.close()


@pytest.fixture
def cfg(base_cfg, tmp_path, api):
    c = replace(base_cfg, "run", work_dir=str(tmp_path / "work"))
    c = replace(c, "duckdb", threads=2, memory_limit="1GB", max_temp_directory_size="1GB")
    return replace(c, "recrawl", root=str(tmp_path / "recrawl"), api_url=api.url,
                   archive_workers=1, min_free_gb=0, batch_urls=2)


def client(api: FakeCrawlApi) -> CrawlApi:
    return CrawlApi(api.url, TOKEN, 2.0)


def make_run(cfg, run_id: int, urls: list[str]) -> LiveRun:
    """A select-full run whose manifest lists `urls` by rank, as validate_run expects."""
    label = f"20261020T00000{run_id}000000Z"
    run_dir = cfg.work_dir / "full" / "selection" / f"run={label}"
    path = run_dir / "manifest" / "part-0.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({
        "doc_id": pa.array([md5_number_lower(u) for u in urls], pa.uint64()),
        "canonical_url": urls, "host": [u.split("/")[2] for u in urls],
        "rank": pa.array(range(1, len(urls) + 1), pa.int64())}), path)
    (run_dir / "_MANIFEST.json").write_text(json.dumps({
        "scope": "full", "rows": len(urls),
        "files": {"manifest/part-0.parquet": {
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "rows": len(urls)}}}))
    return LiveRun(run_id, label, run_dir)


def due_ids(cfg, live: LiveRun, request_id: str, now: datetime) -> list[int]:
    path, _ = compute_due(cfg, layout(cfg), live, request_id, now)
    return pq.read_table(path).column("doc_id").to_pylist()


def outcomes(cfg) -> list[dict]:
    files = sorted(layout(cfg).outcome.glob("**/*.parquet"))
    return [row for f in files for row in pq.read_table(f).to_pylist()]


def request_id(cfg) -> str:
    return json.loads(layout(cfg).request_file.read_text())["request_id"]


URLS = [f"https://h{i}.example/p" for i in range(5)]


# ---------------------------------------------------------------- pieces

@pytest.mark.parametrize(("error", "html", "expected"), [
    (None, GOOD, ("fetched", None)),
    (None, None, ("retry", "no_html")),
    ("non_html", None, ("gone", "non_html")),
    ("http_404", None, ("gone", "http_404")),
    ("http_410", None, ("gone", "http_410")),
    ("robots", None, ("robots", "robots")),
    ("expired", None, ("requeue", "expired")),
    ("cancelled", None, ("requeue", "cancelled")),
    ("http_500", None, ("retry", "http_500")),
    ("http_429", None, ("retry", "http_429")),
    ("dns", None, ("retry", "dns")),
    ("retries_exhausted", None, ("retry", "retries_exhausted")),
])
def test_classify(error, html, expected):
    assert classify(error, html) == expected


def _record(**kw):
    rec = {"id": "12", "url": "https://a.example/", "priority": 1, "final_url": None,
           "status_code": None, "error": "dns", "fetched_at": None,
           "finished_at": "2026-10-20T12:31:00+00:00", "html": None}
    rec.update(kw)
    return json.dumps(rec)


@pytest.mark.parametrize(("kw", "message"), [
    ({"id": "x1"}, "not a doc_id"),
    ({"id": str(1 << 64)}, "not a doc_id"),
    ({"status_code": 99}, "HTTP status"),
    ({"status_code": True}, "HTTP status"),
    ({"html": 1}, "html must be"),
    ({"finished_at": "2026-10-20T12:31:00"}, "UTC offset"),
    ({"finished_at": None}, "ISO 8601"),
])
def test_read_record_enforces_the_contract(kw, message):
    with pytest.raises(RecrawlError, match=message):
        read_record(_record(**kw))


def test_read_record_ignores_new_keys_and_parses_times():
    rec = read_record(_record(extra=1, fetched_at="2026-10-20T12:30:00+00:00"))
    assert rec["doc_id"] == 12 and rec["fetched_at"].tzinfo is not None
    with pytest.raises(RecrawlError, match="contract key"):
        read_record(json.dumps({"id": "1"}))


def test_run_loop_once_returns_the_first_cycle():
    assert run_loop(lambda: {"wait_s": 3.0}, once=True, sleep=pytest.fail) == {"wait_s": 3.0}


def test_recrawl_config_is_strict(tmp_path):
    path = tmp_path / "isel.toml"
    cases = [
        (("batch_urls = 10000 ", "batch_urls = 10001 "), "max_urls"),
        (("clock_skew_s = 300 ", "clock_skew_s = 600 "), "clock_skew_s"),
        (("expires_days = 30 ", "expires_days = 61 "), "expires_days"),
        (("long_poll_s = 30 ", "long_poll_s = 61 "), "long_poll_s"),
        (("archive_workers = 8", "archive_workers = 0"), "must be > 0"),
        (("retry_max_s = 86400", "retry_max_s = 60"), "retry_base_s"),
        (('root = "work/recrawl" ', 'root = "" '), "root must be set"),
    ]
    for (old, new), message in cases:
        assert old in BASE.read_text(), old
        path.write_text(BASE.read_text().replace(old, new))
        with pytest.raises(ConfigError, match=message):
            load_config([path])


def test_the_api_client_needs_its_token(api):
    assert CrawlApi(api.url, "wrong", 2.0).requests().status == 401
    assert client(api).requests().status == 200
    assert CrawlApi("http://127.0.0.1:9", TOKEN, 0.5).requests().status is None


# ---------------------------------------------------------------- push

def test_push_sends_the_list_by_rank_and_records_invalid_urls(cfg, api):
    live = make_run(cfg, 1, URLS)
    api.reject.add(URLS[3])
    stats = push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    assert stats["created"] == rid and stats["due"] == 5
    assert [b for _, b, _ in api.posts] == [f"{p.stem}" for p in
                                             sorted(layout(cfg).push(rid).glob("*.parquet"))]
    sent = [i for _, _, ids in api.posts for i in ids]
    assert sent == [str(md5_number_lower(u)) for u in URLS]          # by rank
    assert stats["invalid"] == 1 and stats["docs"] == 4
    (gone,) = outcomes(cfg)
    assert gone["doc_id"] == md5_number_lower(URLS[3]) and gone["fail_reason"] == "invalid_url"
    # Everything is in flight or gone: nothing is due until results arrive.
    later = push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0 + timedelta(hours=25))
    assert later["due"] == 0 and len(api.posts) == 3
    assert len(list(layout(cfg).due(rid).glob("due-*.parquet"))) == 1    # the old one went
    idle = push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0 + timedelta(hours=26))
    assert idle["idle"] and idle["wait_s"] > 3600


def test_push_pauses_on_503_and_resends_the_same_batch(cfg, api):
    live = make_run(cfg, 1, URLS)
    api.forced = [(503, 7)]
    first = push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    assert first["paused"] and first["wait_s"] == 7
    rid = request_id(cfg)
    (push_file,) = layout(cfg).push(rid).glob("*.parquet")
    before = pq.read_table(push_file).column("pushed_at").to_pylist()
    second = push_cycle(cfg, layout(cfg), client(api), live,
                        clock=lambda: T0 + timedelta(minutes=5))
    assert second["batches"] == 3 and api.posts[0][1] == push_file.stem
    assert pq.read_table(push_file).column("pushed_at").to_pylist() == before


def test_push_answers_a_resent_batch_once(cfg, api):
    live = make_run(cfg, 1, URLS)
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    last = sorted(layout(cfg).push(rid).glob("*.parquet"))[-1]
    layout(cfg).due(rid).joinpath(next(layout(cfg).due(rid).glob("*.parquet")).stem
                                  + ".done").unlink()
    again = push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    assert again["batches"] == 1 and api.posts[-1][1] == last.stem
    assert len(api.posts) == 3 and len(api.queued(rid)) == 5     # the stored answer


def test_push_waits_out_a_guard_window(cfg, api):
    live = make_run(cfg, 1, URLS)
    stats = push_cycle(cfg, layout(cfg), client(api), live,
                       clock=lambda: datetime(2026, 10, 20, 12, 59, tzinfo=UTC))
    assert stats["guarded"] and stats["batches"] == 0 and 0 < stats["wait_s"] <= 11 * 60


def test_a_refused_batch_is_not_left_in_flight(cfg, api):
    live = make_run(cfg, 1, URLS)
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    api.crawl({})                                       # dns for all: retries are due
    Puller(cfg, client(api)).cycle(force=True, now=T0)
    api.requests[rid].cancelled = True                  # e.g. cancelled by an operator
    with pytest.raises(RecrawlError, match="409"):
        push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0 + timedelta(hours=25))
    assert len(list(layout(cfg).push(rid).glob("*.parquet"))) == 3   # the refused one is gone


def test_a_new_live_run_replaces_the_request(cfg, api):
    push_cycle(cfg, layout(cfg), client(api), make_run(cfg, 1, URLS), clock=lambda: T0)
    old = request_id(cfg)
    stats = push_cycle(cfg, layout(cfg), client(api), make_run(cfg, 2, URLS[:2]),
                       clock=lambda: T0)
    new = request_id(cfg)
    assert stats["cancelled"] == old and stats["created"] == new != old
    assert api.deletes == [old] and api.requests[old].cancelled
    assert json.loads(layout(cfg).request_file.read_text())["model_run_id"] == 2


def test_a_request_near_its_expiry_is_replaced(cfg, api):
    live = make_run(cfg, 1, URLS)
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    old = request_id(cfg)
    days = cfg.recrawl.expires_days
    near = T0 + timedelta(days=days - 2)
    assert "renewed" not in push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: near)
    late = T0 + timedelta(days=days - 1, minutes=1)
    stats = push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: late)
    assert stats["renewed"] and stats["cancelled"] == old and request_id(cfg) != old
    # Docs still queued under the old request are sent again under the new one.
    assert len(api.queued(request_id(cfg))) == 5


# ---------------------------------------------------------------- pull

def test_pull_stores_text_and_outcomes_and_maps_by_id(cfg, api):
    urls = ["https://a.example/q?b=2&a=1", *URLS[:3]]         # the API reorders the query
    live = make_run(cfg, 1, urls)
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    canon = "https://a.example/q?a=1&b=2"
    api.crawl({canon: Page(html=GOOD), URLS[0]: Page(html=None, status=404, error="http_404"),
               URLS[1]: Page(html=None, status=200, error="non_html")})
    stats = Puller(cfg, client(api)).cycle(force=True, now=T0)
    assert stats["segments"] == 1 and stats["records"] == 4
    assert api.requests[rid].segments == {}                      # acked
    rows = {r["doc_id"]: r for r in outcomes(cfg)}
    assert rows[md5_number_lower(urls[0])]["outcome"] == "fetched"
    assert rows[md5_number_lower(URLS[0])]["outcome"] == "gone"
    assert rows[md5_number_lower(URLS[2])]["fail_reason"] == "dns"
    (text,) = layout(cfg).text.glob("**/*.parquet")
    (page,) = pq.read_table(text).to_pylist()
    assert page["doc_id"] == md5_number_lower(urls[0]) and page["url"] == canon
    assert "alpha" in page["body"] and page["source"].startswith(rid)
    assert rows[md5_number_lower(URLS[1])]["fail_reason"] == "non_html"
    assert latest_docs(duckdb.connect(), layout(cfg).text, 100)[0] == 1


def test_the_same_segments_twice_write_the_same_files(cfg, api):
    live = make_run(cfg, 1, URLS[:2])
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    name = api.crawl({URLS[0]: Page(html=GOOD)})
    data = api.requests[rid].segments[name]
    apply_segments(cfg, layout(cfg), rid, [(name, data)], T0)
    first = {p: p.read_bytes() for p in layout(cfg).root.glob("[ot]*/**/*.parquet")}
    apply_segments(cfg, layout(cfg), rid, [(name, data)], T0)
    assert {p: p.read_bytes() for p in layout(cfg).root.glob("[ot]*/**/*.parquet")} == first


def test_pull_bounds_a_batch_by_size(cfg, api):
    live = make_run(cfg, 1, URLS)
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    for u in URLS[:3]:
        api.crawl({u: Page(html=GOOD)}, only={str(md5_number_lower(u))})
    small = replace(cfg, "recrawl", pull_batch_mb=1)
    sizes = [len(b) for b in api.requests[rid].segments.values()]
    assert sum(sizes) < 1e6                            # all three fit in one batch
    assert Puller(small, client(api)).cycle(force=True, now=T0)["segments"] == 3
    for u in URLS[3:]:
        api.crawl({u: Page(html=GOOD)}, only={str(md5_number_lower(u))})
    tiny = replace(cfg, "recrawl", pull_batch_mb=1, pull_batch_segments=1)
    assert Puller(tiny, client(api)).cycle(force=True, now=T0)["segments"] == 1
    assert len(api.requests[rid].segments) == 1


def test_pull_stops_below_its_free_space(cfg, api):
    stats = Puller(replace(cfg, "recrawl", min_free_gb=10**9), client(api)).cycle(force=True)
    assert stats["disk_full"] and stats["segments"] == 0


def test_a_bad_record_is_not_acked(cfg, api):
    live = make_run(cfg, 1, URLS[:1])
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    api.requests[rid].segments["00-000000000009.jsonl.gz"] = gzip.compress(b'{"id": "1"}\n')
    with pytest.raises(RecrawlError):
        Puller(cfg, client(api)).cycle(force=True, now=T0)
    assert "00-000000000009.jsonl.gz" in api.requests[rid].segments


# ---------------------------------------------------------------- due

def _cycle(cfg, api, live, at, pages):
    assert not push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: at).get("guarded")
    api.clock = lambda: at + timedelta(minutes=1)
    api.crawl(pages)
    Puller(cfg, client(api)).cycle(force=True, now=at)


def test_due_follows_each_outcome(cfg, api):
    live = make_run(cfg, 1, URLS)
    _cycle(cfg, api, live, T0, {
        URLS[0]: Page(html=GOOD),
        URLS[1]: Page(html=None, status=404, error="http_404"),
        URLS[2]: Page(html=None, status=None, error="robots"),
        URLS[3]: Page(html=None, status=500, error="http_500"),
        # URLS[4]: no page, so dns
    })
    rid = request_id(cfg)
    ids = [md5_number_lower(u) for u in URLS]
    assert due_ids(cfg, live, rid, T0 + timedelta(minutes=5)) == []
    # retry_base_s 600 after the first failure; the clock skew does not hide it.
    assert due_ids(cfg, live, rid, T0 + timedelta(minutes=12)) == [ids[3], ids[4]]
    assert due_ids(cfg, live, rid, T0 + timedelta(days=31)) == [ids[0], ids[2], ids[3], ids[4]]


def test_retries_back_off_and_then_wait_a_revisit(cfg, api):
    cfg = replace(cfg, "recrawl", due_interval_h=0.0001)   # a new due file every cycle
    cfg = replace(cfg, "guard", month_days=[], hourly_window=[59, 0])   # off these times
    live = make_run(cfg, 1, URLS[:1])
    rid = None
    at = T0
    for n in range(1, cfg.recrawl.max_attempts + 1):
        _cycle(cfg, api, live, at, {})                       # dns every time
        rid = request_id(cfg)
        delay = min(cfg.recrawl.retry_base_s * 2 ** (n - 1), cfg.recrawl.retry_max_s)
        if n < cfg.recrawl.max_attempts:
            assert due_ids(cfg, live, rid, at + timedelta(seconds=delay - 1)) == []
        at = at + timedelta(seconds=delay + 120)
        if n < cfg.recrawl.max_attempts:
            assert due_ids(cfg, live, rid, at) == [md5_number_lower(URLS[0])]
    assert due_ids(cfg, live, rid, at + timedelta(days=1)) == []
    assert due_ids(cfg, live, rid, at + timedelta(days=31)) == [md5_number_lower(URLS[0])]


def test_expired_docs_are_due_again_at_once(cfg, api):
    live = make_run(cfg, 1, URLS)
    push_cycle(cfg, layout(cfg), client(api), live, clock=lambda: T0)
    rid = request_id(cfg)
    assert due_ids(cfg, live, rid, T0 + timedelta(minutes=5)) == []      # all in flight
    api.finish_queued(rid, "expired")
    Puller(cfg, client(api)).cycle(force=True, now=T0)
    assert due_ids(cfg, live, rid, T0 + timedelta(minutes=5)) == [md5_number_lower(u)
                                                                  for u in URLS]


def test_a_late_cancel_does_not_free_docs_queued_in_the_new_request(cfg, api):
    push_cycle(cfg, layout(cfg), client(api), make_run(cfg, 1, URLS), clock=lambda: T0)
    old = request_id(cfg)
    later = T0 + timedelta(hours=1)
    live2 = make_run(cfg, 2, URLS)
    push_cycle(cfg, layout(cfg), client(api), live2, clock=lambda: later)   # cancel not pulled yet
    new = request_id(cfg)
    assert len(api.queued(new)) == 5
    api.clock = lambda: later + timedelta(minutes=2)          # the cancel records come later
    Puller(cfg, client(api)).cycle(force=True, now=later)
    assert {r["outcome"] for r in outcomes(cfg)} == {"requeue"}
    assert api.requests[old].segments == {}
    assert due_ids(cfg, live2, new, later + timedelta(minutes=10)) == []


def test_a_cancel_written_after_the_new_fetch_does_not_make_it_due(cfg, api):
    push_cycle(cfg, layout(cfg), client(api), make_run(cfg, 1, URLS), clock=lambda: T0)
    switch = T0 + timedelta(hours=1)
    api.clock = lambda: switch + timedelta(minutes=30)       # the crawler closes the old
    live2 = make_run(cfg, 2, URLS)                           # request late...
    push_cycle(cfg, layout(cfg), client(api), live2, clock=lambda: switch)
    new = request_id(cfg)
    api.clock = lambda: switch + timedelta(minutes=5)        # ...after the new fetches
    api.crawl({u: Page(html=GOOD) for u in URLS}, request_id=new)
    Puller(cfg, client(api)).cycle(force=True, now=switch)
    latest = {}
    for r in sorted(outcomes(cfg), key=lambda r: r["finished_at"]):
        latest[r["doc_id"]] = r["outcome"]
    assert set(latest.values()) == {"requeue"}               # newest rows are the old cancel
    assert due_ids(cfg, live2, new, switch + timedelta(hours=1)) == []


def test_compaction_keeps_what_is_due(cfg, api):
    live = make_run(cfg, 1, URLS)
    _cycle(cfg, api, live, T0 - timedelta(days=5), {URLS[0]: Page(html=GOOD)})
    _cycle(cfg, api, live, T0 - timedelta(days=3), {})       # the 16th is a guarded day
    rid = request_id(cfg)
    now = T0 + timedelta(days=1)
    before = due_ids(cfg, live, rid, now)
    old = sorted(layout(cfg).outcome.glob("**/*.parquet"))
    stats = compact(cfg, T0)
    assert stats["outcome_files"] == len(old) >= 2
    assert len(list(layout(cfg).outcome.glob("**/*.parquet"))) == 1
    assert due_ids(cfg, live, rid, now) == before
    con = duckdb.connect()
    assert latest_docs(con, layout(cfg).text, 100)[0] == 1


def test_compaction_drops_pushes_of_old_requests(cfg, api):
    push_cycle(cfg, layout(cfg), client(api), make_run(cfg, 1, URLS), clock=lambda: T0)
    old = request_id(cfg)
    push_cycle(cfg, layout(cfg), client(api), make_run(cfg, 2, URLS), clock=lambda: T0)
    stats = compact(cfg, T0)
    assert stats["push_dirs"] == 1 and stats["due_dirs"] == 1
    assert not layout(cfg).push(old).exists() and layout(cfg).push(request_id(cfg)).is_dir()
    assert not layout(cfg).due(old).exists() and layout(cfg).due(request_id(cfg)).is_dir()


def test_files_root_holds_only_known_dirs(cfg, api):
    _cycle(cfg, api, make_run(cfg, 1, URLS[:1]), T0, {URLS[0]: Page(html=GOOD)})
    names = {p.name for p in Path(cfg.recrawl.root).iterdir()}
    assert names <= {"request.json", "due", "push", "outcome", "text", "duckdb_tmp"}
