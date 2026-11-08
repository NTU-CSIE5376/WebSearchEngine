"""Golden coverage treats a URL with and without Google's `srsltid` as the
same page: either form found in the crawler tables counts. Only `srsltid`
gets this treatment."""
import pytest

from Metric.Measure.CrawlerAllMetricMeasure import (
    apply_found_urls,
    build_lookup_map,
    golden_lookup_urls,
    normalize_golden_url,
    strip_srsltid,
)

TOKEN = "AfmBOoqfCkB7Gr36L8FzV4ncwpCatQZRYNOjavRk_Qp1ZbjcmArzMmZw"


@pytest.mark.parametrize(
    "url, stripped",
    [
        (f"https://ew.com/story-12174222?srsltid={TOKEN}", "https://ew.com/story-12174222"),
        (f"https://a.com/s?sort=relevance&srsltid={TOKEN}&term=b", "https://a.com/s?sort=relevance&term=b"),
        ("https://a.com/?SrsLtId=x", "https://a.com/"),
        ("https://a.com/p?srsltid=x#reviews", "https://a.com/p#reviews"),
        ("https://a.com/p?id=1", "https://a.com/p?id=1"),
        ("https://a.com/p?xsrsltid=1", "https://a.com/p?xsrsltid=1"),
        ("https://a.com/srsltid/page", "https://a.com/srsltid/page"),
    ],
)
def test_strip_srsltid(url, stripped):
    assert strip_srsltid(url) == stripped


def test_lookup_urls_only_add_srsltid_variant():
    assert golden_lookup_urls("https://a.com/p?id=1") == ["https://a.com/p?id=1"]
    # Other tracking params are not stripped.
    assert golden_lookup_urls("https://a.com/p?utm_source=x") == ["https://a.com/p?utm_source=x"]
    golden = normalize_golden_url(f"https://a.com/p?srsltid={TOKEN}&id=1")
    assert golden_lookup_urls(golden) == [golden, "https://a.com/p?id=1"]


def _status(golden_urls):
    return {u: {"discovered": False, "crawled": False, "indexed": False, "shard_id": -1}
            for u in golden_urls}


def test_either_form_counts():
    with_id = normalize_golden_url(f"https://shop.com/p/1?srsltid={TOKEN}")
    plain = "https://news.com/a"
    goldens = [with_id, plain]
    lookup = build_lookup_map(goldens)
    status = _status(goldens)

    # Crawler holds only the stripped form, crawled.
    apply_found_urls(status, lookup, [("https://shop.com/p/1", True, False, 7)])
    assert status[with_id] == {"discovered": True, "crawled": True, "indexed": False, "shard_id": 7}
    assert status[plain]["discovered"] is False


def test_crawled_if_any_form_crawled():
    golden = normalize_golden_url(f"https://shop.com/p/1?srsltid={TOKEN}")
    lookup = build_lookup_map([golden])
    status = _status([golden])
    # Both rows exist; only the stripped one was fetched. Order must not matter.
    apply_found_urls(status, lookup, [("https://shop.com/p/1", True, False, 7)])
    apply_found_urls(status, lookup, [(golden, False, False, 7)])
    assert status[golden]["crawled"] is True


def test_stripped_form_shared_by_two_goldens():
    a = normalize_golden_url(f"https://shop.com/p/1?srsltid={TOKEN}")
    b = "https://shop.com/p/1"
    lookup = build_lookup_map([a, b])
    status = _status([a, b])
    apply_found_urls(status, lookup, [(b, False, False, 3)])
    assert status[a]["discovered"] and status[b]["discovered"]
