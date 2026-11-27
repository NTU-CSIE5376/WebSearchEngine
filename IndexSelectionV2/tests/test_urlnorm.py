from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from isel.urlnorm import (
    DomainKeyer,
    ShardRouter,
    canonicalize,
    lookup_urls,
    md5_number_lower,
    strip_srsltid,
    url_words,
    wire_url,
)

# The cases of Metric tests/test_coverage_srsltid.py: the rank job must count the same URLs.
TOKEN = "AfmBOoqfCkB7Gr36L8FzV4ncwpCatQZRYNOjavRk_Qp1ZbjcmArzMmZw"

# A copy of NTU-CSIE5376/WebCrawler tests/fixtures/url_keys.json: the crawler's url_state keys.
URL_KEYS = json.loads((Path(__file__).parent / "fixtures" / "url_keys.json")
                      .read_text(encoding="utf-8"))["vectors"]


def test_shard_router_matches_crawler_rule(base_cfg):
    keyer = DomainKeyer(["news.example.co.uk"], offline=True)
    router = ShardRouter(keyer, 256, base_cfg.routing.overrides)
    expected = int(hashlib.md5(b"example.org").hexdigest(), 16) % 256
    assert router.shard_for_key(keyer.key("https://a.b.example.org/x")) == expected
    assert router.shard_for_key(keyer.key("https://en.wikipedia.org/wiki/X")) == 0
    assert router.shard_for_key(keyer.key("https://www.youtube.com/watch?v=1")) == 64
    split = router.candidate_shards("https://news.example.co.uk/a")
    assert len(split) == 2
    assert router.shard_for_key("example.co.uk") in split


def test_canonicalize_and_hash():
    assert canonicalize("https://x.com/a?b=2&a=1#f") == "https://x.com/a?a=1&b=2"
    assert md5_number_lower("x") == int.from_bytes(hashlib.md5(b"x").digest()[8:], "little")


@pytest.mark.parametrize("vector", URL_KEYS, ids=[v["url"] for v in URL_KEYS])
def test_canonicalize_gives_the_crawler_key(vector):
    assert canonicalize(vector["url"]) == vector["key"]


@pytest.mark.parametrize(("url", "wire"), [
    ("https://www.doshinsports.com/article_list/contents_type[]=4",
     "https://www.doshinsports.com/article_list/contents_type%5B%5D=4"),
    ("https://www.fotmob.com/matches/[seo]/[h2h]",
     "https://www.fotmob.com/matches/%5Bseo%5D/%5Bh2h%5D"),
    ("https://www.gob.mx/agricultura|puebla/articulos/gusano-barrenador-recomendaciones-para"
     "-su-control-379092",
     "https://www.gob.mx/agricultura%7Cpuebla/articulos/gusano-barrenador-recomendaciones-para"
     "-su-control-379092"),
    ("https://www.elsan.care/fr/pathologie-et-traitement/maladies-de-la-peau/maladie-verneuil"
     "|causes|traitements",
     "https://www.elsan.care/fr/pathologie-et-traitement/maladies-de-la-peau/maladie-verneuil"
     "%7Ccauses%7Ctraitements"),
    ("https://x.com/a b/%20%C3%A9%7C?q=%5B1%5D|2#f[1]",
     "https://x.com/a%20b/%20%C3%A9%7C?q=%5B1%5D%7C2#f%5B1%5D"),
    ("https://x.com/caf\u00e9?q=\u00e9", "https://x.com/caf%C3%A9?q=%C3%A9"),
    ("http://[2001:db8::1]/a[b]", "http://[2001:db8::1]/a%5Bb%5D"),
])
def test_wire_url_encodes_what_the_api_rejects(url, wire):
    assert wire_url(url) == wire
    assert wire_url(wire) == wire
    # The API canonicalizes what it gets: the wire form lists as the stored URL would.
    assert canonicalize(wire) == canonicalize(url)


@pytest.mark.parametrize("url", [
    "https://x.com/a/b;p=1?a=1&b=2#frag",
    "https://u:p@x.com:8080/~a/b-c_d.e/!$&'()*+,;=:@?x=/?:@#/?",
    "https://x.com/%E4%B8%AD%2Fa?q=%25",
    "http://[2001:db8::1]:80/a",
    "https://x.com",
])
def test_wire_url_keeps_a_valid_url(url):
    assert wire_url(url) == url


@pytest.mark.parametrize(("url", "stripped"), [
    (f"https://ew.com/story-12174222?srsltid={TOKEN}", "https://ew.com/story-12174222"),
    (f"https://a.com/s?sort=relevance&srsltid={TOKEN}&term=b",
     "https://a.com/s?sort=relevance&term=b"),
    ("https://a.com/?SrsLtId=x", "https://a.com/"),
    ("https://a.com/p?srsltid=x#reviews", "https://a.com/p#reviews"),
    ("https://a.com/p?id=1", "https://a.com/p?id=1"),
    ("https://a.com/p?xsrsltid=1", "https://a.com/p?xsrsltid=1"),
    ("https://a.com/srsltid/page", "https://a.com/srsltid/page"),
])
def test_strip_srsltid(url, stripped):
    assert strip_srsltid(url) == stripped


def test_lookup_urls_add_only_the_srsltid_variant():
    assert lookup_urls("https://a.com/p?id=1") == ["https://a.com/p?id=1"]
    assert lookup_urls("https://a.com/p?utm_source=x") == ["https://a.com/p?utm_source=x"]
    golden = canonicalize(f"https://a.com/p?srsltid={TOKEN}&id=1")
    assert lookup_urls(golden) == [golden, "https://a.com/p?id=1"]


@pytest.mark.parametrize(("url", "words"), [
    ("https://ja.wikipedia.org/wiki/%E6%9D%B1%E4%BA%AC", "https://ja.wikipedia.org/wiki/\u6771\u4eac"),
    ("https://x.com/caf%C3%A9?q=%E4%B8%AD#%E6%96%87", "https://x.com/caf\u00e9?q=\u4e2d#\u6587"),
    ("https://xn--wgv71a119e.jp/a", "https://\u65e5\u672c\u8a9e.jp/a"),
    ("https://www.xn--mnchen-3ya.de:8080/", "https://www.m\u00fcnchen.de:8080/"),
    # Not punycode: the label stays, and the rest is still decoded.
    ("https://xn--!!.example/%C3%A9", "https://xn--!!.example/\u00e9"),
    ("https://xn--99999999999999999999.example/", "https://xn--99999999999999999999.example/"),
    # Not a whole A-label: decoding `xn--abc` alone would lose the word `abc`.
    ("https://xn--abc_def.example/%C3%A9", "https://xn--abc_def.example/\u00e9"),
    ("https://u@XN--wgv71a119e.jp/", "https://u@\u65e5\u672c\u8a9e.jp/"),
    # Punycode for a lone surrogate, which a bulk request could not encode.
    ("https://xn--ib9b.example/", "https://xn--ib9b.example/"),
    # Not UTF-8: one U+FFFD each, which no analyzer keeps as a word.
    ("https://x.com/a%FFb/%E6%9D", "https://x.com/a\ufffdb/\ufffd"),
    ("https://x.com/s?q=new+york%20city%2Bguide", "https://x.com/s?q=new+york city+guide"),
    ("http://[bad/%C3%A9", "http://[bad/%C3%A9"),
])
def test_url_words_decodes_escapes_and_idn_labels(url, words):
    assert url_words(url) == words


@pytest.mark.parametrize("url", [
    "https://news.example.com/olympics/2020-Games_x?a=1&b=2#top",
    "https://u:p@x.com:8080/~a/b-c_d.e/!$&'()*+,;=:@",
    "https://www.espn.com/nfl/story/_/id/123/bills-vs-rams-highlights-week-5",
    "http://[2001:db8::1]:80/a",
    "https://x.com",
])
def test_url_words_keeps_an_ascii_url(url):
    assert url_words(url) == url
