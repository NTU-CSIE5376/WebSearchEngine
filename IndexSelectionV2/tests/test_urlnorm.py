from __future__ import annotations

import hashlib

import pytest

from isel.urlnorm import (
    DomainKeyer,
    ShardRouter,
    canonicalize,
    lookup_urls,
    md5_number_lower,
    strip_srsltid,
    wire_url,
)

# The cases of Metric tests/test_coverage_srsltid.py: the rank job must count the same URLs.
TOKEN = "AfmBOoqfCkB7Gr36L8FzV4ncwpCatQZRYNOjavRk_Qp1ZbjcmArzMmZw"


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
