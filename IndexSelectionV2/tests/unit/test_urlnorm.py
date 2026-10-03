import duckdb
import pytest

from idxsel.urlnorm import DomainKeyer, canonicalize, md5_number_lower


def test_canonicalize_matches_crawler_spelling():
    assert canonicalize("https://a.com/p?b=2&a=1#frag") == "https://a.com/p?a=1&b=2"
    assert canonicalize("https://a.com") == "https://a.com/"


def test_canonicalize_keeps_unparseable_input():
    assert canonicalize("http://[invalid") == "http://[invalid"


@pytest.mark.parametrize("text", ["abc", "", "https://例子.tw/路徑?q=é", "a" * 3000])
def test_md5_number_lower_matches_duckdb(text):
    (expected,) = duckdb.execute("SELECT md5_number_lower(?)", [text]).fetchone()
    assert md5_number_lower(text) == expected


def test_domain_key_follows_split_whitelist():
    keyer = DomainKeyer(["en.wikipedia.org"], offline=True)
    assert keyer.key("https://en.wikipedia.org/wiki/X") == "en.wikipedia.org"
    assert keyer.key("https://fr.wikipedia.org/wiki/X") == "wikipedia.org"
    assert keyer.key("https://news.yahoo.co.jp/a") == "yahoo.co.jp"
    assert keyer.key("https://www.bbc.co.uk/news") == "bbc.co.uk"
    assert keyer.key("") == "unknown"

