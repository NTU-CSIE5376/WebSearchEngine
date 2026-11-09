"""Since batch 21 (2026-11-01) SerpApi returns most organic results at rank
4-10 as `https://www.google.com/goto?url=<token>` redirect links. Golden URLs
must hold the page Google redirects to, not the redirect link."""
import pytest

from Metric.Query.GoogleRedirect import is_google_goto, resolve_golden_urls, resolve_google_goto

GOTO = "https://www.google.com/goto?url=CAESeAHrOzAVFZ"
TARGET = "https://web.gekisaka.jp/news/jleague/detail/?443557-443557-fl"


class FakeResponse:
    def __init__(self, status, location=None):
        self.status = status
        self.headers = {"Location": location} if location else {}


def fetcher(responses):
    calls = []

    def fetch(url, timeout):
        calls.append(url)
        result = responses[url]
        if isinstance(result, Exception):
            raise result
        return result

    fetch.calls = calls
    return fetch


@pytest.mark.parametrize(
    "url, expected",
    [
        (GOTO, True),
        ("https://google.com/goto?url=x", True),
        ("https://www.google.co.jp/goto?url=x", True),
        ("https://www.google.com/search?q=goto", False),
        ("https://example.com/goto?url=x", False),
        ("https://notgoogle.com/goto?url=x", False),
        (TARGET, False),
        ("not a url", False),
    ],
)
def test_is_google_goto(url, expected):
    assert is_google_goto(url) is expected


def test_resolve_follows_one_redirect():
    fetch = fetcher({GOTO: FakeResponse(302, TARGET)})
    assert resolve_google_goto(GOTO, fetch=fetch) == (TARGET, "resolved")


@pytest.mark.parametrize(
    "response, reason",
    [
        (FakeResponse(200), "HTTP 200, no redirect"),
        (FakeResponse(302), "HTTP 302 without Location"),
        (FakeResponse(302, "https://www.google.com/sorry/index?continue=x"),
         "HTTP 302 to https://www.google.com/sorry/index?continue=x"),
        (FakeResponse(302, "/relative/path"), "HTTP 302 to /relative/path"),
        (TimeoutError("timed out"), "request failed: TimeoutError: timed out"),
    ],
)
def test_resolve_explains_why_target_unknown(response, reason):
    assert resolve_google_goto(GOTO, fetch=fetcher({GOTO: response})) == (None, reason)


def test_resolve_golden_urls_keeps_order_and_drops_unresolved(capsys):
    bad = "https://www.google.com/goto?url=expired"
    fetch = fetcher({GOTO: FakeResponse(302, TARGET), bad: FakeResponse(200)})
    urls = ["https://a.com/1", GOTO, bad, "https://b.com/2"]
    resolved = resolve_golden_urls(urls, query="j league", fetch=fetch, delay=0)
    assert resolved == ["https://a.com/1", TARGET, "https://b.com/2"]
    # Ordinary links are passed through without a request.
    assert fetch.calls == [GOTO, bad]
    # The dropped link is logged with its query, SerpApi rank and reason.
    assert capsys.readouterr().out.splitlines() == [
        f"[goto] dropped unresolved link: query='j league' rank=3 reason=HTTP 200, no redirect url={bad}"
    ]
