"""Since batch 21 (2026-11-01) SerpApi returns most organic results at rank
4-10 as `https://www.google.com/goto?url=<token>` redirect links. Golden URLs
must hold the page Google redirects to, not the redirect link, without
changing the benchmark: the golden set keeps SerpApi's size and ranks."""
import pytest

from Metric.Query.GoogleRedirect import is_google_goto, resolve_golden_urls, resolve_google_goto

GOTO = "https://www.google.com/goto?url=CAESeAHrOzAVFZ"
TARGET = "https://web.gekisaka.jp/news/jleague/detail/?443557-443557-fl"
CAPTCHA = "https://www.google.com/sorry/index?continue=x"


class FakeResponse:
    def __init__(self, status, location=None):
        self.status = status
        self.headers = {"Location": location} if location else {}
        self.closed = False

    def close(self):
        self.closed = True


def fetcher(responses):
    """`responses[url]` is one result, or a list consumed one per request."""
    calls = []

    def fetch(url, timeout):
        calls.append(url)
        result = responses[url]
        if isinstance(result, list):
            result = result.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    fetch.calls = calls
    return fetch


def resolve(url, responses, **kwargs):
    return resolve_google_goto(url, fetch=fetcher(responses), backoff=0, **kwargs)


@pytest.mark.parametrize(
    "url, expected",
    [
        (GOTO, True),
        ("https://google.com/goto?url=x", True),
        ("https://www.google.co.jp/goto?url=x", True),
        ("https://www.google.com.tw/goto?url=x", True),
        ("https://www.google.de/goto?url=x", True),
        ("https://www.google.com/search?q=goto", False),
        ("https://example.com/goto?url=x", False),
        ("https://notgoogle.com/goto?url=x", False),
        ("https://www.google.example.org/goto?url=x", False),
        ("https://google.evil.com/goto?url=x", False),
        (TARGET, False),
        ("not a url", False),
    ],
)
def test_is_google_goto(url, expected):
    assert is_google_goto(url) is expected


def test_resolve_follows_one_redirect():
    response = FakeResponse(302, TARGET)
    assert resolve(GOTO, {GOTO: response}) == (TARGET, "resolved")
    assert response.closed


@pytest.mark.parametrize(
    "target",
    [
        # metric_url 189793 in batch 21: Google's official Android news page.
        "https://blog.google/products-and-platforms/platforms/android/",
        "https://support.google.com/android/answer/2819582",
        "https://www.youtube.com/watch?v=cRxepTa1Q8c",
        "https://www.google.com/maps/place/Taipei",
    ],
)
def test_google_owned_content_is_a_real_target(target):
    assert resolve(GOTO, {GOTO: FakeResponse(302, target)}) == (target, "resolved")


@pytest.mark.parametrize(
    "response, reason",
    [
        (FakeResponse(200), "HTTP 200, no redirect"),
        (FakeResponse(404), "HTTP 404, no redirect"),
        (FakeResponse(302), "HTTP 302 without Location"),
        (FakeResponse(302, "/relative/path"), "HTTP 302 to invalid target /relative/path"),
        (FakeResponse(302, "javascript:alert(1)"), "HTTP 302 to invalid target javascript:alert(1)"),
    ],
)
def test_permanent_failure_is_not_retried(response, reason):
    fetch = fetcher({GOTO: response})
    assert resolve_google_goto(GOTO, fetch=fetch, backoff=0) == (None, reason)
    assert fetch.calls == [GOTO]


@pytest.mark.parametrize(
    "make, reason",
    [
        (lambda: FakeResponse(503), "HTTP 503, no redirect"),
        (lambda: FakeResponse(429), "HTTP 429, no redirect"),
        (lambda: TimeoutError("timed out"), "request failed: TimeoutError: timed out"),
        (lambda: FakeResponse(302, CAPTCHA), f"HTTP 302 to Google interstitial {CAPTCHA}"),
        (lambda: FakeResponse(302, "https://consent.google.com/ml?continue=x"),
         "HTTP 302 to Google interstitial https://consent.google.com/ml?continue=x"),
        (lambda: FakeResponse(302, "https://www.google.com/url?q=https://a.com"),
         "HTTP 302 to Google interstitial https://www.google.com/url?q=https://a.com"),
    ],
)
def test_transient_failure_is_retried(make, reason):
    fetch = fetcher({GOTO: [make() for _ in range(3)]})
    assert resolve_google_goto(GOTO, fetch=fetch, backoff=0) == (None, f"{reason} (after 3 attempts)")
    assert fetch.calls == [GOTO] * 3


def test_transient_failure_then_redirect_resolves():
    fetch = fetcher({GOTO: [FakeResponse(503), FakeResponse(302, CAPTCHA), FakeResponse(302, TARGET)]})
    assert resolve_google_goto(GOTO, fetch=fetch, backoff=0) == (TARGET, "resolved")
    assert len(fetch.calls) == 3


def test_resolve_golden_urls_replaces_goto_links_in_place():
    other = "https://www.google.com/goto?url=other"
    fetch = fetcher({GOTO: FakeResponse(302, TARGET), other: FakeResponse(302, "https://b.com/x")})
    urls = ["https://a.com/1", GOTO, other]
    assert resolve_golden_urls(urls, fetch=fetch, delay=0, backoff=0) == ["https://a.com/1", TARGET, "https://b.com/x"]
    # Ordinary links are passed through without a request.
    assert fetch.calls == [GOTO, other]


def test_transient_failure_preserves_the_benchmark(capsys):
    """[A, goto(B), C] with B answering 503 every time: C must stay at rank 3 and
    the set must keep 3 entries, so coverage is still measured out of 3."""
    a, c = "https://a.com/1", "https://c.com/3"
    fetch = fetcher({GOTO: [FakeResponse(503) for _ in range(3)]})
    resolved = resolve_golden_urls([a, GOTO, c], query="j league", fetch=fetch, delay=0, backoff=0)

    assert resolved == [a, GOTO, c]
    # Head/RandomQueryStrategy assign rank = position in the returned list.
    ranks = {u: i + 1 for i, u in enumerate(resolved)}
    assert ranks[c] == 3 and len(resolved) == 3
    assert capsys.readouterr().out.splitlines() == [
        f"[goto] kept unresolved link: query='j league' rank=2 "
        f"reason=HTTP 503, no redirect (after 3 attempts) url={GOTO}"
    ]


def test_permanent_failure_also_keeps_the_original_link():
    fetch = fetcher({GOTO: FakeResponse(200)})
    urls = ["https://a.com/1", GOTO, "https://c.com/3"]
    assert resolve_golden_urls(urls, fetch=fetch, delay=0, backoff=0) == urls
