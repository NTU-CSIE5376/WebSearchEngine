"""The SerpApi key is read in one place and checked for free before a batch spends searches.
Until 11-16 a placeholder key reached cron, every request failed, and an empty batch was saved."""
import pytest
import requests
from Metric.Query import SerpApi
from Metric.Query.SerpApi import SerpApiError, account, require_searches, serpapi_key


class FakeResponse:
    def __init__(self, status, body):
        self.status_code = status
        self.body = body

    def json(self):
        if self.body is None:
            raise ValueError("not json")
        return self.body


@pytest.fixture
def serve(monkeypatch):
    """`serve(status, body)` makes the account endpoint answer with that response."""
    seen = []

    def install(status, body):
        def get(url, params, timeout):
            seen.append(params)
            return FakeResponse(status, body)
        monkeypatch.setattr(SerpApi.requests, "get", get)
        return seen
    return install


@pytest.mark.parametrize("value", [None, "", "  "])
def test_a_missing_key_is_an_error(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("SERPAPI_KEY", raising=False)
    else:
        monkeypatch.setenv("SERPAPI_KEY", value)
    with pytest.raises(SerpApiError, match="not set"):
        serpapi_key()


def test_the_key_comes_from_the_env(monkeypatch, serve):
    monkeypatch.setenv("SERPAPI_KEY", "real-key")
    seen = serve(200, {"total_searches_left": 5000})
    assert account()["total_searches_left"] == 5000
    assert seen == [{"api_key": "real-key"}]


def test_an_invalid_key_is_an_error(monkeypatch, serve):
    monkeypatch.setenv("SERPAPI_KEY", "YOUR_ACTUAL_API_KEY_HERE")
    serve(401, {"error": "Invalid API key. Your API key should be here: https://serpapi.com/manage-api-key"})
    with pytest.raises(SerpApiError, match=r"HTTP 401.*Invalid API key"):
        account()


def test_a_200_with_an_error_field_is_an_error(monkeypatch, serve):
    monkeypatch.setenv("SERPAPI_KEY", "k")
    serve(200, {"error": "Your account has run out of searches."})
    with pytest.raises(SerpApiError, match="run out"):
        account()


def test_a_body_that_is_not_json_is_an_error(monkeypatch, serve):
    monkeypatch.setenv("SERPAPI_KEY", "k")
    serve(502, None)
    with pytest.raises(SerpApiError, match="HTTP 502"):
        account()


def test_a_network_error_does_not_print_the_key(monkeypatch):
    monkeypatch.setenv("SERPAPI_KEY", "secret-key")

    def get(url, params, timeout):
        raise requests.ConnectionError(f"failed for {url}?api_key={params['api_key']}")
    monkeypatch.setattr(SerpApi.requests, "get", get)
    with pytest.raises(SerpApiError) as e:
        account()
    assert "secret-key" not in str(e.value)
    assert e.value.__cause__ is None


@pytest.mark.parametrize("left, ok", [(1010, True), (1009, False), (None, False)])
def test_require_searches_needs_enough_left(monkeypatch, serve, left, ok):
    monkeypatch.setenv("SERPAPI_KEY", "k")
    serve(200, {} if left is None else {"total_searches_left": left})
    if ok:
        assert require_searches(1010)["total_searches_left"] == left
    else:
        with pytest.raises(SerpApiError, match="searches left"):
            require_searches(1010)
