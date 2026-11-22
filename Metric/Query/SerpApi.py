"""The one place that reads the SerpApi key and checks it before any search is spent."""
import os

import requests

ACCOUNT_URL = "https://serpapi.com/account.json"


class SerpApiError(RuntimeError):
    pass


def serpapi_key() -> str:
    key = os.environ.get("SERPAPI_KEY", "").strip()
    if not key:
        raise SerpApiError("SERPAPI_KEY is not set")
    return key


def account(key: str | None = None, timeout: float = 30) -> dict:
    """Account and quota info. Free: it uses no search."""
    try:
        resp = requests.get(ACCOUNT_URL, params={"api_key": key or serpapi_key()}, timeout=timeout)
    except requests.RequestException as e:
        # The request URL holds the key, so never put the exception text in the message.
        raise SerpApiError(f"SerpApi account request failed: {type(e).__name__}") from None
    try:
        data = resp.json()
    except ValueError:
        data = {}
    if resp.status_code != 200 or "error" in data:
        raise SerpApiError(f"SerpApi account check failed (HTTP {resp.status_code}): "
                           f"{data.get('error', 'no error message')}")
    return data


def require_searches(n: int) -> dict:
    """Fail unless the key is valid and at least n searches are left."""
    data = account()
    left = data.get("total_searches_left")
    if not isinstance(left, int) or left < n:
        raise SerpApiError(f"SerpApi has {left} searches left, this run needs up to {n}")
    print(f"SerpApi key OK: {left} searches left, this run needs up to {n}")
    return data
