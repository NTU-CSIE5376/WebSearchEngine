import re
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"

# Google Search hosts: google.com, www.google.com, google.co.jp, www.google.com.tw, ...
_GOOGLE_SEARCH_HOST = re.compile(r"(?:www\.)?google\.(?:com|[a-z]{2}|co\.[a-z]{2}|com\.[a-z]{2})")

# Pages Google sends a client to instead of the result: rate-limit captcha,
# redirect notice, another goto hop, consent and sign-in walls. Other Google-owned
# pages (blog.google, support.google.com, youtube.com, ...) are real results.
_INTERSTITIAL_PATHS = ("/sorry", "/url", "/goto", "/interstitial")
_INTERSTITIAL_HOST = re.compile(r"(?:consent|accounts)\.google\.[a-z.]+")

# Worth asking again: the next attempt may get the redirect.
_TRANSIENT_STATUS = {429, 500, 502, 503, 504}


def is_google_goto(url: str) -> bool:
    """True for Google's `https://www.google.<tld>/goto?url=<token>` redirect links.

    Since 2026-11 SerpApi returns most organic results at rank 4-10 in this
    form; the token is opaque, so the target is only known by requesting it.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return False
    host = (parts.hostname or "").lower()
    return bool(_GOOGLE_SEARCH_HOST.fullmatch(host)) and parts.path == "/goto"


def _interstitial(location: str) -> bool:
    parts = urlsplit(location)
    host = (parts.hostname or "").lower()
    if _INTERSTITIAL_HOST.fullmatch(host):
        return True
    return bool(_GOOGLE_SEARCH_HOST.fullmatch(host)) and parts.path.startswith(_INTERSTITIAL_PATHS)


def _valid_target(location: str) -> bool:
    try:
        parts = urlsplit(location)
        return parts.scheme in ("http", "https") and bool(parts.hostname)
    except ValueError:
        return False


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


_opener = urllib.request.build_opener(_NoRedirect)


def _fetch(url: str, timeout: float):
    """GET `url` without following redirects; a 3xx comes back as the response."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        return _opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as e:
        return e


def _request_once(url: str, fetch, timeout: float) -> tuple[str | None, str, bool]:
    """One request: (target, reason, transient)."""
    try:
        response = fetch(url, timeout)
    except Exception as e:
        return None, f"request failed: {type(e).__name__}: {e}", True
    try:
        status = response.status
        location = response.headers.get("Location")
    finally:
        close = getattr(response, "close", None)
        if close:
            close()

    if not 300 <= status < 400:
        return None, f"HTTP {status}, no redirect", status in _TRANSIENT_STATUS
    if not location:
        return None, f"HTTP {status} without Location", False
    if not _valid_target(location):
        return None, f"HTTP {status} to invalid target {location}", False
    if _interstitial(location):
        # /sorry/ is Google's rate-limit captcha; it usually clears on retry.
        return None, f"HTTP {status} to Google interstitial {location}", True
    return location, "resolved", False


def resolve_google_goto(url: str, fetch=_fetch, timeout: float = 10,
                        attempts: int = 3, backoff: float = 2) -> tuple[str | None, str]:
    """Return (target, reason): the page a goto link redirects to, or None and why
    it can't be resolved. Transient failures (request errors, 429/5xx, captcha or
    other interstitial) are retried up to `attempts` times."""
    for attempt in range(attempts):
        target, reason, transient = _request_once(url, fetch, timeout)
        if target or not transient:
            break
        if attempt < attempts - 1 and backoff:
            time.sleep(backoff * (2 ** attempt))
    if not target and attempts > 1 and transient:
        reason = f"{reason} (after {attempts} attempts)"
    return target, reason


def resolve_golden_urls(urls: list, query: str = "", fetch=_fetch, delay: float = 0.5,
                        attempts: int = 3, backoff: float = 2) -> list:
    """Replace goto links with their targets, one for one.

    The list keeps its length and order, so ranks and the golden-set size stay
    what SerpApi returned. A link that can't be resolved keeps its original goto
    URL at its rank and is logged with the query, rank and reason.
    """
    resolved = []
    for rank, url in enumerate(urls, start=1):
        if not is_google_goto(url):
            resolved.append(url)
            continue
        target, reason = resolve_google_goto(url, fetch=fetch, attempts=attempts, backoff=backoff)
        if target:
            resolved.append(target)
        else:
            resolved.append(url)
            print(f"[goto] kept unresolved link: query={query!r} rank={rank} reason={reason} url={url}")
        if delay:
            time.sleep(delay)
    return resolved
