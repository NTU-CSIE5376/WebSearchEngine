import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"


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
    labels = host.split(".")
    is_google = "google" in labels and labels[0] in ("google", "www")
    return is_google and parts.path == "/goto"


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


def resolve_google_goto(url: str, fetch=_fetch, timeout: float = 10) -> str | None:
    """Return the page a goto link redirects to, or None if it can't be resolved
    (no redirect, a redirect back to Google such as a captcha page, or an error)."""
    try:
        response = fetch(url, timeout)
    except Exception as e:
        print(f"[goto] resolve failed for {url}: {e}")
        return None

    location = response.headers.get("Location") if 300 <= response.status < 400 else None
    if not location:
        return None
    host = (urlsplit(location).hostname or "").lower()
    if not host or "google" in host.split("."):
        return None
    return location


def resolve_golden_urls(urls: list, fetch=_fetch, delay: float = 0.5) -> list:
    """Replace goto links with their targets; drop the ones that can't be resolved."""
    resolved = []
    for url in urls:
        if not is_google_goto(url):
            resolved.append(url)
            continue
        target = resolve_google_goto(url, fetch=fetch)
        if target:
            resolved.append(target)
        else:
            print(f"[goto] dropped unresolved link {url}")
        if delay:
            time.sleep(delay)
    return resolved
