"""URL canonicalization, domain keys and shard routing that match the crawler.

The crawler stores `w3lib.url.canonicalize_url(response.url)` and routes a URL
to a shard with WebCrawler `libs/db/sharding/key.py:compute_shard`: the key is
the full host if it is whitelisted in `shard_split_subdomain`, otherwise the
eTLD+1; a fixed override wins, else `int(md5(key), 16) % num_shards`.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from functools import lru_cache
from urllib.parse import quote, urlsplit, urlunsplit

import tldextract
from w3lib.url import canonicalize_url

# DuckDB twin of `host_of`: group 1 is the host of an absolute URL.
HOST_RE = r"^[A-Za-z][A-Za-z0-9+.-]*://(?:[^@/?#]*@)?([^:/?#]+)"
# RFC 3986 §3.3-3.5 beyond the unreserved set `quote` always keeps: sub-delims, ":" and
# "@" (pchar) and "/", plus "?" in a query or fragment, plus "%" so escapes stay as they are.
PATH_SAFE = "!$&'()*+,;=:@/%"
QUERY_SAFE = PATH_SAFE + "?"


def canonicalize(url: str) -> str:
    """Returns the crawler's spelling of `url`, or `url` itself if it cannot parse."""
    try:
        return canonicalize_url(url)
    except Exception:
        return url


def wire_url(url: str) -> str:
    """Returns `url` as sent to the crawler API: path, query and fragment percent-encoded.

    The API's `validators.url` rejects a raw `[`, `]` or `|` there. Scheme and netloc
    are kept, so an IPv6 literal keeps its brackets, and a URL that is already valid
    comes back unchanged. Non-ASCII characters become UTF-8 escapes, as w3lib's
    `canonicalize_url` spells them anyway, so the API lists the same canonical URL.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    path = quote(parts.path, safe=PATH_SAFE)
    query = quote(parts.query, safe=QUERY_SAFE)
    fragment = quote(parts.fragment, safe=QUERY_SAFE)
    if (path, query, fragment) == (parts.path, parts.query, parts.fragment):
        return url
    return urlunsplit(parts._replace(path=path, query=query, fragment=fragment))


def md5_number_lower(text: str) -> int:
    """Python twin of DuckDB `md5_number_lower`: MD5 bytes 8..15 read little endian."""
    return int.from_bytes(hashlib.md5(text.encode("utf-8")).digest()[8:], "little")


def host_of(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


class DomainKeyer:
    """Maps hosts to the crawler's shard key and to their registered domain."""

    def __init__(self, split_hosts: Iterable[str], offline: bool) -> None:
        self._split = frozenset(h.lower() for h in split_hosts)
        self._extract = (
            tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
            if offline
            else tldextract.TLDExtract()
        )
        self.registered = lru_cache(maxsize=1_000_000)(self._registered)

    def _registered(self, host: str) -> str:
        if not host:
            return "unknown"
        return self._extract(host).registered_domain or host

    def key_for_host(self, host: str) -> str:
        if host and host in self._split:
            return host
        return self.registered(host)

    def key(self, url: str) -> str:
        return self.key_for_host(host_of(url))


class ShardRouter:
    """Mirrors the crawler's `compute_shard`."""

    def __init__(self, keyer: DomainKeyer, num_shards: int, overrides: Mapping[str, int]) -> None:
        self.keyer = keyer
        self._num_shards = num_shards
        self._overrides = dict(overrides)

    def shard_for_key(self, key: str) -> int:
        if key in self._overrides:
            return int(self._overrides[key])
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16) % self._num_shards

    def candidate_shards(self, url: str) -> list[int]:
        """Shards that may hold `url`: its host-key shard and its eTLD+1 shard.

        `golden_inject.py` routes by eTLD+1 and ignores split hosts, and a split
        host whose rows are not migrated yet still lives in the eTLD+1 shard.
        """
        host = host_of(url)
        shards = [self.shard_for_key(self.keyer.key_for_host(host)),
                  self.shard_for_key(self.keyer.registered(host))]
        return sorted(set(shards))
