"""URL canonicalization and domain keys that match the crawler's own rules.

The crawler stores `w3lib.url.canonicalize_url(response.url)`
(WebCrawler `containers/crawler/crawler/spiders/spider.py:361`) and shards by
`libs/db/sharding/key.py`: the full host if it is whitelisted in
`shard_split_subdomain`, otherwise the eTLD+1.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from functools import lru_cache
from urllib.parse import urlsplit

import tldextract
from w3lib.url import canonicalize_url


def canonicalize(url: str) -> str:
    """Returns the crawler's spelling of `url`, or `url` itself if it cannot parse."""
    try:
        return canonicalize_url(url)
    except Exception:
        return url


def md5_number_lower(text: str) -> int:
    """Python twin of DuckDB `md5_number_lower`: MD5 bytes 8..15 read little endian."""
    return int.from_bytes(hashlib.md5(text.encode("utf-8")).digest()[8:], "little")


class DomainKeyer:
    """Maps a URL to the crawler's shard key."""

    def __init__(self, split_hosts: Iterable[str], offline: bool) -> None:
        self._split = frozenset(h.lower() for h in split_hosts)
        self._extract = (
            tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)
            if offline
            else tldextract.TLDExtract()
        )
        self.key_for_host = lru_cache(maxsize=1_000_000)(self._key_for_host)

    def _key_for_host(self, host: str) -> str:
        if not host:
            return "unknown"
        if host in self._split:
            return host
        return self._extract(host).registered_domain or host

    def key(self, url: str) -> str:
        try:
            host = (urlsplit(url).hostname or "").lower()
        except ValueError:
            host = ""
        return self.key_for_host(host)
