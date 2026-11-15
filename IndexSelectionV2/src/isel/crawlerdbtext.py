"""crawlerdb's title and anchor text for the listed docs the text index has no text for.

A doc that was never fetched, was refused, or got only a bot-challenge page is indexed by
its URL alone (`isel.textindex`). The normal crawler often saw such a page before, and
crawlerdb keeps the title it read and the first anchor text that linked to it. This module
reads both, for exactly the docs a build would index without text, into a small Parquet
dataset that the next build joins.

Every statement is one primary-key read through `ReadOnlySession`: `url = ANY(...)` on one
shard table, paced by `pull.min_interval_s` and held by the read guard. Nothing is written
to crawlerdb. The key is the crawler's canonical URL, which is the list's URL, and a URL
can live in either of its candidate shards (`ShardRouter.candidate_shards`).
"""

from __future__ import annotations

import logging
import os
import shutil
import time
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa

from isel.config import Config
from isel.io import TMP_SUFFIX, open_duckdb, write_parquet_atomic
from isel.pg import ReadOnlySession
from isel.pull import _table_name
from isel.textindex import TEXTLESS, TextIndexError, parquet_scan, pick_docs, resolve_prior
from isel.urlnorm import ShardRouter

log = logging.getLogger(__name__)

# One row per doc that crawlerdb has a title or an anchor text for; the other one may be null.
CRAWLERDB_TEXT_SCHEMA = pa.schema([("doc_id", pa.uint64()), ("title", pa.string()),
                                   ("anchor", pa.string())])


def lookup_sql(cfg: Config, shard: int) -> str:
    """The read of one chunk of URLs from a shard table, by its primary key."""
    return f"SELECT url, title, anchor_text FROM {_table_name(cfg, shard)} WHERE url = ANY(%s)"


def _chunks(docs: Iterable[pa.RecordBatch], router: ShardRouter,
            size: int) -> Iterator[tuple[int, dict[str, int]]]:
    """(shard, doc id by URL) lookups of at most `size` URLs, in the order of `docs`.

    A URL goes to each of its candidate shards. A shard's chunk is sent when it is full,
    so the best ranks are asked first; the partial chunks follow at the end.
    """
    pending: dict[int, dict[str, int]] = defaultdict(dict)
    for batch in docs:
        for doc_id, url in zip(batch.column("doc_id").to_pylist(),
                               batch.column("url").to_pylist(), strict=True):
            for shard in router.candidate_shards(url):
                pending[shard][url] = doc_id
                if len(pending[shard]) == size:
                    yield shard, pending.pop(shard)
    yield from sorted(pending.items())


def _text(value: str | None, chars: int | None = None) -> str | None:
    return (value or "").strip()[:chars] or None


def pull_crawlerdb_text(cfg: Config, session: ReadOnlySession, router: ShardRouter,
                        text_root: Path,
                        clock: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    """Replaces the dataset under `crawlerdb_text_root` with what crawlerdb holds now.

    The docs asked for are the listed docs that `pick_docs` leaves without text, best list
    rank first. After `crawlerdb_text_max_minutes` of lookups no further one starts: the docs not
    asked yet stay out of the dataset, and the report says `stopped`.

    The new files are written beside the dataset and take its place only at the end, so a
    run that fails leaves the previous dataset as it was.

    Raises:
        TextIndexError: If `crawlerdb_text_root` or the list (`prior_file`) is not set.
    """
    t = cfg.text_index
    if not t.crawlerdb_text_root:
        raise TextIndexError("text_index.crawlerdb_text_root is not set")
    prior_file, prior_run = resolve_prior(cfg)
    if not prior_file:
        raise TextIndexError("the crawlerdb lookup needs the list: set text_index.prior_file")
    root = Path(t.crawlerdb_text_root)
    staging = root.with_name(root.name + TMP_SUFFIX)
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    started = clock()
    rows: list[dict[str, Any]] = []
    files = found = titles = anchors = queries = 0
    stopped = False
    # A doc with two candidate shards can be in both; it gets one row.
    seen: set[int] = set()

    def flush() -> None:
        nonlocal files
        write_parquet_atomic(pa.Table.from_pylist(rows, schema=CRAWLERDB_TEXT_SCHEMA),
                             staging / f"part-{files:05d}.parquet")
        files += 1
        rows.clear()

    con = open_duckdb(cfg, cfg.work_dir / "text_index" / "crawlerdb_text_tmp")
    try:
        pick_docs(con, text_root, prior_file, cfg.recrawl.challenge_sha1)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE wanted AS
            SELECT p.doc_id, arg_min(p.url, p.rank) AS url, min(p.rank) AS rank
            FROM {parquet_scan(prior_file)} p SEMI JOIN ({TEXTLESS}) t ON p.doc_id = t.doc_id
            GROUP BY p.doc_id""")
        (wanted,) = con.execute("SELECT count(*) FROM wanted").fetchone()
        docs = con.execute("SELECT doc_id, url FROM wanted ORDER BY rank, doc_id"
                           ).to_arrow_reader(t.read_batch_rows)
        # The limit bounds the time on crawlerdb, not the local pick of the docs before it.
        lookups_started = clock()
        deadline = (lookups_started + 60 * t.crawlerdb_text_max_minutes
                    if t.crawlerdb_text_max_minutes else None)
        for shard, urls in _chunks(docs, router, t.crawlerdb_text_chunk):
            if deadline is not None and clock() >= deadline:
                stopped = True
                break
            queries += 1
            for url, title, anchor in session.fetchall(lookup_sql(cfg, shard), (list(urls),)):
                doc_id = urls[url]
                title, anchor = _text(title), _text(anchor, t.crawlerdb_anchor_chars)
                if doc_id in seen or (title is None and anchor is None):
                    continue
                if len(router.candidate_shards(url)) > 1:
                    seen.add(doc_id)
                rows.append({"doc_id": doc_id, "title": title, "anchor": anchor})
                found += 1
                titles += title is not None
                anchors += anchor is not None
            if len(rows) >= t.crawlerdb_text_file_rows:
                flush()
            if queries % t.progress_every == 0:
                log.info("crawlerdb text: %d queries, %d of %d docs found in %.0f s",
                         queries, found, wanted, clock() - lookups_started)
        lookup_seconds = clock() - lookups_started
    finally:
        con.close()
    if rows:
        flush()
    shutil.rmtree(root, ignore_errors=True)
    os.replace(staging, root)
    return {"crawlerdb_text_root": str(root), "prior_file": prior_file, "prior_run": prior_run,
            "docs_wanted": int(wanted), "docs_found": found, "with_title": titles,
            "with_anchor": anchors, "queries": queries, "files": files, "stopped": stopped,
            "lookup_seconds": round(lookup_seconds, 3), "seconds": round(clock() - started, 3)}
