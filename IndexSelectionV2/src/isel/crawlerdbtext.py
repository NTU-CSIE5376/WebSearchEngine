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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from isel.config import Config
from isel.io import TMP_SUFFIX, file_lock, open_duckdb, write_parquet_atomic
from isel.pg import ReadOnlySession, next_allowed_start
from isel.pull import _table_name
from isel.textindex import (
    TEXTLESS,
    TextIndexError,
    build_lock_path,
    crawlerdb_text_old,
    parquet_scan,
    pick_docs,
    resolve_prior,
)
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


def _window_wait(cfg: Config, session: ReadOnlySession, now: Callable[[], datetime]) -> float:
    """Seconds the read guard would hold the next lookup for its read window; 0 unguarded."""
    if session.guard is None:
        return 0.0
    current = now()
    return (next_allowed_start(current, session.timeout_s or 0, cfg.guard)
            - current).total_seconds()


def _text(value: str | None, chars: int | None = None) -> str | None:
    return (value or "").strip()[:chars] or None


def _replace_dataset(staging: Path, root: Path) -> None:
    """Puts `staging` in the place of `root` by two renames, then deletes the old dataset.

    Between the renames only `<root>.old` exists; a build then reads that one
    (`textindex.crawlerdb_text_dir`), and the next pull moves it back first.
    """
    old = crawlerdb_text_old(root)
    if root.exists():
        os.replace(root, old)
    os.replace(staging, root)
    shutil.rmtree(old, ignore_errors=True)


def pull_crawlerdb_text(cfg: Config, session: ReadOnlySession, router: ShardRouter,
                        text_root: Path,
                        clock: Callable[[], float] = time.monotonic,
                        now: Callable[[], datetime] = lambda: datetime.now(UTC),
                        ) -> dict[str, Any]:
    """Replaces the dataset under `crawlerdb_text_root` with what crawlerdb holds now.

    The docs asked for are the listed docs that `pick_docs` leaves without text, best list
    rank first. After `crawlerdb_text_max_minutes` of lookups no further one starts: the docs not
    asked yet stay out of the dataset, and the report says `stopped`.

    With a limit, a lookup that the read guard would hold past it is not sent either: the
    guard can wait out a closed read window for hours, all the while holding the work lock.

    The new files are written beside the dataset and take its place only at the end, so a
    run that fails, or stops before its first lookup, leaves the previous dataset as it
    was. The run holds the text index build lock, so a build never reads a dataset that is
    being replaced, and two runs never share the staging folder.

    Raises:
        TextIndexError: If `crawlerdb_text_root` or the list (`prior_file`) is not set, or
            the read windows leave no lookup within the limit.
        LockBusy: If a build or another run holds the build lock.
    """
    t = cfg.text_index
    if not t.crawlerdb_text_root:
        raise TextIndexError("text_index.crawlerdb_text_root is not set")
    prior_file, prior_run = resolve_prior(cfg)
    if not prior_file:
        raise TextIndexError("the crawlerdb lookup needs the list: set text_index.prior_file")
    root = Path(t.crawlerdb_text_root)
    with file_lock(build_lock_path(cfg)):
        return _pull(cfg, session, router, text_root, prior_file, prior_run, root, clock, now)


def _pull(cfg: Config, session: ReadOnlySession, router: ShardRouter, text_root: Path,
          prior_file: str, prior_run: str | None, root: Path, clock: Callable[[], float],
          now: Callable[[], datetime]) -> dict[str, Any]:
    t = cfg.text_index
    old = crawlerdb_text_old(root)
    if old.exists():
        # A run died between the two renames of `_replace_dataset`.
        if root.exists():
            shutil.rmtree(old)
        else:
            os.replace(old, root)
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
            if deadline is not None and clock() + _window_wait(cfg, session, now) >= deadline:
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
    if stopped and not queries:
        shutil.rmtree(staging, ignore_errors=True)
        raise TextIndexError("no crawlerdb lookup fits crawlerdb_text_max_minutes before the "
                             "read windows close; the dataset is left as it was")
    if rows:
        flush()
    _replace_dataset(staging, root)
    return {"crawlerdb_text_root": str(root), "prior_file": prior_file, "prior_run": prior_run,
            "docs_wanted": int(wanted), "docs_found": found, "with_title": titles,
            "with_anchor": anchors, "queries": queries, "files": files, "stopped": stopped,
            "lookup_seconds": round(lookup_seconds, 3), "seconds": round(clock() - started, 3)}
