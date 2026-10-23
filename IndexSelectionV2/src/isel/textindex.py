"""Full-text index of the recrawl text in OpenSearch: build it, query it, measure it.

The input is the text Parquet that `recrawl-archive` writes (`TEXT_SCHEMA`), and only
the newest row of each doc is indexed. A build never touches the live index: it loads
a new physical index `{alias}-{utc id}`, checks the document count, merges it, blocks
writes, and only then moves the alias to it in one `_aliases` call. Searches see the
old index or the new one, never a half-built one, and a failed build leaves nothing.

`_id` is the doc id in decimal (`md5_number_lower` of the canonical URL, a u64), and ids
are only ever read from it, so they never pass through a float. A hit is matched to a
golden URL by `_id` alone: the list URL defines the doc, as in the indexed rate, so
ranked can never exceed indexed. The same number is also an `unsigned_long` doc-values
field that breaks score ties: Lucene otherwise orders equal scores by load order, so two
builds of the same text could return different top-k sets.
"""

from __future__ import annotations

import json
import logging
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from isel.config import Config, TextIndexCfg
from isel.io import (
    atomic_write_json,
    code_version,
    open_duckdb,
    sql_str,
    utc_id,
    write_parquet_atomic,
)

log = logging.getLogger(__name__)

RETRY_STATUS = (429, 503)
DOC_FIELDS = ("url", "lang", "title", "description", "body")
TEXT_FIELDS = ("title", "description", "body")
# Bulk-load mode: nothing searches the new index before the swap, and the Parquet is
# the source of truth, so a lost translog only costs a rebuild.
BUILD_SETTINGS = {"number_of_replicas": 0, "refresh_interval": "-1",
                  "translog.durability": "async"}
# Back to the defaults, read-only from here on.
SERVE_SETTINGS = {"index": {"refresh_interval": None, "translog.durability": None,
                            "blocks.write": True}}


class TextIndexError(RuntimeError):
    """Raised when OpenSearch refuses a request or a build check fails."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class OpenSearch:
    """A JSON client of one OpenSearch node; `_bulk` resends whole requests on 429/503."""

    def __init__(self, base_url: str, timeout_s: float, retries: int, backoff_s: float,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.url = base_url
        self.timeout_s = timeout_s
        self.retries = retries
        self.backoff_s = backoff_s
        self.sleep = sleep

    def call(self, method: str, path: str, body: Any = None,
             ndjson: bytes | None = None) -> Any:
        """Sends one request; returns the decoded reply. Raises TextIndexError on non-2xx."""
        if ndjson is not None:
            data, ctype = ndjson, "application/x-ndjson"
        else:
            data = None if body is None else json.dumps(body).encode()
            ctype = "application/json"
        request = urllib.request.Request(f"{self.url}{path}", data=data, method=method,
                                         headers={"Content-Type": ctype})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read()[:500].decode("utf-8", "replace")
            raise TextIndexError(f"{method} {path}: HTTP {exc.code} {detail}", exc.code) from exc
        return json.loads(raw) if raw else {}

    def bulk(self, index: str, payload: bytes) -> int:
        """Indexes one NDJSON payload; returns its document count.

        Every action is an `index` with an explicit `_id`, so resending the whole
        payload after a 429 or 503 only rewrites the same documents.
        """
        path = f"/{index}/_bulk?filter_path=errors,items.*.status,items.*.error"
        for attempt in range(self.retries + 1):
            last = attempt == self.retries
            try:
                reply = self.call("POST", path, ndjson=payload)
            except TextIndexError as exc:
                if exc.status not in RETRY_STATUS or last:
                    raise
                self.sleep(self.backoff_s * 2 ** attempt)
                continue
            items = [next(iter(item.values())) for item in reply.get("items", [])]
            if not reply.get("errors"):
                return len(items)
            failed = [i for i in items if i.get("status", 200) >= 300]
            other = next((i for i in failed if i.get("status") != 429), None)
            if other is not None:
                raise TextIndexError(f"bulk into {index}: {other.get('error')}", other["status"])
            if last:
                raise TextIndexError(f"bulk into {index}: still rejected after "
                                     f"{self.retries} resends", 429)
            self.sleep(self.backoff_s * 2 ** attempt)
        raise AssertionError("unreachable")


def bulk_payloads(batches: Iterable[pa.RecordBatch], max_bytes: int) -> Iterator[bytes]:
    """NDJSON `_bulk` bodies of at most `max_bytes`; a larger document goes alone."""
    parts: list[bytes] = []
    size = 0
    for batch in batches:
        for row in batch.to_pylist():
            doc = {"doc_id": row["doc_id"],
                   **{f: row[f] for f in DOC_FIELDS if row[f] is not None}}
            line = (b'{"index":{"_id":"%d"}}\n' % row["doc_id"]
                    + json.dumps(doc, ensure_ascii=False).encode() + b"\n")
            if parts and size + len(line) > max_bytes:
                yield b"".join(parts)
                parts, size = [], 0
            parts.append(line)
            size += len(line)
    if parts:
        yield b"".join(parts)


def latest_docs(con: duckdb.DuckDBPyConnection, text_root: Path,
                batch_rows: int) -> tuple[int, pa.RecordBatchReader]:
    """The newest row of each doc_id under `text_root`, as (rows, batches).

    The first pass picks each doc's winner from (doc_id, fetched_at, file name) alone,
    so the page bodies are never sorted; the second pass reads the winners. A tie on
    `fetched_at` goes to the larger file name. `*.parquet` skips `.parquet.tmp` files.
    """
    if next(text_root.glob("**/*.parquet"), None) is None:
        raise TextIndexError(f"no text Parquet under {text_root}")
    files = (f"read_parquet({sql_str(str(text_root / '**' / '*.parquet'))}, "
             "filename=true, hive_partitioning=false)")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE winner AS
        SELECT doc_id, max({{'t': fetched_at, 'f': filename}}) AS win FROM {files}
        GROUP BY doc_id""")
    (rows,) = con.execute("SELECT count(*) FROM winner").fetchone()
    reader = con.execute(f"""SELECT s.doc_id, {', '.join(f's.{f}' for f in DOC_FIELDS)}
        FROM {files} s JOIN winner w ON s.doc_id = w.doc_id
         AND s.fetched_at = w.win.t AND s.filename = w.win.f""").to_arrow_reader(batch_rows)
    return int(rows), reader


def _text_bytes(batch: pa.RecordBatch) -> int:
    """UTF-8 bytes of the searchable text, the base of the size projection."""
    return sum(pc.sum(pc.binary_length(batch.column(f))).as_py() or 0 for f in TEXT_FIELDS)


def _load(client: OpenSearch, t: TextIndexCfg, index: str,
          batches: Iterable[pa.RecordBatch]) -> tuple[int, int]:
    """Bulk-loads `batches` with at most `bulk_workers` requests in flight."""
    text_bytes = 0

    def counted() -> Iterator[pa.RecordBatch]:
        nonlocal text_bytes
        for batch in batches:
            text_bytes += _text_bytes(batch)
            yield batch

    indexed = 0
    with ThreadPoolExecutor(t.bulk_workers) as pool:
        pending: set[Future[int]] = set()
        for payload in bulk_payloads(counted(), t.bulk_bytes):
            if len(pending) >= t.bulk_workers:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                indexed += sum(f.result() for f in done)
            pending.add(pool.submit(client.bulk, index, payload))
        indexed += sum(f.result() for f in pending)
    return indexed, text_bytes


def _copies(client: OpenSearch, index: str) -> list[dict[str, Any]]:
    stats = client.call("GET", f"/{index}/_stats?level=shards")
    return [c for shard in stats["indices"][index]["shards"].values() for c in shard]


def _wait_for_leases(client: OpenSearch, index: str, timeout_s: float) -> None:
    """Waits up to `timeout_s` until every retention lease is past the last operation.

    Until then the merge keeps a `_recovery_source` copy of each document, the
    excluded body included.
    """
    deadline = time.monotonic() + timeout_s
    while not all(lease["retaining_seq_no"] > c["seq_no"]["max_seq_no"]
                  for c in _copies(client, index) for lease in c["retention_leases"]["leases"]):
        if time.monotonic() > deadline:
            return
        client.sleep(1.0)


def _history_released(client: OpenSearch, index: str) -> bool:
    """True if the last commit retains no operation, so the merge dropped every copy."""
    return all(int(c["commit"]["user_data"]["min_retained_seq_no"]) > c["seq_no"]["max_seq_no"]
               for c in _copies(client, index))


def _force_merge(client: OpenSearch, index: str, segments: int) -> None:
    """Merges as a task and polls it, so a long merge outlives no HTTP timeout.

    The merge only commits the new segments; searches keep the old ones open, and their
    files stay on disk, until a refresh. A shard without searches for 30 s skips the
    scheduled refreshes, so the refresh is explicit.
    """
    reply = client.call("POST", f"/{index}/_forcemerge?max_num_segments={segments}"
                                "&wait_for_completion=false")
    task = client.call("GET", f"/_tasks/{reply['task']}")
    while not task.get("completed"):
        client.sleep(1.0)
        task = client.call("GET", f"/_tasks/{reply['task']}")
    if task.get("error") or task["response"]["_shards"]["failed"]:
        raise TextIndexError(f"force merge of {index} failed: "
                             f"{task.get('error') or task['response']}")
    client.call("POST", f"/{index}/_refresh")


def _physical(alias: str) -> re.Pattern[str]:
    # Only this alias's own builds: "isel-text" must not match "isel-text-v2-...".
    return re.compile(re.escape(alias) + r"-\d{8}t\d{12}z")


def _aliased(client: OpenSearch, alias: str) -> list[str]:
    try:
        return sorted(client.call("GET", f"/_alias/{alias}"))
    except TextIndexError as exc:
        if exc.status == 404:
            return []
        raise


def build(cfg: Config, client: OpenSearch, text_root: Path,
          now: datetime | None = None) -> dict[str, Any]:
    """Builds a new index from `text_root` and makes it the alias's only index.

    Raises:
        TextIndexError: If a request fails or the index does not hold every doc; the
            new index is then deleted and the alias is left as it was.
    """
    t = cfg.text_index
    spec_bytes = Path(t.settings_file).read_bytes()
    spec = json.loads(spec_bytes)
    index = f"{t.alias}-{utc_id(now).lower()}"
    settings = {**spec.get("settings", {}), **BUILD_SETTINGS, "number_of_shards": t.shards}
    started = time.monotonic()
    con = open_duckdb(cfg, cfg.work_dir / "text_index" / "tmp")
    try:
        rows, batches = latest_docs(con, text_root, t.read_batch_rows)
        client.call("PUT", f"/{index}", {**spec, "settings": settings})
        try:
            indexed, text_bytes = _load(client, t, index, batches)
            load_s = time.monotonic() - started
            client.call("POST", f"/{index}/_refresh")
            docs = client.call("GET", f"/{index}/_count")["count"]
            if docs != rows:
                raise TextIndexError(f"{index} holds {docs} docs, expected {rows}")
            client.call("POST", f"/{index}/_flush")
            _wait_for_leases(client, index, t.lease_wait_s)
            # The merge also keeps the copies at or above the last safe commit, and a
            # commit is safe only once the global checkpoint is persisted. The leases
            # passing means it moved, so a new commit is safe.
            client.call("POST", f"/{index}/_flush?force=true")
            _force_merge(client, index, t.force_merge_segments)
            pruned = _history_released(client, index)
            client.call("PUT", f"/{index}/_settings", SERVE_SETTINGS)
            previous = _aliased(client, t.alias)
            client.call("POST", "/_aliases", {"actions": [
                *({"remove": {"index": i, "alias": t.alias}} for i in previous),
                {"add": {"index": index, "alias": t.alias}}]})
        except BaseException:
            try:
                client.call("DELETE", f"/{index}")
            except (TextIndexError, OSError):
                log.exception("could not delete the failed index %s", index)
            raise
    finally:
        con.close()
    keep = {index, *previous} if t.keep_prev else {index}
    names = client.call("GET", f"/_cat/indices/{t.alias}-*?format=json&h=index")
    deleted = sorted(n["index"] for n in names
                     if _physical(t.alias).fullmatch(n["index"]) and n["index"] not in keep)
    for name in deleted:
        client.call("DELETE", f"/{name}")
    stats = client.call("GET", f"/{index}/_stats/store,segments"
                               "?include_segment_file_sizes=true")["indices"][index]["primaries"]
    # The store can still hold the unmerged segments' files for a few seconds after the
    # merge; the live segments are the index itself.
    size = sum(f["size_in_bytes"] for f in stats["segments"]["file_sizes"].values())
    total_s = time.monotonic() - started
    out = {
        "index": index, "alias": t.alias, "text_root": str(text_root), "rows": rows,
        "indexed": indexed, "docs": docs, "text_bytes": text_bytes,
        "store_bytes": stats["store"]["size_in_bytes"], "segment_bytes": size,
        "bytes_per_doc": size / docs if docs else None,
        "bytes_per_text_byte": size / text_bytes if text_bytes else None,
        "segments": stats["segments"]["count"], "recovery_source_pruned": pruned,
        "load_s": round(load_s, 3), "total_s": round(total_s, 3),
        "docs_per_s_load": round(rows / load_s, 1) if load_s else None,
        "previous": previous, "deleted": deleted,
        "settings_sha256": sha256(spec_bytes).hexdigest(), "config_sha256": cfg.sha256(),
        "code_version": code_version(),
    }
    atomic_write_json(cfg.work_dir / "text_index" / "build" / f"{index}.json", out)
    return out


def query_body(t: TextIndexCfg, text: str) -> dict[str, Any]:
    """A search for `text` over `t.fields`; never `query_string`, so no query syntax.

    Equal scores are ordered by doc id, so the top k is the same after every build.
    """
    if t.query_type == "combined_fields":
        query = {"combined_fields": {"query": text, "fields": t.fields}}
    else:
        query = {"multi_match": {"query": text, "fields": t.fields, "type": t.query_type}}
    return {"query": query, "size": t.k, "sort": [{"_score": "desc"}, {"doc_id": "asc"}],
            "_source": False, "track_total_hits": False}


def search(client: OpenSearch, t: TextIndexCfg, text: str) -> tuple[list[str], float]:
    """The ids of the top `t.k` hits and the server's `took` in ms."""
    reply = client.call("POST", f"/{t.alias}/_search", query_body(t, text))
    return [h["_id"] for h in reply["hits"]["hits"]], float(reply["took"])


def present_ids(client: OpenSearch, alias: str, ids: list[str], chunk: int) -> set[str]:
    """The ids among `ids` that the index holds."""
    out: set[str] = set()
    for start in range(0, len(ids), chunk):
        part = ids[start:start + chunk]
        reply = client.call("POST", f"/{alias}/_search", {
            "query": {"ids": {"values": part}}, "size": len(part), "_source": False})
        out.update(h["_id"] for h in reply["hits"]["hits"])
    return out


def evaluate_golden(cfg: Config, client: OpenSearch, batch_id: int,
                    now: datetime | None = None) -> dict[str, Any]:
    """Recall@k of one golden batch: each keyword's top k against its golden URLs."""
    from isel.golden import read_golden
    from isel.rankeval import run_eval
    from isel.urlnorm import md5_number_lower

    t = cfg.text_index
    rows = read_golden(cfg.work_dir).filter(pc.field("batch_id") == batch_id).to_pylist()
    if not rows:
        raise TextIndexError(f"golden has no batch {batch_id}")
    judged: dict[int, set[str]] = {}
    keyword: dict[int, str] = {}
    tags: dict[str, set[int]] = {tag: set() for tag in cfg.golden.tags}
    for r in rows:
        judged.setdefault(r["query_id"], set()).add(str(md5_number_lower(r["canonical_url"])))
        keyword[r["query_id"]] = r["keyword"]
        for tag in r["tags"] or []:
            if tag in tags:
                tags[tag].add(r["query_id"])
    texts = sorted(set(keyword.values()))
    for text in texts[:t.warmup_queries]:
        search(client, t, text)
    replies = {text: search(client, t, text) for text in texts}
    ranked = {q: replies[keyword[q]][0] for q in judged}
    took = {q: replies[keyword[q]][1] for q in judged}
    every = sorted(set().union(*judged.values()))
    present = present_ids(client, t.alias, every, t.lookup_chunk)
    out = {"batch_id": batch_id, "alias": t.alias, "index": _aliased(client, t.alias),
           "k": t.k, "query_type": t.query_type, "fields": t.fields,
           **run_eval(judged, ranked, took, present, tags, t.k, cfg.eval.bootstrap_samples,
                      cfg.eval.bootstrap_seed)}
    run = utc_id(now)
    out_dir = cfg.work_dir / "text_index" / "eval"
    atomic_write_json(out_dir / f"{run}.json", out)
    write_parquet_atomic(pa.Table.from_pylist([
        {"query_id": q, "keyword": keyword[q], "judged": len(judged[q]),
         "hits": len(ranked[q]), "found": len(judged[q] & set(ranked[q])),
         "took_ms": took[q]} for q in sorted(judged)]), out_dir / f"{run}.queries.parquet")
    return out
