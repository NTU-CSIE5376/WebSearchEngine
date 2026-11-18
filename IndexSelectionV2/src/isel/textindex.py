"""Full-text index of the recrawl text in OpenSearch: build it, query it, measure it.

The input is the text Parquet that `recrawl-archive` writes (`TEXT_SCHEMA`), and only
the newest row of each doc is indexed. A build never touches the live index: it loads
a new physical index `{alias}-{utc id}`, checks the document count, merges it, blocks
writes, and only then moves the alias to it in one `_aliases` call. Searches see the
old index or the new one, never a half-built one, and a failed build leaves nothing.

`_id` is the doc id in decimal (`md5_number_lower` of the canonical URL, a u64), and ids
are only ever read from it, so they never pass through a float. A hit is matched to a
golden URL by `_id` alone: the list URL defines the doc, as in the indexed rate. With a
prior, only the docs on that list are indexed (`docs/RECRAWL.md` §13): a doc that left
the list leaves the index at the next build, and one that comes back on a later list
returns with the text fetched before. `prior_file = "live"` takes the manifest of the
live selection run, so a build after a swap uses the new list. The same number is also
an `unsigned_long` doc-values field that breaks score ties: Lucene otherwise orders equal
scores by load order, so two builds of the same text could return different top-k sets.

The node shares its disk with the crawler. The flood stage blocks writes but not a merge
that is already running, so a build checks the free space itself before it loads and
before it merges.
"""

from __future__ import annotations

import http.client
import json
import logging
import re
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import datetime
from hashlib import sha256
from math import ceil
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from isel.config import Config, TextIndexCfg
from isel.io import (
    atomic_write_json,
    code_version,
    file_lock,
    open_duckdb,
    sql_str,
    utc_id,
    write_parquet_atomic,
)

log = logging.getLogger(__name__)

RETRY_STATUS = (429, 503)
# A lost connection has no status. The request may have run, so only a request that can
# run twice is sent again.
RESEND_STATUS = (None, *RETRY_STATUS)
DOC_FIELDS = ("url", "lang", "title", "description", "body")
TEXT_FIELDS = ("title", "description", "body")
# The selection model's list rank as a static score, the `rank_feature` of the mapping.
PRIOR_FIELD = "sel_prior"
LIVE_PRIOR = "live"                    # prior_file value: the live run's manifest
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
    """A JSON client of one OpenSearch node; requests that can run twice are resent."""

    def __init__(self, base_url: str, timeout_s: float, retries: int, backoff_s: float,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.url = base_url
        self.timeout_s = timeout_s
        self.retries = retries
        self.backoff_s = backoff_s
        self.sleep = sleep

    def call(self, method: str, path: str, body: Any = None, ndjson: bytes | None = None,
             resend: bool = False) -> Any:
        """Sends one request; returns the decoded reply. Raises TextIndexError on non-2xx.

        With `resend`, a 429, a 503 or a lost connection sends the request again after a
        backoff: only for requests that do no harm when they run twice.
        """
        for attempt in range(self.retries + 1 if resend else 1):
            try:
                return self._send(method, path, body, ndjson)
            except TextIndexError as exc:
                if not resend or exc.status not in RESEND_STATUS or attempt == self.retries:
                    raise
                self.sleep(self.backoff_s * 2 ** attempt)
        raise AssertionError("unreachable")

    def _send(self, method: str, path: str, body: Any, ndjson: bytes | None) -> Any:
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
        except (OSError, http.client.HTTPException) as exc:
            # Refused, reset, timed out or cut short: the node may have run it.
            raise TextIndexError(f"{method} {path}: {exc!r}") from exc
        return json.loads(raw) if raw else {}

    def bulk(self, index: str, payload: bytes) -> int:
        """Indexes one NDJSON payload; returns its document count.

        Every action is an `index` with an explicit `_id`, so resending the whole
        payload after a 429, a 503 or a lost reply only rewrites the same documents.
        """
        path = f"/{index}/_bulk?filter_path=errors,items.*.status,items.*.error"
        for attempt in range(self.retries + 1):
            last = attempt == self.retries
            try:
                reply = self.call("POST", path, ndjson=payload)
            except TextIndexError as exc:
                if exc.status not in RESEND_STATUS or last:
                    raise
                self.sleep(self.backoff_s * 2 ** attempt)
                continue
            items = [next(iter(item.values())) for item in reply.get("items", [])]
            if not reply.get("errors"):
                return len(items)
            failed = [i for i in items if i.get("status", 200) >= 300]
            other = next((i for i in failed if i.get("status") not in RETRY_STATUS), None)
            if other is not None:
                raise TextIndexError(f"bulk into {index}: {other.get('error')}", other["status"])
            if last:
                raise TextIndexError(f"bulk into {index}: still rejected after "
                                     f"{self.retries} resends", failed[0]["status"])
            self.sleep(self.backoff_s * 2 ** attempt)
        raise AssertionError("unreachable")


def bulk_payloads(batches: Iterable[pa.RecordBatch], max_bytes: int,
                  prior_scale: float) -> Iterator[bytes]:
    """NDJSON `_bulk` bodies of at most `max_bytes`; a larger document goes alone.

    A row's `prior_rank` becomes `prior_scale / (rank + 1)`: a `rank_feature` must be
    positive and larger must mean better. A row without a rank gets no such field.
    """
    parts: list[bytes] = []
    size = 0
    for batch in batches:
        for row in batch.to_pylist():
            doc = {"doc_id": row["doc_id"],
                   **{f: row[f] for f in DOC_FIELDS if row[f] is not None}}
            rank = row.get("prior_rank")
            if rank is not None:
                doc[PRIOR_FIELD] = prior_scale / (rank + 1)
            line = (b'{"index":{"_id":"%d"}}\n' % row["doc_id"]
                    + json.dumps(doc, ensure_ascii=False).encode() + b"\n")
            if parts and size + len(line) > max_bytes:
                yield b"".join(parts)
                parts, size = [], 0
            parts.append(line)
            size += len(line)
    if parts:
        yield b"".join(parts)


def latest_docs(con: duckdb.DuckDBPyConnection, text_root: Path, batch_rows: int,
                prior_file: str = "") -> tuple[int, int, pa.RecordBatchReader]:
    """The newest row of each doc_id under `text_root`, as (rows, docs off the list, batches).

    The first pass picks each doc's winner without the page bodies, so they are never
    sorted; the second pass reads the winners. A row with text beats any row without it,
    such as an empty WAF page, which older archives stored; a doc with no text still
    gets a document, as its URL's words can find it. A tie on `fetched_at` goes to the
    larger file name. `*.parquet` skips `.parquet.tmp` files.

    `prior_file` is a Parquet glob of the list's `doc_id` and `rank`. With it, only docs on
    the list are kept; the docs off it are counted. A doc can be on the list twice, under
    two URLs with one canonical form, so it takes its best rank and the join adds no rows.
    """
    if next(text_root.glob("**/*.parquet"), None) is None:
        raise TextIndexError(f"no text Parquet under {text_root}")
    files = (f"read_parquet({sql_str(str(text_root / '**' / '*.parquet'))}, "
             "filename=true, hive_partitioning=false)")
    winners = f"""SELECT doc_id, max({{'text': coalesce(text_len, 0) > 0 OR title IS NOT NULL
                                               OR description IS NOT NULL,
                                       't': fetched_at, 'f': filename}}) AS win FROM {files}
                  GROUP BY doc_id"""
    if prior_file:
        ranks = (f"SELECT doc_id, min(rank) AS prior_rank FROM read_parquet("
                 f"{sql_str(prior_file)}, hive_partitioning=false) GROUP BY doc_id")
        source = f"""SELECT w.doc_id, w.win, r.prior_rank FROM ({winners}) w
                     LEFT JOIN ({ranks}) r ON w.doc_id = r.doc_id"""
    else:
        source = f"SELECT doc_id, win, NULL::BIGINT AS prior_rank FROM ({winners})"
    con.execute(f"CREATE OR REPLACE TEMP TABLE winner AS {source}")
    rows, ranked = con.execute("SELECT count(*), count(prior_rank) FROM winner").fetchone()
    if prior_file:
        con.execute("DELETE FROM winner WHERE prior_rank IS NULL")
    reader = con.execute(f"""SELECT s.doc_id, {', '.join(f's.{f}' for f in DOC_FIELDS)},
               w.prior_rank
        FROM {files} s JOIN winner w ON s.doc_id = w.doc_id
         AND s.fetched_at = w.win.t AND s.filename = w.win.f""").to_arrow_reader(batch_rows)
    return (int(ranked), int(rows - ranked), reader) if prior_file else (int(rows), 0, reader)


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

    indexed = sent = 0
    started = time.monotonic()
    with ThreadPoolExecutor(t.bulk_workers) as pool:
        pending: set[Future[int]] = set()
        for payload in bulk_payloads(counted(), t.bulk_bytes, t.prior_scale):
            if len(pending) >= t.bulk_workers:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                indexed += sum(f.result() for f in done)
            pending.add(pool.submit(client.bulk, index, payload))
            sent += 1
            if sent % t.progress_every == 0:
                log.info("%s: %d docs indexed in %.0f s", index, indexed,
                         time.monotonic() - started)
        indexed += sum(f.result() for f in pending)
    return indexed, text_bytes


def _copies(client: OpenSearch, index: str) -> list[dict[str, Any]]:
    stats = client.call("GET", f"/{index}/_stats?level=shards", resend=True)
    return [c for shard in stats["indices"][index]["shards"].values() for c in shard]


def _wait_for_leases(client: OpenSearch, index: str, timeout_s: float) -> bool:
    """True once every retention lease is past the last operation; False after `timeout_s`.

    Until then the merge keeps a `_recovery_source` copy of each document, the
    excluded body included.
    """
    deadline = time.monotonic() + timeout_s
    while not all(lease["retaining_seq_no"] > c["seq_no"]["max_seq_no"]
                  for c in _copies(client, index) for lease in c["retention_leases"]["leases"]):
        if time.monotonic() > deadline:
            return False
        client.sleep(1.0)
    return True


def _history_released(client: OpenSearch, index: str) -> bool:
    """True if the last commit retains no operation, so the merge dropped every copy."""
    return all(int(c["commit"]["user_data"]["min_retained_seq_no"]) > c["seq_no"]["max_seq_no"]
               for c in _copies(client, index))


def _force_merge(client: OpenSearch, index: str, segments: int) -> None:
    """Merges as a task and polls it, so a long merge outlives no HTTP timeout.

    The merge only commits the new segments; searches keep the old ones open, and their
    files stay on disk, until a refresh. A shard without searches for 30 s skips the
    scheduled refreshes, so every poll refreshes: a merged shard frees its old files
    while the next one merges, instead of all shards at the end.
    """
    reply = client.call("POST", f"/{index}/_forcemerge?max_num_segments={segments}"
                                "&wait_for_completion=false")
    while True:
        task = client.call("GET", f"/_tasks/{reply['task']}", resend=True)
        client.call("POST", f"/{index}/_refresh", resend=True)
        if task.get("completed"):
            break
        client.sleep(1.0)
    if task.get("error") or task["response"]["_shards"]["failed"]:
        raise TextIndexError(f"force merge of {index} failed: "
                             f"{task.get('error') or task['response']}")


def _ensure_room(client: OpenSearch, t: TextIndexCfg, need: int, what: str) -> None:
    """Raises unless the node can write `need` more bytes and stay out of the flood stage.

    One node only. The flood stage blocks writes, but a merge that is running goes on and
    can fill the disk the node shares with the crawler.
    """
    reply = client.call("GET", "/_nodes/stats/fs?filter_path=nodes.*.fs.total", resend=True)
    fs = next(iter(reply["nodes"].values()))["fs"]["total"]
    floor = t.flood_free_ratio * fs["total_in_bytes"]
    if fs["available_in_bytes"] - need < floor:
        raise TextIndexError(f"{what} needs {need} bytes, but {fs['available_in_bytes']} "
                             f"are free and the flood stage keeps {floor:.0f}")


def _check_fields(properties: dict[str, Any], t: TextIndexCfg) -> None:
    """Raises unless every query field is a text field of the mapping.

    `multi_match` skips a field the index lacks, so a misspelt name would only lower
    recall; `combined_fields` also needs one analyzer for all its fields.
    """
    analyzers = {}
    for spec in t.fields:
        name = spec.partition("^")[0]
        top, _, sub = name.partition(".")
        field = properties.get(top, {})
        if sub:
            field = field.get("fields", {}).get(sub, {})
        if field.get("type") != "text":
            raise TextIndexError(f"text_index.fields: {name} is not a text field of the index")
        analyzers[name] = field.get("analyzer", "default")
    if t.query_type == "combined_fields" and len(set(analyzers.values())) > 1:
        raise TextIndexError(f"combined_fields needs one analyzer, got {analyzers}")
    # A `rescore_query` on a field the index lacks matches nothing, so the prior would
    # quietly stop counting on an older index.
    if t.prior_boost > 0 and properties.get(PRIOR_FIELD, {}).get("type") != "rank_feature":
        raise TextIndexError(f"{PRIOR_FIELD} is not a rank_feature field of the index")


def _physical(alias: str) -> re.Pattern[str]:
    # Only this alias's own builds: "isel-text" must not match "isel-text-v2-...".
    return re.compile(re.escape(alias) + r"-\d{8}t\d{12}z")


def _builds(client: OpenSearch, alias: str) -> list[str]:
    """This alias's physical indices, oldest first: the names sort by their UTC id."""
    names = client.call("GET", f"/_cat/indices/{alias}-*?format=json&h=index", resend=True)
    return sorted(n["index"] for n in names if _physical(alias).fullmatch(n["index"]))


def _aliased(client: OpenSearch, alias: str) -> list[str]:
    try:
        return sorted(client.call("GET", f"/_alias/{alias}", resend=True))
    except TextIndexError as exc:
        if exc.status == 404:
            return []
        raise


def _drop(client: OpenSearch, index: str) -> None:
    try:
        client.call("DELETE", f"/{index}")
    except TextIndexError:
        log.exception("could not delete the failed index %s", index)


def _swap(client: OpenSearch, alias: str, index: str, previous: list[str]) -> None:
    """Moves `alias` from `previous` to `index` in one call.

    If the call fails, the alias shows what happened: if it names `index`, only the
    reply was lost; if not, the new index is dropped and the alias stays as it was.
    """
    try:
        client.call("POST", "/_aliases", {"actions": [
            *({"remove": {"index": i, "alias": alias}} for i in previous),
            {"add": {"index": index, "alias": alias}}]})
    except TextIndexError:
        if index in _aliased(client, alias):
            log.warning("%s moved to %s; only the reply was lost", alias, index)
            return
        _drop(client, index)
        raise


def resolve_prior(cfg: Config) -> tuple[str, str | None]:
    """The prior's Parquet glob and its run label; `"live"` reads the live run's manifest.

    Raises:
        TextIndexError: If selectdb has no single live run.
        ManifestError: If that run's files fail their checksums.
    """
    prior = cfg.text_index.prior_file
    if prior != LIVE_PRIOR:
        return prior, None
    from isel.manifest import validate_run
    from isel.selectdb import connect

    conn = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=True)
    try:
        rows = conn.execute("SELECT run_label FROM public.model_runs WHERE status = 'live'"
                            ).fetchall()
    finally:
        conn.close()
    if len(rows) != 1:
        raise TextIndexError(f"expected one live run, found {len(rows)}")
    ((label,),) = rows
    run_dir = cfg.work_dir / "full" / "selection" / f"run={label}"
    validate_run(run_dir)
    return str(run_dir / "manifest" / "*.parquet"), label


def build(cfg: Config, client: OpenSearch, text_root: Path,
          now: datetime | None = None) -> dict[str, Any]:
    """Builds a new index from `text_root` and makes it the alias's only index.

    Raises:
        LockBusy: If another build holds `text_index/build.lock`: a second build would
            delete the first one's index.
        TextIndexError: If a request fails, the disk lacks room, or the index does not
            hold every doc; the new index is then deleted and the alias is left as it was.
    """
    t = cfg.text_index
    spec_bytes = Path(t.settings_file).read_bytes()
    spec = json.loads(spec_bytes)
    _check_fields(spec["mappings"]["properties"], t)
    index = f"{t.alias}-{utc_id(now).lower()}"
    settings = {**spec.get("settings", {}), **BUILD_SETTINGS, "number_of_shards": t.shards}
    built = cfg.work_dir / "text_index" / "build"
    started = time.monotonic()
    prior_file, prior_run = resolve_prior(cfg)
    with file_lock(cfg.work_dir / "text_index" / "build.lock"):
        con = open_duckdb(cfg, cfg.work_dir / "text_index" / "tmp")
        try:
            rows, off_list, batches = latest_docs(con, text_root, t.read_batch_rows,
                                                  prior_file)
            if prior_file and not rows:
                raise TextIndexError(f"no doc of {text_root} is on the list of {prior_file}")
            # A build killed before it could delete its index left it unaliased and
            # without a manifest; it would add to this build's disk peak.
            aliased = set(_aliased(client, t.alias))
            strays = [n for n in _builds(client, t.alias)
                      if n < index and n not in aliased and not (built / f"{n}.json").exists()]
            for name in strays:
                client.call("DELETE", f"/{name}")
            _ensure_room(client, t, 2 * rows * t.expected_bytes_per_doc, f"loading {rows} docs")
            client.call("PUT", f"/{index}", {**spec, "settings": settings})
            try:
                loading = time.monotonic()
                indexed, text_bytes = _load(client, t, index, batches)
                load_s = time.monotonic() - loading
                client.call("POST", f"/{index}/_refresh", resend=True)
                docs = client.call("GET", f"/{index}/_count", resend=True)["count"]
                if docs != rows:
                    raise TextIndexError(f"{index} holds {docs} docs, expected {rows}")
                client.call("POST", f"/{index}/_flush", resend=True)
                if not _wait_for_leases(client, index, t.lease_wait_s):
                    raise TextIndexError(f"the retention leases of {index} did not pass its "
                                         f"last operation in {t.lease_wait_s} s; a merge now "
                                         "would keep every document's recovery source")
                store = client.call("GET", f"/{index}/_stats/store", resend=True)
                _ensure_room(client, t,
                             store["indices"][index]["primaries"]["store"]["size_in_bytes"],
                             f"merging {index}")
                # The merge also keeps the copies at or above the last safe commit, and a
                # commit is safe only once the global checkpoint is persisted. The leases
                # passing means it moved, so a new commit is safe.
                client.call("POST", f"/{index}/_flush?force=true", resend=True)
                _force_merge(client, index, t.force_merge_segments)
                pruned = _history_released(client, index)
                client.call("PUT", f"/{index}/_settings", SERVE_SETTINGS)
                previous = _aliased(client, t.alias)
            except BaseException:
                _drop(client, index)
                raise
            _swap(client, t.alias, index, previous)
        finally:
            con.close()
        # Only older builds: a newer one may still be loading.
        keep = {index, *previous} if t.keep_prev else {index}
        deleted = [n for n in _builds(client, t.alias) if n < index and n not in keep]
        for name in deleted:
            client.call("DELETE", f"/{name}")
        stats = client.call("GET", f"/{index}/_stats/store,segments"
                                   "?include_segment_file_sizes=true",
                            resend=True)["indices"][index]["primaries"]
        # The store can still hold the unmerged segments' files for a few seconds after
        # the merge; the live segments are the index itself.
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
            "previous": previous, "deleted": deleted, "strays": strays,
            "prior_file": prior_file, "prior_run": prior_run,
            "prior_docs": rows if prior_file else 0, "docs_off_list": off_list,
            "settings_sha256": sha256(spec_bytes).hexdigest(), "config_sha256": cfg.sha256(),
            "code_version": code_version(),
        }
        atomic_write_json(built / f"{index}.json", out)
    return out


def query_body(t: TextIndexCfg, text: str) -> dict[str, Any]:
    """A search for `text` over `t.fields`; never `query_string`, so no query syntax.

    Equal scores are ordered by doc id, so the top k is the same after every build. The
    prior is a `rescore` of the best `t.prior_depth` text matches: over every match, it
    lets a popular page with a weak match replace a good match. `rescore` rules out the
    doc id sort, so `search` breaks those ties. `collapse` keeps one hit per host, while
    Google's top 10 often has two, so `search` also applies the host cap.

    With `prior_score_mode` "total" the prior adds `prior_boost × ln(scaling + S)` points;
    with "multiply" it scales the text score by `1 + prior_boost × ln(scaling + S)`, so its
    effect follows each query's BM25 scale. The constant 1 keeps a doc without a prior at
    its text score.
    """
    if t.query_type == "combined_fields":
        query = {"combined_fields": {"query": text, "fields": t.fields}}
    else:
        query = {"multi_match": {"query": text, "fields": t.fields, "type": t.query_type}}
    body: dict[str, Any] = {"query": query, "size": t.fetch_k if t.host_cap else t.k,
                            "_source": ["url"] if t.host_cap else False,
                            "track_total_hits": False}
    if t.prior_boost > 0:
        prior = {"rank_feature": {"field": PRIOR_FIELD, "boost": t.prior_boost,
                                  "log": {"scaling_factor": t.prior_scaling_factor}}}
        if t.prior_score_mode == "multiply":
            one = {"constant_score": {"filter": {"match_all": {}}, "boost": 1.0}}
            rescore = {"score_mode": "multiply", "query_weight": 1.0, "rescore_query_weight": 1.0,
                       "rescore_query": {"bool": {"should": [prior, one]}}}
        else:
            rescore = {"rescore_query": prior}
        body["rescore"] = {"window_size": ceil(t.prior_depth / t.shards),  # per shard
                           "query": rescore}
    else:
        body["sort"] = [{"_score": "desc"}, {"doc_id": "asc"}]
    return body


# A shard that fails a search fails it, instead of quietly returning fewer hits.
WHOLE = "allow_partial_search_results=false"


def cap_hosts(hits: list[dict[str, Any]], cap: int, k: int) -> list[str]:
    """The ids of the best `k` hits with at most `cap` per host; `www.` is the same host."""
    seen: Counter[str] = Counter()
    ids: list[str] = []
    for hit in hits:
        host = (urlsplit(hit["_source"]["url"]).hostname or "").removeprefix("www.")
        if seen[host] < cap:
            seen[host] += 1
            ids.append(hit["_id"])
            if len(ids) == k:
                break
    return ids


def search(client: OpenSearch, t: TextIndexCfg, text: str) -> tuple[list[str], float]:
    """The ids of the top `t.k` hits and the server's `took` in ms."""
    reply = client.call("POST", f"/{t.alias}/_search?{WHOLE}", query_body(t, text),
                        resend=True)
    hits = reply["hits"]["hits"]
    if t.prior_boost > 0:
        hits.sort(key=lambda h: (-h["_score"], int(h["_id"])))
    ids = cap_hosts(hits, t.host_cap, t.k) if t.host_cap else [h["_id"] for h in hits]
    return ids, float(reply["took"])


def present_ids(client: OpenSearch, alias: str, ids: list[str], chunk: int) -> set[str]:
    """The ids among `ids` that the index holds."""
    out: set[str] = set()
    for start in range(0, len(ids), chunk):
        part = ids[start:start + chunk]
        reply = client.call("POST", f"/{alias}/_search?{WHOLE}", {
            "query": {"ids": {"values": part}}, "size": len(part), "_source": False},
            resend=True)
        out.update(h["_id"] for h in reply["hits"]["hits"])
    return out


def evaluate_golden(cfg: Config, client: OpenSearch, batch_id: int,
                    now: datetime | None = None) -> dict[str, Any]:
    """Recall@k of one golden batch: each keyword's top k against its golden URLs."""
    from isel.golden import read_golden
    from isel.rankeval import run_eval
    from isel.urlnorm import md5_number_lower

    t = cfg.text_index
    mapping = client.call("GET", f"/{t.alias}/_mapping", resend=True)
    _check_fields(next(iter(mapping.values()))["mappings"]["properties"], t)
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
           "prior_score_mode": t.prior_score_mode, "prior_boost": t.prior_boost,
           "prior_depth": t.prior_depth, "host_cap": t.host_cap,
           **run_eval(judged, ranked, took, present, tags, t.k, cfg.eval.bootstrap_samples,
                      cfg.eval.bootstrap_seed)}
    run = utc_id(now)
    out_dir = cfg.work_dir / "text_index" / "eval"
    atomic_write_json(out_dir / f"{run}.json", out)
    write_parquet_atomic(pa.Table.from_pylist([
        {"query_id": q, "keyword": keyword[q], "judged": len(judged[q]),
         "present": len(judged[q] & present), "hits": len(ranked[q]),
         "found": len(judged[q] & set(ranked[q])), "took_ms": took[q], "ranked": ranked[q]}
        for q in sorted(judged)]), out_dir / f"{run}.queries.parquet")
    return out
