"""Recrawl frontier: hands the live selection to the crawler and stores the text it fetched.

The frontier lives in selectdb's `recrawl` schema (`isel.schema`). The crawler
side, including the API it serves to us, is specified in
the crawler proposal linked from `docs/RECRAWL.md`.

- `sync` (after each swap): loads the live run's manifest. A doc is on the
  list while `rank` is set; sync writes only `rank` and `model_run_id`.
- `plan` (loop): sends one batch per hostname to the crawler API
  (`POST recrawl-batches`), at most one queued per host. A batch is marked
  `dispatched` before it is sent and unmarked if the API refuses it, so no
  HTTP call runs inside a transaction. The crawler's host gate enforces the
  0.2 qps rule; the planner only feeds it. Each batch also carries
  `recrawl_share`, the share of the host's requests the crawler gives the
  recrawl lane when the normal lane wants the same host (`pace_share`).
- `archive` (loop): reads closed fetch logs and their WARCs without changing
  them (the crawler prunes them). Worker processes extract each page's text
  into Parquet and keep a gzipped copy of the log; the main process then
  applies the results idempotently.

Every frontier transaction takes one advisory lock, so plan, archive and sync
never interleave their updates and cannot deadlock.
"""

from __future__ import annotations

import contextlib
import gzip
import json
import math
import multiprocessing
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import psycopg
import pyarrow as pa

from isel.config import Config, RecrawlCfg
from isel.extract import extract, read_response
from isel.io import TMP_SUFFIX, commit_file, open_duckdb, sql_str, write_parquet_atomic
from isel.manifest import validate_run
from isel.pg import require_env
from isel.publish import swap_allowed
from isel.schema import DUE_STATES
from isel.urlnorm import md5_number_lower

LOCK_CYCLE = 0x15E1_DB02      # xact lock held by every frontier transaction
LOCK_INSTANCE = {"sync": 0x15E1_DB03, "plan": 0x15E1_DB04, "archive": 0x15E1_DB05}
LOG_NAME_RE = re.compile(r"^recrawl-(\d{2})-(\d{8})T\d{6}Z-\d+\.jsonl$")
HTML_TYPES = ("text/html", "application/xhtml+xml")
# The site said no; asked again after `robots_retry_days`, not retried with backoff.
ROBOTS_REFUSALS = ("robots_disallow", "robots_crawl_delay")
FETCH_LOG_KEYS = frozenset({
    "batch_id", "doc_id", "source_url", "final_url", "http_status", "content_type", "etag",
    "last_modified", "dispatched_at", "fetched_at", "fail_reason", "retry_after_s",
    "warc_file", "warc_offset", "warc_length", "payload_sha1"})
RESULT_COLUMNS = ("batch_id", "doc_id", "outcome", "http_status", "content_type", "final_url",
                  "fail_reason", "payload_sha1", "etag", "last_modified", "fetched_at",
                  "retry_after_s", "text_len", "lang")
# One row per fetched page, the input of the inverted index (docs/RECRAWL.md).
# A doc may appear in several files; the row with the latest `fetched_at` wins. `doc_id`
# is unsigned, as in the manifest.
TEXT_SCHEMA = pa.schema([
    ("doc_id", pa.uint64()), ("batch_id", pa.int64()), ("url", pa.string()),
    ("final_url", pa.string()), ("fetched_at", pa.timestamp("us", tz="UTC")),
    ("http_status", pa.int16()), ("lang", pa.string()), ("title", pa.string()),
    ("description", pa.string()), ("body", pa.string()), ("text_len", pa.int32()),
    ("payload_sha1", pa.string())])
_DUE = "state IN ({})".format(", ".join(f"'{s}'" for s in DUE_STATES))
_INT63 = (1 << 63) - 1
_UINT64 = 1 << 64


class RecrawlError(RuntimeError):
    """Raised when the frontier or a fetch log is inconsistent."""


def host_id(host: str) -> int:
    """Stable queue id of a hostname: the low 63 bits of `md5_number_lower`."""
    return md5_number_lower(host) & _INT63


def pg_doc_id(url: str) -> int:
    """`doc_id` of a canonical URL as selectdb stores it (two's-complement BIGINT)."""
    value = md5_number_lower(url)
    return value - (1 << 64) if value > _INT63 else value


def process_of(cfg: Config, crawl_shard: int) -> int:
    """The crawler process that owns `crawl_shard`: the `crawler_id` of its batches."""
    return crawl_shard // (cfg.crawlerdb.num_shards // cfg.recrawl.crawler_processes)


def pace_count_cap(cfg: Config) -> int:
    """Backlog at which `pace_share` reaches `share_max` for any `since` in the past."""
    r = cfg.recrawl
    return math.ceil(r.share_max * max(r.pace_days * 86400, r.plan_interval_s) / r.pace_cycle_s)


def pace_share(cfg: Config, backlog: int, since: datetime, now: datetime) -> float:
    """Recrawl share of a contended host that ends its backlog `pace_days` after `since`.

    `since` is the oldest due_at of the host's due docs. Past that deadline the
    share is `share_max`, so a host that fell behind catches up.
    """
    r = cfg.recrawl
    left = (since + timedelta(days=r.pace_days) - now).total_seconds()
    share = backlog * r.pace_cycle_s / max(left, r.plan_interval_s)
    return min(max(share, r.share_min), r.share_max)


def classify(rec: dict[str, Any]) -> tuple[str, str | None]:
    """Maps one fetch-log record to (outcome, fail_reason)."""
    status, reason = rec["http_status"], rec["fail_reason"]
    if reason is None and status is not None and 200 <= status < 300:
        ctype = (rec["content_type"] or "").split(";")[0].strip().lower()
        if ctype not in HTML_TYPES:
            return "gone", "non_html"
        if not rec["warc_file"] or rec["warc_offset"] is None or rec["warc_length"] is None:
            return "retry", "no_warc_record"
        return "fetched", None
    if status in (404, 410):
        return "gone", reason or f"http_{status}"
    if reason in ROBOTS_REFUSALS:
        return "robots", reason
    if status in (429, 503) or reason == "host_backoff":
        return "throttled", reason or f"http_{status}"
    return "retry", reason or f"http_{status}"


def _instance_lock(conn: psycopg.Connection, name: str) -> None:
    (ok,) = conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_INSTANCE[name],)).fetchone()
    if not ok:
        raise RecrawlError(f"another recrawl-{name} holds the instance lock")


def _cycle_lock(cur: psycopg.Cursor) -> None:
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_CYCLE,))


def _crawler_dir(root: str | Path, process: int) -> Path:
    return Path(root) / f"crawler_{process:02d}"


def _refresh_best_rank(cur: psycopg.Cursor, hosts: Iterable[int]) -> None:
    ids = sorted(set(hosts))
    if ids:
        cur.execute(
            "UPDATE recrawl.host h SET best_rank = (SELECT min(d.rank) FROM recrawl.doc d "
            f"WHERE d.host_id = h.host_id AND d.rank IS NOT NULL AND d.{_DUE}) "
            "WHERE h.host_id = ANY(%s::bigint[])", (ids,))


def run_loop(cycle: Callable[[], dict[str, Any]], interval_s: float, once: bool,
             sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Runs `cycle` every `interval_s` seconds; prints each result as one JSON line."""
    while True:
        result = cycle()
        if once:
            return result
        print(json.dumps(result, sort_keys=True, default=str), flush=True)
        sleep(interval_s)


# ---------------------------------------------------------------- sync

def sync(cfg: Config, conn: psycopg.Connection,
         now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> dict[str, Any]:
    """Makes the frontier match the live run: new docs, new ranks, docs off the list."""
    at = now()
    if not swap_allowed(at, cfg.schedule):
        raise RecrawlError("swap freeze window: the metric may be reading selectdb")
    _instance_lock(conn, "sync")
    run_id, label = _live_run(conn)
    run_dir = cfg.work_dir / "full" / "selection" / f"run={label}"
    meta = validate_run(run_dir)
    if meta.get("scope") != "full":
        raise RecrawlError(f"{run_dir} is not a select-full run")
    tmp = cfg.work_dir / "duckdb_tmp"
    con = open_duckdb(cfg, tmp)
    started = time.monotonic()
    try:
        docs = _stage_docs(con, run_dir)
        csv_path = tmp / "recrawl_sync.csv"
        for lo in range(0, docs, cfg.recrawl.sync_chunk_rows):
            con.execute(
                "COPY (SELECT doc_id, url, host_id, host, crawl_shard, rank FROM sync_rows "
                f"WHERE i > {lo} AND i <= {lo + cfg.recrawl.sync_chunk_rows}) "
                f"TO {sql_str(str(csv_path))} (FORMAT csv, HEADER false)")
            try:
                _load_chunk(cfg, conn, csv_path, run_id, at)
            finally:
                csv_path.unlink(missing_ok=True)
    finally:
        con.close()
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("UPDATE recrawl.doc SET rank = NULL WHERE rank IS NOT NULL "
                    "AND model_run_id IS DISTINCT FROM %s", (run_id,))
        dropped = cur.rowcount
        cur.execute(
            "UPDATE recrawl.host h SET best_rank = m.r FROM (SELECT host_id, min(rank) AS r "
            f"FROM recrawl.doc WHERE rank IS NOT NULL AND {_DUE} GROUP BY host_id) m "
            "WHERE h.host_id = m.host_id AND h.best_rank IS DISTINCT FROM m.r")
        cur.execute(
            "UPDATE recrawl.host h SET best_rank = NULL WHERE best_rank IS NOT NULL AND "
            "NOT EXISTS (SELECT 1 FROM recrawl.doc d WHERE d.host_id = h.host_id "
            f"AND d.rank IS NOT NULL AND d.{_DUE})")
    conn.execute("ANALYZE recrawl.host")
    conn.execute("ANALYZE recrawl.doc")
    (hosts,) = conn.execute("SELECT count(*) FROM recrawl.host WHERE best_rank IS NOT NULL"
                            ).fetchone()
    return {"model_run_id": run_id, "run_label": label, "docs": docs, "dropped": dropped,
            "hosts_due": hosts, "secs": round(time.monotonic() - started, 1)}


def _live_run(conn: psycopg.Connection) -> tuple[int, str]:
    rows = conn.execute("SELECT id, run_label FROM public.model_runs WHERE status = 'live'"
                        ).fetchall()
    if len(rows) != 1:
        raise RecrawlError(f"expected one live run, found {len(rows)}")
    return rows[0]


def _stage_docs(con: Any, run_dir: Path) -> int:
    """One row per doc_id (its best rank), routed by the shard of its host's best doc."""
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE sync_rows AS
WITH best AS (
  SELECT * FROM read_parquet({sql_str(str(run_dir / 'manifest' / '*.parquet'))})
  QUALIFY row_number() OVER (PARTITION BY doc_id ORDER BY rank, url) = 1
), shard AS (
  SELECT host, arg_min(crawl_shard, rank) AS crawl_shard FROM best GROUP BY host
)
SELECT row_number() OVER (ORDER BY b.rank) AS i,
       CASE WHEN b.doc_id > {_INT63}
            THEN (b.doc_id - {_INT63 + 1})::BIGINT - {_INT63} - 1
            ELSE b.doc_id::BIGINT END AS doc_id,
       b.canonical_url AS url, (md5_number_lower(b.host) & {_INT63})::BIGINT AS host_id,
       b.host, s.crawl_shard, b.rank
FROM best b JOIN shard s USING (host)""")
    (docs,) = con.execute("SELECT count(*) FROM sync_rows").fetchone()
    return int(docs)


def _load_chunk(cfg: Config, conn: psycopg.Connection, csv_path: Path, run_id: int,
                at: datetime) -> None:
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("CREATE TEMP TABLE s (doc_id BIGINT, url TEXT, host_id BIGINT, host TEXT, "
                    "crawl_shard SMALLINT, rank BIGINT) ON COMMIT DROP")
        with cur.copy("COPY s FROM STDIN (FORMAT csv)") as copy, csv_path.open("rb") as fh:
            while block := fh.read(cfg.selectdb.copy_block_bytes):
                copy.write(block)
        cur.execute(
            "INSERT INTO recrawl.host (host_id, host, crawl_shard) "
            "SELECT DISTINCT ON (host_id) host_id, host, crawl_shard FROM s ORDER BY host_id "
            "ON CONFLICT (host_id) DO UPDATE SET crawl_shard = EXCLUDED.crawl_shard "
            "WHERE recrawl.host.crawl_shard <> EXCLUDED.crawl_shard")
        cur.execute("SELECT count(*) FROM s JOIN recrawl.host h USING (host_id) "
                    "WHERE h.host <> s.host")
        (clashes,) = cur.fetchone()
        if clashes:
            raise RecrawlError(f"{clashes} docs hit a host_id taken by another hostname")
        cur.execute(
            # A new doc is due from the sync; pace_share counts its deadline from here.
            "WITH up AS (INSERT INTO recrawl.doc (doc_id, url, host_id, rank, model_run_id, "
            "due_at) SELECT doc_id, url, host_id, rank, %s, %s FROM s ON CONFLICT (doc_id) "
            "DO UPDATE SET rank = EXCLUDED.rank, model_run_id = EXCLUDED.model_run_id "
            "WHERE (recrawl.doc.rank, recrawl.doc.model_run_id) "
            "IS DISTINCT FROM (EXCLUDED.rank, EXCLUDED.model_run_id) RETURNING host_id) "
            + _WAKE.format("(SELECT DISTINCT host_id FROM up)"),
            (run_id, at, at + timedelta(seconds=cfg.recrawl.host_pause_max_s)))


# ---------------------------------------------------------------- plan

class ApiUnavailable(RecrawlError):
    """Raised when the crawler API cannot say which batches are queued."""


@dataclass(frozen=True)
class Reply:
    """Outcome of one `POST recrawl-batches`; `status` is None when it is unknown."""

    status: int | None
    rejected: tuple[int, ...] = ()


class CrawlerApi:
    """Client of the crawler's `recrawl-batches` routes (proposal C10).

    `paused_until` is set when the API refused a whole cycle; the planner claims
    nothing before then.
    """

    def __init__(self, base_url: str, token: str, timeout_s: float) -> None:
        self.url = f"{base_url}/v1/recrawl-batches"
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        self.timeout_s = timeout_s
        self.paused_until: datetime | None = None

    def queued(self) -> set[int]:
        """Host ids with a batch still waiting in any crawler queue directory."""
        request = urllib.request.Request(self.url, headers=self.headers)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as resp:
                return {int(h) for h in json.load(resp)["queued"]}
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ApiUnavailable(f"GET {self.url}: {exc}") from exc

    def submit(self, batch: dict[str, Any]) -> Reply:
        request = urllib.request.Request(self.url, data=json.dumps(batch).encode(),
                                         headers=self.headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as resp:
                return Reply(resp.status)
        except urllib.error.HTTPError as exc:
            rejected: tuple[int, ...] = ()
            if exc.code == 400:
                with contextlib.suppress(ValueError, KeyError, TypeError):
                    rejected = tuple(int(r["doc_id"]) for r in json.load(exc)["rejected"])
            return Reply(exc.code, rejected)
        except OSError:
            return Reply(None)


def run_planner(cfg: Config, conn: psycopg.Connection, once: bool) -> dict[str, Any]:
    _instance_lock(conn, "plan")
    r = cfg.recrawl
    api = CrawlerApi(r.api_url, require_env(r.api_token_env), r.api_timeout_s)
    return run_loop(lambda: plan_cycle(cfg, conn, datetime.now(UTC), api),
                    r.plan_interval_s, once)


def plan_cycle(cfg: Config, conn: psycopg.Connection, now: datetime,
               api: CrawlerApi) -> dict[str, Any]:
    """One planning pass; returns counts.

    Batches are claimed and committed first, then sent, then released if the API
    refused them. A send with an unknown outcome stays claimed: the next pass
    either sees it queued or counts it as taken, and `dispatch_timeout_s`
    recovers it if it never ran.
    """
    r = cfg.recrawl
    stats: dict[str, Any] = {"queued": 0, "taken": 0, "timed_out": 0, "swept": 0,
                             "batches": 0, "docs": 0, "refused": 0, "rejected": 0,
                             "unknown": 0, "paused": False, "disk_full": False,
                             "result_full": False}
    try:
        queued = api.queued()
    except ApiUnavailable as exc:
        return {**stats, "api_error": str(exc)}
    stats["queued"] = len(queued)
    batches: list[dict[str, Any]] = []
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        stats["taken"] = _note_taken(cur, queued, now)
        cur.execute("UPDATE recrawl.doc SET state = 'pending' WHERE state = 'dispatched' "
                    "AND dispatched_at < %s RETURNING host_id",
                    (now - timedelta(seconds=r.dispatch_timeout_s),))
        timed_out = [h for (h,) in cur.fetchall()]
        cur.execute(
            "UPDATE recrawl.doc SET state = 'pending' WHERE doc_id IN (SELECT doc_id "
            "FROM recrawl.doc WHERE state = 'fetched' AND rank IS NOT NULL AND due_at <= %s "
            "LIMIT %s) RETURNING host_id", (now, r.sweep_rows))
        swept = [h for (h,) in cur.fetchall()]
        stats["timed_out"], stats["swept"] = len(timed_out), len(swept)
        _wake(cur, timed_out + swept, now, r.host_pause_max_s)
        _refresh_best_rank(cur, timed_out + swept)
        text_root = Path(r.text_root)
        text_root.mkdir(parents=True, exist_ok=True)
        if api.paused_until is not None and now < api.paused_until:
            stats["paused"] = True
        elif shutil.disk_usage(text_root).free < r.min_free_gb * 1e9:
            stats["disk_full"] = True
        elif _result_bytes(cfg) > r.result_cap_gb * 1e9:
            stats["result_full"] = True
        else:
            batches = _claim(cfg, cur, queued, now)
    if batches:
        _send(conn, api, batches, stats, now + timedelta(seconds=r.api_backoff_s))
    return stats


def _result_bytes(cfg: Config) -> int:
    """Bytes the crawler holds in `result_root`: WARCs and logs not yet pruned."""
    total = 0
    for p in range(cfg.recrawl.crawler_processes):
        d = _crawler_dir(cfg.recrawl.result_root, p)
        if d.is_dir():
            with os.scandir(d) as entries:
                total += sum(e.stat().st_size for e in entries if e.is_file())
    return total


def _note_taken(cur: psycopg.Cursor, queued: set[int], now: datetime) -> int:
    """Hosts whose batch left the queue: the crawler took it; the dispatch timeout starts."""
    cur.execute("SELECT host_id, batch_id FROM recrawl.host WHERE batch_id IS NOT NULL")
    taken = [(h, b) for h, b in cur.fetchall() if h not in queued]
    if taken:
        hosts, batches = [h for h, _ in taken], [b for _, b in taken]
        cur.execute("UPDATE recrawl.doc SET dispatched_at = %s WHERE state = 'dispatched' "
                    "AND batch_id = ANY(%s::bigint[])", (now, batches))
        cur.execute("UPDATE recrawl.host SET batch_id = NULL WHERE host_id = ANY(%s::bigint[])",
                    (hosts,))
    return len(taken)


# A throttle pause ends within host_pause_max_s, so a later next_at was set by _claim for
# docs in backoff; it must not hold back docs that are due now.
_WAKE = ("UPDATE recrawl.host h SET next_at = NULL FROM {} AS w(host_id) "
         "WHERE h.host_id = w.host_id AND h.next_at > %s")


def _wake(cur: psycopg.Cursor, hosts: Iterable[int], now: datetime, pause_max_s: int) -> None:
    """Clears the backoff `next_at` of hosts that have a doc due now."""
    ids = sorted(set(hosts))
    if ids:
        cur.execute(_WAKE.format("unnest(%s::bigint[])"),
                    (ids, now + timedelta(seconds=pause_max_s)))


def _claim(cfg: Config, cur: psycopg.Cursor, queued: set[int],
           now: datetime) -> list[dict[str, Any]]:
    """Marks the best-ranked ready hosts' due docs `dispatched`; returns their batches.

    Each crawler process gets at most its free queue slots, so a full process
    neither draws refusals every cycle nor holds back the others.
    """
    r = cfg.recrawl
    per = cfg.crawlerdb.num_shards // r.crawler_processes
    cur.execute("SELECT crawl_shard / %s, count(*) FROM recrawl.host "
                "WHERE host_id = ANY(%s::bigint[]) GROUP BY 1", (per, sorted(queued)))
    busy = dict(cur.fetchall())
    rooms = {p: r.queue_files_per_dir - busy.get(p, 0) for p in range(r.crawler_processes)}
    rooms = {p: n for p, n in rooms.items() if n > 0}
    if not rooms:
        return []
    cur.execute(
        "SELECT x.host_id, x.crawl_shard FROM unnest(%s::int[], %s::int[]) AS q(p, room) "
        "CROSS JOIN LATERAL (SELECT host_id, crawl_shard, best_rank FROM recrawl.host "
        "WHERE crawl_shard >= q.p * %s AND crawl_shard < (q.p + 1) * %s "
        "AND batch_id IS NULL AND best_rank IS NOT NULL AND (next_at IS NULL OR next_at <= %s) "
        "AND NOT host_id = ANY(%s::bigint[]) ORDER BY best_rank LIMIT least(q.room, %s)) x "
        "ORDER BY x.best_rank LIMIT %s",
        (list(rooms), list(rooms.values()), per, per, now, sorted(queued),
         r.plan_max_batches, r.plan_max_batches))
    shards = dict(cur.fetchall())
    if not shards:
        return []
    cur.execute(
        "SELECT h.host_id, d.doc_id, d.url FROM unnest(%s::bigint[]) AS h(host_id) "
        "CROSS JOIN LATERAL (SELECT doc_id, url, rank FROM recrawl.doc "
        f"WHERE host_id = h.host_id AND rank IS NOT NULL AND {_DUE} AND due_at <= %s "
        "ORDER BY rank LIMIT %s) d ORDER BY h.host_id, d.rank",
        (list(shards), now, r.batch_size))
    docs: dict[int, list[tuple[int, str]]] = {}
    for h, doc_id, url in cur.fetchall():
        docs.setdefault(h, []).append((doc_id, url))
    idle = [h for h in shards if h not in docs]
    if idle:
        # Only docs in backoff are left: wait for the earliest, or drop out of the ready set.
        cur.execute(
            "UPDATE recrawl.host h SET next_at = m.n, best_rank = CASE WHEN m.n IS NULL "
            "THEN NULL ELSE h.best_rank END FROM (SELECT u.host_id, (SELECT min(d.due_at) "
            "FROM recrawl.doc d WHERE d.host_id = u.host_id AND d.rank IS NOT NULL "
            f"AND d.{_DUE}) AS n FROM unnest(%s::bigint[]) AS u(host_id)) m "
            "WHERE h.host_id = m.host_id", (idle,))
    if not docs:
        return []
    # A capped count already gives share_max, whatever the min(due_at) of the sample.
    cur.execute(
        "SELECT h.host_id, p.n, p.since FROM unnest(%s::bigint[]) AS h(host_id) "
        "CROSS JOIN LATERAL (SELECT count(*) AS n, min(due_at) AS since FROM (SELECT due_at "
        f"FROM recrawl.doc WHERE host_id = h.host_id AND rank IS NOT NULL AND {_DUE} "
        "AND due_at <= %s LIMIT %s) d) p", (list(docs), now, pace_count_cap(cfg)))
    shares = {h: round(pace_share(cfg, n, since, now), 3) for h, n, since in cur.fetchall()}
    cur.execute("SELECT nextval('recrawl.batch_seq') FROM generate_series(1, %s)", (len(docs),))
    ids = [b for (b,) in cur.fetchall()]
    batches, doc_ids, doc_batches = [], [], []
    for (h, rows), batch in zip(docs.items(), ids, strict=True):
        batches.append({"batch_id": batch, "crawler_id": process_of(cfg, shards[h]),
                        "host_id": h, "recrawl_share": shares[h],
                        "docs": [{"doc_id": d, "url": u} for d, u in rows]})
        doc_ids += [d for d, _ in rows]
        doc_batches += [batch] * len(rows)
    cur.execute(
        "UPDATE recrawl.doc d SET state = 'dispatched', batch_id = u.batch_id, "
        "dispatched_at = NULL FROM unnest(%s::bigint[], %s::bigint[]) AS u(doc_id, batch_id) "
        "WHERE d.doc_id = u.doc_id", (doc_ids, doc_batches))
    cur.execute(
        "UPDATE recrawl.host h SET batch_id = u.batch_id FROM unnest(%s::bigint[], "
        "%s::bigint[]) AS u(host_id, batch_id) WHERE h.host_id = u.host_id",
        ([b["host_id"] for b in batches], [b["batch_id"] for b in batches]))
    _refresh_best_rank(cur, list(docs))
    return batches


def _send(conn: psycopg.Connection, api: CrawlerApi, batches: list[dict[str, Any]],
          stats: dict[str, Any], pause_until: datetime) -> None:
    """Sends claimed batches; releases the ones the API refused or that were never sent.

    - 2xx: queued.
    - 409: this host is still queued; 429: this process is full, so its other
      batches are not sent; 400 with `rejected`: those docs are unfetchable.
    - Any other 4xx, or 503: the API refuses everything. Sending stops and the
      planner pauses until `pause_until`.
    - 5xx, timeout or no connection: the batch may be queued, so it stays
      claimed (the dispatch timeout recovers it), and sending stops.
    """
    refused: list[int] = []
    rejected: list[int] = []
    full: set[int] = set()
    stop = False
    for batch in batches:
        if stop or batch["crawler_id"] in full:
            refused.append(batch["batch_id"])
            continue
        reply = api.submit(batch)
        status = reply.status
        if status is not None and 200 <= status < 300:
            stats["batches"] += 1
            stats["docs"] += len(batch["docs"])
        elif status is None or status < 400 or (status >= 500 and status != 503):
            stats["unknown"] += 1
            stop = True
        else:
            refused.append(batch["batch_id"])
            if status == 429:
                full.add(batch["crawler_id"])
            elif status == 400 and reply.rejected:
                rejected += reply.rejected
            elif status != 409:
                stats["api_error"] = f"POST {api.url} answered {status}"
                api.paused_until = pause_until
                stop = True
    stats["refused"], stats["rejected"] = len(refused), len(rejected)
    if refused:
        _release(conn, refused, rejected)


def _release(conn: psycopg.Connection, batches: list[int], rejected: list[int]) -> None:
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        # The crawler API judged these URLs unfetchable; asking again would repeat the answer.
        cur.execute("UPDATE recrawl.doc SET state = 'gone', fail_reason = 'invalid_url' "
                    "WHERE state = 'dispatched' AND batch_id = ANY(%s::bigint[]) "
                    "AND doc_id = ANY(%s::bigint[])", (batches, rejected))
        cur.execute("UPDATE recrawl.doc SET state = 'pending' WHERE state = 'dispatched' "
                    "AND batch_id = ANY(%s::bigint[])", (batches,))
        cur.execute("UPDATE recrawl.host SET batch_id = NULL WHERE batch_id = ANY(%s::bigint[]) "
                    "RETURNING host_id", (batches,))
        _refresh_best_rank(cur, [h for (h,) in cur.fetchall()])


def capacity_report(cfg: Config, conn: psycopg.Connection) -> dict[str, Any]:
    """Per-hostname capacity of the live list at 1 / `min_gap_s` requests per second."""
    r = cfg.recrawl
    rows = conn.execute("SELECT count(*) FROM recrawl.doc WHERE rank IS NOT NULL "
                        "AND state <> 'gone' GROUP BY host_id").fetchall()
    counts = np.array([n for (n,) in rows], dtype=np.int64)
    states = dict(conn.execute("SELECT state, count(*) FROM recrawl.doc "
                               "WHERE rank IS NOT NULL GROUP BY state").fetchall())
    per_day = 86400 / r.min_gap_s
    total = int(counts.sum())
    horizons = []
    for days in r.report_days:
        cap = per_day * days
        covered = int(np.minimum(counts, cap).sum())
        horizons.append({"days": days, "covered": covered,
                         "share": round(covered / total, 4) if total else 0.0,
                         "hosts_unfinished": int((counts > cap).sum()),
                         "mean_req_s": round(covered / (days * 86400), 1)})
    return {"docs": total, "hosts": int(counts.size), "states": states,
            "max_host_docs": int(counts.max()) if counts.size else 0,
            "days_for_largest_host": round(int(counts.max()) / per_day, 1) if counts.size
            else 0.0, "horizons": horizons}


# ---------------------------------------------------------------- archive

def run_archiver(cfg: Config, conn: psycopg.Connection, once: bool) -> dict[str, Any]:
    _instance_lock(conn, "archive")
    return run_loop(lambda: archive_cycle(cfg, conn, datetime.now(UTC)),
                    cfg.recrawl.archive_interval_s, once)


def archive_cycle(cfg: Config, conn: psycopg.Connection, now: datetime) -> dict[str, Any]:
    """Applies every closed fetch log not yet in `recrawl.log`; never changes `result_root`.

    `archive_workers` processes do the file work of each log; this process
    applies the results one log at a time.
    """
    r = cfg.recrawl
    totals: dict[str, Any] = dict.fromkeys(
        ("logs", "records", "applied", "texts", "no_warc", "io_error", "bad_logs"), 0)
    todo: list[tuple[int, Path]] = []
    for p in range(r.crawler_processes):
        d = _crawler_dir(r.result_root, p)
        if not d.is_dir():
            continue
        logs = sorted(d.glob("*.jsonl"))
        done = {n for (n,) in conn.execute("SELECT name FROM recrawl.log WHERE name = ANY(%s)",
                                           ([log.name for log in logs],)).fetchall()}
        todo += [(p, log) for log in logs if log.name not in done]
    if r.archive_workers == 1 or len(todo) < 2:
        prepared: Iterable[PreparedLog] = (prepare_log(r, p, log) for p, log in todo)
        _finish_logs(cfg, conn, todo, prepared, now, totals)
    else:
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(min(r.archive_workers, len(todo)), mp_context=context) as pool:
            jobs = [pool.submit(prepare_log, r, p, log) for p, log in todo]
            _finish_logs(cfg, conn, todo, (job.result() for job in jobs), now, totals)
    return totals


def _finish_logs(cfg: Config, conn: psycopg.Connection, todo: list[tuple[int, Path]],
                 prepared: Iterable[PreparedLog], now: datetime, totals: dict[str, Any]) -> None:
    for (_, log), done in zip(todo, prepared, strict=True):
        if done.status == "ok":
            totals["logs"] += 1
            totals["records"] += len(done.records)
            totals["texts"] += done.texts
            totals["applied"] += _apply(cfg, conn, done.records, done.text_file, log.name, now)
        elif done.status == "bad":
            _record_log(conn, log.name, 0, done.error)
            totals["logs"] += 1
            totals["bad_logs"] += 1
        else:
            # No WARC yet, or an unreadable file: the log is tried again next cycle.
            totals[done.status] += 1
            if done.error:
                totals["last_error"] = done.error


@dataclass(frozen=True)
class PreparedLog:
    """The file work for one fetch log; `status` is ok, bad, no_warc or io_error."""

    status: str
    records: list[dict[str, Any]] = field(default_factory=list)
    text_file: str | None = None
    texts: int = 0
    error: str | None = None


def prepare_log(r: RecrawlCfg, process: int, log: Path) -> PreparedLog:
    """Parses one log, writes its text Parquet and its gzipped copy; touches no DB."""
    m = LOG_NAME_RE.match(log.name)
    if m is None or int(m.group(1)) != process:
        return PreparedLog("bad", error=f"name is not recrawl-{process:02d}-<ts>Z-<seq>.jsonl")
    base = log.name.removesuffix(".jsonl")
    warc = log.with_name(f"{base}.warc.gz")
    if not warc.exists():
        # The crawler prunes the log before its WARC, so this is a log still being closed
        # or a WARC lost outside the contract; its docs time out and go out again.
        return PreparedLog("no_warc")
    try:
        try:
            records = read_fetch_log(log, warc.name)
        except (RecrawlError, ValueError) as exc:
            return PreparedLog("bad", error=str(exc))
        rows = _extract_texts(r.max_body_chars, warc, records)
        rel = _crawler_dir(f"fetch_date={m.group(2)}", process) / f"{base}.parquet"
        if rows:
            write_parquet_atomic(pa.Table.from_pylist(rows, schema=TEXT_SCHEMA),
                                 Path(r.text_root) / rel)
        dest = _crawler_dir(Path(r.log_root) / m.group(2), process)
        dest.mkdir(parents=True, exist_ok=True)
        gz_tmp = dest / f"{base}.jsonl.gz{TMP_SUFFIX}"
        with log.open("rb") as src, gzip.open(gz_tmp, "wb") as out:
            shutil.copyfileobj(src, out)
        commit_file(gz_tmp, dest / f"{base}.jsonl.gz")
    except OSError as exc:
        return PreparedLog("io_error", error=f"{log.name}: {exc}")
    return PreparedLog("ok", records, str(rel) if rows else None, len(rows))


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_record(rec: Any) -> None:
    """Raises ValueError unless `rec` has the C6 types; parses its timestamps in place."""
    if not isinstance(rec, dict) or set(rec) != FETCH_LOG_KEYS:
        raise ValueError("keys differ from the fetch-log contract")
    if not (_is_int(rec["batch_id"]) and _is_int(rec["doc_id"])
            and isinstance(rec["source_url"], str)):
        raise ValueError("batch_id, doc_id or source_url has the wrong type")
    for key in ("final_url", "content_type", "etag", "fail_reason", "warc_file", "payload_sha1"):
        if rec[key] is not None and not isinstance(rec[key], str):
            raise ValueError(f"{key} must be a string or null")
    status = rec["http_status"]
    if status is not None and not (_is_int(status) and 100 <= status <= 599):
        raise ValueError("http_status must be an HTTP status code or null")
    for key in ("warc_offset", "warc_length"):
        if rec[key] is not None and not (_is_int(rec[key]) and rec[key] >= 0):
            raise ValueError(f"{key} must be a non-negative integer or null")
    wait = rec["retry_after_s"]
    if wait is not None and (isinstance(wait, bool) or not isinstance(wait, int | float)):
        raise ValueError("retry_after_s must be a number or null")
    for key in ("dispatched_at", "fetched_at", "last_modified"):
        if rec[key] is None and key == "last_modified":
            continue
        if not isinstance(rec[key], str):
            raise ValueError(f"{key} must be an ISO 8601 string")
        rec[key] = datetime.fromisoformat(rec[key])
        if rec[key].tzinfo is None:
            raise ValueError(f"{key} has no UTC offset")


def read_fetch_log(path: Path, warc_name: str) -> list[dict[str, Any]]:
    """Parses a closed fetch log; one record per doc_id, a 2xx beating a failure."""
    best: dict[int, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            try:
                rec = json.loads(line)
                _check_record(rec)
            except ValueError as exc:
                raise RecrawlError(f"{path.name}:{n}: {exc}") from exc
            if rec["doc_id"] != pg_doc_id(rec["source_url"]):
                raise RecrawlError(f"{path.name}:{n}: doc_id does not match source_url")
            if rec["warc_file"] not in (None, warc_name):
                raise RecrawlError(f"{path.name}:{n}: warc_file is not {warc_name}")
            rec["outcome"], rec["fail_reason"] = classify(rec)
            key = (rec["outcome"] == "fetched", rec["fetched_at"])
            old = best.get(rec["doc_id"])
            if old is None or key >= (old["outcome"] == "fetched", old["fetched_at"]):
                best[rec["doc_id"]] = rec
    return list(best.values())


def _extract_texts(max_body_chars: int, warc: Path,
                   records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Text rows of the fetched records; a record that fails to parse becomes a retry.

    An OSError means the WARC itself is unreadable; it propagates so the whole
    log is tried again instead of losing its pages.
    """
    rows = []
    for rec in records:
        rec["text_len"] = rec["lang"] = None
        if rec["outcome"] != "fetched":
            continue
        try:
            payload, charset = read_response(warc, rec["warc_offset"], rec["warc_length"],
                                             rec["final_url"])
            doc = extract(payload, charset, max_body_chars)
        except OSError:
            raise
        except Exception:
            # Hostile HTML or a bad record must not stop the archive.
            rec["outcome"], rec["fail_reason"] = "retry", "extract_error"
            continue
        rec["text_len"], rec["lang"] = len(doc.body), doc.lang
        rows.append({"doc_id": rec["doc_id"] % _UINT64, "batch_id": rec["batch_id"],
                     "url": rec["source_url"], "final_url": rec["final_url"],
                     "fetched_at": rec["fetched_at"],
                     "http_status": rec["http_status"], "lang": doc.lang, "title": doc.title,
                     "description": doc.description, "body": doc.body,
                     "text_len": len(doc.body), "payload_sha1": rec["payload_sha1"]})
    return rows


def _record_log(db: psycopg.Connection | psycopg.Cursor, name: str, records: int,
                error: str | None) -> None:
    db.execute("INSERT INTO recrawl.log (name, records, error) VALUES (%s, %s, %s) "
               "ON CONFLICT (name) DO NOTHING", (name, records, error))


def _apply(cfg: Config, conn: psycopg.Connection, records: list[dict[str, Any]],
           text_file: str | None, log_name: str, now: datetime) -> int:
    """Applies results idempotently and records the log; returns the number of docs changed."""
    r = cfg.recrawl
    args = {"now": now, "stale": r.stale_days, "robots": r.robots_retry_days,
            "base": r.retry_base_s, "max": r.retry_max_s, "attempts": r.max_attempts,
            "pause": r.host_pause_max_s, "text_file": text_file}
    changed = 0
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("CREATE TEMP TABLE r (batch_id BIGINT, doc_id BIGINT, outcome TEXT, "
                    "http_status SMALLINT, content_type TEXT, final_url TEXT, fail_reason TEXT, "
                    "payload_sha1 TEXT, etag TEXT, last_modified TIMESTAMPTZ, "
                    "fetched_at TIMESTAMPTZ, retry_after_s DOUBLE PRECISION, text_len INTEGER, "
                    "lang TEXT) ON COMMIT DROP")
        with cur.copy(f"COPY r ({', '.join(RESULT_COLUMNS)}) FROM STDIN") as copy:
            for rec in records:
                copy.write_row([rec[c] for c in RESULT_COLUMNS])
        # A 2xx is news whatever the doc's state; it only has to be newer than the last one.
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'fetched', fetched_at = r.fetched_at, "
            "http_status = r.http_status, content_type = r.content_type, "
            "final_url = r.final_url, fail_reason = NULL, payload_sha1 = r.payload_sha1, "
            "etag = r.etag, last_modified = r.last_modified, text_file = %(text_file)s, "
            "text_len = r.text_len, lang = r.lang, attempts = 0, "
            "due_at = r.fetched_at + make_interval(days => %(stale)s) FROM r "
            "WHERE r.outcome = 'fetched' AND d.doc_id = r.doc_id "
            "AND (d.fetched_at IS NULL OR d.fetched_at < r.fetched_at)", args)
        changed += cur.rowcount
        # A failure counts only for the batch the doc is waiting on.
        current = ("FROM r WHERE r.outcome = {} AND d.doc_id = r.doc_id "
                   "AND d.state = 'dispatched' AND d.batch_id = r.batch_id")
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'gone', http_status = r.http_status, "
            "content_type = r.content_type, final_url = r.final_url, "
            "fail_reason = r.fail_reason " + current.format("'gone'"), args)
        changed += cur.rowcount
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'failed', http_status = r.http_status, "
            "final_url = r.final_url, fail_reason = r.fail_reason, "
            "due_at = %(now)s + make_interval(days => %(robots)s) "
            + current.format("'robots'"), args)
        changed += cur.rowcount
        cur.execute(
            "UPDATE recrawl.host h SET next_at = greatest(coalesce(h.next_at, %(now)s), "
            "%(now)s + make_interval(secs => least(coalesce(p.wait, %(base)s), %(pause)s))) "
            "FROM (SELECT d.host_id, max(r.retry_after_s) AS wait FROM r "
            "JOIN recrawl.doc d USING (doc_id) WHERE r.outcome = 'throttled' "
            "AND d.state = 'dispatched' AND d.batch_id = r.batch_id GROUP BY d.host_id) p "
            "WHERE h.host_id = p.host_id", args)
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'failed', attempts = d.attempts + 1, "
            "http_status = r.http_status, final_url = r.final_url, "
            "fail_reason = r.fail_reason, due_at = %(now)s + CASE "
            "WHEN d.attempts + 1 >= %(attempts)s THEN make_interval(days => %(stale)s) "
            "ELSE make_interval(secs => least(%(base)s * power(2, d.attempts), %(max)s)) END "
            "FROM r WHERE r.outcome IN ('throttled', 'retry') AND d.doc_id = r.doc_id "
            "AND d.state = 'dispatched' AND d.batch_id = r.batch_id", args)
        changed += cur.rowcount
        cur.execute("SELECT DISTINCT d.host_id FROM r JOIN recrawl.doc d USING (doc_id)")
        _refresh_best_rank(cur, [h for (h,) in cur.fetchall()])
        # Last, so a crash before here leaves the log to be applied again.
        _record_log(cur, log_name, len(records), None)
    return changed
