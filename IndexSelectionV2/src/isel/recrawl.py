"""Recrawl frontier: hands the live selection to the crawler and stores the text it fetched.

The frontier lives in selectdb's `recrawl` schema (`isel.schema`). The crawler
side is the manual-bulk-crawl job API of WebCrawler PR #84 at bd40191
(`docs/RECRAWL.md`): POST a URL list with a `ttl_days`, get a
`job_id`, and poll the job's status until the API reports it `completed`. Only
the API decides that: the results dir `result_root/{job_id}` exists from the
POST on, and each crawler fills its own `crawler_NN/` partition. The TTL is the
job's only deadline: past it the API stops the job and deletes its results, and
isel puts its docs back in line.

- `sync` (after each swap): loads the live run's manifest. A doc is on the
  list while `rank` is set; sync writes only `rank` and `model_run_id`.
- `plan` (loop): packs the due docs of the best-ranked ready hosts into jobs
  and POSTs them. A host is in at most one open job, and its next job waits
  `min_gap_s` per doc it was sent, so the recrawl alone stays within 0.2 qps
  per host. A job is recorded `claimed` before it is sent, so no HTTP call
  runs inside a transaction. Plan also polls each unapplied job's status and
  marks it `completed` with the API's `completed_at`. Once a job is applied,
  plan DELETEs its results.
- `archive` (loop): reads each completed job's directory without changing it
  and without calling the API. Worker processes extract each page's text into
  Parquet and keep a gzipped copy of the per-URL results; the main process then
  applies them idempotently.

Results map to docs by position: `request.json` lists the URLs in the order
sent, after the API canonicalizes them and drops duplicates, and the crawler
that owns each one (`crawler_ids`). Result n is `crawler_{crawler_ids[n]:02d}/
{n:05d}.json`, as the API resolves its downloads. The archive checks the order
before it trusts it (`map_results`).

Every frontier transaction takes one advisory lock, so plan, archive and sync
never interleave their updates and cannot deadlock.
"""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import itertools
import json
import multiprocessing
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
from isel.extract import extract
from isel.io import TMP_SUFFIX, commit_file, open_duckdb, sql_str, write_parquet_atomic
from isel.manifest import validate_run
from isel.publish import swap_allowed
from isel.schema import DUE_STATES, OPEN_JOB_STATES
from isel.urlnorm import md5_number_lower

LOCK_CYCLE = 0x15E1_DB02      # xact lock held by every frontier transaction
LOCK_INSTANCE = {"sync": 0x15E1_DB03, "plan": 0x15E1_DB04, "archive": 0x15E1_DB05}
JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")
BAD_INDEX_RE = re.compile(r"\bat index (\d+)$")
NON_HTML = "NonHTML content-type"       # the crawler's error for a 2xx without HTML
ITEM_KEYS = frozenset({"url", "final_url", "status_code", "error", "html_file", "fetched_at"})
RESULT_COLUMNS = ("doc_id", "outcome", "http_status", "final_url", "fail_reason",
                  "payload_sha1", "fetched_at", "text_len", "lang")
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
_OPEN = "state IN ({})".format(", ".join(f"'{s}'" for s in OPEN_JOB_STATES))
_INT63 = (1 << 63) - 1
_UINT64 = 1 << 64


class RecrawlError(RuntimeError):
    """Raised when the frontier or a job's results are inconsistent."""


def host_id(host: str) -> int:
    """Stable queue id of a hostname: the low 63 bits of `md5_number_lower`."""
    return md5_number_lower(host) & _INT63


def pg_doc_id(url: str) -> int:
    """`doc_id` of a canonical URL as selectdb stores it (two's-complement BIGINT)."""
    value = md5_number_lower(url)
    return value - (1 << 64) if value > _INT63 else value


def classify(item: dict[str, Any]) -> tuple[str, str | None]:
    """Maps one result record (`NNNNN.json`) to (outcome, fail_reason)."""
    status, error = item["status_code"], item["error"]
    if status is not None and 200 <= status < 300:
        if error is None and item["html_file"]:
            return "fetched", None
        if error == NON_HTML:
            return "gone", "non_html"
        return "retry", error or "no_html"
    if status in (404, 410):
        return "gone", f"http_{status}"
    if status in (429, 503):
        return "throttled", f"http_{status}"
    if status is None and error == "IgnoreRequest":
        # Raised by robots.txt refusals; other downloader middlewares may raise it too.
        return "robots", "ignore_request"
    return "retry", f"http_{status}" if status is not None else (error or "no_response")


def _instance_lock(conn: psycopg.Connection, name: str) -> None:
    (ok,) = conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_INSTANCE[name],)).fetchone()
    if not ok:
        raise RecrawlError(f"another recrawl-{name} holds the instance lock")


def _cycle_lock(cur: psycopg.Cursor) -> None:
    cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_CYCLE,))


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
                "COPY (SELECT doc_id, url, host_id, host, rank FROM sync_rows "
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
    """One row per doc_id, at its best rank."""
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE sync_rows AS
WITH best AS (
  SELECT * FROM read_parquet({sql_str(str(run_dir / 'manifest' / '*.parquet'))})
  QUALIFY row_number() OVER (PARTITION BY doc_id ORDER BY rank, url) = 1
)
SELECT row_number() OVER (ORDER BY rank) AS i,
       CASE WHEN doc_id > {_INT63}
            THEN (doc_id - {_INT63 + 1})::BIGINT - {_INT63} - 1
            ELSE doc_id::BIGINT END AS doc_id,
       canonical_url AS url, (md5_number_lower(host) & {_INT63})::BIGINT AS host_id,
       host, rank
FROM best""")
    (docs,) = con.execute("SELECT count(*) FROM sync_rows").fetchone()
    return int(docs)


def _load_chunk(cfg: Config, conn: psycopg.Connection, csv_path: Path, run_id: int,
                at: datetime) -> None:
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("CREATE TEMP TABLE s (doc_id BIGINT, url TEXT, host_id BIGINT, host TEXT, "
                    "rank BIGINT) ON COMMIT DROP")
        with cur.copy("COPY s FROM STDIN (FORMAT csv)") as copy, csv_path.open("rb") as fh:
            while block := fh.read(cfg.selectdb.copy_block_bytes):
                copy.write(block)
        cur.execute("INSERT INTO recrawl.host (host_id, host) SELECT DISTINCT ON (host_id) "
                    "host_id, host FROM s ORDER BY host_id ON CONFLICT (host_id) DO NOTHING")
        cur.execute("SELECT count(*) FROM s JOIN recrawl.host h USING (host_id) "
                    "WHERE h.host <> s.host")
        (clashes,) = cur.fetchone()
        if clashes:
            raise RecrawlError(f"{clashes} docs hit a host_id taken by another hostname")
        cur.execute(
            "WITH up AS (INSERT INTO recrawl.doc (doc_id, url, host_id, rank, model_run_id, "
            "due_at) SELECT doc_id, url, host_id, rank, %s, %s FROM s ON CONFLICT (doc_id) "
            "DO UPDATE SET rank = EXCLUDED.rank, model_run_id = EXCLUDED.model_run_id "
            "WHERE (recrawl.doc.rank, recrawl.doc.model_run_id) "
            "IS DISTINCT FROM (EXCLUDED.rank, EXCLUDED.model_run_id) RETURNING host_id) "
            + _WAKE.format("(SELECT DISTINCT host_id FROM up)"),
            (run_id, at, at + timedelta(seconds=cfg.recrawl.host_pause_max_s)))


# ---------------------------------------------------------------- plan

@dataclass(frozen=True)
class Reply:
    """Outcome of one POST; `status` is None when acceptance is unknown."""

    status: int | None
    job_id: str | None = None
    error: str | None = None


class CrawlerApi:
    """Client of the crawler's `/v1/manual-bulk-crawls` routes (WebCrawler PR #84).

    `paused_until` is set when the API refused a job for a reason a retry
    cannot fix soon; the planner claims nothing before then.
    """

    def __init__(self, base_url: str, timeout_s: float) -> None:
        self.url = f"{base_url}/v1/manual-bulk-crawls"
        self.timeout_s = timeout_s
        self.paused_until: datetime | None = None

    def submit(self, urls: list[str], ttl_days: int) -> Reply:
        request = urllib.request.Request(
            self.url, data=json.dumps({"urls": urls, "ttl_days": ttl_days}).encode(),
            method="POST", headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as resp:
                job_id = json.load(resp).get("job_id")
        except urllib.error.HTTPError as exc:
            error = None
            with contextlib.suppress(ValueError, AttributeError):
                error = json.load(exc).get("error")
            return Reply(exc.code, error=error if isinstance(error, str) else None)
        except (OSError, ValueError, AttributeError):
            return Reply(None)
        if not (isinstance(job_id, str) and JOB_ID_RE.match(job_id)):
            # Accepted, but under an id we cannot use: as good as an unknown answer.
            return Reply(None, error=f"unexpected job_id {job_id!r}")
        return Reply(200, job_id)

    def status(self, job_id: str) -> tuple[str | None, Any]:
        """The job's (status, completed_at) at the API.

        status is `pending`, `running`, `completed`, `missing` (404), or None if
        the API answered but not usably. `completed_at` is the API's value,
        unchecked. Raises OSError if the API cannot be reached.
        """
        try:
            with urllib.request.urlopen(f"{self.url}/{job_id}", timeout=self.timeout_s) as resp:
                body = json.load(resp)
                status, completed_at = body.get("status"), body.get("completed_at")
        except urllib.error.HTTPError as exc:
            return ("missing" if exc.code == 404 else None), None
        except (ValueError, AttributeError):
            return None, None
        if status not in ("pending", "running", "completed"):
            return None, None
        return status, completed_at

    def delete(self, job_id: str) -> bool:
        """Removes the job and its results; True once the API holds nothing of it."""
        request = urllib.request.Request(f"{self.url}/{job_id}", method="DELETE")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as resp:
                return resp.status == 204
        except urllib.error.HTTPError as exc:
            return exc.code == 404
        except OSError:
            return False


def run_planner(cfg: Config, conn: psycopg.Connection, once: bool) -> dict[str, Any]:
    _instance_lock(conn, "plan")
    api = CrawlerApi(cfg.recrawl.api_url, cfg.recrawl.api_timeout_s)
    return run_loop(lambda: plan_cycle(cfg, conn, datetime.now(UTC), api),
                    cfg.recrawl.plan_interval_s, once)


@dataclass
class Job:
    """A claimed job: its docs in the order their URLs are sent."""

    batch_id: int
    doc_ids: list[int]
    urls: list[str]


def plan_cycle(cfg: Config, conn: psycopg.Connection, now: datetime,
               api: CrawlerApi) -> dict[str, Any]:
    """One planning pass; returns counts.

    1. Recover: a job whose POST got no answer is lost after `claim_timeout_s`.
    2. Poll: every unapplied job is asked for its status every `status_poll_s`.
       A submitted job the API reports done becomes `completed`; a job the API
       no longer knows is lost (its TTL passed).
    3. Delete the results of applied jobs at the API.
    4. Stop if any job is in `error` (the API broke its contract), if a
       completed job's results are not mounted, if the API asked for a pause,
       if `max_jobs_inflight` jobs are at the crawler or wait for the archive,
       or if the text disk is short.
    5. Claim jobs, commit, then send them.
    """
    r = cfg.recrawl
    stats: dict[str, Any] = {"lost": 0, "orphans": 0, "swept": 0, "checked": 0,
                             "completed": 0, "deleted": 0, "open": 0, "jobs": 0, "docs": 0,
                             "invalid": 0, "refused": 0, "unknown": 0,
                             "halted": False, "unmounted": False, "paused": False,
                             "archive_lag": False, "disk_full": False}
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("UPDATE recrawl.job SET state = 'lost', finished_at = %s, "
                    "error = 'no reply to the POST' WHERE state = 'claimed' AND claimed_at < %s "
                    "RETURNING batch_id", (now, now - timedelta(seconds=r.claim_timeout_s)))
        lost = [b for (b,) in cur.fetchall()]
        stats["lost"] = len(lost)
        _release(cur, lost)
        stats["orphans"] = _release_orphans(cur)
        cur.execute(
            "UPDATE recrawl.doc SET state = 'pending' WHERE doc_id IN (SELECT doc_id "
            "FROM recrawl.doc WHERE state = 'fetched' AND rank IS NOT NULL AND due_at <= %s "
            "LIMIT %s) RETURNING host_id", (now, r.sweep_rows))
        swept = [h for (h,) in cur.fetchall()]
        stats["swept"] = len(swept)
        _wake(cur, swept, now, r.host_pause_max_s)
        _refresh_best_rank(cur, swept)
    _check_jobs(cfg, conn, api, now, stats)
    _delete_applied(conn, api, now, stats)

    counts = dict(conn.execute("SELECT state, count(*) FROM recrawl.job "
                               f"WHERE {_OPEN} GROUP BY state").fetchall())
    stats["open"] = counts.get("claimed", 0) + counts.get("submitted", 0)
    room = min(r.plan_max_jobs, r.max_jobs_inflight - stats["open"])
    text_root = Path(r.text_root)
    text_root.mkdir(parents=True, exist_ok=True)
    if counts.get("error"):
        stats["halted"] = True
    elif stats["unmounted"]:
        pass                              # config_error names the job
    elif api.paused_until is not None and now < api.paused_until:
        stats["paused"] = True
    elif counts.get("completed", 0) >= r.max_jobs_inflight:
        stats["archive_lag"] = True
    elif shutil.disk_usage(text_root).free < r.min_free_gb * 1e9:
        stats["disk_full"] = True
    elif room > 0:
        with conn.transaction(), conn.cursor() as cur:
            _cycle_lock(cur)
            jobs = _claim(cfg, cur, now, room)
        _send(cfg, conn, api, jobs, stats, now)
    return stats


def _release(cur: psycopg.Cursor, batches: list[int]) -> None:
    """Puts the docs of these jobs back in line and frees their hosts."""
    if not batches:
        return
    cur.execute("UPDATE recrawl.doc SET state = 'pending' WHERE state = 'dispatched' "
                "AND batch_id = ANY(%s::bigint[]) RETURNING host_id", (batches,))
    hosts = [h for (h,) in cur.fetchall()]
    cur.execute("UPDATE recrawl.host SET batch_id = NULL WHERE batch_id = ANY(%s::bigint[]) "
                "RETURNING host_id", (batches,))
    _refresh_best_rank(cur, hosts + [h for (h,) in cur.fetchall()])


def _release_orphans(cur: psycopg.Cursor) -> int:
    """Safety net: docs and hosts held by a job that is no longer open."""
    alive = f"EXISTS (SELECT 1 FROM recrawl.job j WHERE j.batch_id = {{}}.batch_id AND j.{_OPEN})"
    cur.execute("UPDATE recrawl.doc d SET state = 'pending' WHERE d.state = 'dispatched' "
                f"AND NOT {alive.format('d')} RETURNING host_id")
    hosts = [h for (h,) in cur.fetchall()]
    cur.execute("UPDATE recrawl.host h SET batch_id = NULL WHERE h.batch_id IS NOT NULL "
                f"AND NOT {alive.format('h')} RETURNING host_id")
    freed = [h for (h,) in cur.fetchall()]
    _refresh_best_rank(cur, hosts + freed)
    return len(hosts)


def _check_jobs(cfg: Config, conn: psycopg.Connection, api: CrawlerApi, now: datetime,
                stats: dict[str, Any]) -> None:
    """Asks the API about each unapplied job at most every `status_poll_s`.

    Only the API knows when a job is done (docs/03 §3.7). A submitted job it
    reports `completed` becomes `completed` once its results dir is mounted; if
    the dir is missing the mount is wrong for every job, so claims stop. A job
    the API no longer knows is lost: it expired, or its results were deleted.
    That also covers a completed or `error` job left unapplied past its TTL. A
    job whose answer is unusable is skipped, so it cannot hold back the others;
    the first query that cannot reach the API ends the round, so a dead API
    costs one timeout.
    """
    r = cfg.recrawl
    rows = conn.execute(
        "SELECT batch_id, job_id, state FROM recrawl.job "
        "WHERE state IN ('submitted', 'completed', 'error') "
        "AND coalesce(checked_at, submitted_at) <= %s ORDER BY batch_id",
        (now - timedelta(seconds=r.status_poll_s),)).fetchall()
    for batch, job_id, state in rows:
        try:
            status, completed_at = api.status(job_id)
        except OSError as exc:
            stats["api_error"] = f"GET {api.url}/{job_id}: {exc}"
            return
        if status is None:
            stats["api_error"] = f"GET {api.url}/{job_id} gave no usable answer"
            continue
        stats["checked"] += 1
        if status == "missing":
            with conn.transaction(), conn.cursor() as cur:
                _cycle_lock(cur)
                cur.execute("UPDATE recrawl.job SET state = 'lost', finished_at = %s, "
                            "error = 'the API does not know it' WHERE batch_id = %s "
                            "AND state IN ('submitted', 'completed', 'error')", (now, batch))
                if cur.rowcount:
                    _release(cur, [batch])
                    stats["lost"] += 1
            continue
        if status == "completed" and state == "submitted":
            if not (Path(r.result_root) / job_id).is_dir():
                stats["unmounted"] = True
                stats["config_error"] = f"job {job_id} is completed but not under {r.result_root}"
                continue
            try:
                done = _timestamp(completed_at, "completed_at")
            except ValueError as exc:
                stats["api_error"] = f"GET {api.url}/{job_id}: {exc}"
                continue
            conn.execute("UPDATE recrawl.job SET state = 'completed', completed_at = %s, "
                         "checked_at = %s WHERE batch_id = %s AND state = 'submitted'",
                         (done, now, batch))
            stats["completed"] += 1
            continue
        conn.execute("UPDATE recrawl.job SET checked_at = %s WHERE batch_id = %s",
                     (now, batch))


def _delete_applied(conn: psycopg.Connection, api: CrawlerApi, now: datetime,
                    stats: dict[str, Any]) -> None:
    """Deletes applied jobs at the API; a failure stops here and is tried next cycle."""
    rows = conn.execute("SELECT batch_id, job_id FROM recrawl.job WHERE state = 'applied' "
                        "AND deleted_at IS NULL ORDER BY batch_id").fetchall()
    for batch, job_id in rows:
        if not api.delete(job_id):
            stats["api_error"] = f"DELETE {api.url}/{job_id} failed"
            return
        conn.execute("UPDATE recrawl.job SET deleted_at = %s WHERE batch_id = %s",
                     (now, batch))
        stats["deleted"] += 1


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


def _ready_hosts(cfg: Config, cur: psycopg.Cursor, now: datetime,
                 want: int) -> Iterable[tuple[int, list[tuple[int, str]]]]:
    """Ready hosts by best rank with up to `host_docs_per_job` due docs each.

    Pages through `host_ready` so a cycle reads about the hosts it needs. A
    ready host with no doc due now (all in backoff) waits for its earliest one,
    or leaves the ready set.
    """
    r = cfg.recrawl
    page = max(100, want // r.host_docs_per_job + 1)
    last = (-1, -1)
    while True:
        cur.execute(
            "SELECT host_id, best_rank FROM recrawl.host WHERE batch_id IS NULL "
            "AND best_rank IS NOT NULL AND (next_at IS NULL OR next_at <= %s) "
            "AND (best_rank, host_id) > (%s, %s) ORDER BY best_rank, host_id LIMIT %s",
            (now, *last, page))
        hosts = cur.fetchall()
        if not hosts:
            return
        last = hosts[-1][1], hosts[-1][0]
        cur.execute(
            "SELECT h.host_id, d.doc_id, d.url FROM unnest(%s::bigint[]) "
            "WITH ORDINALITY AS h(host_id, o) CROSS JOIN LATERAL (SELECT doc_id, url, rank "
            f"FROM recrawl.doc WHERE host_id = h.host_id AND rank IS NOT NULL AND {_DUE} "
            "AND due_at <= %s ORDER BY rank LIMIT %s) d ORDER BY h.o, d.rank",
            ([h for h, _ in hosts], now, r.host_docs_per_job))
        docs: dict[int, list[tuple[int, str]]] = {}
        for h, doc_id, url in cur.fetchall():
            docs.setdefault(h, []).append((doc_id, url))
        idle = [h for h, _ in hosts if h not in docs]
        if idle:
            cur.execute(
                "UPDATE recrawl.host h SET next_at = m.n, best_rank = CASE WHEN m.n IS NULL "
                "THEN NULL ELSE h.best_rank END FROM (SELECT u.host_id, (SELECT min(d.due_at) "
                "FROM recrawl.doc d WHERE d.host_id = u.host_id AND d.rank IS NOT NULL "
                f"AND d.{_DUE}) AS n FROM unnest(%s::bigint[]) AS u(host_id)) m "
                "WHERE h.host_id = m.host_id", (idle,))
        for h, _ in hosts:
            if h in docs:
                yield h, docs[h]


def _claim(cfg: Config, cur: psycopg.Cursor, now: datetime, n_jobs: int) -> list[Job]:
    """Packs ready hosts into up to `n_jobs` jobs and marks their docs `dispatched`.

    Hosts fill each job in rank order; inside a job their URLs alternate, so the
    crawler's prefetch window spans many hosts instead of one.
    """
    r = cfg.recrawl
    packed: list[list[tuple[int, list[tuple[int, str]]]]] = [[]]
    size = 0
    hosts = _ready_hosts(cfg, cur, now, n_jobs * r.job_size)
    for h, docs in hosts:
        if size == r.job_size:
            if len(packed) == n_jobs:
                break
            packed.append([])
            size = 0
        take = docs[:r.job_size - size]
        packed[-1].append((h, take))
        size += len(take)
    hosts.close()
    packed = [p for p in packed if p]
    if not packed:
        return []
    cur.execute("SELECT nextval('recrawl.batch_seq') FROM generate_series(1, %s)",
                (len(packed),))
    jobs = []
    for (batch,), groups in zip(cur.fetchall(), packed, strict=True):
        order = [d for row in itertools.zip_longest(*(docs for _, docs in groups))
                 for d in row if d is not None]
        jobs.append(Job(batch, [d for d, _ in order], [u for _, u in order]))
    cur.executemany("INSERT INTO recrawl.job (batch_id, doc_ids, state, claimed_at) "
                    "VALUES (%s, %s, 'claimed', %s)",
                    [(j.batch_id, j.doc_ids, now) for j in jobs])
    cur.execute(
        "UPDATE recrawl.doc d SET state = 'dispatched', batch_id = u.batch_id "
        "FROM unnest(%s::bigint[], %s::bigint[]) AS u(doc_id, batch_id) "
        "WHERE d.doc_id = u.doc_id",
        ([d for j in jobs for d in j.doc_ids],
         [j.batch_id for j in jobs for _ in j.doc_ids]))
    # The next job of a host waits min_gap_s per doc sent: 0.2 qps on average.
    rows = [(h, j.batch_id, len(docs)) for j, groups in zip(jobs, packed, strict=True)
            for h, docs in groups]
    cur.execute(
        "UPDATE recrawl.host h SET batch_id = u.batch_id, "
        "next_at = %s + make_interval(secs => u.n * %s) "
        "FROM unnest(%s::bigint[], %s::bigint[], %s::int[]) AS u(host_id, batch_id, n) "
        "WHERE h.host_id = u.host_id",
        (now, r.min_gap_s, *map(list, zip(*rows, strict=True))))
    _refresh_best_rank(cur, [h for h, _, _ in rows])
    return jobs


def _send(cfg: Config, conn: psycopg.Connection, api: CrawlerApi, jobs: list[Job],
          stats: dict[str, Any], now: datetime) -> None:
    """POSTs claimed jobs in order and records each answer (docs/03 §3.6).

    - 200: the job is `submitted` under its `job_id`.
    - 400 "... at index N": that URL is unfetchable; it becomes `gone` and the
      rest is sent again, at most `max_resend` times.
    - Any other 4xx or 5xx: the API accepted nothing. The job is released,
      sending stops, and the planner pauses `api_backoff_s`.
    - No answer: acceptance is unknown, so the job stays `claimed` (the claim
      timeout recovers it) and sending stops.
    Jobs not sent are released.
    """
    r = cfg.recrawl
    dropped: list[Job] = []
    for n, job in enumerate(jobs):
        for _ in range(r.max_resend + 1):
            reply = api.submit(job.urls, r.result_ttl_days)
            bad = BAD_INDEX_RE.search(reply.error or "") if reply.status == 400 else None
            if bad is None or int(bad.group(1)) >= len(job.urls):
                break
            _drop_invalid(conn, job, int(bad.group(1)))
            stats["invalid"] += 1
            if not job.urls:
                break
        if reply.status == 200 and job.urls:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute("UPDATE recrawl.job SET state = 'submitted', job_id = %s, "
                            "submitted_at = %s WHERE batch_id = %s",
                            (reply.job_id, now, job.batch_id))
            stats["jobs"] += 1
            stats["docs"] += len(job.urls)
            continue
        if reply.status is None:
            stats["unknown"] += 1
            if reply.error:
                stats["api_error"] = reply.error
            dropped += jobs[n + 1:]
            break
        dropped.append(job)
        if job.urls and not (reply.status == 400 and bad is not None):
            stats["api_error"] = f"POST {api.url} answered {reply.status}: {reply.error}"
            api.paused_until = now + timedelta(seconds=r.api_backoff_s)
            dropped += jobs[n + 1:]
            break
    stats["refused"] = len(dropped)
    if dropped:
        with conn.transaction(), conn.cursor() as cur:
            _cycle_lock(cur)
            batches = [j.batch_id for j in dropped]
            cur.execute("UPDATE recrawl.job SET state = 'lost', finished_at = %s, "
                        "error = 'not accepted' WHERE batch_id = ANY(%s::bigint[])",
                        (now, batches))
            _release(cur, batches)


def _drop_invalid(conn: psycopg.Connection, job: Job, index: int) -> None:
    """The API judged this URL invalid; asking again would repeat the answer."""
    doc_id = job.doc_ids.pop(index)
    job.urls.pop(index)
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("UPDATE recrawl.doc SET state = 'gone', fail_reason = 'invalid_url' "
                    "WHERE doc_id = %s AND state = 'dispatched' AND batch_id = %s "
                    "RETURNING host_id", (doc_id, job.batch_id))
        hosts = [h for (h,) in cur.fetchall()]
        cur.execute("UPDATE recrawl.job SET doc_ids = %s WHERE batch_id = %s",
                    (job.doc_ids, job.batch_id))
        # A host with nothing left in this job need not wait for it.
        cur.execute("UPDATE recrawl.host h SET batch_id = NULL WHERE h.host_id = ANY(%s) "
                    "AND h.batch_id = %s AND NOT EXISTS (SELECT 1 FROM recrawl.doc d "
                    "WHERE d.host_id = h.host_id AND d.state = 'dispatched' "
                    "AND d.batch_id = h.batch_id)", (hosts, job.batch_id))
        _refresh_best_rank(cur, hosts)


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


@dataclass(frozen=True)
class JobInput:
    """What a worker needs to read one job's results: the docs and URLs as sent."""

    batch_id: int
    job_id: str
    doc_ids: list[int]
    urls: list[str]
    completed_at: datetime                # the API's; names the day the results land in


@dataclass(frozen=True)
class PreparedJob:
    """The file work for one job; `status` is ok, error or io_error."""

    status: str
    records: list[dict[str, Any]] = field(default_factory=list)
    text_file: str | None = None
    texts: int = 0
    error: str | None = None


def archive_cycle(cfg: Config, conn: psycopg.Connection, now: datetime) -> dict[str, Any]:
    """Applies every completed job; never changes `result_root` and never calls the API.

    Candidates are completed jobs and jobs in `error`, which are tried again so
    a code fix resumes them. A job whose dir is gone (expired, deleted) is left
    for the planner, which loses it once the API stops knowing it.
    """
    r = cfg.recrawl
    totals: dict[str, Any] = dict.fromkeys(
        ("jobs", "records", "applied", "texts", "errors", "io_error"), 0)
    rows = conn.execute(
        "SELECT batch_id, job_id, doc_ids, completed_at FROM recrawl.job "
        "WHERE state IN ('completed', 'error') ORDER BY batch_id").fetchall()
    rows = [row for row in rows if (Path(r.result_root) / row[1]).is_dir()]
    if not rows:
        return totals
    urls = dict(conn.execute("SELECT doc_id, url FROM recrawl.doc WHERE doc_id = ANY(%s)",
                             ([d for row in rows for d in row[2]],)).fetchall())
    todo = [JobInput(b, j, ids, [urls[d] for d in ids], done) for b, j, ids, done in rows]
    if r.archive_workers == 1 or len(todo) < 2:
        _finish_jobs(cfg, conn, todo, (prepare_job(r, j) for j in todo), now, totals)
    else:
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(min(r.archive_workers, len(todo)), mp_context=context) as pool:
            futures = [pool.submit(prepare_job, r, j) for j in todo]
            _finish_jobs(cfg, conn, todo, (f.result() for f in futures), now, totals)
    return totals


def _finish_jobs(cfg: Config, conn: psycopg.Connection, todo: list[JobInput],
                 prepared: Iterable[PreparedJob], now: datetime,
                 totals: dict[str, Any]) -> None:
    for job, done in zip(todo, prepared, strict=True):
        if done.status == "ok":
            totals["jobs"] += 1
            totals["records"] += len(done.records)
            totals["texts"] += done.texts
            totals["applied"] += _apply(cfg, conn, job, done, now)
        elif done.status == "error":
            # The results break the contract. The job keeps its docs and halts the
            # planner until a fix lets a later cycle apply it, or its TTL passes.
            totals["errors"] += 1
            totals["last_error"] = done.error
            with conn.transaction(), conn.cursor() as cur:
                _cycle_lock(cur)
                cur.execute("UPDATE recrawl.job SET state = 'error', error = %s "
                            "WHERE batch_id = %s AND state IN ('completed', 'error')",
                            (done.error, job.batch_id))
        else:
            # An unreadable file: the job is tried again next cycle.
            totals["io_error"] += 1
            totals["last_error"] = done.error


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _timestamp(value: Any, key: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{key} must be an ISO 8601 string")
    at = datetime.fromisoformat(value)
    if at.tzinfo is None:
        raise ValueError(f"{key} has no UTC offset")
    return at


def read_item(part: Path, index: int, url: str) -> dict[str, Any]:
    """Reads and type-checks `NNNNN.json` in its crawler's partition `part`.

    Raises ValueError if the record breaks the contract.
    """
    name = f"{part.name}/{index:05d}.json"
    item = json.loads((part / f"{index:05d}.json").read_text(encoding="utf-8"))
    if not isinstance(item, dict) or not set(item) >= ITEM_KEYS:
        raise ValueError(f"{name} lacks a result key")
    if item["url"] != url:
        raise ValueError(f"{name} is for another URL than request.json lists")
    status = item["status_code"]
    if status is not None and not (_is_int(status) and 100 <= status <= 599):
        raise ValueError(f"{name}: status_code must be an HTTP status or null")
    for key in ("final_url", "error"):
        if item[key] is not None and not isinstance(item[key], str):
            raise ValueError(f"{name}: {key} must be a string or null")
    if item["html_file"] not in (None, f"{index:05d}.html"):
        raise ValueError(f"{name}: html_file must be {index:05d}.html or null")
    item["fetched_at"] = _timestamp(item["fetched_at"], f"{name} fetched_at")
    return item


def map_results(sent: list[str], listed: list[str]) -> list[tuple[int, int]]:
    """Pairs (position sent, position in `request.json`) for every doc with a result.

    The API keeps the order and may only canonicalize a URL or drop a later
    duplicate (docs/03 §3.6). With nothing dropped, position i is doc i; a URL
    that differs there but equals another URL we sent means the order changed,
    which would attach one page's text to another doc. With duplicates dropped,
    only URLs that came back unchanged are paired; the rest are retried alone.
    """
    if len(listed) > len(sent):
        raise RecrawlError(f"request.json lists {len(listed)} URLs; we sent {len(sent)}")
    if len(listed) == len(sent):
        ours = set(sent)
        moved = [i for i, (a, b) in enumerate(zip(sent, listed, strict=True))
                 if a != b and b in ours]
        if moved:
            raise RecrawlError(f"request.json is not in the order sent (position {moved[0]})")
        return [(i, i) for i in range(len(sent))]
    where = {u: n for n, u in enumerate(listed)}
    return [(i, where[u]) for i, u in enumerate(sent) if u in where]


def prepare_job(r: RecrawlCfg, job: JobInput) -> PreparedJob:
    """Maps one job's results to its docs and writes its text and results; no DB."""
    d = Path(r.result_root) / job.job_id
    try:
        try:
            request = json.loads((d / "request.json").read_text(encoding="utf-8"))
            listed, owners = request["urls"], request["crawler_ids"]
            if not (isinstance(listed, list) and all(isinstance(u, str) for u in listed)):
                raise ValueError("request.json urls must be a list of strings")
            if not (isinstance(owners, list) and len(owners) == len(listed)
                    and all(_is_int(c) and c >= 0 for c in owners)):
                raise ValueError("request.json crawler_ids must be one crawler per URL")
            parts = [d / f"crawler_{c:02d}" for c in owners]
            items = [read_item(parts[n], n, u) for n, u in enumerate(listed)]
            paired = dict(map_results(job.urls, listed))
            records, rows, lines = [], [], []
            for i, doc_id in enumerate(job.doc_ids):
                n = paired.get(i)
                item = items[n] if n is not None else None
                rec = _record(r, parts[n] if n is not None else d, job, i, item, rows)
                records.append(rec)
                lines.append(json.dumps({
                    "batch_id": job.batch_id, "job_id": job.job_id, "doc_id": doc_id,
                    "url": job.urls[i], "index": n,
                    **({k: str(v) if k == "fetched_at" else v for k, v in item.items()}
                       if item else {}),
                    "outcome": rec["outcome"], "fail_reason": rec["fail_reason"]},
                    ensure_ascii=False))
        except FileNotFoundError as exc:
            # A completed job's files are all written, so a missing file breaks the
            # contract, unless the whole dir went away meanwhile.
            if not d.is_dir():
                return PreparedJob("io_error", error=f"{job.job_id} vanished")
            return PreparedJob("error", error=f"{job.job_id}: missing {Path(exc.filename).name}")
        except (ValueError, KeyError, TypeError, RecrawlError) as exc:
            return PreparedJob("error", error=f"{job.job_id}: {exc}")
        day = f"{job.completed_at.astimezone(UTC):%Y%m%d}"
        rel = f"fetch_date={day}/job-{job.batch_id}.parquet"
        if rows:
            write_parquet_atomic(pa.Table.from_pylist(rows, schema=TEXT_SCHEMA),
                                 Path(r.text_root) / rel)
        dest = Path(r.log_root) / day
        dest.mkdir(parents=True, exist_ok=True)
        gz_tmp = dest / f"job-{job.batch_id}.jsonl.gz{TMP_SUFFIX}"
        with gzip.open(gz_tmp, "wt", encoding="utf-8") as out:
            out.writelines(line + "\n" for line in lines)
        commit_file(gz_tmp, dest / f"job-{job.batch_id}.jsonl.gz")
    except OSError as exc:
        return PreparedJob("io_error", error=f"{job.job_id}: {exc}")
    return PreparedJob("ok", records, rel if rows else None, len(rows))


def _record(r: RecrawlCfg, part: Path, job: JobInput, i: int, item: dict[str, Any] | None,
            rows: list[dict[str, Any]]) -> dict[str, Any]:
    """The result of doc i, read from its crawler's partition `part`.

    Appends its text row when the page was fetched. An OSError reading the HTML
    propagates, so the whole job is tried again instead of losing its pages.
    """
    rec = dict.fromkeys(RESULT_COLUMNS)
    rec["doc_id"] = job.doc_ids[i]
    if item is None:
        rec["outcome"], rec["fail_reason"] = "retry", "unmatched"
        return rec
    rec["outcome"], rec["fail_reason"] = classify(item)
    rec.update(http_status=item["status_code"], final_url=item["final_url"],
               fetched_at=item["fetched_at"])
    if rec["outcome"] != "fetched":
        return rec
    payload = (part / item["html_file"]).read_bytes()
    try:
        doc = extract(payload, None, r.max_body_chars)
    except Exception:
        # Hostile HTML must not stop the archive.
        rec["outcome"], rec["fail_reason"] = "retry", "extract_error"
        return rec
    rec.update(payload_sha1=hashlib.sha1(payload).hexdigest(), text_len=len(doc.body),
               lang=doc.lang)
    rows.append({"doc_id": rec["doc_id"] % _UINT64, "batch_id": job.batch_id,
                 "url": job.urls[i], "final_url": item["final_url"],
                 "fetched_at": item["fetched_at"], "http_status": item["status_code"],
                 "lang": doc.lang, "title": doc.title, "description": doc.description,
                 "body": doc.body, "text_len": len(doc.body), "payload_sha1": rec["payload_sha1"]})
    return rec


def _apply(cfg: Config, conn: psycopg.Connection, job: JobInput, done: PreparedJob,
           now: datetime) -> int:
    """Applies one job's results idempotently; returns the number of docs changed.

    Only a `completed` or `error` job is applied; a job lost meanwhile has had
    its docs put back in line. A fetched page counts whenever it is newer than
    the doc's last one. A failure counts only while the doc still waits on
    this job.
    """
    r = cfg.recrawl
    args = {"now": now, "stale": r.stale_days, "robots": r.robots_retry_days,
            "base": r.retry_base_s, "max": r.retry_max_s, "attempts": r.max_attempts,
            "pause": r.host_pause_max_s, "text_file": done.text_file, "batch": job.batch_id,
            "n": len(done.records)}
    changed = 0
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("SELECT state FROM recrawl.job WHERE batch_id = %(batch)s FOR UPDATE", args)
        if cur.fetchone() not in (("completed",), ("error",)):
            return 0
        cur.execute("CREATE TEMP TABLE r (doc_id BIGINT, outcome TEXT, http_status SMALLINT, "
                    "final_url TEXT, fail_reason TEXT, payload_sha1 TEXT, "
                    "fetched_at TIMESTAMPTZ, text_len INTEGER, lang TEXT) ON COMMIT DROP")
        with cur.copy(f"COPY r ({', '.join(RESULT_COLUMNS)}) FROM STDIN") as copy:
            for rec in done.records:
                copy.write_row([rec[c] for c in RESULT_COLUMNS])
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'fetched', fetched_at = r.fetched_at, "
            "http_status = r.http_status, final_url = r.final_url, fail_reason = NULL, "
            "payload_sha1 = r.payload_sha1, text_file = %(text_file)s, text_len = r.text_len, "
            "lang = r.lang, attempts = 0, "
            "due_at = r.fetched_at + make_interval(days => %(stale)s) FROM r "
            "WHERE r.outcome = 'fetched' AND d.doc_id = r.doc_id "
            "AND (d.fetched_at IS NULL OR d.fetched_at < r.fetched_at)", args)
        changed += cur.rowcount
        # Fetched here, but the doc already holds a newer page: it is fetched, not failed.
        cur.execute("UPDATE recrawl.doc d SET state = 'fetched' FROM r WHERE r.outcome = 'fetched' "
                    "AND d.doc_id = r.doc_id AND d.state = 'dispatched' "
                    "AND d.batch_id = %(batch)s", args)
        current = ("FROM r WHERE r.outcome = {} AND d.doc_id = r.doc_id "
                   "AND d.state = 'dispatched' AND d.batch_id = %(batch)s")
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'gone', http_status = r.http_status, "
            "final_url = r.final_url, fail_reason = r.fail_reason " + current.format("'gone'"),
            args)
        changed += cur.rowcount
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'failed', http_status = r.http_status, "
            "final_url = r.final_url, fail_reason = r.fail_reason, "
            "due_at = %(now)s + make_interval(days => %(robots)s) "
            + current.format("'robots'"), args)
        changed += cur.rowcount
        cur.execute(
            "UPDATE recrawl.host h SET next_at = greatest(coalesce(h.next_at, %(now)s), "
            "%(now)s + make_interval(secs => least(%(base)s, %(pause)s))) "
            "FROM (SELECT DISTINCT d.host_id FROM r JOIN recrawl.doc d USING (doc_id) "
            "WHERE r.outcome = 'throttled' AND d.state = 'dispatched' "
            "AND d.batch_id = %(batch)s) p WHERE h.host_id = p.host_id", args)
        backoff = ("state = 'failed', attempts = d.attempts + 1, due_at = %(now)s + CASE "
                   "WHEN d.attempts + 1 >= %(attempts)s THEN make_interval(days => %(stale)s) "
                   "ELSE make_interval(secs => least(%(base)s * power(2, d.attempts), %(max)s)) "
                   "END")
        cur.execute(
            f"UPDATE recrawl.doc d SET {backoff}, http_status = r.http_status, "
            "final_url = r.final_url, fail_reason = r.fail_reason "
            "FROM r WHERE r.outcome IN ('throttled', 'retry') AND d.doc_id = r.doc_id "
            "AND d.state = 'dispatched' AND d.batch_id = %(batch)s", args)
        changed += cur.rowcount
        # A doc of this job with no record at all: never expected, never left waiting.
        cur.execute(f"UPDATE recrawl.doc d SET {backoff}, fail_reason = 'no_result' "
                    "WHERE d.state = 'dispatched' AND d.batch_id = %(batch)s", args)
        changed += cur.rowcount
        cur.execute("UPDATE recrawl.host SET batch_id = NULL WHERE batch_id = %(batch)s "
                    "RETURNING host_id", args)
        freed = [h for (h,) in cur.fetchall()]
        cur.execute("SELECT DISTINCT d.host_id FROM r JOIN recrawl.doc d USING (doc_id)")
        _refresh_best_rank(cur, freed + [h for (h,) in cur.fetchall()])
        # Last, so a crash before here leaves the job to be applied again.
        cur.execute("UPDATE recrawl.job SET state = 'applied', finished_at = %(now)s, "
                    "records = %(n)s, error = NULL WHERE batch_id = %(batch)s", args)
    return changed
