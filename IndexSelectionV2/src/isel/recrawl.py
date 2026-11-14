"""Recrawl frontier: hands the live selection to the crawler and stores the text it fetched.

The frontier lives in selectdb's `recrawl` schema (`isel.schema`). The crawler
side is the manual-bulk-crawl job API of WebCrawler PR #84 at cc96500, the same
as at bd40191 (`docs/RECRAWL.md`): POST a URL list with a `ttl_days`, get a
`job_id`, and poll the job's status until the API reports it `completed`. Only
the API decides that: the results dir `result_root/{job_id}` exists from the
POST on, and each crawler fills its own `crawler_NN/` partition. The TTL is the
job's only deadline: past it the API stops the job and deletes its results, and
isel puts its docs back in line.

- `sync` (after each swap): loads the live run's manifest. A doc is on the
  list while `rank` is set; sync writes only `rank` and `model_run_id`.
- `plan` (loop): packs the due docs of the best-ranked ready hosts into jobs
  and POSTs them. A host has at most one batch not yet done at the crawler:
  once all its docs in a job have results, its next batch goes out in a new
  job, even while the old job waits for other hosts; a host with a 429 or
  503 among them is paused first. The crawler's
  AutoThrottle slot sets how fast a host's docs go out; isel adds no rate of
  its own. A job is recorded
  `claimed` before it is sent, so no HTTP call runs inside a transaction. Plan
  also polls each unapplied job's status and marks it `completed` with the
  API's `completed_at`. Once a job is applied, plan DELETEs its results.
- `archive` (loop): reads each completed job's directory without changing it
  and without calling the API. Worker processes extract each page's text into
  Parquet and keep a gzipped copy of the per-URL results; the main process then
  applies them idempotently.

Results map to docs by position: `request.json` lists the URLs in the order
sent, after the API canonicalizes them and drops duplicates, and the crawler
that owns each one (`crawler_ids`). Result n is `crawler_{crawler_ids[n]:02d}/
{n:05d}.json`, as the API resolves its downloads. The archive checks the order
before it trusts it (`map_results`). URLs go out percent-encoded (`wire_url`),
as the API rejects a raw `[`, `]` or `|`; a doc keeps its stored URL in
`recrawl.doc` and in its text rows.

Every frontier transaction takes one advisory lock, so plan, archive and sync
never interleave their updates and cannot deadlock.
"""

from __future__ import annotations

import contextlib
import gzip
import hashlib
import json
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
from isel.extract import extract
from isel.io import TMP_SUFFIX, commit_file, open_duckdb, sql_str, write_parquet_atomic
from isel.schedule import swap_allowed
from isel.schema import DUE_STATES, OPEN_JOB_STATES, REVISIT_RANK
from isel.urlnorm import md5_number_lower, wire_url

LOCK_CYCLE = 0x15E1_DB02      # xact lock held by every frontier transaction
LOCK_INSTANCE = {"sync": 0x15E1_DB03, "plan": 0x15E1_DB04, "archive": 0x15E1_DB05}
JOB_ID_RE = re.compile(r"^[0-9a-f]{32}$")
BAD_INDEX_RE = re.compile(r"\bat index (\d+)$")
NON_HTML = "NonHTML content-type"       # the crawler's error for a 2xx without HTML
ITEM_KEYS = frozenset({"url", "final_url", "status_code", "error", "html_file", "fetched_at"})
HTTP_STATUS = range(100, 1000)          # three digits (RFC 9110 §15); fits SMALLINT
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


def _due_tiers(select: str, host: str) -> tuple[str, str]:
    """`(SELECT {select} ...)` over the due docs of `host`: first fetches, then revisits.

    Each subquery repeats its tier's partial-index predicate, so it probes
    `doc_due_first` or `doc_due_revisit` by host. A per-host lookup without the
    `fetched_at` test matches neither index and scans all of `recrawl.doc`.
    """
    first, revisit = (f"(SELECT {select} FROM recrawl.doc d WHERE d.host_id = {host} "
                      f"AND d.rank IS NOT NULL AND d.{_DUE} AND d.fetched_at {test})"
                      for test in ("IS NULL", "IS NOT NULL"))
    return first, revisit


def _refresh_best_rank(cur: psycopg.Cursor, hosts: Iterable[int]) -> None:
    ids = sorted(set(hosts))
    if ids:
        first, revisit = _due_tiers("min(d.rank)", "h.host_id")
        cur.execute(
            f"UPDATE recrawl.host h SET best_rank = coalesce({first}, {revisit} + {REVISIT_RANK}) "
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
    # Imported here: it imports DuckDB, which starts one thread per host core on
    # import, and the archive workers import this module.
    from isel.manifest import validate_run

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
            "UPDATE recrawl.host h SET best_rank = m.r FROM (SELECT host_id, min(rank + CASE "
            f"WHEN fetched_at IS NULL THEN 0 ELSE {REVISIT_RANK} END) AS r "
            f"FROM recrawl.doc WHERE rank IS NOT NULL AND {_DUE} GROUP BY host_id) m "
            "WHERE h.host_id = m.host_id AND h.best_rank IS DISTINCT FROM m.r")
        first, revisit = _due_tiers("1", "h.host_id")
        cur.execute("UPDATE recrawl.host h SET best_rank = NULL WHERE best_rank IS NOT NULL "
                    f"AND NOT EXISTS {first} AND NOT EXISTS {revisit}")
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
    """A claimed job: its docs and, in the same order, their URLs as sent (`wire_url`)."""

    batch_id: int
    doc_ids: list[int]
    urls: list[str]


def plan_cycle(cfg: Config, conn: psycopg.Connection, now: datetime,
               api: CrawlerApi) -> dict[str, Any]:
    """One planning pass; returns counts.

    1. Recover: a job whose POST got no answer is lost after `claim_timeout_s`.
       With `revisit` on, fetched docs past `due_at` return to `pending`.
    2. Poll: every unapplied job is asked for its status every `status_poll_s`.
       A submitted job the API reports done becomes `completed`; a job the API
       no longer knows is lost (its TTL passed).
    3. Drain: a host whose docs in its job all have results is free again.
    4. Delete the results of applied jobs at the API.
    5. Stop if any job is in `error` (the API broke its contract), if a
       completed job's results are not mounted, if the API asked for a pause,
       if `max_jobs_inflight` jobs are at the crawler or wait for the archive,
       or if the text disk is short.
    6. Claim jobs, commit, then send them.
    """
    r = cfg.recrawl
    stats: dict[str, Any] = {"lost": 0, "orphans": 0, "swept": 0, "checked": 0,
                             "completed": 0, "drained": 0, "deleted": 0, "open": 0,
                             "jobs": 0, "docs": 0,
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
        if r.revisit:
            cur.execute(
                "UPDATE recrawl.doc SET state = 'pending' WHERE doc_id IN (SELECT doc_id "
                "FROM recrawl.doc WHERE state = 'fetched' AND rank IS NOT NULL "
                "AND due_at <= %s LIMIT %s) RETURNING host_id", (now, r.sweep_rows))
            swept = [h for (h,) in cur.fetchall()]
            stats["swept"] = len(swept)
            _wake(cur, swept, now, r.host_pause_max_s)
            _refresh_best_rank(cur, swept)
    _check_jobs(cfg, conn, api, now, stats)
    _drain(cfg, conn, now, stats)
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


def _drain(cfg: Config, conn: psycopg.Connection, now: datetime,
           stats: dict[str, Any]) -> None:
    """Frees each host whose docs in its open job all have a result on disk.

    A job completes only when all its URLs are done, so without this a host
    would wait for the slowest host of its job. Once freed, its next docs go out
    in a new job while the old one is still open; the crawler still holds at
    most one unfinished batch per host. The docs stay `dispatched` until the
    archive applies the job. A host with a 429 or 503 among them gets its pause
    here, in the same transaction, so the pause still lands before its next
    job; the apply leaves a host it no longer holds alone. A page is judged from
    the record alone, before text extraction, so a page the archive later fails
    to extract still counts as one here. A job whose
    `request.json` is missing or dropped duplicates (positions differ from the
    docs sent) is left to the archive.
    """
    jobs = conn.execute(
        "SELECT batch_id, job_id, doc_ids FROM recrawl.job j "
        "WHERE state IN ('submitted', 'completed') "
        "AND EXISTS (SELECT 1 FROM recrawl.host h WHERE h.batch_id = j.batch_id) "
        "ORDER BY batch_id").fetchall()
    if not jobs:
        return
    held: dict[int, dict[int, list[int]]] = {}
    for batch, host, doc_id in conn.execute(
            "SELECT d.batch_id, d.host_id, d.doc_id FROM recrawl.doc d JOIN recrawl.host h "
            "ON h.host_id = d.host_id AND h.batch_id = d.batch_id "
            "WHERE d.state = 'dispatched' AND d.batch_id = ANY(%s::bigint[])",
            ([b for b, _, _ in jobs],)):
        held.setdefault(batch, {}).setdefault(host, []).append(doc_id)
    r = cfg.recrawl
    free: list[tuple[int, int]] = []
    throttled: dict[int, bool] = {}      # host -> no page in its batch (the pause grows)
    for batch, job_id, doc_ids in jobs:
        if batch in held:
            for host, hit, paged in _drained(Path(r.result_root) / job_id, doc_ids,
                                             held[batch]):
                free.append((host, batch))
                if hit:
                    throttled[host] = not paged
    if not free:
        return
    with conn.transaction(), conn.cursor() as cur:
        _cycle_lock(cur)
        cur.execute("UPDATE recrawl.host h SET batch_id = NULL "
                    "FROM unnest(%s::bigint[], %s::bigint[]) AS u(host_id, batch_id) "
                    "WHERE h.host_id = u.host_id AND h.batch_id = u.batch_id RETURNING h.host_id",
                    tuple(map(list, zip(*free, strict=True))))
        freed = [h for (h,) in cur.fetchall()]
        paused = [h for h in freed if h in throttled]
        cur.execute(_THROTTLE.format("u.grow", "FROM unnest(%(hosts)s::bigint[], "
                                     "%(grow)s::bool[]) AS u(host_id, grow) "
                                     "WHERE h.host_id = u.host_id"),
                    {"now": now, "base": r.retry_base_s, "pause": r.host_pause_max_s,
                     "hosts": paused, "grow": [throttled[h] for h in paused]})
        cur.execute("UPDATE recrawl.host SET throttle_streak = 0 "
                    "WHERE host_id = ANY(%s::bigint[]) AND throttle_streak > 0",
                    ([h for h in freed if h not in throttled],))
        _refresh_best_rank(cur, freed)
    stats["drained"] = len(freed)


def _drained(job_dir: Path, doc_ids: list[int],
             docs: dict[int, list[int]]) -> list[tuple[int, bool, bool]]:
    """(host, any 429/503, any 200 page) per host in `docs` with all results on disk."""
    try:
        listing = json.loads((job_dir / "request.json").read_text(encoding="utf-8"))
        urls, owners = listing["urls"], listing["crawler_ids"]
    except (OSError, ValueError, KeyError, TypeError):
        return []
    if len(urls) != len(doc_ids) or len(owners) != len(urls):
        return []
    where = {d: n for n, d in enumerate(doc_ids)}
    names: dict[int, set[str]] = {}
    out = []
    for host, ids in docs.items():
        spots = [where[d] for d in ids]
        for n in spots:
            if owners[n] not in names:
                part = job_dir / f"crawler_{owners[n]:02d}"
                try:
                    names[owners[n]] = {e.name for e in os.scandir(part)}
                except OSError:
                    names[owners[n]] = set()
        if not all(f"{n:05d}.json" in names[owners[n]] for n in spots):
            continue
        try:
            items = [read_item(job_dir / f"crawler_{owners[n]:02d}", n, urls[n]) for n in spots]
        except (OSError, ValueError):
            continue                     # the archive reports a bad record
        outcomes = [classify(item)[0] for item in items]
        out.append((host, "throttled" in outcomes,
                     any(o == "fetched" and item["status_code"] == 200
                         for o, item in zip(outcomes, items, strict=True))))
    return out


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


# A batch with a 429 or 503 pauses its host. If it fetched no 200 page ({0} is true), the
# host blocks us: the pause is retry_base_s * 2^throttle_streak, at most host_pause_max_s,
# and the streak grows until the pause reaches that cap. A batch that also fetched a page
# only meets a rate limit: the pause is retry_base_s and the streak resets. Both SET
# expressions read the old streak. {1} picks the hosts.
_THROTTLE = ("UPDATE recrawl.host h SET next_at = greatest(coalesce(h.next_at, %(now)s), "
             "%(now)s + make_interval(secs => least(%(base)s * power(2, CASE WHEN {0} "
             "THEN h.throttle_streak ELSE 0 END), %(pause)s))), throttle_streak = CASE "
             "WHEN {0} THEN h.throttle_streak + CASE WHEN %(base)s * "
             "power(2, h.throttle_streak) < %(pause)s THEN 1 ELSE 0 END ELSE 0 END {1}")

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

    A host's docs never fetched go before its revisits, and `best_rank` puts hosts
    with a first fetch due before hosts with only revisits (`REVISIT_RANK`).

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
        # First fetches, then revisits, each read in rank order from its own index.
        tier = ("(SELECT doc_id, url, rank, {} AS tier FROM recrawl.doc WHERE "
                f"host_id = h.host_id AND rank IS NOT NULL AND {_DUE} AND fetched_at IS {{}} "
                "AND due_at <= %(now)s ORDER BY rank LIMIT %(n)s)")
        cur.execute(
            "SELECT h.host_id, d.doc_id, d.url FROM unnest(%(hosts)s::bigint[]) "
            "WITH ORDINALITY AS h(host_id, o) CROSS JOIN LATERAL ("
            f"{tier.format(0, 'NULL')} UNION ALL {tier.format(1, 'NOT NULL')} "
            "ORDER BY tier, rank LIMIT %(n)s) d ORDER BY h.o, d.tier, d.rank",
            {"hosts": [h for h, _ in hosts], "now": now, "n": r.host_docs_per_job})
        docs: dict[int, list[tuple[int, str]]] = {}
        for h, doc_id, url in cur.fetchall():
            docs.setdefault(h, []).append((doc_id, url))
        idle = [h for h, _ in hosts if h not in docs]
        if idle:
            # MATERIALIZED: m.n is read twice below, and an inlined m would run both
            # subqueries twice per host.
            first, revisit = _due_tiers("min(d.due_at)", "u.host_id")
            cur.execute(
                f"WITH m AS MATERIALIZED (SELECT u.host_id, least({first}, {revisit}) AS n "
                "FROM unnest(%s::bigint[]) AS u(host_id)) "
                "UPDATE recrawl.host h SET next_at = m.n, best_rank = CASE WHEN m.n IS NULL "
                "THEN NULL ELSE h.best_rank END FROM m WHERE h.host_id = m.host_id", (idle,))
        for h, _ in hosts:
            if h in docs:
                yield h, docs[h]


def _claim(cfg: Config, cur: psycopg.Cursor, now: datetime, n_jobs: int) -> list[Job]:
    """Packs ready hosts into up to `n_jobs` jobs and marks their docs `dispatched`.

    Hosts fill each job in `best_rank` order, each host's first fetches and then its
    revisits in rank order (`_ready_hosts`). The order
    across hosts does not matter: the crawler regroups a job's URLs by host.
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
        order = [d for _, docs in groups for d in docs]
        jobs.append(Job(batch, [d for d, _ in order], [wire_url(u) for _, u in order]))
    cur.executemany("INSERT INTO recrawl.job (batch_id, doc_ids, state, claimed_at) "
                    "VALUES (%s, %s, 'claimed', %s)",
                    [(j.batch_id, j.doc_ids, now) for j in jobs])
    cur.execute(
        "UPDATE recrawl.doc d SET state = 'dispatched', batch_id = u.batch_id "
        "FROM unnest(%s::bigint[], %s::bigint[]) AS u(doc_id, batch_id) "
        "WHERE d.doc_id = u.doc_id",
        ([d for j in jobs for d in j.doc_ids],
         [j.batch_id for j in jobs for _ in j.doc_ids]))
    # next_at stays as it is: the batch not yet done alone holds the host back.
    rows = [(h, j.batch_id) for j, groups in zip(jobs, packed, strict=True) for h, _ in groups]
    cur.execute(
        "UPDATE recrawl.host h SET batch_id = u.batch_id "
        "FROM unnest(%s::bigint[], %s::bigint[]) AS u(host_id, batch_id) "
        "WHERE h.host_id = u.host_id", tuple(map(list, zip(*rows, strict=True))))
    _refresh_best_rank(cur, [h for h, _ in rows])
    return jobs


def _send(cfg: Config, conn: psycopg.Connection, api: CrawlerApi, jobs: list[Job],
          stats: dict[str, Any], now: datetime) -> None:
    """POSTs claimed jobs in order and records each answer (docs/03 §3.6).

    - 200: the job is `submitted` under its `job_id`.
    - 400 "... at index N": the API rejects that URL even percent-encoded; it
      becomes `gone` and the rest is sent again, at most `max_resend` times.
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


def _days(left: int, per_day: int) -> float | None:
    return round(left / per_day, 1) if per_day else None


def capacity_report(r: RecrawlCfg, conn: psycopg.Connection, now: datetime) -> dict[str, Any]:
    """Progress of the live list, measured: docs left and docs fetched in the last day.

    No rate is assumed: the crawler's pace shows in `fetched_at`. Days left are the
    docs left over the last day's fetches, for the list and for its largest host.
    `fetched` counts every 2xx HTML; `real_text` counts the fetched 200s with at least
    `real_text_min_chars` of body that are not a shared challenge page, and `waf_202` the
    fetched 202s (AWS WAF challenges). Both only report: outcomes do not change.
    """
    rows = conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE state <> 'fetched'), "
        "count(*) FILTER (WHERE fetched_at > %s) FROM recrawl.doc "
        "WHERE rank IS NOT NULL AND state <> 'gone' GROUP BY host_id",
        (now - timedelta(days=1),)).fetchall()
    docs, left, day = (np.array(c, dtype=np.int64) for c in zip(*rows, strict=True)) \
        if rows else (np.zeros(0, dtype=np.int64),) * 3
    counts = conn.execute(
        "SELECT state, count(*), count(*) FILTER (WHERE state = 'fetched' "
        "AND http_status = 200 AND text_len >= %s AND payload_sha1 IS NOT NULL "
        "AND payload_sha1 <> ALL(%s::text[])), "
        "count(*) FILTER (WHERE state = 'fetched' AND http_status = 202) "
        "FROM recrawl.doc WHERE rank IS NOT NULL GROUP BY state",
        (r.real_text_min_chars, r.challenge_sha1)).fetchall()
    states = {state: n for state, n, _, _ in counts}
    report = {"docs": int(docs.sum()), "hosts": int(docs.size), "states": states,
              "real_text": sum(c[2] for c in counts), "waf_202": sum(c[3] for c in counts),
              "max_host_docs": int(docs.max()) if docs.size else 0,
              "left": int(left.sum()), "fetched_1d": int(day.sum()),
              "days_left": _days(int(left.sum()), int(day.sum())), "largest_left": None}
    if left.size:
        i = int(left.argmax())
        report["largest_left"] = {"left": int(left[i]), "fetched_1d": int(day[i]),
                                  "days_left": _days(int(left[i]), int(day[i]))}
    return report


def swap_report(cfg: Config, conn: psycopg.Connection, label: str | None = None,
                now: datetime | None = None) -> dict[str, Any]:
    """What a sync of run `label` (default: the live run) would change; writes nothing.

    Sync keeps every doc already in the frontier with its fetch state, so the new list
    sends only its new docs and the kept docs not yet fetched (`left`). A kept `failed`
    doc whose `due_at` falls after the next `report_period_days` (a refusal or robots
    wait, `stale_days` after its last attempt) goes out only in a later period: it is
    `kept_waiting`, not left. Live docs that the run leaves out are `dropped`. A host's
    pace is its results per day over the last `report_rate_days` of `log_root`, and
    `days_left` is its `left` over that pace, None without a recent result. The frontier
    is streamed out with one `COPY`, no sort, and joined in DuckDB.
    """
    from isel.manifest import validate_run

    r = cfg.recrawl
    at = now or datetime.now(UTC)
    if label is None:
        _, label = _live_run(conn)
    run_dir = cfg.work_dir / "full" / "selection" / f"run={label}"
    if validate_run(run_dir).get("scope") != "full":
        raise RecrawlError(f"{run_dir} is not a select-full run")
    tmp = cfg.work_dir / "duckdb_tmp"
    con = open_duckdb(cfg, tmp)
    docs_csv = tmp / "recrawl_report_docs.csv"
    started = time.monotonic()
    try:
        docs = _stage_docs(con, run_dir)
        with conn.cursor() as cur, docs_csv.open("wb") as fh, cur.copy(
                "COPY (SELECT doc_id, host_id, (rank IS NOT NULL)::int, state, "
                "(state = 'fetched' AND http_status = 200 AND text_len >= %s "
                "AND payload_sha1 IS NOT NULL AND payload_sha1 <> ALL(%s::text[]))::int, "
                "(state = 'failed' AND due_at > %s)::int "
                "FROM recrawl.doc) TO STDOUT (FORMAT csv)",
                (r.real_text_min_chars, r.challenge_sha1,
                 at + timedelta(days=r.report_period_days))) as copy:
            while block := copy.read():
                fh.write(block)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE fr AS SELECT * FROM read_csv(
            {sql_str(str(docs_csv))}, header = false, columns = {{'doc_id': 'BIGINT',
            'host_id': 'BIGINT', 'listed': 'INTEGER', 'state': 'VARCHAR',
            'is_real': 'INTEGER', 'waiting': 'INTEGER'}})""")
        since = at - timedelta(days=r.report_rate_days)
        logs = [str(p) for i in range((at.date() - since.date()).days + 1)
                for p in sorted((Path(r.log_root) / (since.date() + timedelta(days=i))
                                 .strftime("%Y%m%d")).glob("*.jsonl.gz"))]
        files = "[" + ", ".join(map(sql_str, logs)) + "]"
        results = (f"""SELECT doc_id, status_code FROM read_json({files},
            format = 'newline_delimited', columns = {{'doc_id': 'BIGINT',
            'fetched_at': 'VARCHAR', 'status_code': 'INTEGER'}})
            WHERE CAST(fetched_at AS TIMESTAMPTZ) >= {sql_str(since.isoformat())}::TIMESTAMPTZ"""
                   if logs else "SELECT NULL::BIGINT AS doc_id, NULL::INTEGER AS status_code "
                                "WHERE false")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE pace AS
            SELECT f.host_id, count(*) / {r.report_rate_days} AS per_day,
                   avg(coalesce(l.status_code = 200, false)::INT) AS ok_share
            FROM ({results}) l JOIN fr f USING (doc_id) GROUP BY f.host_id""")
        con.execute("""CREATE OR REPLACE TEMP TABLE cmp AS
            SELECT s.host_id, s.host, f.doc_id IS NULL AS is_new,
                   CASE WHEN f.waiting = 1 THEN 'waiting' ELSE f.state END AS state,
                   coalesce(f.is_real, 0) AS is_real
            FROM sync_rows s LEFT JOIN fr f USING (doc_id)""")
        con.execute("""CREATE OR REPLACE TEMP TABLE dropped AS
            SELECT f.* FROM fr f ANTI JOIN sync_rows s USING (doc_id) WHERE f.listed = 1""")
        con.execute("""CREATE OR REPLACE TEMP TABLE per_host AS
            WITH k AS (
              SELECT host_id, any_value(host) AS host, count(*) AS docs,
                     count(*) FILTER (WHERE is_new) AS new,
                     count(*) FILTER (WHERE state = 'fetched') AS kept_fetched,
                     sum(is_real) AS kept_real,
                     count(*) FILTER (WHERE state = 'waiting') AS kept_waiting,
                     count(*) FILTER (WHERE state NOT IN ('fetched', 'gone', 'waiting'))
                       AS kept_left
              FROM cmp GROUP BY host_id)
            SELECT k.*, k.new + k.kept_left AS "left", p.per_day, p.ok_share,
                   round((k.new + k.kept_left) / p.per_day, 1) AS days_left
            FROM k LEFT JOIN pace p USING (host_id)""")
        (totals,) = con.execute(f"""SELECT
              (SELECT count(*) FILTER (WHERE NOT is_new) FROM cmp),
              (SELECT count(*) FILTER (WHERE is_new) FROM cmp),
              (SELECT count(*) FILTER (WHERE state = 'fetched') FROM cmp),
              (SELECT coalesce(sum(is_real), 0) FROM cmp),
              (SELECT count(*) FILTER (WHERE state = 'waiting') FROM cmp),
              (SELECT count(*) FILTER (WHERE state NOT IN ('fetched', 'gone', 'waiting'))
               FROM cmp),
              (SELECT count(*) FROM dropped),
              (SELECT count(*) FILTER (WHERE state = 'fetched') FROM dropped),
              (SELECT coalesce(sum(is_real), 0) FROM dropped),
              coalesce(sum("left"), 0), count(*) FILTER (WHERE "left" > 0),
              count(*) FILTER (WHERE days_left > {r.report_period_days}),
              coalesce(sum("left") FILTER (WHERE days_left > {r.report_period_days}), 0),
              coalesce(sum("left") FILTER (WHERE per_day IS NULL), 0)
            FROM per_host""").fetchall()
        hosts = con.execute(f"""SELECT host, docs, new, kept_fetched, kept_real, kept_waiting,
              kept_left, "left", round(per_day, 1), round(ok_share, 3), days_left
            FROM per_host WHERE "left" > 0
            ORDER BY "left" DESC, host LIMIT {r.report_top_hosts}""").fetchall()
    finally:
        con.close()
        docs_csv.unlink(missing_ok=True)
    keys = ("kept", "new", "kept_fetched", "kept_real", "kept_waiting", "kept_left", "dropped",
            "dropped_fetched", "dropped_real", "left", "hosts_left", "hosts_over_period",
            "left_over_period", "left_without_pace")
    host_keys = ("host", "docs", "new", "kept_fetched", "kept_real", "kept_waiting",
                 "kept_left", "left", "per_day", "ok_share", "days_left")
    return {"run_label": label, "docs": docs, **dict(zip(keys, map(int, totals), strict=True)),
            "rate_days": r.report_rate_days, "period_days": r.report_period_days,
            "hosts": [{**dict(zip(host_keys, h, strict=True)),
                       "over_period": h[-1] is not None and h[-1] > r.report_period_days}
                      for h in hosts],
            "secs": round(time.monotonic() - started, 1)}


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


def _http_status(value: int | None) -> int | None:
    """The status as stored: anything but three digits is kept only in `fail_reason`."""
    return value if value in HTTP_STATUS else None


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
    # Only the type: the value is whatever integer the site sent (781, 901, ...).
    if item["status_code"] is not None and not _is_int(item["status_code"]):
        raise ValueError(f"{name}: status_code must be an integer or null")
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
                    "crawler_id": owners[n] if n is not None else None,
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
    rec.update(http_status=_http_status(item["status_code"]), final_url=item["final_url"],
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


# A failed doc retries after retry_base_s * 2^attempts, or waits stale_days once its
# attempts reach {} (an SQL expression).
_BACKOFF = ("state = 'failed', attempts = d.attempts + 1, due_at = %(now)s + CASE "
            "WHEN d.attempts + 1 >= {} THEN make_interval(days => %(stale)s) "
            "ELSE make_interval(secs => least(%(base)s * power(2, d.attempts), %(max)s)) END")
# A refusal (403 ...) almost never turns into a page on retry, so it gets fewer attempts.
_RETRY_LIMIT = ("CASE WHEN r.fail_reason = ANY(%(refusals)s::text[]) "
                "THEN %(refusal_attempts)s ELSE %(attempts)s END")


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
            "refusals": r.refusal_reasons, "refusal_attempts": r.refusal_max_attempts,
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
        # Hosts this job still holds; a drained host had its batch judged by the drain.
        throttled = ("SELECT 1 FROM r JOIN recrawl.doc d USING (doc_id) "
                     "WHERE r.outcome = 'throttled' AND d.host_id = h.host_id "
                     "AND d.state = 'dispatched' AND d.batch_id = %(batch)s")
        # The docs fetched here are no longer `dispatched`, so r alone names them.
        no_page = ("NOT EXISTS (SELECT 1 FROM r JOIN recrawl.doc d USING (doc_id) "
                   "WHERE d.host_id = h.host_id AND r.outcome = 'fetched' "
                   "AND r.http_status = 200)")
        cur.execute(_THROTTLE.format(
            no_page, f"WHERE h.batch_id = %(batch)s AND EXISTS ({throttled})"), args)
        cur.execute("UPDATE recrawl.host h SET throttle_streak = 0 WHERE h.batch_id = %(batch)s "
                    f"AND h.throttle_streak > 0 AND NOT EXISTS ({throttled})", args)
        # The host's pause alone paces a throttled doc, so it keeps its attempts.
        cur.execute(
            "UPDATE recrawl.doc d SET state = 'failed', due_at = %(now)s, "
            "http_status = r.http_status, final_url = r.final_url, fail_reason = r.fail_reason "
            + current.format("'throttled'"), args)
        changed += cur.rowcount
        cur.execute(
            f"UPDATE recrawl.doc d SET {_BACKOFF.format(_RETRY_LIMIT)}, "
            "http_status = r.http_status, final_url = r.final_url, fail_reason = r.fail_reason "
            + current.format("'retry'"), args)
        changed += cur.rowcount
        # A doc of this job with no record at all: never expected, never left waiting.
        cur.execute(f"UPDATE recrawl.doc d SET {_BACKOFF.format('%(attempts)s')}, "
                    "fail_reason = 'no_result' "
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
