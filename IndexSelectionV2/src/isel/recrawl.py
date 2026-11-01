"""Recrawl client: hands the live list to the crawler API v2 and stores the text it fetched.

The crawler decides when and how fast each host is fetched (WebCrawler bulk crawl v2,
`docs/RECRAWL.md`). isel decides what to fetch and when a doc is due again. It keeps no
database for this: every piece of state is a file under `recrawl.root` with one writer.

- `request.json` (push): the crawl request of the live run.
- `due/{request}/due-{ts}.parquet` (push): the docs to send, by rank; `.done` once sent.
- `push/{request}/{ts}-{n}.parquet` (push): the docs of batch n and when it was sent,
  written before the POST, so a doc is in flight from the moment it may be at the crawler.
- `outcome/fetch_date=D/*.parquet` (pull; push for URLs the API skipped as invalid):
  one row per result.
- `text/fetch_date=D/*.parquet` (pull): the page text, the input of the text index.

Whether a doc is due comes from the live run's manifest and these files alone
(`compute_due`). Result rows may repeat (a segment read twice, a compaction cut short);
readers take distinct (doc_id, request_id, outcome, finished_at), and the text index the
newest row of each doc, so a repeat changes nothing.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import multiprocessing
import shutil
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from isel.config import Config, RecrawlCfg
from isel.extract import extract
from isel.io import (
    atomic_write_json,
    copy_atomic,
    open_duckdb,
    read_json,
    sql_str,
    utc_id,
    write_parquet_atomic,
)
from isel.manifest import validate_run
from isel.pg import next_allowed_start

_TS = pa.timestamp("us", tz="UTC")
# One row per fetched page, the input of the inverted index (docs/RECRAWL.md). A doc may
# appear in several files; the row with the latest `fetched_at` wins.
TEXT_SCHEMA = pa.schema([
    ("doc_id", pa.uint64()), ("source", pa.string()), ("url", pa.string()),
    ("final_url", pa.string()), ("fetched_at", _TS),
    ("http_status", pa.int16()), ("lang", pa.string()), ("title", pa.string()),
    ("description", pa.string()), ("body", pa.string()), ("text_len", pa.int32()),
    ("payload_sha1", pa.string())])
OUTCOME_SCHEMA = pa.schema([
    ("doc_id", pa.uint64()), ("request_id", pa.string()), ("outcome", pa.string()),
    ("fail_reason", pa.string()), ("http_status", pa.int16()), ("final_url", pa.string()),
    ("fetched_at", _TS), ("finished_at", _TS), ("payload_sha1", pa.string()),
    ("text_len", pa.int32()), ("lang", pa.string())])
PUSH_SCHEMA = pa.schema([("doc_id", pa.uint64()), ("pushed_at", _TS)])
RECORD_KEYS = frozenset({"id", "url", "final_url", "status_code", "error", "fetched_at",
                         "finished_at", "html"})
GONE_ERRORS = frozenset({"non_html", "http_404", "http_410"})
REQUEUE_ERRORS = frozenset({"expired", "cancelled"})
_UINT64 = 1 << 64
_EMPTY = {
    "outcome": "SELECT NULL::UBIGINT AS doc_id, NULL::VARCHAR AS request_id, "
               "NULL::VARCHAR AS outcome, NULL::TIMESTAMPTZ AS finished_at WHERE false",
    "push": "SELECT NULL::UBIGINT AS doc_id, NULL::TIMESTAMPTZ AS pushed_at WHERE false",
}


class RecrawlError(RuntimeError):
    """Raised when the API or the files break the contract."""


@dataclass(frozen=True)
class Layout:
    """The recrawl files under `recrawl.root`."""

    root: Path

    @property
    def request_file(self) -> Path:
        return self.root / "request.json"

    @property
    def outcome(self) -> Path:
        return self.root / "outcome"

    @property
    def text(self) -> Path:
        return self.root / "text"

    def due(self, request_id: str) -> Path:
        return self.root / "due" / request_id

    def push(self, request_id: str) -> Path:
        return self.root / "push" / request_id


def layout(cfg: Config) -> Layout:
    return Layout(Path(cfg.recrawl.root))


@dataclass(frozen=True)
class LiveRun:
    run_id: int
    label: str
    run_dir: Path


def live_run(cfg: Config, conn: psycopg.Connection) -> LiveRun:
    """The live run in selectdb and its select-full output under `work_dir`."""
    rows = conn.execute("SELECT id, run_label FROM public.model_runs WHERE status = 'live'"
                        ).fetchall()
    if len(rows) != 1:
        raise RecrawlError(f"expected one live run, found {len(rows)}")
    run_id, label = rows[0]
    return LiveRun(run_id, label, cfg.work_dir / "full" / "selection" / f"run={label}")


def run_loop(cycle: Callable[[], dict[str, Any]], once: bool,
             sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """Runs `cycle` until `once`; each result is one JSON line and names its own wait."""
    while True:
        result = cycle()
        if once:
            return result
        print(json.dumps(result, sort_keys=True, default=str), flush=True)
        sleep(result["wait_s"])


# ---------------------------------------------------------------- API client

@dataclass(frozen=True)
class Reply:
    status: int | None                    # None: no answer
    body: Any = None
    retry_after: float | None = None
    error: str | None = None


class CrawlApi:
    """The crawler's bulk crawl v2 routes (WebCrawler `docs/03`)."""

    def __init__(self, base_url: str, token: str, timeout_s: float) -> None:
        self.url = f"{base_url}/v1/crawl-requests"
        self._token = token
        self._timeout_s = timeout_s

    def call(self, method: str, path: str = "", body: Any = None,
             timeout_s: float | None = None, raw: bool = False) -> Reply:
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(f"{self.url}{path}", data=data, method=method)
        req.add_header("Authorization", f"Bearer {self._token}")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=timeout_s or self._timeout_s) as resp:
                payload = resp.read()
                return Reply(resp.status, payload if raw else _json(payload))
        except urllib.error.HTTPError as exc:
            with exc:
                payload = _json(exc.read())
                retry = exc.headers.get("Retry-After")
                error = payload.get("error") if isinstance(payload, dict) else None
                return Reply(exc.code, payload, float(retry) if _is_number(retry) else None,
                             error or exc.reason)
        except (OSError, ValueError) as exc:
            return Reply(None, error=f"{method} {self.url}{path}: {exc}")

    def create(self, name: str, result_quota_gb: float, expires_days: int) -> Reply:
        return self.call("POST", "", {"name": name, "result_quota_gb": result_quota_gb,
                                      "expires_days": expires_days})

    def requests(self) -> Reply:
        return self.call("GET")

    def status(self, request_id: str) -> Reply:
        return self.call("GET", f"/{request_id}")

    def delete(self, request_id: str) -> Reply:
        return self.call("DELETE", f"/{request_id}")

    def post_urls(self, request_id: str, batch_id: str, items: list[dict[str, Any]]) -> Reply:
        return self.call("POST", f"/{request_id}/urls", {"batch_id": batch_id, "urls": items})

    def segments(self, request_id: str, wait_s: float) -> Reply:
        return self.call("GET", f"/{request_id}/segments?wait={int(wait_s)}",
                         timeout_s=self._timeout_s + wait_s)

    def download(self, request_id: str, name: str) -> Reply:
        return self.call("GET", f"/{request_id}/segments/{name}", raw=True)

    def ack(self, request_id: str, names: list[str]) -> Reply:
        return self.call("POST", f"/{request_id}/ack", {"segments": names})


def _json(payload: bytes) -> Any:
    try:
        return json.loads(payload) if payload else None
    except ValueError:
        return None


def _is_number(value: str | None) -> bool:
    try:
        return value is not None and float(value) >= 0
    except ValueError:
        return False


def open_api(cfg: Config) -> CrawlApi:
    r = cfg.recrawl
    token = Path(r.api_token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise RecrawlError(f"{r.api_token_file} is empty")
    return CrawlApi(r.api_url, token, r.api_timeout_s)


# ---------------------------------------------------------------- due

def _scan(path: Path, kind: str) -> str:
    """A DuckDB relation over the Parquet files under `path`, or an empty one of that kind."""
    if next(path.glob("**/*.parquet"), None) is None:
        return f"({_EMPTY[kind]})"
    return (f"read_parquet({sql_str(str(path / '**' / '*.parquet'))}, "
            "hive_partitioning=false, union_by_name=true)")


def _seconds(value: float) -> str:
    return f"to_microseconds({int(value * 1_000_000)}::BIGINT)"


def due_query(r: RecrawlCfg, lay: Layout, manifest: Path, request_id: str,
              now: datetime) -> str:
    """The docs of the live list to send now, by rank (docs/RECRAWL.md §4)."""
    at = f"{sql_str(now.isoformat())}::TIMESTAMPTZ"
    backoff = (f"CASE WHEN a.n >= {r.max_attempts} THEN {_seconds(r.stale_days * 86400)} "
               f"ELSE to_microseconds(CAST(least({r.retry_base_s} * pow(2, a.n - 1), "
               f"{r.retry_max_s}) * 1000000 AS BIGINT)) END")
    return f"""
WITH list AS (
  SELECT doc_id, canonical_url AS url, rank
  FROM read_parquet({sql_str(str(manifest / '*.parquet'))})
  QUALIFY row_number() OVER (PARTITION BY doc_id ORDER BY rank, canonical_url) = 1),
-- Another request's requeue says only that it ended; a doc it held was due, and its
-- other rows still say so. It must not hide a result the current request got since.
out AS (SELECT DISTINCT doc_id, request_id, outcome, finished_at
        FROM {_scan(lay.outcome, 'outcome')}
        WHERE outcome <> 'requeue' OR request_id = {sql_str(request_id)}),
last AS (SELECT doc_id, arg_max(outcome, finished_at) AS outcome, max(finished_at) AS at
         FROM out GROUP BY doc_id),
ok AS (SELECT doc_id, max(finished_at) AS at FROM out WHERE outcome = 'fetched'
       GROUP BY doc_id),
attempts AS (SELECT o.doc_id, count(*) AS n FROM out o LEFT JOIN ok USING (doc_id)
             WHERE o.outcome = 'retry' AND (ok.at IS NULL OR o.finished_at > ok.at)
             GROUP BY o.doc_id),
pushed AS (SELECT doc_id, max(pushed_at) AS at FROM {_scan(lay.push(request_id), 'push')}
           GROUP BY doc_id),
inflight AS (
  SELECT p.doc_id FROM pushed p WHERE NOT EXISTS (
    SELECT 1 FROM out o WHERE o.doc_id = p.doc_id AND o.request_id = {sql_str(request_id)}
      AND o.finished_at >= p.at - {_seconds(r.clock_skew_s)}))
SELECT l.doc_id, l.url, l.rank
FROM list l
LEFT JOIN last ON last.doc_id = l.doc_id
LEFT JOIN attempts a ON a.doc_id = l.doc_id
ANTI JOIN inflight i ON i.doc_id = l.doc_id
WHERE last.outcome IS NULL OR last.outcome = 'requeue'
   OR (last.outcome = 'fetched' AND last.at + {_seconds(r.stale_days * 86400)} <= {at})
   OR (last.outcome = 'robots' AND last.at + {_seconds(r.robots_retry_days * 86400)} <= {at})
   OR (last.outcome = 'retry' AND last.at + {backoff} <= {at})
ORDER BY l.rank, l.doc_id"""


def compute_due(cfg: Config, lay: Layout, live: LiveRun, request_id: str,
                now: datetime) -> tuple[Path, int]:
    """Writes `due-{ts}.parquet` for the request; returns it and its row count."""
    path = lay.due(request_id) / f"due-{utc_id(now)}.parquet"
    con = open_duckdb(cfg, lay.root / "duckdb_tmp")
    try:
        rows = copy_atomic(con, due_query(cfg.recrawl, lay, live.run_dir / "manifest",
                                          request_id, now), path)
    finally:
        con.close()
    return path, rows


# ---------------------------------------------------------------- push

def _wait(stats: dict[str, Any], seconds: float, **flags: Any) -> dict[str, Any]:
    return {**stats, **flags, "wait_s": max(1.0, float(seconds))}


def _ensure_request(cfg: Config, lay: Layout, api: CrawlApi, live: LiveRun,
                    stats: dict[str, Any], now: datetime) -> str | None:
    """The request of the live run, replaced for a new live run or a day before expiry.

    An expired request answers 409 to every batch, so it must not run out.
    """
    r = cfg.recrawl
    current = read_json(lay.request_file)
    if current and current["model_run_id"] == live.run_id:
        renew = (datetime.fromisoformat(current["created_at"])
                 + timedelta(days=r.expires_days - 1))
        if now < renew:
            return current["request_id"]
        stats["renewed"] = True
    meta = validate_run(live.run_dir)
    if meta.get("scope") != "full":
        raise RecrawlError(f"{live.run_dir} is not a select-full run")
    if current:
        reply = api.delete(current["request_id"])
        if reply.status not in (202, 404):
            stats["api_error"] = f"DELETE {current['request_id']}: {reply.status} {reply.error}"
            return None
        stats["cancelled"] = current["request_id"]
    reply = api.create(f"isel-run-{live.run_id}", r.result_quota_gb, r.expires_days)
    if reply.status != 201:
        if reply.status in (None, 503) or (reply.status or 0) >= 500:
            stats["api_error"] = f"POST create: {reply.status} {reply.error}"
            return None
        raise RecrawlError(f"POST {api.url} answered {reply.status}: {reply.error}")
    request_id = reply.body["request_id"]
    atomic_write_json(lay.request_file, {"request_id": request_id, "model_run_id": live.run_id,
                                         "run_label": live.label, "created_at": now.isoformat()})
    stats["created"] = request_id
    return request_id


def _batch_no(path: Path) -> int:
    return int(path.stem.rsplit("-", 1)[1])


def push_cycle(cfg: Config, lay: Layout, api: CrawlApi, live: LiveRun,
               clock: Callable[[], datetime] = lambda: datetime.now(UTC)) -> dict[str, Any]:
    """Sends the current due file batch by batch; computes the next one when it is time.

    Stops for a guard window, a 503 or a lost answer, and names how long to wait.
    """
    r = cfg.recrawl
    stats: dict[str, Any] = {"batches": 0, "docs": 0, "invalid": 0, "duplicate": 0}
    now = clock()
    request_id = _ensure_request(cfg, lay, api, live, stats, now)
    if request_id is None:
        return _wait(stats, r.api_backoff_s, paused=True)
    dues = sorted(lay.due(request_id).glob("due-*.parquet"))
    due = dues[-1] if dues else None
    if due is not None and due.with_suffix(".done").exists():
        made = datetime.strptime(due.stem.removeprefix("due-"), "%Y%m%dT%H%M%S%fZ"
                                 ).replace(tzinfo=UTC)
        ready = made + timedelta(hours=r.due_interval_h)
        if now < ready:
            return _wait(stats, (ready - now).total_seconds(), idle=True)
        due = None
    if due is None:
        due, stats["due"] = compute_due(cfg, lay, live, request_id, now)
        for old in dues:                  # sent in full; their push files stay
            old.unlink()
            old.with_suffix(".done").unlink(missing_ok=True)
    ts = due.stem.removeprefix("due-")
    pushes = lay.push(request_id)
    sent = [_batch_no(p) for p in pushes.glob(f"{ts}-*.parquet")]
    start = max(sent) if sent else 0      # the last batch may lack its answer: send it again
    batches = pq.ParquetFile(due).iter_batches(batch_size=r.batch_urls,
                                               columns=["doc_id", "url", "rank"])
    for n, batch in enumerate(batches):
        if n < start:
            continue
        begin = next_allowed_start(clock(), r.api_timeout_s, cfg.guard)
        if begin > clock():
            return _wait(stats, (begin - clock()).total_seconds(), guarded=True)
        stop = _send(cfg, lay, api, request_id, f"{ts}-{n:06d}", batch, clock(), stats)
        if stop is not None:
            return stop
    due.with_suffix(".done").touch()
    return _wait(stats, 1.0, sent=ts)


def _send(cfg: Config, lay: Layout, api: CrawlApi, request_id: str, batch_id: str,
          batch: pa.RecordBatch, now: datetime, stats: dict[str, Any]) -> dict[str, Any] | None:
    """POSTs one batch; returns the cycle's result when sending must stop."""
    r = cfg.recrawl
    ids = batch.column("doc_id").to_pylist()
    push_file = lay.push(request_id) / f"{batch_id}.parquet"
    # A batch sent again keeps its first time: its docs may already have results since.
    if not push_file.exists():
        write_parquet_atomic(pa.table({"doc_id": pa.array(ids, pa.uint64()),
                                       "pushed_at": pa.array([now] * len(ids), _TS)}),
                             push_file)
    items = [{"id": str(d), "url": u, "priority": int(k)}
             for d, u, k in zip(ids, batch.column("url").to_pylist(),
                                batch.column("rank").to_pylist(), strict=True)]
    reply = api.post_urls(request_id, batch_id, items)
    if reply.status == 202:
        skipped = reply.body.get("skipped", [])
        invalid = [ids[s["index"]] for s in skipped if s["reason"] == "invalid"]
        if invalid:
            write_parquet_atomic(_outcomes([{
                "doc_id": d, "request_id": request_id, "outcome": "gone",
                "fail_reason": "invalid_url", "finished_at": now} for d in invalid]),
                lay.outcome / f"fetch_date={now:%Y%m%d}" / f"push-{request_id}-{batch_id}.parquet")
        stats["batches"] += 1
        stats["docs"] += reply.body["accepted"]
        stats["invalid"] += len(invalid)
        stats["duplicate"] += len(skipped) - len(invalid)
        return None
    if reply.status is None or reply.status == 503 or reply.status >= 500:
        stats["api_error"] = f"POST {batch_id}: {reply.status} {reply.error}"
        return _wait(stats, reply.retry_after or r.api_backoff_s, paused=True)
    # Refused for good: the batch never reached the crawler, so its docs are not in flight.
    push_file.unlink(missing_ok=True)
    raise RecrawlError(f"POST {api.url}/{request_id}/urls answered {reply.status}: "
                       f"{reply.error}")


def run_pusher(cfg: Config, api: CrawlApi, live: Callable[[], LiveRun], once: bool,
               sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    lay = layout(cfg)
    return run_loop(lambda: push_cycle(cfg, lay, api, live()), once, sleep)


# ---------------------------------------------------------------- pull

def classify(error: str | None, html: str | None) -> tuple[str, str | None]:
    """Maps a record's `error` to (outcome, fail_reason) (docs/RECRAWL.md §5)."""
    if error is None:
        return ("fetched", None) if html else ("retry", "no_html")
    if error in GONE_ERRORS:
        return "gone", error
    if error == "robots":
        return "robots", error
    if error in REQUEUE_ERRORS:
        return "requeue", error
    return "retry", error


def _timestamp(value: Any, key: str) -> datetime:
    if not isinstance(value, str):
        raise RecrawlError(f"{key} must be an ISO 8601 string")
    at = datetime.fromisoformat(value)
    if at.tzinfo is None:
        raise RecrawlError(f"{key} has no UTC offset")
    return at


def read_record(line: str) -> dict[str, Any]:
    """Parses and type-checks one segment line; raises RecrawlError on a contract break."""
    rec = json.loads(line)
    if not isinstance(rec, dict) or not set(rec) >= RECORD_KEYS:
        raise RecrawlError("a record lacks a contract key")
    doc_id = rec["id"]
    if not (isinstance(doc_id, str) and doc_id.isdecimal() and int(doc_id) < _UINT64):
        raise RecrawlError(f"record id {doc_id!r} is not a doc_id")
    status = rec["status_code"]
    if status is not None and not (isinstance(status, int) and not isinstance(status, bool)
                                   and 100 <= status <= 599):
        raise RecrawlError("status_code must be an HTTP status or null")
    for key in ("url", "final_url", "error", "html"):
        if rec[key] is not None and not isinstance(rec[key], str):
            raise RecrawlError(f"{key} must be a string or null")
    rec["doc_id"] = int(doc_id)
    rec["fetched_at"] = (None if rec["fetched_at"] is None
                         else _timestamp(rec["fetched_at"], "fetched_at"))
    rec["finished_at"] = _timestamp(rec["finished_at"], "finished_at")
    return rec


def _outcomes(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist([{c: row.get(c) for c in OUTCOME_SCHEMA.names} for row in rows],
                                schema=OUTCOME_SCHEMA)


def _extract_all(max_body_chars: int, pages: list[tuple[int, str]]) -> list[tuple]:
    """(doc_id, doc or None, sha1) per page; runs in a worker process."""
    out = []
    for doc_id, html in pages:
        payload = html.encode("utf-8")
        try:
            doc = extract(payload, "utf-8", max_body_chars)
        except Exception:      # hostile HTML must not stop the pull
            doc = None
        out.append((doc_id, doc, hashlib.sha1(payload).hexdigest()))
    return out


def _extract(r: RecrawlCfg, pages: list[tuple[int, str]]) -> dict[int, tuple]:
    if r.archive_workers == 1 or len(pages) < 2 * r.archive_workers:
        return {d: (doc, sha) for d, doc, sha in _extract_all(r.max_body_chars, pages)}
    size = -(-len(pages) // r.archive_workers)
    chunks = [pages[i:i + size] for i in range(0, len(pages), size)]
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(len(chunks), mp_context=context) as pool:
        done = pool.map(_extract_all, [r.max_body_chars] * len(chunks), chunks)
        return {d: (doc, sha) for part in done for d, doc, sha in part}


def apply_segments(cfg: Config, lay: Layout, request_id: str,
                   segments: list[tuple[str, bytes]], now: datetime) -> dict[str, int]:
    """Writes the text and outcome rows of these segments; same segments, same files."""
    records = [(name, read_record(line)) for name, data in segments
               for line in gzip.decompress(data).decode("utf-8").splitlines() if line]
    pages = [(rec["doc_id"], rec["html"]) for _, rec in records
             if rec["error"] is None and rec["html"]]
    docs = _extract(cfg.recrawl, pages)
    texts, outcomes = [], []
    for name, rec in records:
        outcome, reason = classify(rec["error"], rec["html"])
        row = {"doc_id": rec["doc_id"], "request_id": request_id, "outcome": outcome,
               "fail_reason": reason, "http_status": rec["status_code"],
               "final_url": rec["final_url"], "fetched_at": rec["fetched_at"],
               "finished_at": rec["finished_at"]}
        if outcome == "fetched":
            doc, sha = docs[rec["doc_id"]]
            if doc is None:
                row.update(outcome="retry", fail_reason="extract_error")
            else:
                row.update(payload_sha1=sha, text_len=len(doc.body), lang=doc.lang)
                texts.append({"doc_id": rec["doc_id"], "source": f"{request_id}/{name}",
                              "url": rec["url"], "final_url": rec["final_url"],
                              "fetched_at": rec["fetched_at"] or rec["finished_at"],
                              "http_status": rec["status_code"], "lang": doc.lang,
                              "title": doc.title, "description": doc.description,
                              "body": doc.body, "text_len": len(doc.body),
                              "payload_sha1": sha})
        outcomes.append(row)
    key = hashlib.sha1("\n".join(sorted(n for n, _ in segments)).encode()).hexdigest()[:20]
    name = f"pull-{request_id}-{key}.parquet"
    day = f"fetch_date={now:%Y%m%d}"
    if texts:
        write_parquet_atomic(pa.Table.from_pylist(texts, schema=TEXT_SCHEMA),
                             lay.text / day / name)
    # The outcome file goes last: a doc is fetched only once its text is on disk.
    write_parquet_atomic(_outcomes(outcomes), lay.outcome / day / name)
    return {"records": len(records), "texts": len(texts)}


class Puller:
    """Reads closed segments of every request of this tenant, stores them, acks them."""

    def __init__(self, cfg: Config, api: CrawlApi,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.cfg = cfg
        self.lay = layout(cfg)
        self.api = api
        self.clock = clock
        self.first_seen: dict[tuple[str, str], float] = {}

    def cycle(self, force: bool, now: datetime | None = None) -> dict[str, Any]:
        r = self.cfg.recrawl
        stats: dict[str, Any] = {"segments": 0, "records": 0, "texts": 0}
        self.lay.root.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(self.lay.root).free < r.min_free_gb * 1e9:
            return _wait(stats, r.long_poll_s, disk_full=True)
        reply = self.api.requests()
        if reply.status != 200:
            stats["api_error"] = f"GET requests: {reply.status} {reply.error}"
            return _wait(stats, r.api_backoff_s, paused=True)
        current = (read_json(self.lay.request_file) or {}).get("request_id")
        ids = [q["request_id"] for q in reply.body["requests"]
               if q["unacked_segments"] or q["request_id"] == current]
        for request_id in ids:
            wait = r.long_poll_s if request_id == current and len(ids) == 1 else 0
            listed = self.api.segments(request_id, wait)
            if listed.status != 200:
                stats["api_error"] = f"GET segments {request_id}: {listed.status} {listed.error}"
                return _wait(stats, r.api_backoff_s, paused=True)
            names, size, full = [], 0, False
            for seg in listed.body["segments"]:
                # A segment unpacks to several times its size; bound what one batch holds.
                if names and (len(names) == r.pull_batch_segments
                              or size + seg["bytes"] > r.pull_batch_mb * 1e6):
                    full = True
                    break
                names.append(seg["name"])
                size += seg["bytes"]
            if not names:
                continue
            seen = min(self.first_seen.setdefault((request_id, n), self.clock()) for n in names)
            if not (force or full or self.clock() - seen >= r.pull_batch_s):
                continue
            segments = []
            for name in names:
                got = self.api.download(request_id, name)
                if got.status != 200:
                    stats["api_error"] = f"GET {name}: {got.status} {got.error}"
                    return _wait(stats, r.api_backoff_s, paused=True)
                segments.append((name, got.body))
            done = apply_segments(self.cfg, self.lay, request_id, segments,
                                  now or datetime.now(UTC))
            acked = self.api.ack(request_id, names)
            if acked.status != 200:
                # The files are written; the segments come again and rewrite the same rows.
                stats["api_error"] = f"POST ack {request_id}: {acked.status} {acked.error}"
                return _wait(stats, r.api_backoff_s, paused=True)
            for name in names:
                self.first_seen.pop((request_id, name), None)
            stats["segments"] += len(names)
            stats["records"] += done["records"]
            stats["texts"] += done["texts"]
        return _wait(stats, 1.0 if stats["segments"] else min(r.pull_batch_s, r.long_poll_s))


def run_puller(cfg: Config, api: CrawlApi, once: bool,
               sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    puller = Puller(cfg, api)
    return run_loop(lambda: puller.cycle(force=once), once, sleep)


# ---------------------------------------------------------------- compact

def _old_files(root: Path, before: str) -> list[Path]:
    return sorted(p for p in root.glob("fetch_date=*/*.parquet")
                  if p.parent.name.removeprefix("fetch_date=") < before)


def _files_sql(files: Iterable[Path]) -> str:
    return "[" + ", ".join(sql_str(str(p)) for p in files) + "]"


def compact(cfg: Config, now: datetime) -> dict[str, Any]:
    """Rewrites the files of days before yesterday into one file each; due stays the same.

    Outcome keeps each doc's latest `fetched` row and every row after it, which is all
    `due_query` reads. Text keeps each doc's newest row. Push files of requests other than
    the current one go: only the current request's pushes decide what is in flight.
    """
    r = cfg.recrawl
    lay = layout(cfg)
    before = f"{now - timedelta(days=1):%Y%m%d}"
    stats: dict[str, Any] = {"outcome_files": 0, "text_files": 0, "push_dirs": 0, "due_dirs": 0}
    con = open_duckdb(cfg, lay.root / "duckdb_tmp")
    try:
        for kind, root, query in (
            ("outcome", lay.outcome,
             "SELECT * FROM read_parquet({f}, hive_partitioning=false, union_by_name=true) "
             "QUALIFY row_number() OVER (PARTITION BY doc_id, request_id, outcome, "
             "finished_at) = 1 AND finished_at >= coalesce(max(CASE WHEN outcome = 'fetched' "
             "THEN finished_at END) OVER (PARTITION BY doc_id), '-infinity'::TIMESTAMPTZ)"),
            ("text", lay.text,
             "SELECT * EXCLUDE (filename) FROM read_parquet({f}, filename=true, "
             "hive_partitioning=false, union_by_name=true) "
             "QUALIFY row_number() OVER (PARTITION BY doc_id ORDER BY fetched_at DESC, "
             "filename DESC) = 1"),
        ):
            files = _old_files(root, before)
            if len(files) < 2:
                continue
            need = sum(p.stat().st_size for p in files) + r.min_free_gb * 1e9
            if shutil.disk_usage(lay.root).free < need:
                stats[f"{kind}_skipped"] = "disk"
                continue
            day = max(p.parent.name for p in files)
            final = root / day / f"compact-{utc_id(now)}.parquet"
            copy_atomic(con, query.format(f=_files_sql(files)), final)
            for p in files:
                p.unlink()
            stats[f"{kind}_files"] = len(files)
    finally:
        con.close()
    current = (read_json(lay.request_file) or {}).get("request_id")
    for kind in ("push", "due"):
        for d in (lay.root / kind).glob("*"):
            if d.is_dir() and d.name != current:
                for p in d.glob("*"):
                    p.unlink()
                d.rmdir()
                stats[f"{kind}_dirs"] = stats.get(f"{kind}_dirs", 0) + 1
    return stats


# ---------------------------------------------------------------- report

def report(cfg: Config, api: CrawlApi, live: LiveRun, now: datetime) -> dict[str, Any]:
    """Progress of the live list: the crawler's counts and the outcome files' view."""
    r = cfg.recrawl
    lay = layout(cfg)
    current = read_json(lay.request_file) or {}
    out: dict[str, Any] = {"request": current}
    if current.get("request_id"):
        reply = api.status(current["request_id"])
        out["crawler"] = reply.body if reply.status == 200 else {"error": reply.error}
    con = open_duckdb(cfg, lay.root / "duckdb_tmp")
    try:
        fresh = f"{sql_str((now - timedelta(days=r.stale_days)).isoformat())}::TIMESTAMPTZ"
        day = f"{sql_str((now - timedelta(days=1)).isoformat())}::TIMESTAMPTZ"
        rows = con.execute(f"""
WITH list AS (SELECT DISTINCT doc_id FROM read_parquet(
                {sql_str(str(live.run_dir / 'manifest' / '*.parquet'))})),
out AS (SELECT DISTINCT doc_id, outcome, finished_at FROM {_scan(lay.outcome, 'outcome')}
        WHERE outcome <> 'requeue' OR request_id = {sql_str(current.get('request_id', ''))}),
last AS (SELECT doc_id, arg_max(outcome, finished_at) AS outcome, max(finished_at) AS at
         FROM out GROUP BY doc_id)
SELECT count(*), count(*) FILTER (WHERE last.outcome IS NULL),
       count(*) FILTER (WHERE last.outcome = 'fetched' AND last.at >= {fresh}),
       count(*) FILTER (WHERE last.outcome = 'retry'),
       count(*) FILTER (WHERE last.outcome = 'gone'),
       count(*) FILTER (WHERE last.outcome = 'robots'),
       (SELECT count(*) FROM out WHERE outcome = 'fetched' AND finished_at >= {day})
FROM list LEFT JOIN last USING (doc_id)""").fetchone()
    finally:
        con.close()
    docs, never, fetched, retry, gone, robots, fetched_1d = (int(v) for v in rows)
    left = docs - fetched - gone
    out.update(docs=docs, never_fetched=never, fetched_fresh=fetched, retry=retry, gone=gone,
               robots=robots, left=left, fetched_1d=fetched_1d,
               days_left=round(left / fetched_1d, 1) if fetched_1d else None)
    return out
