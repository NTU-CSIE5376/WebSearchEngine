"""Stand-ins for the crawler side of the recrawl: WebCrawler PR #84's job API and workers.

`ApiStub` serves `POST /v1/manual-bulk-crawls` and `GET` / `DELETE .../{job_id}`
as PR #84 at bd40191 does. A POST canonicalizes each URL with w3lib, drops later
duplicates, routes each URL to one of `CRAWLERS` crawlers by hostname, and
creates `results/{job}/request.json` with an empty `crawler_NN/` partition per
crawler, a queue entry `pending/crawler_NN/{job}.json` per crawler, and the
manifest `pending/{job}.json` last. The job expires `ttl_days` after the POST:
it then answers 404 and is removed by `collect_expired`. A GET sums the
partitions' `progress.json` like `job_status`: completed once every partition
has a `completed_at`.
`FakeCrawler` answers queued partitions from a dict of canned pages and writes
them like `ManualBulkCrawlBatch`: `{index:05d}.json` and `.html`, then
`progress.json`, and removes its queue entry.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import shutil
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from w3lib.url import canonicalize_url


def canonical(url: str) -> str | None:
    """The API's `validate_url`: None if rejected, else the canonical spelling."""
    parts = urlsplit(url)
    host = (parts.hostname or "").rstrip(".")
    if not (parts.scheme in ("http", "https") and host and parts.username is None
            and len(url) <= 2500 and "|" not in url):
        return None
    try:
        if not ipaddress.ip_address(host).is_global:
            return None
    except ValueError:
        if "." not in host or host == "localhost":
            return None
    return canonicalize_url(url)


DEFAULT_TTL_DAYS = 14
CRAWLERS = 2


def expires_at(payload: dict) -> datetime:
    return datetime.fromisoformat(payload["expires_at"])


def crawler_of(url: str) -> int:
    """The crawler that owns a URL's hostname, as the API's routing picks one."""
    host = (urlsplit(url).hostname or "").encode()
    return int(hashlib.md5(host).hexdigest(), 16) % CRAWLERS


def partition(results: Path, job_id: str, crawler_id: int) -> Path:
    return results / job_id / f"crawler_{crawler_id:02d}"


class ApiStub:
    """The crawler's manual-bulk-crawl routes over a temporary data dir."""

    def __init__(self, root: Path) -> None:
        self.pending = root / "pending"
        self.results = root / "results"
        self.pending.mkdir(parents=True, exist_ok=True)
        self.results.mkdir(parents=True, exist_ok=True)
        self.forced: list[tuple[int, str]] = []    # (status, error) to answer POSTs with
        self.status_codes: dict[str, int] = {}     # job_id -> forced status of a GET
        self.lose_reply_s = 0.0                    # accept, then answer too late
        self.reject: set[str] = set()              # URLs judged invalid, as sent
        self.delete_codes: list[int] = []          # statuses to answer DELETEs with first
        self.clock = lambda: datetime.now(UTC)
        self.posts: list[list[str]] = []
        self.ttls: list[int] = []
        self.deletes: list[str] = []
        self._lock = threading.Lock()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def jobs(self) -> list[str]:
        """The jobs the API holds: one manifest each."""
        return sorted(p.stem for p in self.pending.glob("*.json"))

    def manifest(self, job_id: str) -> dict:
        return json.loads((self.pending / f"{job_id}.json").read_text())

    def queued(self, job_id: str) -> list[int]:
        """The crawlers that still have this job in their queue."""
        return sorted(int(p.parent.name[-2:]) for p in self.pending.glob(f"crawler_*/{job_id}.json"))

    def _post(self, body: object) -> tuple[int, dict]:
        if self.forced:
            status, error = self.forced.pop(0)
            return status, {"error": error}
        if not isinstance(body, dict) or "urls" not in body or set(body) - {"urls", "ttl_days"}:
            return 400, {"error": 'Expected an object containing "urls" and optional "ttl_days"'}
        ttl = body.get("ttl_days", DEFAULT_TTL_DAYS)
        if isinstance(ttl, bool) or not isinstance(ttl, int) or not 1 <= ttl <= 60:
            return 400, {"error": '"ttl_days" must be an integer between 1 and 60'}
        urls = body["urls"]
        if not isinstance(urls, list) or not 1 <= len(urls) <= 10000:
            return 400, {"error": '"urls" must contain 1 to 10000 URLs'}
        normalized: dict[str, None] = {}
        for index, url in enumerate(urls):
            fixed = canonical(url) if isinstance(url, str) and url not in self.reject else None
            if fixed is None:
                return 400, {"error": f"Invalid HTTP(S) URL at index {index}"}
            normalized[fixed] = None
        job_id = uuid.uuid4().hex
        created = self.clock()
        listed = list(normalized)
        payload = {"version": 1, "urls": listed, "max_concurrency_ratio": 0.5,
                   "crawler_ids": [crawler_of(u) for u in listed],
                   "result_dir": str(self.results), "created_at": created.isoformat(),
                   "ttl_days": ttl, "expires_at": (created + timedelta(days=ttl)).isoformat()}
        with self._lock:
            (self.results / job_id).mkdir()
            (self.results / job_id / "request.json").write_text(json.dumps(payload))
            for crawler_id in set(payload["crawler_ids"]):
                partition(self.results, job_id, crawler_id).mkdir()
                queue = self.pending / f"crawler_{crawler_id:02d}"
                queue.mkdir(exist_ok=True)
                (queue / f"{job_id}.json").write_text(json.dumps({"job_id": job_id}))
            (self.pending / f"{job_id}.json").write_text(json.dumps(payload))
        self.posts.append(urls)
        self.ttls.append(ttl)
        return 200, {"job_id": job_id}

    def _status(self, job_id: str) -> tuple[int, dict]:
        if job_id in self.status_codes:
            return self.status_codes[job_id], {"error": "forced"}
        path = self.pending / f"{job_id}.json"
        if not path.exists() or expires_at(payload := json.loads(path.read_text())) <= self.clock():
            return 404, {"error": "Job not found"}
        progress = []
        for crawler_id in sorted(set(payload["crawler_ids"])):
            done = partition(self.results, job_id, crawler_id) / "progress.json"
            if done.exists():
                progress.append(json.loads(done.read_text()))
        completed = (len(progress) == len(set(payload["crawler_ids"]))
                     and all(p["completed_at"] for p in progress))
        return 200, {
            "job_id": job_id,
            "status": "completed" if completed else ("running" if progress else "pending"),
            "total_urls": len(payload["urls"]),
            "completed_urls": sum(p["completed_urls"] for p in progress),
            "created_at": payload["created_at"], "ttl_days": payload["ttl_days"],
            "expires_at": payload["expires_at"],
            "completed_at": max(p["completed_at"] for p in progress) if completed else None,
            "results_url": f"/v1/manual-bulk-crawls/{job_id}/results" if completed else None}

    def delete(self, job_id: str) -> int:
        """`DELETE .../{job_id}`: removes the manifest, the queue entries and the results."""
        if self.delete_codes:
            return self.delete_codes.pop(0)
        pending = self.pending / f"{job_id}.json"
        if not pending.exists() and not (self.results / job_id).exists():
            return 404
        pending.unlink(missing_ok=True)
        for entry in self.pending.glob(f"crawler_*/{job_id}.json"):
            entry.unlink()
        shutil.rmtree(self.results / job_id, ignore_errors=True)
        self.deletes.append(job_id)
        return 204

    def collect_expired(self) -> int:
        """The API's hourly GC: deletes every job past its `expires_at`."""
        found = {p.stem: p for p in self.pending.glob("*.json")}
        found.update({d.name: d / "request.json" for d in self.results.glob("*")})
        expired = [j for j, p in found.items()
                   if expires_at(json.loads(p.read_text())) <= self.clock()]
        for job_id in expired:
            self.delete(job_id)
        return len(expired)

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:
                pass

            def _reply(self, status: int, body: dict) -> None:
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                self._reply(*stub._status(self.path.rsplit("/", 1)[-1]))

            def do_DELETE(self) -> None:
                status = stub.delete(self.path.rsplit("/", 1)[-1])
                if status == 204:
                    self.send_response(204)
                    self.end_headers()
                else:
                    self._reply(status, {"error": "Job or result file not found"
                                         if status == 404 else "Job results unavailable"})

            def do_POST(self) -> None:
                if self.headers.get("Content-Type") != "application/json":
                    self._reply(415, {"error": "Content-Type must be application/json"})
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                status, payload = stub._post(body)
                if stub.lose_reply_s:
                    time.sleep(stub.lose_reply_s)
                self._reply(status, payload)

        return Handler


@dataclass(frozen=True)
class Page:
    """A canned answer: an HTTP status with a body, or a failure without a response."""

    status: int | None = 200
    body: bytes = b""
    content_type: str = "text/html; charset=utf-8"
    final_url: str | None = None
    error: str | None = None          # exception name when there is no response


class FakeCrawler:
    """Answers queued partitions from `web` and writes them as the PR's workers do."""

    def __init__(self, stub: ApiStub, web: dict[str, Page]) -> None:
        self.stub, self.web = stub, web

    def take(self, now: datetime, hold: set[str] = frozenset()) -> int:
        """Finishes every live queued job not in `hold`; returns the number of URLs fetched."""
        fetched = 0
        for job_id in self.stub.jobs():
            if job_id not in hold and expires_at(self.stub.manifest(job_id)) > self.stub.clock():
                fetched += self.finish(job_id, now)
        return fetched

    def finish(self, job_id: str, now: datetime, crawlers: Iterable[int] | None = None) -> int:
        """Fetches this job's part on each crawler in `crawlers` (default: all still queued)."""
        payload = self.stub.manifest(job_id)
        fetched = 0
        for crawler_id in self.stub.queued(job_id) if crawlers is None else crawlers:
            fetched += self._finish_part(job_id, payload, crawler_id, now)
        return fetched

    def _finish_part(self, job_id: str, payload: dict, crawler_id: int, now: datetime) -> int:
        part = partition(self.stub.results, job_id, crawler_id)
        indices = [i for i, c in enumerate(payload["crawler_ids"]) if c == crawler_id]
        for index in indices:
            url = payload["urls"][index]
            page = self.web.get(url, Page(status=None, error="DNSLookupError"))
            item = {"url": url, "final_url": page.final_url or url, "status_code": page.status,
                    "error": page.error, "html_file": None,
                    "fetched_at": (now + timedelta(seconds=index)).isoformat()}
            if page.status is not None:
                ctype = page.content_type.split(";")[0].strip().lower()
                html = ctype in ("text/html", "application/xhtml+xml")
                item["error"] = None if 200 <= page.status < 300 else f"HTTP {page.status}"
                if not html:
                    item["error"] = item["error"] or "NonHTML content-type"
                else:
                    item["html_file"] = f"{index:05d}.html"
                    (part / item["html_file"]).write_bytes(page.body)
            (part / f"{index:05d}.json").write_text(json.dumps(item))
        (part / "progress.json").write_text(json.dumps({
            "started_at": now.isoformat(), "completed_at": now.isoformat(),
            "completed_urls": len(indices)}))
        (self.stub.pending / f"crawler_{crawler_id:02d}" / f"{job_id}.json").unlink()
        return len(indices)
