"""Stand-ins for the crawler side of the recrawl: WebCrawler PR #84's job API and worker.

`ApiStub` serves `POST /v1/manual-bulk-crawls` and `GET` / `DELETE .../{job_id}`
as PR #84 at 4dce38f does: it canonicalizes each URL with w3lib, drops later
duplicates, and writes one pending file per job that expires `ttl_days` after
the POST. An expired job answers 404 and is removed by `collect_expired`.
`FakeCrawler` answers pending jobs from a dict of canned pages and publishes
each job like `ManualBulkCrawlBatch`: `{index:05d}.json` and `.html` under
`.incomplete/{job}`, then one rename to `results/{job}`.
"""

from __future__ import annotations

import ipaddress
import json
import shutil
import threading
import time
import uuid
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


def expires_at(payload: dict) -> datetime:
    return datetime.fromisoformat(payload["expires_at"])


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
        return sorted(p.stem for p in self.pending.glob("*.json"))

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
        with self._lock:
            (self.pending / f"{job_id}.json").write_text(json.dumps({
                "version": 1, "urls": list(normalized), "crawler_ids": [0] * len(normalized),
                "result_dir": str(self.results), "created_at": created.isoformat(),
                "ttl_days": ttl, "expires_at": (created + timedelta(days=ttl)).isoformat()}))
        self.posts.append(urls)
        self.ttls.append(ttl)
        return 200, {"job_id": job_id}

    def _status(self, job_id: str) -> tuple[int, dict]:
        if job_id in self.status_codes:
            return self.status_codes[job_id], {"error": "forced"}
        for path in (self.results / job_id / "status.json", self.pending / f"{job_id}.json"):
            if path.exists():
                payload = json.loads(path.read_text())
                if expires_at(payload) <= self.clock():
                    break
                return 200, {"job_id": job_id, "status": payload.get("status", "pending")}
        return 404, {"error": "Job not found"}

    def delete(self, job_id: str) -> int:
        """`DELETE .../{job_id}`: removes the pending file and both result dirs."""
        if self.delete_codes:
            return self.delete_codes.pop(0)
        pending = self.pending / f"{job_id}.json"
        dirs = [self.results / job_id, self.results / ".incomplete" / job_id]
        if not pending.exists() and not any(d.exists() for d in dirs):
            return 404
        pending.unlink(missing_ok=True)
        for d in dirs:
            shutil.rmtree(d, ignore_errors=True)
        self.deletes.append(job_id)
        return 204

    def collect_expired(self) -> int:
        """The API's hourly GC: deletes every job past its `expires_at`."""
        found = {p.stem: p for p in self.pending.glob("*.json")}
        found.update({d.name: d / "request.json" for d in self.results.glob("*")
                      if d.name != ".incomplete"})
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
    """Answers pending jobs from `web` and publishes them as the PR's worker does."""

    def __init__(self, stub: ApiStub, web: dict[str, Page]) -> None:
        self.stub, self.web = stub, web

    def take(self, now: datetime, hold: set[str] = frozenset()) -> int:
        """Finishes every live pending job not in `hold`; returns the number of URLs fetched."""
        fetched = 0
        for job_id in self.stub.jobs():
            payload = json.loads((self.stub.pending / f"{job_id}.json").read_text())
            if job_id not in hold and expires_at(payload) > self.stub.clock():
                fetched += self.finish(job_id, now)
        return fetched

    def finish(self, job_id: str, now: datetime) -> int:
        pending = self.stub.pending / f"{job_id}.json"
        payload = json.loads(pending.read_text())
        staging = self.stub.results / ".incomplete" / job_id
        staging.mkdir(parents=True)
        (staging / "request.json").write_text(json.dumps(payload))
        for index, url in enumerate(payload["urls"]):
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
                    (staging / item["html_file"]).write_bytes(page.body)
            (staging / f"{index:05d}.json").write_text(json.dumps(item))
        (staging / "status.json").write_text(json.dumps({
            "job_id": job_id, "status": "completed", "total_urls": len(payload["urls"]),
            "ttl_days": payload["ttl_days"], "expires_at": payload["expires_at"],
            "completed_at": now.isoformat(), "result_dir": str(self.stub.results / job_id)}))
        staging.rename(self.stub.results / job_id)
        pending.unlink()
        return len(payload["urls"])
