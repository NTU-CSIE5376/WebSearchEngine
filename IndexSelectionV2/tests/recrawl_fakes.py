"""Stand-ins for the crawler side of the recrawl: the C10 API and the C5/C6 lane.

`ApiStub` serves `GET/POST /v1/recrawl-batches` as the crawler proposal's
C10 specifies and writes its queue files. `FakeCrawler` takes those files
like the crawler's queue consumer (oldest mtime first, delete after read) and
writes WARC + fetch log pairs as C6 specifies, from a dict of canned responses.
"""

from __future__ import annotations

import io
import ipaddress
import json
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

QUEUE_NAME = "domain_{:06d}.json"
QUEUE_RE = re.compile(r"^domain_(\d+)\.json$")


class ApiStub:
    """The crawler's recrawl-batches routes over a temporary queue root."""

    def __init__(self, queue_root: Path, processes: int, token: str = "t0ken",
                 max_files_per_dir: int = 384) -> None:
        self.queue_root, self.processes, self.token = queue_root, processes, token
        self.max_files_per_dir = max_files_per_dir   # the crawler's 429 limit (C10)
        self.forced: list[int] = []      # statuses to answer POSTs with, in order
        self.fail_get = False
        self.reject_urls: set[str] = set()   # judged invalid by the server
        self.lose_reply_s = 0.0          # write the file, then answer too late
        self.posts: list[dict] = []
        self._lock = threading.Lock()
        for p in range(processes):
            self.dir(p).mkdir(parents=True, exist_ok=True)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def dir(self, process: int) -> Path:
        return self.queue_root / f"crawler_{process:02d}"

    def queued(self) -> set[int]:
        return {int(m.group(1)) for p in range(self.processes) for n in os.listdir(self.dir(p))
                if (m := QUEUE_RE.match(n))}

    def _post(self, body: dict) -> tuple[int, dict]:
        if self.forced:
            status = self.forced.pop(0)
            return status, {"error": "forced", "rejected": []} if status == 400 else {}
        crawler, docs = body.get("crawler_id"), body.get("docs") or []
        if not (isinstance(crawler, int) and 0 <= crawler < self.processes) or not docs:
            return 400, {"error": "Invalid request body", "rejected": []}
        hosts = {urlsplit(d["url"]).hostname for d in docs}
        if len(hosts) != 1:
            return 400, {"error": "URLs of more than one hostname", "rejected": []}
        rejected = [{"doc_id": d["doc_id"], "reason": "invalid_url"} for d in docs
                    if not _valid(d["url"]) or d["url"] in self.reject_urls]
        if rejected:
            return 400, {"error": "Invalid HTTP(S) URL", "rejected": rejected}
        with self._lock:
            name = QUEUE_NAME.format(body["host_id"])
            if any((self.dir(p) / name).exists() for p in range(self.processes)):
                return 409, {}
            target = self.dir(crawler) / name
            if len(os.listdir(target.parent)) >= self.max_files_per_dir:
                return 429, {}
            tmp = target.with_name(f".{name}.{uuid.uuid4().hex}.part")
            tmp.write_text(json.dumps({
                "generated_at": datetime.now(UTC).isoformat(), "domain_id": body["host_id"],
                "batch_id": body["batch_id"], "urls": [d["url"] for d in docs],
                "doc_ids": [d["doc_id"] for d in docs],
                "recrawl_share": body["recrawl_share"]}))
            try:
                os.link(tmp, target)
            except FileExistsError:
                return 409, {}
            finally:
                tmp.unlink()
        self.posts.append(body)
        return 201, {"file": f"crawler_{crawler:02d}/{name}"}

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

            def _authorized(self) -> bool:
                if self.headers.get("Authorization") == f"Bearer {stub.token}":
                    return True
                self._reply(401, {"error": "unauthorized"})
                return False

            def do_GET(self) -> None:
                if not self._authorized():
                    return
                if stub.fail_get:
                    self._reply(502, {"error": "bad gateway"})
                    return
                self._reply(200, {"queued": sorted(stub.queued())})

            def do_POST(self) -> None:
                if not self._authorized():
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                status, payload = stub._post(body)
                if stub.lose_reply_s:
                    time.sleep(stub.lose_reply_s)
                self._reply(status, payload)

        return Handler


def _valid(url: str) -> bool:
    """C10's URL rule: used as sent, so only its scheme, host and length are checked."""
    parts = urlsplit(url)
    host = parts.hostname
    if not (parts.scheme in ("http", "https") and host and parts.username is None
            and len(url) <= 2500):
        return False
    try:
        return ipaddress.ip_address(host).is_global
    except ValueError:
        return host != "localhost" and "." in host


@dataclass(frozen=True)
class Page:
    """A canned answer: an HTTP status with a body, or a failure without a response."""

    status: int | None = 200
    body: bytes = b""
    content_type: str | None = "text/html; charset=utf-8"
    final_url: str | None = None
    fail_reason: str | None = None
    retry_after_s: float | None = None


class FakeCrawler:
    """Takes queue files and answers them from `web`, writing C6 result pairs."""

    def __init__(self, queue_root: Path, result_root: Path, web: dict[str, Page],
                 processes: int) -> None:
        self.queue_root, self.result_root, self.web = queue_root, result_root, web
        self.processes = processes
        self.seq = 0

    def take(self, now: datetime) -> int:
        """Answers every queued file; returns the number of URLs fetched."""
        fetched = 0
        for p in range(self.processes):
            d = self.queue_root / f"crawler_{p:02d}"
            files = sorted((f for f in d.glob("domain_*.json")), key=lambda f: f.stat().st_mtime)
            if not files:
                continue
            batches = []
            for f in files:
                batches.append(json.loads(f.read_text()))
                f.unlink()
            self.seq += 1
            fetched += self._write(p, batches, now)
        return fetched

    def _write(self, process: int, batches: list[dict], now: datetime) -> int:
        out = self.result_root / f"crawler_{process:02d}"
        out.mkdir(parents=True, exist_ok=True)
        base = f"recrawl-{process:02d}-{now:%Y%m%dT%H%M%S}Z-{self.seq}"
        warc_open, log_open = out / f"{base}.warc.gz.open", out / f"{base}.jsonl.open"
        lines = []
        with warc_open.open("wb") as fh:
            writer = WARCWriter(fh, gzip=True)
            writer.write_record(writer.create_warcinfo_record(f"{base}.warc.gz",
                                                              {"software": "fake"}))
            for n, batch in enumerate(batches):
                for url, doc_id in zip(batch["urls"], batch["doc_ids"], strict=True):
                    page = self.web.get(url, Page(status=None, fail_reason="dns"))
                    at = now + timedelta(seconds=n)
                    rec = {"batch_id": batch["batch_id"], "doc_id": doc_id, "source_url": url,
                           "final_url": page.final_url or (url if page.status else None),
                           "http_status": page.status, "content_type": page.content_type
                           if page.status else None, "etag": None, "last_modified": None,
                           "dispatched_at": at.isoformat(), "fetched_at": at.isoformat(),
                           "fail_reason": page.fail_reason, "retry_after_s": page.retry_after_s,
                           "warc_file": None, "warc_offset": None, "warc_length": None,
                           "payload_sha1": None}
                    if page.status is not None and 200 <= page.status < 300:
                        http = StatusAndHeaders(f"{page.status} OK", [
                            ("Content-Type", page.content_type or ""),
                            ("Content-Length", str(len(page.body)))], protocol="HTTP/1.1")
                        offset = fh.tell()
                        writer.write_record(writer.create_warc_record(
                            rec["final_url"], "response", payload=io.BytesIO(page.body),
                            http_headers=http))
                        rec.update(warc_file=f"{base}.warc.gz", warc_offset=offset,
                                   warc_length=fh.tell() - offset, payload_sha1="sha1")
                    lines.append(json.dumps(rec))
        log_open.write_text("".join(line + "\n" for line in lines))
        # C6: the WARC is renamed before its log, so a visible log has its WARC.
        warc_open.rename(out / f"{base}.warc.gz")
        log_open.rename(out / f"{base}.jsonl")
        return len(lines)
