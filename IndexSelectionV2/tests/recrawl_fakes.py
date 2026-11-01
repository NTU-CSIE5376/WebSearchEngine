"""A stand-in for the crawler's bulk crawl v2 API (WebCrawler bulk-crawl-v2, docs/03).

`FakeCrawlApi` serves `/v1/crawl-requests` over HTTP as the contract says: bearer tokens,
per-item ids, `skipped` for invalid URLs and ids repeated in a batch, idempotent
`batch_id`s, ids dropped while still queued, `DELETE` that cancels the queue but keeps
unacked segments, and segments of gzip JSON lines acked by name. It canonicalizes URLs
with w3lib, so a record's `url` may differ from the one sent; only `id` maps it back.
`crawl` stands for the crawler: it turns queued items into one segment of records.
"""

from __future__ import annotations

import gzip
import json
import threading
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from w3lib.url import canonicalize_url

TOKEN = "test-token"


@dataclass
class Page:
    """What the crawler gets for one URL: a page (`error` None) or an error."""

    html: str | None = "<html><head><title>t</title></head><body><p>hello text</p></body></html>"
    status: int | None = 200
    error: str | None = None


@dataclass
class Request:
    request_id: str
    name: str
    tenant: str
    expires_at: datetime
    cancelled: bool = False
    items: dict[str, dict] = field(default_factory=dict)   # id -> {url, priority, queued}
    batches: dict[str, dict] = field(default_factory=dict)
    segments: dict[str, bytes] = field(default_factory=dict)
    seq: int = 0


def canonical(url: str) -> str | None:
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.hostname == "localhost":
        return None
    return canonicalize_url(url)


class FakeCrawlApi:
    def __init__(self) -> None:
        self.tenants = {TOKEN: "isel"}
        self.requests: dict[str, Request] = {}
        self.forced: list[tuple[int, float | None]] = []   # (status, Retry-After) for POST urls
        self.reject: set[str] = set()                        # URLs judged invalid, as sent
        self.posts: list[tuple[str, str, list[str]]] = []    # (request, batch_id, ids)
        self.deletes: list[str] = []
        self.clock = lambda: datetime.now(UTC)
        self._lock = threading.Lock()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def queued(self, request_id: str) -> list[str]:
        return sorted(i for i, it in self.requests[request_id].items.items() if it["queued"])

    def _segment(self, req: Request, records: list[dict]) -> str:
        req.seq += 1
        name = f"00-{req.seq:012d}.jsonl.gz"
        req.segments[name] = gzip.compress(
            "".join(json.dumps(r) + "\n" for r in records).encode("utf-8"))
        return name

    def crawl(self, pages: dict[str, Page], request_id: str | None = None,
              now: datetime | None = None, only: set[str] | None = None) -> str | None:
        """Fetches the queued items (all or `only` these ids) into one new segment."""
        now = now or self.clock()
        with self._lock:
            req = self.requests[request_id or next(reversed(self.requests))]
            records = []
            for item_id, it in req.items.items():
                if not it["queued"] or (only is not None and item_id not in only):
                    continue
                page = pages.get(it["url"], Page(html=None, status=None, error="dns"))
                it["queued"] = False
                records.append({
                    "id": item_id, "url": it["url"], "priority": it["priority"],
                    "final_url": it["url"] if page.status else None,
                    "status_code": page.status, "error": page.error,
                    "fetched_at": now.isoformat() if page.status else None,
                    "finished_at": now.isoformat(), "html": page.html})
            return self._segment(req, records) if records else None

    def _finish_queued(self, req: Request, error: str) -> None:
        records = [{"id": i, "url": it["url"], "priority": it["priority"], "final_url": None,
                    "status_code": None, "error": error, "fetched_at": None,
                    "finished_at": self.clock().isoformat(), "html": None}
                   for i, it in req.items.items() if it["queued"]]
        for it in req.items.values():
            it["queued"] = False
        if records:
            self._segment(req, records)

    def finish_queued(self, request_id: str, error: str) -> None:
        """Ends every queued item with `error`, as an expiry does."""
        with self._lock:
            self._finish_queued(self.requests[request_id], error)

    def _status(self, req: Request) -> dict:
        queued = sum(it["queued"] for it in req.items.values())
        return {"request_id": req.request_id, "name": req.name,
                "expires_at": req.expires_at.isoformat(), "sealed": False,
                "cancelled": req.cancelled, "queued": queued, "leased": 0,
                "unacked_segments": len(req.segments),
                "unacked_bytes": sum(len(b) for b in req.segments.values()),
                "top_hosts": [], "updated_at": self.clock().isoformat()}

    def handle(self, method: str, path: str, token: str | None,
               body: object) -> tuple[int, object, dict]:
        tenant = self.tenants.get(token or "")
        if tenant is None:
            return 401, {"error": "Unauthorized"}, {}
        parts = urlsplit(path)
        segs = [s for s in parts.path.split("/") if s][2:]          # after v1/crawl-requests
        with self._lock:
            if not segs:
                if method == "POST":
                    rid = uuid.uuid4().hex
                    days = body.get("expires_days", 30)
                    self.requests[rid] = Request(rid, body["name"], tenant,
                                                 self.clock() + timedelta(days=days))
                    return 201, {"request_id": rid, "name": body["name"],
                                 "result_quota_gb": body.get("result_quota_gb", 100),
                                 "expires_at": self.requests[rid].expires_at.isoformat()}, {}
                return 200, {"requests": [self._status(r) for r in self.requests.values()
                                          if r.tenant == tenant]}, {}
            req = self.requests.get(segs[0])
            if req is None or req.tenant != tenant:
                return 404, {"error": "Request not found"}, {}
            rest = segs[1:]
            if not rest and method == "GET":
                return 200, self._status(req), {}
            if not rest and method == "DELETE":
                self.deletes.append(req.request_id)
                req.cancelled = True
                self._finish_queued(req, "cancelled")
                return 202, {"cancelled": True}, {}
            if rest == ["urls"] and method == "POST":
                return self._post_urls(req, body)
            if rest == ["segments"]:
                return 200, {"segments": [{"name": n, "bytes": len(b)}
                                          for n, b in sorted(req.segments.items())]}, {}
            if len(rest) == 2 and rest[0] == "segments":
                data = req.segments.get(rest[1])
                return (200, data, {}) if data is not None else (404, {"error": "No segment"}, {})
            if rest == ["ack"] and method == "POST":
                deleted = sum(req.segments.pop(n, None) is not None for n in body["segments"])
                return 200, {"deleted": deleted}, {}
        return 404, {"error": "Not found"}, {}

    def _post_urls(self, req: Request, body: dict) -> tuple[int, object, dict]:
        if self.forced:
            status, retry = self.forced.pop(0)
            return status, {"error": "forced"}, {"Retry-After": str(retry)} if retry else {}
        if req.cancelled:
            return 409, {"error": "Request cancelled"}, {}
        batch_id = body["batch_id"]
        if batch_id in req.batches:
            return 202, req.batches[batch_id], {}
        skipped, seen, accepted = [], set(), 0
        for index, item in enumerate(body["urls"]):
            fixed = canonical(item["url"]) if item["url"] not in self.reject else None
            if fixed is None:
                skipped.append({"index": index, "id": item["id"], "reason": "invalid"})
                continue
            if item["id"] in seen:
                skipped.append({"index": index, "id": item["id"], "reason": "duplicate"})
                continue
            seen.add(item["id"])
            old = req.items.get(item["id"])
            if old is not None and old["queued"]:
                continue                       # still queued: the earlier one stands
            req.items[item["id"]] = {"url": fixed, "priority": item["priority"], "queued": True}
            accepted += 1
        reply = {"accepted": accepted, "skipped": skipped}
        req.batches[batch_id] = reply
        self.posts.append((req.request_id, batch_id, [i["id"] for i in body["urls"]]))
        return 202, reply, {}

    def _handler(self):
        api = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _serve(self, method: str) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length)) if length else None
                auth = self.headers.get("Authorization", "")
                token = auth.removeprefix("Bearer ") if auth.startswith("Bearer ") else None
                status, payload, headers = api.handle(method, self.path, token, body)
                data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(status)
                for k, v in headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                self._serve("GET")

            def do_POST(self):
                self._serve("POST")

            def do_DELETE(self):
                self._serve("DELETE")

        return Handler
