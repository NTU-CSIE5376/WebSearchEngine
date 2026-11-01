"""A stand-in for the OpenSearch routes `isel.textindex` calls, over loopback HTTP.

`OpenSearchStub` keeps indices as dicts of documents, applies `_bulk` and `_aliases`,
and records every request as (method, path, body). Tests can make it answer the next
`_bulk` requests with an HTTP status (`bulk_http`), make the next `_bulk` fail some
items (`item_status`), skew `_count` (`count_offset`), fail the force merge on some
shards (`merge_failed`), and set the ids a search for a given text returns (`hits`).
They can also apply a request and drop its reply (`drop`), refuse a request with HTTP
500 without applying it (`fail`), hold the retention leases back (`lease_lag`), set the
node's disk (`fs`), fail shards of a search (`failed_shards`), and keep the merge task
running for some polls (`merge_polls`).
"""

from __future__ import annotations

import fnmatch
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit


class OpenSearchStub:
    """One OpenSearch node with just enough behavior for the text index."""

    def __init__(self) -> None:
        self.indices: dict[str, dict] = {}       # name -> {"docs", "body", "settings"}
        self.aliases: dict[str, set[str]] = {}    # alias -> indices
        self.requests: list[tuple[str, str, object]] = []
        self.bulk_http: list[int] = []            # statuses for the next _bulk requests
        self.item_status: list[int] = []          # per-item statuses of the next _bulk
        self.count_offset = 0
        self.merge_failed = 0                     # shards the force merge task fails on
        self.hits: dict[str, list[str]] = {}      # query text -> ids, best first
        self.drop: list[str] = []                 # path parts: apply, then close unanswered
        self.fail: list[str] = []                 # path parts: answer 500, apply nothing
        self.lease_lag = 0
        self.fs = {"total_in_bytes": 10**15, "available_in_bytes": 10**14}
        self.failed_shards = 0
        self.merge_polls = 0                      # _tasks answers "not completed" this often
        self._lock = threading.Lock()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def paths(self, method: str) -> list[str]:
        return [p for m, p, _ in self.requests if m == method]

    def _bulk(self, index: str, raw: bytes) -> tuple[int, dict]:
        if self.bulk_http:
            return self.bulk_http.pop(0), {"error": "forced"}
        lines = raw.decode().splitlines()
        items, errors = [], False
        for n, (action, source) in enumerate(zip(lines[::2], lines[1::2], strict=True)):
            status = self.item_status[n] if n < len(self.item_status) else 201
            if status < 300:
                self.indices[index]["docs"][json.loads(action)["index"]["_id"]] = (
                    json.loads(source))
                items.append({"index": {"status": status}})
            else:
                errors = True
                items.append({"index": {"status": status,
                                        "error": {"type": "forced", "reason": "forced"}}})
        self.item_status = []
        return 200, {"errors": errors, "items": items}

    def _search(self, target: str, body: dict, params: str) -> tuple[int, dict]:
        if self.failed_shards:
            if "allow_partial_search_results=false" in params:
                return 503, {"error": "search_phase_execution_exception"}
            return 200, {"took": 1, "_shards": {"failed": self.failed_shards},
                         "hits": {"hits": []}}
        names = self.aliases.get(target, {target})
        docs = {i: d for name in names for i, d in self.indices[name]["docs"].items()}
        query = body["query"]
        if "ids" in query:
            ids = [i for i in query["ids"]["values"] if i in docs]
        else:
            text = next(iter(query.values()))["query"]
            ids = [i for i in self.hits.get(text, []) if i in docs]
        return 200, {"took": 1, "hits": {"hits": [{"_id": i} for i in ids[:body["size"]]]}}

    def _route(self, method: str, path: str, query: str, raw: bytes) -> tuple[int, object]:
        parts = [unquote(p) for p in path.strip("/").split("/")]
        body = json.loads(raw) if raw and not path.endswith("_bulk") else None
        self.requests.append((method, path + (f"?{query}" if query else ""), body))
        name = parts[0]
        if method == "PUT" and len(parts) == 1:
            self.indices[name] = {"docs": {}, "body": body, "settings": {}}
            return 200, {"acknowledged": True}
        if method == "DELETE" and len(parts) == 1:
            if self.indices.pop(name, None) is None:
                return 404, {"error": "index_not_found_exception"}
            for members in self.aliases.values():
                members.discard(name)
            return 200, {"acknowledged": True}
        if parts == ["_aliases"]:
            for action in body["actions"]:
                (kind, spec), = action.items()
                members = self.aliases.setdefault(spec["alias"], set())
                (members.add if kind == "add" else members.discard)(spec["index"])
            return 200, {"acknowledged": True}
        if parts[0] == "_alias":
            members = self.aliases.get(parts[1])
            if not members:
                return 404, {"error": "alias missing"}
            return 200, {i: {"aliases": {parts[1]: {}}} for i in members}
        if parts[:2] == ["_cat", "indices"]:
            return 200, [{"index": i} for i in sorted(self.indices)
                         if fnmatch.fnmatch(i, parts[2])]
        if parts[:3] == ["_nodes", "stats", "fs"]:
            return 200, {"nodes": {"n1": {"fs": {"total": self.fs}}}}
        if parts[0] == "_tasks":
            if self.merge_polls:
                self.merge_polls -= 1
                return 200, {"completed": False}
            failed = self.merge_failed
            return 200, {"completed": True, "response": {"_shards": {
                "total": 1, "successful": 1 - failed, "failed": failed}}}
        op = parts[1] if len(parts) > 1 else ""
        if op == "_bulk":
            return self._bulk(name, raw)
        if op == "_search":
            return self._search(name, body, query)
        if op == "_mapping":
            return 200, {i: {"mappings": self.indices[i]["body"]["mappings"]}
                         for i in self.aliases.get(name, {name})}
        if op == "_count":
            return 200, {"count": len(self.indices[name]["docs"]) + self.count_offset}
        if op == "_settings":
            self.indices[name]["settings"].update(body["index"])
            return 200, {"acknowledged": True}
        if op == "_forcemerge":
            return 200, {"task": "node:1"}
        if op == "_stats" and len(parts) == 2:
            n = len(self.indices[name]["docs"])
            copy = {"seq_no": {"max_seq_no": n - 1},
                    "retention_leases": {"leases": [{"retaining_seq_no": n - self.lease_lag}]},
                    "commit": {"user_data": {"min_retained_seq_no": str(n)}}}
            return 200, {"indices": {name: {"shards": {"0": [copy]}}}}
        if op == "_stats":
            files = {"doc": {"size_in_bytes": 300}, "pos": {"size_in_bytes": 600}}
            return 200, {"indices": {name: {"primaries": {
                "store": {"size_in_bytes": 1000},
                "segments": {"count": 1, "file_sizes": files}}}}}
        if op in ("_refresh", "_flush"):
            return 200, {}
        return 400, {"error": f"stub has no route {method} {path}"}

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:
                pass

            def _serve(self) -> None:
                raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                parts = urlsplit(self.path)
                with stub._lock:
                    refused = next((f for f in stub.fail if f in parts.path), None)
                    if refused:
                        stub.fail.remove(refused)
                        stub.requests.append((self.command, parts.path, None))
                        status, body = 500, {"error": "forced"}
                    else:
                        status, body = stub._route(self.command, parts.path, parts.query, raw)
                    lost = next((d for d in stub.drop if d in parts.path), None)
                    if lost:
                        stub.drop.remove(lost)
                if lost:
                    self.close_connection = True      # applied; the client sees no reply
                    return
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_PUT = do_POST = do_DELETE = _serve

        return Handler
