"""Hourly throughput and lag of each crawl pipeline stage, read from
Loki and stored in metricdb.pipeline_stage_rate for the Superset dashboard.

    crawler  ->  router  ->  ingestor  (+ extractor, which reads the same folders)

Every stage hands work on in 10-minute folders
(/data/ipc/crawl_result/<stage>_NN/<YYYYMMDD>/<HHMM>), and the router,
ingestor and extractor log `*.folder_start` / `*.folder_done` for each folder.
From those events, per hour and stage:

    folders_done     folders finished in the hour (healthy: ~96 = 16 workers x 6)
    avg/max_lag_min  done time minus the folder's own time (healthy: ~20-30 min;
                     crawl_result is deleted after ~8 h, so lag near 480 = data loss)
    avg_duration_s   folder_start -> folder_done (must stay < 600 s to keep up)
    skipped_folders  10-minute folders a worker never finished (deleted before
                     it got to them, or never written)
    records          crawler: request.end events; ingestor: crawl results ingested
    new_links        ingestor: newly inserted urls
    errors           ERROR log lines of the stage's service

Run hourly; each run recomputes the last --hours finished hours and upserts
them, so late log lines are picked up on the next run.

    python3 -m Metric.pipeline_rate --metric_db_url 172.16.191.1:5433 --hours 2
    python3 -m Metric.pipeline_rate --hours 24 --dry-run     # print, no DB write
"""
from __future__ import annotations

import json
import os
import time
from argparse import ArgumentParser
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

import requests

BUCKET_SEC = 3600
# Stages hand work on in 10-minute folders.
FOLDER_SEC = 600
# How far back to look for the folder_start / previous folder_done that a
# bucket's folder_done events pair with.
LOOKBACK_SEC = 6 * 3600
LOKI_PAGE = 5000
DEFAULT_LOKI_URL = "http://172.16.191.2:3100"

# stage -> (Loki service label, event prefix)
FOLDER_STAGES = {
    "router": ("router", "route."),
    "ingestor": ("ingestor", "ingest."),
    "extractor": ("extractor", "extract."),
}
STAGES = ("crawler",) + tuple(FOLDER_STAGES)


@dataclass
class FolderEvent:
    ts: float
    worker: str
    kind: str  # "start" | "done"
    folder: str
    fields: dict = field(default_factory=dict)


def folder_time(folder: str) -> float | None:
    """Epoch seconds of a .../<YYYYMMDD>/<HHMM> folder, or None."""
    parts = folder.rstrip("/").split("/")
    if len(parts) < 2:
        return None
    try:
        dt = datetime.strptime(parts[-2] + parts[-1], "%Y%m%d%H%M")
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc).timestamp()


def bucket_of(ts: float) -> int:
    return int(ts // BUCKET_SEC) * BUCKET_SEC


def empty_row() -> dict:
    return {
        "records": None,
        "new_links": None,
        "folders_done": 0,
        "avg_lag_min": None,
        "max_lag_min": None,
        "avg_duration_s": None,
        "skipped_folders": 0,
        "errors": 0,
        "workers": 0,
    }


def folder_stats(events: list[FolderEvent], buckets: list[int], with_records: bool) -> dict[int, dict]:
    """Aggregate folder events into one row per bucket.

    `events` may start well before the first bucket (LOOKBACK_SEC) so that a
    folder_done can be paired with its folder_start and with the same worker's
    previous folder_done. Only done events inside `buckets` are counted.
    """
    wanted = set(buckets)
    rows = {b: empty_row() for b in buckets}
    lags: dict[int, list[float]] = defaultdict(list)
    durs: dict[int, list[float]] = defaultdict(list)
    workers: dict[int, set[str]] = defaultdict(set)
    started: dict[tuple[str, str], float] = {}
    prev_done: dict[str, float] = {}  # worker -> folder time of last done folder

    for ev in sorted(events, key=lambda e: e.ts):
        if ev.kind == "start":
            started[(ev.worker, ev.folder)] = ev.ts
            continue
        ftime = folder_time(ev.folder)
        b = bucket_of(ev.ts)
        if b in wanted:
            row = rows[b]
            row["folders_done"] += 1
            workers[b].add(ev.worker)
            if ftime is not None:
                lags[b].append((ev.ts - ftime) / 60.0)
            start = started.get((ev.worker, ev.folder))
            if start is not None:
                durs[b].append(ev.ts - start)
            prev = prev_done.get(ev.worker)
            if prev is not None and ftime is not None:
                row["skipped_folders"] += max(0, int(round((ftime - prev) / FOLDER_SEC)) - 1)
            if with_records:
                f = ev.fields
                row["records"] = (row["records"] or 0) + int(f.get("num_fetch_ok", 0) or 0) \
                    + int(f.get("num_fetch_fail", 0) or 0)
                row["new_links"] = (row["new_links"] or 0) + int(f.get("new_links", 0) or 0)
        if ftime is not None:
            prev_done[ev.worker] = max(ftime, prev_done.get(ev.worker, ftime))
        started.pop((ev.worker, ev.folder), None)

    for b, row in rows.items():
        if lags[b]:
            row["avg_lag_min"] = round(sum(lags[b]) / len(lags[b]), 2)
            row["max_lag_min"] = round(max(lags[b]), 2)
        if durs[b]:
            row["avg_duration_s"] = round(sum(durs[b]) / len(durs[b]), 1)
        row["workers"] = len(workers[b])
        if with_records and row["records"] is None:
            row["records"] = 0
            row["new_links"] = 0
    return rows


class Loki:
    def __init__(self, url: str, timeout: float = 120.0):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.http = requests.Session()

    def lines(self, expr: str, start: float, end: float):
        """Yield (ts_seconds, labels, line) oldest first."""
        start_ns, end_ns = int(start * 1e9), int(end * 1e9)
        while True:
            r = self.http.get(
                f"{self.url}/loki/api/v1/query_range",
                params={"query": expr, "start": start_ns, "end": end_ns,
                        "limit": LOKI_PAGE, "direction": "forward"},
                timeout=self.timeout,
            )
            r.raise_for_status()
            rows = sorted(
                (int(ts), s["stream"], line)
                for s in r.json()["data"]["result"]
                for ts, line in s["values"]
            )
            for ts, labels, line in rows:
                yield ts / 1e9, labels, line
            if len(rows) < LOKI_PAGE:
                return
            start_ns = rows[-1][0] + 1

    def bucket_counts(self, expr_1h: str, buckets: list[int]) -> dict[int, dict[str, float]]:
        """Run a `...[1h]` metric query once per bucket (an instant query at the
        bucket's end covers (end - 1h, end]); returns bucket -> {series -> value}.
        One hour at a time keeps each query small: a single 24 h range over
        crawler request.end (~1M lines/h) makes Loki drop the connection."""
        out: dict[int, dict[str, float]] = defaultdict(dict)
        for b in buckets:
            for attempt in range(3):
                try:
                    r = self.http.get(
                        f"{self.url}/loki/api/v1/query",
                        params={"query": expr_1h, "time": b + BUCKET_SEC},
                        timeout=self.timeout,
                    )
                    r.raise_for_status()
                    break
                except requests.RequestException:
                    if attempt == 2:
                        raise
                    time.sleep(2 ** attempt)
            for series in r.json()["data"]["result"]:
                key = ",".join(f"{v}" for _, v in sorted(series["metric"].items()))
                out[b][key] = float(series["value"][1])
        return out


def parse_folder_event(ts: float, labels: dict, line: str, prefix: str) -> FolderEvent | None:
    try:
        payload = json.loads(line)
    except ValueError:
        return None
    event = payload.get("event") or labels.get("event") or ""
    if event == prefix + "folder_start":
        kind = "start"
    elif event == prefix + "folder_done":
        kind = "done"
    else:
        return None
    folder = payload.get("folder")
    if not folder:
        return None
    worker = str(payload.get("worker_id", labels.get("worker_id", "")))
    return FolderEvent(ts, worker, kind, folder, payload)


def collect(loki: Loki, buckets: list[int]) -> list[dict]:
    """One row per (bucket, stage) for the given finished buckets."""
    start, end = buckets[0], buckets[-1] + BUCKET_SEC
    per_stage: dict[str, dict[int, dict]] = {}

    for stage, (service, prefix) in FOLDER_STAGES.items():
        expr = f'{{service="{service}"}} |~ "{prefix.replace(".", "[.]")}folder_(start|done)"'
        events = [
            ev for ev in (
                parse_folder_event(ts, labels, line, prefix)
                for ts, labels, line in loki.lines(expr, start - LOOKBACK_SEC, end)
            ) if ev is not None
        ]
        per_stage[stage] = folder_stats(events, buckets, with_records=(stage == "ingestor"))

    crawler = {b: empty_row() for b in buckets}
    fetched = loki.bucket_counts(
        'sum(count_over_time({service="crawler", event="request.end"}[1h]))', buckets,
    )
    for b in buckets:
        crawler[b]["records"] = int(sum(fetched.get(b, {}).values()))
        crawler[b]["folders_done"] = None
        crawler[b]["skipped_folders"] = None
        crawler[b]["workers"] = None
    per_stage["crawler"] = crawler

    services = "|".join(["crawler"] + [svc for svc, _ in FOLDER_STAGES.values()])
    errors = loki.bucket_counts(
        f'sum by (service) (count_over_time({{service=~"{services}", level="ERROR"}}[1h]))',
        buckets,
    )
    service_to_stage = {"crawler": "crawler", **{svc: st for st, (svc, _) in FOLDER_STAGES.items()}}
    for b, by_service in errors.items():
        for svc, n in by_service.items():
            stage = service_to_stage.get(svc)
            if stage and b in per_stage[stage]:
                per_stage[stage][b]["errors"] = int(n)

    rows = []
    for stage in STAGES:
        for b in buckets:
            rows.append({
                "bucket_start": datetime.fromtimestamp(b, timezone.utc),
                "stage": stage,
                **per_stage[stage][b],
            })
    return rows


def finished_buckets(now: float, hours: int) -> list[int]:
    end = bucket_of(now)  # the current hour is still filling up
    return [end - BUCKET_SEC * i for i in range(max(1, hours), 0, -1)]


def upsert(db_url: str, rows: list[dict]) -> None:
    from sqlalchemy import create_engine
    from sqlalchemy.dialects.postgresql import insert

    from Database.MetricModels import PipelineStageRate

    engine = create_engine(db_url, pool_pre_ping=True)
    PipelineStageRate.__table__.create(engine, checkfirst=True)
    table = PipelineStageRate.__table__
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        for chunk_start in range(0, len(rows), 500):
            chunk = [dict(r, updated_at=now) for r in rows[chunk_start:chunk_start + 500]]
            stmt = insert(table).values(chunk)
            stmt = stmt.on_conflict_do_update(
                index_elements=["bucket_start", "stage"],
                set_={c: stmt.excluded[c] for c in chunk[0] if c not in ("bucket_start", "stage")},
            )
            conn.execute(stmt)
    engine.dispose()


def main(argv=None):
    ap = ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--metric_db_url", default="172.16.191.1:5433", help="metricdb host:port")
    ap.add_argument("--loki_url", default=os.environ.get("LOKI_URL", DEFAULT_LOKI_URL))
    ap.add_argument("--hours", type=int, default=2,
                    help="recompute this many finished hours")
    ap.add_argument("--dry-run", action="store_true", help="print rows instead of writing")
    args = ap.parse_args(argv)

    t0 = time.time()
    buckets = finished_buckets(time.time(), args.hours)
    rows = collect(Loki(args.loki_url), buckets)
    if args.dry_run:
        for r in rows:
            print(json.dumps({**r, "bucket_start": r["bucket_start"].isoformat()}))
    else:
        upsert(f"postgresql+psycopg2://metric:metric@{args.metric_db_url}/metricdb", rows)
    print(f"pipeline_rate: {len(rows)} rows for {len(buckets)} buckets "
          f"({datetime.fromtimestamp(buckets[0], timezone.utc):%m-%d %H:%M} .. "
          f"{datetime.fromtimestamp(buckets[-1], timezone.utc):%m-%d %H:%M} UTC) "
          f"in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
