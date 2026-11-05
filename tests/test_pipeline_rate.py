from datetime import datetime, timezone

from Metric.pipeline_rate import (
    BUCKET_SEC,
    FolderEvent,
    finished_buckets,
    folder_stats,
    folder_time,
    parse_folder_event,
)

H = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc).timestamp()


def folder(ts):
    d = datetime.fromtimestamp(ts, timezone.utc)
    return f"/data/ipc/crawl_result/ingestor_03/{d:%Y%m%d}/{d:%H%M}"


def test_folder_time():
    assert folder_time("/x/ingestor_00/20261105/1200") == H
    assert folder_time("/x/ingestor_00/notadate/12") is None


def test_lag_duration_records_and_skips():
    # Worker 3 finished 11:00, then 11:10 (no gap), then 11:40 (skipped 11:20, 11:30).
    f0, f1, f2 = H - 3600, H - 3000, H - 1200
    events = [
        FolderEvent(H - 900, "3", "done", folder(f0)),           # before the bucket
        FolderEvent(H + 60, "3", "start", folder(f1)),
        FolderEvent(H + 360, "3", "done", folder(f1),
                    {"num_fetch_ok": 90, "num_fetch_fail": 10, "new_links": 500}),
        FolderEvent(H + 400, "3", "start", folder(f2)),
        FolderEvent(H + 700, "3", "done", folder(f2),
                    {"num_fetch_ok": 5, "num_fetch_fail": 0, "new_links": 7}),
    ]
    row = folder_stats(events, [int(H)], with_records=True)[int(H)]
    assert row["folders_done"] == 2
    assert row["skipped_folders"] == 2
    assert row["avg_duration_s"] == 300.0
    assert row["max_lag_min"] == round((H + 360 - f1) / 60, 2)  # f1 waited longest
    assert row["avg_lag_min"] == round(((H + 360 - f1) + (H + 700 - f2)) / 120, 2)
    assert row["records"] == 105 and row["new_links"] == 507
    assert row["workers"] == 1


def test_idle_bucket_reports_zero_not_missing():
    rows = folder_stats([], [int(H)], with_records=True)
    assert rows[int(H)]["folders_done"] == 0
    assert rows[int(H)]["records"] == 0
    assert rows[int(H)]["max_lag_min"] is None


def test_done_outside_buckets_is_ignored():
    ev = FolderEvent(H + BUCKET_SEC + 5, "1", "done", folder(H))
    assert folder_stats([ev], [int(H)], with_records=False)[int(H)]["folders_done"] == 0


def test_parse_folder_event():
    line = '{"event": "route.folder_done", "folder": "/a/crawler_01/20261105/1200", "worker_id": 1}'
    ev = parse_folder_event(H, {}, line, "route.")
    assert (ev.kind, ev.worker) == ("done", "1")
    assert parse_folder_event(H, {}, line, "ingest.") is None
    assert parse_folder_event(H, {}, "not json", "route.") is None


def test_finished_buckets_excludes_current_hour():
    assert finished_buckets(H + 1800, 2) == [int(H) - 7200, int(H) - 3600]
