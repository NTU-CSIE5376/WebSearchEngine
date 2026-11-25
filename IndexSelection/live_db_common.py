import csv
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from itertools import count
from pathlib import Path

import psycopg2
from psycopg2 import InterfaceError, OperationalError

from training_common import ensure_parent, write_json


METRIC_EXPORT_FIELDNAMES = [
    "split",
    "metric_url_id",
    "query_id",
    "query_keyword",
    "query_geo",
    "query_frequency",
    "query_tags",
    "batch_id",
    "batch_created_at",
    "url",
    "rank",
    "is_discovered",
    "is_crawled",
    "is_indexed",
    "is_ranked",
    "shard_id",
]

NEGATIVE_EXPORT_FIELDNAMES = ["url", "first_seen", "source_table", "sample_attempt"]
DEFAULT_DB_RETRY_ATTEMPTS = 30
DEFAULT_LIVE_WORKER_COUNT = 16
DB_RETRY_BASE_DELAY_SEC = 0.2
DB_RETRY_MAX_DELAY_SEC = 60.0


def normalize_pg_url(db_url: str) -> str:
    text = "".join((db_url or "").strip().split())
    if not text:
        raise ValueError("database URL is empty")
    return re.sub(r"^postgresql\+psycopg2://", "postgresql://", text)


def close_pg_quietly(conn) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass


def is_retryable_pg_error(exc: Exception) -> bool:
    return isinstance(exc, (OperationalError, InterfaceError))


def sleep_before_retry(attempt: int, max_delay: float = DB_RETRY_MAX_DELAY_SEC) -> None:
    delay = DB_RETRY_BASE_DELAY_SEC * (2 ** max(0, attempt - 1))
    time.sleep(min(delay, max_delay))


def _retry_range(db_retry_attempts: int):
    if db_retry_attempts <= 0:
        return count(1)
    return range(1, db_retry_attempts + 1)


def build_shard_groups(shard_start: int, shard_end: int, worker_count: int) -> list[list[int]]:
    if shard_end < shard_start:
        raise ValueError("shard_end must be >= shard_start")
    shards = list(range(shard_start, shard_end + 1))
    if not shards:
        return []
    effective_workers = min(max(1, int(worker_count)), len(shards))
    base_size, remainder = divmod(len(shards), effective_workers)
    groups = []
    cursor = 0
    for worker_idx in range(effective_workers):
        group_size = base_size + (1 if worker_idx < remainder else 0)
        groups.append(shards[cursor:cursor + group_size])
        cursor += group_size
    return [group for group in groups if group]


def connect_pg(
    db_url: str,
    *,
    readonly: bool = False,
    application_name: str = "indexselection_v1",
    isolation_level: str | None = None,
):
    conn = psycopg2.connect(
        normalize_pg_url(db_url),
        application_name=application_name,
        connect_timeout=15,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=5,
        keepalives_count=5,
    )
    session_kwargs = {"autocommit": False}
    if readonly:
        session_kwargs["readonly"] = True
    if isolation_level is not None:
        session_kwargs["isolation_level"] = isolation_level
    conn.set_session(**session_kwargs)
    return conn


def parse_db_datetime(value) -> datetime:
    if value is None:
        raise ValueError("timestamp is empty")
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def list_metric_batches(
    metric_db_url: str,
    *,
    db_retry_attempts: int = DEFAULT_DB_RETRY_ATTEMPTS,
) -> list[dict]:
    rows = None
    for attempt in _retry_range(db_retry_attempts):
        conn = None
        try:
            conn = connect_pg(metric_db_url, readonly=True, application_name="indexselection_v1_metric_batches")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, created_at
                    FROM public.metric_batches
                    ORDER BY created_at ASC, id ASC
                    """
                )
                rows = cur.fetchall()
            break
        except Exception as exc:
            if not is_retryable_pg_error(exc):
                raise
            print(f"[metric_batches] retry attempt={attempt + 1} error={exc}", flush=True)
            sleep_before_retry(attempt)
        finally:
            close_pg_quietly(conn)

    batches = []
    for batch_id, created_at in rows:
        batches.append(
            {
                "batch_id": int(batch_id),
                "created_at": parse_db_datetime(created_at),
            }
        )
    if not batches:
        raise ValueError("No metric_batches rows were found in metricdb")
    return batches


def assign_metric_batch_splits(
    batches: list[dict],
    *,
    train_batches: int | None = None,
    test_batches: int = 0,
) -> dict:
    if test_batches < 0:
        raise ValueError("test_batches must be >= 0")
    if train_batches is not None and train_batches <= 0:
        raise ValueError("train_batches must be > 0 when provided")
    ordered = sorted(batches, key=lambda item: (item["created_at"], item["batch_id"]))
    if test_batches > len(ordered):
        raise ValueError("test_batches cannot exceed total metric batches")

    test_slice = ordered[-test_batches:] if test_batches > 0 else []
    test_ids = {item["batch_id"] for item in test_slice}
    remaining_for_train = ordered[:-test_batches] if test_batches > 0 else ordered
    if not remaining_for_train:
        raise ValueError("No metric batches remain for training after reserving test batches")
    if train_batches is None:
        train_slice = remaining_for_train
        effective_train_batches = len(train_slice)
    else:
        effective_train_batches = min(train_batches, len(remaining_for_train))
        train_slice = remaining_for_train[-effective_train_batches:]
    train_ids = {item["batch_id"] for item in train_slice}
    cutoff_dt = min(item["created_at"] for item in test_slice) if test_slice else None

    return {
        "ordered_batches": ordered,
        "test_ids": test_ids,
        "train_ids": train_ids,
        "effective_train_batches": effective_train_batches,
        "cutoff_datetime": cutoff_dt,
    }


def open_csv_writer(path: Path, fieldnames: list[str]):
    ensure_parent(path)
    handle = open(path, "w", encoding="utf-8", newline="")
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    return handle, writer


def export_metric_urls(
    metric_db_url: str,
    train_csv_path: Path,
    test_csv_path: Path | None,
    *,
    train_batches: int | None = None,
    test_batches: int = 1,
    summary_path: Path | None = None,
    db_retry_attempts: int = DEFAULT_DB_RETRY_ATTEMPTS,
):
    batches = list_metric_batches(metric_db_url, db_retry_attempts=db_retry_attempts)
    split = assign_metric_batch_splits(
        batches,
        train_batches=train_batches,
        test_batches=test_batches,
    )
    train_ids = split["train_ids"]
    test_ids = split["test_ids"]

    counts = None
    unique_urls = None
    for attempt in _retry_range(db_retry_attempts):
        conn = None
        train_handle = None
        test_handle = None
        try:
            if train_csv_path.exists():
                train_csv_path.unlink()
            if test_csv_path is not None and test_csv_path.exists():
                test_csv_path.unlink()

            conn = connect_pg(metric_db_url, readonly=True, application_name="indexselection_v1_metric_export")
            train_handle, train_writer = open_csv_writer(train_csv_path, METRIC_EXPORT_FIELDNAMES)
            test_writer = None
            if test_csv_path is not None and test_batches > 0:
                test_handle, test_writer = open_csv_writer(test_csv_path, METRIC_EXPORT_FIELDNAMES)
            counts = {"train_rows": 0, "test_rows": 0}
            unique_urls = {"train": set(), "test": set()}

            with conn.cursor(name=f"metric_url_export_{attempt}") as cur:
                cur.itersize = 20_000
                all_ids = sorted(train_ids | test_ids)
                placeholders = ",".join(["%s"] * len(all_ids))
                query = f"""
                    SELECT
                        u.id AS metric_url_id,
                        q.id AS query_id,
                        q.keyword,
                        q.geo::text,
                        q.frequency,
                        q.tags::text,
                        b.id AS batch_id,
                        b.created_at,
                        u.url,
                        u.rank,
                        u.is_discovered,
                        u.is_crawled,
                        u.is_indexed,
                        u.is_ranked,
                        u.shard_id
                    FROM public.metric_url AS u
                    JOIN public.metric_queries AS q
                      ON q.id = u.query_id
                    JOIN public.metric_batches AS b
                      ON b.id = q.batch_id
                    WHERE b.id IN ({placeholders})
                    ORDER BY b.created_at ASC, b.id ASC, q.id ASC, u.id ASC
                """
                cur.execute(query, all_ids)
                while True:
                    rows = cur.fetchmany(cur.itersize)
                    if not rows:
                        break
                    for (
                        metric_url_id,
                        query_id,
                        keyword,
                        geo,
                        frequency,
                        tags,
                        batch_id,
                        batch_created_at,
                        url,
                        rank,
                        is_discovered,
                        is_crawled,
                        is_indexed,
                        is_ranked,
                        shard_id,
                    ) in rows:
                        batch_id_int = int(batch_id)
                        if batch_id_int in train_ids:
                            split_name = "train"
                        else:
                            split_name = "test"
                        out_row = {
                            "split": split_name,
                            "metric_url_id": metric_url_id,
                            "query_id": query_id,
                            "query_keyword": keyword or "",
                            "query_geo": geo or "",
                            "query_frequency": frequency if frequency is not None else "",
                            "query_tags": tags or "",
                            "batch_id": batch_id,
                            "batch_created_at": parse_db_datetime(batch_created_at).isoformat(),
                            "url": (url or "").strip(),
                            "rank": rank if rank is not None else "",
                            "is_discovered": is_discovered,
                            "is_crawled": is_crawled,
                            "is_indexed": is_indexed,
                            "is_ranked": is_ranked,
                            "shard_id": shard_id if shard_id is not None else "",
                        }
                        if not out_row["url"]:
                            continue
                        if split_name == "train":
                            train_writer.writerow(out_row)
                            counts["train_rows"] += 1
                        else:
                            if test_writer is None:
                                continue
                            test_writer.writerow(out_row)
                            counts["test_rows"] += 1
                        unique_urls[split_name].add(out_row["url"])
            break
        except Exception as exc:
            if not is_retryable_pg_error(exc):
                raise
            print(f"[metric_export] retry attempt={attempt + 1} error={exc}", flush=True)
            sleep_before_retry(attempt)
        finally:
            if train_handle is not None:
                train_handle.close()
            if test_handle is not None:
                test_handle.close()
            close_pg_quietly(conn)

    summary = {
        "train_csv": str(train_csv_path),
        "test_csv": str(test_csv_path) if test_csv_path is not None and test_batches > 0 else None,
        "train_batches": int(train_batches) if train_batches is not None else None,
        "effective_train_batches": int(split["effective_train_batches"]),
        "test_batches": int(test_batches),
        "cutoff_datetime": split["cutoff_datetime"].isoformat() if split["cutoff_datetime"] is not None else None,
        "batches": [
            {
                "batch_id": item["batch_id"],
                "created_at": item["created_at"].isoformat(),
                "split": "train" if item["batch_id"] in train_ids else ("test" if item["batch_id"] in test_ids else "unused"),
            }
            for item in split["ordered_batches"]
        ],
        "counts": {
            **counts,
            "train_unique_urls": len(unique_urls["train"]),
            "test_unique_urls": len(unique_urls["test"]),
            "train_test_overlap_unique_urls": len(unique_urls["train"] & unique_urls["test"]),
        },
    }
    if summary_path is not None:
        write_json(summary_path, summary)
    return summary, unique_urls["train"], unique_urls["test"]


def estimate_crawler_total_rows(
    crawler_db_url: str,
    *,
    shard_start: int = 0,
    shard_end: int = 255,
    db_retry_attempts: int = DEFAULT_DB_RETRY_ATTEMPTS,
) -> int:
    shard_names = [f"url_state_current_{shard:03d}" for shard in range(shard_start, shard_end + 1)]
    row = None
    for attempt in _retry_range(db_retry_attempts):
        conn = None
        try:
            conn = connect_pg(crawler_db_url, readonly=True, application_name="indexselection_v1_row_estimate")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(SUM(reltuples), 0)
                    FROM pg_class
                    WHERE relnamespace = 'public'::regnamespace
                      AND relname = ANY(%s)
                    """,
                    (shard_names,),
                )
                row = cur.fetchone()
            break
        except Exception as exc:
            if not is_retryable_pg_error(exc):
                raise
            print(f"[crawler_total_rows] retry attempt={attempt + 1} error={exc}", flush=True)
            sleep_before_retry(attempt)
        finally:
            close_pg_quietly(conn)
    return int(row[0] or 0)


def estimate_crawler_shard_rows(
    crawler_db_url: str,
    *,
    shard_start: int = 0,
    shard_end: int = 255,
    db_retry_attempts: int = DEFAULT_DB_RETRY_ATTEMPTS,
) -> list[tuple[int, int]]:
    shard_names = [f"url_state_current_{shard:03d}" for shard in range(shard_start, shard_end + 1)]
    rows = None
    for attempt in _retry_range(db_retry_attempts):
        conn = None
        try:
            conn = connect_pg(crawler_db_url, readonly=True, application_name="indexselection_v1_shard_row_estimates")
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT relname, COALESCE(reltuples, 0)
                    FROM pg_class
                    WHERE relnamespace = 'public'::regnamespace
                      AND relname = ANY(%s)
                    ORDER BY relname
                    """,
                    (shard_names,),
                )
                rows = cur.fetchall()
            break
        except Exception as exc:
            if not is_retryable_pg_error(exc):
                raise
            print(f"[shard_row_estimates] retry attempt={attempt + 1} error={exc}", flush=True)
            sleep_before_retry(attempt)
        finally:
            close_pg_quietly(conn)

    estimates = []
    for relname, reltuples in rows:
        shard_num = int(relname.split("_")[-1])
        estimates.append((shard_num, int(reltuples)))
    return estimates

def _export_crawler_negatives_hash_chunk_worker(
    *,
    worker_id: int,
    shard_ids: list[int],
    crawler_db_url: str,
    cutoff_datetime: datetime | None,
    comparator: str,
    chunk_count: int,
    chunk_id: int,
    fetch_size: int,
    exclude_urls: set[str],
    state: dict,
):
    local_matched_rows = 0
    local_skipped_positive = 0
    local_rows: list[dict] = []
    sample_label = f"hash_chunk_{chunk_id}_of_{chunk_count}"

    conn = None
    try:
        conn = connect_pg(
            crawler_db_url,
            readonly=True,
            application_name=f"indexselection_v1_hash_negative_w{worker_id:02d}",
        )

        for shard in shard_ids:
            table_name = f"url_state_current_{shard:03d}"
            where_clauses = ["MOD(ABS(HASHTEXT(url)::bigint), %s) = %s"]
            params = [chunk_count, chunk_id]
            if cutoff_datetime is not None:
                where_clauses.append(f"first_seen {comparator} %s")
                params.append(cutoff_datetime)

            query = f"""
                SELECT url, first_seen
                FROM public.{table_name}
                WHERE {" AND ".join(where_clauses)}
            """
            cursor_name = f"crawler_hash_chunk_{worker_id:02d}_{shard:03d}"
            with conn.cursor(name=cursor_name) as cur:
                cur.itersize = fetch_size
                cur.execute(query, params)
                while True:
                    rows = cur.fetchmany(fetch_size)
                    if not rows:
                        break
                    local_matched_rows += len(rows)
                    for url, first_seen in rows:
                        clean_url = (url or "").strip()
                        if not clean_url:
                            continue
                        if clean_url in exclude_urls:
                            local_skipped_positive += 1
                            continue

                        local_rows.append(
                            {
                                "url": clean_url,
                                "first_seen": parse_db_datetime(first_seen).isoformat(),
                                "source_table": table_name,
                                "sample_attempt": sample_label,
                            }
                        )

                        if len(local_rows) >= 10_000:
                            with state["lock"]:
                                for row in local_rows:
                                    state["writer"].writerow(row)
                                state["counts"]["written_rows"] += len(local_rows)
                            local_rows = []

            if local_rows:
                with state["lock"]:
                    for row in local_rows:
                        state["writer"].writerow(row)
                    state["counts"]["written_rows"] += len(local_rows)
                local_rows = []
    finally:
        close_pg_quietly(conn)

    return {
        "matched_rows": local_matched_rows,
        "skipped_positive": local_skipped_positive,
    }


def export_crawler_negatives_hash_chunk(
    crawler_db_url: str,
    output_csv_path: Path,
    *,
    cutoff_datetime: datetime | None,
    newer_or_equal: bool,
    target_rows: int,
    exclude_urls: set[str],
    chunk_count: int,
    chunk_id: int,
    fetch_size: int = 20_000,
    shard_start: int = 0,
    shard_end: int = 255,
    summary_path: Path | None = None,
    worker_count: int = DEFAULT_LIVE_WORKER_COUNT,
    db_retry_attempts: int = DEFAULT_DB_RETRY_ATTEMPTS,
):
    if target_rows <= 0:
        raise ValueError("target_rows must be > 0")
    if chunk_count <= 0:
        raise ValueError("chunk_count must be > 0")
    if chunk_id < 0 or chunk_id >= chunk_count:
        raise ValueError("chunk_id must satisfy 0 <= chunk_id < chunk_count")

    handle, writer = open_csv_writer(output_csv_path, NEGATIVE_EXPORT_FIELDNAMES)
    counts = {"written_rows": 0, "skipped_positive": 0}
    comparator = ">=" if newer_or_equal else "<"
    shard_groups = build_shard_groups(shard_start, shard_end, worker_count)
    matched_rows = 0

    try:
        state = {
            "lock": threading.Lock(),
            "writer": writer,
            "counts": counts,
        }
        with ThreadPoolExecutor(max_workers=len(shard_groups)) as executor:
            futures = [
                executor.submit(
                    _export_crawler_negatives_hash_chunk_worker,
                    worker_id=worker_idx,
                    shard_ids=shard_ids,
                    crawler_db_url=crawler_db_url,
                    cutoff_datetime=cutoff_datetime,
                    comparator=comparator,
                    chunk_count=chunk_count,
                    chunk_id=chunk_id,
                    fetch_size=fetch_size,
                    exclude_urls=exclude_urls,
                    state=state,
                )
                for worker_idx, shard_ids in enumerate(shard_groups)
            ]
            for future in as_completed(futures):
                result = future.result()
                matched_rows += int(result["matched_rows"])
                counts["skipped_positive"] += int(result["skipped_positive"])
    finally:
        handle.close()

    summary = {
        "output_csv": str(output_csv_path),
        "sampling_method": "hash_chunk",
        "cutoff_datetime": cutoff_datetime.isoformat() if cutoff_datetime is not None else None,
        "comparator": comparator if cutoff_datetime is not None else "all_rows",
        "target_rows": int(target_rows),
        "chunk_count": int(chunk_count),
        "chunk_id": int(chunk_id),
        "hash_sql": "MOD(ABS(HASHTEXT(url)::bigint), chunk_count) = chunk_id",
        "matched_rows": int(matched_rows),
        "written_rows": int(counts["written_rows"]),
        "skipped_positive": int(counts["skipped_positive"]),
        "worker_count": int(len(shard_groups)),
        "db_retry_attempts": int(db_retry_attempts),
    }
    if summary_path is not None:
        write_json(summary_path, summary)
    return summary


def write_manifest(path: Path, payload: dict) -> None:
    ensure_parent(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    write_json(tmp_path, payload)
    tmp_path.replace(path)
