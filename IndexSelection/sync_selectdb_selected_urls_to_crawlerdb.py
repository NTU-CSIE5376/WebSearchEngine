import argparse
import json
from pathlib import Path

from psycopg2.extras import execute_values

from live_db_common import (
    DEFAULT_DB_RETRY_ATTEMPTS,
    close_pg_quietly,
    connect_pg,
    is_retryable_pg_error,
    sleep_before_retry,
)
from training_common import write_json


DEFAULT_NUM_SHARDS = 256
STAGE_TABLE = "selectdb_selected_urls_stage"


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Notify crawlerdb that IndexSelection finished by mirroring "
            "selectdb.selected_urls_current into url_state_current shards."
        )
    )
    parser.add_argument("--crawler-db-url", required=True, help="Target crawlerdb URL.")
    parser.add_argument("--select-db-url", required=True, help="Source selectdb URL.")
    parser.add_argument("--summary-path", type=Path, required=True, help="Output JSON summary path.")
    parser.add_argument("--num-shards", type=int, default=DEFAULT_NUM_SHARDS)
    parser.add_argument(
        "--select-fetch-batch-size",
        type=int,
        default=50000,
        help="Rows fetched from selectdb per round trip.",
    )
    parser.add_argument(
        "--stage-insert-batch-size",
        type=int,
        default=50000,
        help="Rows inserted into crawlerdb stage per execute_values call.",
    )
    parser.add_argument(
        "--db-retry-attempts",
        type=int,
        default=DEFAULT_DB_RETRY_ATTEMPTS,
        help="Retry attempts when database connections drop during sync.",
    )
    return parser.parse_args()


def iter_selected_rows(select_conn, fetch_batch_size: int):
    with select_conn.cursor(name="selectdb_selected_urls_for_crawler_cursor") as cur:
        cur.itersize = fetch_batch_size
        cur.execute(
            """
            SELECT url, score, run_id, selected_at
            FROM public.selected_urls_current
            """
        )
        while True:
            rows = cur.fetchmany(fetch_batch_size)
            if not rows:
                break
            for url, score, run_id, selected_at in rows:
                if url:
                    yield (str(url), score, run_id, selected_at)


def create_stage(cur) -> None:
    cur.execute(
        f"""
        CREATE TEMP TABLE {STAGE_TABLE} (
            url TEXT PRIMARY KEY,
            score DOUBLE PRECISION,
            run_id BIGINT,
            selected_at TIMESTAMPTZ
        ) ON COMMIT DROP
        """
    )


def load_stage(crawler_conn, rows, insert_batch_size: int) -> int:
    total = 0
    with crawler_conn.cursor() as cur:
        create_stage(cur)
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= insert_batch_size:
                total += insert_stage_batch(cur, batch)
                batch.clear()
        if batch:
            total += insert_stage_batch(cur, batch)
        cur.execute(f"ANALYZE {STAGE_TABLE}")
    return total


def insert_stage_batch(cur, rows: list[tuple]) -> int:
    execute_values(
        cur,
        f"""
        INSERT INTO {STAGE_TABLE} (url, score, run_id, selected_at)
        VALUES %s
        ON CONFLICT (url) DO UPDATE SET
            score = EXCLUDED.score,
            run_id = EXCLUDED.run_id,
            selected_at = EXCLUDED.selected_at
        """,
        rows,
        page_size=min(len(rows), 10000),
    )
    return len(rows)


def matched_update_sql(shard_id: int) -> str:
    table = f"public.url_state_current_{shard_id:03d}"
    return f"""
    UPDATE {table} AS u
    SET
        is_selectdb_selected = TRUE,
        selectdb_score = s.score,
        selectdb_run_id = s.run_id,
        selectdb_selected_at = s.selected_at,
        selectdb_synced_at = NOW(),
        should_crawl = CASE
            WHEN u.selectdb_run_id IS DISTINCT FROM s.run_id THEN TRUE
            ELSE u.should_crawl
        END
    FROM {STAGE_TABLE} AS s
    WHERE u.url = s.url
    """


def clear_removed_sql(shard_id: int) -> str:
    table = f"public.url_state_current_{shard_id:03d}"
    return f"""
    UPDATE {table} AS u
    SET
        is_selectdb_selected = FALSE,
        selectdb_score = NULL,
        selectdb_run_id = NULL,
        selectdb_selected_at = NULL,
        selectdb_synced_at = NOW()
    WHERE u.is_selectdb_selected = TRUE
      AND NOT EXISTS (
        SELECT 1 FROM {STAGE_TABLE} AS s
        WHERE s.url = u.url
      )
    """


def apply_stage_to_crawlerdb(crawler_conn, num_shards: int) -> dict:
    matched_urls = 0
    cleared_urls = 0
    reopened_urls = 0

    with crawler_conn.cursor() as cur:
        for shard_id in range(num_shards):
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM public.url_state_current_{shard_id:03d} AS u
                JOIN {STAGE_TABLE} AS s ON s.url = u.url
                WHERE u.selectdb_run_id IS DISTINCT FROM s.run_id
                """
            )
            reopened_urls += int(cur.fetchone()[0])

            cur.execute(matched_update_sql(shard_id))
            matched_urls += int(cur.rowcount)

            cur.execute(clear_removed_sql(shard_id))
            cleared_urls += int(cur.rowcount)

    return {
        "matched_urls": matched_urls,
        "reopened_urls": reopened_urls,
        "cleared_urls": cleared_urls,
    }


def run_sync(
    crawler_db_url: str,
    select_db_url: str,
    *,
    num_shards: int,
    select_fetch_batch_size: int,
    stage_insert_batch_size: int,
) -> dict:
    crawler_conn = None
    select_conn = None
    try:
        crawler_conn = connect_pg(
            crawler_db_url,
            readonly=False,
            application_name="indexselection_notify_crawlerdb_selected_urls",
        )
        select_conn = connect_pg(
            select_db_url,
            readonly=True,
            application_name="indexselection_notify_crawlerdb_selectdb",
        )

        staged_urls = load_stage(
            crawler_conn,
            iter_selected_rows(select_conn, select_fetch_batch_size),
            stage_insert_batch_size,
        )
        sync_counts = apply_stage_to_crawlerdb(crawler_conn, num_shards)
        crawler_conn.commit()

        missing_urls = max(staged_urls - sync_counts["matched_urls"], 0)
        summary = {
            "selected_url_total": int(staged_urls),
            "staged_url_total": int(staged_urls),
            "missing_urls": int(missing_urls),
            "num_shards": int(num_shards),
            **{key: int(value) for key, value in sync_counts.items()},
        }
        return summary
    except Exception:
        if crawler_conn is not None:
            crawler_conn.rollback()
        raise
    finally:
        close_pg_quietly(select_conn)
        close_pg_quietly(crawler_conn)


def main():
    args = parse_args()
    if args.num_shards <= 0:
        raise ValueError("--num-shards must be > 0")
    if args.select_fetch_batch_size <= 0:
        raise ValueError("--select-fetch-batch-size must be > 0")
    if args.stage_insert_batch_size <= 0:
        raise ValueError("--stage-insert-batch-size must be > 0")
    if args.db_retry_attempts <= 0:
        raise ValueError("--db-retry-attempts must be > 0")

    summary = None
    for attempt in range(1, args.db_retry_attempts + 1):
        try:
            summary = run_sync(
                args.crawler_db_url,
                args.select_db_url,
                num_shards=args.num_shards,
                select_fetch_batch_size=args.select_fetch_batch_size,
                stage_insert_batch_size=args.stage_insert_batch_size,
            )
            break
        except Exception as exc:
            if not is_retryable_pg_error(exc) or attempt >= args.db_retry_attempts:
                raise
            print(f"[sync_selectdb_selected_urls] retry attempt={attempt + 1} error={exc}", flush=True)
            sleep_before_retry(attempt)

    write_json(args.summary_path, summary)
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
