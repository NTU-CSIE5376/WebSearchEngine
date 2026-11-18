import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from live_db_common import DEFAULT_DB_RETRY_ATTEMPTS, close_pg_quietly, connect_pg, is_retryable_pg_error, sleep_before_retry
from selectdb_common import ensure_selectdb_schema, insert_model_run
from training_common import write_json


def parse_args():
    parser = argparse.ArgumentParser(description="Reset the current insel/selectdb contents before the next golden-set window.")
    parser.add_argument("--select-db-url", required=True, help="Target selectdb URL.")
    parser.add_argument("--run-label", default="", help="Logical reset label stored in model_runs.")
    parser.add_argument("--reason", default="pre_golden_reset", help="Free-form reset reason stored in selection_metrics.")
    parser.add_argument("--cutoff-datetime", default="", help="Optional UTC cutoff stored with the reset event.")
    parser.add_argument("--summary-path", type=Path, required=True, help="Output JSON summary path.")
    parser.add_argument(
        "--db-retry-attempts",
        type=int,
        default=DEFAULT_DB_RETRY_ATTEMPTS,
        help="Retry attempts when the selectdb connection drops during schema creation or reset.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.db_retry_attempts <= 0:
        raise ValueError("--db-retry-attempts must be > 0")

    ensure_selectdb_schema(args.select_db_url, db_retry_attempts=args.db_retry_attempts)
    reset_started_at = datetime.now(timezone.utc)
    run_label = args.run_label or f"reset_{reset_started_at.strftime('%Y%m%dT%H%M%SZ')}"

    previous_row_count = 0
    run_id = None

    for attempt in range(1, args.db_retry_attempts + 1):
        conn = None
        try:
            conn = connect_pg(
                args.select_db_url,
                readonly=False,
                application_name="indexselection_v1_selectdb_reset",
            )
            run_id = insert_model_run(
                conn,
                run_label=run_label,
                cutoff_datetime=args.cutoff_datetime or None,
                model_path="",
                train_metrics={},
                eval_metrics={},
                selection_metrics={
                    "event": "reset",
                    "reason": args.reason,
                    "selected_rows": 0,
                },
                source_paths={},
            )

            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM public.selected_urls_current")
                previous_row_count = int(cur.fetchone()[0])
                cur.execute("TRUNCATE TABLE public.selected_urls_current")
                cur.execute("ANALYZE public.selected_urls_current")
            conn.commit()
            break
        except Exception as exc:
            if conn is not None:
                conn.rollback()
            if not is_retryable_pg_error(exc) or attempt >= args.db_retry_attempts:
                raise
            print(f"[selectdb_reset] retry attempt={attempt + 1} error={exc}", flush=True)
            sleep_before_retry(attempt)
        finally:
            close_pg_quietly(conn)

    summary = {
        "run_label": run_label,
        "run_id": int(run_id) if run_id is not None else None,
        "reason": args.reason,
        "previous_row_count": int(previous_row_count),
        "current_row_count": 0,
        "cutoff_datetime": args.cutoff_datetime or None,
        "summary_path": str(args.summary_path),
        "reset_started_at": reset_started_at.isoformat(),
        "db_retry_attempts": int(args.db_retry_attempts),
    }
    write_json(args.summary_path, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
