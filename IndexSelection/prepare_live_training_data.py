import argparse
import json
import math
from pathlib import Path

from live_db_common import (
    DEFAULT_DB_RETRY_ATTEMPTS,
    DEFAULT_LIVE_WORKER_COUNT,
    estimate_crawler_total_rows,
    export_crawler_negatives_hash_chunk,
    export_metric_urls,
    parse_db_datetime,
)
from training_common import write_json


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export live metricdb/crawlerdb train-test sources for IndexSelection_v1 without exporting the full crawler corpus first."
    )
    parser.add_argument("--metric-db-url", required=True, help="Live metricdb URL.")
    parser.add_argument("--crawler-db-url", required=True, help="Live crawlerdb URL.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory used for exported CSVs and summaries.")
    parser.add_argument(
        "--train-batches",
        type=int,
        default=10,
        help="Number of newest non-test metric batches kept for training. Defaults to the latest 10 batches.",
    )
    parser.add_argument(
        "--test-batches",
        type=int,
        default=0,
        help="Number of newest metric batches reserved for test. Defaults to 0 for train-only mode.",
    )
    parser.add_argument(
        "--negative-sample-target",
        type=int,
        default=1_000_000,
        help="Approximate negative rows per split. Used to derive the hash chunk count from estimated crawler rows.",
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        default=DEFAULT_LIVE_WORKER_COUNT,
        help="Parallel crawler shard workers. Defaults to 16 to match WebCrawler shard fan-out.",
    )
    parser.add_argument(
        "--db-retry-attempts",
        type=int,
        default=DEFAULT_DB_RETRY_ATTEMPTS,
        help="Retry attempts when metricdb/crawlerdb connections drop mid-run.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Deterministic seed used to choose the negative hash chunk id.",
    )
    return parser.parse_args()


def resolve_negative_hash_chunk(total_rows: int, target_rows: int, seed: int) -> tuple[int, int, float]:
    if target_rows <= 0:
        raise ValueError("--negative-sample-target must be > 0")
    safe_total_rows = max(0, int(total_rows))
    chunk_count = max(1, math.ceil(safe_total_rows / target_rows)) if safe_total_rows > 0 else 1
    chunk_id = abs(int(seed)) % chunk_count
    estimated_rows_per_chunk = (safe_total_rows / chunk_count) if safe_total_rows > 0 else 0.0
    return chunk_count, chunk_id, estimated_rows_per_chunk


def main():
    args = parse_args()
    if args.worker_count <= 0:
        raise ValueError("--worker-count must be > 0")
    if args.db_retry_attempts <= 0:
        raise ValueError("--db-retry-attempts must be > 0")
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    metric_train_csv = output_dir / "metric_url_train.csv"
    metric_test_csv = output_dir / "metric_url_test.csv" if args.test_batches > 0 else None
    metric_summary_path = output_dir / "metric_url_train_test_summary.json"

    metric_summary, train_positive_urls, test_positive_urls = export_metric_urls(
        args.metric_db_url,
        metric_train_csv,
        metric_test_csv,
        train_batches=args.train_batches,
        test_batches=args.test_batches,
        summary_path=metric_summary_path,
        db_retry_attempts=args.db_retry_attempts,
    )

    cutoff_dt = parse_db_datetime(metric_summary["cutoff_datetime"]) if metric_summary["cutoff_datetime"] else None

    estimated_total_rows = estimate_crawler_total_rows(
        args.crawler_db_url,
        db_retry_attempts=args.db_retry_attempts,
    )
    negative_chunk_count, negative_chunk_id, estimated_rows_per_chunk = resolve_negative_hash_chunk(
        estimated_total_rows,
        args.negative_sample_target,
        args.seed,
    )

    train_neg_csv = output_dir / "crawler_negative_train.csv"
    test_neg_csv = output_dir / "crawler_negative_test.csv" if args.test_batches > 0 else None
    train_neg_summary_path = output_dir / "crawler_negative_train_summary.json"
    test_neg_summary_path = output_dir / "crawler_negative_test_summary.json" if args.test_batches > 0 else None

    train_neg_summary = export_crawler_negatives_hash_chunk(
        args.crawler_db_url,
        train_neg_csv,
        cutoff_datetime=cutoff_dt,
        newer_or_equal=False,
        target_rows=args.negative_sample_target,
        exclude_urls=set(train_positive_urls),
        chunk_count=negative_chunk_count,
        chunk_id=negative_chunk_id,
        summary_path=train_neg_summary_path,
        worker_count=args.worker_count,
        db_retry_attempts=args.db_retry_attempts,
    )
    test_neg_summary = None
    if args.test_batches > 0 and cutoff_dt is not None and test_neg_csv is not None and test_neg_summary_path is not None:
        test_neg_summary = export_crawler_negatives_hash_chunk(
            args.crawler_db_url,
            test_neg_csv,
            cutoff_datetime=cutoff_dt,
            newer_or_equal=True,
            target_rows=args.negative_sample_target,
            exclude_urls=set(test_positive_urls),
            chunk_count=negative_chunk_count,
            chunk_id=negative_chunk_id,
            summary_path=test_neg_summary_path,
            worker_count=args.worker_count,
            db_retry_attempts=args.db_retry_attempts,
        )

    summary = {
        "metric": metric_summary,
        "crawler_negative_train": train_neg_summary,
        "crawler_negative_test": test_neg_summary,
        "estimated_crawler_total_rows": int(estimated_total_rows),
        "negative_sampling_method": "hash_chunk",
        "negative_hash_partition": {
            "chunk_count": int(negative_chunk_count),
            "chunk_id": int(negative_chunk_id),
            "estimated_rows_per_chunk": float(estimated_rows_per_chunk),
            "hash_rule": "abs(hash(url)) % chunk_count == chunk_id",
        },
        "worker_count": int(args.worker_count),
        "db_retry_attempts": int(args.db_retry_attempts),
        "paths": {
            "metric_train_csv": str(metric_train_csv),
            "metric_test_csv": str(metric_test_csv) if metric_test_csv is not None else None,
            "crawler_negative_train_csv": str(train_neg_csv),
            "crawler_negative_test_csv": str(test_neg_csv) if test_neg_csv is not None else None,
        },
    }
    summary_path = output_dir / "prepare_live_training_data_summary.json"
    write_json(summary_path, summary)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
