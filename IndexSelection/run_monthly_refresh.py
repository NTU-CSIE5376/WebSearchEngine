import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from live_db_common import DEFAULT_DB_RETRY_ATTEMPTS, DEFAULT_LIVE_WORKER_COUNT, write_manifest
from training_common import run_python_step, write_json


KEEP_RUNS_DEFAULT = 3


def _cleanup_old_runs(run_root: Path, keep: int) -> None:
    runs_dir = run_root / "runs"
    if not runs_dir.is_dir():
        return
    entries = sorted(
        [d for d in runs_dir.iterdir() if d.is_dir()],
        key=lambda d: d.name,
    )
    if len(entries) <= keep:
        return
    for old in entries[:-keep]:
        print(f"[cleanup] removing old run: {old}", flush=True)
        shutil.rmtree(old, ignore_errors=True)


BASE_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the full live IndexSelection_v1 refresh: export -> train -> score -> load selectdb."
    )
    parser.add_argument("--metric-db-url", required=True, help="Live metricdb URL.")
    parser.add_argument("--crawler-db-url", required=True, help="Live crawlerdb URL.")
    parser.add_argument("--select-db-url", required=True, help="Target selectdb URL.")
    parser.add_argument("--run-root", type=Path, required=True, help="Base directory used to store refresh artifacts.")
    parser.add_argument("--run-label", default="", help="Optional run label. Defaults to the current UTC timestamp.")
    parser.add_argument("--train-batches", type=int, default=10, help="Number of newest metric batches kept as the positive source pool.")
    parser.add_argument("--test-batches", type=int, default=0, help="Number of newest metric batches reserved for temporal test.")
    parser.add_argument(
        "--negative-sample-target",
        type=int,
        default=1_000_000,
        help="Approximate negative rows per split via deterministic hash chunking.",
    )
    parser.add_argument("--selection-top-k", type=int, default=32_000_000, help="Number of URLs kept in selectdb.")
    parser.add_argument("--top-frac", type=float, default=0.06, help="Coverage fraction used by eval.py.")
    parser.add_argument("--random-test-frac", type=float, default=0.1, help="Randomly split this fraction from the source pool into test.")
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Base random seed.",
    )
    parser.add_argument(
        "--worker-count",
        type=int,
        default=DEFAULT_LIVE_WORKER_COUNT,
        help="Parallel shard workers for crawler negative sampling and connectorx-backed scoring.",
    )
    parser.add_argument(
        "--chunk-worker-count",
        type=int,
        default=32,
        help="Parallel batch scorers inside each shard during connectorx-backed scoring.",
    )
    parser.add_argument(
        "--db-retry-attempts",
        type=int,
        default=DEFAULT_DB_RETRY_ATTEMPTS,
        help="Retry attempts when metricdb/crawlerdb/selectdb connections drop.",
    )
    parser.add_argument("--score-batch-size", type=int, default=250_000, help="Batch size for connectorx-backed crawler scoring.")
    parser.add_argument(
        "--selection-first-seen-before",
        default="",
        help="Optional UTC cutoff when scoring crawler shards. Omit to score every row.",
    )
    parser.add_argument(
        "--ipc-queue-dir",
        type=Path,
        default=None,
        help="Optional directory where run manifests are written for IPC consumers.",
    )
    parser.add_argument(
        "--keep-runs",
        type=int,
        default=KEEP_RUNS_DEFAULT,
        help=f"Number of recent refresh runs to keep. Older runs are cleaned up. Default: {KEEP_RUNS_DEFAULT}.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main():
    args = parse_args()
    if args.worker_count <= 0:
        raise ValueError("--worker-count must be > 0")
    if args.chunk_worker_count <= 0:
        raise ValueError("--chunk-worker-count must be > 0")
    if args.db_retry_attempts <= 0:
        raise ValueError("--db-retry-attempts must be > 0")
    run_label = args.run_label or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = args.run_root.resolve() / "runs" / run_label
    source_dir = run_dir / "source"
    training_dir = run_dir / "training"
    artifacts_dir = run_dir / "artifacts"
    selection_dir = run_dir / "selection"

    source_dir.mkdir(parents=True, exist_ok=True)
    training_dir.mkdir(parents=True, exist_ok=True)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    selection_dir.mkdir(parents=True, exist_ok=True)

    source_summary_path = source_dir / "prepare_live_training_data_summary.json"
    train_feature_csv = training_dir / "training_features_ext_train.csv"
    test_feature_csv = training_dir / "training_features_ext_test.csv"
    feature_list_path = artifacts_dir / "features_ext.txt"
    domain_freq_path = artifacts_dir / "domain_freq_train.tsv"
    model_path = artifacts_dir / "model_lightgbm_ext.txt"
    train_metrics_path = artifacts_dir / "model_lightgbm_ext_metrics.json"
    eval_metrics_path = artifacts_dir / "eval_metrics.json"
    selection_output_path = selection_dir / "top_32m.tsv"
    selection_summary_path = selection_dir / "top_32m_summary.json"
    selection_progress_log_path = selection_dir / "score_progress.jsonl"
    selectdb_load_summary_path = selection_dir / "selectdb_load_summary.json"
    selected_url_crawlerdb_sync_summary_path = selection_dir / "selected_url_crawlerdb_sync_summary.json"
    indexed_flag_sync_summary_path = selection_dir / "indexed_flag_sync_summary.json"

    run_python_step(
        BASE_DIR,
        "prepare_live_training_data.py",
        [
            "--metric-db-url", args.metric_db_url,
            "--crawler-db-url", args.crawler_db_url,
            "--output-dir", str(source_dir),
            "--train-batches", str(args.train_batches),
            "--test-batches", str(args.test_batches),
            "--negative-sample-target", str(args.negative_sample_target),
            "--worker-count", str(args.worker_count),
            "--db-retry-attempts", str(args.db_retry_attempts),
            "--seed", str(args.seed),
        ],
    )

    source_summary = load_json(source_summary_path)
    cutoff_datetime = source_summary["metric"]["cutoff_datetime"]

    train_pipeline_args = [
        "--seed", str(args.seed),
        "--top-frac", str(args.top_frac),
        "--pos-csv", source_summary["paths"]["metric_train_csv"],
        "--neg-csv", source_summary["paths"]["crawler_negative_train_csv"],
        "--train-feature-csv", str(train_feature_csv),
        "--test-feature-csv", str(test_feature_csv),
        "--feature-list-path", str(feature_list_path),
        "--domain-freq-path", str(domain_freq_path),
        "--model-path", str(model_path),
        "--train-metrics-path", str(train_metrics_path),
        "--eval-metrics-path", str(eval_metrics_path),
    ]
    if args.random_test_frac > 0:
        train_pipeline_args.extend(["--random-test-frac", str(args.random_test_frac)])
    else:
        metric_test_csv = source_summary["paths"]["metric_test_csv"]
        crawler_negative_test_csv = source_summary["paths"]["crawler_negative_test_csv"]
        if metric_test_csv and crawler_negative_test_csv:
            train_pipeline_args.extend([
                "--pos-test-csv", metric_test_csv,
                "--neg-test-csv", crawler_negative_test_csv,
            ])
        else:
            raise ValueError("Temporal test mode requested, but test source CSVs were not generated")
    run_python_step(BASE_DIR, "train_pipeline.py", train_pipeline_args)

    score_args = [
        "--crawler-db-url", args.crawler_db_url,
        "--model-path", str(model_path),
        "--domain-freq-path", str(domain_freq_path),
        "--selection-top-k", str(args.selection_top_k),
        "--selection-output-path", str(selection_output_path),
        "--summary-path", str(selection_summary_path),
        "--progress-log-path", str(selection_progress_log_path),
        "--score-shard-dir", str(selection_dir / "score_shards"),
        "--score-batch-size", str(args.score_batch_size),
        "--chunk-worker-count", str(args.chunk_worker_count),
        "--worker-count", str(args.worker_count),
        "--db-retry-attempts", str(args.db_retry_attempts),
        "--verify-row-counts",
        "--db-stream-method", "connectorx",
    ]
    if args.selection_first_seen_before:
        score_args.extend(["--first-seen-before", args.selection_first_seen_before])
    run_python_step(BASE_DIR, "score_live_crawler.py", score_args)

    load_args = [
        "--select-db-url", args.select_db_url,
        "--selection-tsv", str(selection_output_path),
        "--run-label", run_label,
        "--model-path", str(model_path),
        "--train-metrics-path", str(train_metrics_path),
        "--eval-metrics-path", str(eval_metrics_path),
        "--selection-metrics-path", str(selection_summary_path),
        "--source-summary-path", str(source_summary_path),
        "--summary-path", str(selectdb_load_summary_path),
        "--db-retry-attempts", str(args.db_retry_attempts),
    ]
    if cutoff_datetime:
        load_args.extend(["--cutoff-datetime", cutoff_datetime])
    run_python_step(BASE_DIR, "load_selection_into_selectdb.py", load_args)
    refresh_summary = {
        "run_label": run_label,
        "run_dir": str(run_dir),
        "source_summary_path": str(source_summary_path),
        "train_metrics_path": str(train_metrics_path),
        "eval_metrics_path": str(eval_metrics_path),
        "selection_summary_path": str(selection_summary_path),
        "selection_progress_log_path": str(selection_progress_log_path),
        "selectdb_load_summary_path": str(selectdb_load_summary_path),
        "selected_url_crawlerdb_sync_summary_path": str(selected_url_crawlerdb_sync_summary_path),
        "indexed_flag_sync_summary_path": str(indexed_flag_sync_summary_path),
        "cutoff_datetime": cutoff_datetime,
        "selection_first_seen_before": args.selection_first_seen_before or None,
        "selection_top_k": int(args.selection_top_k),
        "train_batches": int(args.train_batches),
        "test_batches": int(args.test_batches),
        "random_test_frac": float(args.random_test_frac),
        "worker_count": int(args.worker_count),
        "chunk_worker_count": int(args.chunk_worker_count),
        "db_retry_attempts": int(args.db_retry_attempts),
    }
    refresh_summary_path = run_dir / "refresh_summary.json"
    write_json(refresh_summary_path, refresh_summary)
    print(json.dumps(refresh_summary, indent=2), flush=True)

    if args.ipc_queue_dir is not None:
        queue_dir = args.ipc_queue_dir.resolve()
        write_manifest(queue_dir / "indexselection_v1" / "latest_refresh.json", refresh_summary)
        write_manifest(queue_dir / "indexselection_v1" / "runs" / f"{run_label}.json", refresh_summary)

    _cleanup_old_runs(args.run_root, args.keep_runs)


if __name__ == "__main__":
    main()
