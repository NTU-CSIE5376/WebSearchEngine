import argparse
from pathlib import Path

from training_common import (
    DEFAULT_BINARY_EVAL_PATH,
    DEFAULT_BINARY_METRICS_PATH,
    DEFAULT_BINARY_MODEL_PATH,
    DEFAULT_DOMAIN_FREQ_PATH,
    DEFAULT_FEATURE_LIST_PATH,
    DEFAULT_METRIC_DATA_DIR,
    DEFAULT_TEST_FEATURE_CSV,
    DEFAULT_TRAIN_FEATURE_CSV,
    SPLIT_NEG_TRAIN_CSV,
    SPLIT_POS_TRAIN_CSV,
    run_python_step,
)


BASE_DIR = Path(__file__).resolve().parent


def parse_args():
    parser = argparse.ArgumentParser(description="Run the end-to-end IndexSelection_v1 training pipeline.")
    parser.add_argument("--skip-extract", action="store_true", help="Skip feature extraction.")
    parser.add_argument("--skip-binary-train", action="store_true", help="Skip binary model training.")
    parser.add_argument("--skip-binary-eval", action="store_true", help="Skip binary model evaluation.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed passed through the pipeline.")
    parser.add_argument("--top-frac", type=float, default=0.06, help="Top fraction used in evaluation.")
    parser.add_argument("--random-test-frac", type=float, default=0.0, help="When > 0, build test data by random splitting a single source pool.")
    parser.add_argument("--train-pos-frac", type=float, default=1.0, help="Fraction of training positives to keep.")
    parser.add_argument("--test-pos-frac", type=float, default=1.0, help="Fraction of test positives to keep.")
    parser.add_argument(
        "--neg-ratio",
        type=float,
        default=None,
        help="Training negative-to-positive ratio. Omit to use the full training negative pool.",
    )
    parser.add_argument(
        "--scale-pos-weight",
        type=float,
        default=1.0,
        help="LightGBM scale_pos_weight passed through to train_model.py.",
    )
    parser.add_argument(
        "--positive-source",
        choices=("metric_url_csv", "metric_data"),
        default="metric_url_csv",
        help="Source used to build binary-model positives.",
    )
    parser.add_argument(
        "--pos-csv",
        default=SPLIT_POS_TRAIN_CSV,
        help="Training positive source path.",
    )
    parser.add_argument(
        "--pos-test-csv",
        default=None,
        help="Test positive source path.",
    )
    parser.add_argument(
        "--neg-csv",
        default=SPLIT_NEG_TRAIN_CSV,
        help="Training negative URL CSV path.",
    )
    parser.add_argument(
        "--neg-test-csv",
        default=None,
        help="Test negative URL CSV path.",
    )
    parser.add_argument("--train-feature-csv", default=DEFAULT_TRAIN_FEATURE_CSV, help="Training feature CSV output path.")
    parser.add_argument("--test-feature-csv", default=None, help="Test feature CSV output path.")
    parser.add_argument("--feature-list-path", default=DEFAULT_FEATURE_LIST_PATH, help="Output feature-list path.")
    parser.add_argument(
        "--domain-freq-path",
        default=DEFAULT_DOMAIN_FREQ_PATH,
        help="Output train domain-frequency TSV path with separate label=1/label=0 counts.",
    )
    parser.add_argument("--model-path", default=DEFAULT_BINARY_MODEL_PATH, help="Trained model output path.")
    parser.add_argument("--train-metrics-path", default=DEFAULT_BINARY_METRICS_PATH, help="Training metrics JSON output path.")
    parser.add_argument("--eval-metrics-path", default=DEFAULT_BINARY_EVAL_PATH, help="Evaluation metrics JSON output path.")
    parser.add_argument("--metric-data-dir", default=DEFAULT_METRIC_DATA_DIR, help="Metric snapshot directory used by metric_data modes.")
    parser.add_argument("--label-snapshot-count", type=int, default=2, help="Use the latest N snapshot dates as test labels.")
    parser.add_argument("--label-start-date", default="", help="Optional YYYYMMDD cutoff; dates on/after this are test labels.")
    return parser.parse_args()


def main():
    args = parse_args()
    pos_test_exists = bool(args.pos_test_csv and Path(str(args.pos_test_csv)).exists())
    neg_test_exists = bool(args.neg_test_csv and Path(str(args.neg_test_csv)).exists())
    has_external_test_sources = pos_test_exists and neg_test_exists
    effective_test_feature_csv = args.test_feature_csv
    if effective_test_feature_csv is None and (args.random_test_frac > 0 or has_external_test_sources):
        effective_test_feature_csv = DEFAULT_TEST_FEATURE_CSV

    if not args.skip_extract:
        extract_args = [
            "--seed", str(args.seed),
            "--positive-source", args.positive_source,
            "--pos-csv", str(args.pos_csv),
            "--neg-csv", str(args.neg_csv),
            "--train-pos-frac", str(args.train_pos_frac),
            "--metric-data-dir", str(args.metric_data_dir),
            "--output-csv", str(args.train_feature_csv),
            "--feature-list-path", str(args.feature_list_path),
            "--domain-freq-path", str(args.domain_freq_path),
            "--random-test-frac", str(args.random_test_frac),
        ]
        if pos_test_exists:
            extract_args.extend(["--pos-test-csv", str(args.pos_test_csv)])
        if neg_test_exists:
            extract_args.extend(["--neg-test-csv", str(args.neg_test_csv)])
        if args.test_pos_frac is not None and args.test_pos_frac != 1.0:
            extract_args.extend(["--test-pos-frac", str(args.test_pos_frac)])
        if effective_test_feature_csv:
            extract_args.extend(["--test-output-csv", str(effective_test_feature_csv)])
        if args.neg_ratio is not None:
            extract_args.extend(["--neg-ratio", str(args.neg_ratio)])
        if args.positive_source == "metric_data":
            extract_args.extend(["--label-snapshot-count", str(args.label_snapshot_count)])
            if args.label_start_date:
                extract_args.extend(["--label-start-date", args.label_start_date])
        run_python_step(BASE_DIR, "feature_extract_ext.py", extract_args)
    if not args.skip_binary_train:
        train_args = [
            "--seed", str(args.seed),
            "--train-csv-path", str(args.train_feature_csv),
            "--model-path", str(args.model_path),
            "--metrics-path", str(args.train_metrics_path),
            "--feature-list-path", str(args.feature_list_path),
            "--scale-pos-weight", str(args.scale_pos_weight),
        ]
        if effective_test_feature_csv:
            train_args.extend(["--test-csv-path", str(effective_test_feature_csv)])
        run_python_step(BASE_DIR, "train_model.py", train_args)
    if not args.skip_binary_eval and effective_test_feature_csv:
        run_python_step(
            BASE_DIR,
            "eval.py",
            [
                "--top-frac", str(args.top_frac),
                "--test-csv-path", str(effective_test_feature_csv),
                "--model-path", str(args.model_path),
                "--metrics-path", str(args.eval_metrics_path),
            ],
        )


if __name__ == "__main__":
    main()
