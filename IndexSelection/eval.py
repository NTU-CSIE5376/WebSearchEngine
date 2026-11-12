import argparse
import json
from pathlib import Path

from training_common import (
    DEFAULT_BINARY_EVAL_PATH,
    DEFAULT_BINARY_MODEL_PATH,
    DEFAULT_TEST_FEATURE_CSV,
    read_feature_csv,
    top_fraction_stats,
    write_json,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate top-fraction coverage for the binary LightGBM model.")
    parser.add_argument("--test-csv-path", type=Path, default=DEFAULT_TEST_FEATURE_CSV, help="Test feature CSV path.")
    parser.add_argument("--model-path", type=Path, default=DEFAULT_BINARY_MODEL_PATH, help="Trained model path.")
    parser.add_argument("--metrics-path", type=Path, default=DEFAULT_BINARY_EVAL_PATH, help="Output evaluation metrics JSON path.")
    parser.add_argument("--top-frac", type=float, default=0.035, help="Selected top fraction for coverage.")
    return parser.parse_args()


def main():
    args = parse_args()
    import lightgbm as lgb

    x_test, y_test, _ = read_feature_csv(args.test_csv_path)

    booster = lgb.Booster(model_file=str(args.model_path))
    scores = booster.predict(x_test)
    stats = top_fraction_stats(y_test, scores, args.top_frac)

    metrics = {
        "test_size": int(len(y_test)),
        "positives": stats["positives"],
        "selected": stats["selected"],
        "threshold": stats["threshold"],
        "coverage_top_fraction": stats["coverage"],
        "top_frac": args.top_frac,
        "test_csv_path": str(args.test_csv_path),
    }

    write_json(args.metrics_path, metrics)
    print(json.dumps(metrics, indent=2))
    print("metrics_saved", args.metrics_path)


if __name__ == "__main__":
    main()
