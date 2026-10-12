import argparse
import json
from pathlib import Path

from train_model import write_top_k_score_runs


def parse_args():
    parser = argparse.ArgumentParser(description="Run a Polars top-k merge in a fresh process.")
    parser.add_argument("--input-path", action="append", dest="input_paths", default=None, help="Input Parquet shard path.")
    parser.add_argument("--input-manifest", type=Path, default=None, help="Text file containing one input parquet path per line.")
    parser.add_argument("--output-path", type=Path, required=True, help="Merged output path.")
    parser.add_argument("--output-format", choices=("tsv", "parquet"), required=True, help="Output file format.")
    parser.add_argument("--top-k", type=int, required=True, help="Top-K rows to keep.")
    parser.add_argument("--input-row-count", type=int, default=None, help="Optional total input row count.")
    parser.add_argument("--max-urls-per-domain", type=int, default=None, help="Maximum URLs per domain.")
    args = parser.parse_args()
    if not args.input_paths and args.input_manifest is None:
        parser.error("one of --input-path or --input-manifest is required")
    return args


def main():
    args = parse_args()
    shard_paths = [Path(path) for path in (args.input_paths or [])]
    if args.input_manifest is not None:
        shard_paths.extend(
            Path(line.strip())
            for line in args.input_manifest.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    selected_rows = write_top_k_score_runs(
        shard_paths,
        args.output_path,
        args.top_k,
        output_format=args.output_format,
        input_row_count=args.input_row_count,
        max_urls_per_domain=args.max_urls_per_domain,
    )
    print(json.dumps({"selected_rows": int(selected_rows), "output_path": str(args.output_path)}))


if __name__ == "__main__":
    main()
