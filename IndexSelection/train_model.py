import argparse
import json
import os
from pathlib import Path

import numpy as np

from native_feature_matrix import build_feature_matrix_with_optional_native
from training_common import (
    DEFAULT_BINARY_METRICS_PATH,
    DEFAULT_BINARY_MODEL_PATH,
    DEFAULT_FEATURE_LIST_PATH,
    DEFAULT_TEST_FEATURE_CSV,
    DEFAULT_TRAIN_FEATURE_CSV,
    URL_FEATURE_COLUMNS,
    auc_score,
    build_url_feature_vector,
    ensure_parent,
    logloss,
    read_feature_csv,
    write_feature_list,
    write_json,
)


def _configure_polars_thread_env() -> int:
    configured = os.environ.get("INDEXSELECTION_POLARS_THREADS", "1").strip() or "1"
    try:
        thread_count = max(1, int(configured))
    except ValueError:
        thread_count = 1
    value = str(thread_count)
    for env_name in ("POLARS_MAX_THREADS", "RAYON_NUM_THREADS"):
        if not os.environ.get(env_name):
            os.environ[env_name] = value
    return thread_count


_POLARS_THREAD_COUNT = _configure_polars_thread_env()


def _require_polars():
    try:
        import polars as pl
    except ImportError as exc:
        raise ImportError(
            "polars is required for Parquet-backed selection scoring. Install it with `pip install polars`."
        ) from exc
    return pl


def parse_args():
    parser = argparse.ArgumentParser(description="Train the binary LightGBM model for Select Repo v1.")
    parser.add_argument("--train-csv-path", type=Path, default=DEFAULT_TRAIN_FEATURE_CSV, help="Training feature CSV path.")
    parser.add_argument("--test-csv-path", type=Path, default=DEFAULT_TEST_FEATURE_CSV, help="Test feature CSV path.")
    parser.add_argument("--model-path", type=Path, default=DEFAULT_BINARY_MODEL_PATH, help="Output LightGBM model path.")
    parser.add_argument("--metrics-path", type=Path, default=DEFAULT_BINARY_METRICS_PATH, help="Output metrics JSON path.")
    parser.add_argument("--feature-list-path", type=Path, default=DEFAULT_FEATURE_LIST_PATH, help="Output feature-name list path.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--learning-rate", type=float, default=0.5, help="LightGBM learning rate.")
    parser.add_argument("--num-leaves", type=int, default=31, help="LightGBM num_leaves.")
    parser.add_argument("--num-boost-round", type=int, default=2000, help="Max boosting rounds.")
    parser.add_argument("--early-stopping-rounds", type=int, default=100, help="Early stopping rounds.")
    parser.add_argument("--feature-fraction", type=float, default=0.8, help="LightGBM feature_fraction.")
    parser.add_argument("--bagging-fraction", type=float, default=0.8, help="LightGBM bagging_fraction.")
    parser.add_argument("--bagging-freq", type=int, default=1, help="LightGBM bagging_freq.")
    parser.add_argument("--min-data-in-leaf", type=int, default=100, help="LightGBM min_data_in_leaf.")
    parser.add_argument("--scale-pos-weight", type=float, default=1.0, help="LightGBM scale_pos_weight.")
    return parser.parse_args()


def _build_feature_matrix_python(
    urls: list[str],
    domain_freq: dict[int, dict[str, int]],
    progress_label: str,
    progress_interval: int,
    labels: list[int] | np.ndarray | None = None,
    use_leave_one_out: bool = False,
) -> np.ndarray:
    n_features = len(URL_FEATURE_COLUMNS)
    matrix = np.empty((len(urls), n_features), dtype=np.float32)
    for idx, url in enumerate(urls, start=1):
        row_label = None if labels is None else int(labels[idx - 1])
        _, values = build_url_feature_vector(
            url,
            domain_freq,
            label=row_label,
            use_leave_one_out=use_leave_one_out,
        )
        matrix[idx - 1] = values
        if progress_interval > 0 and idx % progress_interval == 0:
            print(f"{progress_label}_features_built={idx}", flush=True)
    return matrix


def _build_feature_matrix_chunk(args):
    urls, domain_freq, labels, use_leave_one_out = args
    return build_feature_matrix_with_optional_native(
        urls,
        domain_freq,
        progress_label="",
        progress_interval=0,
        labels=labels,
        use_leave_one_out=use_leave_one_out,
        python_builder=_build_feature_matrix_python,
    )


def build_feature_matrix(
    urls: list[str],
    domain_freq: dict[int, dict[str, int]],
    *,
    progress_label: str,
    n_workers: int = 1,
    progress_interval: int = 0,
    labels: list[int] | np.ndarray | None = None,
    use_leave_one_out: bool = False,
) -> np.ndarray:
    if n_workers <= 1 or len(urls) <= 100_000:
        return build_feature_matrix_with_optional_native(
            urls,
            domain_freq,
            progress_label=progress_label,
            progress_interval=progress_interval,
            labels=labels,
            use_leave_one_out=use_leave_one_out,
            python_builder=_build_feature_matrix_python,
        )

    from concurrent.futures import ProcessPoolExecutor, as_completed
    import os

    chunk_size = max(1, len(urls) // n_workers)
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    label_chunks = None if labels is None else [labels[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    n_features = len(URL_FEATURE_COLUMNS)
    matrix = np.empty((len(urls), n_features), dtype=np.float32)

    with ProcessPoolExecutor(max_workers=min(n_workers, len(chunks), os.cpu_count() or n_workers)) as executor:
        chunk_args = [
            (
                chunk,
                domain_freq,
                None if label_chunks is None else label_chunks[idx],
                use_leave_one_out,
            )
            for idx, chunk in enumerate(chunks)
        ]
        futures = {executor.submit(_build_feature_matrix_chunk, arg): i for i, arg in enumerate(chunk_args)}
        completed = 0
        for future in as_completed(futures):
            chunk_matrix = future.result()
            start = sum(len(chunks[j]) for j in range(futures[future]))
            end = start + len(chunk_matrix)
            matrix[start:end] = chunk_matrix
            completed += len(chunk_matrix)
            if progress_interval > 0 and (completed % progress_interval == 0 or completed == len(urls)):
                print(f"{progress_label}_features_built={completed}", flush=True)

    return matrix


def train_booster(args, x_train, y_train, x_test, y_test, feature_cols):
    import lightgbm as lgb

    if args.scale_pos_weight <= 0:
        raise ValueError("--scale-pos-weight must be > 0")

    train_data = lgb.Dataset(x_train, label=y_train, feature_name=feature_cols)
    valid_data = lgb.Dataset(x_test, label=y_test, feature_name=feature_cols)

    params = {
        "objective": "binary",
        "metric": ["auc", "binary_logloss"],
        "learning_rate": args.learning_rate,
        "num_leaves": args.num_leaves,
        "feature_fraction": args.feature_fraction,
        "bagging_fraction": args.bagging_fraction,
        "bagging_freq": args.bagging_freq,
        "min_data_in_leaf": args.min_data_in_leaf,
        "scale_pos_weight": args.scale_pos_weight,
        "seed": args.seed,
        "verbosity": -1,
    }

    booster = lgb.train(
        params,
        train_data,
        num_boost_round=args.num_boost_round,
        valid_sets=[valid_data],
        valid_names=["valid"],
        callbacks=[lgb.early_stopping(stopping_rounds=args.early_stopping_rounds, verbose=False)],
    )

    best_iter = booster.best_iteration or 0
    preds = booster.predict(x_test, num_iteration=best_iter if best_iter > 0 else None)
    metrics = {
        "test_auc": auc_score(y_test, preds),
        "test_logloss": logloss(y_test, preds),
        "test_acc": float(np.mean((preds >= 0.5) == y_test)),
        "train_size": int(len(y_train)),
        "test_size": int(len(y_test)),
        "train_positives": int((y_train == 1).sum()),
        "test_positives": int((y_test == 1).sum()),
        "n_features": int(len(feature_cols)),
        "has_golden_recency_feature": "golden_recency_score" in feature_cols,
        "best_iteration": int(best_iter),
    }
    return booster, metrics, best_iter


def write_score_shard(
    shard_dir: Path,
    shard_idx: int,
    urls: list[str],
    first_seen_values: list[str],
    scores: np.ndarray,
    *,
    file_name: str | None = None,
    top_k: int | None = None,
) -> Path:
    pl = _require_polars()
    shard_path = shard_dir / (file_name or f"score_shard_{shard_idx:05d}.parquet")
    ensure_parent(shard_path)
    order = np.argsort(scores)[::-1]
    if top_k is not None:
        if top_k <= 0:
            raise ValueError("top_k must be > 0 when provided")
        order = order[:top_k]
    score_values = np.asarray(scores)[order].astype(np.float64, copy=False)
    url_values = [urls[int(idx)] for idx in order]
    first_seen_ordered = [first_seen_values[int(idx)] for idx in order]
    frame = pl.DataFrame(
        {
            "score": score_values,
            "url": url_values,
            "first_seen": first_seen_ordered,
        }
    )
    frame.write_parquet(shard_path, compression="snappy", statistics=False)
    return shard_path


def _build_top_k_query(run_paths: list[Path], top_k: int, *, select_columns: list[str], sort_output: bool, max_urls_per_domain: int | None = None):
    if top_k <= 0:
        raise ValueError("top_k must be > 0")
    parquet_paths = [str(path) for path in run_paths if path.exists()]
    if not parquet_paths:
        raise ValueError("No score shard files were provided for top-k selection")
    pl = _require_polars()

    print(f"[running_top_k] Processing {len(parquet_paths)} files, target top_k={top_k}", flush=True)

    running_winners = None
    for i, path_str in enumerate(parquet_paths):
        if (i + 1) % 10 == 0 or i == 0:
            print(f"[running_top_k] [{i+1}/{len(parquet_paths)}] Processing: {Path(path_str).name}", flush=True)

        current_chunk = (
            pl.scan_parquet(path_str)
            .select(["score", "url", "first_seen"])
            .sort("score", descending=True)
            .head(top_k)
            .collect(streaming=True)
        )

        if running_winners is None:
            running_winners = current_chunk
        else:
            running_winners = (
                pl.concat([running_winners, current_chunk])
                .sort("score", descending=True)
                .head(top_k)
            )

    print(f"[running_top_k] Done, final winners: {len(running_winners)} rows", flush=True)

    if max_urls_per_domain is not None and max_urls_per_domain > 0:
        print(f"[running_top_k] Applying per-domain limit: max_urls_per_domain={max_urls_per_domain}", flush=True)

        df = running_winners if isinstance(running_winners, pl.DataFrame) else running_winners.collect()

        try:
            from _url_features_native import extract_domains
            print(f"[running_top_k] Using native extract_domains", flush=True)
            urls_list = df["url"].to_list()
            domains_list = extract_domains(urls_list)
            df = df.with_columns(pl.Series("domain", domains_list))
        except ImportError:
            print(f"[running_top_k] Using Python extract_domains (slower)", flush=True)
            def extract_domain(url_str):
                if not url_str:
                    return ""
                scheme_end = url_str.find("://")
                if scheme_end == -1:
                    return ""
                start = scheme_end + 3
                end = url_str.find("/", start)
                if end == -1:
                    end = len(url_str)
                domain = url_str[start:end]
                at_pos = domain.find("@")
                if at_pos != -1:
                    domain = domain[at_pos+1:]
                colon_pos = domain.find(":")
                if colon_pos != -1:
                    domain = domain[:colon_pos]
                if domain.startswith("www."):
                    domain = domain[4:]
                return domain.lower()
            df = running_winners.collect()
            df = df.with_columns(
                pl.col("url").map_elements(extract_domain, return_dtype=pl.String).alias("domain")
            )

        df = df.sort("score", descending=True)
        df = df.group_by("domain", maintain_order=True).head(max_urls_per_domain)
        df = df.sort("score", descending=True).head(top_k)
        running_winners = pl.LazyFrame(df)

    if sort_output:
        running_winners = running_winners.sort("score", descending=True)
    return running_winners.select(select_columns)


def write_top_k_score_runs(
    shard_paths: list[Path],
    output_path: Path,
    top_k: int,
    *,
    output_format: str,
    input_row_count: int | None = None,
    max_urls_per_domain: int | None = None,
) -> int:
    ensure_parent(output_path)
    if output_format == "parquet":
        df = _build_top_k_query(
            shard_paths,
            top_k,
            select_columns=["score", "url", "first_seen"],
            sort_output=False,
            max_urls_per_domain=max_urls_per_domain,
        )
        df.write_parquet(output_path, compression="snappy")
    elif output_format == "tsv":
        df = _build_top_k_query(
            shard_paths,
            top_k,
            select_columns=["url", "score", "first_seen"],
            sort_output=True,
            max_urls_per_domain=max_urls_per_domain,
        )
        df.write_csv(
            output_path,
            separator="\t",
            include_header=True,
        )
    else:
        raise ValueError("output_format must be 'parquet' or 'tsv'")
    return min(int(input_row_count), int(top_k)) if input_row_count is not None else int(top_k)


def main():
    args = parse_args()
    np.random.seed(args.seed)

    x_train, y_train, train_feature_cols = read_feature_csv(args.train_csv_path)
    x_test, y_test, test_feature_cols = read_feature_csv(args.test_csv_path)
    if train_feature_cols != test_feature_cols:
        raise ValueError("Train/test feature columns do not match")
    feature_cols = train_feature_cols

    booster, metrics, _best_iter = train_booster(args, x_train, y_train, x_test, y_test, feature_cols)
    metrics["config"] = {
        "seed": args.seed,
        "learning_rate": args.learning_rate,
        "num_leaves": args.num_leaves,
        "num_boost_round": args.num_boost_round,
        "early_stopping_rounds": args.early_stopping_rounds,
        "scale_pos_weight": args.scale_pos_weight,
    }
    metrics["source"] = {
        "mode": "feature_csv",
        "train_csv_path": str(args.train_csv_path),
        "test_csv_path": str(args.test_csv_path),
    }

    ensure_parent(args.model_path)
    booster.save_model(str(args.model_path))
    write_json(args.metrics_path, metrics)
    write_feature_list(args.feature_list_path, feature_cols)

    print(json.dumps(metrics, indent=2))
    print("model_saved", args.model_path)
    print("metrics_saved", args.metrics_path)
    print("feature_list_saved", args.feature_list_path)


if __name__ == "__main__":
    main()
