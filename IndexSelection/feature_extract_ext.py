import argparse
import csv
import random
from pathlib import Path

from training_common import (
    DEFAULT_DOMAIN_FREQ_PATH,
    DEFAULT_FEATURE_LIST_PATH,
    DEFAULT_METRIC_DATA_DIR,
    DEFAULT_TEST_FEATURE_CSV,
    DEFAULT_TRAIN_FEATURE_CSV,
    DOMAIN_FREQ_FEATURE_COLUMNS,
    SPLIT_NEG_TRAIN_CSV,
    SPLIT_POS_TRAIN_CSV,
    URL_FEATURE_COLUMNS,
    build_url_feature_vector,
    collect_unique_metric_urls,
    compute_domain_freq,
    ensure_parent,
    extract_domain,
    iter_csv_urls,
    split_metric_snapshot_files,
    write_domain_freq,
    write_feature_list,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Build train/test feature CSVs for the binary Select Repo model.")
    parser.add_argument(
        "--positive-source",
        choices=("metric_url_csv", "metric_data"),
        default="metric_url_csv",
        help="Source used to build positive labels.",
    )
    parser.add_argument(
        "--pos-csv",
        type=Path,
        default=SPLIT_POS_TRAIN_CSV,
        help="Training positive source path.",
    )
    parser.add_argument(
        "--pos-test-csv",
        type=Path,
        default=None,
        help="Test positive source path.",
    )
    parser.add_argument(
        "--metric-data-dir",
        type=Path,
        default=DEFAULT_METRIC_DATA_DIR,
        help="Metric golden-set snapshot directory used by metric_data mode.",
    )
    parser.add_argument(
        "--neg-csv",
        type=Path,
        default=SPLIT_NEG_TRAIN_CSV,
        help="Training negative URL CSV path.",
    )
    parser.add_argument(
        "--neg-test-csv",
        type=Path,
        default=None,
        help="Test negative URL CSV path.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_TRAIN_FEATURE_CSV,
        help="Training feature CSV output path.",
    )
    parser.add_argument(
        "--test-output-csv",
        type=Path,
        default=DEFAULT_TEST_FEATURE_CSV,
        help="Test feature CSV output path.",
    )
    parser.add_argument(
        "--feature-list-path",
        type=Path,
        default=DEFAULT_FEATURE_LIST_PATH,
        help="Output feature-name list path.",
    )
    parser.add_argument(
        "--domain-freq-path",
        type=Path,
        default=DEFAULT_DOMAIN_FREQ_PATH,
        help="Output train-domain frequency TSV path with separate label=1/label=0 counts, used by live scoring.",
    )
    parser.add_argument(
        "--label-snapshot-count",
        type=int,
        default=2,
        help="When positive-source=metric_data, use the latest N snapshot dates as test labels.",
    )
    parser.add_argument(
        "--label-start-date",
        default="",
        help="When positive-source=metric_data, optional YYYYMMDD cutoff; dates on/after this are test labels.",
    )
    parser.add_argument(
        "--train-pos-frac",
        type=float,
        default=1.0,
        help="Fraction of training positives to keep. Defaults to 100%%.",
    )
    parser.add_argument(
        "--test-pos-frac",
        type=float,
        default=1.0,
        help="Fraction of test positives to keep. Defaults to 100%%.",
    )
    parser.add_argument(
        "--neg-ratio",
        type=float,
        default=None,
        help="Training negative-to-positive ratio. Omit to use the full training negative pool.",
    )
    parser.add_argument(
        "--random-test-frac",
        type=float,
        default=0.0,
        help="When > 0, ignore external test sources and randomly split the single positive/negative source into train/test.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for negative sampling.")
    return parser.parse_args()


def sample_positive_urls(urls: list[str], fraction: float, rng: random.Random) -> list[str]:
    if not urls:
        return []
    if fraction <= 0:
        raise ValueError("positive fraction must be > 0")
    if fraction >= 1.0:
        return list(urls)
    sample_size = max(1, int(len(urls) * fraction))
    selected = rng.sample(urls, sample_size)
    selected.sort()
    return selected


def build_negative_urls(
    pos_urls: list[str],
    neg_csv: Path,
    neg_ratio: float | None,
    rng: random.Random,
    *,
    exclude_urls: list[str] | set[str] | None = None,
) -> list[str]:
    pos_set = set(exclude_urls or pos_urls)
    neg_pool = []
    for url in iter_csv_urls(neg_csv) or []:
        if url not in pos_set:
            neg_pool.append(url)

    if neg_ratio is None:
        return list(dict.fromkeys(neg_pool))

    pos_domain_counts = {}
    for url in pos_urls:
        domain = extract_domain(url)
        if domain:
            pos_domain_counts[domain] = pos_domain_counts.get(domain, 0) + 1

    neg_domain_buckets = {}
    for url in neg_pool:
        domain = extract_domain(url)
        if not domain:
            continue
        neg_domain_buckets.setdefault(domain, []).append(url)

    neg_target = int(len(pos_urls) * neg_ratio)
    neg_selected = []
    for domain, count in pos_domain_counts.items():
        want = int(round(count * neg_ratio))
        bucket = neg_domain_buckets.get(domain, [])
        if not bucket:
            continue
        if len(bucket) <= want:
            neg_selected.extend(bucket)
        else:
            neg_selected.extend(rng.sample(bucket, want))

    if len(neg_selected) < neg_target:
        current = set(neg_selected)
        remaining = [url for url in neg_pool if url not in current]
        need = min(len(remaining), neg_target - len(neg_selected))
        if need > 0:
            neg_selected.extend(rng.sample(remaining, need))

    return list(dict.fromkeys(neg_selected))


def split_urls_randomly(urls: list[str], test_frac: float, rng: random.Random) -> tuple[list[str], list[str]]:
    if not urls:
        return [], []
    if not (0.0 < test_frac < 1.0):
        raise ValueError("random_test_frac must be between 0 and 1")
    shuffled = list(urls)
    rng.shuffle(shuffled)
    test_size = max(1, int(len(shuffled) * test_frac))
    if test_size >= len(shuffled):
        test_size = len(shuffled) - 1
    if test_size <= 0:
        return sorted(shuffled), []
    test_urls = sorted(shuffled[:test_size])
    train_urls = sorted(shuffled[test_size:])
    return train_urls, test_urls


def write_feature_csv(
    path: Path,
    pos_urls: list[str],
    neg_urls: list[str],
    domain_freq: dict[int, dict[str, int]],
    fieldnames: list[str],
    *,
    use_leave_one_out: bool,
):
    ensure_parent(path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for label, urls in ((1, pos_urls), (0, neg_urls)):
            for url in urls:
                domain, feature_values = build_url_feature_vector(
                    url,
                    domain_freq,
                    label=label,
                    use_leave_one_out=use_leave_one_out,
                )
                feature_row = {}
                for name, value in zip(URL_FEATURE_COLUMNS, feature_values):
                    if name in DOMAIN_FREQ_FEATURE_COLUMNS or (name.startswith("tld_") and name != "tld_len"):
                        feature_row[name] = int(value)
                    else:
                        feature_row[name] = value
                row = {
                    "url": url,
                    "label": label,
                    "domain": domain,
                    **feature_row,
                }
                writer.writerow(row)


def main():
    args = parse_args()
    rng = random.Random(args.seed)

    if args.positive_source == "metric_data":
        split = split_metric_snapshot_files(
            args.metric_data_dir,
            label_snapshot_count=args.label_snapshot_count,
            label_start_date=args.label_start_date or None,
        )
        full_train_pos_urls = sorted(collect_unique_metric_urls(split["history_paths"]))
        full_test_pos_urls = sorted(collect_unique_metric_urls(split["label_paths"])) if args.random_test_frac <= 0 else []
    else:
        full_train_pos_urls = sorted(set(iter_csv_urls(args.pos_csv)))
        full_test_pos_urls = []
        if args.pos_test_csv and args.random_test_frac <= 0:
            full_test_pos_urls = sorted(set(iter_csv_urls(args.pos_test_csv)))

    if args.random_test_frac > 0:
        sampled_pos_urls = sample_positive_urls(full_train_pos_urls, args.train_pos_frac, rng)
        full_neg_urls = build_negative_urls(
            sampled_pos_urls,
            args.neg_csv,
            args.neg_ratio,
            rng,
            exclude_urls=full_train_pos_urls,
        )
        train_pos_urls, test_pos_urls = split_urls_randomly(sampled_pos_urls, args.random_test_frac, rng)
        train_neg_urls, test_neg_urls = split_urls_randomly(full_neg_urls, args.random_test_frac, rng)
    else:
        train_pos_urls = sample_positive_urls(full_train_pos_urls, args.train_pos_frac, rng)
        test_pos_urls = sample_positive_urls(full_test_pos_urls, args.test_pos_frac or 1.0, rng)

        train_neg_urls = build_negative_urls(
            train_pos_urls,
            args.neg_csv,
            args.neg_ratio,
            rng,
            exclude_urls=full_train_pos_urls,
        )

        test_neg_urls = []
        if args.neg_test_csv:
            test_neg_urls = build_negative_urls(
                test_pos_urls,
                args.neg_test_csv,
                None,
                rng,
                exclude_urls=full_test_pos_urls,
            )

    train_rows = [("positive", url, 1) for url in train_pos_urls] + [("negative", url, 0) for url in train_neg_urls]
    domain_freq = compute_domain_freq(train_rows)

    fieldnames = [
        "url", "label", "domain", *URL_FEATURE_COLUMNS,
    ]

    write_feature_csv(
        args.output_csv,
        train_pos_urls,
        train_neg_urls,
        domain_freq,
        fieldnames,
        use_leave_one_out=True,
    )
    if args.test_output_csv and (test_pos_urls or test_neg_urls):
        write_feature_csv(
            args.test_output_csv,
            test_pos_urls,
            test_neg_urls,
            domain_freq,
            fieldnames,
            use_leave_one_out=False,
        )

    feature_cols = [name for name in fieldnames if name not in ("url", "label", "domain")]
    write_feature_list(args.feature_list_path, feature_cols)
    write_domain_freq(args.domain_freq_path, domain_freq)

    print(f"train_positives={len(train_pos_urls)} train_negatives={len(train_neg_urls)} train_output={args.output_csv}")
    print(f"test_positives={len(test_pos_urls)} test_negatives={len(test_neg_urls)} test_output={args.test_output_csv if (test_pos_urls or test_neg_urls) else 'N/A'}")
    print(f"feature_list_saved={args.feature_list_path}")
    print(f"domain_freq_saved={args.domain_freq_path}")
    print(f"positive_source={args.positive_source}")
    if args.random_test_frac > 0:
        print(f"random_test_frac={args.random_test_frac}")
    print(f"train_pos_frac={args.train_pos_frac}")
    print(f"test_pos_frac={args.test_pos_frac}")
    print(f"train_neg_ratio={'all' if args.neg_ratio is None else args.neg_ratio}")
    if args.positive_source == "metric_data":
        print(f"history_dates={[d.strftime('%Y%m%d') for d in split['history_dates']]}")
        print(f"label_dates={[d.strftime('%Y%m%d') for d in split['label_dates']]}")
        print(f"history_files={len(split['history_paths'])} label_files={len(split['label_paths'])}")
    else:
        print(f"train_pos_csv={args.pos_csv}")
        if args.pos_test_csv:
            print(f"test_pos_csv={args.pos_test_csv}")
        print(f"train_neg_csv={args.neg_csv}")
        if args.neg_test_csv:
            print(f"test_neg_csv={args.neg_test_csv}")


if __name__ == "__main__":
    main()
