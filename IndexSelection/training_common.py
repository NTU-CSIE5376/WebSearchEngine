import csv
import json
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import numpy as np


BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent


def first_existing_path(*candidates: Path) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


DATA_DIR = BASE_DIR / "data"
ARTIFACT_DIR = BASE_DIR / "artifacts"

DEFAULT_TRAIN_FEATURE_CSV = DATA_DIR / "training_features_ext_train.csv"
DEFAULT_TEST_FEATURE_CSV = DATA_DIR / "training_features_ext_test.csv"
DEFAULT_FEATURE_LIST_PATH = DATA_DIR / "features_ext.txt"
DEFAULT_BINARY_MODEL_PATH = ARTIFACT_DIR / "model_lightgbm_ext.txt"
DEFAULT_BINARY_METRICS_PATH = ARTIFACT_DIR / "model_lightgbm_ext_metrics.json"
DEFAULT_BINARY_EVAL_PATH = ARTIFACT_DIR / "eval_metrics.json"
DEFAULT_DOMAIN_FREQ_PATH = ARTIFACT_DIR / "domain_freq_train.tsv"
DEFAULT_METRIC_DATA_DIR = first_existing_path(
    REPO_ROOT / "Metric" / "metricData",
    REPO_ROOT.parent / "WebSearchEngine" / "Metric" / "metricData",
)

DATE_PATTERN = re.compile(r"\b(?:\d{4}[\-/]\d{1,2}[\-/]\d{1,2}|\d{1,2}[\-/]\d{1,2}[\-/]\d{2,4}|(?:19|20)\d{2})\b")

MULTI_TLDS = {
    "co.uk", "org.uk", "ac.uk", "gov.uk",
    "co.jp", "ne.jp", "or.jp",
    "com.au", "net.au", "org.au",
    "com.br", "com.cn", "com.tw",
}
BASE_TLDS = {
    "com", "org", "edu", "gov", "net", "info", "io", "co", "us", "uk", "jp", "cn",
    "de", "fr", "ru", "br", "it", "es", "kr", "au", "ca", "tw",
}
TLD_COLS = sorted(list(MULTI_TLDS | BASE_TLDS))
DROP_COLUMNS = ("url", "label", "domain")
BASE_URL_FEATURE_COLUMNS = [
    "url_len", "path_len", "query_len", "path_depth", "has_query", "num_params",
    "has_fragment", "https", "is_homepage",
    "num_digits", "digit_ratio", "num_hyphen", "num_underscore",
    "domain_len", "subdomain_count", "tld_len", "file_ext_len", "has_date",
]
DOMAIN_FREQ_FEATURE_COLUMNS = ["domain_freq_label_1", "domain_freq_label_0"]
URL_FEATURE_COLUMNS = [*DOMAIN_FREQ_FEATURE_COLUMNS, *BASE_URL_FEATURE_COLUMNS] + [f"tld_{tld_name}" for tld_name in TLD_COLS] + ["tld_other"]
TLD_FEATURE_START_INDEX = len(DOMAIN_FREQ_FEATURE_COLUMNS) + len(BASE_URL_FEATURE_COLUMNS)
TLD_FEATURE_INDEX = {tld_name: TLD_FEATURE_START_INDEX + idx for idx, tld_name in enumerate(TLD_COLS)}
TLD_OTHER_FEATURE_INDEX = TLD_FEATURE_START_INDEX + len(TLD_COLS)
URL_SHAPE_FEATURE_CLIP_MAX = {
    "url_len": 2048.0,
    "path_len": 1024.0,
    "query_len": 1024.0,
    "path_depth": 32.0,
    "num_params": 32.0,
    "num_digits": 512.0,
    "num_hyphen": 64.0,
    "num_underscore": 64.0,
    "domain_len": 64.0,
    "subdomain_count": 8.0,
    "tld_len": 16.0,
    "file_ext_len": 8.0,
}

DEFAULT_POS_TEST_CSV = first_existing_path(REPO_ROOT / "crawlerdb_csv" / "metric_url.csv", REPO_ROOT.parent / "crawlerdb_csv" / "metric_url.csv")
DEFAULT_NEG_TEST_CSV = first_existing_path(REPO_ROOT / "url_state_current_sample_raw.csv", REPO_ROOT.parent / "url_state_current_sample_raw.csv")
SPLIT_POS_TRAIN_CSV = DEFAULT_POS_TEST_CSV
SPLIT_NEG_TRAIN_CSV = DEFAULT_NEG_TEST_CSV


def ensure_parent(path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: dict) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def run_python_step(base_dir: Path, script_name: str, extra_args: list[str]) -> None:
    cmd = [sys.executable, str(Path(base_dir) / script_name), *extra_args]
    print("running", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


def iter_csv_urls(path: Path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "url" not in (reader.fieldnames or []):
            return
        for row in reader:
            url = (row.get("url") or "").strip()
            if url:
                yield url


def iter_metric_snapshot_urls(path: Path):
    urls = set()
    if path.suffix == ".csv":
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                blob = row.get("urls", "") or ""
                for url in blob.split(","):
                    url = url.strip()
                    if url:
                        urls.add(url)
    elif path.suffix == ".json":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for meta in data.values():
            for url in meta.get("url", []) or []:
                url = str(url).strip()
                if url:
                    urls.add(url)
    return urls


def parse_snapshot_date(path: Path):
    match = re.search(r"_(\d{8})\.(csv|json)$", path.name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def list_metric_snapshot_files(metric_data_dir: Path):
    snapshots = []
    for path in sorted(metric_data_dir.glob("*")):
        if path.suffix not in {".csv", ".json"}:
            continue
        snapshot_date = parse_snapshot_date(path)
        if snapshot_date is None:
            continue
        snapshots.append((snapshot_date, path))
    return sorted(snapshots, key=lambda item: (item[0], item[1].name))


def parse_yyyymmdd(value: str):
    return datetime.strptime(value, "%Y%m%d").date()


def split_metric_snapshot_files(metric_data_dir: Path, label_snapshot_count: int = 2, label_start_date: str | None = None):
    snapshots = list_metric_snapshot_files(metric_data_dir)
    if not snapshots:
        raise ValueError(f"No metric snapshots found under {metric_data_dir}")

    unique_dates = sorted({snapshot_date for snapshot_date, _ in snapshots})

    if label_start_date:
        cutoff_date = parse_yyyymmdd(label_start_date)
        history_dates = [d for d in unique_dates if d < cutoff_date]
        label_dates = [d for d in unique_dates if d >= cutoff_date]
    else:
        if label_snapshot_count <= 0:
            raise ValueError("label_snapshot_count must be > 0")
        if len(unique_dates) <= label_snapshot_count:
            raise ValueError("Need at least one earlier snapshot date reserved for history features")
        history_dates = unique_dates[:-label_snapshot_count]
        label_dates = unique_dates[-label_snapshot_count:]

    if not history_dates:
        raise ValueError("No earlier snapshot dates selected for history features")
    if not label_dates:
        raise ValueError("No later snapshot dates selected for labels")

    history_date_set = set(history_dates)
    label_date_set = set(label_dates)
    history_paths = [path for snapshot_date, path in snapshots if snapshot_date in history_date_set]
    label_paths = [path for snapshot_date, path in snapshots if snapshot_date in label_date_set]

    return {
        "all_dates": unique_dates,
        "history_dates": history_dates,
        "label_dates": label_dates,
        "history_paths": history_paths,
        "label_paths": label_paths,
    }


def collect_unique_metric_urls(snapshot_paths: list[Path]):
    urls = set()
    for path in snapshot_paths:
        urls.update(iter_metric_snapshot_urls(path))
    return urls

def normalize_netloc(netloc: str) -> str:
    if not netloc:
        return ""
    netloc = netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if ":" in netloc:
        netloc = netloc.split(":", 1)[0]
    return netloc


def _is_valid_url_scheme(value: str) -> bool:
    if not value or not value[0].isalpha():
        return False
    for ch in value:
        if ch.isalnum() or ch in "+-.":
            continue
        return False
    return True


def _split_url_fast(url: str):
    raw = (url or "").strip()
    if not raw:
        return "", "", "", "", "", ""

    scheme = ""
    path = ""
    query = ""
    fragment = ""
    scheme_sep = raw.find("://")
    if scheme_sep > 0 and _is_valid_url_scheme(raw[:scheme_sep]):
        scheme = raw[:scheme_sep].lower()
        remainder = raw[scheme_sep + 3:]

        fragment_sep = remainder.find("#")
        if fragment_sep != -1:
            fragment = remainder[fragment_sep + 1:]
            remainder = remainder[:fragment_sep]

        query_sep = remainder.find("?")
        if query_sep != -1:
            query = remainder[query_sep + 1:]
            remainder = remainder[:query_sep]

        path_sep = remainder.find("/")
        if path_sep != -1:
            netloc = normalize_netloc(remainder[:path_sep])
            path = remainder[path_sep:]
        else:
            netloc = normalize_netloc(remainder)
        return raw, scheme, netloc, path, query, fragment

    remainder = raw
    fragment_sep = remainder.find("#")
    if fragment_sep != -1:
        fragment = remainder[fragment_sep + 1:]
        remainder = remainder[:fragment_sep]

    query_sep = remainder.find("?")
    if query_sep != -1:
        query = remainder[query_sep + 1:]
        remainder = remainder[:query_sep]

    path = remainder
    path_sep = remainder.find("/")
    domain_guess = remainder if path_sep == -1 else remainder[:path_sep]
    if domain_guess.startswith("/"):
        domain_guess = ""
    netloc = normalize_netloc(domain_guess)
    return raw, scheme, netloc, path, query, fragment


def _count_non_empty_segments(text: str, separator: str) -> int:
    if not text:
        return 0
    count = 0
    in_segment = False
    for ch in text:
        if ch == separator:
            if in_segment:
                count += 1
                in_segment = False
        elif not in_segment:
            in_segment = True
    if in_segment:
        count += 1
    return count


def _count_url_characters(text: str) -> tuple[int, int, int]:
    num_digits = 0
    num_hyphen = 0
    num_underscore = 0
    for ch in text:
        if "0" <= ch <= "9":
            num_digits += 1
        elif ch == "-":
            num_hyphen += 1
        elif ch == "_":
            num_underscore += 1
    return num_digits, num_hyphen, num_underscore


def extract_domain(url: str) -> str:
    _raw, _scheme, domain, _path, _query, _fragment = _split_url_fast(url)
    return domain


def extract_tld(domain: str) -> str:
    if not domain:
        return "other"
    parts = domain.split(".")
    if len(parts) >= 2:
        candidate = ".".join(parts[-2:])
        if candidate in MULTI_TLDS:
            return candidate
        return parts[-1]
    return "other"


def extract_extension(path: str) -> str:
    if not path:
        return ""
    tail = path.rsplit("/", 1)[-1]
    if "." not in tail:
        return ""
    ext = tail.rsplit(".", 1)[-1].lower()
    if len(ext) > 8:
        return ""
    return ext


def has_date(text: str) -> bool:
    if not text:
        return False
    return DATE_PATTERN.search(text) is not None


def clip_url_shape_feature_values(values: list[float]) -> list[float]:
    clipped = list(values)
    for idx, feature_name in enumerate(BASE_URL_FEATURE_COLUMNS):
        clip_max = URL_SHAPE_FEATURE_CLIP_MAX.get(feature_name)
        if clip_max is not None and clipped[idx] > clip_max:
            clipped[idx] = clip_max
    return clipped


def _build_url_feature_core(url: str):
    raw, scheme, domain, path, query, fragment = _split_url_fast(url)
    if not raw:
        return "", "other", [0.0] * len(BASE_URL_FEATURE_COLUMNS)

    parts = [part for part in domain.split(".") if part] if domain else []
    tld = extract_tld(domain)
    url_len = float(len(raw))
    path_len = float(len(path)) if path else 0.0
    query_len = float(len(query)) if query else 0.0
    path_depth = float(_count_non_empty_segments(path, "/"))
    has_query = 1.0 if query else 0.0
    num_params = float(_count_non_empty_segments(query, "&"))
    has_fragment = 1.0 if fragment else 0.0
    https = 1.0 if scheme == "https" else 0.0
    is_homepage = 1.0 if path in ("", "/") else 0.0
    num_digits_raw, num_hyphen_raw, num_underscore_raw = _count_url_characters(raw)
    num_digits = float(num_digits_raw)
    digit_ratio = (num_digits / url_len) if url_len > 0 else 0.0
    domain_len = float(len(domain)) if domain else 0.0
    subdomain_count = float(max(len(parts) - 2, 0)) if parts else 0.0
    tld_len = float(len(parts[-1])) if parts else 0.0
    ext = extract_extension(path)
    file_ext_len = float(len(ext)) if ext else 0.0
    has_date_flag = 1.0 if has_date(path) or has_date(query) else 0.0
    return domain, tld, clip_url_shape_feature_values([
        url_len,
        path_len,
        query_len,
        path_depth,
        has_query,
        num_params,
        has_fragment,
        https,
        is_homepage,
        num_digits,
        digit_ratio,
        float(num_hyphen_raw),
        float(num_underscore_raw),
        domain_len,
        subdomain_count,
        tld_len,
        file_ext_len,
        has_date_flag,
    ])


def url_shape_features(url: str) -> dict[str, float]:
    _domain, _tld, values = _build_url_feature_core(url)
    return {col: value for col, value in zip(BASE_URL_FEATURE_COLUMNS, values)}


def resolve_domain_freq_features(
    domain: str,
    domain_freq: dict[int, dict[str, int]],
    *,
    label: int | None = None,
    use_leave_one_out: bool = False,
) -> tuple[int, int]:
    label_1_freq = int(domain_freq.get(1, {}).get(domain, 0))
    label_0_freq = int(domain_freq.get(0, {}).get(domain, 0))

    if use_leave_one_out and domain and label in (0, 1):
        if label == 1 and label_1_freq > 0:
            label_1_freq -= 1
        elif label == 0 and label_0_freq > 0:
            label_0_freq -= 1

    return label_1_freq, label_0_freq


def build_url_feature_vector(
    url: str,
    domain_freq: dict[int, dict[str, int]],
    *,
    label: int | None = None,
    use_leave_one_out: bool = False,
):
    domain, tld, base_values = _build_url_feature_core(url)
    values = [0.0] * len(URL_FEATURE_COLUMNS)
    label_1_freq, label_0_freq = resolve_domain_freq_features(
        domain,
        domain_freq,
        label=label,
        use_leave_one_out=use_leave_one_out,
    )
    values[0] = float(label_1_freq)
    values[1] = float(label_0_freq)
    values[len(DOMAIN_FREQ_FEATURE_COLUMNS):TLD_FEATURE_START_INDEX] = base_values
    values[TLD_FEATURE_INDEX.get(tld, TLD_OTHER_FEATURE_INDEX)] = 1.0
    return domain, values


def read_feature_csv(path: Path, drop_columns=DROP_COLUMNS):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        feature_cols = [c for c in fieldnames if c not in drop_columns]
        n_features = len(feature_cols)

        rows = list(reader)
        n_rows = len(rows)
        x = np.empty((n_rows, n_features), dtype=np.float32)
        y = np.empty(n_rows, dtype=np.int32)

        for i, row in enumerate(rows):
            y[i] = int(row["label"])
            for j, col in enumerate(feature_cols):
                raw = row.get(col, "")
                if raw in ("", None):
                    x[i, j] = 0.0
                else:
                    try:
                        x[i, j] = float(raw)
                    except ValueError:
                        x[i, j] = 0.0

    return x, y, feature_cols


def auc_score(y_true: np.ndarray, y_score: np.ndarray) -> float:
    order = np.argsort(y_score)
    ranks = np.empty_like(order)
    ranks[order] = np.arange(len(y_score)) + 1
    pos = y_true == 1
    n_pos = pos.sum()
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return float("nan")
    sum_ranks = ranks[pos].sum()
    auc = (sum_ranks - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)
    return float(auc)


def logloss(y_true: np.ndarray, y_score: np.ndarray) -> float:
    eps = 1e-12
    return float(-np.mean(y_true * np.log(y_score + eps) + (1 - y_true) * np.log(1 - y_score + eps)))


def top_fraction_stats(y_true: np.ndarray, y_score: np.ndarray, top_frac: float) -> dict[str, float]:
    n_select = max(1, int(len(y_score) * top_frac))
    threshold = np.partition(y_score, -n_select)[-n_select]
    selected = y_score >= threshold
    pos_total = int((y_true == 1).sum())
    pos_selected = int(((y_true == 1) & selected).sum())
    coverage = pos_selected / pos_total if pos_total else 0.0
    return {
        "selected": int(selected.sum()),
        "threshold": float(threshold),
        "positives": pos_total,
        "positive_selected": pos_selected,
        "coverage": float(coverage),
    }


def compute_domain_freq(rows: list[tuple[str, str, int]]):
    domain_freq = {
        1: defaultdict(int),
        0: defaultdict(int),
    }
    for _, url, label in rows:
        domain = extract_domain(url)
        if domain:
            label_int = int(label)
            if label_int in domain_freq:
                domain_freq[label_int][domain] += 1
    return {
        1: dict(domain_freq[1]),
        0: dict(domain_freq[0]),
    }


def write_feature_list(path: Path, feature_cols: list[str]) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        for col in feature_cols:
            f.write(f"{col}\n")


def write_domain_freq(path: Path, domain_freq: dict[int, dict[str, int]]) -> None:
    ensure_parent(path)
    label_1_lookup = domain_freq.get(1, {})
    label_0_lookup = domain_freq.get(0, {})
    all_domains = sorted(set(label_1_lookup) | set(label_0_lookup))
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["domain", "freq_label_1", "freq_label_0"])
        for domain in all_domains:
            writer.writerow([domain, int(label_1_lookup.get(domain, 0)), int(label_0_lookup.get(domain, 0))])


def read_domain_freq(path: Path) -> dict[int, dict[str, int]]:
    domain_freq = {
        1: {},
        0: {},
    }

    def _read_int(row: dict, key: str) -> int | None:
        raw = (row.get(key) or "0").strip() or "0"
        try:
            return int(raw)
        except ValueError:
            return None

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = set(reader.fieldnames or [])
        has_split_columns = "freq_label_1" in fieldnames or "freq_label_0" in fieldnames
        for row in reader:
            domain = (row.get("domain") or "").strip()
            if not domain:
                continue
            if has_split_columns:
                label_1_freq = _read_int(row, "freq_label_1")
                label_0_freq = _read_int(row, "freq_label_0")
                if label_1_freq is None or label_0_freq is None:
                    continue
                if label_1_freq != 0:
                    domain_freq[1][domain] = label_1_freq
                if label_0_freq != 0:
                    domain_freq[0][domain] = label_0_freq
            else:
                freq = _read_int(row, "freq")
                if freq is None:
                    continue
                if freq != 0:
                    domain_freq[1][domain] = freq
    return domain_freq
