"""Strict TOML configuration.

Every tunable lives in the config file. Loading fails on unknown keys, missing
keys, wrong types and inconsistent values, so a typo cannot silently fall back
to a default. Several files can be layered; later files override earlier ones.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import tomllib
import types
import typing
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class ConfigError(ValueError):
    """Raised when a config file is invalid."""


@dataclass(frozen=True)
class RunCfg:
    work_dir: str
    app_name: str


@dataclass(frozen=True)
class GuardCfg:
    month_days: list[int]
    month_window: list[str]
    hourly_window: list[int]
    max_io_waiters: int
    probes: int
    probe_interval_s: float
    probe_ttl_s: float
    pause_s: float
    min_mb_s: float
    timeout_floor_s: int
    timeout_cap_s: int


@dataclass(frozen=True)
class CrawlerDbCfg:
    dsn_env: str
    connect_timeout_s: int
    num_shards: int
    url_table_fmt: str
    split_table: str
    session: dict[str, str]


@dataclass(frozen=True)
class MetricDbCfg:
    dsn_env: str
    connect_timeout_s: int
    session: dict[str, str]


@dataclass(frozen=True)
class RoutingCfg:
    tldextract_offline: bool
    overrides: dict[str, int]


@dataclass(frozen=True)
class PullCfg:
    min_interval_s: float
    shards: list[int]


@dataclass(frozen=True)
class GoldenCfg:
    lead_h: float
    tags: list[str]
    exclude_batches: list[int]
    use_pseudo: bool


@dataclass(frozen=True)
class FeaturesCfg:
    half_life_days: float
    eb_pseudo_hits: float
    url_len_cap: int
    path_markers: list[str]
    ext_classes: dict[str, int]
    date_regex: str
    sets: dict[str, list[str]]


@dataclass(frozen=True)
class DatasetCfg:
    neg_rate: float
    neg_salt: str
    first_label_batch: int


@dataclass(frozen=True)
class ModelCfg:
    feature_set: str
    num_boost_round: int
    early_stopping_rounds: int
    refit_on_all: bool
    params: dict[str, int | float | str | bool]
    logreg_c: float
    logreg_max_iter: int

    @property
    def num_threads(self) -> int:
        """LightGBM threads for training and prediction."""
        return int(self.params["num_threads"])


@dataclass(frozen=True)
class EvalCfg:
    test_batches: list[int]
    k_values: list[int]
    k_headline: int
    fractions: list[float]
    gain_curve_k: list[int]
    bootstrap_samples: int
    bootstrap_seed: int
    baselines: list[str]
    v2_formula: dict[str, float]
    importance_top_n: int


@dataclass(frozen=True)
class ManifestCfg:
    schema_version: int
    index_shards: int
    reason_top_n: int
    canonical_workers: int
    canonical_batch_rows: int


@dataclass(frozen=True)
class DuckDbCfg:
    threads: int
    memory_limit: str
    max_temp_directory_size: str


@dataclass(frozen=True)
class FullCfg:
    chunk_blocks: int
    chunks_per_file: int
    min_free_gb: float
    parse_threads: int
    connections: int
    max_heap_mb_s: float
    sample_percent: float
    sample_salt: str
    agg_share: float
    agg_salt: str


@dataclass(frozen=True)
class Config:
    run: RunCfg
    guard: GuardCfg
    crawlerdb: CrawlerDbCfg
    metricdb: MetricDbCfg
    routing: RoutingCfg
    pull: PullCfg
    golden: GoldenCfg
    features: FeaturesCfg
    dataset: DatasetCfg
    model: ModelCfg
    eval: EvalCfg
    manifest: ManifestCfg
    duckdb: DuckDbCfg
    full: FullCfg

    @property
    def work_dir(self) -> Path:
        return Path(self.run.work_dir)

    def sha256(self) -> str:
        """Returns a stable digest of the effective config, for run auditing."""
        payload = json.dumps(dataclasses.asdict(self), sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()


_SCALARS = (int, float, str, bool)
KNOWN_BASELINES = (
    "random", "inlinks", "ext", "domain_prior", "logreg", "v1_features",
    "url_score", "v2_baseline_v1", "later_unlabeled",
)
# LightGBM 4.7 aliases of num_threads; predict would not see a thread count set this way.
LGBM_THREAD_ALIASES = ("n_jobs", "nthread", "nthreads", "num_thread")
# crawlerdb session settings that fix the CSV text the full export parses: other
# values fail the parse, or round url_score (extra_float_digits < 1).
CSV_SESSION = {"datestyle": "ISO", "extra_float_digits": "1", "client_encoding": "UTF8"}


def _deep_merge(base: dict[str, Any], over: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in over.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = _deep_merge(dict(out[key]), value)
        else:
            out[key] = value
    return out


def _check_scalar(tp: type, value: Any, where: str) -> Any:
    if tp is float and isinstance(value, int) and not isinstance(value, bool):
        return float(value)
    if tp is int and isinstance(value, bool):
        raise ConfigError(f"{where}: expected int, got bool")
    if not isinstance(value, tp):
        raise ConfigError(f"{where}: expected {tp.__name__}, got {type(value).__name__}")
    return value


def _convert(tp: Any, value: Any, where: str) -> Any:
    origin = typing.get_origin(tp)
    if dataclasses.is_dataclass(tp):
        return _build(tp, value, where)
    if origin is list:
        (item_tp,) = typing.get_args(tp)
        if not isinstance(value, Sequence) or isinstance(value, str):
            raise ConfigError(f"{where}: expected a list")
        return [_convert(item_tp, v, f"{where}[{i}]") for i, v in enumerate(value)]
    if origin is dict:
        key_tp, val_tp = typing.get_args(tp)
        if not isinstance(value, Mapping):
            raise ConfigError(f"{where}: expected a table")
        return {
            _check_scalar(key_tp, k, f"{where} key"): _convert(val_tp, v, f"{where}.{k}")
            for k, v in value.items()
        }
    if origin in (typing.Union, types.UnionType):
        for option in typing.get_args(tp):
            if option is int and isinstance(value, bool):
                continue
            if option is int and isinstance(value, float):
                continue
            try:
                return _check_scalar(option, value, where)
            except ConfigError:
                continue
        raise ConfigError(f"{where}: expected one of {typing.get_args(tp)}")
    # Session GUC values may be written as TOML numbers for readability.
    if tp is str and isinstance(value, int | float) and not isinstance(value, bool):
        return str(value)
    if tp in _SCALARS:
        return _check_scalar(tp, value, where)
    raise ConfigError(f"{where}: unsupported type {tp}")


def _build(cls: type, data: Any, where: str) -> Any:
    if not isinstance(data, Mapping):
        raise ConfigError(f"{where}: expected a table")
    hints = typing.get_type_hints(cls)
    names = {f.name for f in dataclasses.fields(cls)}
    unknown = sorted(set(data) - names)
    missing = sorted(names - set(data))
    if unknown:
        raise ConfigError(f"{where}: unknown keys {unknown}")
    if missing:
        raise ConfigError(f"{where}: missing keys {missing}")
    kwargs = {n: _convert(hints[n], data[n], f"{where}.{n}" if where else n) for n in names}
    return cls(**kwargs)


def hhmm_minutes(text: str) -> int:
    """Parses `HH:MM` (00:00-24:00) into minutes after midnight.

    Raises:
        ConfigError: If `text` is not a valid time of day.
    """
    hours, sep, minutes = text.partition(":")
    if not (sep and hours.isdigit() and minutes.isdigit() and len(minutes) == 2):
        raise ConfigError(f"expected HH:MM, got {text!r}")
    value = int(hours) * 60 + int(minutes)
    if int(minutes) >= 60 or value > 24 * 60:
        raise ConfigError(f"expected HH:MM, got {text!r}")
    return value


def hourly_open_s(g: GuardCfg) -> int:
    """Seconds per hour outside the hourly forbidden window."""
    return (60 - (g.hourly_window[1] - g.hourly_window[0]) % 60) * 60


def timeout_seconds(session: Mapping[str, str]) -> int:
    """The session `statement_timeout`, written as whole seconds such as `'120s'`.

    Raises:
        ConfigError: If the value is missing or not whole seconds.
    """
    value = session.get("statement_timeout", "")
    if not (value.endswith("s") and value[:-1].isdigit() and int(value[:-1]) > 0):
        raise ConfigError(f"statement_timeout must be whole seconds such as '120s', got {value!r}")
    return int(value[:-1])


def _validate_guard(g: GuardCfg, need: Any) -> None:
    need(len(g.month_window) == 2, "guard.month_window must be [start, end]")
    start, end = (hhmm_minutes(v) for v in g.month_window)
    need(start < end, "guard.month_window must not wrap past midnight")
    need(all(1 <= d <= 31 for d in g.month_days), "guard.month_days must be 1-31")
    need(len(g.hourly_window) == 2 and all(0 <= m < 60 for m in g.hourly_window),
         "guard.hourly_window must be [start_minute, end_minute]")
    need(0 < g.timeout_floor_s <= g.timeout_cap_s < hourly_open_s(g),
         "guard timeouts must satisfy 0 < floor <= cap < the hourly open gap")
    need(g.max_io_waiters >= 0, "guard.max_io_waiters must be >= 0")
    need(g.probes >= 1, "guard.probes must be >= 1")
    need(min(g.probe_interval_s, g.probe_ttl_s) >= 0 and g.pause_s > 0,
         "guard intervals must be >= 0 and pause_s > 0")
    need(g.min_mb_s > 0, "guard.min_mb_s must be > 0")


def _validate_session(name: str, session: Mapping[str, str], g: GuardCfg, need: Any) -> None:
    need(session.get("default_transaction_read_only") == "on",
         f"{name}.session.default_transaction_read_only must be 'on'")
    # The client-side SQL gate lexes strings with standard (non-backslash) quoting.
    need(session.get("standard_conforming_strings") == "on",
         f"{name}.session.standard_conforming_strings must be 'on'")
    need(timeout_seconds(session) <= g.timeout_cap_s,
         f"{name}.session.statement_timeout must not exceed guard.timeout_cap_s")


def _validate(cfg: Config) -> None:
    def need(ok: bool, message: str) -> None:
        if not ok:
            raise ConfigError(message)

    need(cfg.crawlerdb.num_shards > 0, "crawlerdb.num_shards must be > 0")
    _validate_guard(cfg.guard, need)
    _validate_session("crawlerdb", cfg.crawlerdb.session, cfg.guard, need)
    _validate_session("metricdb", cfg.metricdb.session, cfg.guard, need)
    need(all(0 <= s < cfg.crawlerdb.num_shards for s in cfg.routing.overrides.values()),
         "routing.overrides must map to valid shards")
    need(cfg.pull.min_interval_s >= 0, "pull.min_interval_s must be >= 0")
    need(all(0 <= s < cfg.crawlerdb.num_shards for s in cfg.pull.shards),
         "pull.shards must be valid shard ids")
    need(cfg.golden.lead_h >= 0, "golden.lead_h must be >= 0")
    need(bool(cfg.golden.tags), "golden.tags must not be empty")
    need(cfg.features.half_life_days > 0, "features.half_life_days must be > 0")
    need(cfg.features.eb_pseudo_hits > 0, "features.eb_pseudo_hits must be > 0")
    need(cfg.features.url_len_cap > 0, "features.url_len_cap must be > 0")
    need(bool(cfg.features.sets), "features.sets must not be empty")
    need(all(v > 0 for v in cfg.features.ext_classes.values()),
         "features.ext_classes values must be > 0 (0 means no extension)")
    need(0 < cfg.dataset.neg_rate <= 1, "dataset.neg_rate must be in (0, 1]")
    need(cfg.model.feature_set in cfg.features.sets, "model.feature_set must name a features set")
    need(cfg.model.num_boost_round > 0, "model.num_boost_round must be > 0")
    need(cfg.model.early_stopping_rounds > 0, "model.early_stopping_rounds must be > 0")
    need(cfg.model.logreg_c > 0, "model.logreg_c must be > 0")
    need(cfg.model.params.get("objective") == "binary", "model.params.objective must be 'binary'")
    threads = cfg.model.params.get("num_threads")
    need(isinstance(threads, int) and not isinstance(threads, bool) and threads > 0,
         "model.params.num_threads must be an integer > 0")
    aliases = sorted(set(cfg.model.params) & set(LGBM_THREAD_ALIASES))
    need(not aliases, f"model.params must set num_threads, not its aliases {aliases}")
    need(bool(cfg.eval.test_batches), "eval.test_batches must not be empty")
    need(cfg.eval.k_headline in cfg.eval.k_values, "eval.k_headline must be in eval.k_values")
    need(all(k > 0 for k in cfg.eval.k_values + cfg.eval.gain_curve_k), "K values must be > 0")
    need(all(0 < f <= 1 for f in cfg.eval.fractions), "eval.fractions must be in (0, 1]")
    need(cfg.eval.bootstrap_samples > 0, "eval.bootstrap_samples must be > 0")
    need(cfg.eval.importance_top_n >= 0, "eval.importance_top_n must be >= 0")
    unknown = sorted(set(cfg.eval.baselines) - set(KNOWN_BASELINES))
    need(not unknown, f"eval.baselines has unknown names {unknown}")
    v2_keys = {"inlinks_cap", "ext_cap", "prior_cap", "w_inlinks", "w_ext", "w_url", "w_prior"}
    need(set(cfg.eval.v2_formula) == v2_keys, f"eval.v2_formula must have keys {sorted(v2_keys)}")
    need(cfg.manifest.index_shards > 0, "manifest.index_shards must be > 0")
    need(cfg.manifest.reason_top_n >= 0, "manifest.reason_top_n must be >= 0")
    need(min(cfg.manifest.canonical_workers, cfg.manifest.canonical_batch_rows) > 0,
         "manifest.canonical_workers and manifest.canonical_batch_rows must be > 0")
    need(cfg.duckdb.threads > 0, "duckdb.threads must be > 0")
    _validate_full(cfg, need)


def _validate_full(cfg: Config, need: Any) -> None:
    f, g = cfg.full, cfg.guard
    need(f.chunk_blocks > 0 and f.chunks_per_file > 0,
         "full.chunk_blocks and full.chunks_per_file must be > 0")
    # A chunk read at the worst-case speed must still finish inside its timeout.
    need(f.chunk_blocks * 8192 <= g.min_mb_s * 1_000_000 * g.timeout_cap_s,
         "full.chunk_blocks must be readable at guard.min_mb_s within guard.timeout_cap_s")
    need(f.min_free_gb >= 0, "full.min_free_gb must be >= 0")
    need(f.parse_threads > 0, "full.parse_threads must be > 0")
    need(f.connections > 0 and f.max_heap_mb_s > 0,
         "full.connections and full.max_heap_mb_s must be > 0")
    need(all(cfg.crawlerdb.session.get(k) == v for k, v in CSV_SESSION.items()),
         f"crawlerdb.session must set {CSV_SESSION} for the CSV text the full export parses")
    need(0 < f.sample_percent <= 100
         and abs(f.sample_percent * 1e4 - round(f.sample_percent * 1e4)) < 1e-9,
         "full.sample_percent must be in (0, 100] with at most 4 decimals")
    need(bool(f.sample_salt) and f.sample_salt != cfg.dataset.neg_salt,
         "full.sample_salt must be set and differ from dataset.neg_salt")
    need(0 < f.agg_share < 1, "full.agg_share must be in (0, 1)")
    need(bool(f.agg_salt) and f.agg_salt not in (f.sample_salt, cfg.dataset.neg_salt),
         "full.agg_salt must be set and differ from full.sample_salt and dataset.neg_salt")




def load_config(paths: Sequence[str | Path]) -> Config:
    """Loads and validates one or more TOML files; later files override earlier ones.

    Args:
        paths: Config files in override order.

    Returns:
        The validated, frozen config.

    Raises:
        ConfigError: If any file is missing keys, has unknown keys or bad values.
    """
    if not paths:
        raise ConfigError("at least one --config file is required")
    merged: dict[str, Any] = {}
    for path in paths:
        with open(path, "rb") as fh:
            merged = _deep_merge(merged, tomllib.load(fh))
    cfg = _build(Config, merged, "")
    _validate(cfg)
    return cfg
