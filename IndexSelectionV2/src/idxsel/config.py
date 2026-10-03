"""Strict TOML configuration.

Every tunable lives in the config file. Loading fails on unknown keys, missing
keys, wrong types, and inconsistent values, so a typo cannot silently fall back
to a default. Several files can be layered; later files override earlier ones.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
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
    app_name_prefix: str
    k: int


@dataclass(frozen=True)
class CrawlerDbCfg:
    dsn_env: str
    num_shards: int
    url_table_fmt: str
    history_table_fmt: str
    domain_table: str
    split_table: str
    connect_timeout_s: int
    session: dict[str, str]


@dataclass(frozen=True)
class MetricDbCfg:
    dsn_env: str
    connect_timeout_s: int
    session: dict[str, str]


@dataclass(frozen=True)
class SelectDbCfg:
    admin_dsn_env: str
    reader_dsn_env: str
    reader_role: str
    reader_password_env: str
    connect_timeout_s: int
    max_db_bytes: int
    est_bytes_per_row: int
    swap_lock_timeout: str
    swap_retry_attempts: int
    swap_retry_backoff_s: float
    maintenance_work_mem: str
    copy_block_bytes: int
    keep_prev: bool


@dataclass(frozen=True)
class ExportCfg:
    chunk_pages: int
    chunks_per_file: int
    shard_order: str
    shards_per_ingestor: int
    history_chunk_ids: int
    history_overlap_ids: int
    history_prune_slack_hours: float
    history_row_bytes_estimate: int
    base_observation_slack_s: float
    full_refresh_every_n_cycles: int
    max_chunk_retries: int
    retry_backoff_s: list[float]
    parquet_compression: str
    parquet_compression_level: int


@dataclass(frozen=True)
class ThrottleCfg:
    rate_mb_s_initial: float
    rate_mb_s_min: float
    rate_mb_s_max: float
    increase_mb_s: float
    increase_every_healthy_chunks: int
    decrease_factor: float
    pause_backoff_s: list[float]


@dataclass(frozen=True)
class RemoteProbeCfg:
    enabled: bool
    command: list[str]
    data_mount: str
    root_mount: str
    timeout_s: float
    interval_s: float


@dataclass(frozen=True)
class GuardCfg:
    db_checks_enabled: bool
    host_meminfo_enabled: bool
    fail_closed_on_probe_error: bool
    max_datfrozenxid_age: int
    max_wraparound_vacuum_workers: int
    min_free_connections: int
    foreign_long_query_s: float
    foreign_app_name_patterns: list[str]
    db_io_wait_backends_soft: int
    db_io_wait_backends_hard: int
    db_lwlock_wait_backends_soft: int
    watchdog_interval_s: float
    work_dir_max_gb: float
    work_dir_scan_interval_s: float
    work_dir_min_free_gb: float
    build_reserve_gb: float
    host_mem_available_min_gb_start: float
    host_mem_available_min_gb_run: float
    remote_data_min_free_gb_start: float
    remote_data_min_free_gb_run: float
    remote_root_min_free_gb: float
    remote_io_psi_some_avg60_soft: float
    remote_io_psi_some_avg60_hard: float
    remote: RemoteProbeCfg


@dataclass(frozen=True)
class ScheduleCfg:
    enabled: bool
    tick_s: float
    measure_days: list[int]
    measure_time_utc: str
    blackout_daily_utc: list[str]
    blackout_measure_day_utc: list[str]
    swap_freeze_measure_day_utc: list[str]
    full_export_start_h_before_measure: float
    full_export_deadline_h_before_measure: float
    build_start_h_before_measure: float
    daily_delta_utc: str
    min_hours_between_deltas: float
    delta_max_h: float
    error_cooldown_s: float
    forward_eval_delay_h: float


@dataclass(frozen=True)
class DuckDbCfg:
    threads: int
    memory_limit: str
    max_temp_directory_size: str
    preserve_insertion_order: bool


@dataclass(frozen=True)
class GoldenCfg:
    target_batch: int | str
    prior_half_life_days: float
    prior_rate_pseudo_golden: float
    tldextract_offline: bool


@dataclass(frozen=True)
class FiltersCfg:
    exclude_robots_bits: list[int]
    max_consecutive_fail: int


@dataclass(frozen=True)
class CapsCfg:
    inlinks: float
    ext_inlinks: float
    parent_page_score: float
    prior: float
    prior_rate_lift: float
    domain_ext_inlinks: float
    url_len: float
    fresh_tau_days: float
    url_date_tau_days: float


@dataclass(frozen=True)
class ImputeCfg:
    parent_page_score: float
    url_score: float


@dataclass(frozen=True)
class WeightsCfg:
    inlinks: float
    ext: float
    url_score: float
    prior: float
    parent: float
    short: float
    root: float
    query: float
    anchor: float
    redirect: float
    fresh: float
    pageview: float
    prior_rate: float
    domain_ext: float
    url_date: float
    json_ld: float
    hreflang: float
    no_title: float


@dataclass(frozen=True)
class ProfileCfg:
    tiers: list[str]
    weights: WeightsCfg


@dataclass(frozen=True)
class SelectionCfg:
    profile: str
    exact_sort_max_rows: int
    max_dedupe_rounds: int
    url_score_natural_source: int
    pageview_source: int
    emit_raw_spelling_for_past_golden: bool
    url_date_regex: str
    filters: FiltersCfg
    caps: CapsCfg
    impute: ImputeCfg
    profiles: dict[str, ProfileCfg]


@dataclass(frozen=True)
class EvalCfg:
    batches: list[int]
    tags: list[str]
    lookup_chunk_size: int
    bootstrap_samples: int
    bootstrap_seed: int
    live_check_sample_size: int


@dataclass(frozen=True)
class LabelsCfg:
    sample_mod: int
    sweep_memory_limit: str
    top_domains: int


@dataclass(frozen=True)
class StorageCfg:
    keep_runs: int


@dataclass(frozen=True)
class LoggingCfg:
    level: str


@dataclass(frozen=True)
class Config:
    run: RunCfg
    crawlerdb: CrawlerDbCfg
    metricdb: MetricDbCfg
    selectdb: SelectDbCfg
    export: ExportCfg
    throttle: ThrottleCfg
    guard: GuardCfg
    schedule: ScheduleCfg
    duckdb: DuckDbCfg
    golden: GoldenCfg
    selection: SelectionCfg
    eval: EvalCfg
    labels: LabelsCfg
    storage: StorageCfg
    logging: LoggingCfg

    @property
    def active_profile(self) -> ProfileCfg:
        return self.selection.profiles[self.selection.profile]

    def sha256(self) -> str:
        """Returns a stable digest of the effective config, for run auditing."""
        payload = json.dumps(dataclasses.asdict(self), sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()


_SCALARS = (int, float, str, bool)
_KNOWN_TIERS = ("past_golden", "external_inlink")
_SHARD_ORDERS = ("sequential", "ingestor_round_robin")


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


def _validate(cfg: Config) -> None:
    errors: list[str] = []
    if cfg.run.k < 1:
        errors.append("run.k must be >= 1")
    if cfg.crawlerdb.num_shards <= 0:
        errors.append("crawlerdb.num_shards must be > 0")
    t = cfg.throttle
    if not (0 < t.rate_mb_s_min <= t.rate_mb_s_initial <= t.rate_mb_s_max):
        errors.append("throttle: need 0 < rate_mb_s_min <= rate_mb_s_initial <= rate_mb_s_max")
    if not (0 < t.decrease_factor < 1):
        errors.append("throttle.decrease_factor must be in (0, 1)")
    if not t.pause_backoff_s:
        errors.append("throttle.pause_backoff_s must not be empty")
    e = cfg.export
    if e.shard_order not in _SHARD_ORDERS:
        errors.append(f"export.shard_order must be one of {_SHARD_ORDERS}")
    if e.chunk_pages <= 0 or e.chunks_per_file <= 0 or e.history_chunk_ids <= 0:
        errors.append("export chunk sizes must be > 0")
    if not e.retry_backoff_s:
        errors.append("export.retry_backoff_s must not be empty")
    if e.shard_order == "ingestor_round_robin" and (
        cfg.crawlerdb.num_shards % e.shards_per_ingestor != 0
    ):
        errors.append("export.shards_per_ingestor must divide crawlerdb.num_shards")
    s = cfg.selection
    if s.profile not in s.profiles:
        errors.append(f"selection.profile '{s.profile}' is not in selection.profiles")
    for name, prof in s.profiles.items():
        bad = [tr for tr in prof.tiers if tr not in _KNOWN_TIERS]
        if bad:
            errors.append(f"selection.profiles.{name}.tiers has unknown tiers {bad}")
        if all(w == 0 for w in dataclasses.asdict(prof.weights).values()):
            errors.append(f"selection.profiles.{name}.weights must not all be zero")
    caps = dataclasses.asdict(s.caps)
    if any(v <= 0 for v in caps.values()):
        errors.append("selection.caps values must be > 0")
    if s.exact_sort_max_rows < 1:
        errors.append("selection.exact_sort_max_rows must be >= 1")
    if isinstance(cfg.golden.target_batch, str) and cfg.golden.target_batch != "next":
        errors.append("golden.target_batch must be 'next' or an integer batch id")
    if cfg.guard.remote.enabled and not cfg.guard.remote.command:
        errors.append("guard.remote.command must be set when guard.remote.enabled")
    st = cfg.storage
    if st.keep_runs < 1:
        errors.append("storage.keep_runs must be >= 1")
    if cfg.schedule.delta_max_h <= 0:
        errors.append("schedule.delta_max_h must be > 0")
    if cfg.guard.build_reserve_gb < 0:
        errors.append("guard.build_reserve_gb must be >= 0")
    if cfg.guard.work_dir_scan_interval_s < 0:
        errors.append("guard.work_dir_scan_interval_s must be >= 0")
    if cfg.eval.live_check_sample_size < 1:
        errors.append("eval.live_check_sample_size must be >= 1")
    if cfg.schedule.forward_eval_delay_h <= 0:
        errors.append("schedule.forward_eval_delay_h must be > 0")
    if cfg.golden.prior_rate_pseudo_golden <= 0:
        errors.append("golden.prior_rate_pseudo_golden must be > 0")
    if s.caps.prior_rate_lift <= 1:
        errors.append("selection.caps.prior_rate_lift must be > 1")
    try:
        if re.compile(s.url_date_regex).groups != 3:
            errors.append("selection.url_date_regex needs 3 groups: year, month, day")
    except re.error as exc:
        errors.append(f"selection.url_date_regex does not compile: {exc}")
    lb = cfg.labels
    if lb.sample_mod < 1 or lb.top_domains < 1:
        errors.append("labels.sample_mod and labels.top_domains must be >= 1")
    if not lb.sweep_memory_limit:
        errors.append("labels.sweep_memory_limit must not be empty")
    if errors:
        raise ConfigError("; ".join(errors))


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
