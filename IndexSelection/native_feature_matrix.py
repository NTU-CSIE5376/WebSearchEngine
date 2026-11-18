import os
from typing import Callable

import numpy as np

from training_common import TLD_COLS, URL_FEATURE_COLUMNS


_ENV_FLAG = os.environ.get("INDEXSELECTION_USE_NATIVE_FEATURES", "1").strip().lower()
_NATIVE_ENABLED = _ENV_FLAG not in {"0", "false", "no", "off"}
_NATIVE_IMPORT_ERROR: Exception | None = None
_NATIVE_LOOKUP_CACHE: dict[int, tuple[tuple[int, int], object]] = {}
_FALLBACK_LOGGED = False

if _NATIVE_ENABLED:
    try:
        from _url_features_native import DomainFreqLookup, build_feature_matrix_native, feature_count, tld_cols

        if feature_count() != len(URL_FEATURE_COLUMNS):
            raise RuntimeError(
                f"native feature count mismatch: native={feature_count()} python={len(URL_FEATURE_COLUMNS)}"
            )
        if list(tld_cols()) != list(TLD_COLS):
            raise RuntimeError("native TLD column order does not match training_common.TLD_COLS")
    except Exception as exc:  # pragma: no cover - exercised only when native import fails
        _NATIVE_IMPORT_ERROR = exc
        DomainFreqLookup = None
        build_feature_matrix_native = None
else:
    DomainFreqLookup = None
    build_feature_matrix_native = None


def _log_fallback(reason: Exception | str) -> None:
    global _FALLBACK_LOGGED
    if _FALLBACK_LOGGED:
        return
    print(f"[native_features] fallback_to_python reason={reason}", flush=True)
    _FALLBACK_LOGGED = True


def _get_domain_freq_lookup(domain_freq: dict[int, dict[str, int]]):
    shape = (
        len(domain_freq.get(1, {})),
        len(domain_freq.get(0, {})),
    )
    cache_key = id(domain_freq)
    cached = _NATIVE_LOOKUP_CACHE.get(cache_key)
    if cached is not None and cached[0] == shape:
        return cached[1]
    lookup = DomainFreqLookup(domain_freq.get(1, {}), domain_freq.get(0, {}))
    _NATIVE_LOOKUP_CACHE[cache_key] = (shape, lookup)
    return lookup


def build_feature_matrix_with_optional_native(
    urls: list[str],
    domain_freq: dict[int, dict[str, int]],
    *,
    progress_label: str,
    progress_interval: int,
    labels: list[int] | np.ndarray | None = None,
    use_leave_one_out: bool = False,
    python_builder: Callable[[list[str], dict[int, dict[str, int]], str, int, list[int] | np.ndarray | None, bool], np.ndarray],
) -> np.ndarray:
    if use_leave_one_out and labels is not None:
        _log_fallback("leave_one_out_requires_python_builder")
        return python_builder(urls, domain_freq, progress_label, progress_interval, labels, use_leave_one_out)

    if build_feature_matrix_native is None or DomainFreqLookup is None:
        if _NATIVE_ENABLED and _NATIVE_IMPORT_ERROR is not None:
            _log_fallback(_NATIVE_IMPORT_ERROR)
        return python_builder(urls, domain_freq, progress_label, progress_interval, labels, use_leave_one_out)

    try:
        lookup = _get_domain_freq_lookup(domain_freq)
        return build_feature_matrix_native(urls, lookup, progress_label, progress_interval)
    except Exception as exc:  # pragma: no cover - only hit when native execution fails at runtime
        _log_fallback(exc)
        return python_builder(urls, domain_freq, progress_label, progress_interval, labels, use_leave_one_out)
