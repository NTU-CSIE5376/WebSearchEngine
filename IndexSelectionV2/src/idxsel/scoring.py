"""Rule-based scoring: tiers, then a weighted sum of normalized signals.

Every signal is mapped into [0, 1] with fixed caps from the config (not
min-max over the data), so a URL's score does not drift between cycles just
because an outlier appeared somewhere else. DuckDB's `greatest`/`least` skip
NULL arguments, so NULL inputs are handled with explicit CASE expressions.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

from idxsel.config import Config, ProfileCfg
from idxsel.duck import sql_str
from idxsel.manifest import atomic_write_json

KEY_BITS = 52
KEY_SCALE = 2**KEY_BITS

# Normalized signal expressions; `f` is the feature row, placeholders are caps.
_SIGNALS: dict[str, str] = {
    "inlinks": "least(ln(1 + greatest(f.inlink_count_approx, 0)), ln(1 + {inlinks}))"
               " / ln(1 + {inlinks})",
    "ext": "least(ln(1 + greatest(f.inlink_count_external, 0)), ln(1 + {ext_inlinks}))"
           " / ln(1 + {ext_inlinks})",
    "parent": "CASE WHEN f.parent_page_score IS NULL THEN {impute_parent}"
              " ELSE least(greatest(f.parent_page_score, 0), {parent_page_score})"
              " / {parent_page_score} END",
    "url_score": "CASE WHEN f.source = {natural_source} AND f.url_score BETWEEN 0 AND 1"
                 " THEN f.url_score ELSE {impute_url_score} END",
    # Wiki pageview injection stores 1 + pageviews / max_pageviews (WebCrawler
    # scripts/wiki_pageview_inject.py); golden injection's sentinel never qualifies.
    "pageview": "CASE WHEN f.source = {pageview_source} AND f.url_score BETWEEN 1 AND 2"
                " THEN f.url_score - 1 ELSE 0.0 END",
    "prior": "least(ln(1 + coalesce(dp.prior, 0)), ln(1 + {prior})) / ln(1 + {prior})",
    "fetch_ok": "least(ln(1 + greatest(coalesce(f.num_fetch_ok_90d, 0), 0)),"
                " ln(1 + {fetch_ok_90d})) / ln(1 + {fetch_ok_90d})",
    "short": "1.0 - least(length(f.url), {url_len}) / {url_len}",
    "root": "CAST(regexp_matches(f.url, '^https?://[^/?#]+/?$') AS DOUBLE)",
    "query": "CAST(contains(f.url, '?') AS DOUBLE)",
    "anchor": "CAST(coalesce(f.has_anchor, false) AS DOUBLE)",
    "redirect": "CAST(coalesce(f.is_redirect, false) AS DOUBLE)",
    "fresh": "least(1.0, exp(-greatest(date_diff('second', f.last_fetch_ok, {as_of}), 0)"
             " / 86400.0 / {fresh_tau_days}))",
}
_TIER_EXPR: dict[str, str] = {
    "past_golden": "pg.canonical_url IS NOT NULL",
    "external_inlink": "f.inlink_count_external > 0",
}


def _num(x: float) -> str:
    return repr(float(x))


def score_expressions(cfg: Config, profile: ProfileCfg, as_of: datetime) -> tuple[str, str]:
    """Returns SQL for (tier, score_unit) under `profile`."""
    caps = {k: _num(v) for k, v in dataclasses.asdict(cfg.selection.caps).items()}
    params = {
        **caps,
        "impute_parent": _num(cfg.selection.impute.parent_page_score),
        "impute_url_score": _num(cfg.selection.impute.url_score),
        "natural_source": str(int(cfg.selection.url_score_natural_source)),
        "pageview_source": str(int(cfg.selection.pageview_source)),
        "as_of": f"{sql_str(as_of.isoformat())}::TIMESTAMPTZ",
    }
    weights = dataclasses.asdict(profile.weights)
    terms = [f"({_num(w)}) * ({_SIGNALS[name].format(**params)})"
             for name, w in weights.items() if w != 0]
    raw_min = sum(min(0.0, w) for w in weights.values())
    raw_max = sum(max(0.0, w) for w in weights.values())
    unit = f"(({' + '.join(terms)}) - ({_num(raw_min)})) / ({_num(raw_max - raw_min)})"
    n = len(profile.tiers)
    cases = " ".join(f"WHEN {_TIER_EXPR[t]} THEN {n - i}" for i, t in enumerate(profile.tiers))
    tier = f"CASE {cases} ELSE 0 END" if cases else "0"
    return tier, unit


def _filters(cfg: Config) -> str:
    preds = ["TRUE"]
    f = cfg.selection.filters
    if f.exclude_robots_bits:
        bits = ", ".join(str(int(b)) for b in f.exclude_robots_bits)
        preds.append(f"f.robots_bits NOT IN ({bits})")
    if f.max_consecutive_fail >= 0:
        preds.append(f"coalesce(f.num_consecutive_fail, 0) <= {int(f.max_consecutive_fail)}")
    return " AND ".join(preds)


def scored_select_sql(cfg: Config, features_glob: str, golden_dir: Path, as_of: datetime,
                      profile: ProfileCfg) -> str:
    """SQL producing (url, shard, first_seen, tier, key52, h, score) for every candidate.

    With `selection.emit_raw_spelling_for_past_golden`, a past-golden URL is
    listed in its newest raw golden spelling, because the metric compares raw
    strings exactly.
    """
    tier, unit = score_expressions(cfg, profile, as_of)
    past = sql_str(str(golden_dir / "past_golden.parquet"))
    prior = sql_str(str(golden_dir / "domain_prior.parquet"))
    url = ("coalesce(pg.raw_url, f.url)" if cfg.selection.emit_raw_spelling_for_past_golden
           else "f.url")
    return f"""
        WITH x AS (
            SELECT {url} AS url, f.shard, f.first_seen,
                   CAST({tier} AS INTEGER) AS tier,
                   greatest(0.0, least(1.0, coalesce({unit}, 0.0))) AS score_unit
            FROM read_parquet({sql_str(features_glob)}, hive_partitioning = false) f
            LEFT JOIN read_parquet({past}) pg ON pg.canonical_url = f.url
            LEFT JOIN read_parquet({prior}) dp ON dp.domain_id = f.domain_id
            WHERE {_filters(cfg)}
        )
        SELECT url, shard, first_seen, tier,
               CAST(least(floor(score_unit * {KEY_SCALE}), {KEY_SCALE - 1}) AS BIGINT) AS key52,
               md5_number_lower(url) AS h,
               tier + score_unit AS score
        FROM x
    """


def build_scored(cfg: Config, con: duckdb.DuckDBPyConnection, features_dir: Path,
                 golden_dir: Path, run_dir: Path, as_of: datetime) -> dict[str, Any]:
    """Writes scored candidates for the active profile to `run_dir/scored/`."""
    out = run_dir / "scored"
    run_dir.mkdir(parents=True, exist_ok=True)
    query = scored_select_sql(cfg, str(features_dir / "shard=*" / "*.parquet"), golden_dir,
                              as_of, cfg.active_profile)
    con.execute(
        f"COPY ({query}) TO {sql_str(str(out))} "
        f"(FORMAT parquet, PARTITION_BY (shard), OVERWRITE_OR_IGNORE true, COMPRESSION zstd)"
    )
    scored_glob = sql_str(str(out / "**" / "*.parquet"))
    total, tiers = con.execute(
        f"SELECT count(*), histogram(tier) FROM read_parquet({scored_glob})").fetchone()
    summary = {"candidates": total, "tier_counts": {str(k): v for k, v in (tiers or {}).items()},
               "profile": cfg.selection.profile, "as_of": as_of}
    atomic_write_json(run_dir / "scored_summary.json", summary)
    return summary
