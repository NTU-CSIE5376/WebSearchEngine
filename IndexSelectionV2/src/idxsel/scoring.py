"""Rule-based scoring: tiers, then a weighted sum of normalized signals.

Every signal is mapped into [0, 1] with fixed caps from the config (not
min-max over the data), so a URL's score does not drift between cycles just
because an outlier appeared somewhere else. DuckDB's `greatest`/`least` skip
NULL arguments, so NULL inputs are handled with explicit CASE expressions.

Domain signals (`prior_rate`, `domain_ext`) need one aggregation over the whole
feature set (`build_domain_stats`); it runs only for profiles that use them.
"""

from __future__ import annotations

import dataclasses
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from idxsel.config import Config, ProfileCfg
from idxsel.duck import copy_atomic, sql_str
from idxsel.manifest import atomic_write_json, read_json

KEY_BITS = 52
KEY_SCALE = 2**KEY_BITS
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# Year, month and optional day from the path; NULL without a valid date.
_URL_DATE = ("CAST(TRY_CAST(regexp_extract(f.url, {url_date_re}, 1) || '-' || "
             "regexp_extract(f.url, {url_date_re}, 2) || '-' || "
             "coalesce(nullif(regexp_extract(f.url, {url_date_re}, 3), ''), '01') AS DATE)"
             " AS TIMESTAMPTZ)")
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
    "short": "1.0 - least(length(f.url), {url_len}) / {url_len}",
    "root": "CAST(regexp_matches(f.url, '^https?://[^/?#]+/?$') AS DOUBLE)",
    "query": "CAST(contains(f.url, '?') AS DOUBLE)",
    "anchor": "CAST(coalesce(f.has_anchor, false) AS DOUBLE)",
    "redirect": "CAST(coalesce(f.is_redirect, false) AS DOUBLE)",
    "fresh": "least(1.0, exp(-greatest(date_diff('second', f.last_fetch_ok, {as_of}), 0)"
             " / 86400.0 / {fresh_tau_days}))",
    # Golden rate of the domain over its mean, as a log lift clipped to +-ln(lift).
    "prior_rate": "(least(greatest(ln((coalesce(ds.g, 0) + {pr_a})"
                  " / ((coalesce(ds.e, 0) + {pr_a_over_p0}) * {pr_p0})),"
                  " -ln({prior_rate_lift})), ln({prior_rate_lift})) + ln({prior_rate_lift}))"
                  " / (2 * ln({prior_rate_lift}))",
    "domain_ext": "least(ln(1 + coalesce(ds.ext_inlinks, 0)), ln(1 + {domain_ext_inlinks}))"
                  " / ln(1 + {domain_ext_inlinks})",
    # A date in the path is the page's own time evidence; the crawler cannot move it.
    "url_date": f"CASE WHEN {_URL_DATE} IS NULL THEN 0.0 ELSE exp(-greatest(date_diff("
                f"'second', {_URL_DATE}, {{as_of}}), 0) / 86400.0 / {{url_date_tau_days}}) END",
    "json_ld": "CAST(coalesce(f.has_json_ld, false) AS DOUBLE)",
    "hreflang": "CAST(coalesce(f.hreflang_count, 0) > 0 AS DOUBLE)",
    "no_title": "CAST(coalesce(f.title_null, false) AS DOUBLE)",
}
DOMAIN_SIGNALS = frozenset({"prior_rate", "domain_ext"})
_TIER_EXPR: dict[str, str] = {
    "past_golden": "pg.canonical_url IS NOT NULL",
    "external_inlink": "f.inlink_count_external > 0",
}


@dataclass(frozen=True)
class DomainInputs:
    """Per-domain stats file and p0, the mean golden rate over all domains."""

    stats: Path
    p0: float


def _num(x: float) -> str:
    return repr(float(x))


def uses_domain_stats(profile: ProfileCfg) -> bool:
    weights = dataclasses.asdict(profile.weights)
    return any(weights[name] != 0 for name in DOMAIN_SIGNALS)


def _signal(name: str, params: dict[str, str], domain: DomainInputs | None) -> str:
    if name == "prior_rate" and domain is not None and domain.p0 <= 0:
        return "0.5"  # no golden hit in any domain yet: every domain is average
    return _SIGNALS[name].format(**params)


def score_expressions(cfg: Config, profile: ProfileCfg, as_of: datetime,
                      domain: DomainInputs | None = None) -> tuple[str, str]:
    """Returns SQL for (tier, score_unit) under `profile`."""
    caps = {k: _num(v) for k, v in dataclasses.asdict(cfg.selection.caps).items()}
    a = cfg.golden.prior_rate_pseudo_golden
    p0 = domain.p0 if domain is not None and domain.p0 > 0 else 1.0
    params = {
        **caps,
        "impute_parent": _num(cfg.selection.impute.parent_page_score),
        "impute_url_score": _num(cfg.selection.impute.url_score),
        "natural_source": str(int(cfg.selection.url_score_natural_source)),
        "pageview_source": str(int(cfg.selection.pageview_source)),
        "as_of": f"{sql_str(as_of.isoformat())}::TIMESTAMPTZ",
        "url_date_re": sql_str(cfg.selection.url_date_regex),
        "pr_a": _num(a), "pr_p0": _num(p0), "pr_a_over_p0": _num(a / p0),
    }
    weights = dataclasses.asdict(profile.weights)
    terms = [f"({_num(w)}) * ({_signal(name, params, domain)})"
             for name, w in weights.items() if w != 0]
    raw_min = sum(min(0.0, w) for w in weights.values())
    raw_max = sum(max(0.0, w) for w in weights.values())
    unit = f"(({' + '.join(terms)}) - ({_num(raw_min)})) / ({_num(raw_max - raw_min)})"
    n = len(profile.tiers)
    cases = " ".join(f"WHEN {_TIER_EXPR[t]} THEN {n - i}" for i, t in enumerate(profile.tiers))
    tier = f"CASE {cases} ELSE 0 END" if cases else "0"
    return tier, unit


def candidate_filter(cfg: Config) -> str:
    """SQL predicate on feature row `f` for the configured candidate filters."""
    preds = ["TRUE"]
    f = cfg.selection.filters
    if f.exclude_robots_bits:
        bits = ", ".join(str(int(b)) for b in f.exclude_robots_bits)
        preds.append(f"f.robots_bits NOT IN ({bits})")
    if f.max_consecutive_fail >= 0:
        preds.append(f"coalesce(f.num_consecutive_fail, 0) <= {int(f.max_consecutive_fail)}")
    return " AND ".join(preds)


def scored_select_sql(cfg: Config, features_glob: str, golden_dir: Path, as_of: datetime,
                      profile: ProfileCfg, domain: DomainInputs | None = None) -> str:
    """SQL producing (url, shard, first_seen, tier, key52, h, score) for every candidate.

    With `selection.emit_raw_spelling_for_past_golden`, a past-golden URL is
    listed in its newest raw golden spelling, because the metric compares raw
    strings exactly.

    Raises:
        ValueError: If `profile` uses a domain signal and `domain` is None.
    """
    needs_domain = uses_domain_stats(profile)
    if needs_domain and domain is None:
        raise ValueError("the profile uses domain signals; pass the domain stats")
    tier, unit = score_expressions(cfg, profile, as_of, domain)
    past = sql_str(str(golden_dir / "past_golden.parquet"))
    prior = sql_str(str(golden_dir / "domain_prior.parquet"))
    stats_join = (f"LEFT JOIN read_parquet({sql_str(str(domain.stats))}) ds "
                  f"ON ds.domain_id = f.domain_id" if needs_domain and domain else "")
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
            {stats_join}
            WHERE {candidate_filter(cfg)}
        )
        SELECT url, shard, first_seen, tier,
               CAST(least(floor(score_unit * {KEY_SCALE}), {KEY_SCALE - 1}) AS BIGINT) AS key52,
               md5_number_lower(url) AS h,
               tier + score_unit AS score
        FROM x
    """


def build_domain_stats(cfg: Config, con: duckdb.DuckDBPyConnection, features_glob: str,
                       golden_dir: Path, out: Path) -> DomainInputs:
    """Writes per-domain candidate stats and golden exposure; returns them with p0.

    For each past batch b (the golden set's `batches_used`), with cutoff
    t_b = created_b - build_start_h_before_measure and weight
    w_b = 0.5 ** (age_b / prior_half_life_days):
    - e: sum of w_b * (candidates of the domain fetched by t_b). The crawler
      fetches a URL once, so today's feature set rebuilds that count.
    - g: sum of w_b * (batch-b golden URLs whose raw string is a candidate of the
      domain fetched by t_b): only hits a list built then could have scored.
    The weighted sums run inside each row in a fixed order and p0 uses
    `math.fsum`, so the output is bit-identical across runs and threads.
    """
    meta = read_json(golden_dir / "meta.json")
    as_of = datetime.fromisoformat(str(meta["as_of"]))
    golden = sql_str(str(golden_dir / "golden.parquet"))
    created = {b: _EPOCH + timedelta(microseconds=us) for b, us in con.execute(
        f"SELECT batch_id, epoch_us(any_value(batch_created_at)) FROM read_parquet({golden}) "
        f"GROUP BY batch_id").fetchall()}
    lead = timedelta(hours=cfg.schedule.build_start_h_before_measure)
    half_life = cfg.golden.prior_half_life_days
    batches = [(int(b), created[b] - lead,
                0.5 ** ((as_of - created[b]).total_seconds() / 86400 / half_life))
               for b in sorted(meta["batches_used"]) if b in created]
    feats = f"read_parquet({sql_str(features_glob)}, hive_partitioning = false)"
    cutoff = {b: f"{sql_str(t.isoformat())}::TIMESTAMPTZ" for b, t, _ in batches}
    counts = "".join(f", count_if(f.last_fetch_ok <= {cutoff[b]}) AS c{b}" for b, _, _ in batches)
    hits = "".join(f", count(DISTINCT g.raw_url) FILTER (WHERE g.batch_id = {b} AND "
                   f"f.last_fetch_ok <= {cutoff[b]}) AS g{b}" for b, _, _ in batches)
    e = " + ".join(f"{_num(w)} * c.c{b}" for b, _, w in batches) or "0.0"
    g = " + ".join(f"{_num(w)} * coalesce(h.g{b}, 0)" for b, _, w in batches) or "0.0"
    used = ", ".join(str(b) for b, _, _ in batches) or "NULL"
    copy_atomic(con, f"""
        WITH c AS (
            SELECT f.domain_id, count(*) AS crawled,
                   CAST(sum(greatest(coalesce(f.inlink_count_external, 0), 0)) AS BIGINT)
                       AS ext_inlinks{counts}
            FROM {feats} f WHERE {candidate_filter(cfg)} GROUP BY f.domain_id
        ), h AS (
            SELECT f.domain_id{hits}
            FROM {feats} f
            JOIN (SELECT batch_id, raw_url FROM read_parquet({golden})
                  WHERE batch_id IN ({used})) g ON g.raw_url = f.url
            WHERE {candidate_filter(cfg)} GROUP BY f.domain_id
        )
        SELECT c.domain_id, c.crawled, c.ext_inlinks,
               CAST({e} AS DOUBLE) AS e, CAST({g} AS DOUBLE) AS g
        FROM c LEFT JOIN h ON h.domain_id = c.domain_id
        WHERE c.domain_id IS NOT NULL
    """, out)
    return DomainInputs(out, mean_rate(out))


def mean_rate(stats: Path) -> float:
    """p0 = sum(g) / sum(e) over every domain; 0 without exposure.

    `math.fsum` is exactly rounded, so p0 does not depend on the order of the
    rows, which a parallel engine sum would.
    """
    table = pq.read_table(stats, columns=["e", "g"])
    total_e = math.fsum(table["e"].to_pylist())
    return math.fsum(table["g"].to_pylist()) / total_e if total_e > 0 else 0.0


def build_scored(cfg: Config, con: duckdb.DuckDBPyConnection, features_dir: Path,
                 golden_dir: Path, run_dir: Path, as_of: datetime) -> dict[str, Any]:
    """Writes scored candidates for the active profile to `run_dir/scored/`."""
    out = run_dir / "scored"
    run_dir.mkdir(parents=True, exist_ok=True)
    features_glob = str(features_dir / "shard=*" / "*.parquet")
    domain = (build_domain_stats(cfg, con, features_glob, golden_dir,
                                 run_dir / "domain_stats.parquet")
              if uses_domain_stats(cfg.active_profile) else None)
    query = scored_select_sql(cfg, features_glob, golden_dir, as_of, cfg.active_profile, domain)
    con.execute(
        f"COPY ({query}) TO {sql_str(str(out))} "
        f"(FORMAT parquet, PARTITION_BY (shard), OVERWRITE_OR_IGNORE true, COMPRESSION zstd)"
    )
    scored_glob = sql_str(str(out / "**" / "*.parquet"))
    total, tiers = con.execute(
        f"SELECT count(*), histogram(tier) FROM read_parquet({scored_glob})").fetchone()
    summary = {"candidates": total, "tier_counts": {str(k): v for k, v in (tiers or {}).items()},
               "profile": cfg.selection.profile, "as_of": as_of,
               "p0": domain.p0 if domain else None}
    atomic_write_json(run_dir / "scored_summary.json", summary)
    return summary
