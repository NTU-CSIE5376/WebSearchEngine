"""Feature logs per golden batch ("labels") and a fast profile sweep on them.

Rules of ML #29: keep the features a decision used, then tune and train on that
log. `write_labels` keeps, for one golden batch, the feature rows of its URLs
and a uniform hash sample of every candidate, as they were when the list was
built. Labels written after a measurement from the list that was live for it
(`forward`) are leak-free; any other label set is in sample.

`sweep` estimates a profile's top-K hit rate from a label set in seconds. It
sorts the sample in the production order (`select_topk.FINAL_ORDER`, best copy
per URL) and takes the row at rank round(K * n / N) as the boundary, where n
counts sample rows and N candidate rows. A golden URL is a hit when its best
copy sorts no later than that row, so ties and tiers behave as in production.
With `labels.sample_mod = 1` the result is the exact top-K. Golden rows never
join the sample: added at the sample's weight they would shift the boundary.
"""

from __future__ import annotations

import dataclasses
import shutil
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

import idxsel
from idxsel.config import Config, ConfigError, ProfileCfg
from idxsel.duck import copy_atomic, sql_str
from idxsel.evaluate import _bootstrap, _paired_bootstrap
from idxsel.manifest import atomic_write_json, read_json, utc_id
from idxsel.scoring import (
    DomainInputs,
    build_domain_stats,
    candidate_filter,
    scored_select_sql,
    uses_domain_stats,
)
from idxsel.select_topk import FINAL_ORDER

META = "meta.json"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def label_sets(root: Path) -> list[Path]:
    """Complete label sets (a batch id name and a meta file), oldest batch first."""
    if not root.exists():
        return []
    return sorted((p for p in root.iterdir()
                   if p.is_dir() and p.name.isdigit() and (p / META).exists()),
                  key=lambda p: int(p.name))


def _diagnostics(cfg: Config, con: duckdb.DuckDBPyConnection, feats: str, tmp: Path,
                 dims_dir: Path | None) -> dict[str, Any]:
    """List share and golden hits of the top domains; duplicate content of listed pages."""
    sample = f"read_parquet({sql_str(str(tmp / 'sample.parquet'))})"
    golden = f"read_parquet({sql_str(str(tmp / 'golden.parquet'))})"
    (listed,) = con.execute(f"SELECT count(*) FROM {sample} WHERE selected").fetchone()
    top = con.execute(
        f"SELECT domain_id, count(*) AS n FROM {sample} WHERE selected "
        f"GROUP BY domain_id ORDER BY n DESC, domain_id LIMIT {int(cfg.labels.top_domains)}"
    ).fetchall()
    hits = dict(con.execute(
        f"SELECT golden_domain_id, count(DISTINCT raw_url) FROM {golden} "
        f"WHERE selected_exact GROUP BY golden_domain_id").fetchall())
    names: dict[int, str] = {}
    if dims_dir is not None and top:
        ids = ", ".join(str(int(d)) for d, _ in top if d is not None) or "NULL"
        names = dict(con.execute(
            f"SELECT domain_id, domain FROM "
            f"read_parquet({sql_str(str(dims_dir / 'domain_state.parquet'))}) "
            f"WHERE domain_id IN ({ids})").fetchall())
    copies, with_dups, mean = con.execute(
        f"WITH s AS (SELECT content_hash64 FROM {sample} "
        f"           WHERE selected AND content_hash64 IS NOT NULL), "
        f"m AS (SELECT f.content_hash64, count(*) AS n FROM {feats} f "
        f"      WHERE f.content_hash64 IN (SELECT content_hash64 FROM s) "
        f"      AND {candidate_filter(cfg)} GROUP BY f.content_hash64) "
        f"SELECT count(*), count(*) FILTER (WHERE m.n > 1), avg(m.n) "
        f"FROM s JOIN m ON m.content_hash64 = s.content_hash64").fetchone()
    return {
        "sampled_listed": listed,
        "top_domains": [{"domain_id": d, "domain": names.get(d), "list_share": n / listed,
                         "golden_hits": hits.get(d, 0)} for d, n in top] if listed else [],
        "content_dup": {"sampled_listed_with_hash": copies,
                        "share_with_copies": with_dups / copies if copies else None,
                        "mean_copies": mean},
    }


def write_labels(cfg: Config, con: duckdb.DuckDBPyConnection, *, batch: int,
                 features_dir: Path, run_golden_dir: Path, eval_golden_dir: Path,
                 selected: Path, root: Path, dims_dir: Path | None, in_sample: bool,
                 forward: bool, extra: dict[str, Any]) -> Path | None:
    """Writes `root/<batch>/`; returns it, or None if a better label set exists.

    An existing label set is only replaced by an out-of-sample one replacing an
    in-sample one. The set is built in a hidden directory and renamed in place.

    Args:
        cfg: Config.
        con: DuckDB connection.
        batch: Golden batch id.
        features_dir: Feature set the list was selected from.
        run_golden_dir: Golden set the list used (past-golden tier and priors).
        eval_golden_dir: Golden set holding `batch`.
        selected: The list's `selected.parquet`.
        root: Labels root directory.
        dims_dir: Dims export for domain names, if any.
        in_sample: Whether the batch's numbers are in sample.
        forward: Whether this is a post-measurement log of the live list.
        extra: More meta fields (e.g. the batch's evaluation).
    """
    dest = root / str(batch)
    old = read_json(dest / META)
    if old is not None and not (old["in_sample"] and not in_sample):
        return None
    tmp = root / f".{batch}.{utc_id()}"
    tmp.mkdir(parents=True)
    try:
        feats = (f"read_parquet({sql_str(str(features_dir / 'shard=*' / '*.parquet'))}, "
                 f"hive_partitioning = false)")
        sel = f"read_parquet({sql_str(str(selected))})"
        gold = f"read_parquet({sql_str(str(eval_golden_dir / 'golden.parquet'))})"
        mod = int(cfg.labels.sample_mod)
        copy_atomic(con, f"SELECT f.*, s.url IS NOT NULL AS selected FROM {feats} f "
                         f"LEFT JOIN (SELECT DISTINCT url FROM {sel}) s ON s.url = f.url "
                         f"WHERE md5_number_lower(f.url) % {mod} = 0", tmp / "sample.parquet")
        copy_atomic(con, f"SELECT f.* FROM {feats} f SEMI JOIN ("
                         f"SELECT raw_url AS u FROM {gold} WHERE batch_id = {int(batch)} "
                         f"UNION ALL SELECT canonical_url FROM {gold} WHERE batch_id = {int(batch)}"
                         f") g ON g.u = f.url", tmp / "golden_features.parquet")
        for name in ("past_golden.parquet", "domain_prior.parquet"):
            shutil.copy2(run_golden_dir / name, tmp / name)
        domain = build_domain_stats(cfg, con, str(features_dir / "shard=*" / "*.parquet"),
                                    run_golden_dir, tmp / "domain_stats_full.parquet")
        copy_atomic(con, f"SELECT * FROM read_parquet({sql_str(str(domain.stats))}) WHERE "
                         f"domain_id IN (SELECT domain_id FROM read_parquet("
                         f"{sql_str(str(tmp / 'sample.parquet'))}) UNION ALL SELECT domain_id "
                         f"FROM read_parquet({sql_str(str(tmp / 'golden_features.parquet'))}))",
                    tmp / "domain_stats.parquet")
        domain.stats.unlink()
        as_of = datetime.fromisoformat(str(read_json(features_dir / META)["as_of"]))
        live = DomainInputs(tmp / "domain_stats.parquet", domain.p0)
        scored = scored_select_sql(cfg, str(tmp / "golden_features.parquet"), tmp, as_of,
                                   cfg.active_profile,
                                   live if uses_domain_stats(cfg.active_profile) else None)
        gf = f"read_parquet({sql_str(str(tmp / 'golden_features.parquet'))})"
        copy_atomic(con, f"""
            SELECT g.batch_id, g.batch_created_at, g.query_id, g.tags, g.raw_url,
                   g.canonical_url, g.domain_id AS golden_domain_id,
                   g.raw_url IN (SELECT url FROM {gf}) AS crawled_exact,
                   g.canonical_url IN (SELECT url FROM {gf}) AS crawled_canonical,
                   g.raw_url IN (SELECT url FROM {sel}) AS selected_exact,
                   g.canonical_url IN (SELECT url FROM {sel}) AS selected_canonical,
                   sc.tier, sc.score
            FROM {gold} g
            LEFT JOIN (SELECT url, max(tier) AS tier, max(score) AS score FROM ({scored})
                       GROUP BY url) sc ON sc.url = g.raw_url
            WHERE g.batch_id = {int(batch)}""", tmp / "golden.parquet")
        (candidates,) = con.execute(
            f"SELECT count(*) FROM {feats} f WHERE {candidate_filter(cfg)}").fetchone()
        (sample_rows,) = con.execute(
            f"SELECT count(*) FROM read_parquet({sql_str(str(tmp / 'sample.parquet'))})"
        ).fetchone()
        (created_us,) = con.execute(f"SELECT epoch_us(any_value(batch_created_at)) FROM {gold} "
                                    f"WHERE batch_id = {int(batch)}").fetchone()
        created = _EPOCH + timedelta(microseconds=created_us)
        atomic_write_json(tmp / META, {
            "batch_id": batch, "batch_created_at": created, "features_id": features_dir.name,
            "features_as_of": as_of, "in_sample": in_sample, "forward": forward,
            "profile": cfg.selection.profile, "k": cfg.run.k, "sample_mod": mod,
            "candidates": candidates, "sample_rows": sample_rows,
            "filters": dataclasses.asdict(cfg.selection.filters), "p0": domain.p0,
            "config_sha256": cfg.sha256(), "code_version": idxsel.__version__,
            "diagnostics": _diagnostics(cfg, con, feats, tmp, dims_dir), **extra})
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    if dest.exists():
        stale = root / f".{batch}.old.{utc_id()}"
        dest.rename(stale)
        tmp.rename(dest)
        shutil.rmtree(stale)
    else:
        tmp.rename(dest)
    return dest


def _key(tier: int, key52: int, h: int, url: str, shard: int) -> tuple[Any, ...]:
    """Sort key of `select_topk.FINAL_ORDER`: smaller sorts first."""
    return (-tier, -key52, h, url, shard)


def _hits(cfg: Config, con: duckdb.DuckDBPyConnection, label: Path, meta: dict[str, Any],
          profile: ProfileCfg) -> set[str]:
    """Listed URLs among the label's golden feature rows under `profile`."""
    domain = (DomainInputs(label / "domain_stats.parquet", float(meta["p0"]))
              if uses_domain_stats(profile) else None)
    as_of = datetime.fromisoformat(str(meta["features_as_of"]))

    def scored(name: str) -> str:
        return scored_select_sql(cfg, str(label / name), label, as_of, profile, domain)

    sample = scored("sample.parquet")
    (n,) = con.execute(f"SELECT count(*) FROM ({sample})").fetchone()
    rank = max(1, round(cfg.run.k * n / meta["candidates"])) if meta["candidates"] else 1
    boundary = con.execute(
        f"SELECT tier, key52, h, url, shard FROM ({sample}) "
        f"QUALIFY row_number() OVER (PARTITION BY url ORDER BY {FINAL_ORDER}) = 1 "
        f"ORDER BY {FINAL_ORDER} LIMIT 1 OFFSET {rank - 1}").fetchone()
    best: dict[str, tuple[Any, ...]] = {}
    for url, tier, key52, h, shard in con.execute(
            f"SELECT url, tier, key52, h, shard FROM ({scored('golden_features.parquet')})"
    ).fetchall():
        key = _key(tier, key52, h, url, shard)
        best[url] = min(best.get(url, key), key)
    if boundary is None:  # fewer distinct candidates than K: everything is listed
        return set(best)
    limit = _key(*boundary)
    return {u for u, key in best.items() if key <= limit}


def _rates(cfg: Config, queries: dict[Any, set[str]], hits: dict[str, set[str]],
           first: str) -> dict[str, Any]:
    urls = set().union(*queries.values())
    e = cfg.eval
    out = {}
    for name, listed in hits.items():
        found = urls & listed
        out[name] = {
            "golden_urls": len(urls), "indexed_exact": len(found),
            "indexed_exact_rate": len(found) / len(urls) if urls else None,
            "ci95": list(_bootstrap(queries, found, e.bootstrap_samples, e.bootstrap_seed)),
            "diff_vs_first_ci95": list(_paired_bootstrap(
                queries, urls & hits[first], found, e.bootstrap_samples, e.bootstrap_seed)),
        }
    return out


def sweep(cfg: Config, con: duckdb.DuckDBPyConnection, root: Path, profiles: Sequence[str],
          batches: Sequence[int] | None = None) -> dict[str, Any]:
    """Estimates each profile's indexed rate on every label set and pooled.

    Pooled results are reported for forward (leak-free) label sets and for all.
    `diff_vs_first_ci95` is the paired 95% interval of a profile's rate minus
    the first profile's, with both resampled on the same queries.

    Raises:
        ConfigError: For an unknown profile.
        ValueError: If a label set was written under other candidate filters.
    """
    unknown = [p for p in profiles if p not in cfg.selection.profiles]
    if unknown or not profiles:
        raise ConfigError(f"unknown profiles {unknown}" if unknown else "no profiles given")
    filters = dataclasses.asdict(cfg.selection.filters)
    first = profiles[0]
    pooled: dict[str, dict[str, Any]] = {
        scope: {"queries": defaultdict(lambda: defaultdict(set)),
                "hits": {p: set() for p in profiles}} for scope in ("forward", "all")}
    report: dict[str, Any] = {"k": cfg.run.k, "profiles": list(profiles), "batches": {}}
    for label in label_sets(root):
        meta = read_json(label / META)
        batch = int(meta["batch_id"])
        if batches and batch not in batches:
            continue
        if meta["filters"] != filters:
            raise ValueError(f"label set {batch} used filters {meta['filters']}, not {filters}")
        hits = {p: _hits(cfg, con, label, meta, cfg.selection.profiles[p]) for p in profiles}
        rows = con.execute(
            f"SELECT query_id, tags, raw_url FROM "
            f"read_parquet({sql_str(str(label / 'golden.parquet'))})").fetchall()
        entry: dict[str, Any] = {}
        for tag in cfg.eval.tags:
            queries: dict[Any, set[str]] = defaultdict(set)
            for qid, tags, url in rows:
                if tag in (tags or []):
                    queries[qid].add(url)
            if not queries:
                continue
            entry[tag] = _rates(cfg, queries, hits, first)
            for scope in ("forward", "all") if meta["forward"] else ("all",):
                for qid, urls in queries.items():
                    pooled[scope]["queries"][tag][f"{batch}:{qid}"] |= {
                        f"{batch}:{u}" for u in urls}
                for p in profiles:
                    pooled[scope]["hits"][p] |= {f"{batch}:{u}" for u in hits[p]}
        report["batches"][str(batch)] = {
            "in_sample": meta["in_sample"], "forward": meta["forward"],
            "features_id": meta["features_id"], "tags": entry}
    report["pooled"] = {
        scope: {tag: _rates(cfg, queries, data["hits"], first)
                for tag, queries in data["queries"].items()}
        for scope, data in pooled.items()}
    return report
