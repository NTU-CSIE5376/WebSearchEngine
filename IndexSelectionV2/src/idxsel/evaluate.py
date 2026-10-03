"""Evaluation that replicates the official metric, plus a funnel and confidence.

The official metric (`WebSearchEngine/Metric/Measure/CrawlerAllMetricMeasure.py:66-86`)
counts a raw golden URL as indexed iff the string equals a row of
`public.selected_urls_current`. `evaluate` applies the same exact match to the
run's own `selected.parquet`: DuckDB string equality and PostgreSQL text
equality (deterministic collation) both compare bytes. It therefore measures
the evaluated run even when that run is not live. `verify_live` then checks,
as the reader role, that the live table is the run that was evaluated.

A batch is in sample when it fed the past-golden tier or the prior, or when it
already existed while the feature set was observed: the crawler reacts to new
batches (WebCrawler `update_golden_domain_scores.py` re-ranks their domains
daily, `golden_inject.py` injects them after four weeks), so their URLs get
crawled because they are golden. Only a list built before its batch existed
gives an out-of-sample number.

`cutoff_loss` counts golden URLs the crawler fetched after the feature set was
observed but before the batch existed: no list built from that feature set
could hold them. It needs a later delta that covers the batch's creation.
"""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from idxsel.config import Config
from idxsel.duck import sql_str
from idxsel.manifest import atomic_write_json, read_json
from idxsel.pg import ReadOnlySession

LOOKUP_SQL = "SELECT url FROM public.selected_urls_current WHERE url = ANY(%s)"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def lookup(session: ReadOnlySession, urls: Iterable[str], chunk: int) -> set[str]:
    """Returns the subset of `urls` present in the live selection (metric's exact SQL)."""
    items = list(dict.fromkeys(urls))
    found: set[str] = set()
    for i in range(0, len(items), chunk):
        found.update(r[0] for r in session.fetchall(LOOKUP_SQL, (items[i:i + chunk],)))
    return found


def _per_query(queries: dict[Any, set[str]], hits: set[str]) -> list[tuple[float, float]]:
    """(hit weight, total weight) per query; a URL shared by c queries weighs 1/c in each.

    Queries come in id order, so the draws do not depend on how rows were read.
    """
    shared = Counter(u for urls in queries.values() for u in urls)
    return [(sum(1 / shared[u] for u in sorted(queries[q]) if u in hits),
             sum(1 / shared[u] for u in sorted(queries[q]))) for q in sorted(queries)]


def _interval(values: list[float]) -> tuple[float, float]:
    values.sort()
    return values[int(0.025 * (len(values) - 1))], values[int(0.975 * (len(values) - 1))]


def _bootstrap(queries: dict[Any, set[str]], hits: set[str], samples: int, seed: int
               ) -> tuple[float, float]:
    """95% interval of the distinct-URL hit rate, resampling whole queries.

    The full sample gives exactly the distinct-URL rate, and a query drawn twice
    counts twice.
    """
    per_query = _per_query(queries, hits)
    rng = random.Random(seed)
    rates = []
    for _ in range(samples):
        draw = rng.choices(per_query, k=len(per_query))
        weight = sum(w for _, w in draw)
        rates.append(sum(h for h, _ in draw) / weight if weight else 0.0)
    return _interval(rates)


def _paired_bootstrap(queries: dict[Any, set[str]], hits_a: set[str], hits_b: set[str],
                      samples: int, seed: int) -> tuple[float, float]:
    """95% interval of rate(b) - rate(a); both rates use the same query draws."""
    a, b = _per_query(queries, hits_a), _per_query(queries, hits_b)
    rng = random.Random(seed)
    index = range(len(a))
    diffs = []
    for _ in range(samples):
        draw = rng.choices(index, k=len(a))
        weight = sum(a[i][1] for i in draw)
        diffs.append((sum(b[i][0] for i in draw) - sum(a[i][0] for i in draw)) / weight
                     if weight else 0.0)
    return _interval(diffs)


def target_batches(cfg: Config, golden_meta: dict[str, Any], golden_path: Path) -> list[int]:
    if cfg.eval.batches:
        return list(cfg.eval.batches)
    if golden_meta.get("target_batch") is not None:
        return [int(golden_meta["target_batch"])]
    batch_ids = pq.read_table(golden_path, columns=["batch_id"])["batch_id"].to_pylist()
    return [max(batch_ids)] if batch_ids else []


def _late_fetches(con: duckdb.DuckDBPyConnection, golden: str, deltas: Sequence[Path],
                  after: datetime) -> dict[str, datetime]:
    """First fetch after `after` of each golden URL (raw or canonical) in the deltas."""
    files = [f for d in deltas for f in sorted(d.glob("shard=*/*.parquet"))]
    if not files:
        return {}
    paths = "[" + ", ".join(sql_str(str(f)) for f in files) + "]"
    rows = con.execute(
        f"SELECT d.url, epoch_us(min(d.last_fetch_ok)) "
        f"FROM read_parquet({paths}, hive_partitioning = false) d "
        f"SEMI JOIN (SELECT raw_url AS u FROM read_parquet({golden}) UNION ALL "
        f"SELECT canonical_url FROM read_parquet({golden})) g ON g.u = d.url "
        f"WHERE d.last_fetch_ok > {sql_str(after.isoformat())}::TIMESTAMPTZ GROUP BY d.url"
    ).fetchall()
    return {u: _EPOCH + timedelta(microseconds=us) for u, us in rows}


def evaluate(cfg: Config, con: duckdb.DuckDBPyConnection, selected: Path, golden_dir: Path,
             features_dir: Path, golden_meta: dict[str, Any], out_dir: Path, *,
             deltas: Sequence[Path] = (), covered_until: datetime | None = None
             ) -> dict[str, Any]:
    """Computes the metric replica, funnel and bootstrap CI per (batch, tag).

    A batch that fed the past-golden tier or the prior, or that existed when the
    feature set was observed, is marked `in_sample`: its rate is inflated and
    does not predict the next batch.

    Args:
        cfg: Config.
        con: DuckDB connection.
        selected: The evaluated run's `selected.parquet`.
        golden_dir: Golden set holding the evaluated batches.
        features_dir: The feature set the run was selected from.
        golden_meta: The golden set's meta (target batch, batches used).
        out_dir: Where `eval.json` and `report.md` go.
        deltas: Delta exports newer than the feature set, for `cutoff_loss`.
        covered_until: The time up to which `deltas` hold every shard's history.
    """
    golden_path = golden_dir / "golden.parquet"
    golden = sql_str(str(golden_path))
    batches = target_batches(cfg, golden_meta, golden_path)
    used = set(golden_meta.get("batches_used", []))
    features_as_of = datetime.fromisoformat(str(read_json(features_dir / "meta.json")["as_of"]))
    features_glob = sql_str(str(features_dir / "shard=*" / "*.parquet"))
    crawled = {r[0] for r in con.execute(
        f"SELECT DISTINCT f.url FROM read_parquet({features_glob}, hive_partitioning = false) f "
        f"SEMI JOIN (SELECT raw_url AS u FROM read_parquet({golden}) UNION ALL "
        f"SELECT canonical_url FROM read_parquet({golden})) g ON g.u = f.url"
    ).fetchall()}
    late = _late_fetches(con, golden, deltas, features_as_of)
    listed = {r[0] for r in con.execute(
        f"SELECT DISTINCT s.url FROM read_parquet({sql_str(str(selected))}) s "
        f"SEMI JOIN (SELECT raw_url AS u FROM read_parquet({golden}) UNION ALL "
        f"SELECT canonical_url FROM read_parquet({golden})) g ON g.u = s.url"
    ).fetchall()}
    rows = con.execute(
        f"SELECT batch_id, query_id, tags, raw_url, canonical_url, epoch_us(batch_created_at) "
        f"FROM read_parquet({golden})"
    ).fetchall()
    created = {r[0]: _EPOCH + timedelta(microseconds=r[5]) for r in rows}
    results: dict[str, Any] = {}
    for batch in batches:
        for tag in cfg.eval.tags:
            sel = [r for r in rows if r[0] == batch and tag in (r[2] or [])]
            if not sel:
                continue
            canon = {r[3]: r[4] for r in sel}
            queries: dict[int, set[str]] = defaultdict(set)
            for r in sel:
                queries[r[1]].add(r[3])
            raw_urls = list(canon)
            exact = {u for u in raw_urls if u in listed}
            canonical = {u for u in raw_urls if canon[u] in listed}
            crawled_raw = {u for u in raw_urls if canon[u] in crawled}
            crawled_exact = {u for u in raw_urls if u in crawled}
            n = len(raw_urls)
            covered = covered_until is not None and covered_until >= created[batch]

            def lost(url: str, batch: int = batch) -> bool:
                return url not in crawled and url in late and late[url] <= created[batch]

            loss_exact = sum(lost(u) for u in raw_urls) if covered else None
            loss_canon = sum(lost(canon[u]) for u in raw_urls) if covered else None
            lo, hi = _bootstrap(queries, exact, cfg.eval.bootstrap_samples,
                                cfg.eval.bootstrap_seed)
            results[f"batch{batch}_{tag}"] = {
                "in_sample": batch in used or created[batch] <= features_as_of,
                "batch_created_at": created[batch],
                "features_as_of": features_as_of,
                "golden_urls": n,
                "queries": len(queries),
                "crawled_in_export": len(crawled_raw),
                "crawled_rate": len(crawled_raw) / n,
                "crawled_exact": len(crawled_exact),
                "crawled_exact_rate": len(crawled_exact) / n,
                "cutoff_loss_exact": loss_exact,
                "cutoff_loss_exact_rate": None if loss_exact is None else loss_exact / n,
                "cutoff_loss_canonical": loss_canon,
                "cutoff_loss_canonical_rate": None if loss_canon is None else loss_canon / n,
                "indexed_exact": len(exact),
                "indexed_exact_rate": len(exact) / n,
                "indexed_exact_ci95": [lo, hi],
                "indexed_canonical": len(canonical),
                "indexed_canonical_rate": len(canonical) / n,
                "recall_of_crawled": (len(exact & crawled_raw) / len(crawled_raw)
                                      if crawled_raw else None),
            }
    report = {"batches": batches, "results": results}
    atomic_write_json(out_dir / "eval.json", report)
    (out_dir / "report.md").write_text(render_markdown(report), encoding="utf-8")
    return report


def verify_live(cfg: Config, con: duckdb.DuckDBPyConnection, reader: ReadOnlySession,
                selected: Path, db_run_id: int) -> dict[str, Any]:
    """Checks, as the metric's role, that the live table is run `db_run_id`, complete."""
    (rows,) = reader.fetchone("SELECT count(*) FROM public.selected_urls_current")
    live = reader.fetchone("SELECT run_id FROM public.selected_urls_current LIMIT 1")
    n = cfg.eval.live_check_sample_size
    sample = [r[0] for r in con.execute(
        f"SELECT url FROM read_parquet({sql_str(str(selected))}) "
        f"USING SAMPLE reservoir({int(n)} ROWS) REPEATABLE ({int(cfg.eval.bootstrap_seed)})"
    ).fetchall()]
    found = lookup(reader, sample, cfg.eval.lookup_chunk_size)
    result = {"rows": rows, "live_run_id": live[0] if live else None, "sampled": len(sample),
              "sample_found": len(found)}
    result["ok"] = (rows == cfg.run.k and result["live_run_id"] == db_run_id
                    and len(found) == len(sample))
    return result


def render_markdown(report: dict[str, Any]) -> str:
    lines = ["# Index Selection V2 evaluation", "",
             "| set | in sample | golden URLs | crawled | crawled (exact) | indexed (exact) |"
             " 95% CI | indexed (canonical) | recall of crawled | cutoff loss (exact) |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for name, e in report["results"].items():
        lo, hi = e["indexed_exact_ci95"]
        recall = "-" if e["recall_of_crawled"] is None else f"{e['recall_of_crawled']:.1%}"
        loss = ("-" if e["cutoff_loss_exact_rate"] is None
                else f"{e['cutoff_loss_exact_rate']:.1%}")
        lines.append(
            f"| {name} | {'yes' if e['in_sample'] else 'no'} | {e['golden_urls']} | "
            f"{e['crawled_rate']:.1%} | {e['crawled_exact_rate']:.1%} | "
            f"{e['indexed_exact_rate']:.1%} | {lo:.1%}–{hi:.1%} | "
            f"{e['indexed_canonical_rate']:.1%} | {recall} | {loss} |")
    return "\n".join(lines) + "\n"
