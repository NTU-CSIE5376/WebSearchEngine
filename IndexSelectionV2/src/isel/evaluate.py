"""Recall@K of a scored pool, estimated from the row sample, with query bootstrap.

The official metric counts a raw golden URL as indexed iff the string equals a
row of the selected list. With a sample at rate p, the K-th best row of the
pool sits near rank round(K·p) of the sample, so that sample row is the
boundary: a golden row is listed iff it orders at or before the boundary under
(score desc, URL hash asc). With p = 1 this is the exact top-K.
"""

from __future__ import annotations

import random
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from isel.config import Config


def _per_query(queries: Mapping[Any, set[str]], hits: set) -> list[tuple[float, float]]:
    """(hit weight, total weight) per query; a URL shared by c queries weighs 1/c in each.

    Queries come in key order, so the draws do not depend on how rows were read.
    """
    shared = Counter(u for urls in queries.values() for u in urls)
    return [(sum(1 / shared[u] for u in sorted(queries[q]) if u in hits),
             sum(1 / shared[u] for u in sorted(queries[q]))) for q in sorted(queries)]


def _interval(values: list[float]) -> tuple[float, float]:
    values.sort()
    return values[int(0.025 * (len(values) - 1))], values[int(0.975 * (len(values) - 1))]


def bootstrap(queries: Mapping[Any, set], hits: set, samples: int, seed: int
              ) -> tuple[float, float]:
    """95% interval of the distinct-URL hit rate, resampling whole queries."""
    per_query = _per_query(queries, hits)
    rng = random.Random(seed)
    rates = []
    for _ in range(samples):
        draw = rng.choices(per_query, k=len(per_query))
        weight = sum(w for _, w in draw)
        rates.append(sum(h for h, _ in draw) / weight if weight else 0.0)
    return _interval(rates)


def paired_bootstrap(queries: Mapping[Any, set], hits_a: set, hits_b: set, samples: int,
                     seed: int) -> tuple[float, float]:
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


@dataclass
class Golden:
    """One batch's golden rows for one tag."""

    queries: dict[Any, set[str]]
    canonical: dict[str, str]

    @property
    def urls(self) -> list[str]:
        return sorted(self.canonical)


def golden_by_tag(rows: Iterable[Mapping[str, Any]], batch_id: int, tags: list[str],
                  key_prefix: Any = None) -> dict[str, Golden]:
    """Groups golden rows of `batch_id`; query keys are `(key_prefix, query_id)`."""
    out: dict[str, Golden] = {}
    for tag in tags:
        queries: dict[Any, set[str]] = defaultdict(set)
        canonical: dict[str, str] = {}
        for r in rows:
            if r["batch_id"] == batch_id and tag in (r["tags"] or []):
                queries[(key_prefix, r["query_id"])].add(r["raw_url"])
                canonical[r["raw_url"]] = r["canonical_url"]
        if canonical:
            out[tag] = Golden(dict(queries), canonical)
    return out


class Ranker:
    """Orders sample rows once and answers which golden rows a K-list holds."""

    def __init__(self, scores: np.ndarray, frame: pa.Table, rate: float) -> None:
        scores = np.nan_to_num(np.asarray(scores, dtype=np.float64), nan=-np.inf)
        h = frame.column("h").to_numpy(zero_copy_only=False).astype(np.uint64)
        sample = frame.column("is_sample").to_numpy(zero_copy_only=False).astype(bool)
        label = frame.column("label").to_numpy(zero_copy_only=False).astype(bool)
        self.rate = rate
        self._s, self._h = scores[sample], h[sample]
        self._order = np.lexsort((self._h, -self._s))
        self.n_sample = int(sample.sum())
        idx = np.flatnonzero(label)
        self._label_s, self._label_h = scores[idx], h[idx]
        urls = frame.column("url").take(pa.array(idx)).to_pylist()
        self.label_urls = np.array(urls, dtype=object)

    def rank_for_k(self, k: float) -> int:
        return max(1, round(k * self.rate))

    def listed_at_rank(self, rank: int) -> set[str]:
        """Golden-row URLs that order at or before sample rank `rank` (1-based)."""
        if rank >= self.n_sample:
            return set(self.label_urls.tolist())
        b = self._order[rank - 1]
        bs, bh = self._s[b], self._h[b]
        mask = (self._label_s > bs) | ((self._label_s == bs) & (self._label_h <= bh))
        return set(self.label_urls[mask].tolist())


def rates(golden: Golden, listed: set[str]) -> tuple[set[str], float, float]:
    """(exact hits, exact rate, canonical rate) of one tag under one list."""
    urls = golden.urls
    exact = {u for u in urls if u in listed}
    canonical = sum(u in listed or golden.canonical[u] in listed for u in urls)
    return exact, len(exact) / len(urls), canonical / len(urls)



def metric_rate(golden: Golden, listed: set[str]) -> float:
    """The official `indexed_rate`: distinct canonical golden URLs found in the list.

    WebSearchEngine `09c732e` canonicalizes each golden URL, looks only that key
    up in `selected_urls_current` and counts raw URLs sharing a key once.
    """
    keys = set(golden.canonical.values())
    return sum(k in listed for k in keys) / len(keys)

def evaluate_batch(cfg: Config, frame: pa.Table, scores: Mapping[str, np.ndarray],
                   golden: Mapping[str, Golden], rate: float, reference: str
                   ) -> tuple[dict[str, Any], dict[str, dict[str, set[str]]]]:
    """Metrics of every scorer on one batch, plus exact-hit sets at the headline K."""
    e = cfg.eval
    pool_urls = set(frame.filter(pc.equal(frame.column("label"), 1)).column("url").to_pylist())
    out: dict[str, Any] = {"tags": {}}
    hits: dict[str, dict[str, set[str]]] = defaultdict(dict)
    rankers = {name: Ranker(s, frame, rate) for name, s in scores.items()}
    n_sample = next(iter(rankers.values())).n_sample if rankers else 0
    out["pool_rows_est"] = n_sample / rate
    for tag, g in golden.items():
        urls = g.urls
        tag_out: dict[str, Any] = {
            "golden_urls": len(urls),
            "queries": len(g.queries),
            "crawled_exact_rate": sum(u in pool_urls for u in urls) / len(urls),
            "crawled_canonical_rate": sum(u in pool_urls or g.canonical[u] in pool_urls
                                          for u in urls) / len(urls),
            "scorers": {},
        }
        for name, ranker in rankers.items():
            res: dict[str, Any] = {"k": {}, "fractions": {}, "gain_curve": {}}
            for k in e.k_values:
                exact, r_exact, r_canon = rates(g, ranker.listed_at_rank(ranker.rank_for_k(k)))
                res["k"][str(k)] = {"indexed_exact_rate": r_exact,
                                    "indexed_canonical_rate": r_canon}
                if k == e.k_headline:
                    hits[name][tag] = exact
                    res["k"][str(k)]["ci95"] = list(bootstrap(
                        g.queries, exact, e.bootstrap_samples, e.bootstrap_seed))
            for f in e.fractions:
                _, r_exact, r_canon = rates(g, ranker.listed_at_rank(
                    max(1, round(f * ranker.n_sample))))
                res["fractions"][str(f)] = {"indexed_exact_rate": r_exact,
                                            "indexed_canonical_rate": r_canon}
            for k in e.gain_curve_k:
                res["gain_curve"][str(k)] = rates(
                    g, ranker.listed_at_rank(ranker.rank_for_k(k)))[1]
            tag_out["scorers"][name] = res
        if reference in hits:
            for name in rankers:
                if name != reference:
                    tag_out["scorers"][name]["diff_vs_reference_ci95"] = list(paired_bootstrap(
                        g.queries, hits[reference][tag], hits[name][tag],
                        e.bootstrap_samples, e.bootstrap_seed))
        out["tags"][tag] = tag_out
    return out, hits


def calibration(probabilities: np.ndarray, frame: pa.Table, rate: float,
                golden: Mapping[str, Golden]) -> dict[str, float]:
    """Expected crawled golden pages (Σp over the pool) against the observed count."""
    sample = frame.column("is_sample").to_numpy(zero_copy_only=False).astype(bool)
    expected = float(np.nansum(probabilities[sample])) / rate
    pool_urls = set(frame.filter(pc.equal(frame.column("label"), 1)).column("url").to_pylist())
    observed = {c for g in golden.values() for u, c in g.canonical.items()
                if u in pool_urls or c in pool_urls}
    return {"expected": expected, "observed": float(len(observed)),
            "ratio": expected / len(observed) if observed else float("nan")}


def pooled(cfg: Config, queries: Mapping[str, dict[Any, set]],
           hits: Mapping[str, Mapping[str, set]], reference: str) -> dict[str, Any]:
    """Headline-K rates over every test batch, with bootstrap and paired intervals."""
    e = cfg.eval
    out: dict[str, Any] = {}
    for tag, q in queries.items():
        urls = set().union(*q.values()) if q else set()
        tag_out: dict[str, Any] = {"golden_urls": len(urls), "queries": len(q), "scorers": {}}
        for name, by_tag in hits.items():
            found = by_tag.get(tag, set())
            entry: dict[str, Any] = {
                "indexed_exact_rate": len(found & urls) / len(urls) if urls else None,
                "ci95": list(bootstrap(q, found, e.bootstrap_samples, e.bootstrap_seed)),
            }
            if name != reference and reference in hits:
                entry["diff_vs_reference_ci95"] = list(paired_bootstrap(
                    q, hits[reference].get(tag, set()), found,
                    e.bootstrap_samples, e.bootstrap_seed))
            tag_out["scorers"][name] = entry
        out[tag] = tag_out
    return out


def render_report(results: Mapping[str, Any]) -> str:
    """Markdown summary of a backtest or forward evaluation."""
    k = results["k_headline"]
    lines = [f"# {results['title']}", "",
             f"- reference scorer: `{results['reference']}`",
             f"- headline K: {k:,}; sample rate: {results['sample_rate']}",
             f"- snapshot S0: {results['snapshot_at']}", ""]
    if results.get("pooled"):
        lines += ["## Pooled over test batches (indexed exact rate at headline K)", "",
                  "| tag | scorer | rate | 95% CI | diff vs reference (95% CI) |",
                  "|---|---|---|---|---|"]
        for tag, t in results["pooled"].items():
            for name, s in t["scorers"].items():
                lo, hi = s["ci95"]
                diff = s.get("diff_vs_reference_ci95")
                d = "-" if diff is None else f"{diff[0]:+.2%} … {diff[1]:+.2%}"
                lines.append(f"| {tag} | {name} | {s['indexed_exact_rate']:.2%} | "
                             f"{lo:.2%}–{hi:.2%} | {d} |")
        lines.append("")
    for batch, b in results["batches"].items():
        if b.get("skipped"):
            lines += [f"## Batch {batch}: skipped ({b['skipped']})", ""]
            continue
        lines += [f"## Batch {batch} (forward: {b.get('forward', False)})", "",
                  "| tag | crawled (exact) | scorer | indexed exact | 95% CI | canonical |",
                  "|---|---|---|---|---|---|"]
        for tag, t in b["tags"].items():
            for name, s in t["scorers"].items():
                at = s["k"][str(k)]
                lo, hi = at["ci95"]
                lines.append(f"| {tag} | {t['crawled_exact_rate']:.2%} | {name} | "
                             f"{at['indexed_exact_rate']:.2%} | {lo:.2%}–{hi:.2%} | "
                             f"{at['indexed_canonical_rate']:.2%} |")
        lines.append("")
    return "\n".join(lines) + "\n"
