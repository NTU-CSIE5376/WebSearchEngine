"""Retrieval metrics of a search run against judged documents, independent of the engine.

`judged` maps a query key to the ids of its relevant documents; `ranked` maps it to
the ids the engine returned, best first. For the official metric a query is a golden
keyword, its judged ids are the doc ids of its golden URLs, and a URL counts as ranked
when it is in that keyword's top k. The official rank code counted (keyword, URL)
pairs, so `recall_pairs` is the headline; the coverage tables count distinct URLs,
so `unique_recall` is reported next to it.
"""

from __future__ import annotations

import math
from collections.abc import Hashable, Mapping
from typing import Any

import numpy as np

from isel.evaluate import bootstrap


def found(judged: Mapping[Hashable, set[str]], ranked: Mapping[Hashable, list[str]],
          k: int) -> dict[Hashable, set[str]]:
    """The judged ids in each query's top k."""
    return {q: judged[q] & set(ranked.get(q, [])[:k]) for q in judged}


def recall_pairs(judged: Mapping[Hashable, set[str]],
                 hits: Mapping[Hashable, set[str]]) -> tuple[int, int]:
    """(found, judged) (query, id) pairs; judged ids that were never crawled still count."""
    return sum(len(hits[q]) for q in judged), sum(len(ids) for ids in judged.values())


def unique_recall(judged: Mapping[Hashable, set[str]],
                  hits: Mapping[Hashable, set[str]]) -> float:
    """Share of distinct judged ids found for at least one of their queries."""
    every = set().union(*judged.values()) if judged else set()
    return len(set().union(*hits.values())) / len(every) if every else 0.0


def presence(judged: Mapping[Hashable, set[str]], present: set[str]) -> tuple[int, int]:
    """(judged pairs whose document is in the index, judged pairs)."""
    return (sum(len(ids & present) for ids in judged.values()),
            sum(len(ids) for ids in judged.values()))


def ndcg_at_k(ranked: list[str], relevant: set[str], k: int) -> float:
    """Binary-relevance nDCG@k; the ideal ranking puts min(|relevant|, k) hits first."""
    gain = sum(1 / math.log2(i + 2) for i, d in enumerate(ranked[:k]) if d in relevant)
    ideal = sum(1 / math.log2(i + 2) for i in range(min(len(relevant), k)))
    return gain / ideal if ideal else 0.0


def _pair_interval(judged: Mapping[Hashable, set[str]], hits: Mapping[Hashable, set[str]],
                   samples: int, seed: int) -> tuple[float, float]:
    # Keyed by (query, id), every item belongs to one query and weighs 1, so the
    # distinct-URL bootstrap of `evaluate` resamples the pair recall by query.
    queries = {q: {(q, d) for d in ids} for q, ids in judged.items()}
    return bootstrap(queries, {(q, d) for q, ids in hits.items() for d in ids}, samples, seed)


def summarize(judged: Mapping[Hashable, set[str]], ranked: Mapping[Hashable, list[str]],
              took_ms: Mapping[Hashable, float], present: set[str], k: int, samples: int,
              seed: int) -> dict[str, Any]:
    """Every metric of one query set."""
    hits = found(judged, ranked, k)
    hit, total = recall_pairs(judged, hits)
    in_index, _ = presence(judged, present)
    lo, hi = _pair_interval(judged, hits, samples, seed) if judged else (0.0, 0.0)
    # Distinct URLs, as the coverage table counts them: a URL shared by c queries weighs
    # 1/c in each, so the full sample gives `unique_recall` itself.
    ulo, uhi = (bootstrap(judged, set().union(*hits.values()), samples, seed)
                if judged else (0.0, 0.0))
    given = {q: judged[q] & present for q in judged if judged[q] & present}
    glo, ghi = (_pair_interval(given, {q: hits[q] & given[q] for q in given}, samples, seed)
                if given else (0.0, 0.0))
    took = [took_ms[q] for q in judged if q in took_ms]
    return {
        "queries": len(judged),
        "pairs": total,
        "recall_pairs": hit / total if total else 0.0,
        "recall_pairs_ci95": [lo, hi],
        "recall_unique": unique_recall(judged, hits),
        "recall_unique_ci95": [ulo, uhi],
        "presence": in_index / total if total else 0.0,
        "recall_given_present": hit / in_index if in_index else None,
        "recall_given_present_ci95": [glo, ghi],
        "ndcg": (sum(ndcg_at_k(ranked.get(q, []), judged[q], k) for q in judged) / len(judged)
                 if judged else 0.0),
        "zero_hit_rate": (sum(not ranked.get(q) for q in judged) / len(judged)
                          if judged else 0.0),
        "took_ms_p50": float(np.percentile(took, 50)) if took else None,
        "took_ms_p95": float(np.percentile(took, 95)) if took else None,
    }


def run_eval(judged: Mapping[Hashable, set[str]], ranked: Mapping[Hashable, list[str]],
             took_ms: Mapping[Hashable, float], present: set[str],
             tags: Mapping[str, set[Hashable]], k: int, samples: int,
             seed: int) -> dict[str, Any]:
    """`summarize` over all queries and over each tag's queries."""
    out = {"all": summarize(judged, ranked, took_ms, present, k, samples, seed)}
    for tag, keys in sorted(tags.items()):
        subset = {q: judged[q] for q in judged if q in keys}
        out[tag] = summarize(subset, ranked, took_ms, present, k, samples, seed)
    return out
