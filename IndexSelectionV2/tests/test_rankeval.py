from __future__ import annotations

import math

import pytest

from isel.rankeval import (
    found,
    ndcg_at_k,
    presence,
    recall_pairs,
    run_eval,
    summarize,
    unique_recall,
)

# "a" is golden for both queries; only q1 finds it.
JUDGED = {1: {"a", "b"}, 2: {"a"}}
RANKED = {1: ["x", "a"], 2: ["y"]}


def test_pairs_and_unique_recall_differ_on_a_shared_url():
    hits = found(JUDGED, RANKED, 10)
    assert hits == {1: {"a"}, 2: set()}
    assert recall_pairs(JUDGED, hits) == (1, 3)
    assert unique_recall(JUDGED, hits) == 0.5


def test_found_cuts_at_k():
    assert found(JUDGED, RANKED, 1) == {1: set(), 2: set()}


def test_presence_counts_judged_pairs():
    assert presence(JUDGED, {"a", "z"}) == (2, 3)


def test_ndcg_at_k():
    assert ndcg_at_k(["r1", "x", "r2"], {"r1", "r2"}, 10) == pytest.approx(
        (1 + 1 / 2) / (1 + 1 / math.log2(3)))
    assert ndcg_at_k(["r1"], {"r1", "r2", "r3"}, 1) == 1.0
    assert ndcg_at_k(["x"], set(), 10) == 0.0


def test_summarize():
    out = summarize(JUDGED, RANKED, {1: 2.0, 2: 4.0}, {"a"}, 10, 200, 7)
    assert out["recall_pairs"] == pytest.approx(1 / 3)
    assert out["presence"] == pytest.approx(2 / 3)
    assert out["recall_given_present"] == pytest.approx(1 / 2)
    glo, ghi = out["recall_given_present_ci95"]
    assert 0 <= glo <= out["recall_given_present"] <= ghi <= 1
    assert out["zero_hit_rate"] == 0.0
    assert out["took_ms_p50"] == 3.0
    lo, hi = out["recall_pairs_ci95"]
    assert 0 <= lo <= out["recall_pairs"] <= hi <= 1
    again = summarize(JUDGED, RANKED, {}, {"a"}, 10, 200, 7)
    assert again["recall_pairs_ci95"] == [lo, hi]
    # The distinct-URL interval resamples queries; a URL shared by c queries weighs 1/c.
    ulo, uhi = out["recall_unique_ci95"]
    assert 0 <= ulo <= out["recall_unique"] == 0.5 <= uhi <= 1
    assert again["recall_unique_ci95"] == [ulo, uhi]


def test_summarize_with_nothing_present():
    out = summarize(JUDGED, {}, {}, set(), 10, 50, 1)
    assert out["recall_given_present"] is None
    assert out["zero_hit_rate"] == 1.0 and out["took_ms_p50"] is None


def test_run_eval_splits_by_tag():
    out = run_eval(JUDGED, RANKED, {}, set(), {"head": {1}, "random": {2}}, 10, 50, 1)
    assert out["all"]["queries"] == 2
    assert out["head"]["recall_pairs"] == 0.5 and out["random"]["recall_pairs"] == 0.0
