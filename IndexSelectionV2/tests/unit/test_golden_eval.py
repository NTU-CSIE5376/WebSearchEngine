from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import example_config, with_changes

from idxsel.evaluate import LOOKUP_SQL, _bootstrap, evaluate, lookup, verify_live
from idxsel.golden import build_golden
from idxsel.manifest import atomic_write_json


class FakeSession:
    """Answers the two queries the code sends, without a database."""

    def __init__(self, golden_rows=None, selected=(), run_id=None):
        self.golden_rows = golden_rows or []
        self.selected = set(selected)
        self.run_id = run_id
        self.queries = []

    def fetchall(self, query, params=None):
        self.queries.append(query)
        if query == LOOKUP_SQL:
            return [(u,) for u in params[0] if u in self.selected]
        if "count(*)" in query:
            return [(len(self.selected),)]
        if "run_id" in query:
            return [(self.run_id,)]
        return self.golden_rows

    def fetchone(self, query, params=None):
        rows = self.fetchall(query, params)
        return rows[0] if rows else None


def _dims(tmp_path):
    d = tmp_path / "dims"
    d.mkdir()
    pq.write_table(pa.table({"domain_id": pa.array([1, 2, 3], pa.int64()),
                             "domain": ["a.com", "en.wikipedia.org", "wikipedia.org"],
                             "shard_id": pa.array([0, 1, 1], pa.int32())}),
                   d / "domain_state.parquet")
    pq.write_table(pa.table({"host": ["en.wikipedia.org"]}), d / "split_hosts.parquet")
    return d


GOLDEN = [
    (1, datetime(2026, 2, 1), 10, ["head"], "https://a.com/x"),
    (1, datetime(2026, 2, 1), 10, ["head"], "https://en.wikipedia.org/wiki/A"),
    (2, datetime(2026, 5, 1), 20, ["random"], "https://a.com/y?b=1&a=2"),
    (3, datetime(2026, 9, 1), 30, ["head"], "https://a.com/z"),
    (3, datetime(2026, 9, 1), 31, ["head"], "https://fr.wikipedia.org/wiki/B"),
]


def _con():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def test_golden_prior_uses_only_batches_before_target(tmp_path):
    cfg = with_changes(example_config(), golden__target_batch=3)
    meta = build_golden(cfg, FakeSession(GOLDEN), _dims(tmp_path), tmp_path / "g", _con(),
                        as_of_hint=datetime(2026, 10, 1))
    assert meta["target_batch"] == 3
    assert meta["batches_used"] == [1, 2]
    past = {r["canonical_url"]: r["raw_url"]
            for r in pq.read_table(tmp_path / "g" / "past_golden.parquet").to_pylist()}
    assert past == {"https://a.com/x": "https://a.com/x",
                    "https://en.wikipedia.org/wiki/A": "https://en.wikipedia.org/wiki/A",
                    "https://a.com/y?a=2&b=1": "https://a.com/y?b=1&a=2"}
    prior = {r["domain_id"]: r["prior"] for r in
             pq.read_table(tmp_path / "g" / "domain_prior.parquet").to_pylist()}
    assert set(prior) == {1, 2}  # fr.wikipedia.org (batch 3) is excluded
    # a.com: one URL from batch 1 (212 days old) + one from batch 2 (123 days), half-life 90
    assert prior[1] == pytest.approx(0.5 ** (212 / 90) + 0.5 ** (123 / 90), rel=1e-6)


def test_golden_next_target_uses_all_batches(tmp_path):
    meta = build_golden(example_config(), FakeSession(GOLDEN), _dims(tmp_path), tmp_path / "g",
                        _con(), as_of_hint=datetime(2026, 10, 1))
    assert meta["target_batch"] is None
    assert meta["batches_used"] == [1, 2, 3]
    keys = pq.read_table(tmp_path / "g" / "golden.parquet").to_pylist()
    wiki = {r["raw_url"]: (r["domain_key"], r["domain_id"]) for r in keys}
    assert wiki["https://fr.wikipedia.org/wiki/B"] == ("wikipedia.org", 3)
    assert wiki["https://en.wikipedia.org/wiki/A"] == ("en.wikipedia.org", 2)


def test_lookup_uses_metric_sql_in_chunks():
    s = FakeSession(selected={"u1", "u3"})
    assert lookup(s, ["u1", "u2", "u3", "u1"], chunk=2) == {"u1", "u3"}
    assert s.queries == [LOOKUP_SQL, LOOKUP_SQL]


def test_bootstrap_is_deterministic():
    q = {1: {"a", "b"}, 2: {"c"}, 3: {"d", "e"}}
    assert _bootstrap(q, {"a", "c"}, 200, 7) == _bootstrap(q, {"a", "c"}, 200, 7)


def test_bootstrap_matches_a_proportion_when_queries_share_nothing():
    # One URL per query: the cluster bootstrap is the plain bootstrap of a proportion.
    n, hits = 400, {f"u{i}" for i in range(100)}
    lo, hi = _bootstrap({i: {f"u{i}"} for i in range(n)}, hits, 2000, 7)
    assert (hi - lo) / 2 == pytest.approx(1.96 * (0.25 * 0.75 / n) ** 0.5, rel=0.12)


def test_bootstrap_counts_a_shared_url_once_in_every_resample():
    # b is in both queries, so any resample still holds 1 hit among 3 distinct URLs.
    assert _bootstrap({1: {"a", "b"}, 2: {"b", "c"}}, {"b"}, 200, 7) == (
        pytest.approx(1 / 3), pytest.approx(1 / 3))


def _eval_inputs(tmp_path, cfg, selected, features_as_of=datetime(2026, 8, 15, tzinfo=UTC)):
    con = _con()
    g = tmp_path / "g"
    meta = build_golden(cfg, FakeSession(GOLDEN), _dims(tmp_path), g, con,
                        as_of_hint=datetime(2026, 10, 1))
    f = tmp_path / "features" / "shard=000"
    f.mkdir(parents=True)
    pq.write_table(pa.table({"url": ["https://a.com/z", "https://fr.wikipedia.org/wiki/B",
                                     "https://a.com/y?a=2&b=1"]}), f / "part.parquet")
    atomic_write_json(tmp_path / "features" / "meta.json", {"as_of": features_as_of})
    sel = tmp_path / "run" / "selected.parquet"
    sel.parent.mkdir()
    pq.write_table(pa.table({"url": pa.array(sorted(selected), pa.string())}), sel)
    return con, g, meta, sel


def test_evaluate_replicates_metric_on_the_runs_own_selection(tmp_path):
    cfg = with_changes(example_config(), golden__target_batch=3, eval__bootstrap_samples=50)
    con, g, meta, sel = _eval_inputs(tmp_path, cfg, {"https://a.com/z"})
    report = evaluate(cfg, con, sel, g, tmp_path / "features", meta, tmp_path / "run")
    head = report["results"]["batch3_head"]
    assert head["in_sample"] is False
    assert head["golden_urls"] == 2
    assert head["indexed_exact_rate"] == 0.5
    assert head["crawled_rate"] == 1.0
    assert head["recall_of_crawled"] == 0.5
    assert "batch3_random" not in report["results"]
    assert (tmp_path / "run" / "report.md").read_text().count("| batch3_head | no |") == 1


def test_evaluate_marks_batches_older_than_the_features_in_sample(tmp_path):
    # The crawler re-ranks and injects golden batches, so features observed after
    # batch 3 existed may hold its URLs because they are golden.
    cfg = with_changes(example_config(), golden__target_batch=3, eval__bootstrap_samples=50)
    con, g, meta, sel = _eval_inputs(tmp_path, cfg, {"https://a.com/z"},
                                     features_as_of=datetime(2026, 9, 2, tzinfo=UTC))
    head = evaluate(cfg, con, sel, g, tmp_path / "features", meta,
                    tmp_path / "run")["results"]["batch3_head"]
    assert head["in_sample"] is True
    assert head["batch_created_at"] == datetime(2026, 9, 1, tzinfo=UTC)
    assert head["features_as_of"] == datetime(2026, 9, 2, tzinfo=UTC)


def test_evaluate_marks_tier_batches_in_sample_and_splits_spellings(tmp_path):
    cfg = with_changes(example_config(), eval__batches=[2], eval__bootstrap_samples=50)
    con, g, meta, sel = _eval_inputs(tmp_path, cfg, {"https://a.com/y?a=2&b=1"})
    rand = evaluate(cfg, con, sel, g, tmp_path / "features", meta,
                    tmp_path / "run")["results"]["batch2_random"]
    assert rand["in_sample"] is True  # "next" mode: batch 2 also fed the past-golden tier
    assert rand["indexed_exact"] == 0  # the raw spelling differs from the listed one
    assert rand["indexed_canonical"] == 1


def test_verify_live_checks_count_run_and_sample(tmp_path):
    cfg = with_changes(example_config(), run__k=3, eval__live_check_sample_size=2)
    sel = tmp_path / "selected.parquet"
    urls = ["https://a.com/1", "https://a.com/2", "https://a.com/3"]
    pq.write_table(pa.table({"url": urls}), sel)
    good = verify_live(cfg, _con(), FakeSession(selected=urls, run_id=7), sel, 7)
    assert good["ok"] and good["sampled"] == 2 == good["sample_found"]
    assert not verify_live(cfg, _con(), FakeSession(selected=urls, run_id=6), sel, 7)["ok"]
    assert not verify_live(cfg, _con(), FakeSession(selected=urls[:2], run_id=7), sel, 7)["ok"]
