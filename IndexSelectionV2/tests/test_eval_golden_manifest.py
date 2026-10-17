from __future__ import annotations

import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from conftest import replace

from isel.dataset import tag_weights
from isel.evaluate import (
    Golden,
    Ranker,
    _per_query,
    bootstrap,
    metric_rate,
    paired_bootstrap,
    rates,
)
from isel.golden import (
    OFFICIAL,
    PSEUDO,
    golden_table,
    pool_table,
    pseudo_batch_path,
    read_golden,
    summarize,
)
from isel.manifest import ManifestError, _worker_env, to_pg_bigint, validate_run, write_run
from isel.urlnorm import md5_number_lower


def _frame(scores_h_sample_label_url):
    s, h, sample, label, url = zip(*scores_h_sample_label_url, strict=True)
    return np.array(s, float), pa.table({
        "h": pa.array(h, pa.uint64()), "is_sample": pa.array(sample),
        "label": pa.array([int(x) for x in label], pa.int32()), "url": pa.array(url)})


def _exact_topk(rows, k):
    sample = sorted((r for r in rows if r[2]), key=lambda r: (-r[0], r[1]))
    if k >= len(sample):
        return {r[4] for r in rows if r[3]}
    bs, bh = sample[k - 1][0], sample[k - 1][1]
    return {r[4] for r in rows if r[3] and (r[0] > bs or (r[0] == bs and r[1] <= bh))}


@pytest.mark.parametrize("k", [1, 3, 5, 8, 50])
def test_ranker_equals_exact_topk_at_full_sample(k):
    rows = [(1.0, 5, True, False, "a"), (2.0, 1, True, True, "b"), (2.0, 9, True, True, "c"),
            (0.5, 2, True, False, "d"), (2.0, 4, True, False, "e"), (3.0, 7, False, True, "f"),
            (0.1, 3, True, True, "g"), (np.nan, 8, True, True, "h")]
    scores, frame = _frame(rows)
    ranker = Ranker(scores, frame, rate=1.0)
    clean = [(-np.inf if np.isnan(r[0]) else r[0], *r[1:]) for r in rows]
    assert ranker.listed_at_rank(ranker.rank_for_k(k)) == _exact_topk(clean, k)


def test_ranker_scales_rank_by_sample_rate():
    rng = np.random.default_rng(0)
    n = 20_000
    scores = rng.normal(size=n)
    frame = pa.table({"h": pa.array(rng.integers(0, 2**63, n), pa.uint64()),
                      "is_sample": pa.array(rng.random(n) < 0.1),
                      "label": pa.array((rng.random(n) < 0.05).astype(np.int32)),
                      "url": pa.array([f"u{i}" for i in range(n)])})
    estimate = Ranker(scores, frame, rate=0.1).listed_at_rank(round(5000 * 0.1))
    labels = frame.column("label").to_numpy().astype(bool)
    exact = {f"u{i}" for i in np.flatnonzero(labels & (scores >= np.sort(scores)[-5000]))}
    overlap = len(estimate & exact) / len(exact)
    assert overlap > 0.9


def test_per_query_weights_shared_urls():
    queries = {1: {"a", "b"}, 2: {"b"}}
    assert _per_query(queries, {"b"}) == [(0.5, 1.5), (0.5, 0.5)]


def test_bootstrap_is_deterministic_and_paired():
    queries = {i: {f"u{i}", f"v{i}"} for i in range(40)}
    hits = {f"u{i}" for i in range(20)}
    assert bootstrap(queries, hits, 200, 1) == bootstrap(queries, hits, 200, 1)
    lo, hi = paired_bootstrap(queries, hits, hits, 200, 1)
    assert lo == hi == 0.0


def test_rates_count_exact_and_canonical():
    g = Golden({1: {"https://a/#x", "https://b/"}}, {"https://a/#x": "https://a/",
                                                    "https://b/": "https://b/"})
    exact, r_exact, r_canon = rates(g, {"https://a/"})
    assert exact == set() and r_exact == 0.0 and r_canon == 0.5


def test_metric_rate_counts_each_canonical_url_once_as_the_metric_does():
    g = Golden({1: {"https://a/?y=2&x=1", "https://a/?x=1&y=2", "https://b/#f"}},
               {"https://a/?y=2&x=1": "https://a/?x=1&y=2",
                "https://a/?x=1&y=2": "https://a/?x=1&y=2",
                "https://b/#f": "https://b/"})
    assert metric_rate(g, {"https://a/?x=1&y=2"}) == 0.5
    assert metric_rate(g, {"https://b/#f"}) == 0.0     # a listed raw spelling is not the key


def test_tag_weights_balance_head_and_random():
    head = np.array([1, 1, 1, 0, 0], bool)
    rand = np.array([0, 0, 0, 1, 1], bool)
    w = tag_weights(head, rand)
    assert np.isclose(w.sum(), 5)
    assert np.isclose(w[head].sum(), w[rand].sum())


def test_golden_summary_reports_drift_and_recurrence():
    t1 = datetime(2026, 2, 1, tzinfo=UTC)
    t2 = datetime(2026, 2, 15, tzinfo=UTC)
    rows = [(1, t1, 1, "k", 10, ["head"], 1, "https://a.com/x#f"),
            (1, t1, 2, "k2", 5, ["random"], 1, "https://b.com/"),
            (2, t2, 3, "k", 10, ["head"], 1, "https://a.com/x")]
    summary = summarize(golden_table(rows), ["head", "random"])
    assert summary["1"]["head"]["raw_ne_canonical_rate"] == 1.0
    assert summary["2"]["recurring_canonical_rate"] == 1.0
    assert summary["1"]["random"]["queries"] == 1


def test_official_golden_is_available_when_created():
    t1 = datetime(2026, 2, 1, tzinfo=UTC)
    table = golden_table([(1, t1, 1, "k", 10, ["head"], 1, "https://a.com/")])
    assert table.column("available_at").to_pylist() == [t1]
    assert table.column("label_source").to_pylist() == ["official"]


def test_pool_keeps_every_trending_query():
    t1 = datetime(2026, 2, 1, tzinfo=UTC)
    pool = pool_table([(1, t1, 7, "k", 10, ["head"]), (1, t1, 8, "z", None, None)])
    assert pool.column("keyword").to_pylist() == ["k", "z"]
    assert pool.column("tags").to_pylist() == [["head"], []]
    assert pool.column("frequency").to_pylist() == [10, None]


def test_read_golden_adds_pseudo_batches_only_on_request(tmp_path):
    t1 = datetime(2026, 2, 1, tzinfo=UTC)
    t2 = datetime(2026, 2, 9, tzinfo=UTC)
    official = golden_table([(1, t1, 1, "k", 10, ["head"], 1, "https://a.com/")])
    pseudo = golden_table([(10001, t1, 2, "z", 3, ["random"], 1, "https://b.com/")],
                          label_source=PSEUDO, available_at=t2)
    pseudo_batch_path(tmp_path, 10001).parent.mkdir(parents=True)
    pq.write_table(official, tmp_path / "golden" / "golden.parquet")
    pq.write_table(pseudo, pseudo_batch_path(tmp_path, 10001))
    assert read_golden(tmp_path, include_pseudo=False).num_rows == 1
    both = read_golden(tmp_path, include_pseudo=True)
    assert sorted(both.column("label_source").to_pylist()) == [OFFICIAL, PSEUDO]
    assert both.filter(pc.equal(both.column("batch_id"), 10001)).column(
        "available_at").to_pylist() == [t2]


def _write(cfg, urls, scores, sel, run_id):
    con = duckdb.connect()
    con.register("rows", pa.table({
        "url": urls, "crawl_shard": pa.array([1] * len(urls), pa.int16()),
        "domain_id": pa.array([7] * len(urls), pa.int64()),
        "score": pa.array(scores, pa.float64()),
        "reason": pa.array([["x:+1"]] * len(urls), pa.list_(pa.string()))}))
    return write_run(cfg, con, "SELECT * FROM rows", sel, run_id)


def test_manifest_runs_deltas_and_validation(tmp_path, base_cfg):
    cfg = replace(base_cfg, "manifest", index_shards=4)
    sel = tmp_path / "selection"
    first = _write(cfg, ["https://a/", "https://b/", "https://c/"], [3, 2, 1], sel, "r1")
    assert first["parent_run_id"] is None and first["rows"] == 3
    second = _write(cfg, ["https://b/", "https://d/"], [5, 4], sel, "r2")
    assert second["parent_run_id"] == "r1"
    run = sel / "run=r2"
    added = pq.read_table(run / "deltas" / "added.parquet").column("url").to_pylist()
    removed = pq.read_table(run / "deltas" / "removed.parquet").column("url").to_pylist()
    assert added == ["https://d/"] and sorted(removed) == ["https://a/", "https://c/"]
    manifest = pq.read_table(run / "manifest").sort_by("rank")
    assert manifest.column("url").to_pylist() == ["https://b/", "https://d/"]
    assert manifest.column("rank").to_pylist() == [1, 2]
    assert manifest.column("host").to_pylist() == ["b", "d"]
    assert manifest.column("domain_id").to_pylist() == [7, 7]
    assert manifest.column("doc_id").to_pylist() == [md5_number_lower(u)
                                                     for u in ["https://b/", "https://d/"]]
    assert len(list((run / "manifest").glob("index_shard=*.parquet"))) == 4
    assert validate_run(run)["rows"] == 2
    pq.write_table(manifest.slice(0, 1), run / "manifest" / "index_shard=00.parquet")
    with pytest.raises(ManifestError):
        validate_run(run)


def test_manifest_rejects_duplicate_urls(tmp_path, base_cfg):
    with pytest.raises(ManifestError, match="duplicate"):
        _write(base_cfg, ["https://a/", "https://a/"], [1, 2], tmp_path, "r1")


def test_spellings_of_one_page_share_a_doc_id(tmp_path, base_cfg):
    # The crawler stored both spellings; canonicalize resolves the dot segment.
    meta = _write(base_cfg, ["https://a.example/x/./y", "https://a.example/x/y"], [2, 1],
                  tmp_path, "r1")
    manifest = pq.read_table(tmp_path / "run=r1" / "manifest")
    assert meta["rows"] == 2 and meta["docs"] == 1
    assert set(manifest.column("canonical_url").to_pylist()) == {"https://a.example/x/y"}
    added = pq.read_table(tmp_path / "run=r1" / "deltas" / "added.parquet")
    assert added.column("url").to_pylist() == ["https://a.example/x/./y"]   # best-ranked spelling


def test_doc_ids_map_to_signed_bigint_and_back():
    ids = pa.array([0, 2**63 - 1, 2**63, 2**64 - 1], pa.uint64())
    signed = to_pg_bigint(ids)
    assert signed.to_pylist() == [0, 2**63 - 1, -(2**63), -1]
    assert pa.compute.cast(signed, pa.uint64(), safe=False).equals(ids)


def test_the_canonicalize_pool_matches_the_inline_path(tmp_path, base_cfg):
    urls = [f"https://a{i}.example/x/./p{i}" if i % 7 == 0 else f"https://a{i}.example/p{i}"
            for i in range(3000)]
    metas = []
    for workers in (1, 2):
        cfg = replace(base_cfg, "manifest", canonical_workers=workers, canonical_batch_rows=500)
        metas.append(_write(cfg, urls, list(range(len(urls))), tmp_path / str(workers), "r1"))
    assert metas[0]["docs"] == metas[1]["docs"] == len(urls)
    tables = [pq.read_table(tmp_path / str(w) / "run=r1" / "manifest").sort_by("rank")
              for w in (1, 2)]
    assert tables[0].column("canonical_url").equals(tables[1].column("canonical_url"))
    rows = tables[0].to_pylist()
    assert sum(r["url"] != r["canonical_url"] for r in rows) == len(range(0, len(urls), 7))


def test_canonicalize_workers_start_with_one_blas_thread(monkeypatch):
    # OpenBLAS starts a thread per CPU in every worker; 16 workers broke a 512-pid limit.
    monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
    spawn = multiprocessing.get_context("spawn")
    with _worker_env(), ProcessPoolExecutor(1, mp_context=spawn) as pool:
        assert pool.submit(os.getenv, "OPENBLAS_NUM_THREADS").result() == "1"
    assert "OPENBLAS_NUM_THREADS" not in os.environ
