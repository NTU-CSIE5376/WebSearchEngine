from __future__ import annotations

import dataclasses
from argparse import Namespace
from datetime import timedelta

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from conftest import replace, small_cfg
from synth import make_world, write_work_dir

from isel.backtest import forward_eval, run_backtest, select_sample, train_frozen
from isel.cli import cmd_backtest, cmd_build_dataset
from isel.dataset import train_batches, training_matrix
from isel.features import FeatureStore, resolve_set
from isel.golden import golden_table
from isel.io import atomic_write_json, open_duckdb, read_json, write_parquet_atomic
from isel.manifest import validate_run
from isel.urlnorm import canonicalize


@pytest.fixture(scope="module")
def world():
    return make_world()


@pytest.fixture
def work(tmp_path, world):
    write_work_dir(world, tmp_path)
    return tmp_path


def _store(cfg, work, next_batch=False):
    return FeatureStore(cfg, open_duckdb(cfg, work / "tmp"), work, next_batch)


def test_pool_excludes_rows_fetched_after_as_of(work):
    cfg = small_cfg(work)
    store = _store(cfg, work)
    for batch in (3, 6):
        b = store.batch(batch)
        frame = store.frame(batch, "eval")
        (late,) = store.con.execute(
            f"SELECT count(*) FROM crawl WHERE last_fetch_ok >= '{b.as_of.isoformat()}'"
        ).fetchone()
        assert late > 0
        assert frame.num_rows == store.pool_size(batch) + int(pc.sum(pc.invert(
            frame.column("is_sample"))).as_py() or 0)
        assert min(frame.column("fetch_age_days").to_pylist()) > 0


def test_future_batches_do_not_change_features(work, world):
    cfg = small_cfg(work)
    before = _store(cfg, work).frame(5, "eval").sort_by([("url", "ascending")])
    rows = list(world.golden_rows)
    rows += [(6, world.batches[6], 99999, "x", 1, ["head"], 1, p["url"])
             for p in world.pages[:300]]
    rows += [(5, world.batches[5], 99998, "y", 1, ["head"], 1, world.pages[1]["url"])]
    write_parquet_atomic(golden_table(rows), work / "golden" / "golden.parquet")
    after = _store(cfg, work).frame(5, "eval").sort_by([("url", "ascending")])
    common = after.filter(pc.is_in(after.column("url"), value_set=before.column("url")))
    assert common.num_rows == before.num_rows
    for name in ("hist_decayed", "hist_days_since", "lift_hp", "lift_dkey", "dem_dkey",
                 "log_size_dkey", "rel_inlinks"):
        a = np.nan_to_num(np.array(before.column(name).to_pylist(), float), nan=-9)
        b = np.nan_to_num(np.array(common.column(name).to_pylist(), float), nan=-9)
        assert np.allclose(a, b), name


def test_training_batches_precede_the_test_batch(work):
    cfg = small_cfg(work)
    store = _store(cfg, work)
    for t in (5, 8):
        test = store.batch(t)
        chosen = train_batches(cfg, store, t)
        assert chosen and t not in chosen
        assert all(store.batch(b).created_at <= test.as_of for b in chosen)


def test_frozen_model_validates_on_the_latest_batch(work):
    model_dir = train_frozen(small_cfg(work), work, [])
    meta = read_json(model_dir / "lgbm_full" / "meta.json")
    assert meta["train_batches"] == list(range(2, 9))
    assert meta["valid_batch"] == 8


def test_forward_eval_scores_the_models_of_several_runs(work):
    cfg = small_cfg(work)
    runs = [train_frozen(cfg, work, []), train_frozen(cfg, work, ["v1_features"])]
    out = forward_eval(cfg, work, runs, 9, ["random"])
    scorers = read_json(out / "results.json")["batches"]["9"]["tags"]["head"]["scorers"]
    assert {"lgbm:full", "v1_features", "random"} <= set(scorers)
    assert "diff_vs_reference_ci95" in scorers["v1_features"]


def test_store_refuses_an_unfinished_sample(work):
    meta = read_json(work / "sample" / "_meta.json")
    del meta["finished_at"]
    atomic_write_json(work / "sample" / "_meta.json", meta)
    with pytest.raises(RuntimeError, match="unfinished"):
        _store(small_cfg(work), work)


def test_store_refuses_batches_without_positives(work):
    (work / "positives" / "batch=3.parquet").unlink()
    with pytest.raises(RuntimeError, match=r"positives are missing for batches \[3\]"):
        _store(small_cfg(work), work)


def test_decay_weights_fall_with_age(work):
    store = _store(small_cfg(work), work)
    rows = store.con.execute("SELECT src, w FROM pairs WHERE dst = 8 ORDER BY src").fetchall()
    weights = [w for _, w in rows]
    assert [s for s, _ in rows] == list(range(1, 8))
    assert weights == sorted(weights) and all(0 < w <= 1 for w in weights)


def test_empirical_bayes_falls_back_to_parent(work):
    store = _store(small_cfg(work), work)
    rows = store.con.execute(
        f"SELECT f.lift_hp, f.lift_host, l.ex, l.g, f.in_agg FROM ({store.frame_sql(2, 'eval')}) f "
        "JOIN crawl c USING (url, shard) "
        "LEFT JOIN (SELECT * FROM lvl_hp WHERE batch_id = 2) l ON l.k = c.hp").fetchall()
    # A row outside the aggregate half adds its own exposure, so only aggregate rows
    # can see an empty group.
    empty = [(hp, host) for hp, host, e, g, agg in rows if agg and not e and not g]
    seen_no_hits = [(hp, host) for hp, host, e, g, _ in rows if e and not g]
    assert empty and seen_no_hits
    assert all(np.isclose(hp, host) for hp, host in empty)
    assert all(hp < host for hp, host in seen_no_hits)


def test_backtest_beats_random_and_links_on_planted_signal(work):
    cfg = small_cfg(work)
    out = run_backtest(cfg, work, ["full", "no_domain"],
                       ["random", "inlinks", "ext", "domain_prior", "logreg", "v1_features"])
    results = read_json(out / "results.json")
    pooled = results["pooled"]
    for tag in ("head", "random"):
        s = pooled[tag]["scorers"]
        model = s["lgbm:full"]["indexed_exact_rate"]
        assert model > s["random"]["indexed_exact_rate"] + 0.05
        assert model >= s["inlinks"]["indexed_exact_rate"]
        assert s["random"]["diff_vs_reference_ci95"][1] < 0
    b8 = results["batches"]["8"]
    ratio = b8["calibration"]["lgbm:full"]["ratio"]
    assert 0.3 < ratio < 3
    assert b8["leaky_in_backtest"] == []
    assert (out / "report.md").read_text().startswith("# Forward-chained backtest")


def test_frozen_model_forward_eval_and_manifest(work, world):
    cfg = small_cfg(work)
    model_dir = train_frozen(cfg, work, ["v1_features"])
    assert world.batches[9].isoformat() > read_json(work / "sample" / "_meta.json")["snapshot_at"]
    out = forward_eval(cfg, work, [model_dir], 9, ["random", "inlinks", "url_score",
                                                   "v2_baseline_v1"])
    result = read_json(out / "results.json")["batches"]["9"]
    assert result["forward"] is True
    assert "v2_baseline_v1" in result["tags"]["head"]["scorers"]
    meta = select_sample(replace(cfg, "eval", k_headline=1000), work, model_dir, 1000)
    run = work / "selection" / f"run={meta['run_id']}"
    assert validate_run(run)["rows"] == meta["rows"] > 0


def test_backtest_batch_before_snapshot_is_rejected_for_forward_eval(work):
    cfg = small_cfg(work)
    model_dir = train_frozen(cfg, work, [])
    with pytest.raises(RuntimeError, match="existed before the snapshot"):
        forward_eval(cfg, work, [model_dir], 8, ["random"])


def test_forward_eval_refuses_a_model_of_another_sample(work):
    cfg = small_cfg(work)
    model_dir = train_frozen(cfg, work, [])
    meta = read_json(work / "sample" / "_meta.json")
    atomic_write_json(work / "sample" / "_meta.json", {**meta, "sample_percent": 50.0})
    with pytest.raises(RuntimeError, match="another sample"):
        forward_eval(cfg, work, [model_dir], 9, ["random"])


def _add_golden(work, world, extra):
    """Rewrites golden.parquet as `world`'s rows plus `{batch: [url, ...]}`."""
    rows = list(world.golden_rows)
    for qid, (batch, urls) in enumerate(extra.items(), start=990_000):
        rows += [(batch, world.batches[batch], qid, f"extra{qid}", 1, ["random"], rank, u)
                 for rank, u in enumerate(urls, start=1)]
    write_parquet_atomic(golden_table(rows), work / "golden" / "golden.parquet")


def _never_golden(world, fetched_before, n=1):
    golden = {u for r in world.golden_rows for u in (r[7], canonicalize(r[7]))}
    return [p["url"] for p in world.pages
            if p["url"] not in golden and p["last_fetch_ok"] < fetched_before][:n]


def _flagged(m, batch):
    return int(m.later_golden[m.batch == batch].sum())


def test_label_audit_counts_negatives_that_turn_golden_later(work, world):
    cfg = small_cfg(work)
    before = cmd_build_dataset(cfg, Namespace())
    (url,) = _never_golden(world, world.batches[5] - timedelta(days=2))
    _add_golden(work, world, {7: [url]})
    after = cmd_build_dataset(cfg, Namespace())

    def delta(batch, key):
        return after[str(batch)][key] - before[str(batch)][key]

    assert (delta(5, "neg_later_golden"), delta(5, "neg_next_golden"),
            delta(5, "neg_rows")) == (1, 0, 0)
    assert (delta(6, "neg_later_golden"), delta(6, "neg_next_golden")) == (1, 1)
    assert (delta(7, "neg_rows"), delta(7, "positive_rows")) == (-1, 1)
    assert [delta(8, k) for k in ("neg_rows", "neg_later_golden", "neg_next_golden")] == [0] * 3
    assert after["9"]["neg_next_golden"] is None
    assert after["5"]["later_golden_share"] == pytest.approx(
        after["5"]["neg_later_golden"] / after["5"]["neg_rows"])
    assert read_json(work / "datasets" / "summary.json")["7"]["neg_rows"] == after["7"]["neg_rows"]


def test_label_audit_counts_positives_matched_only_by_canonical_spelling(work, world):
    cfg = small_cfg(work)
    summary = cmd_build_dataset(cfg, Namespace())
    store = _store(cfg, work)
    for batch in (5, 8):
        as_of = store.batch(batch).as_of
        crawled = {p["url"] for p in world.pages if p["last_fetch_ok"] < as_of}
        raws = {r[7] for r in world.golden_rows if r[0] == batch}
        expected = len(({canonicalize(u) for u in raws} - raws) & crawled)
        assert expected > 0
        assert summary[str(batch)]["canonical_only_positives"] == expected


def test_label_audit_counts_other_spellings_of_golden_pages(work, world):
    cfg = small_cfg(work)
    before = cmd_build_dataset(cfg, Namespace())
    early = world.batches[5] - timedelta(days=2)
    raws = {r[7] for r in world.golden_rows if r[0] == 5}
    page = next(p for p in world.pages if p["url"] in raws and p["last_fetch_ok"] < early)
    variant = {k: v for k, v in page.items() if not k.startswith("_")}
    variant.update(url=page["url"].replace("https://www.", "http://"), shard=0, in_agg=False)
    path = work / "sample" / "shard=000.parquet"
    sample = pq.read_table(path)
    write_parquet_atomic(pa.Table.from_pylist([*sample.to_pylist(), variant],
                                              schema=sample.schema), path)
    after = cmd_build_dataset(cfg, Namespace())
    assert after["5"]["neg_variant_golden"] - before["5"]["neg_variant_golden"] == 1


def test_training_matrix_flags_negatives_golden_in_a_later_training_batch(work, world):
    cfg = replace(small_cfg(work), "dataset", neg_rate=1.0)
    features = resolve_set(cfg, cfg.model.feature_set)
    base = training_matrix(_store(cfg, work), [4, 5, 6], features)
    (url,) = _never_golden(world, world.batches[4] - timedelta(days=2))
    _add_golden(work, world, {6: [url]})
    m = training_matrix(_store(cfg, work), [4, 5, 6], features)
    assert [_flagged(m, b) - _flagged(base, b) for b in (4, 5)] == [1, 1]
    assert _flagged(m, 6) == 0
    assert not m.later_golden[m.y == 1].any()
    kept = m.subset(~m.later_golden)
    assert len(kept.y) == len(m.y) - int(m.later_golden.sum())
    assert kept.y.sum() == m.y.sum() and not kept.later_golden.any()


def test_later_golden_flags_ignore_batches_outside_the_training_set(work, world):
    cfg = replace(small_cfg(work), "dataset", neg_rate=1.0)
    features = resolve_set(cfg, cfg.model.feature_set)
    store = _store(cfg, work)
    train = train_batches(cfg, store, 8)
    base = training_matrix(store, train, features)
    urls = _never_golden(world, world.batches[2], n=5)
    _add_golden(work, world, {8: urls[:3], 9: urls[3:]})
    store = _store(cfg, work)
    assert train_batches(cfg, store, 8) == train
    m = training_matrix(store, train, features)
    for b in train:
        assert _flagged(m, b) == _flagged(base, b)
        assert int(m.y[m.batch == b].sum()) == int(base.y[base.batch == b].sum())


def test_backtest_reports_the_later_unlabeled_variant(work):
    cfg = replace(small_cfg(work), "eval", test_batches=[8])
    out = run_backtest(cfg, work, ["full"], ["later_unlabeled"])
    results = read_json(out / "results.json")
    models = results["batches"]["8"]["models"]
    ref, variant = models["lgbm:full"], models["later_unlabeled"]
    assert variant["train_batches"] == ref["train_batches"]
    assert variant["positives"] == ref["positives"] and variant["rows"] < ref["rows"]
    assert "diff_vs_reference_ci95" in results["pooled"]["head"]["scorers"]["later_unlabeled"]
    assert "later_unlabeled" in (out / "report.md").read_text()


def test_cli_rejects_unknown_baseline_names(work):
    with pytest.raises(SystemExit, match="later_unlabled"):
        cmd_backtest(small_cfg(work), Namespace(feature_sets=None,
                                                baselines="random,later_unlabled"))


def test_training_frames_do_not_depend_on_the_order_of_crawl_rows(work, world):
    # LightGBM bagging picks rows by position and bins exact zeros apart, so the
    # row order and the last bit of every feature both decide the model.
    other = work / "reversed"
    write_work_dir(world, other)
    for path in (other / "sample").glob("shard=*.parquet"):
        table = pq.read_table(path)
        write_parquet_atomic(table.take(np.arange(table.num_rows)[::-1]), path)
    a = _store(replace(small_cfg(work), "duckdb", threads=8), work).frame(5, "train")
    b = _store(replace(small_cfg(other), "duckdb", threads=8), other).frame(5, "train")
    assert a.num_rows == b.num_rows
    assert [c for c in a.column_names if not a.column(c).equals(b.column(c))] == []


def test_relative_inlinks_are_zero_for_pages_at_the_domain_mean(tmp_path, world):
    flat = {f"site{d}.com" for d in range(10)}
    pages = [{**p, "inlink_count_approx": 37, "inlink_count_external": 11}
             if p["url"].split("/")[2].removeprefix("www.") in flat else p
             for p in world.pages]
    write_work_dir(dataclasses.replace(world, pages=pages), tmp_path)
    frame = _store(small_cfg(tmp_path), tmp_path).frame(5, "eval")
    rows = [r for r in frame.select(["dkey", "rel_inlinks", "rel_ext"]).to_pylist()
            if r["dkey"] in flat]
    assert rows
    assert [r for r in rows if r["rel_inlinks"] != 0 or r["rel_ext"] != 0] == []


def test_select_sample_refuses_a_model_of_another_sample(work):
    cfg = small_cfg(work)
    model_dir = train_frozen(cfg, work, [])
    meta = read_json(work / "sample" / "_meta.json")
    atomic_write_json(work / "sample" / "_meta.json", {**meta, "sample_salt": "other"})
    with pytest.raises(RuntimeError, match="another sample"):
        select_sample(cfg, work, model_dir, 1000)
