"""Label sets and the sweep, checked against the production top-K."""

import dataclasses
from datetime import UTC, datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import example_config, with_changes

from idxsel import pipeline
from idxsel.evaluate import _paired_bootstrap, evaluate
from idxsel.labels import label_sets, sweep, write_labels
from idxsel.manifest import atomic_write_json, read_json
from idxsel.scoring import build_scored
from idxsel.select_topk import select_top_k
from idxsel.urlnorm import canonicalize, md5_number_lower

AS_OF = datetime(2026, 9, 10, tzinfo=UTC)
PAST = datetime(2026, 6, 1, tzinfo=UTC)
FETCHED = datetime(2026, 5, 1, tzinfo=UTC)  # before batch 1's cutoff: its hits count
TARGET = datetime(2026, 9, 20, tzinfo=UTC)
K = 120
RAW = "https://d3.com/q?b=1&a=2"  # golden spelling; the crawler stores the canonical one


def _row(url, shard, domain_id, inlinks, ext=0, chash=0):
    return {"url": url, "domain_id": domain_id, "first_seen": AS_OF, "last_fetch_ok": FETCHED,
            "num_fetch_ok_90d": 1, "num_consecutive_fail": 0, "inlink_count_approx": inlinks,
            "inlink_count_external": ext, "parent_page_score": None, "url_score": 0.0,
            "source": 0, "discovery_source_type": 1, "robots_bits": 1, "is_redirect": None,
            "redirect_hop_count": None, "has_json_ld": None, "hreflang_count": None,
            "has_anchor": False, "title_null": False, "content_hash64": chash,
            "observed_at": AS_OF, "shard": shard}


def _con():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def _universe(tmp_path):
    """~450 rows with three inlink levels (large tie groups), a cross-shard duplicate,
    past-golden URLs (tier) and a golden batch mixing ranks, misses and spellings."""
    rows = {s: [] for s in range(3)}
    for s in range(3):
        for i in range(150):
            rows[s].append(_row(f"https://d{i % 6}.com/p{s}-{i}", s, i % 6 + 1,
                                (0, 5, 50)[i % 3], ext=3 if i % 7 == 0 else 0,
                                chash=i % 40))
    rows[0].append(_row("https://d0.com/dup", 0, 1, 50))
    rows[1].append(_row("https://d0.com/dup", 1, 1, 0))
    rows[2].append(_row(canonicalize(RAW), 2, 4, 5))
    features = tmp_path / "features"
    for s, part in rows.items():
        (features / f"shard={s:03d}").mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(part), features / f"shard={s:03d}" / "part.parquet")
    atomic_write_json(features / "meta.json", {"as_of": AS_OF})
    past = [f"https://d{i % 6}.com/p0-{i}" for i in range(0, 30, 3)] + [RAW]
    target = ([f"https://d{i % 6}.com/p{i % 3}-{i}" for i in range(0, 150, 6)]
              + ["https://d0.com/dup", "https://d1.com/never", RAW, "https://d5.com/never"])
    golden_rows = (
        [(1, PAST, 100 + n // 3, ["head"], u) for n, u in enumerate(past)]
        + [(2, TARGET, 200 + n // 3, ["head"] if (n // 3) % 2 else ["random", "head"], u)
           for n, u in enumerate(target)])
    g = tmp_path / "golden"
    g.mkdir()
    domain_of = {f"d{i}.com": i + 1 for i in range(6)}
    pq.write_table(pa.table({
        "batch_id": pa.array([r[0] for r in golden_rows], pa.int64()),
        "batch_created_at": pa.array([r[1] for r in golden_rows], pa.timestamp("us", tz="UTC")),
        "query_id": pa.array([r[2] for r in golden_rows], pa.int64()),
        "tags": [r[3] for r in golden_rows],
        "raw_url": [r[4] for r in golden_rows],
        "canonical_url": [canonicalize(r[4]) for r in golden_rows],
        "domain_key": [r[4].split("/")[2] for r in golden_rows],
        "domain_id": pa.array([domain_of[r[4].split("/")[2]] for r in golden_rows], pa.int64()),
    }), g / "golden.parquet")
    pq.write_table(pa.table({"canonical_url": [canonicalize(u) for u in past],
                             "raw_url": past}), g / "past_golden.parquet")
    pq.write_table(pa.table({"domain_id": pa.array([1, 4], pa.int64()),
                             "prior": pa.array([3.0, 1.0], pa.float64())}),
                   g / "domain_prior.parquet")
    atomic_write_json(g / "meta.json", {"target_batch": 2, "as_of": TARGET,
                                        "batches_used": [1]})
    return features, g


def _cfg(**changes):
    cfg = with_changes(example_config(), **{
        "run__k": K, "labels__sample_mod": 1, "selection__exact_sort_max_rows": 10,
        "eval__bootstrap_samples": 50, **changes})
    base = cfg.selection.profiles["baseline_v1"]
    rate = dataclasses.replace(base, weights=dataclasses.replace(
        base.weights, prior=0.0, prior_rate=2.0, domain_ext=1.0, url_date=0.5))
    return with_changes(cfg, selection__profiles={**cfg.selection.profiles, "rate_v1": rate})


def _produce(cfg, tmp_path, features, g, profile):
    """The production path: score, exact top-K, metric replica."""
    cfg = with_changes(cfg, selection__profile=profile)
    run = tmp_path / f"run_{profile}"
    con = _con()
    build_scored(cfg, con, features, g, run, AS_OF)
    select_top_k(con, str(run / "scored" / "**" / "*.parquet"), K,
                 cfg.selection.exact_sort_max_rows, cfg.selection.max_dedupe_rounds,
                 run / "selected.parquet")
    return run, evaluate(cfg, con, run / "selected.parquet", g, features,
                         read_json(g / "meta.json"), run)


def _label(cfg, tmp_path, features, g, selected, **kw):
    args = {"batch": 2, "in_sample": False, "forward": True, "extra": {"source": "test"}}
    args.update(kw)
    return write_labels(cfg, _con(), features_dir=features, run_golden_dir=g,
                        eval_golden_dir=g, selected=selected, root=tmp_path / "labels",
                        dims_dir=None, **args)


@pytest.mark.parametrize("emit_raw", [False, True])
def test_sweep_at_sample_mod_1_equals_exact_top_k(tmp_path, emit_raw):
    cfg = _cfg(selection__emit_raw_spelling_for_past_golden=emit_raw)
    features, g = _universe(tmp_path)
    profiles = ["baseline_v1", "ext_tier_v1", "rate_v1"]
    reports = {p: _produce(cfg, tmp_path, features, g, p)[1] for p in profiles}
    _label(cfg, tmp_path, features, g, tmp_path / "run_baseline_v1" / "selected.parquet")
    got = sweep(cfg, _con(), tmp_path / "labels", profiles)
    tags = got["batches"]["2"]["tags"]
    for p in profiles:
        for tag in ("head", "random"):
            exact = reports[p]["results"][f"batch2_{tag}"]
            assert tags[tag][p]["indexed_exact"] == exact["indexed_exact"], (p, tag)
            assert tags[tag][p]["ci95"] == pytest.approx(exact["indexed_exact_ci95"])
    assert got["pooled"]["forward"]["head"]["baseline_v1"]["indexed_exact"] == (
        reports["baseline_v1"]["results"]["batch2_head"]["indexed_exact"])
    # The raw spelling of a past-golden URL is listed only when emitted.
    head = reports["baseline_v1"]["results"]["batch2_head"]
    assert (head["indexed_exact"] < head["indexed_canonical"]) is not emit_raw


def test_sweep_tie_handling_matches_production_order(tmp_path):
    """One tie group straddles K: production keeps md5 order, and so must the sweep."""
    cfg = _cfg()
    features, g = _universe(tmp_path)
    flat = with_changes(cfg, selection__profiles={
        **cfg.selection.profiles,
        "flat": dataclasses.replace(cfg.selection.profiles["baseline_v1"], tiers=[],
                                    weights=dataclasses.replace(
                                        cfg.selection.profiles["baseline_v1"].weights,
                                        inlinks=0.0, ext=0.0, url_score=1.0, prior=0.0))})
    _, report = _produce(flat, tmp_path, features, g, "flat")
    _label(flat, tmp_path, features, g, tmp_path / "run_flat" / "selected.parquet")
    got = sweep(flat, _con(), tmp_path / "labels", ["flat"])
    for tag in ("head", "random"):
        exact = report["results"][f"batch2_{tag}"]["indexed_exact"]
        assert got["batches"]["2"]["tags"][tag]["flat"]["indexed_exact"] == exact
        assert 0 < exact < report["results"][f"batch2_{tag}"]["crawled_in_export"]


def test_labels_hold_flagged_golden_rows_and_pure_hash_sample(tmp_path):
    cfg = _cfg(labels__sample_mod=3)
    features, g = _universe(tmp_path)
    run, report = _produce(cfg, tmp_path, features, g, "baseline_v1")
    path = _label(cfg, tmp_path, features, g, run / "selected.parquet",
                  extra={"eval": report["results"]})
    rows = [r for s in range(3)
            for r in pq.read_table(features / f"shard={s:03d}" / "part.parquet").to_pylist()]
    sample = pq.read_table(path / "sample.parquet").to_pylist()
    assert sorted(r["url"] for r in sample) == sorted(
        r["url"] for r in rows if md5_number_lower(r["url"]) % 3 == 0)
    listed = set(pq.read_table(run / "selected.parquet")["url"].to_pylist())
    assert all(r["selected"] == (r["url"] in listed) for r in sample)
    golden = {r["raw_url"]: r for r in pq.read_table(path / "golden.parquet").to_pylist()}
    assert not golden[RAW]["crawled_exact"] and golden[RAW]["crawled_canonical"]
    assert not golden["https://d1.com/never"]["crawled_canonical"]
    assert golden["https://d1.com/never"]["tier"] is None
    assert all(r["selected_exact"] == (u in listed) for u, r in golden.items())
    meta = read_json(path / "meta.json")
    assert meta["candidates"] == len(rows) and meta["sample_rows"] == len(sample)
    assert meta["forward"] is True and meta["in_sample"] is False and meta["p0"] > 0
    assert meta["eval"].keys() == report["results"].keys()
    assert meta["eval"]["batch2_head"]["indexed_exact"] == (
        report["results"]["batch2_head"]["indexed_exact"])
    assert label_sets(tmp_path / "labels") == [path]


def test_labels_keep_out_of_sample_over_in_sample(tmp_path):
    cfg = _cfg()
    features, g = _universe(tmp_path)
    run, _ = _produce(cfg, tmp_path, features, g, "baseline_v1")
    sel = run / "selected.parquet"
    first = _label(cfg, tmp_path, features, g, sel, in_sample=True, forward=False)
    assert read_json(first / "meta.json")["in_sample"] is True
    assert _label(cfg, tmp_path, features, g, sel, in_sample=True, forward=False) is None
    replaced = _label(cfg, tmp_path, features, g, sel)
    assert read_json(replaced / "meta.json")["in_sample"] is False
    assert _label(cfg, tmp_path, features, g, sel, in_sample=True) is None
    assert sorted(p.name for p in (tmp_path / "labels").iterdir()) == ["2"]


def test_labels_skip_batches_that_fed_the_tier(tmp_path):
    cfg = _cfg()
    features, g = _universe(tmp_path)
    run, report = _produce(cfg, tmp_path, features, g, "baseline_v1")
    rt = pipeline.Runtime(with_changes(cfg, run__work_dir=str(tmp_path / "work")))
    both = {"batches": [1, 2], "results": {
        "batch1_head": {"in_sample": True}, **report["results"]}}
    out = pipeline._log_labels(rt, both, features_dir=features, run_golden_dir=g,
                               eval_golden_dir=g, selected=run / "selected.parquet",
                               forward=True, source="test")
    assert out == {"1": "skipped", "2": "written"}
    assert [p.name for p in label_sets(rt.layout.labels)] == ["2"]


def test_paired_bootstrap_uses_same_query_draws():
    queries = {i: {f"u{i}", f"v{i}"} for i in range(40)}
    a = {f"u{i}" for i in range(0, 40, 2)}
    assert _paired_bootstrap(queries, a, a, 200, 7) == (0.0, 0.0)
    lo, hi = _paired_bootstrap(queries, a, a | {f"v{i}" for i in range(10)}, 200, 7)
    assert 0 < lo <= hi  # b only adds hits, so every paired draw is >= 0


def test_diagnostics_top_domains_and_dup_multiplicity(tmp_path):
    cfg = _cfg(labels__top_domains=2)
    features, g = _universe(tmp_path)
    run, _ = _produce(cfg, tmp_path, features, g, "baseline_v1")
    path = _label(cfg, tmp_path, features, g, run / "selected.parquet")
    diag = read_json(path / "meta.json")["diagnostics"]
    con = _con()
    feats = f"read_parquet('{features}/shard=*/*.parquet')"
    sel = f"read_parquet('{run / 'selected.parquet'}')"
    # sample_mod = 1: the sample is every feature row, so the estimates are exact.
    listed_rows = f"SELECT f.* FROM {feats} f SEMI JOIN {sel} s ON s.url = f.url"
    (n_listed,) = con.execute(f"SELECT count(*) FROM ({listed_rows})").fetchone()
    expected = con.execute(f"SELECT domain_id, count(*) n FROM ({listed_rows}) "
                           f"GROUP BY 1 ORDER BY n DESC, 1 LIMIT 2").fetchall()
    assert diag["sampled_listed"] == n_listed
    assert [(d["domain_id"], round(d["list_share"] * n_listed)) for d in diag["top_domains"]
            ] == expected
    (share,) = con.execute(
        f"WITH m AS (SELECT content_hash64, count(*) n FROM {feats} GROUP BY 1) "
        f"SELECT avg(CASE WHEN m.n > 1 THEN 1.0 ELSE 0.0 END) FROM ({listed_rows}) s "
        f"JOIN m ON m.content_hash64 = s.content_hash64").fetchone()
    assert diag["content_dup"]["share_with_copies"] == pytest.approx(share)
