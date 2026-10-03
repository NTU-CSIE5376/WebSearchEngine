import dataclasses
import math
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import example_config, with_changes

from idxsel.features import build_features
from idxsel.manifest import atomic_write_json
from idxsel.scoring import KEY_SCALE, build_domain_stats, mean_rate, scored_select_sql
from idxsel.urlnorm import md5_number_lower

AS_OF = datetime(2026, 10, 1, tzinfo=UTC)


def _feature_row(url, **kw):
    row = {"url": url, "domain_id": 1, "first_seen": AS_OF, "last_fetch_ok": AS_OF,
           "num_fetch_ok_90d": 1, "num_consecutive_fail": 0, "inlink_count_approx": 0,
           "inlink_count_external": 0, "parent_page_score": None, "url_score": 0.0,
           "source": 0, "discovery_source_type": 1, "robots_bits": 1, "is_redirect": None,
           "redirect_hop_count": None, "has_json_ld": None, "hreflang_count": None,
           "has_anchor": False, "title_null": False, "content_hash64": 1,
           "observed_at": AS_OF, "shard": 0}
    row.update(kw)
    return row


def _write(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def _golden(tmp_path, past=(), prior=(), raw=None):
    g = tmp_path / "golden"
    g.mkdir()
    raw = raw or {}
    pq.write_table(pa.table({"canonical_url": pa.array(list(past), pa.string()),
                             "raw_url": pa.array([raw.get(u, u) for u in past], pa.string())}),
                   g / "past_golden.parquet")
    pq.write_table(pa.table({"domain_id": pa.array([d for d, _ in prior], pa.int64()),
                             "prior": pa.array([p for _, p in prior], pa.float64())}),
                   g / "domain_prior.parquet")
    return g


def _scores(tmp_path, rows, cfg=None, past=(), prior=(), raw=None):
    cfg = cfg or example_config()
    _write(tmp_path / "f" / "shard=000" / "part.parquet", rows)
    g = _golden(tmp_path, past, prior, raw)
    sql = scored_select_sql(cfg, str(tmp_path / "f" / "shard=*" / "*.parquet"), g, AS_OF,
                            cfg.active_profile)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return {r[0]: r for r in con.execute(f"SELECT url, tier, key52, h, score FROM ({sql})")
            .fetchall()}


def test_past_golden_tier_and_monotone_inlinks(tmp_path):
    rows = [_feature_row("https://a.com/1", inlink_count_approx=5),
            _feature_row("https://a.com/2", inlink_count_approx=50),
            _feature_row("https://a.com/3", inlink_count_approx=0)]
    s = _scores(tmp_path, rows, past=["https://a.com/3"])
    assert s["https://a.com/3"][1] == 1
    assert s["https://a.com/1"][1] == s["https://a.com/2"][1] == 0
    assert s["https://a.com/2"][2] > s["https://a.com/1"][2]
    assert s["https://a.com/3"][4] > s["https://a.com/2"][4]  # tier dominates
    assert s["https://a.com/1"][3] == md5_number_lower("https://a.com/1")
    assert all(0 <= r[2] < KEY_SCALE for r in s.values())


def test_injected_url_score_is_imputed_and_prior_counts(tmp_path):
    rows = [_feature_row("https://a.com/natural", url_score=0.0),
            _feature_row("https://a.com/injected", url_score=3.0, source=1),
            _feature_row("https://b.com/x", domain_id=2)]
    s = _scores(tmp_path, rows, prior=[(2, 30.0)])
    assert s["https://a.com/natural"][2] == s["https://a.com/injected"][2]
    assert s["https://b.com/x"][2] > s["https://a.com/natural"][2]


def test_negative_weights_stay_in_unit_range(tmp_path):
    cfg = with_changes(example_config(), selection__profile="ext_tier_v1")
    rows = [_feature_row("https://a.com/?q=1"), _feature_row("https://a.com/",
                                                             inlink_count_external=3)]
    s = _scores(tmp_path, rows, cfg=cfg)
    assert s["https://a.com/"][1] == 1  # external_inlink tier
    for _, tier, _, _, score in s.values():
        assert tier <= score <= tier + 1


def test_features_latest_observation_wins_with_slack(tmp_path):
    cfg = example_config()
    slack = timedelta(seconds=cfg.export.base_observation_slack_s)
    read_at = AS_OF
    base = [_feature_row("https://a.com/1", inlink_count_approx=1, observed_at=read_at),
            _feature_row("https://a.com/2", inlink_count_approx=1, observed_at=read_at),
            _feature_row("https://a.com/3", inlink_count_approx=1, observed_at=read_at)]
    _write(tmp_path / "base" / "shard=000" / "p.parquet", base)
    delta = [
        # committed after the base read: wins
        {**_feature_row("https://a.com/1", inlink_count_approx=9,
                        observed_at=read_at + timedelta(minutes=5)), "snapshot_id": 1},
        # started before the read but within slack (still open): wins
        {**_feature_row("https://a.com/2", inlink_count_approx=8,
                        observed_at=read_at - slack / 2), "snapshot_id": 2},
        # clearly older than the base read: loses
        {**_feature_row("https://a.com/3", inlink_count_approx=7,
                        observed_at=read_at - slack * 2), "snapshot_id": 3},
        {**_feature_row("https://a.com/4", inlink_count_approx=4), "snapshot_id": 4},
    ]
    _write(tmp_path / "delta" / "shard=000" / "p.parquet", delta)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    summary = build_features(cfg, con, ("base", tmp_path / "base"), [tmp_path / "delta"],
                             tmp_path / "out", [0, 1])
    got = {r["url"]: r["inlink_count_approx"] for r in pq.read_table(
        tmp_path / "out" / "shard=000" / "part.parquet").to_pylist()}
    assert got == {"https://a.com/1": 9, "https://a.com/2": 8, "https://a.com/3": 1,
                   "https://a.com/4": 4}
    assert summary["rows"] == 4
    out_schema = pq.read_schema(tmp_path / "out" / "shard=000" / "part.parquet")
    assert "snapshot_id" not in out_schema.names
    assert "obs_rank" in out_schema.names


def test_compaction_equals_a_full_merge(tmp_path):
    """(base + d1) compacted, then + d2, equals base + d1 + d2.

    d2 re-reads d1's history rows (the id overlap). Two fetches of a.com/1 lie
    two minutes apart, inside the slack: re-applying the slack to the compacted
    row would let the older fetch win.
    """
    cfg = example_config()
    slack = timedelta(seconds=cfg.export.base_observation_slack_s)
    base = [_feature_row(f"https://a.com/{i}", inlink_count_approx=1) for i in (1, 2, 3)]
    older = {**_feature_row("https://a.com/1", inlink_count_approx=5,
                            observed_at=AS_OF + timedelta(minutes=1)), "snapshot_id": 10}
    newer = {**_feature_row("https://a.com/1", inlink_count_approx=6,
                            observed_at=AS_OF + timedelta(minutes=3)), "snapshot_id": 11}
    d1 = [older, newer, {**_feature_row("https://a.com/2", inlink_count_approx=7,
                                        observed_at=AS_OF - slack / 2), "snapshot_id": 12}]
    d2 = [older, newer,
          {**_feature_row("https://a.com/3", inlink_count_approx=8,
                          observed_at=AS_OF + timedelta(days=1)), "snapshot_id": 20},
          {**_feature_row("https://a.com/9", inlink_count_approx=9), "snapshot_id": 21}]
    for name, rows in (("base", base), ("d1", d1), ("d2", d2)):
        _write(tmp_path / name / "shard=000" / "p.parquet", rows)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    build_features(cfg, con, ("base", tmp_path / "base"), [tmp_path / "d1", tmp_path / "d2"],
                   tmp_path / "full", [0])
    build_features(cfg, con, ("base", tmp_path / "base"), [tmp_path / "d1"],
                   tmp_path / "step1", [0])
    build_features(cfg, con, ("merged", tmp_path / "step1"), [tmp_path / "d2"],
                   tmp_path / "step2", [0])

    def rows(name):
        return pq.read_table(tmp_path / name / "shard=000" / "part.parquet").sort_by(
            "url").to_pylist()

    assert rows("step2") == rows("full")
    assert {r["url"]: r["inlink_count_approx"] for r in rows("step2")} == {
        "https://a.com/1": 6, "https://a.com/2": 7, "https://a.com/3": 8, "https://a.com/9": 9}


def test_same_transaction_rows_break_ties_on_snapshot_id(tmp_path):
    """The ingestor writes duplicate URLs of a batch as separate statements of one
    transaction, so their history rows share snapshot_at; the later id is final."""
    cfg = example_config()
    d1 = []
    for i in range(200):
        url = f"https://a.com/{i}"
        d1.append({**_feature_row(url, num_consecutive_fail=0), "snapshot_id": 2 * i})
        d1.append({**_feature_row(url, num_consecutive_fail=1), "snapshot_id": 2 * i + 1})
    _write(tmp_path / "d1" / "shard=000" / "p.parquet", d1)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    build_features(cfg, con, ("base", tmp_path / "none"), [tmp_path / "d1"], tmp_path / "s1", [0])
    # Compaction keeps obs_seq: re-reading the same rows (the id overlap) changes nothing.
    build_features(cfg, con, ("merged", tmp_path / "s1"), [tmp_path / "d1"], tmp_path / "s2", [0])
    for name in ("s1", "s2"):
        got = pq.read_table(tmp_path / name / "shard=000" / "part.parquet").to_pylist()
        assert len(got) == 200 and {r["num_consecutive_fail"] for r in got} == {1}


def _only(signal, weight=1.0, **changes):
    """A profile that scores with one signal; `changes` go into the config."""
    cfg = with_changes(example_config(), **changes)
    weights = {k: 0.0 for k in dataclasses.asdict(cfg.active_profile.weights)}
    weights[signal] = weight
    prof = dataclasses.replace(cfg.active_profile,
                               weights=dataclasses.replace(cfg.active_profile.weights, **weights))
    return with_changes(cfg, selection__profiles={**cfg.selection.profiles, "baseline_v1": prof})


def test_null_parent_score_uses_impute(tmp_path):
    cfg = _only("parent", selection__impute__parent_page_score=0.7)
    rows = [_feature_row("https://a.com/null", parent_page_score=None),
            _feature_row("https://a.com/low", parent_page_score=0.2)]
    s = _scores(tmp_path, rows, cfg=cfg)
    assert s["https://a.com/null"][2] > s["https://a.com/low"][2]
    assert s["https://a.com/null"][4] == pytest.approx(0.7)


def test_pageview_signal_only_for_pageview_rows(tmp_path):
    cfg = _only("pageview")
    rows = [_feature_row("https://w.org/hot", source=2, url_score=1.9),
            _feature_row("https://w.org/cold", source=2, url_score=1.1),
            _feature_row("https://w.org/golden", source=1, url_score=3.0),
            _feature_row("https://w.org/natural", source=0, url_score=0.9)]
    s = _scores(tmp_path, rows, cfg=cfg)
    assert s["https://w.org/hot"][4] == pytest.approx(0.9)
    assert s["https://w.org/cold"][4] == pytest.approx(0.1)
    assert s["https://w.org/golden"][4] == s["https://w.org/natural"][4] == 0.0
    # Off by default: baseline_v1 gives pageview no weight.
    base = _scores(tmp_path / "b", rows)
    assert base["https://w.org/hot"][2] == base["https://w.org/cold"][2]


@pytest.mark.parametrize("emit", [False, True])
def test_raw_spelling_for_past_golden(tmp_path, emit):
    cfg = with_changes(example_config(), selection__emit_raw_spelling_for_past_golden=emit)
    raw = "https://a.com/p?b=2&a=1"
    s = _scores(tmp_path, [_feature_row("https://a.com/p?a=1&b=2"), _feature_row("https://a.com/q")],
                cfg=cfg, past=["https://a.com/p?a=1&b=2"], raw={"https://a.com/p?a=1&b=2": raw})
    listed = raw if emit else "https://a.com/p?a=1&b=2"
    assert s[listed][1] == 1
    assert s[listed][3] == md5_number_lower(listed)
    assert "https://a.com/q" in s


@pytest.mark.parametrize("bits", [[], [2]])
def test_robots_filter(tmp_path, bits):
    cfg = with_changes(example_config(), selection__filters__exclude_robots_bits=bits)
    rows = [_feature_row("https://a.com/ok", robots_bits=1),
            _feature_row("https://a.com/no", robots_bits=2)]
    s = _scores(tmp_path, rows, cfg=cfg)
    assert ("https://a.com/no" in s) == (not bits)


BATCH1 = datetime(2026, 9, 1, tzinfo=UTC)
EARLY, LATE = datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 9, 20, tzinfo=UTC)


def _rate_fixture(tmp_path):
    """Domain 1: 3 pages fetched before batch 1's cutoff, 1 after; domain 2: 2 before.

    Batch 1 lists a.com/1 (hittable), a.com/4 (fetched after the cutoff) and
    a.com/9 (never crawled): only a.com/1 counts.
    """
    rows = [_feature_row(f"https://a.com/{i}", last_fetch_ok=EARLY) for i in (1, 2, 3)]
    rows += [_feature_row("https://a.com/4", last_fetch_ok=LATE, inlink_count_external=7)]
    rows += [_feature_row(f"https://b.com/{i}", domain_id=2, last_fetch_ok=EARLY,
                          inlink_count_external=3) for i in (1, 2)]
    _write(tmp_path / "f" / "shard=000" / "part.parquet", rows)
    g = _golden(tmp_path)
    pq.write_table(pa.table({
        "batch_id": pa.array([1, 1, 1], pa.int64()),
        "batch_created_at": pa.array([BATCH1] * 3, pa.timestamp("us", tz="UTC")),
        "raw_url": ["https://a.com/1", "https://a.com/4", "https://a.com/9"]}),
        g / "golden.parquet")
    atomic_write_json(g / "meta.json", {"as_of": AS_OF, "batches_used": [1]})
    return str(tmp_path / "f" / "shard=*" / "*.parquet"), g


def _con():
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    return con


def _signal_scores(cfg, glob, g, domain):
    sql = scored_select_sql(cfg, glob, g, AS_OF, cfg.active_profile, domain)
    return dict(_con().execute(f"SELECT url, score FROM ({sql})").fetchall())


def test_prior_rate_counts_only_hittable_golden(tmp_path):
    cfg = example_config()
    glob, g = _rate_fixture(tmp_path)
    domain = build_domain_stats(cfg, _con(), glob, g, tmp_path / "ds.parquet")
    w = 0.5 ** (30 / 90)  # batch 1 is 30 days older than the target's as_of
    stats = {r["domain_id"]: r for r in pq.read_table(domain.stats).to_pylist()}
    assert stats[1]["crawled"] == 4 and stats[1]["ext_inlinks"] == 7
    assert stats[1]["e"] == pytest.approx(3 * w) and stats[1]["g"] == pytest.approx(w)
    assert stats[2]["e"] == pytest.approx(2 * w) and stats[2]["g"] == 0
    assert domain.p0 == pytest.approx(0.2)


def test_prior_rate_matches_gamma_poisson_formula(tmp_path):
    cfg = example_config()
    glob, g = _rate_fixture(tmp_path)
    domain = build_domain_stats(cfg, _con(), glob, g, tmp_path / "ds.parquet")
    w, a, p0, lift = 0.5 ** (30 / 90), 1.0, 0.2, math.log(20.0)

    def expected(golden, exposure):
        lifted = math.log((golden + a) / ((exposure + a / p0) * p0))
        return (max(-lift, min(lift, lifted)) + lift) / (2 * lift)

    s = _signal_scores(_only("prior_rate"), glob, g, domain)
    assert s["https://a.com/2"] == pytest.approx(expected(w, 3 * w), rel=1e-12)
    assert s["https://b.com/1"] == pytest.approx(expected(0.0, 2 * w), rel=1e-12)
    assert s["https://a.com/2"] > 0.5 > s["https://b.com/1"]
    # A tight lift clips both domains to the ends of the unit range.
    s = _signal_scores(_only("prior_rate", selection__caps__prior_rate_lift=1.1), glob, g,
                       domain)
    assert s["https://a.com/2"] == pytest.approx(1.0) and s["https://b.com/1"] == pytest.approx(0)
    # A large pseudo-count shrinks every domain toward the mean rate (0.5).
    s = _signal_scores(_only("prior_rate", golden__prior_rate_pseudo_golden=1e6), glob, g,
                       domain)
    assert s["https://a.com/2"] == pytest.approx(0.5, abs=1e-5)


def test_domain_stats_are_deterministic(tmp_path):
    cfg = example_config()
    rows = [_feature_row(f"https://d{i % 50}.com/{i}", domain_id=i % 50 + 1,
                         last_fetch_ok=EARLY if i % 3 else LATE,
                         inlink_count_external=i % 7) for i in range(20_000)]
    for part in range(4):
        _write(tmp_path / "f" / f"shard={part:03d}" / "part.parquet", rows[part::4])
    g = _golden(tmp_path)
    pq.write_table(pa.table({
        "batch_id": pa.array([1] * 200, pa.int64()),
        "batch_created_at": pa.array([BATCH1] * 200, pa.timestamp("us", tz="UTC")),
        "raw_url": [f"https://d{i % 50}.com/{i}" for i in range(0, 2000, 10)]}),
        g / "golden.parquet")
    atomic_write_json(g / "meta.json", {"as_of": AS_OF, "batches_used": [1]})
    glob = str(tmp_path / "f" / "shard=*" / "*.parquet")
    runs = []
    for n in (1, 2):
        con = _con()
        con.execute("SET threads = 4")
        d = build_domain_stats(cfg, con, glob, g, tmp_path / f"ds{n}.parquet")
        runs.append((d.p0, sorted(pq.read_table(d.stats).to_pylist(),
                                  key=lambda r: r["domain_id"])))
    assert runs[0] == runs[1]
    assert runs[0][0] > 0


def test_domain_ext_url_date_and_markup_flags(tmp_path):
    cfg = example_config()
    glob, g = _rate_fixture(tmp_path)
    domain = build_domain_stats(cfg, _con(), glob, g, tmp_path / "ds.parquet")
    s = _signal_scores(_only("domain_ext"), glob, g, domain)
    assert s["https://a.com/1"] == pytest.approx(math.log1p(7) / math.log1p(1_000_000))
    assert s["https://b.com/1"] == pytest.approx(math.log1p(6) / math.log1p(1_000_000))
    rows = [_feature_row("https://n.com/2026/09/30/story"),
            _feature_row("https://n.com/2026-09/story"),
            _feature_row("https://n.com/2026/02/31/bad-date"),
            _feature_row("https://n.com/2026/11/02/future"),
            _feature_row("https://n.com/plain", has_json_ld=True, hreflang_count=3,
                         title_null=True)]
    _write(tmp_path / "u" / "shard=000" / "part.parquet", rows)
    uglob = str(tmp_path / "u" / "shard=*" / "*.parquet")
    s = _signal_scores(_only("url_date"), uglob, g, None)
    assert s["https://n.com/2026/09/30/story"] == pytest.approx(math.exp(-1 / 30))
    assert s["https://n.com/2026-09/story"] == pytest.approx(math.exp(-30 / 30))
    assert s["https://n.com/2026/02/31/bad-date"] == 0.0
    assert s["https://n.com/2026/11/02/future"] == pytest.approx(1.0)
    assert s["https://n.com/plain"] == 0.0
    for name in ("json_ld", "hreflang", "no_title"):
        flags = _signal_scores(_only(name), uglob, g, None)
        assert flags["https://n.com/plain"] == 1.0 and flags["https://n.com/2026-09/story"] == 0


def test_shipped_profiles_never_join_domain_stats(tmp_path):
    cfg = example_config()
    glob, g = _rate_fixture(tmp_path)
    for name, profile in cfg.selection.profiles.items():
        sql = scored_select_sql(cfg, glob, g, AS_OF, profile)
        assert " ds " not in sql and "ds." not in sql, name
    with pytest.raises(ValueError, match="domain stats"):
        scored_select_sql(cfg, glob, g, AS_OF, _only("prior_rate").active_profile)


def test_mean_rate_is_exact_whatever_the_row_order(tmp_path):
    # A naive float sum drops each 1.0 added to 1e16 (doubles there are 2 apart).
    e = [1e16] + [1.0] * 1000
    g = [3.0] + [0.5] * 1000
    for name, order in (("fwd", 1), ("rev", -1)):
        pq.write_table(pa.table({"e": e[::order], "g": g[::order]}), tmp_path / f"{name}.parquet")
    p0 = mean_rate(tmp_path / "fwd.parquet")
    assert p0 == mean_rate(tmp_path / "rev.parquet") == (3.0 + 500.0) / (1e16 + 1000.0)
    pq.write_table(pa.table({"e": [0.0], "g": [0.0]}), tmp_path / "none.parquet")
    assert mean_rate(tmp_path / "none.parquet") == 0.0
