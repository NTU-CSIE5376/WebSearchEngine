import dataclasses
from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from helpers import example_config, with_changes

from idxsel.features import build_features
from idxsel.scoring import KEY_SCALE, scored_select_sql
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
