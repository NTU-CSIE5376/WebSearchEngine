"""End to end through the real CLI against local stand-ins of every database."""

from __future__ import annotations

import json
import math
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from urllib.parse import urlsplit

import psycopg
import pyarrow.parquet as pq
import pytest
from conftest import ENV, K
from fixture_db import STATE_COLUMNS, add_batch, drop_batch, write_accounting_rows
from helpers import EXAMPLE

from idxsel.manifest import work_lock
from idxsel.urlnorm import canonicalize, md5_number_lower

pytestmark = pytest.mark.integration


def _cli(overlay, *args, env=None, code=0):
    """Runs the CLI; returns its stdout JSON, or for a failure code the stderr error JSON."""
    done = subprocess.run(
        [sys.executable, "-m", "idxsel.cli", "--config", str(EXAMPLE), "--config", str(overlay),
         *args], capture_output=True, text=True, env=env or os.environ.copy(), timeout=600)
    assert done.returncode == code, done.stderr[-4000:]
    if code:
        return json.loads(done.stderr.strip().splitlines()[-1])
    return json.loads(done.stdout)


def _fetch_fresh(servers, per_shard):
    """The crawler fetches never-crawled pages with huge inlink counts; history is written."""
    fresh = []
    with psycopg.connect(servers["crawler"].dsn(db="crawlerdb"), autocommit=True) as conn:
        for shard in range(4):
            rows = conn.execute(
                f"SELECT url FROM url_state_current_{shard:03d} WHERE last_fetch_ok IS NULL "
                f"ORDER BY url LIMIT {per_shard}").fetchall()
            for (url,) in rows:
                fresh.append(url)
                conn.execute(
                    f"UPDATE url_state_current_{shard:03d} SET last_fetch_ok = now(), "
                    f"inlink_count_approx = 100000, inlink_count_external = 5000, url_score = 1.0, "
                    f"robots_bits = 1, should_crawl = false WHERE url = %s", (url,))
                conn.execute(
                    f"INSERT INTO url_state_history_{shard:03d} ({', '.join(STATE_COLUMNS)}, "
                    f"snapshot_at) SELECT {', '.join(STATE_COLUMNS)}, now() FROM "
                    f"url_state_current_{shard:03d} WHERE url = %s", (url,))
    return fresh


def _admin(servers):
    return psycopg.connect(servers["env"]["IDXSEL_TEST_SELECTDB_ADMIN_DSN"], autocommit=True)


def _truth(servers):
    """Current crawled rows straight from the fixture crawlerdb."""
    rows = {}
    with psycopg.connect(servers["crawler"].dsn(db="crawlerdb")) as conn:
        for shard in range(4):
            cur = conn.execute(f"SELECT {', '.join(STATE_COLUMNS)} FROM url_state_current_{shard:03d}"
                               f" WHERE last_fetch_ok IS NOT NULL")
            for r in cur.fetchall():
                rows[r[0]] = dict(zip(STATE_COLUMNS, r, strict=True))
    return rows


def _golden(servers):
    with psycopg.connect(servers["crawler"].dsn(db="metricdb")) as conn:
        return conn.execute(
            "SELECT b.id, b.created_at, q.tags, u.url FROM metric_url u "
            "JOIN metric_queries q ON q.id = u.query_id JOIN metric_batches b ON b.id = q.batch_id"
        ).fetchall()


def _independent_scores(servers, truth):
    """Re-implements baseline_v1 in plain Python (no DuckDB, no idxsel scoring code)."""
    golden = _golden(servers)
    target_at = datetime(2026, 9, 17, tzinfo=UTC)
    domain_of = {}
    for dom_id, key, _ in servers["universe"].domains:
        domain_of[key] = dom_id

    def domain_id(url):
        host = urlsplit(url).hostname or ""
        if host == "en.wikipedia.org":
            return domain_of["en.wikipedia.org"]
        key = ".".join(host.split(".")[-2:])
        return domain_of.get(key)

    past = {canonicalize(u) for b, _, _, u in golden if b < 3}
    per_batch = {}
    for b, created, _, u in golden:
        if b < 3 and domain_id(u) is not None:
            per_batch.setdefault((domain_id(u), b, created), set()).add(canonicalize(u))
    prior = {}
    for (d, _, created), urls in per_batch.items():
        age = (target_at - created.replace(tzinfo=UTC)).total_seconds() / 86400
        prior[d] = prior.get(d, 0.0) + len(urls) * 0.5 ** (age / 90)

    def norm_log(x, cap):
        return min(math.log1p(max(x, 0)), math.log1p(cap)) / math.log1p(cap)

    scores = {}
    for url, r in truth.items():
        us = r["url_score"] if r["source"] == 0 and 0 <= r["url_score"] <= 1 else 0.0
        raw = (norm_log(r["inlink_count_approx"], 10_000) + norm_log(r["inlink_count_external"], 1_000)
               + us + 2 * norm_log(prior.get(r["domain_id"], 0.0), 200))
        tier = 1 if url in past else 0
        scores[url] = (tier, raw / 5.0, md5_number_lower(url))
    return scores


def _assert_valid_top_k(scores, selected):
    chosen = [scores[u] for u in selected]
    rest = [s for u, s in scores.items() if u not in selected]
    worst = min(chosen, key=lambda s: (s[0], s[1]))
    best_rest = max(rest, key=lambda s: (s[0], s[1]))
    assert (worst[0], worst[1] + 1e-9) >= (best_rest[0], best_rest[1])


def _shard_of(servers, url):
    by_id = {dom_id: shard for dom_id, _, shard in servers["universe"].domains}
    return by_id[servers["universe"].rows[url]["domain_id"]]


def _selection(servers):
    with _admin(servers) as conn:
        urls = [r[0] for r in conn.execute("SELECT url FROM selected_urls_current").fetchall()]
        runs = conn.execute("SELECT id, status, eval_metrics, selection_metrics, source_paths "
                            "FROM model_runs ORDER BY id").fetchall()
    return urls, runs


def test_run_once_end_to_end(overlay, servers, fresh_selectdb):
    assert _cli(overlay, "init-selectdb") == {"initialized": True}
    out = _cli(overlay, "run", "--once")
    assert out["export"]["mode"] == "full"
    assert out["swap"]["swapped"] is True
    assert out["verify_live"]["ok"] is True
    urls, runs = _selection(servers)
    assert len(urls) == K and len(set(urls)) == K
    truth = _truth(servers)
    assert set(urls) <= set(truth), "every selected URL must be crawled"
    _assert_valid_top_k(_independent_scores(servers, truth), set(urls))
    assert [r[1] for r in runs] == ["live"]
    assert "password" not in json.dumps([r[2:] for r in runs], default=str)

    # The official metric's exact query, run as the role the metric uses.
    golden = _golden(servers)
    with psycopg.connect(servers["env"]["IDXSEL_TEST_SELECTDB_READER_DSN"]) as reader:
        for tag in ("head", "random"):
            raw = sorted({u for b, _, tags, u in golden if b == 3 and tag in tags})
            hits = {r[0] for r in reader.execute(
                "SELECT url FROM public.selected_urls_current WHERE url = ANY(%s)", (raw,))}
            expected = len(hits) / len(raw)
            # Evaluated from the run's own Parquet, identical to the metric's SQL.
            assert out["eval"][f"batch3_{tag}"]["indexed_exact_rate"] == pytest.approx(expected)
            # Batch 3 is older than the export: the crawler may have reacted to it.
            assert out["eval"][f"batch3_{tag}"]["in_sample"] is True
    with pytest.raises(psycopg.errors.InsufficientPrivilege), psycopg.connect(
            servers["env"]["IDXSEL_TEST_SELECTDB_READER_DSN"]) as reader:
        reader.execute("DELETE FROM selected_urls_current")
    with _admin(servers) as conn:
        # COPY FREEZE writes frozen tuples (no hint-bit writes on first read) and marks
        # the pages it filled from empty all-visible; a plain COPY would leave 0 here.
        pages, visible = conn.execute("SELECT relpages, relallvisible FROM pg_class "
                                      "WHERE relname = 'selected_urls_current'").fetchone()
        (evaluated,) = conn.execute("SELECT count(*) FROM model_runs "
                                    "WHERE eval_metrics ? 'results'").fetchone()
    assert pages > 0 and visible > 0
    assert evaluated == 1


def test_backtest_touches_neither_crawlerdb_nor_selectdb(overlay, servers, fresh_selectdb):
    _cli(overlay, "export", "--mode", "full")
    _cli(overlay, "features")
    work = overlay.parent / "work"

    def snapshot():
        state = {p.name: p.read_bytes() for p in (work / "state").glob("*.json")}
        return state, sorted(p.relative_to(work).as_posix() for p in work.glob("exports/**/*"))

    before = snapshot()
    # Without these DSNs any crawlerdb read or selectdb write would fail.
    env = {k: v for k, v in os.environ.items() if k not in (ENV["crawler"], ENV["admin"])}
    out = _cli(overlay, "backtest", env=env)
    assert out["selection"]["k"] == K
    assert out["eval"]["batch3_head"]["in_sample"] is True  # features postdate batch 3
    assert snapshot() == before
    assert (work / "backtests").exists()
    with _admin(servers) as conn:
        (tables,) = conn.execute("SELECT count(*) FROM pg_tables "
                                 "WHERE schemaname = 'public'").fetchone()
    assert tables == 0


def _with(overlay, old, new, name):
    """A copy of the overlay with one setting changed; same work dir."""
    text = overlay.read_text()
    assert old in text
    path = overlay.parent / name
    path.write_text(text.replace(old, new))
    return path


def test_backtest_after_a_measurement_scores_the_live_list_out_of_sample(
        overlay, servers, fresh_selectdb):
    nxt = _with(overlay, "target_batch = 3", 'target_batch = "next"', "next.toml")
    _cli(nxt, "init-selectdb")
    _cli(nxt, "run", "--once")
    live, _ = _selection(servers)
    crawled_not_listed = sorted(set(_truth(servers)) - set(live))
    # The measurement creates batch 4 after the list was built.
    metric_dsn = _metric_dsn(servers)
    add_batch(metric_dsn, 4, datetime.now(UTC), [
        (f"kw4-{q}", ["head", "random"],
         sorted(live)[q * 3:q * 3 + 3] + crawled_not_listed[q * 3:q * 3 + 3])
        for q in range(10)])
    try:
        out = _cli(nxt, "backtest")
        head = out["eval"]["batch4_head"]
        assert head["in_sample"] is False
        backtested = pq.read_table(f"{out['backtest_dir']}/run/selected.parquet")
        assert set(backtested["url"].to_pylist()) == set(live)  # the same list, rebuilt
        golden = [u for b, _, tags, u in _golden(servers) if b == 4 and "head" in tags]
        with psycopg.connect(servers["env"]["IDXSEL_TEST_SELECTDB_READER_DSN"]) as reader:
            hits = {r[0] for r in reader.execute(
                "SELECT url FROM public.selected_urls_current WHERE url = ANY(%s)",
                (sorted(set(golden)),))}
        assert head["indexed_exact_rate"] == pytest.approx(len(hits) / len(set(golden)))
        assert 0 < head["indexed_exact_rate"] < 1
        # The labels of batch 4 are out of sample; batch 3 fed the list and is skipped.
        assert out["labels"] == {"4": "written"}
        label = overlay.parent / "work" / "labels" / "4"
        meta = json.loads((label / "meta.json").read_text())
        assert meta["in_sample"] is False and meta["forward"] is False
        rows = pq.read_table(label / "golden.parquet").to_pylist()
        head_urls = {r["raw_url"] for r in rows if "head" in r["tags"]}
        listed = {r["raw_url"] for r in rows if "head" in r["tags"] and r["selected_exact"]}
        assert len(listed) / len(head_urls) == pytest.approx(head["indexed_exact_rate"])
    finally:
        drop_batch(metric_dsn, 4)


def _metric_dsn(servers):
    return servers["crawler"].dsn(user="metric", password="metric", db="metricdb")


def test_sweep_matches_backtest_per_profile(overlay, servers):
    _cli(overlay, "export", "--mode", "full")
    _cli(overlay, "features")
    runs = {p: _cli(overlay, "backtest", "--profile", p) for p in ("baseline_v1", "ext_tier_v1")}
    assert runs["baseline_v1"]["labels"] == {"3": "written"}  # in sample: features are newer
    assert runs["ext_tier_v1"]["labels"] == {"3": "kept"}
    swept = _cli(overlay, "sweep", "--profiles", "baseline_v1,ext_tier_v1")
    assert swept["batches"]["3"]["in_sample"] is True
    for profile, run in runs.items():
        assert run["profile"] == profile
        for tag in ("head", "random"):
            exact = run["eval"][f"batch3_{tag}"]
            got = swept["batches"]["3"]["tags"][tag][profile]
            assert got["indexed_exact"] == exact["indexed_exact"], (profile, tag)
            assert got["ci95"] == pytest.approx(exact["indexed_exact_ci95"])
    assert swept["pooled"]["forward"] == {}  # no post-measurement labels yet
    err = _cli(overlay, "backtest", "--profile", "nope", code=2)
    assert err["error"] == "ConfigError" and "nope" in err["reason"]


def test_backtest_reports_cutoff_loss(overlay, servers):
    nxt = _with(overlay, "target_batch = 3", 'target_batch = "next"', "next.toml")
    _cli(nxt, "export", "--mode", "full")
    _cli(nxt, "features")
    time.sleep(1.5)
    early = _fetch_fresh(servers, per_shard=2)  # fetched after the features, before the batch
    time.sleep(1.5)
    created = datetime.now(UTC)
    time.sleep(1.5)
    late = _fetch_fresh(servers, per_shard=1)  # fetched after the batch: no cutoff loss
    metric_dsn = _metric_dsn(servers)
    add_batch(metric_dsn, 4, created, [
        (f"kw4-{q}", ["head", "random"], early[q * 2:q * 2 + 2] + late[q:q + 1])
        for q in range(4)])
    try:
        _cli(nxt, "export", "--mode", "delta")
        head = _cli(nxt, "backtest")["eval"]["batch4_head"]
        assert head["in_sample"] is False and head["crawled_exact"] == 0
        assert head["cutoff_loss_exact"] == len(early) == head["cutoff_loss_canonical"]
        assert head["cutoff_loss_exact_rate"] == pytest.approx(len(early) / (len(early) + 4))
    finally:
        drop_batch(metric_dsn, 4)


def test_long_manual_commands_refuse_inside_the_build_window(overlay, servers):
    # The build window is open (its start lies 1000 h before the next measurement).
    sched = _with(overlay, "[schedule]\nenabled = false",
                  "[schedule]\nenabled = true\nbuild_start_h_before_measure = 1000", "w.toml")
    for args in (("export", "--mode", "delta"), ("backtest",)):
        err = _cli(sched, *args, code=4)
        assert "pre-measurement build has not run yet" in err["reason"]
    assert not (overlay.parent / "work" / "exports").exists()


def test_cli_exits_4_while_the_work_lock_is_held(overlay, servers):
    work = overlay.parent / "work"
    with work_lock(work / "state" / "lock", "a test holder"):
        err = _cli(overlay, "gc", code=4)
        assert err["error"] == "WorkDirBusy" and "a test holder" in err["reason"]
        assert "state" in _cli(overlay, "status")  # read-only commands do not lock
    assert _cli(overlay, "gc") == {"removed_dirs": 0}


def test_delta_cycles_on_the_compacted_set_then_rollback(overlay, servers, fresh_selectdb):
    _cli(overlay, "init-selectdb")
    _cli(overlay, "run", "--once")
    work = overlay.parent / "work"
    truth = _truth(servers)
    fresh = _fetch_fresh(servers, per_shard=2)
    # The accounting job ages counters out and appends partial history rows
    # (inlink counts 0) for the most-linked crawled pages.
    with psycopg.connect(servers["crawler"].dsn(db="crawlerdb"), autocommit=True) as conn:
        top = sorted(truth, key=lambda u: -truth[u]["inlink_count_approx"])[:200]
        for shard in range(4):
            write_accounting_rows(conn, shard, [u for u in top if _shard_of(servers, u) == shard])
    assert len(fresh) == 8 and not set(fresh) & set(truth)
    out = _cli(overlay, "run", "--once")
    assert out["export"]["mode"] == "delta"
    assert out["export"]["rows"] >= len(fresh)
    urls, _ = _selection(servers)
    assert set(fresh) <= set(urls)
    # Still the exact top-K of the true current state: accounting rows changed nothing.
    _assert_valid_top_k(_independent_scores(servers, _truth(servers)), set(urls))
    # Compaction: the feature set is the next merge source; base and merged deltas are gone.
    state = json.loads((work / "state" / "exports.json").read_text())
    assert state["compacted"]["features"] == out["features"]["feature_id"]
    assert state["deltas"] == []
    assert [p.name.split("_")[0] for p in (work / "exports").iterdir()] == ["dims"]

    # Third cycle: compacted set + one new delta, still the exact top-K.
    fresh2 = _fetch_fresh(servers, per_shard=1)
    out = _cli(overlay, "run", "--once")
    assert out["export"]["mode"] == "delta"
    urls, runs = _selection(servers)
    assert set(fresh2) <= set(urls)
    _assert_valid_top_k(_independent_scores(servers, _truth(servers)), set(urls))
    assert [r[1] for r in runs] == ["retired", "retired", "live"]
    with _admin(servers) as conn:
        prev = {r[0] for r in conn.execute("SELECT url FROM selected_urls_prev").fetchall()}
    assert not set(fresh2) & prev

    # A fourth list is loaded but not swapped (e.g. waiting for the freeze to end).
    _cli(overlay, "select")
    pending = _cli(overlay, "load")["db_run_id"]
    rolled = _cli(overlay, "rollback")
    assert rolled == {"live_run": runs[1][0], "dropped_unswapped": [pending]}
    urls_after, runs_after = _selection(servers)
    assert set(urls_after) == prev
    # The pending list can no longer undo the rollback.
    assert [r[1] for r in runs_after] == ["retired", "live", "rolled_back", "abandoned"]
    assert _cli(overlay, "swap", "--run-id", str(pending))["swapped"] is False
