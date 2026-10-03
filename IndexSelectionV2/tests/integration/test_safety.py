"""Safety properties against a real PostgreSQL 16."""

from __future__ import annotations

import random
import threading
import time
from datetime import UTC, datetime

import psycopg
import pyarrow.parquet as pq
import pytest
from fixture_db import STATE_COLUMNS
from helpers import with_changes

from idxsel import export as export_mod
from idxsel.duck import open_duckdb
from idxsel.export import ExportContext, export_base, export_delta
from idxsel.features import build_features
from idxsel.guards import Watchdog
from idxsel.load import init_selectdb, load_selection, rollback, swap
from idxsel.pg import ReadOnlyViolation, SessionHolder, connect, require_env
from idxsel.pipeline import Runtime
from idxsel.throttle import Pacer

pytestmark = pytest.mark.integration
SHARDS = [0, 1, 2, 3]


def _ctx(rt: Runtime, guard: SessionHolder) -> ExportContext:
    return ExportContext(rt.cfg, rt.layout, rt.events, rt.guard(guard),
                         Pacer(rt.cfg.throttle), rt.crawler_session)


def _holder(rt: Runtime, purpose: str) -> SessionHolder:
    return SessionHolder(lambda: rt.crawler_session(purpose))


def _features(rt, base_id, delta_ids, out):
    con = open_duckdb(rt.cfg, out.parent / "duck")
    build_features(rt.cfg, con, ("base", rt.layout.base_dir(base_id)),
                   [rt.layout.delta_dir(d) for d in delta_ids], out, SHARDS)
    got = {}
    for f in out.rglob("*.parquet"):
        for r in pq.read_table(f, columns=["url", "inlink_count_approx", "last_fetch_ok"]
                               ).to_pylist():
            got[r["url"]] = (r["inlink_count_approx"], r["last_fetch_ok"])
    return got


def _crawled_truth(servers):
    truth = {}
    with psycopg.connect(servers["crawler"].dsn(db="crawlerdb")) as conn:
        for shard in SHARDS:
            for url, inl, lfo in conn.execute(
                    f"SELECT url, inlink_count_approx, last_fetch_ok FROM "
                    f"url_state_current_{shard:03d} WHERE last_fetch_ok IS NOT NULL"):
                truth[url] = (inl, lfo.astimezone(UTC))
    return truth


def _fetch(conn, shard, url, inlinks):
    """What the ingestor does on a successful fetch: upsert current, append history."""
    t, h = f"url_state_current_{shard:03d}", f"url_state_history_{shard:03d}"
    with conn.transaction():
        conn.execute(f"UPDATE {t} SET last_fetch_ok = now(), robots_bits = 1, "
                     f"inlink_count_approx = %s WHERE url = %s", (inlinks, url))
        conn.execute(f"INSERT INTO {h} ({', '.join(STATE_COLUMNS)}, snapshot_at) "
                     f"SELECT {', '.join(STATE_COLUMNS)}, now() FROM {t} WHERE url = %s", (url,))


def test_session_is_read_only_even_for_a_superuser(cfg):
    rt = Runtime(cfg)
    s = rt.crawler_session("test")
    try:
        with pytest.raises(ReadOnlyViolation):
            s.fetchall("INSERT INTO domain_state VALUES (999, 'x.com', 0)")
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            s.conn.execute("INSERT INTO domain_state VALUES (999, 'x.com', 0)")
        s.conn.execute("SET work_mem = '64kB'")
        with pytest.raises(psycopg.errors.ConfigurationLimitExceeded):
            s.fetchall("SELECT g FROM generate_series(1, 2000000) g ORDER BY random()")
        (app,) = s.fetchone("SELECT current_setting('application_name')")
        assert app == "idxsel_r14725023_test"
    finally:
        s.close()


def test_watchdog_cancels_our_query_when_it_blocks_others(cfg, servers):
    rt = Runtime(cfg)
    ours = rt.crawler_session("export")
    wd_holder = _holder(rt, "watchdog")
    errors, locked = [], []

    def long_read():
        try:
            ours.fetchall("SELECT pg_sleep(20) FROM url_state_current_000 LIMIT 1")
        except psycopg.Error as exc:
            errors.append(exc)

    def locker():
        with psycopg.connect(servers["crawler"].dsn(db="crawlerdb")) as conn:
            started = time.monotonic()
            conn.execute("LOCK TABLE url_state_current_000 IN ACCESS EXCLUSIVE MODE")
            locked.append(time.monotonic() - started)

    with Watchdog(wd_holder, 0.2, rt.events) as wd:
        wd.set_target(ours.conn)
        reader = threading.Thread(target=long_read)
        reader.start()
        # The DDL must queue behind our running read, not the other way round.
        with psycopg.connect(servers["crawler"].dsn(db="crawlerdb"), autocommit=True) as probe:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and probe.execute(
                    "SELECT wait_event FROM pg_stat_activity WHERE pid = %s",
                    (ours.pid,)).fetchone()[0] != "PgSleep":
                time.sleep(0.05)
        lock = threading.Thread(target=locker)
        lock.start()
        reader.join(timeout=10)
        lock.join(timeout=10)
        assert wd.cancels >= 1
    assert errors and isinstance(errors[0], psycopg.errors.QueryCanceled)
    assert locked and locked[0] < 5
    ours.close()
    wd_holder.close()


def test_watchdog_reconnects_after_losing_its_connection(cfg, servers):
    rt = Runtime(cfg)
    wd_holder = _holder(rt, "watchdog")
    old_pid = wd_holder.session.pid
    with Watchdog(wd_holder, 0.1, rt.events) as wd:
        with psycopg.connect(servers["crawler"].dsn(db="crawlerdb"), autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (old_pid,))
        ours = rt.crawler_session("export")
        wd.set_target(ours.conn)  # the next check fails, then the watchdog reconnects
        deadline = time.monotonic() + 10
        while (wd_holder.session.pid == old_pid or not wd.healthy) and time.monotonic() < deadline:
            time.sleep(0.1)
        wd.set_target(None)
    assert wd_holder.session.pid != old_pid and wd.healthy
    assert '"watchdog.error"' in (rt.layout.root / "logs" / "events.jsonl").read_text()
    ours.close()
    wd_holder.close()


def test_export_resumes_after_crash(cfg, monkeypatch):
    rt = Runtime(cfg)
    guard = _holder(rt, "guard")
    real_file = export_mod._export_file
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("simulated crash")
        return real_file(*args, **kwargs)

    monkeypatch.setattr(export_mod, "_export_file", flaky)
    with pytest.raises(RuntimeError, match="simulated crash"):
        export_base(_ctx(rt, guard), "resume", SHARDS)
    monkeypatch.setattr(export_mod, "_export_file", real_file)
    resumed = export_base(_ctx(rt, guard), "resume", SHARDS)
    fresh = export_base(_ctx(rt, guard), "fresh", SHARDS)
    guard.close()
    assert resumed["rows"] == fresh["rows"] > 0
    def names(export_id):
        root = rt.layout.base_dir(export_id)
        return sorted(p.relative_to(root).as_posix() for p in root.rglob("*.parquet"))

    assert names("resume") == names("fresh")
    assert not list(rt.layout.base_dir("resume").rglob("*.tmp"))


def test_resume_keeps_the_first_watermark(cfg, servers, monkeypatch, tmp_path):
    """Fetches between a crash and the resume still reach the catch-up delta."""
    cfg = with_changes(cfg, export__history_overlap_ids=0)
    rt = Runtime(cfg)
    guard = _holder(rt, "guard")
    real_file = export_mod._export_file
    calls = {"n": 0}

    def crash_once(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated crash")
        return real_file(*args, **kwargs)

    monkeypatch.setattr(export_mod, "_export_file", crash_once)
    with pytest.raises(RuntimeError, match="simulated crash"):
        export_base(_ctx(rt, guard), "wm", SHARDS)
    monkeypatch.setattr(export_mod, "_export_file", real_file)
    first_shard = export_mod.order_shards(SHARDS, cfg)[0]
    out = rt.layout.base_dir("wm")
    assert not (out / "_shards" / f"{first_shard:03d}.json").exists(), "crash must be mid-shard"
    (written,) = list((out / f"shard={first_shard:03d}").glob("*.parquet"))
    urls = pq.read_table(written, columns=["url"])["url"].to_pylist()[:6]
    # The crawler re-fetches pages that already sit in a written file, before the resume.
    with psycopg.connect(servers["crawler"].dsn(db="crawlerdb"), autocommit=True) as conn:
        for url in urls:
            _fetch(conn, first_shard, url, 777_000)
    base = export_base(_ctx(rt, guard), "wm", SHARDS)
    export_delta(_ctx(rt, guard), "wmc", SHARDS, base["watermarks"])
    guard.close()
    got = _features(rt, "wm", ["wmc"], tmp_path / "features")
    assert all(got[u][0] == 777_000 for u in urls)
    assert got == _crawled_truth(servers)


def test_base_plus_delta_matches_truth_under_concurrent_writes(cfg, servers, tmp_path):
    """Rows move pages while the base export runs; history catch-up repairs it."""
    rt = Runtime(cfg)
    guard = _holder(rt, "guard")
    stop = threading.Event()
    touched = []

    def writer():
        rng = random.Random(3)
        with psycopg.connect(servers["crawler"].dsn(db="crawlerdb"), autocommit=True) as conn:
            while not stop.is_set():
                shard = rng.choice(SHARDS)
                t, h = f"url_state_current_{shard:03d}", f"url_state_history_{shard:03d}"
                row = conn.execute(f"SELECT url FROM {t} ORDER BY random() LIMIT 1").fetchone()
                with conn.transaction():
                    # A long title forces the new row version onto another page.
                    conn.execute(f"UPDATE {t} SET last_fetch_ok = now(), title = repeat('x', 3000),"
                                 f" inlink_count_approx = inlink_count_approx + 1000, "
                                 f"robots_bits = 1 WHERE url = %s", (row[0],))
                    conn.execute(f"INSERT INTO {h} ({', '.join(STATE_COLUMNS)}, snapshot_at) "
                                 f"SELECT {', '.join(STATE_COLUMNS)}, now() FROM {t} WHERE url = %s",
                                 (row[0],))
                touched.append(row[0])
                time.sleep(0.005)

    slow = Pacer(rt.cfg.throttle)
    slow.rate_mb_s = 0.4  # stretch the base export so the writer overlaps it
    ctx = ExportContext(rt.cfg, rt.layout, rt.events, rt.guard(guard), slow,
                        rt.crawler_session)
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        base = export_base(ctx, "conc", SHARDS)
    finally:
        stop.set()
        thread.join()
    catch_up = export_delta(_ctx(rt, guard), "concc", SHARDS, base["watermarks"])
    guard.close()
    assert len(touched) > 20
    assert catch_up["rows"] > 0
    assert _features(rt, "conc", ["concc"], tmp_path / "features") == _crawled_truth(servers)


def test_swap_never_breaks_a_concurrent_metric_reader(cfg, fresh_selectdb, tmp_path):
    rt = Runtime(cfg)
    admin = connect(require_env(cfg.selectdb.admin_dsn_env), app_name="t", session={},
                    connect_timeout_s=5)
    init_selectdb(admin, cfg.selectdb.reader_role, require_env(cfg.selectdb.reader_password_env))
    con = open_duckdb(cfg, tmp_path / "duck")
    selected = tmp_path / "selected.parquet"
    run_ids = []

    def load(i):
        con.execute(
            f"COPY (SELECT 'https://swap.test/' || {i} || '/' || g AS url, g::DOUBLE AS score, "
            f"NULL::TIMESTAMPTZ AS first_seen FROM range({cfg.run.k}) t(g)) TO '{selected}' "
            f"(FORMAT parquet)")
        meta = {"run_label": f"swap-{i}", "cutoff_datetime": datetime.now(UTC),
                "model_path": "rule:test", "config_sha256": "x", "code_version": "t"}
        run_ids.append(load_selection(cfg, admin, con, selected, tmp_path, meta, rt.events))
        return run_ids[-1]

    stop, errors, sizes = threading.Event(), [], []
    probe = [f"https://swap.test/{i}/{g}" for i in range(2) for g in range(0, cfg.run.k, 37)]

    def metric_reader():
        with psycopg.connect(require_env(cfg.selectdb.reader_dsn_env)) as conn:
            while not stop.is_set():
                try:
                    rows = conn.execute("SELECT url FROM public.selected_urls_current "
                                        "WHERE url = ANY(%s)", (probe,)).fetchall()
                    conn.commit()
                    sizes.append(len(rows))
                except psycopg.Error as exc:
                    errors.append(exc)
                    conn.rollback()

    reader = threading.Thread(target=metric_reader)
    reader.start()
    try:
        for i in range(2):
            assert swap(cfg, admin, load(i), rt.events)["swapped"]
            time.sleep(0.3)
    finally:
        stop.set()
        reader.join()
    assert not errors
    assert len(sizes) > 10
    assert set(sizes) <= {0, len(probe) // 2}
    assert swap(cfg, admin, run_ids[1], rt.events) == {"swapped": True, "already_live": True}

    # A reader holding a long transaction: the swap gives up cleanly, old list stays live.
    load(2)
    with psycopg.connect(require_env(cfg.selectdb.reader_dsn_env)) as blocker:
        blocker.execute("SELECT count(*) FROM public.selected_urls_current")
        result = swap(cfg, admin, run_ids[2], rt.events, sleep=lambda s: None)
    assert result == {"swapped": False, "reason": "lock timeout", "retry": True}
    (live,) = admin.execute("SELECT id FROM model_runs WHERE status = 'live'").fetchone()
    assert live == run_ids[1]

    # The next load supersedes the unswapped run 2: its table is dropped.
    load(3)
    status = dict(admin.execute("SELECT id, status FROM model_runs").fetchall())
    assert status[run_ids[2]] == "abandoned" and status[run_ids[3]] == "validated"
    (left,) = admin.execute("SELECT count(*) FROM pg_tables WHERE tablename ~ "
                            "'^selected_urls_r[0-9]+$'").fetchone()
    assert left == 1
    assert swap(cfg, admin, run_ids[2], rt.events)["retry"] is False
    assert rollback(cfg, admin, rt.events)["live_run"] == run_ids[0]
    (live,) = admin.execute("SELECT id FROM model_runs WHERE status = 'live'").fetchone()
    assert live == run_ids[0]
    admin.close()
