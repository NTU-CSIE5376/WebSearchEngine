from __future__ import annotations

import dataclasses
import urllib.error
from datetime import UTC, date, datetime, timedelta

import psycopg
import pyarrow.parquet as pq
import pytest
from conftest import BASE, replace
from opensearch_fakes import OpenSearchStub

from isel import rankjob, textindex
from isel.cli import main as cli_main
from isel.config import load_config
from isel.io import LockBusy, atomic_write_json, utc_id
from isel.rankjob import (
    Batch,
    CoverageRow,
    GoldenRow,
    LiveRun,
    Ports,
    RankRecord,
    Status,
    count_ranks,
    dry_run,
    pick_golden_day,
    shadow_counts,
    tick,
)
from isel.urlnorm import canonicalize, md5_number_lower

DECEMBER = BASE.parent / "december-2026.toml"
G = date(2026, 11, 16)
BUILD = datetime(2026, 11, 15, 17, 30, tzinfo=UTC)     # G-1 at rank.build_start
MEASURE = datetime(2026, 11, 16, 20, 0, tzinfo=UTC)    # G's guard.month_window ends
LIVE = LiveRun("run-1114", datetime(2026, 11, 14, 4, 0, tzinfo=UTC))
TOKEN = "AfmBOoqfCkB7Gr36L8FzV4ncwpCatQZRYNOjavRk_Qp1ZbjcmArzMmZw"


def _row(i, keyword, tags, url, indexed, shard):
    return GoldenRow(i, 100 + i, keyword, tuple(tags), url, indexed, shard)


# Batch 22, measured on G. head: a.com/x (shard 5, team a) twice under two keywords,
# b.com/y (team b), a srsltid URL whose index doc is the stripped form (team b), and
# d.com/w with no shard, found by the search but not indexed. random: e.com/q (team a).
ROWS = [
    _row(1, "alpha", ["head"], "https://a.com/x", True, 5),
    _row(2, "alpha", ["head"], "https://b.com/y", True, 200),
    _row(3, "alpha", ["head"], f"https://c.com/z?srsltid={TOKEN}", True, 130),
    _row(4, "alpha", ["head"], "https://d.com/w", False, -1),
    _row(5, "beta", ["head"], "https://a.com/x#top", True, 5),
    _row(6, "gamma", ["random"], "https://e.com/q", True, 10),
]
INDEXED_URLS = ["https://a.com/x", "https://c.com/z", "https://d.com/w", "https://e.com/q",
                "https://f.com/other"]
# The live list as selectdb holds it: what Metric reads as indexed (row 4 is not on it).
LISTED = {"https://a.com/x", "https://b.com/y", "https://c.com/z", "https://e.com/q"}
TOP = {"alpha": ["https://f.com/other", "https://c.com/z", "https://a.com/x",
                 "https://d.com/w"],
       "beta": ["https://f.com/other"],
       "gamma": ["https://e.com/q"]}
# (tag, team) -> (total, indexed_num) as Metric measured them.
MEASURED = {("head", "total"): (4, 3), ("head", "a"): (1, 1), ("head", "b"): (2, 2),
            ("random", "total"): (1, 1), ("random", "a"): (1, 1), ("random", "b"): (0, 0)}
# The head measurement ran past midnight; random finished on G.
STAT_DATE = {"head": G + timedelta(days=1), "random": G}
RANKED = {("head", "total"): 2, ("head", "a"): 1, ("head", "b"): 1,
          ("random", "total"): 1, ("random", "a"): 1, ("random", "b"): 0}
SHADOW = {"head": {"pairs_found": 3, "pairs_total": 5, "total": 4, "indexed_num": 3,
                   "indexed_rate": 0.75, "ranked_num": 2, "ranked_rate": 0.5},
          "random": {"pairs_found": 1, "pairs_total": 1, "total": 1, "indexed_num": 1,
                     "indexed_rate": 1.0, "ranked_num": 1, "ranked_rate": 1.0}}


def _doc_id(url):
    return str(md5_number_lower(canonicalize(url)))


class FakeMetricDb:
    """Batches, rows and coverage tables in memory; `write` applies all of it or nothing."""

    def __init__(self):
        self.batches = [Batch(21, datetime(2026, 11, 1, 12, tzinfo=UTC)),
                        Batch(22, datetime(2026, 11, 16, 12, tzinfo=UTC))]
        self.rows = {22: list(ROWS)}
        self.coverage = {}
        for (tag, team), (total, indexed) in MEASURED.items():
            table = f"metric_{tag}set_{team}"
            self.coverage[table] = {
                date(2026, 11, 2): {"total": 9, "indexed_num": 1, "ranked_num": None},
                # A manual re-measure on 11-12, before G.
                date(2026, 11, 12): {"total": total, "indexed_num": 0, "ranked_num": None},
                STAT_DATE[tag]: {"total": total, "indexed_num": indexed, "ranked_num": None,
                                 "ranked_rate": None},
            }
        self.is_ranked = {r.id: None for r in ROWS}
        self.reads = 0
        self.writes = 0
        self.crash_after_commit = False
        self.write_error = None

    def batch_before(self, until):
        self.reads += 1
        older = [b for b in self.batches if b.created_at < until]
        return max(older, key=lambda b: b.created_at) if older else None

    def batch_rows(self, batch_id):
        self.reads += 1
        return list(self.rows.get(batch_id, []))

    def coverage_rows(self, table, first, last):
        self.reads += 1
        return [CoverageRow(d, r["total"], r["indexed_num"])
                for d, r in sorted(self.coverage[table].items()) if first <= d <= last]

    def write(self, snapshot, ranked, rows):
        if self.write_error is not None:
            error, self.write_error = self.write_error, None
            raise error
        locked = rankjob.Snapshot(
            snapshot.batch_id, tuple(sorted(self.rows.get(snapshot.batch_id, []),
                                            key=lambda r: r.id)),
            {t: tuple(self.coverage_rows(t, snapshot.first, snapshot.last))
             for t in snapshot.coverage}, snapshot.first, snapshot.last)
        changes = rankjob.snapshot_changes(snapshot, locked)
        if changes:
            raise rankjob.SnapshotStale("; ".join(changes))
        assert set(ranked) <= set(self.is_ranked)
        for w in rows:
            assert w.stat_date in self.coverage[w.table]
        self.is_ranked.update(ranked)
        for w in rows:
            self.coverage[w.table][w.stat_date].update(ranked_num=w.ranked_num,
                                                       ranked_rate=w.ranked_rate)
        self.writes += 1
        if self.crash_after_commit:
            self.crash_after_commit = False
            raise ConnectionError("lost the reply after COMMIT")


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


@pytest.fixture
def stub():
    s = OpenSearchStub()
    s.hits = {kw: [_doc_id(u) for u in urls] for kw, urls in TOP.items()}
    yield s
    s.close()


class World:
    """The rank job against fake metricdb and selectdb, and the OpenSearch stub."""

    def __init__(self, cfg, stub):
        self.cfg = cfg
        self.stub = stub
        self.metric = FakeMetricDb()
        self.live = LIVE
        self.listed = set(LISTED)
        self.builds = []
        self.build_error = None
        self.on_search = None      # called with the number of searches so far
        self.search_count = 0
        self.client = textindex.OpenSearch(stub.url, 5.0, 0, 0.0)

    def put_index(self, started_at, prior_run, urls=INDEXED_URLS):
        """A build, as `textindex.build` leaves it: aliased, the older ones kept."""
        alias = self.cfg.text_index.alias
        name = f"{alias}-{utc_id(started_at).lower()}"
        docs = {_doc_id(u): {"url": canonicalize(u)} for u in urls}
        self.stub.indices[name] = {"docs": docs, "body": {}, "settings": {}}
        self.stub.aliases[alias] = {name}
        atomic_write_json(self.cfg.work_dir / "text_index" / "build" / f"{name}.json",
                          {"index": name, "prior_run": prior_run})
        return name

    def ports(self, clock):
        def build():
            if self.build_error is not None:
                raise self.build_error
            self.builds.append(self.put_index(clock(), self.live and self.live.label))

        def search(index, kw):
            self.search_count += 1
            if self.on_search is not None:
                self.on_search(self.search_count)
            return textindex.search(self.client, self.cfg.text_index, kw, index)[0]

        return Ports(metric=self.metric, live_run=lambda: self.live,
                     builds=lambda: rankjob.index_builds(self.cfg, self.client),
                     build=build,
                     search=search,
                     listed=lambda urls: {u for u in urls if u in self.listed},
                     pin=lambda index, holder: textindex.pin(self.cfg, self.client, index,
                                                             holder))

    def tick(self, now, golden_day=None):
        clock = Clock(now)
        return tick(self.cfg, self.ports(clock), clock, golden_day)

    def record(self, golden_day=G):
        return rankjob.read_record(self.cfg, golden_day)

    def ranked(self):
        return {(tag, team): self.metric.coverage[f"metric_{tag}set_{team}"][STAT_DATE[tag]]
                ["ranked_num"] for tag, team in MEASURED}

    def searched(self):
        return [p.split("/")[1] for p in self.stub.paths("POST") if "_search" in p]

    def pins(self):
        return textindex.pins(self.cfg)


@pytest.fixture
def world(base_cfg, tmp_path, stub):
    cfg = replace(base_cfg, "run", work_dir=str(tmp_path))
    cfg = replace(cfg, "text_index", url=stub.url)
    return World(cfg, stub)


def test_g_is_picked_from_its_build_start_on(base_cfg):
    assert rankjob.build_from(G, base_cfg.rank) == BUILD
    assert rankjob.measure_from(G, base_cfg) == MEASURE
    assert pick_golden_day(BUILD - timedelta(minutes=1), base_cfg) == date(2026, 11, 1)
    assert pick_golden_day(BUILD, base_cfg) == G
    assert pick_golden_day(datetime(2026, 11, 30, 17, 29, tzinfo=UTC), base_cfg) == G


def test_december_ranks_dec_8_without_a_refresh_cycle(base_cfg):
    cfg = load_config([BASE, DECEMBER])
    dec8 = date(2026, 12, 8)
    assert pick_golden_day(datetime(2026, 12, 7, 17, 30, tzinfo=UTC), cfg) == dec8
    assert pick_golden_day(datetime(2026, 12, 7, 17, 29, tzinfo=UTC), cfg) == date(2026, 12, 1)
    assert rankjob.measure_from(dec8, cfg) == datetime(2026, 12, 8, 16, tzinfo=UTC)
    assert rankjob.next_golden_day(date(2026, 12, 1), cfg) == dec8
    assert rankjob.next_golden_day(date(2026, 12, 15), cfg) == date(2027, 1, 1)


def test_g_minus_1_builds_without_reading_metricdb(world):
    out = world.tick(BUILD)
    assert out["decision"] == "idle" and "is built" in out["why"]
    assert len(world.builds) == 1 and world.metric.reads == 0
    assert world.record().status is Status.PENDING
    # Built: the next tick neither builds nor reads metricdb.
    world.tick(BUILD + timedelta(minutes=10))
    assert len(world.builds) == 1 and world.metric.reads == 0


@pytest.mark.parametrize("index, builds", [
    ("fresh", 0),          # the live list, built in G-1's window
    ("other_run", 1),      # another list
    ("before_window", 1),  # the live list, built before G-1's window (Dec 8 after Dec 1)
    ("none", 1),
])
def test_builds_unless_the_newest_build_is_the_live_list_from_this_window(world, index, builds):
    if index == "fresh":
        world.put_index(BUILD + timedelta(minutes=5), LIVE.label)
    elif index == "other_run":
        world.put_index(BUILD + timedelta(minutes=5), "run-1030")
    elif index == "before_window":
        world.put_index(BUILD - timedelta(days=7), LIVE.label)
    world.tick(BUILD + timedelta(minutes=20))
    assert len(world.builds) == builds


def test_no_build_starts_after_build_latest(world):
    out = world.tick(datetime(2026, 11, 15, 19, 1, tzinfo=UTC))
    assert out["decision"] == "idle" and "no build may start" in out["why"]
    assert world.builds == []


def test_g_waits_for_its_guard_window_without_reading_metricdb(world):
    world.tick(BUILD)
    out = world.tick(MEASURE - timedelta(minutes=1))
    assert out["decision"] == "idle" and "measures from" in out["why"]
    assert world.metric.reads == 0


def test_g_shadows_then_writes_only_the_rank_fields(world):
    world.tick(BUILD)
    out = world.tick(MEASURE)
    assert out["decision"] == "measurewrite"
    assert world.ranked() == RANKED
    head = world.metric.coverage["metric_headset_total"]
    assert head[STAT_DATE["head"]]["ranked_rate"] == pytest.approx(2 / 4)
    assert head[STAT_DATE["head"]]["indexed_num"] == 3
    assert head[date(2026, 11, 12)]["ranked_num"] is None
    assert world.metric.coverage["metric_randomset_b"][G]["ranked_rate"] == 0.0
    # Row 4 is in the top 10 but not indexed; row 5's keyword does not find a.com/x.
    assert world.metric.is_ranked == {1: True, 2: False, 3: True, 4: False, 5: False,
                                      6: True}
    record = world.record()
    assert record.status is Status.DONE and record.attempts == 0
    assert (record.batch_id, record.live_run, record.index) == (22, LIVE.label,
                                                                world.builds[0])
    assert record.shadow == SHADOW and record.shadow_at == MEASURE
    assert record.tags["head"]["pairs_found"] == 3
    assert record.tags["head"]["pairs_total"] == 5
    assert record.tags["head"]["teams"]["total"]["stat_date"] == "2026-11-17"
    audit = pq.read_table(world.cfg.work_dir / "rank" / "2026-11-16.ranked.parquet")
    assert dict(zip(audit["metric_url_id"].to_pylist(), audit["hit"].to_pylist(),
                    strict=True)) == {1: True, 2: False, 3: True, 4: True, 5: False, 6: True}
    # Each keyword once for the shadow and once for the write, on the pinned index.
    assert world.searched() == [world.builds[0]] * (2 * len(TOP))


def test_the_shadow_is_written_before_metric_measures(world):
    saved = world.metric.coverage
    world.metric.coverage = {t: {d: r for d, r in rows.items() if d < G}
                             for t, rows in saved.items()}
    world.tick(BUILD)
    out = world.tick(MEASURE)
    assert out["decision"] == "idle" and "waiting for Metric's coverage rows" in out["why"]
    record = world.record()
    assert record.status is Status.PENDING and record.shadow == SHADOW
    assert world.metric.writes == 0
    # Metric's rows appear later; a row dated before the next golden day is G's.
    world.metric.coverage = saved
    world.tick(MEASURE + timedelta(days=3))
    assert world.record().status is Status.DONE and world.ranked() == RANKED
    assert world.record().shadow_at == MEASURE


def test_g_keeps_its_index_after_the_next_build(world):
    # Built on G-1, then rebuilt from the same list after G; G still searches its own.
    first = world.put_index(BUILD + timedelta(minutes=5), LIVE.label)
    second = world.put_index(datetime(2026, 11, 20, tzinfo=UTC), LIVE.label)
    out = world.tick(datetime(2026, 11, 30, 17, 0, tzinfo=UTC))
    assert out["decision"] == "measurewrite"
    assert world.record().index == first
    assert set(world.searched()) == {first} and second != first


def test_a_head_only_batch_ranks_head(world):
    world.metric.rows[22] = [r for r in ROWS if "random" not in r.tags]
    for team in ("total", "a", "b"):
        world.metric.coverage[f"metric_randomset_{team}"].pop(G)
    world.tick(BUILD)
    world.tick(MEASURE)
    record = world.record()
    assert record.status is Status.DONE
    assert set(record.tags) == set(record.shadow) == {"head"}
    assert world.metric.coverage["metric_headset_total"][STAT_DATE["head"]]["ranked_num"] == 2


def test_a_done_record_idles(world):
    world.tick(BUILD)
    world.tick(MEASURE)
    writes = world.metric.writes
    out = world.tick(MEASURE + timedelta(hours=1))
    assert out["decision"] == "idle"
    assert world.metric.writes == writes


def _no_batch(w):
    w.metric.batches = []


def _old_batch(w):
    w.metric.batches = w.metric.batches[:1]
    w.metric.rows[21] = list(ROWS)


def _no_golden_rows(w):
    w.metric.rows[22] = [dataclasses.replace(r, tags=("trending",)) for r in ROWS]


def _late_activation(w):
    w.live = dataclasses.replace(LIVE, activated_at=datetime(2026, 11, 16, 11, 30, tzinfo=UTC))


def _two_rows(w):
    w.metric.coverage["metric_headset_a"][date(2026, 11, 20)] = {"total": 1, "indexed_num": 1}


def _remeasured(w):
    # A later run rewrote is_indexed of one URL after the row was written.
    w.metric.rows[22] = [dataclasses.replace(r, is_indexed=False) if r.id == 2 else r
                         for r in ROWS]


def _wrong_total(w):
    w.metric.coverage["metric_headset_b"][STAT_DATE["head"]]["total"] = 3


def _no_live(w):
    w.live = None


def _no_index(w):
    pass    # no build at all: a scheduled tick does not build on G


@pytest.mark.parametrize("break_it, reason", [
    (_no_batch, "no metric batch was created before 2026-11-17"),
    (_old_batch, "the newest batch 21 was created on 2026-11-01, not on 2026-11-16"),
    (_no_golden_rows, "batch 22 has no head or random rows"),
    (_late_activation, "it is not the list measured on G"),
    (_two_rows, "metric_headset_a has 2 rows dated 2026-11-16 or later"),
    (_remeasured, "metric_headset_total 2026-11-17: total 4 and indexed_num 3"),
    (_wrong_total, "metric_headset_b 2026-11-17: total 3"),
    (_no_live, "selectdb has no live run"),
    (_no_index, "no index of run-1114 was built before 2026-11-16"),
])
def test_refusals_write_nothing_and_stay_refused(world, break_it, reason):
    if break_it is not _no_index:
        world.put_index(BUILD + timedelta(minutes=5), LIVE.label)
    break_it(world)
    out = world.tick(MEASURE)
    assert out["decision"] == "refuse"
    record = world.record()
    assert record.status is Status.REFUSED and reason in record.reason
    assert world.metric.writes == 0 and world.builds == [] and world.pins() == {}
    assert not (world.cfg.work_dir / "rank" / "2026-11-16.ranked.parquet").exists()
    assert world.tick(MEASURE + timedelta(hours=1))["decision"] == "idle"


def _built(world):
    world.put_index(BUILD + timedelta(minutes=5), LIVE.label)


@pytest.mark.parametrize("off_by", [1, -1])
def test_a_count_off_by_one_refuses(world, off_by):
    # Metric counts the rows it measured: any difference means the rows changed after.
    _built(world)
    world.metric.coverage["metric_headset_a"][STAT_DATE["head"]]["total"] += off_by
    world.tick(MEASURE)
    record = world.record()
    assert record.status is Status.REFUSED and "metric_headset_a" in record.reason
    assert world.metric.writes == 0


def test_named_golden_day_builds_at_any_time_and_past_a_later_activation(world):
    # A G whose build was missed: by hand, the job builds now and pins that build.
    _late_activation(world)
    out = world.tick(MEASURE + timedelta(days=2), golden_day=G)
    assert out["decision"] == "measurewrite"
    assert world.ranked() == RANKED
    record = world.record()
    assert record.index == world.builds[0] and record.shadow == SHADOW


def test_named_golden_day_still_refuses_mismatched_rows(world):
    _remeasured(world)
    world.tick(MEASURE, golden_day=G)
    assert world.record().status is Status.REFUSED
    assert world.metric.writes == 0 and world.builds == []


def test_dry_run_builds_and_writes_nothing(world):
    index = world.put_index(BUILD + timedelta(minutes=5), LIVE.label)
    _late_activation(world)
    out = dry_run(world.cfg, world.ports(Clock(MEASURE)), Clock(MEASURE))
    assert out["would"] == "refuse"
    assert any("not the list measured on G" in p for p in out["problems"])
    assert (out["index"], out["index_fresh"], out["missing_rows"]) == (index, True, [])
    assert out["measured"]["random"]["teams"]["a"]["ranked_num"] == 1
    assert out["shadow"] == SHADOW
    assert world.builds == [] and world.metric.writes == 0
    assert not (world.cfg.work_dir / "rank").exists()


def test_dry_run_without_an_index_reports_instead_of_searching(world):
    # Before the first build the alias names no index, so a search would 404.
    out = dry_run(world.cfg, world.ports(Clock(BUILD)), Clock(BUILD))
    assert (out["would"], out["index"], out["index_fresh"]) == ("build", None, False)
    assert "measured" not in out and "shadow" not in out
    out = dry_run(world.cfg, world.ports(Clock(MEASURE)), Clock(MEASURE))
    assert out["would"] == "refuse"
    assert not world.searched()


def test_rerun_after_a_crash_past_commit_converges(world):
    world.tick(BUILD)
    world.metric.crash_after_commit = True
    first = world.tick(MEASURE)
    assert first["decision"] == "idle" and world.record().status is Status.PENDING
    assert world.record().attempts == 1
    assert world.ranked() == RANKED
    world.tick(MEASURE + timedelta(minutes=10))
    assert world.record().status is Status.DONE
    assert world.ranked() == RANKED
    assert len(world.builds) == 1
    assert world.metric.writes == 2


def test_attempts_end_in_failed(world):
    def down(*args):
        raise ConnectionError("metricdb down")

    _built(world)
    world.metric.batch_before = down
    for n in range(world.cfg.rank.max_attempts):
        world.tick(MEASURE + timedelta(minutes=10 * n))
    record = world.record()
    assert record.status is Status.FAILED
    assert record.attempts == world.cfg.rank.max_attempts
    assert "metricdb down" in record.reason
    assert world.tick(MEASURE + timedelta(hours=2))["decision"] == "idle"


@pytest.mark.parametrize("outage", [
    psycopg.errors.AdminShutdown("terminating connection due to administrator command"),
    textindex.TextIndexError("GET /_alias/isel-text: ConnectionRefusedError()"),
    LockBusy("another process holds text_index/build.lock"),
])
def test_an_outage_defers_without_using_an_attempt(world, outage):
    if isinstance(outage, textindex.TextIndexError):
        outage.__cause__ = ConnectionRefusedError()

    def down(*args):
        raise outage

    _built(world)
    world.metric.batch_before = down
    for n in range(world.cfg.rank.max_attempts + 1):
        out = world.tick(MEASURE + timedelta(minutes=10 * n))
        assert out["decision"] == "idle" and out["why"].startswith("deferred")
    record = world.record()
    assert record.status is Status.PENDING and record.attempts == 0
    del world.metric.batch_before
    world.tick(MEASURE + timedelta(hours=1))
    assert world.record().status is Status.DONE
    assert world.ranked() == RANKED


def test_a_failed_build_check_uses_an_attempt(world):
    world.build_error = textindex.TextIndexError("isel-text-x holds 9 docs, expected 10")
    world.tick(BUILD)
    assert world.record().attempts == 1


def test_an_http_error_reply_uses_an_attempt(world):
    # urllib's HTTPError is an OSError; a reply the node repeats is not an outage.
    error = textindex.TextIndexError("PUT /isel-text-x: HTTP 403 cluster_block_exception", 403)
    error.__cause__ = urllib.error.HTTPError("http://127.0.0.1:9200", 403, "Forbidden",
                                             None, None)
    world.build_error = error
    world.tick(BUILD)
    assert world.record().attempts == 1


def test_rank_refuses_a_config_without_the_live_prior(tmp_path):
    path = tmp_path / "overlay.toml"
    path.write_text(f'[run]\nwork_dir = "{tmp_path / "work"}"\n')
    with pytest.raises(SystemExit, match="prior_file"):
        cli_main(["--config", str(BASE), "--config", str(path), "rank", "--once"])


def test_ranked_urls_are_a_subset_of_indexed_ones(base_cfg):
    # Everything is in every top 10; only indexed URLs may count as ranked.
    every = [_doc_id(r.url) for r in ROWS] + [_doc_id("https://c.com/z")]
    ranking = count_ranks(ROWS, lambda kw: every, ["head", "random"], base_cfg.rank)
    for tag, tc in ranking.tags.items():
        for team, c in tc.teams.items():
            assert c.ranked_num == c.indexed_num == MEASURED[(tag, team)][1]


def test_the_shadow_reads_indexed_from_the_list_not_from_metric(base_cfg):
    # Metric has not measured yet: every is_indexed is False, every shard -1.
    unmeasured = [dataclasses.replace(r, is_indexed=False, shard_id=-1) for r in ROWS]
    every = [_doc_id(r.url) for r in ROWS] + [_doc_id("https://c.com/z")]
    out = shadow_counts(unmeasured, LISTED, lambda kw: every, ["head", "random"],
                        base_cfg.rank)
    assert (out["head"]["indexed_num"], out["head"]["ranked_num"]) == (3, 3)
    assert out["random"]["indexed_rate"] == 1.0


def test_record_round_trips(world):
    world.tick(BUILD)
    world.tick(MEASURE)
    record = world.record()
    assert record.shadow and RankRecord.from_json(record.to_json()) == record


def _replace_url(w, row_id, url):
    w.metric.rows[22] = [dataclasses.replace(r, url=url) if r.id == row_id else r
                         for r in w.metric.rows[22]]


def test_g_is_pinned_from_midnight_without_reading_metricdb(world):
    world.tick(BUILD)
    out = world.tick(datetime(2026, 11, 16, 0, 10, tzinfo=UTC))
    assert out["decision"] == "idle" and "measures from" in out["why"]
    assert set(world.pins()) == {world.builds[0]} and world.metric.reads == 0
    assert world.pins()[world.builds[0]]["golden_day"] == G.isoformat()
    assert world.record().index == world.builds[0]
    # Done: the pin goes, so the next G's build may delete the index.
    world.tick(MEASURE)
    assert world.record().status is Status.DONE and world.pins() == {}


def test_a_refusal_drops_the_pin(world):
    world.tick(BUILD)
    world.tick(datetime(2026, 11, 16, 0, 10, tzinfo=UTC))
    _wrong_total(world)
    world.tick(MEASURE)
    assert world.record().status is Status.REFUSED and world.pins() == {}


def test_an_alias_switch_mid_run_leaves_every_search_on_the_pin(world):
    # A manual text-index-build moves the alias while G counts: G keeps its index.
    world.tick(BUILD)
    first = world.builds[0]
    later = []

    def switch(n):
        if n == 1:
            later.append(world.put_index(MEASURE + timedelta(seconds=1), LIVE.label))

    world.on_search = switch
    world.tick(MEASURE)
    assert world.stub.aliases["isel-text"] == {later[0]}
    assert set(world.searched()) == {first}
    record = world.record()
    assert record.status is Status.DONE and record.index == first
    assert world.ranked() == RANKED


def test_a_pinned_index_that_goes_defers_and_never_falls_back(world):
    # An older build of the same list exists; G must not quietly rank on it.
    world.put_index(BUILD - timedelta(days=7), LIVE.label)
    world.tick(BUILD)
    pin = world.builds[0]

    def drop(n):
        if n == 1:
            world.stub.indices.pop(pin)

    world.on_search = drop
    out = world.tick(MEASURE)
    assert out["decision"] == "idle" and out["why"].startswith("deferred")
    assert "answered 404" in out["why"]
    record = world.record()
    assert record.status is Status.PENDING and record.attempts == 0
    out = world.tick(MEASURE + timedelta(minutes=10))
    assert out["decision"] == "idle" and f"pinned index {pin} is missing" in out["why"]
    assert set(world.searched()) == {pin} and world.metric.writes == 0
    assert world.record().attempts == 0


def test_a_url_replaced_after_the_snapshot_refuses(world):
    # The first write is lost after the snapshot; then a rerun replaces b.com/y with a
    # listed URL of the same team: the counts stay, the rows do not.
    world.tick(BUILD)
    world.metric.write_error = RuntimeError("write lost")
    world.tick(MEASURE)
    record = world.record()
    assert record.status is Status.PENDING and record.attempts == 1
    assert rankjob.snapshot_path(world.cfg, G).exists()
    world.listed.add("https://b.com/y2")
    _replace_url(world, 2, "https://b.com/y2")
    world.tick(MEASURE + timedelta(minutes=10))
    record = world.record()
    assert record.status is Status.REFUSED
    assert "metricdb changed since the snapshot: 1 rows changed" in record.reason
    assert world.metric.writes == 0


def test_a_url_replaced_during_the_searches_is_caught_by_the_write(world):
    # The shadow searches 3 keywords, then the write's searches begin: a rerun lands.
    world.tick(BUILD)
    world.listed.add("https://b.com/y2")

    def rerun(n):
        if n == len(TOP) + 1:
            _replace_url(world, 2, "https://b.com/y2")

    world.on_search = rerun
    world.tick(MEASURE)
    record = world.record()
    assert record.status is Status.PENDING and record.attempts == 1
    assert "SnapshotStale" in record.reason and world.metric.writes == 0
    world.on_search = None
    world.tick(MEASURE + timedelta(minutes=10))
    assert world.record().status is Status.REFUSED and world.metric.writes == 0


def test_rows_inserted_after_the_measurement_wait(world):
    # A rerun inserted row 6 again; Metric has not measured it yet.
    world.tick(BUILD)
    world.metric.rows[22] = [dataclasses.replace(r, is_indexed=False, shard_id=None)
                             if r.id == 6 else r for r in ROWS]
    out = world.tick(MEASURE)
    assert out["decision"] == "idle" and "waiting for Metric to measure 1 rows" in out["why"]
    assert world.record().status is Status.PENDING and world.metric.writes == 0
    world.metric.rows[22] = list(ROWS)
    world.tick(MEASURE + timedelta(minutes=10))
    assert world.record().status is Status.DONE and world.ranked() == RANKED


def test_rows_measured_against_another_list_refuse(world):
    # e.com/q is indexed in Metric, but the live list does not hold it.
    world.tick(BUILD)
    world.listed.discard("https://e.com/q")
    world.tick(MEASURE)
    record = world.record()
    assert record.status is Status.REFUSED
    assert "1 rows' is_indexed disagree with the list of run-1114 (first id 6)" in record.reason
    assert world.metric.writes == 0


def test_the_snapshot_is_what_was_ranked(world):
    world.tick(BUILD)
    world.tick(MEASURE)
    record = world.record()
    saved = rankjob.read_snapshot(world.cfg, G)
    assert saved.rows == tuple(ROWS) and saved.batch_id == 22
    assert saved.coverage["metric_headset_total"] == (
        CoverageRow(STAT_DATE["head"], 4, 3),)
    assert record.snapshot_sha256 == saved.sha256()
    assert rankjob.Snapshot.from_json(saved.to_json()) == saved


def test_dry_run_reports_rows_that_disagree_with_the_list(world):
    world.put_index(BUILD + timedelta(minutes=5), LIVE.label)
    world.listed.discard("https://e.com/q")
    out = dry_run(world.cfg, world.ports(Clock(MEASURE)), Clock(MEASURE))
    assert out["would"] == "refuse" and out["indexed_mismatch_ids"] == [6]
    assert any("disagree with the live list" in p for p in out["problems"])
    assert world.pins() == {} and world.metric.writes == 0
