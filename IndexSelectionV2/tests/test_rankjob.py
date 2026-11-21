from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime, timedelta

import psycopg
import pyarrow.parquet as pq
import pytest
from conftest import replace
from opensearch_fakes import OpenSearchStub

from isel import rankjob, textindex
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
    tick,
)
from isel.urlnorm import canonicalize, md5_number_lower

G = date(2026, 11, 16)
DUE = datetime(2026, 11, 19, 0, 15, tzinfo=UTC)
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
                # A manual re-measure on 11-12, outside [G, G+1].
                date(2026, 11, 12): {"total": total, "indexed_num": 0, "ranked_num": None},
                STAT_DATE[tag]: {"total": total, "indexed_num": indexed, "ranked_num": None,
                                 "ranked_rate": None},
            }
        self.is_ranked = {r.id: None for r in ROWS}
        self.reads = 0
        self.writes = 0
        self.crash_after_commit = False

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

    def write(self, ranked, rows):
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
        self.builds = []
        self.build_error = None
        self.client = textindex.OpenSearch(stub.url, 5.0, 0, 0.0)

    def put_index(self, started_at, prior_run):
        """An index on the alias, as `textindex.build` leaves it."""
        alias = self.cfg.text_index.alias
        name = f"{alias}-{utc_id(started_at).lower()}"
        docs = {_doc_id(u): {"url": canonicalize(u)} for u in INDEXED_URLS}
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

        return Ports(metric=self.metric, live_run=lambda: self.live,
                     index_build=lambda: rankjob.index_build(self.cfg, self.client),
                     build=build,
                     search=lambda kw: textindex.search(self.client, self.cfg.text_index,
                                                        kw)[0])

    def tick(self, now, golden_day=None):
        clock = Clock(now)
        return tick(self.cfg, self.ports(clock), clock, golden_day)

    def record(self):
        return rankjob.read_record(self.cfg, G)

    def ranked(self):
        return {(tag, team): self.metric.coverage[f"metric_{tag}set_{team}"][STAT_DATE[tag]]
                ["ranked_num"] for tag, team in MEASURED}


@pytest.fixture
def world(base_cfg, tmp_path, stub):
    cfg = replace(base_cfg, "run", work_dir=str(tmp_path))
    cfg = replace(cfg, "text_index", url=stub.url)
    return World(cfg, stub)


def test_golden_day_due_three_days_later_at_start_time(base_cfg):
    assert rankjob.due_at(G, base_cfg.rank) == DUE
    assert pick_golden_day(DUE - timedelta(minutes=1), base_cfg) == date(2026, 11, 1)
    assert pick_golden_day(DUE, base_cfg) == G
    assert pick_golden_day(datetime(2026, 12, 3, tzinfo=UTC), base_cfg) == G


def test_g_starts_at_its_due_time_and_a_final_record_reads_nothing(world):
    atomic_write_json(rankjob.record_path(world.cfg, date(2026, 11, 1)),
                      RankRecord(date(2026, 11, 1), Status.DONE).to_json())
    out = world.tick(DUE - timedelta(seconds=1))
    assert (out["golden_day"], out["decision"]) == ("2026-11-01", "idle")
    assert world.metric.reads == 0 and world.builds == []
    assert world.tick(DUE)["golden_day"] == "2026-11-16"
    assert world.record().status is Status.DONE


def test_due_tick_builds_then_writes_only_the_rank_fields(world):
    world.put_index(DUE - timedelta(days=1), LIVE.label)   # the live list, but too early
    out = world.tick(DUE + timedelta(minutes=1))
    assert out["decision"] == "measurewrite"
    assert len(world.builds) == 1
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
    assert record.tags["head"]["pairs_found"] == 3
    assert record.tags["head"]["pairs_total"] == 5
    assert record.tags["head"]["teams"]["total"]["stat_date"] == "2026-11-17"
    audit = pq.read_table(world.cfg.work_dir / "rank" / "2026-11-16.ranked.parquet")
    assert dict(zip(audit["metric_url_id"].to_pylist(), audit["hit"].to_pylist(),
                    strict=True)) == {1: True, 2: False, 3: True, 4: True, 5: False, 6: True}
    searches = [p for p in world.stub.paths("POST") if "_search" in p]
    assert len(searches) == len(TOP)


def test_a_done_record_idles(world):
    world.tick(DUE)
    writes = world.metric.writes
    out = world.tick(DUE + timedelta(hours=1))
    assert out["decision"] == "idle"
    assert world.metric.writes == writes


@pytest.mark.parametrize("fresh, builds", [
    ("fresh", 0),
    ("other_run", 1),
    ("before_due", 1),
    ("none", 1),
])
def test_builds_only_without_a_fresh_index_of_the_live_list(world, fresh, builds):
    if fresh == "fresh":
        world.put_index(DUE + timedelta(minutes=5), LIVE.label)
    elif fresh == "other_run":
        world.put_index(DUE + timedelta(minutes=5), "run-1030")
    elif fresh == "before_due":
        world.put_index(DUE - timedelta(seconds=1), LIVE.label)
    world.tick(DUE + timedelta(hours=1))
    assert len(world.builds) == builds
    assert world.record().status is Status.DONE
    assert world.ranked() == RANKED


def _no_batch(w):
    w.metric.batches = []


def _no_random_rows(w):
    w.metric.rows[22] = [r for r in ROWS if "random" not in r.tags]


def _late_activation(w):
    w.live = dataclasses.replace(LIVE, activated_at=datetime(2026, 11, 16, 11, 30, tzinfo=UTC))


def _two_rows(w):
    w.metric.coverage["metric_headset_a"][G] = {"total": 1, "indexed_num": 1}


def _no_row(w):
    del w.metric.coverage["metric_randomset_b"][G]


def _remeasured(w):
    # A later run rewrote is_indexed of one URL after the row was written.
    w.metric.rows[22] = [dataclasses.replace(r, is_indexed=False) if r.id == 2 else r
                         for r in ROWS]


def _wrong_total(w):
    w.metric.coverage["metric_headset_b"][STAT_DATE["head"]]["total"] = 3


def _no_live(w):
    w.live = None


@pytest.mark.parametrize("break_it, reason", [
    (_no_batch, "no metric batch was created before 2026-11-17"),
    (_no_random_rows, "batch 22 has no random rows"),
    (_late_activation, "it is not the list measured on G"),
    (_two_rows, "metric_headset_a has 2 rows dated 2026-11-16"),
    (_no_row, "metric_randomset_b has 0 rows"),
    (_remeasured, "metric_headset_total 2026-11-17: total 4 and indexed_num 3"),
    (_wrong_total, "metric_headset_b 2026-11-17: total 3"),
    (_no_live, "selectdb has no live run"),
])
def test_refusals_write_nothing_and_stay_refused(world, break_it, reason):
    break_it(world)
    out = world.tick(DUE)
    assert out["decision"] == "refuse"
    record = world.record()
    assert record.status is Status.REFUSED and reason in record.reason
    assert world.metric.writes == 0 and world.builds == []
    assert not (world.cfg.work_dir / "rank" / "2026-11-16.ranked.parquet").exists()
    assert world.tick(DUE + timedelta(hours=1))["decision"] == "idle"


def test_named_golden_day_runs_early_and_past_a_later_activation(world):
    _late_activation(world)
    early = DUE - timedelta(days=1)
    out = world.tick(early, golden_day=G)
    assert out["decision"] == "measurewrite"
    assert world.ranked() == RANKED
    assert world.record().index == world.builds[0]


def test_named_golden_day_still_refuses_mismatched_rows(world):
    _remeasured(world)
    world.tick(DUE, golden_day=G)
    assert world.record().status is Status.REFUSED
    assert world.metric.writes == 0


def test_dry_run_builds_and_writes_nothing(world):
    world.put_index(DUE - timedelta(days=2), "run-1030")
    _late_activation(world)
    out = dry_run(world.cfg, world.ports(Clock(DUE)), Clock(DUE))
    assert out["would"] == "refuse"
    assert any("not the list measured on G" in p for p in out["problems"])
    assert out["index_fresh"] is False
    assert out["measured"]["random"]["teams"]["a"]["ranked_num"] == 1
    assert world.builds == [] and world.metric.writes == 0
    assert not (world.cfg.work_dir / "rank").exists()


def test_rerun_after_a_crash_past_commit_converges(world):
    world.metric.crash_after_commit = True
    first = world.tick(DUE)
    assert first["decision"] == "idle" and world.record().status is Status.PENDING
    assert world.record().attempts == 1
    assert world.ranked() == RANKED
    world.tick(DUE + timedelta(minutes=10))
    assert world.record().status is Status.DONE
    assert world.ranked() == RANKED
    assert len(world.builds) == 1
    assert world.metric.writes == 2


def test_attempts_end_in_failed(world):
    def down(*args):
        raise ConnectionError("metricdb down")

    world.metric.batch_before = down
    for n in range(world.cfg.rank.max_attempts):
        world.tick(DUE + timedelta(minutes=10 * n))
    record = world.record()
    assert record.status is Status.FAILED
    assert record.attempts == world.cfg.rank.max_attempts
    assert "metricdb down" in record.reason
    assert world.tick(DUE + timedelta(hours=2))["decision"] == "idle"


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

    world.metric.batch_before = down
    for n in range(world.cfg.rank.max_attempts + 1):
        out = world.tick(DUE + timedelta(minutes=10 * n))
        assert out["decision"] == "idle" and out["why"].startswith("deferred")
    record = world.record()
    assert record.status is Status.PENDING and record.attempts == 0
    del world.metric.batch_before
    world.tick(DUE + timedelta(hours=1))
    assert world.record().status is Status.DONE
    assert world.ranked() == RANKED


def test_a_failed_build_check_uses_an_attempt(world):
    world.build_error = textindex.TextIndexError("isel-text-x holds 9 docs, expected 10")
    world.tick(DUE)
    assert world.record().attempts == 1


def test_ranked_urls_are_a_subset_of_indexed_ones(base_cfg):
    # Everything is in every top 10; only indexed URLs may count as ranked.
    every = [_doc_id(r.url) for r in ROWS] + [_doc_id("https://c.com/z")]
    ranking = count_ranks(ROWS, lambda kw: every, ["head", "random"], base_cfg.rank)
    for tag, tc in ranking.tags.items():
        for team, c in tc.teams.items():
            assert c.ranked_num == c.indexed_num == MEASURED[(tag, team)][1]


def test_record_round_trips(world):
    world.tick(DUE)
    record = world.record()
    assert RankRecord.from_json(record.to_json()) == record
