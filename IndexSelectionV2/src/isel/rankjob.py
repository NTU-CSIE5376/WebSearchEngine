"""The rank rate of each golden day, written into the metric: `isel rank --loop`.

Data shape:

- `GoldenRow`: one `metric_url` row of the batch measured on golden day G, with its
  query's keyword and tags, and the `is_indexed` and final `shard_id` Metric wrote.
- `Counts` per (tag, team): distinct canonical golden URLs (`total`), the indexed and
  the ranked ones, counted as Metric's `CrawlerAllMetricMeasure` counts its coverage.
- `RankRecord` at `work/rank/<G>.json`: the job's `Status` for G, what it used (batch,
  live run, pinned index, snapshot hash), the shadow counts and, per tag, the counts
  it wrote. `work/rank/<G>.ranked.parquet` keeps each row's flags for audit.
- `Snapshot` at `work/rank/<G>.snapshot.json`: the batch rows and G's coverage rows as
  the job first saw them measured and consistent.

G is every `guard.month_days` day: a batch day, which may or may not get a new list.
On G-1 from `rank.build_start` the job builds the text index of the live list, so the
index is ready when the batch appears. From G on it pins G's index: the newest build
of the live list that started before G. The pin is a file under `text_index/pins`
that every build keeps, so a rebuild for the next G leaves the index; it goes when G
is done or refused. Every search of G goes to that physical index, never the alias.

From the end of G's guard window (`guard.month_window`, the batch is complete) the job
writes a shadow count into the record: indexed from selectdb as Metric reads it, ranked
on the pinned index. Once Metric has measured every row of the batch and its coverage
rows match the rows and the live list exactly, the job saves the snapshot and ranks
it. It writes only `metric_url.is_ranked` and the rows' `ranked_num` and `ranked_rate`,
in one transaction that locks the rows and checks them against the snapshot first.
A later read that differs from the snapshot refuses G. Disc/Crawl/Index stay as Metric
measured them: the crawler injects golden URLs after G.

Shape: observe, plan, act, as in `isel.refresh`. `plan` is pure. The build is evidence,
not a flag. Every step is safe to rerun. A crash after the metricdb commit leaves the
record `pending`, and the next tick writes the same values again: the search orders
ties by doc id, so the same index gives the same top k.
"""

from __future__ import annotations

import enum
import http.client
import json
import logging
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import pyarrow as pa

from isel.config import Config, RankCfg, hhmm_minutes, timeout_seconds
from isel.io import (
    LockBusy,
    SecretLeak,
    assert_no_secrets,
    atomic_write_json,
    file_lock,
    read_json,
    write_parquet_atomic,
)
from isel.schedule import golden_days, utc
from isel.urlnorm import canonicalize, lookup_urls, md5_number_lower

if TYPE_CHECKING:
    from isel.pg import ReadOnlySession

log = logging.getLogger(__name__)

RANK_DIR = "rank"   # under work_dir: `lock`, `<G>.json`, `<G>.snapshot.json`, `<G>.ranked.parquet`
# A golden day number recurs within 62 days (see isel.schedule).
_MAX_GAP_DAYS = 62
_REASON_CHARS = 500

Clock = Callable[[], datetime]


def utcnow() -> datetime:
    return datetime.now(UTC)


class RankError(RuntimeError):
    """Raised when a step cannot finish; the tick counts an attempt."""


class SnapshotStale(RankError):
    """The write found metricdb changed since the snapshot; nothing was written."""


class PinMissing(RankError):
    """G's pinned index answered 404: the tick defers, and the plan waits for it."""


# ---------------------------------------------------------------- data shape


class Status(enum.StrEnum):
    PENDING = "pending"      # due, not written yet
    DONE = "done"            # metricdb holds the ranks
    REFUSED = "refused"      # the inputs contradict each other; nothing written
    FAILED = "failed"        # rank.max_attempts ticks raised


TERMINAL = frozenset({Status.DONE, Status.REFUSED, Status.FAILED})


@dataclass(frozen=True)
class GoldenRow:
    """One `metric_url` row of the batch, as Metric left it after measuring G."""

    id: int
    query_id: int
    keyword: str
    tags: tuple[str, ...]
    url: str
    is_indexed: bool
    # -1: Metric found no shard, so the URL counts only in the total. None: no
    # measurement since the row was inserted (a strategy rerun deletes and inserts).
    shard_id: int | None


@dataclass(frozen=True)
class Counts:
    total: int
    indexed_num: int
    ranked_num: int


@dataclass(frozen=True)
class TagCounts:
    teams: dict[str, Counts]
    pairs_found: int         # rows of the tag in their keyword's top k, indexed or not
    pairs_total: int


@dataclass(frozen=True)
class Ranking:
    hits: dict[int, bool]    # metric_url.id -> one of its lookup URLs is in the top k
    ranked: dict[int, bool]  # metric_url.id -> hit and is_indexed: the is_ranked written
    tags: dict[str, TagCounts]


@dataclass(frozen=True)
class RankRecord:
    golden_day: date
    status: Status = Status.PENDING
    reason: str | None = None
    attempts: int = 0
    batch_id: int | None = None
    live_run: str | None = None
    index: str | None = None
    measured_at: datetime | None = None
    written_at: datetime | None = None
    # tag -> {pairs_found, pairs_total, teams: {team: {stat_date, total, indexed_num,
    # ranked_num, ranked_rate}}}
    tags: dict[str, Any] = field(default_factory=dict)
    shadow_at: datetime | None = None
    # tag -> {pairs_found, pairs_total, total, indexed_num, indexed_rate, ranked_num,
    # ranked_rate}: every URL, before Metric measures
    shadow: dict[str, Any] = field(default_factory=dict)
    snapshot_sha256: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {"golden_day": self.golden_day.isoformat(), "status": self.status.value,
                "reason": self.reason, "attempts": self.attempts, "batch_id": self.batch_id,
                "live_run": self.live_run, "index": self.index,
                "measured_at": self.measured_at and self.measured_at.isoformat(),
                "written_at": self.written_at and self.written_at.isoformat(),
                "tags": self.tags,
                "shadow_at": self.shadow_at and self.shadow_at.isoformat(),
                "shadow": self.shadow, "snapshot_sha256": self.snapshot_sha256}

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> RankRecord:
        def at(key: str) -> datetime | None:
            return datetime.fromisoformat(d[key]) if d.get(key) else None

        return cls(date.fromisoformat(d["golden_day"]), Status(d["status"]), d.get("reason"),
                   int(d.get("attempts", 0)), d.get("batch_id"), d.get("live_run"),
                   d.get("index"), at("measured_at"), at("written_at"), d.get("tags") or {},
                   at("shadow_at"), d.get("shadow") or {}, d.get("snapshot_sha256"))


# ---------------------------------------------------------------- counting


def _team_names(r: RankCfg) -> list[str]:
    return [r.total_team, *r.teams]


def _teams_of(shard: int, r: RankCfg) -> list[str]:
    return [r.total_team, *(t for t, (lo, hi) in r.teams.items() if lo <= shard <= hi)]


def tally(rows: Sequence[GoldenRow], hits: Mapping[int, bool], ranked: Mapping[int, bool],
          tags: Sequence[str], r: RankCfg) -> dict[str, TagCounts]:
    """Per tag and team: distinct canonical URLs, the indexed and the ranked ones.

    Metric's rule: a tag's URLs are its rows' canonical URLs; one counts as indexed
    (ranked) if any of its rows is. Its team comes from its shard, and a -1 shard counts
    only in `rank.total_team`.
    """
    out: dict[str, TagCounts] = {}
    for tag in tags:
        urls: dict[str, tuple[bool, bool, int]] = {}
        pairs = found = 0
        for row in rows:
            if tag not in row.tags:
                continue
            pairs += 1
            found += hits.get(row.id, False)
            key = canonicalize(row.url)
            indexed, rk, shard = urls.get(key, (False, False, -1))
            urls[key] = (indexed or row.is_indexed, rk or ranked.get(row.id, False),
                         max(shard, -1 if row.shard_id is None else row.shard_id))
        acc = {team: [0, 0, 0] for team in _team_names(r)}
        for indexed, rk, shard in urls.values():
            for team in _teams_of(shard, r):
                acc[team][0] += 1
                acc[team][1] += indexed
                acc[team][2] += rk
        out[tag] = TagCounts({t: Counts(*c) for t, c in acc.items()}, found, pairs)
    return out


def count_ranks(rows: Sequence[GoldenRow], search: Callable[[str], list[str]],
                tags: Sequence[str], r: RankCfg) -> Ranking:
    """Searches each distinct keyword once and counts the golden URLs in its top k.

    `search(keyword)` returns the top k doc ids (decimal `md5_number_lower` of the
    canonical URL), as `textindex.search` does. A row hits if any of its lookup URLs
    (the canonical URL and its srsltid-stripped form) is among them. It is ranked if it
    hits and Metric found it indexed, so ranked URLs are always a subset of indexed ones.
    """
    top = {kw: set(search(kw)) for kw in sorted({row.keyword for row in rows})}
    hits = {row.id: any(str(md5_number_lower(u)) in top[row.keyword]
                        for u in lookup_urls(canonicalize(row.url))) for row in rows}
    ranked = {row.id: hits[row.id] and row.is_indexed for row in rows}
    return Ranking(hits, ranked, tally(rows, hits, ranked, tags, r))


def shadow_counts(rows: Sequence[GoldenRow], listed: set[str],
                  search: Callable[[str], list[str]], tags: Sequence[str],
                  r: RankCfg) -> dict[str, Any]:
    """Every URL's counts before Metric measures, as Metric would count them now.

    A row is indexed if one of its lookup URLs is in `listed`, the URLs selectdb's live
    list holds, as `CrawlerAllMetricMeasure` reads `selected_urls_current`. Shards are
    unknown before Metric measures, so only `rank.total_team` is counted.
    """
    own = [replace(row, is_indexed=any(u in listed for u in lookup_urls(canonicalize(row.url))),
                   shard_id=-1) for row in rows]
    out: dict[str, Any] = {}
    for tag, tc in count_ranks(own, search, tags, r).tags.items():
        c = tc.teams[r.total_team]
        out[tag] = {"pairs_found": tc.pairs_found, "pairs_total": tc.pairs_total,
                    "total": c.total, "indexed_num": c.indexed_num,
                    "indexed_rate": c.indexed_num / c.total if c.total else 0.0,
                    "ranked_num": c.ranked_num,
                    "ranked_rate": c.ranked_num / c.total if c.total else 0.0}
    return out


# ---------------------------------------------------------------- evidence


@dataclass(frozen=True)
class Batch:
    id: int
    created_at: datetime


@dataclass(frozen=True)
class CoverageRow:
    stat_date: date
    total: int | None
    indexed_num: int | None


@dataclass(frozen=True)
class CoverageWrite:
    table: str
    stat_date: date
    ranked_num: int
    ranked_rate: float


@dataclass(frozen=True)
class LiveRun:
    label: str
    activated_at: datetime | None


@dataclass(frozen=True)
class IndexBuild:
    index: str
    prior_run: str | None    # the live run whose manifest chose the docs; None: no prior
    started_at: datetime     # the build's start, its UTC id


@dataclass(frozen=True)
class Snapshot:
    """Metric's measurement of G as the job ranks it: the batch rows and G's coverage rows.

    Metric keeps no measurement id: a strategy rerun deletes and inserts a keyword's
    rows, and a re-measure rewrites the flags in place. So the job saves this once,
    when Metric has measured every row and the counts match, and every later read and
    the write itself must find exactly the same rows.
    """

    batch_id: int
    rows: tuple[GoldenRow, ...]
    coverage: Mapping[str, tuple[CoverageRow, ...]]   # table -> its rows in [first, last]
    first: date
    last: date

    def to_json(self) -> dict[str, Any]:
        return {"batch_id": self.batch_id, "first": self.first.isoformat(),
                "last": self.last.isoformat(),
                "rows": [[r.id, r.query_id, r.keyword, list(r.tags), r.url, r.is_indexed,
                          r.shard_id] for r in self.rows],
                "coverage": {t: [[c.stat_date.isoformat(), c.total, c.indexed_num]
                                 for c in rows] for t, rows in sorted(self.coverage.items())}}

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> Snapshot:
        return cls(int(d["batch_id"]),
                   tuple(GoldenRow(i, q, kw, tuple(tags), url, indexed, shard)
                         for i, q, kw, tags, url, indexed, shard in d["rows"]),
                   {t: tuple(CoverageRow(date.fromisoformat(sd), total, indexed)
                             for sd, total, indexed in rows)
                    for t, rows in d["coverage"].items()},
                   date.fromisoformat(d["first"]), date.fromisoformat(d["last"]))

    def sha256(self) -> str:
        return sha256(json.dumps(self.to_json(), sort_keys=True).encode()).hexdigest()


class MetricDb(Protocol):
    """The metricdb reads and the one write of the rank job."""

    def batch_before(self, until: datetime) -> Batch | None:
        """The newest batch created before `until`: the one Metric measured."""

    def batch_rows(self, batch_id: int) -> list[GoldenRow]:
        """Every `metric_url` row of the batch with a URL."""

    def coverage_rows(self, table: str, first: date, last: date) -> list[CoverageRow]:
        """The rows of a coverage table with `first <= stat_date <= last`."""

    def write(self, snapshot: Snapshot, ranked: Mapping[int, bool],
              rows: Sequence[CoverageWrite]) -> None:
        """Sets `is_ranked` and the coverage rows' ranked fields in one transaction.

        It first locks the batch rows and the coverage rows and raises `SnapshotStale`
        unless they are exactly `snapshot`'s.
        """


@dataclass(frozen=True)
class Ports:
    metric: MetricDb
    live_run: Callable[[], LiveRun | None]
    builds: Callable[[], list[IndexBuild]]           # every recorded build, oldest first
    build: Callable[[], Any]
    search: Callable[[str, str], list[str]]          # (physical index, keyword) -> top k
    listed: Callable[[Sequence[str]], set[str]]      # URLs the live list holds
    # (physical index, holder) -> False if the index is gone; see `textindex.pin`
    pin: Callable[[str, dict[str, Any]], bool]


@dataclass(frozen=True)
class Evidence:
    batch: Batch | None
    rows: tuple[GoldenRow, ...]
    # (tag, team) -> the table's rows dated from G to the day before the next G
    coverage: Mapping[tuple[str, str], tuple[CoverageRow, ...]]
    live: LiveRun | None
    builds: tuple[IndexBuild, ...]
    pin: str | None = None                   # G's pinned index, if G has one
    listed: frozenset[str] | None = None     # the rows' lookup URLs on the live list
    snapshot: Snapshot | None = None         # saved by an earlier tick of G


def _midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def _day_at(day: date, hhmm: str) -> datetime:
    return _midnight(day) + timedelta(minutes=hhmm_minutes(hhmm))


def build_from(golden_day: date, r: RankCfg) -> datetime:
    """G-1 at `rank.build_start`: the job works on G from here on."""
    return _day_at(golden_day - timedelta(days=1), r.build_start)


def build_until(golden_day: date, r: RankCfg) -> datetime:
    """G-1 at `rank.build_latest`: the last time a build of G may start."""
    return _day_at(golden_day - timedelta(days=1), r.build_latest)


def measure_from(golden_day: date, cfg: Config) -> datetime:
    """The end of G's guard window: the batch is complete and metricdb is open."""
    return _day_at(golden_day, cfg.guard.month_window[1])


def next_golden_day(golden_day: date, cfg: Config) -> date:
    return next(golden_days(golden_day + timedelta(days=1), cfg.guard.month_days))


def coverage_table(r: RankCfg, tag: str, team: str) -> str:
    return r.coverage_table_fmt.format(tag=tag, team=team)


def _last_day(golden_day: date, cfg: Config) -> date:
    """The last stat_date of G's coverage rows: the day before the next G."""
    return next_golden_day(golden_day, cfg) - timedelta(days=1)


def snapshot_path(cfg: Config, golden_day: date) -> Path:
    return rank_dir(cfg) / f"{golden_day.isoformat()}.snapshot.json"


def read_snapshot(cfg: Config, golden_day: date) -> Snapshot | None:
    data = read_json(snapshot_path(cfg, golden_day))
    return None if data is None else Snapshot.from_json(data["snapshot"])


def pin_of(cfg: Config, golden_day: date) -> str | None:
    """G's pinned physical index: the pin file whose holder is G."""
    from isel.textindex import pins

    mine = [i for i, h in pins(cfg).items() if h.get("golden_day") == golden_day.isoformat()]
    return mine[-1] if mine else None


def observe(cfg: Config, golden_day: date, ports: Ports, *, metric: bool = True) -> Evidence:
    """What the job sees for G; with `metric` False it reads no metricdb (before G)."""
    live, builds = ports.live_run(), tuple(ports.builds())
    pin = pin_of(cfg, golden_day)
    if not metric:
        return Evidence(None, (), {}, live, builds, pin)
    batch = ports.metric.batch_before(_midnight(golden_day + timedelta(days=1)))
    rows = tuple(ports.metric.batch_rows(batch.id)) if batch else ()
    last = _last_day(golden_day, cfg)
    coverage = {(tag, team): tuple(ports.metric.coverage_rows(
                    coverage_table(cfg.rank, tag, team), golden_day, last))
                for tag in cfg.golden.tags for team in _team_names(cfg.rank)}
    listed = frozenset(ports.listed(_lookup_spellings(rows))) if rows else frozenset()
    return Evidence(batch, rows, coverage, live, builds, pin, listed,
                    read_snapshot(cfg, golden_day))


# ---------------------------------------------------------------- plan


@dataclass(frozen=True)
class Idle:
    why: str


@dataclass(frozen=True)
class Refuse:
    reason: str


@dataclass(frozen=True)
class Build:
    pass


@dataclass(frozen=True)
class Pin:
    pass


@dataclass(frozen=True)
class Shadow:
    pass


@dataclass(frozen=True)
class MeasureWrite:
    pass


Decision = Idle | Refuse | Build | Pin | Shadow | MeasureWrite


def pick_golden_day(now: datetime, cfg: Config) -> date:
    """The golden day the job works on at `now`: the latest one whose build has begun."""
    now = utc(now)
    first = now.date() - timedelta(days=_MAX_GAP_DAYS)
    picked = None
    for g in golden_days(first, cfg.guard.month_days):
        if build_from(g, cfg.rank) > now:
            break
        picked = g
    assert picked is not None, "the search starts more than one gap before now"
    return picked


def waiting(cfg: Config, now: datetime, record: RankRecord, scheduled: bool) -> Idle | None:
    """Idle if the record is final, or if a scheduled tick comes before G's build."""
    if record.status in TERMINAL:
        return Idle(f"the record of {record.golden_day} is {record.status}")
    start = build_from(record.golden_day, cfg.rank)
    if scheduled and utc(now) < start:
        return Idle(f"builds from {start.isoformat()}")
    return None


def batch_tags(cfg: Config, ev: Evidence) -> list[str]:
    """The tags the batch has rows of: Dec 8 is head only."""
    return [t for t in cfg.golden.tags if any(t in row.tags for row in ev.rows)]


def _golden_rows(cfg: Config, ev: Evidence) -> list[GoldenRow]:
    """The rows Metric measures: those of a golden tag."""
    tags = set(batch_tags(cfg, ev))
    return [row for row in ev.rows if tags & set(row.tags)]


def refusals(cfg: Config, golden_day: date, ev: Evidence, check_activation: bool) -> list[str]:
    """Why G cannot take a rank whatever Metric measures next; empty if it can.

    The batch must be created on G and have rows of a tag. Each of its tags' coverage
    tables may hold no row yet (Metric has not measured) but not several. The list
    measured on G must be the live run, active since before G's freeze start.
    """
    if ev.batch is None:
        return [f"no metric batch was created before {golden_day + timedelta(days=1)}"]
    if ev.batch.created_at < _midnight(golden_day):
        return [f"the newest batch {ev.batch.id} was created on "
                f"{ev.batch.created_at.date()}, not on {golden_day}"]
    tags = batch_tags(cfg, ev)
    if not tags:
        return [f"batch {ev.batch.id} has no {' or '.join(cfg.golden.tags)} rows"]
    reasons = []
    # The metric may read the list from G's freeze start on, a freeze day or not.
    freeze_start = _day_at(golden_day, cfg.schedule.swap_freeze_window[0])
    if ev.live is None:
        reasons.append("selectdb has no live run")
    elif check_activation and (ev.live.activated_at is None
                               or ev.live.activated_at >= freeze_start):
        reasons.append(f"live run {ev.live.label} was activated at "
                       f"{ev.live.activated_at}, not before {freeze_start.isoformat()}: "
                       "it is not the list measured on G")
    for tag in tags:
        for team in _team_names(cfg.rank):
            table, rows = coverage_table(cfg.rank, tag, team), ev.coverage[(tag, team)]
            if len(rows) > 1:
                reasons.append(f"{table} has {len(rows)} rows dated {golden_day} or later, "
                               "before the next golden day")
    return reasons


def count_mismatches(cfg: Config, ev: Evidence) -> list[str]:
    """Coverage rows whose counts are not exactly those of the batch rows.

    Metric counts a row from the rows it measured, so a difference means the rows
    changed after the row was written: a rerun replaced URLs, or a re-measure rewrote
    the flags of a row another run had written.
    """
    tags = batch_tags(cfg, ev)
    counts = tally(ev.rows, {}, {}, tags, cfg.rank)
    reasons = []
    for tag in tags:
        for team in _team_names(cfg.rank):
            table, rows = coverage_table(cfg.rank, tag, team), ev.coverage[(tag, team)]
            if len(rows) != 1:
                continue
            got, want = rows[0], counts[tag].teams[team]
            if (got.total, got.indexed_num) != (want.total, want.indexed_num):
                reasons.append(f"{table} {got.stat_date}: total {got.total} and indexed_num "
                               f"{got.indexed_num}, batch {ev.batch and ev.batch.id} gives "
                               f"{want.total} and {want.indexed_num}")
    return reasons


def unmeasured(cfg: Config, ev: Evidence) -> list[int]:
    """Golden rows inserted after Metric's last measurement: no shard yet."""
    return [row.id for row in _golden_rows(cfg, ev) if row.shard_id is None]


def indexed_mismatches(cfg: Config, ev: Evidence) -> list[int]:
    """Golden rows whose `is_indexed` is not what the live list says.

    Metric sets `is_indexed` from `selected_urls_current` with the same w3lib as isel
    (both pin it, #52), so every row agrees with the list it measured; a disagreement
    means it measured another list.
    """
    listed = ev.listed or frozenset()
    return [row.id for row in _golden_rows(cfg, ev)
            if row.is_indexed != any(u in listed for u in lookup_urls(canonicalize(row.url)))]


def snapshot_of(cfg: Config, golden_day: date, ev: Evidence) -> Snapshot:
    assert ev.batch is not None, "a snapshot needs the batch"
    return Snapshot(ev.batch.id, ev.rows,
                    {coverage_table(cfg.rank, tag, team): tuple(ev.coverage[(tag, team)])
                     for tag in batch_tags(cfg, ev) for team in _team_names(cfg.rank)},
                    golden_day, _last_day(golden_day, cfg))


def snapshot_changes(saved: Snapshot, now: Snapshot) -> list[str]:
    """What differs between the saved snapshot and the rows read now."""
    out = []
    if saved.batch_id != now.batch_id:
        out.append(f"batch {saved.batch_id} became {now.batch_id}")
    was, got = set(saved.rows), set(now.rows)
    if was != got:
        out.append(f"{len(was - got)} rows changed or went, {len(got - was)} rows are new")
    out += [f"{t} changed" for t in sorted(set(saved.coverage) | set(now.coverage))
            if saved.coverage.get(t) != now.coverage.get(t)]
    return out


def fresh(ev: Evidence, since: datetime) -> bool:
    """The newest build holds the live run's list and started at or after `since`."""
    return (bool(ev.builds) and ev.live is not None
            and ev.builds[-1].prior_run == ev.live.label and ev.builds[-1].started_at >= since)


def pin_candidate(ev: Evidence, golden_day: date, scheduled: bool) -> IndexBuild | None:
    """The index G should pin: the newest build of the live list that started before G.

    A tick for a named golden day falls back to the newest build of the live list, so a
    G whose build was missed can be built and written by hand.
    """
    if ev.live is None:
        return None
    mine = [b for b in ev.builds if b.prior_run == ev.live.label]
    before = [b for b in mine if b.started_at < _midnight(golden_day)]
    if before:
        return before[-1]
    return None if scheduled or not mine else mine[-1]


def pinned(ev: Evidence) -> IndexBuild | None:
    """G's pinned build, if it still exists."""
    return next((b for b in ev.builds if b.index == ev.pin), None)


def missing_rows(cfg: Config, ev: Evidence) -> list[str]:
    """The coverage tables of the batch's tags that hold no row of G yet."""
    return [coverage_table(cfg.rank, tag, team) for tag in batch_tags(cfg, ev)
            for team in _team_names(cfg.rank) if not ev.coverage[(tag, team)]]


def plan(cfg: Config, now: datetime, record: RankRecord, ev: Evidence, *,
         scheduled: bool) -> Decision:
    """The next step for `record`.

    A scheduled tick builds on G-1 between `rank.build_start` and `rank.build_latest`
    unless the newest build already holds the live list from that window. From G's
    midnight it pins G's index and waits for `measure_from`. From there it refuses,
    writes the shadow counts once, and waits until Metric has measured every row of
    the batch. Once Metric has measured, the rows, the coverage rows, the live list and
    any saved snapshot must agree before it builds, pins or writes. A tick for a named
    golden day skips the clock and the activation check, and builds if G has no index
    of the live list.
    """
    idle = waiting(cfg, now, record, scheduled)
    if idle is not None:
        return idle
    g, now = record.golden_day, utc(now)
    if scheduled and now < _midnight(g):
        if ev.live is None:
            return Idle("selectdb has no live run")
        if fresh(ev, build_from(g, cfg.rank)):
            return Idle(f"the index of {ev.live.label} is built; measures from "
                        f"{measure_from(g, cfg).isoformat()}")
        if now > build_until(g, cfg.rank):
            return Idle(f"no build may start after {build_until(g, cfg.rank).isoformat()}")
        return Build()
    pin = pinned(ev)
    if ev.pin is not None and pin is None:
        return Idle(f"the pinned index {ev.pin} is missing")
    if scheduled and now < measure_from(g, cfg):
        if ev.pin is None and pin_candidate(ev, g, scheduled) is not None:
            return Pin()
        return Idle(f"measures from {measure_from(g, cfg).isoformat()}")
    reasons = refusals(cfg, g, ev, check_activation=scheduled)
    if reasons:
        return Refuse("; ".join(reasons))
    assert ev.live is not None, "refusals refuse without a live run"
    missing = missing_rows(cfg, ev)
    # Metric has measured: the rows must be what it counted, before any build or search.
    if not missing:
        if ev.snapshot is not None:
            changes = snapshot_changes(ev.snapshot, snapshot_of(cfg, g, ev))
            if changes:
                return Refuse("metricdb changed since the snapshot: " + "; ".join(changes))
        pending = unmeasured(cfg, ev)
        if pending:
            return Idle(f"waiting for Metric to measure {len(pending)} rows inserted after "
                        f"its last measurement (first id {pending[0]})")
        reasons = count_mismatches(cfg, ev)
        if reasons:
            return Refuse("; ".join(reasons))
        wrong = indexed_mismatches(cfg, ev)
        if wrong:
            return Refuse(f"{len(wrong)} rows' is_indexed disagree with the list of "
                          f"{ev.live.label} (first id {wrong[0]}): Metric measured another "
                          "list")
    if pin is None:
        if pin_candidate(ev, g, scheduled) is not None:
            return Pin()
        if not scheduled:
            return Build()
        return Refuse(f"no index of {ev.live.label} was built before {g}")
    if pin.prior_run != ev.live.label:
        return Refuse(f"the live run is {ev.live.label}, but the pinned index {pin.index} "
                      f"holds the list of {pin.prior_run}")
    if record.shadow_at is None:
        return Shadow()
    if missing:
        return Idle(f"waiting for Metric's coverage rows: {', '.join(missing)}")
    return MeasureWrite()


# ---------------------------------------------------------------- act


def rank_dir(cfg: Config) -> Path:
    return cfg.work_dir / RANK_DIR


def record_path(cfg: Config, golden_day: date) -> Path:
    return rank_dir(cfg) / f"{golden_day.isoformat()}.json"


def read_record(cfg: Config, golden_day: date) -> RankRecord:
    data = read_json(record_path(cfg, golden_day))
    return RankRecord(golden_day) if data is None else RankRecord.from_json(data)


def _save(cfg: Config, record: RankRecord) -> None:
    atomic_write_json(record_path(cfg, record.golden_day), record.to_json())


def _reason(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}"[:_REASON_CHARS]
    try:
        assert_no_secrets(text)
    except SecretLeak:
        return type(exc).__name__
    return text


def tag_results(ranking: Ranking, ev: Evidence) -> dict[str, Any]:
    """The record's per-tag counts; `stat_date` is None where no single row matched.

    `ranked_rate` divides by the row's own `total`, so the row stays consistent with
    the rates Metric wrote; without a single row it divides by the recomputed total.
    """
    out: dict[str, Any] = {}
    for tag, tc in ranking.tags.items():
        teams = {}
        for team, c in tc.teams.items():
            rows = ev.coverage.get((tag, team), ())
            row = rows[0] if len(rows) == 1 else None
            total = row.total if row is not None and row.total is not None else c.total
            teams[team] = {"stat_date": row and row.stat_date.isoformat(),
                           "total": c.total, "indexed_num": c.indexed_num,
                           "ranked_num": c.ranked_num,
                           "ranked_rate": c.ranked_num / total if total else 0.0}
        out[tag] = {"pairs_found": tc.pairs_found, "pairs_total": tc.pairs_total,
                    "teams": teams}
    return out


def _audit(cfg: Config, golden_day: date, rows: Sequence[GoldenRow], ranking: Ranking) -> None:
    write_parquet_atomic(pa.table({
        "metric_url_id": pa.array([r.id for r in rows], pa.int64()),
        "query_id": pa.array([r.query_id for r in rows], pa.int64()),
        "hit": pa.array([ranking.hits[r.id] for r in rows], pa.bool_()),
        "is_ranked": pa.array([ranking.ranked[r.id] for r in rows], pa.bool_()),
    }), rank_dir(cfg) / f"{golden_day.isoformat()}.ranked.parquet")


def _search_on(ports: Ports, index: str) -> Callable[[str], list[str]]:
    """Searches the physical `index` only; a 404 means it is gone, not that a doc is."""
    from isel.textindex import TextIndexError

    def search(keyword: str) -> list[str]:
        try:
            return ports.search(index, keyword)
        except TextIndexError as exc:
            if exc.status == 404:
                raise PinMissing(f"the pinned index {index} answered 404") from exc
            raise

    return search


def _lookup_spellings(rows: Sequence[GoldenRow]) -> list[str]:
    return sorted({u for row in rows for u in lookup_urls(canonicalize(row.url))})


def _shadow(cfg: Config, record: RankRecord, ev: Evidence, ports: Ports,
            pin: IndexBuild, clock: Clock) -> RankRecord:
    assert ev.listed is not None, "the shadow reads the list with the rows"
    shadow = shadow_counts(ev.rows, set(ev.listed), _search_on(ports, pin.index),
                           batch_tags(cfg, ev), cfg.rank)
    return replace(record, shadow_at=clock(), shadow=shadow)


def _save_snapshot(cfg: Config, snapshot: Snapshot, ev: Evidence, pin: IndexBuild,
                   clock: Clock) -> None:
    atomic_write_json(snapshot_path(cfg, snapshot.first), {
        "taken_at": clock().isoformat(), "live_run": ev.live and ev.live.label,
        "index": pin.index, "sha256": snapshot.sha256(), "snapshot": snapshot.to_json()})


def _measure_write(cfg: Config, record: RankRecord, ev: Evidence, ports: Ports,
                   pin: IndexBuild, clock: Clock) -> RankRecord:
    snapshot = ev.snapshot or snapshot_of(cfg, record.golden_day, ev)
    if ev.snapshot is None:
        # Saved before the searches: a change during them refuses the next tick.
        _save_snapshot(cfg, snapshot, ev, pin, clock)
    rows = snapshot.rows
    ranking = count_ranks(rows, _search_on(ports, pin.index), batch_tags(cfg, ev), cfg.rank)
    tags = tag_results(ranking, ev)
    measured_at = clock()
    _audit(cfg, record.golden_day, rows, ranking)
    writes = [CoverageWrite(coverage_table(cfg.rank, tag, team),
                            date.fromisoformat(t["stat_date"]), t["ranked_num"], t["ranked_rate"])
              for tag, entry in tags.items() for team, t in entry["teams"].items()]
    ports.metric.write(snapshot, ranking.ranked, writes)
    return replace(record, status=Status.DONE, reason=None, measured_at=measured_at,
                   written_at=clock(), tags=tags, snapshot_sha256=snapshot.sha256())


def transient(exc: BaseException) -> bool:
    """True for an outage a later tick may get past; it must not use up an attempt.

    A dropped or unreachable database server, an OpenSearch node that did not answer,
    a manual `text-index-build` that holds the build lock, and a pinned index that
    answered 404: the plan then waits for it instead of searching another.
    """
    from isel.pg import connection_lost
    from isel.textindex import TextIndexError

    if isinstance(exc, LockBusy | PinMissing) or connection_lost(exc):
        return True
    # An HTTP reply carries a status; urllib's HTTPError is an OSError too, and a
    # reply a retry repeats (400, 403, 500) must use up attempts.
    return (isinstance(exc, TextIndexError) and exc.status is None
            and isinstance(exc.__cause__, OSError | http.client.HTTPException))


def _seen(record: RankRecord, ev: Evidence) -> RankRecord:
    return replace(record, batch_id=ev.batch and ev.batch.id,
                   live_run=ev.live and ev.live.label, index=ev.pin)


def _step(cfg: Config, now: datetime, record: RankRecord, ports: Ports, clock: Clock,
          scheduled: bool) -> tuple[Decision, RankRecord]:
    from isel.textindex import unpin

    g = record.golden_day
    # metricdb is closed until G's guard window ends, and the batch is not complete.
    ev = observe(cfg, g, ports, metric=not (scheduled and utc(now) < measure_from(g, cfg)))
    built = False
    while True:
        record = _seen(record, ev)
        decision = plan(cfg, now, record, ev, scheduled=scheduled)
        if isinstance(decision, Build):
            if built:
                raise RankError("the build left no index of the live run's list")
            ports.build()
            built = True
            ev = replace(ev, builds=tuple(ports.builds()))
        elif isinstance(decision, Pin):
            candidate = pin_candidate(ev, g, scheduled)
            assert candidate is not None, "plan pins only a candidate"
            if not ports.pin(candidate.index, {"golden_day": g.isoformat(),
                                               "pinned_at": clock().isoformat()}):
                raise RankError(f"{candidate.index} was deleted before it could be pinned")
            ev = replace(ev, pin=candidate.index)
        elif isinstance(decision, Shadow):
            pin = pinned(ev)
            assert pin is not None, "plan shadows only with a pinned index"
            record = _shadow(cfg, record, ev, ports, pin, clock)
        elif isinstance(decision, MeasureWrite):
            pin = pinned(ev)
            assert pin is not None, "plan writes only with a pinned index"
            record = _measure_write(cfg, record, ev, ports, pin, clock)
            unpin(cfg, pin.index)
            return decision, record
        elif isinstance(decision, Refuse):
            if ev.pin is not None:
                unpin(cfg, ev.pin)
            return decision, replace(record, status=Status.REFUSED, reason=decision.reason)
        else:
            return decision, record


def tick(cfg: Config, ports: Ports, clock: Clock = utcnow,
         golden_day: date | None = None) -> dict[str, Any]:
    """One observe-plan-act round: for the golden day of `now`, or for `golden_day`.

    A final record and a busy lock idle without reading a database; before G the tick
    reads no metricdb. Otherwise it builds, refuses, shadows or writes as `plan` says. A
    `transient` error defers to the next tick; any other exception counts an attempt,
    and at `rank.max_attempts` the record is `failed`.
    """
    now = clock()
    scheduled = golden_day is None
    g = pick_golden_day(now, cfg) if scheduled else golden_day
    try:
        with file_lock(rank_dir(cfg) / "lock"):
            record = read_record(cfg, g)
            decision: Decision | None = waiting(cfg, now, record, scheduled)
            if decision is None:
                try:
                    decision, record = _step(cfg, now, record, ports, clock, scheduled)
                except Exception as exc:
                    if transient(exc):
                        log.warning("rank tick of %s deferred: %s", g, _reason(exc))
                        decision = Idle(f"deferred: {_reason(exc)}")
                        record = replace(record, reason=decision.why)
                    else:
                        log.exception("rank tick of %s failed", g)
                        attempts = record.attempts + 1
                        failed = attempts >= cfg.rank.max_attempts
                        decision = Idle(f"attempt {attempts} failed")
                        record = replace(record, attempts=attempts, reason=_reason(exc),
                                         status=Status.FAILED if failed else Status.PENDING)
                _save(cfg, record)
    except LockBusy:
        return {"golden_day": g.isoformat(), "decision": "idle",
                "why": "another rank tick holds the lock"}
    return {"golden_day": g.isoformat(), "build_from": build_from(g, cfg.rank).isoformat(),
            "measure_from": measure_from(g, cfg).isoformat(),
            "decision": type(decision).__name__.lower(),
            **({"why": decision.why} if isinstance(decision, Idle) else {}),
            "record": record.to_json()}


def dry_run(cfg: Config, ports: Ports, clock: Clock = utcnow,
            golden_day: date | None = None) -> dict[str, Any]:
    """What a tick would decide, and the counts on G's pinned index; writes nothing.

    It neither builds, pins nor writes a file or a row. Refusal reasons, the live
    run's activation, count mismatches and rows that disagree with the list included,
    come back as `problems`. With rows and an index (G's pin, or the one it would pin)
    it also counts: `measured` with Metric's `is_indexed`, `shadow` with selectdb's list.
    """
    now = clock()
    g = pick_golden_day(now, cfg) if golden_day is None else golden_day
    record = read_record(cfg, g)
    ev = observe(cfg, g, ports)
    pin = pinned(ev) if ev.pin is not None else pin_candidate(ev, g, scheduled=False)
    wrong = indexed_mismatches(cfg, ev)
    problems = refusals(cfg, g, ev, check_activation=True) + count_mismatches(cfg, ev)
    if ev.snapshot is not None and ev.batch is not None:
        problems += snapshot_changes(ev.snapshot, snapshot_of(cfg, g, ev))
    if wrong:
        problems.append(f"{len(wrong)} rows' is_indexed disagree with the live list")
    out: dict[str, Any] = {
        "golden_day": g.isoformat(), "build_from": build_from(g, cfg.rank).isoformat(),
        "measure_from": measure_from(g, cfg).isoformat(),
        "would": type(plan(cfg, now, record, ev, scheduled=golden_day is None)).__name__.lower(),
        "record": record.to_json(),
        "problems": problems,
        "missing_rows": missing_rows(cfg, ev),
        "unmeasured_rows": len(unmeasured(cfg, ev)),
        "indexed_mismatch_ids": wrong[:20],
        "batch_id": ev.batch and ev.batch.id, "live_run": ev.live and ev.live.label,
        "pinned": ev.pin, "index": pin and pin.index,
        "index_fresh": fresh(ev, build_from(g, cfg.rank)),
    }
    if ev.rows and pin is not None:
        search, tags = _search_on(ports, pin.index), batch_tags(cfg, ev)
        out["measured"] = tag_results(count_ranks(ev.rows, search, tags, cfg.rank), ev)
        out["shadow"] = shadow_counts(ev.rows, set(ev.listed or ()), search, tags, cfg.rank)
    return out


# ---------------------------------------------------------------- real ports


_BATCH_SQL = ("SELECT id, created_at FROM metric_batches WHERE created_at < %s "
              "ORDER BY created_at DESC, id DESC LIMIT 1")
_ROWS_SQL = ("SELECT u.id, u.query_id, q.keyword, q.tags, u.url, u.is_indexed, u.shard_id "
             "FROM metric_url u JOIN metric_queries q ON q.id = u.query_id "
             "WHERE q.batch_id = %s AND u.url IS NOT NULL ORDER BY u.id")
# The write locks the batch rows: a strategy rerun deletes them and must wait.
_LOCK_ROWS_SQL = _ROWS_SQL + " FOR UPDATE OF u"
_RANKED_SQL = ("UPDATE metric_url AS m SET is_ranked = u.r "
               "FROM unnest(%s::bigint[], %s::boolean[]) AS u(id, r) WHERE m.id = u.id")


def _golden_row(t: Sequence[Any]) -> GoldenRow:
    i, q, kw, tags, url, indexed, shard = t
    return GoldenRow(int(i), int(q), kw, tuple(tags or ()), url, bool(indexed),
                     None if shard is None else int(shard))


class PgMetricDb:
    """Reads through the guarded read-only session; writes as `rank.writer_dsn_env`.

    Both connections open on first use, so a tick that idles connects to nothing.
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._reader: ReadOnlySession | None = None

    def _read(self) -> ReadOnlySession:
        from isel.pg import open_metricdb

        if self._reader is None:
            self._reader = open_metricdb(self._cfg)
        return self._reader

    def close(self) -> None:
        if self._reader is not None:
            self._reader.close()

    def batch_before(self, until: datetime) -> Batch | None:
        # metric_batches.created_at is naive UTC.
        row = self._read().fetchone(_BATCH_SQL, (utc(until).replace(tzinfo=None),))
        return None if row is None else Batch(int(row[0]), row[1].replace(tzinfo=UTC))

    def batch_rows(self, batch_id: int) -> list[GoldenRow]:
        return [_golden_row(t) for t in self._read().fetchall(_ROWS_SQL, (batch_id,))]

    def coverage_rows(self, table: str, first: date, last: date) -> list[CoverageRow]:
        from psycopg import sql

        query = sql.SQL("SELECT stat_date, total, indexed_num FROM {} "
                        "WHERE stat_date BETWEEN %s AND %s ORDER BY stat_date"
                        ).format(sql.Identifier(table))
        return [CoverageRow(*r) for r in self._read().fetchall(query, (first, last))]

    def write(self, snapshot: Snapshot, ranked: Mapping[int, bool],
              rows: Sequence[CoverageWrite]) -> None:
        from psycopg import sql

        from isel.pg import next_allowed_start, open_metricdb_writer

        timeout_s = timeout_seconds(self._cfg.rank.session)
        now = utcnow()
        start = next_allowed_start(now, timeout_s, self._cfg.guard)
        if start > now:
            log.info("metricdb window closed; writing at %s", start.isoformat())
            time.sleep((start - now).total_seconds())
        ids = sorted(ranked)
        conn = open_metricdb_writer(self._cfg)
        try:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(_LOCK_ROWS_SQL, (snapshot.batch_id,))
                locked = tuple(_golden_row(t) for t in cur.fetchall())
                coverage = {}
                for table in snapshot.coverage:
                    cur.execute(sql.SQL("SELECT stat_date, total, indexed_num FROM {} "
                                        "WHERE stat_date BETWEEN %s AND %s "
                                        "ORDER BY stat_date FOR UPDATE"
                                        ).format(sql.Identifier(table)),
                                (snapshot.first, snapshot.last))
                    coverage[table] = tuple(CoverageRow(*r) for r in cur.fetchall())
                changes = snapshot_changes(snapshot, Snapshot(
                    snapshot.batch_id, locked, coverage, snapshot.first, snapshot.last))
                if changes:
                    raise SnapshotStale("metricdb changed since the snapshot: "
                                        + "; ".join(changes))
                cur.execute(_RANKED_SQL, (ids, [ranked[i] for i in ids]))
                if cur.rowcount != len(ids):
                    raise RankError(f"is_ranked: updated {cur.rowcount} of {len(ids)} rows")
                for w in rows:
                    cur.execute(sql.SQL("UPDATE {} SET ranked_num = %s, ranked_rate = %s "
                                        "WHERE stat_date = %s").format(sql.Identifier(w.table)),
                                (w.ranked_num, w.ranked_rate, w.stat_date))
                    if cur.rowcount != 1:
                        raise RankError(f"{w.table} {w.stat_date}: updated {cur.rowcount} rows")
        finally:
            conn.close()


def read_live_run(cfg: Config) -> LiveRun | None:
    from isel.selectdb import connect

    conn = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=True)
    try:
        rows = conn.execute("SELECT run_label, activated_at FROM public.model_runs "
                            "WHERE status = 'live'").fetchall()
    finally:
        conn.close()
    if len(rows) > 1:
        raise RankError(f"selectdb has {len(rows)} live runs")
    return LiveRun(*rows[0]) if rows else None


def index_builds(cfg: Config, client: Any) -> list[IndexBuild]:
    """Every recorded build of the alias, oldest first."""
    from isel.textindex import build_records

    return [IndexBuild(r["index"], r.get("prior_run"), r["started_at"])
            for r in build_records(cfg, client)]


def read_listed(cfg: Config, urls: Sequence[str]) -> set[str]:
    """The URLs among `urls` on the live list, read as the metric reads them."""
    from isel.selectdb import connect

    out: set[str] = set()
    chunk = cfg.selectdb.verify_chunk
    conn = connect(cfg, cfg.selectdb.reader_dsn_env, read_only=True)
    try:
        for i in range(0, len(urls), chunk):
            rows = conn.execute("SELECT url FROM public.selected_urls_current "
                                "WHERE url = ANY(%s)", (list(urls[i:i + chunk]),)).fetchall()
            out.update(u for (u,) in rows)
    finally:
        conn.close()
    return out


@contextmanager
def real_ports(cfg: Config) -> Iterator[Ports]:
    from isel import textindex

    t = cfg.text_index
    client = textindex.OpenSearch(t.url, t.timeout_s, t.retries, t.backoff_s)
    metric = PgMetricDb(cfg)
    try:
        yield Ports(metric=metric, live_run=lambda: read_live_run(cfg),
                    builds=lambda: index_builds(cfg, client),
                    build=lambda: textindex.build(cfg, client, Path(cfg.recrawl.text_root)),
                    search=lambda index, kw: textindex.search(client, t, kw, index)[0],
                    listed=lambda urls: read_listed(cfg, urls),
                    pin=lambda index, holder: textindex.pin(cfg, client, index, holder))
    finally:
        metric.close()
