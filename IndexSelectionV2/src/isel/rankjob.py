"""The rank rate of each golden day, written into the metric: `isel rank --loop`.

Data shape:

- `GoldenRow`: one `metric_url` row of the batch measured on golden day G, with its
  query's keyword and tags, and the `is_indexed` and final `shard_id` Metric wrote.
- `Counts` per (tag, team): distinct canonical golden URLs (`total`), the indexed and
  the ranked ones, counted as Metric's `CrawlerAllMetricMeasure` counts its coverage.
- `RankRecord` at `work/rank/<G>.json`: the job's `Status` for G, what it used (batch,
  live run, index) and, per tag, the counts it wrote. `work/rank/<G>.ranked.parquet`
  keeps each row's flags for audit.

At G + `rank.delay_days` the recrawl has fetched most of the list that was live at G.
The job builds the text index from that list, searches each golden keyword and counts
the golden URLs in the top k. It writes only `metric_url.is_ranked` and the coverage
rows' `ranked_num` and `ranked_rate`, in one transaction. Disc/Crawl/Index stay as
measured on G: the crawler injects golden URLs after G, so measuring again inflates them.

Shape: observe, plan, act, as in `isel.refresh`. `plan` is pure. The build is evidence,
not a flag: the alias's index holds the live run's list and was built after the due
time. Every step is safe to rerun. A crash after the metricdb commit leaves the record
`pending`, and the next tick writes the same values again: the search orders ties by
doc id, so the same index gives the same top k.
"""

from __future__ import annotations

import enum
import logging
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
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
from isel.schedule import golden_days, utc, window
from isel.urlnorm import canonicalize, lookup_urls, md5_number_lower

if TYPE_CHECKING:
    from isel.pg import ReadOnlySession

log = logging.getLogger(__name__)

RANK_DIR = "rank"                  # under work_dir: `lock`, `<G>.json`, `<G>.ranked.parquet`
# A golden day number recurs within 62 days (see isel.schedule).
_MAX_GAP_DAYS = 62
_REASON_CHARS = 500

Clock = Callable[[], datetime]


def utcnow() -> datetime:
    return datetime.now(UTC)


class RankError(RuntimeError):
    """Raised when a step cannot finish; the tick counts an attempt."""


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
    shard_id: int            # -1: Metric found no shard; such URLs count only in the total


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

    def to_json(self) -> dict[str, Any]:
        return {"golden_day": self.golden_day.isoformat(), "status": self.status.value,
                "reason": self.reason, "attempts": self.attempts, "batch_id": self.batch_id,
                "live_run": self.live_run, "index": self.index,
                "measured_at": self.measured_at and self.measured_at.isoformat(),
                "written_at": self.written_at and self.written_at.isoformat(),
                "tags": self.tags}

    @classmethod
    def from_json(cls, d: Mapping[str, Any]) -> RankRecord:
        def at(key: str) -> datetime | None:
            return datetime.fromisoformat(d[key]) if d.get(key) else None

        return cls(date.fromisoformat(d["golden_day"]), Status(d["status"]), d.get("reason"),
                   int(d.get("attempts", 0)), d.get("batch_id"), d.get("live_run"),
                   d.get("index"), at("measured_at"), at("written_at"), d.get("tags") or {})


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
                         max(shard, row.shard_id))
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


class MetricDb(Protocol):
    """The metricdb reads and the one write of the rank job."""

    def batch_before(self, until: datetime) -> Batch | None:
        """The newest batch created before `until`: the one Metric measured."""

    def batch_rows(self, batch_id: int) -> list[GoldenRow]:
        """Every `metric_url` row of the batch with a URL."""

    def coverage_rows(self, table: str, first: date, last: date) -> list[CoverageRow]:
        """The rows of a coverage table with `first <= stat_date <= last`."""

    def write(self, ranked: Mapping[int, bool], rows: Sequence[CoverageWrite]) -> None:
        """Sets `is_ranked` and the coverage rows' ranked fields in one transaction."""


@dataclass(frozen=True)
class Ports:
    metric: MetricDb
    live_run: Callable[[], LiveRun | None]
    index_build: Callable[[], IndexBuild | None]
    build: Callable[[], Any]
    search: Callable[[str], list[str]]


@dataclass(frozen=True)
class Evidence:
    batch: Batch | None
    rows: tuple[GoldenRow, ...]
    # (tag, team) -> the table's rows dated G or G+1: a measurement may end past midnight
    coverage: Mapping[tuple[str, str], tuple[CoverageRow, ...]]
    live: LiveRun | None
    build: IndexBuild | None


def _midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def coverage_table(r: RankCfg, tag: str, team: str) -> str:
    return r.coverage_table_fmt.format(tag=tag, team=team)


def observe(cfg: Config, golden_day: date, ports: Ports) -> Evidence:
    next_day = golden_day + timedelta(days=1)
    batch = ports.metric.batch_before(_midnight(next_day))
    rows = tuple(ports.metric.batch_rows(batch.id)) if batch else ()
    coverage = {(tag, team): tuple(ports.metric.coverage_rows(
                    coverage_table(cfg.rank, tag, team), golden_day, next_day))
                for tag in cfg.golden.tags for team in _team_names(cfg.rank)}
    return Evidence(batch, rows, coverage, ports.live_run(), ports.index_build())


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
class MeasureWrite:
    pass


Decision = Idle | Refuse | Build | MeasureWrite


def due_at(golden_day: date, r: RankCfg) -> datetime:
    """G + `delay_days` at `start_time` UTC."""
    return (_midnight(golden_day + timedelta(days=r.delay_days))
            + timedelta(minutes=hhmm_minutes(r.start_time)))


def pick_golden_day(now: datetime, cfg: Config) -> date:
    """The latest golden day whose rank job is due at `now`."""
    now = utc(now)
    first = now.date() - timedelta(days=cfg.rank.delay_days + _MAX_GAP_DAYS)
    picked = None
    for g in golden_days(first, cfg.schedule.swap_freeze_days):
        if due_at(g, cfg.rank) > now:
            break
        picked = g
    assert picked is not None, "the search starts more than one gap before now"
    return picked


def waiting(cfg: Config, now: datetime, record: RankRecord, scheduled: bool) -> Idle | None:
    """Idle if the record is final, or if a scheduled tick comes before the due time."""
    if record.status in TERMINAL:
        return Idle(f"the record of {record.golden_day} is {record.status}")
    due = due_at(record.golden_day, cfg.rank)
    if scheduled and utc(now) < due:
        return Idle(f"due at {due.isoformat()}")
    return None


def refusals(cfg: Config, golden_day: date, ev: Evidence, check_activation: bool) -> list[str]:
    """Why G's coverage rows cannot take a rank; empty if they can.

    The recomputed `total` and `indexed_num` must equal the row's: a mismatch means a
    later measurement rewrote `is_indexed`, or the row is another measurement's.
    """
    if ev.batch is None:
        return [f"no metric batch was created before {golden_day + timedelta(days=1)}"]
    reasons = [f"batch {ev.batch.id} has no {tag} rows" for tag in cfg.golden.tags
               if not any(tag in row.tags for row in ev.rows)]
    freeze_start = window(golden_day, cfg).freeze_start
    if ev.live is None:
        reasons.append("selectdb has no live run")
    elif check_activation and (ev.live.activated_at is None
                               or ev.live.activated_at >= freeze_start):
        reasons.append(f"live run {ev.live.label} was activated at "
                       f"{ev.live.activated_at}, not before {freeze_start.isoformat()}: "
                       "it is not the list measured on G")
    counts = tally(ev.rows, {}, {}, cfg.golden.tags, cfg.rank)
    for (tag, team), rows in ev.coverage.items():
        table = coverage_table(cfg.rank, tag, team)
        if len(rows) != 1:
            reasons.append(f"{table} has {len(rows)} rows dated {golden_day} or the day after")
            continue
        got, want = rows[0], counts[tag].teams[team]
        if (got.total, got.indexed_num) != (want.total, want.indexed_num):
            reasons.append(f"{table} {got.stat_date}: total {got.total} and indexed_num "
                           f"{got.indexed_num}, batch {ev.batch.id} gives {want.total} "
                           f"and {want.indexed_num}")
    return reasons


def fresh(ev: Evidence, since: datetime) -> bool:
    """The alias's index holds the live run's list and was built at or after `since`."""
    return (ev.build is not None and ev.live is not None
            and ev.build.prior_run == ev.live.label and ev.build.started_at >= since)


def plan(cfg: Config, now: datetime, record: RankRecord, ev: Evidence, *,
         scheduled: bool) -> Decision:
    """The next step for `record`.

    `scheduled` ticks wait for the due time and refuse a live run activated after G's
    freeze start; a tick for a named golden day (`--golden-day`) does neither. The index
    must be built after the due time, or after `now` when the due time is still ahead.
    """
    idle = waiting(cfg, now, record, scheduled)
    if idle is not None:
        return idle
    reasons = refusals(cfg, record.golden_day, ev, check_activation=scheduled)
    if reasons:
        return Refuse("; ".join(reasons))
    if not fresh(ev, min(due_at(record.golden_day, cfg.rank), utc(now))):
        return Build()
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
    """The record's per-tag counts; `stat_date` is None where no single row matched."""
    out: dict[str, Any] = {}
    for tag, tc in ranking.tags.items():
        teams = {}
        for team, c in tc.teams.items():
            rows = ev.coverage.get((tag, team), ())
            teams[team] = {"stat_date": rows[0].stat_date.isoformat() if len(rows) == 1 else None,
                           "total": c.total, "indexed_num": c.indexed_num,
                           "ranked_num": c.ranked_num,
                           "ranked_rate": c.ranked_num / c.total if c.total else 0.0}
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


def _measure_write(cfg: Config, record: RankRecord, ev: Evidence, ports: Ports,
                   clock: Clock) -> RankRecord:
    ranking = count_ranks(ev.rows, ports.search, cfg.golden.tags, cfg.rank)
    tags = tag_results(ranking, ev)
    measured_at = clock()
    _audit(cfg, record.golden_day, ev.rows, ranking)
    writes = [CoverageWrite(coverage_table(cfg.rank, tag, team),
                            date.fromisoformat(t["stat_date"]), t["ranked_num"], t["ranked_rate"])
              for tag, entry in tags.items() for team, t in entry["teams"].items()]
    ports.metric.write(ranking.ranked, writes)
    return replace(record, status=Status.DONE, reason=None, measured_at=measured_at,
                   written_at=clock(), tags=tags)


def _step(cfg: Config, now: datetime, record: RankRecord, ports: Ports, clock: Clock,
          scheduled: bool) -> tuple[Decision, RankRecord]:
    ev = observe(cfg, record.golden_day, ports)
    decision = plan(cfg, now, record, ev, scheduled=scheduled)
    if isinstance(decision, Build):
        ports.build()
        ev = replace(ev, build=ports.index_build())
        if isinstance(after := plan(cfg, now, record, ev, scheduled=scheduled), Build):
            raise RankError("the build left no index of the live run's list on the alias")
        decision = after
    record = replace(record, batch_id=ev.batch and ev.batch.id,
                     live_run=ev.live and ev.live.label, index=ev.build and ev.build.index)
    if isinstance(decision, Refuse):
        return decision, replace(record, status=Status.REFUSED, reason=decision.reason)
    if isinstance(decision, MeasureWrite):
        return decision, _measure_write(cfg, record, ev, ports, clock)
    return decision, record


def tick(cfg: Config, ports: Ports, clock: Clock = utcnow,
         golden_day: date | None = None) -> dict[str, Any]:
    """One observe-plan-act round: for the latest due golden day, or for `golden_day`.

    A final record, a tick before the due time and a busy lock idle without reading a
    database. Otherwise the tick refuses, or builds the index if it is not fresh, then
    measures and writes. An exception counts an attempt; at `rank.max_attempts` the
    record is `failed`.
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
    return {"golden_day": g.isoformat(), "due_at": due_at(g, cfg.rank).isoformat(),
            "decision": type(decision).__name__.lower(),
            **({"why": decision.why} if isinstance(decision, Idle) else {}),
            "record": record.to_json()}


def dry_run(cfg: Config, ports: Ports, clock: Clock = utcnow,
            golden_day: date | None = None) -> dict[str, Any]:
    """What a tick would decide, and the counts on the current alias; writes nothing.

    It neither builds nor writes a file or a row. Refusal reasons, the live run's
    activation included, come back as `problems`; the counts are measured anyway.
    """
    now = clock()
    g = pick_golden_day(now, cfg) if golden_day is None else golden_day
    record = read_record(cfg, g)
    ev = observe(cfg, g, ports)
    would = plan(cfg, now, record, ev, scheduled=golden_day is None)
    out: dict[str, Any] = {
        "golden_day": g.isoformat(), "due_at": due_at(g, cfg.rank).isoformat(),
        "would": type(would).__name__.lower(), "record": record.to_json(),
        "problems": refusals(cfg, g, ev, check_activation=True),
        "batch_id": ev.batch and ev.batch.id, "live_run": ev.live and ev.live.label,
        "index": ev.build and ev.build.index,
        "index_fresh": fresh(ev, min(due_at(g, cfg.rank), utc(now))),
    }
    if ev.rows:
        out["measured"] = tag_results(count_ranks(ev.rows, ports.search, cfg.golden.tags,
                                                  cfg.rank), ev)
    return out


# ---------------------------------------------------------------- real ports


_BATCH_SQL = ("SELECT id, created_at FROM metric_batches WHERE created_at < %s "
              "ORDER BY created_at DESC, id DESC LIMIT 1")
_ROWS_SQL = ("SELECT u.id, u.query_id, q.keyword, q.tags, u.url, u.is_indexed, u.shard_id "
             "FROM metric_url u JOIN metric_queries q ON q.id = u.query_id "
             "WHERE q.batch_id = %s AND u.url IS NOT NULL ORDER BY u.id")
_RANKED_SQL = ("UPDATE metric_url AS m SET is_ranked = u.r "
               "FROM unnest(%s::bigint[], %s::boolean[]) AS u(id, r) WHERE m.id = u.id")


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
        return [GoldenRow(int(i), int(q), kw, tuple(tags or ()), url, bool(indexed),
                          -1 if shard is None else int(shard))
                for i, q, kw, tags, url, indexed, shard
                in self._read().fetchall(_ROWS_SQL, (batch_id,))]

    def coverage_rows(self, table: str, first: date, last: date) -> list[CoverageRow]:
        from psycopg import sql

        query = sql.SQL("SELECT stat_date, total, indexed_num FROM {} "
                        "WHERE stat_date BETWEEN %s AND %s ORDER BY stat_date"
                        ).format(sql.Identifier(table))
        return [CoverageRow(*r) for r in self._read().fetchall(query, (first, last))]

    def write(self, ranked: Mapping[int, bool], rows: Sequence[CoverageWrite]) -> None:
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


def index_build(cfg: Config, client: Any) -> IndexBuild | None:
    """The alias's build, from its build record; None if there is no single one."""
    from isel.textindex import alias_build

    out = alias_build(cfg, client)
    return out and IndexBuild(out["index"], out.get("prior_run"), out["started_at"])


@contextmanager
def real_ports(cfg: Config) -> Iterator[Ports]:
    from isel import textindex

    t = cfg.text_index
    client = textindex.OpenSearch(t.url, t.timeout_s, t.retries, t.backoff_s)
    metric = PgMetricDb(cfg)
    try:
        yield Ports(metric=metric, live_run=lambda: read_live_run(cfg),
                    index_build=lambda: index_build(cfg, client),
                    build=lambda: textindex.build(cfg, client, Path(cfg.recrawl.text_root)),
                    search=lambda kw: textindex.search(client, t, kw)[0])
    finally:
        metric.close()
