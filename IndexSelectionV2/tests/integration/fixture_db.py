"""Local stand-ins for the production databases, with the production schemas.

- "crawler server": crawlerdb (url_state_current_NNN, partitioned
  url_state_history_NNN, domain_state, shard_split_subdomain) and metricdb
  (metric_batches, metric_queries, metric_url). The `crawler` role is a
  SUPERUSER, exactly like production.
- "selectdb server": our own selectdb with wal_level=minimal.
"""

from __future__ import annotations

import hashlib
import random
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

URL_STATE_DDL = """
CREATE TABLE {t} (
    url varchar NOT NULL,
    domain_id bigint NOT NULL,
    first_seen timestamptz DEFAULT now(),
    last_scheduled timestamptz,
    last_fetch_ok timestamptz,
    last_content_update timestamptz,
    num_scheduled_90d integer DEFAULT 0,
    num_fetch_ok_90d integer DEFAULT 0,
    num_fetch_fail_90d integer DEFAULT 0,
    num_content_update_90d integer DEFAULT 0,
    num_consecutive_fail integer DEFAULT 0,
    last_fail_reason varchar,
    content_hash varchar,
    should_crawl boolean DEFAULT true,
    url_score double precision DEFAULT 0,
    domain_score double precision DEFAULT 0,
    source smallint NOT NULL DEFAULT 0,
    discovered_from varchar,
    title varchar,
    last_modified timestamptz,
    etag varchar,
    cache_control varchar,
    is_redirect boolean,
    redirect_hop_count smallint,
    discovery_source_type smallint NOT NULL DEFAULT 0,
    parent_page_score double precision,
    inlink_count_approx integer NOT NULL DEFAULT 0,
    inlink_count_external integer NOT NULL DEFAULT 0,
    anchor_text varchar,
    robots_bits smallint NOT NULL DEFAULT 0,
    hreflang_count integer,
    url_score_updated_at timestamptz,
    has_json_ld boolean
) WITH (fillfactor = 80)
"""

STATE_COLUMNS = (
    "url", "domain_id", "first_seen", "last_fetch_ok", "num_fetch_ok_90d",
    "num_consecutive_fail", "content_hash", "should_crawl", "url_score", "source", "title",
    "is_redirect", "redirect_hop_count", "discovery_source_type", "parent_page_score",
    "inlink_count_approx", "inlink_count_external", "anchor_text", "robots_bits",
    "hreflang_count", "has_json_ld",
)

METRIC_DDL = """
CREATE TABLE metric_batches (id bigserial PRIMARY KEY, created_at timestamp,
  meta_total_queries integer DEFAULT 0, meta_total_urls integer DEFAULT 0,
  meta_tag_stats jsonb DEFAULT '{}', meta_geo_counts jsonb DEFAULT '{}');
CREATE TABLE metric_queries (id bigserial PRIMARY KEY, batch_id bigint REFERENCES metric_batches(id),
  keyword varchar NOT NULL, geo jsonb DEFAULT '[]', frequency integer DEFAULT 0, tags jsonb DEFAULT '[]');
CREATE TABLE metric_url (id bigserial PRIMARY KEY, query_id bigint REFERENCES metric_queries(id),
  url text, rank integer, is_discovered boolean DEFAULT false, is_crawled boolean DEFAULT false,
  is_indexed boolean DEFAULT false, is_ranked boolean DEFAULT false, shard_id integer);
"""

# The 17 columns the crawler's accounting job copies into history
# (WebCrawler containers/scheduler_control/accounting/service.py); every other
# history column takes its default.
ACCOUNTING_COLUMNS = (
    "url", "domain_id", "first_seen", "last_scheduled", "last_fetch_ok", "last_content_update",
    "num_scheduled_90d", "num_fetch_ok_90d", "num_fetch_fail_90d", "num_content_update_90d",
    "num_consecutive_fail", "last_fail_reason", "content_hash", "should_crawl", "url_score",
    "url_score_updated_at", "domain_score",
)


def docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    done = subprocess.run(["docker", "image", "inspect", "postgres:16"], capture_output=True)
    return done.returncode == 0


@dataclass
class PgContainer:
    name: str
    port: int
    password: str

    def dsn(self, user: str = "postgres", password: str | None = None, db: str = "postgres") -> str:
        pw = self.password if password is None else password
        return f"postgresql://{user}:{pw}@127.0.0.1:{self.port}/{db}"

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


def start_postgres(extra_args: list[str]) -> PgContainer:
    name = f"idxsel-test-{uuid.uuid4().hex[:8]}"
    password = uuid.uuid4().hex
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-e", f"POSTGRES_PASSWORD={password}",
         "-p", "127.0.0.1::5432", "postgres:16", *extra_args],
        check=True, capture_output=True)
    port_line = subprocess.run(["docker", "port", name, "5432/tcp"], check=True,
                               capture_output=True, text=True).stdout.splitlines()[0]
    pg = PgContainer(name, int(port_line.rsplit(":", 1)[1]), password)
    deadline = time.monotonic() + 60
    while True:
        try:
            with psycopg.connect(pg.dsn(), connect_timeout=2) as conn:
                conn.execute("SELECT 1")
            # The image restarts once after initdb; wait until it is stable.
            time.sleep(1.0)
            with psycopg.connect(pg.dsn(), connect_timeout=2) as conn:
                conn.execute("SELECT 1")
            return pg
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                pg.stop()
                raise
            time.sleep(0.5)


@dataclass
class Universe:
    """Synthetic crawl: domains, URL rows per shard, golden batches."""

    num_shards: int
    domains: list[tuple[int, str, int]] = field(default_factory=list)  # (id, key, shard)
    rows: dict[str, dict] = field(default_factory=dict)  # url -> row
    golden: list[tuple[int, datetime, list[tuple[str, list[str], list[str]]]]] = field(
        default_factory=list)  # (batch_id, created_at, [(keyword, tags, urls)])


def shard_of(key: str, num_shards: int) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % num_shards


def build_universe(num_shards: int, urls_per_domain: int, seed: int) -> Universe:
    rng = random.Random(seed)
    u = Universe(num_shards)
    hosts = ["en.wikipedia.org", "wikipedia.org", "news.example.com", "example.com"] + [
        f"site{i}.com" for i in range(36)]
    keys = {"en.wikipedia.org": "en.wikipedia.org", "news.example.com": "example.com"}
    seen_keys: dict[str, int] = {}
    for host in hosts:
        key = keys.get(host, host)
        if key not in seen_keys:
            seen_keys[key] = len(seen_keys) + 1
            u.domains.append((seen_keys[key], key, shard_of(key, num_shards)))
    by_key = {k: (i, s) for i, k, s in u.domains}
    base_time = datetime(2026, 9, 1, tzinfo=UTC)
    for host in hosts:
        dom_id, _ = by_key[keys.get(host, host)]
        for j in range(urls_per_domain):
            url = f"https://{host}/p/{j}"
            crawled = rng.random() < 0.45
            row = {
                "url": url, "domain_id": dom_id,
                "first_seen": base_time - timedelta(days=rng.randrange(200)),
                "last_fetch_ok": (base_time + timedelta(hours=rng.randrange(600))) if crawled
                else None,
                "num_fetch_ok_90d": rng.randrange(1, 4) if crawled else 0,
                "num_consecutive_fail": 0,
                "content_hash": hashlib.sha1(url.encode()).hexdigest() if crawled else None,
                "should_crawl": not crawled,
                "url_score": round(rng.random(), 4),
                "source": 0,
                "title": None if rng.random() < 0.3 else f"title {j}",
                "is_redirect": rng.choice([None, False, False, True]),
                "redirect_hop_count": None,
                "discovery_source_type": 1,
                "parent_page_score": None if rng.random() < 0.3 else round(rng.random(), 3),
                "inlink_count_approx": int(rng.paretovariate(1.2)) - 1,
                "inlink_count_external": int(rng.paretovariate(2.0)) - 1,
                "anchor_text": None if rng.random() < 0.3 else "anchor",
                "robots_bits": rng.choice([0, 1, 1, 1]),
                "hreflang_count": None,
                "has_json_ld": rng.choice([None, True, False]),
            }
            u.rows[url] = row
    all_urls = list(u.rows)
    for batch_id, created in ((1, datetime(2026, 5, 1)), (2, datetime(2026, 6, 5)),
                              (3, datetime(2026, 9, 17))):
        queries = []
        for q in range(30):
            tags = ["head"] if q % 2 == 0 else ["random"]
            picks = rng.sample(all_urls, 6)
            urls = []
            for p in picks:
                r = rng.random()
                if r < 0.15:
                    urls.append(p.replace("https://", "http://"))  # spelling mismatch
                elif r < 0.25:
                    urls.append(f"https://unknown{rng.randrange(99)}.org/x")  # undiscovered
                else:
                    urls.append(p)
            queries.append((f"kw{batch_id}-{q}", tags, urls))
        u.golden.append((batch_id, created, queries))
    return u


def _insert_rows(cur: psycopg.Cursor, table: str, rows: list[dict]) -> None:
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in STATE_COLUMNS)
    with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(sql.Identifier(table), cols)) as cp:
        for r in rows:
            cp.write_row([r[c] for c in STATE_COLUMNS])


def seed_crawler_server(pg: PgContainer, u: Universe) -> None:
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        conn.execute("CREATE ROLE crawler SUPERUSER LOGIN PASSWORD 'crawler'")
        conn.execute("CREATE ROLE metric LOGIN PASSWORD 'metric'")
        for db, owner in (("crawlerdb", "crawler"), ("metricdb", "metric")):
            conn.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(db), sql.Identifier(owner)))
    with psycopg.connect(pg.dsn(user="crawler", password="crawler", db="crawlerdb"),
                         autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("CREATE TABLE domain_state (domain_id bigint PRIMARY KEY, domain varchar "
                    "NOT NULL UNIQUE, shard_id integer NOT NULL, domain_score double precision "
                    "DEFAULT 0)")
        cur.execute("CREATE TABLE shard_split_subdomain (host varchar PRIMARY KEY, "
                    "migrated_at timestamptz)")
        cur.execute("INSERT INTO shard_split_subdomain (host) VALUES ('en.wikipedia.org')")
        with cur.copy("COPY domain_state (domain_id, domain, shard_id) FROM STDIN") as cp:
            for row in u.domains:
                cp.write_row(row)
        shard_by_domain = {i: s for i, _, s in u.domains}
        for shard in range(u.num_shards):
            t = f"url_state_current_{shard:03d}"
            h = f"url_state_history_{shard:03d}"
            cur.execute(URL_STATE_DDL.format(t=t))
            cur.execute(f"ALTER TABLE {t} ADD PRIMARY KEY (url)")
            cur.execute(f"CREATE INDEX idx_{t}_domain ON {t} (domain_id)")
            cur.execute(f"CREATE SEQUENCE {h}_snapshot_id_seq")
            cur.execute(f"CREATE TABLE {h} (LIKE {t} INCLUDING DEFAULTS, snapshot_id bigint "
                        f"NOT NULL DEFAULT nextval('{h}_snapshot_id_seq'), snapshot_at "
                        f"timestamptz NOT NULL DEFAULT now()) PARTITION BY RANGE (snapshot_at)")
            cur.execute(f"ALTER TABLE {h} ADD PRIMARY KEY (snapshot_id, snapshot_at)")
            for month in ("2026-08-01", "2026-09-01", "2026-10-01", "2026-11-01"):
                start = datetime.fromisoformat(month).replace(tzinfo=UTC)
                end = (start + timedelta(days=32)).replace(day=1)
                cur.execute(f"CREATE TABLE {h}_p{start:%Y%m} PARTITION OF {h} FOR VALUES "
                            f"FROM ('{start.isoformat()}') TO ('{end.isoformat()}')")
            rows = [r for r in u.rows.values() if shard_by_domain[r["domain_id"]] == shard]
            _insert_rows(cur, t, rows)
            # History mirrors one fetch per crawled row, written in September.
            hist = [dict(r) for r in rows if r["last_fetch_ok"] is not None]
            if hist:
                cols = sql.SQL(", ").join(sql.Identifier(c) for c in (*STATE_COLUMNS,
                                                                       "snapshot_at"))
                with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(
                        sql.Identifier(h), cols)) as cp:
                    for r in hist:
                        cp.write_row([r[c] for c in STATE_COLUMNS]
                                     + [datetime(2026, 9, 20, tzinfo=UTC)])
            cur.execute(f"ANALYZE {t}")
    with psycopg.connect(pg.dsn(user="metric", password="metric", db="metricdb"),
                         autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(METRIC_DDL)
        for batch_id, created, queries in u.golden:
            cur.execute("INSERT INTO metric_batches (id, created_at) VALUES (%s, %s)",
                        (batch_id, created))
            for keyword, tags, urls in queries:
                cur.execute("INSERT INTO metric_queries (batch_id, keyword, tags) VALUES "
                            "(%s, %s, %s) RETURNING id", (batch_id, keyword, Jsonb(tags)))
                (qid,) = cur.fetchone()
                for rank, url in enumerate(urls, 1):
                    cur.execute("INSERT INTO metric_url (query_id, url, rank) VALUES (%s, %s, %s)",
                                (qid, url, rank))


def write_accounting_rows(conn: psycopg.Connection, shard: int, urls: list[str]) -> None:
    """Appends history rows exactly like the crawler's daily accounting job."""
    cols = ", ".join(ACCOUNTING_COLUMNS)
    conn.execute(
        f"INSERT INTO url_state_history_{shard:03d} ({cols}) SELECT {cols} "
        f"FROM url_state_current_{shard:03d} WHERE url = ANY(%s)", (urls,))


def start_selectdb() -> PgContainer:
    pg = start_postgres(["-c", "wal_level=minimal", "-c", "max_wal_senders=0",
                         "-c", "synchronous_commit=off"])
    with psycopg.connect(pg.dsn(), autocommit=True) as conn:
        # Mirrors POSTGRES_USER of our own selectdb container, which runs init-selectdb.
        conn.execute("CREATE ROLE idxsel_admin LOGIN CREATEROLE PASSWORD 'adminpw'")
        conn.execute("CREATE DATABASE selectdb OWNER idxsel_admin")
    return pg


def add_batch(metric_dsn: str, batch_id: int, created: datetime,
              queries: list[tuple[str, list[str], list[str]]]) -> None:
    """Adds a golden batch the way the metric's `--create` run does (naive UTC time)."""
    with psycopg.connect(metric_dsn, autocommit=True) as conn:
        conn.execute("INSERT INTO metric_batches (id, created_at) VALUES (%s, %s)",
                     (batch_id, created.astimezone(UTC).replace(tzinfo=None)))
        for keyword, tags, urls in queries:
            (qid,) = conn.execute(
                "INSERT INTO metric_queries (batch_id, keyword, tags) VALUES (%s, %s, %s) "
                "RETURNING id", (batch_id, keyword, Jsonb(tags))).fetchone()
            for rank, url in enumerate(urls, 1):
                conn.execute("INSERT INTO metric_url (query_id, url, rank) VALUES (%s, %s, %s)",
                             (qid, url, rank))


def drop_batch(metric_dsn: str, batch_id: int) -> None:
    with psycopg.connect(metric_dsn, autocommit=True) as conn:
        conn.execute("DELETE FROM metric_url WHERE query_id IN "
                     "(SELECT id FROM metric_queries WHERE batch_id = %s)", (batch_id,))
        conn.execute("DELETE FROM metric_queries WHERE batch_id = %s", (batch_id,))
        conn.execute("DELETE FROM metric_batches WHERE id = %s", (batch_id,))
