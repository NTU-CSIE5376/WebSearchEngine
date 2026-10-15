"""Point-in-time features for every (URL, batch) pair, computed in DuckDB.

Leakage rules (see docs/DESIGN.md):

* The pool of batch b is the crawled rows with `last_fetch_ok < as_of_b`, where
  `as_of_b = min(created_b - lead_h, snapshot_at)`. Each URL is fetched once,
  so this rebuilds the crawl as it stood before the batch existed.
* Golden-derived features of batch b only use batches created before b whose
  rows were available (`available_at`) by `as_of_b`, decayed by age. Pseudo
  batches are fetched after the official batch they extend, so they only reach
  batches whose as-of time is later than their fetch time.
* Columns the crawler derives from golden labels are in `FORBIDDEN` and can
  never enter a feature set.
* A row's features never depend on whether the sample holds it. Domain and
  lift statistics come from the sample's aggregate half; a row outside it adds
  itself, so it sees what it would see inside. No training row comes from that
  half: its rows share a group value that no other row can reach, which told
  the sampled negatives from the positives.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa

from isel.config import Config
from isel.golden import OFFICIAL, golden_files
from isel.io import open_duckdb, read_json, sql_str
from isel.urlnorm import HOST_RE, DomainKeyer

PATH_RE = r"^[A-Za-z][A-Za-z0-9+.-]*://[^/?#]*([^?#]*)"
# DuckDB threads add a group's rows in no fixed order. DOUBLE sums then differ in
# the last bits from run to run, while DECIMAL sums are exact (|x| < 1e20).
EXACT = "DECIMAL(38, 18)"


@dataclass(frozen=True)
class Feature:
    name: str
    group: str
    categorical: bool = False


FEATURES: dict[str, Feature] = {f.name: f for f in (
    Feature("url_len", "static"),
    Feature("path_depth", "static"),
    Feature("is_root", "static"),
    Feature("has_query", "static"),
    Feature("n_params", "static"),
    Feature("path_long_number", "static"),
    Feature("path_date", "static"),
    Feature("path_marker", "static", categorical=True),
    Feature("ext_class", "static", categorical=True),
    Feature("is_https", "static"),
    Feature("sub_www", "static"),
    Feature("sub_mobile", "static"),
    Feature("sub_amp", "static"),
    Feature("host_labels", "static"),
    Feature("title_null", "fetch"),
    Feature("title_len", "fetch"),
    Feature("has_anchor", "fetch"),
    Feature("discovery_source_type", "fetch", categorical=True),
    Feature("robots_bits", "fetch", categorical=True),
    Feature("is_redirect", "fetch"),
    Feature("redirect_hop_count", "fetch"),
    Feature("has_json_ld", "fetch"),
    Feature("hreflang_count", "fetch"),
    Feature("has_last_modified", "fetch"),
    Feature("has_cache_control", "fetch"),
    Feature("fetch_age_days", "fetch"),
    Feature("log_inlinks", "cumulative"),
    Feature("log_ext", "cumulative"),
    Feature("has_ext", "cumulative"),
    Feature("rel_inlinks", "cumulative"),
    Feature("rel_ext", "cumulative"),
    Feature("hist_decayed", "history"),
    Feature("hist_head", "history"),
    Feature("hist_random", "history"),
    Feature("hist_days_since", "history"),
    Feature("lift_tld", "domain"),
    Feature("lift_dkey", "domain"),
    Feature("lift_host", "domain"),
    Feature("lift_hp", "domain"),
    Feature("dem_dkey", "domain"),
    Feature("dem_host", "domain"),
    Feature("log_size_dkey", "domain"),
    Feature("log_size_host", "domain"),
    Feature("dkey_ext_share", "domain"),
    Feature("history_batches", "domain"),
)}

# Derived by the crawler from golden labels, or rolling windows relative to now.
FORBIDDEN = frozenset({
    "source", "url_score", "url_score_updated_at", "domain_score", "discovered_from",
    "should_crawl", "last_scheduled", "first_seen", "parent_page_score",
    "num_scheduled_90d", "num_fetch_ok_90d", "num_fetch_fail_90d", "num_content_update_90d",
    "num_consecutive_fail", "last_content_update",
})
ID_COLUMNS = ("url", "shard", "h", "dkey", "is_sample", "in_agg", "label", "in_head", "in_random",
              "url_score", "source")
# The crawl columns the full export keeps and the store loads, in export order.
ROW_COLUMNS = (
    "url", "domain_id", "last_fetch_ok", "title_len", "has_anchor", "discovery_source_type",
    "robots_bits", "is_redirect", "redirect_hop_count", "has_json_ld", "hreflang_count",
    "has_last_modified", "has_cache_control", "inlink_count_approx", "inlink_count_external",
    "url_score", "source", "shard",
)
SAMPLE_METHOD = "hash"
LEVELS = ("tld", "dkey", "host", "hp")


class LeakageError(ValueError):
    """Raised when a feature list contains a forbidden or unknown column."""


def resolve_set(cfg: Config, name: str) -> list[str]:
    """Returns the feature names of set `name`, rejecting forbidden or unknown ones."""
    if name not in cfg.features.sets:
        raise LeakageError(f"unknown feature set {name!r}")
    names = list(cfg.features.sets[name])
    assert_clean(names)
    return names


def assert_clean(names: list[str]) -> None:
    bad = sorted(set(names) & FORBIDDEN)
    if bad:
        raise LeakageError(f"forbidden columns in feature list: {bad}")
    unknown = sorted(set(names) - set(FEATURES))
    if unknown:
        raise LeakageError(f"unregistered features: {unknown}")


@dataclass(frozen=True)
class Batch:
    batch_id: int
    created_at: datetime
    as_of: datetime
    available_at: datetime
    official: bool


def _ts(value: datetime) -> str:
    return f"{sql_str(value.isoformat())}::TIMESTAMPTZ"


def _exact_sum(expr: str) -> str:
    return f"sum(({expr})::{EXACT})::DOUBLE"


class FeatureStore:
    """Holds the crawl rows, golden set and per-batch aggregates in one DuckDB."""

    def __init__(self, cfg: Config, con: duckdb.DuckDBPyConnection, work_dir: Path,
                 next_batch: bool = False) -> None:
        self.cfg = cfg
        self.con = con
        self.work_dir = work_dir
        meta = read_json(work_dir / "sample" / "_meta.json")
        if not meta or "finished_at" not in meta:
            raise RuntimeError("sample/_meta.json is missing or unfinished; run derive-sample")
        if meta.get("method") != SAMPLE_METHOD:
            raise RuntimeError("the sample was not drawn row by row from the full export; "
                               "move it away and run derive-sample")
        positives = read_json(work_dir / "positives" / "_meta.json") or {}
        if positives.get("export_started_at") != meta["export_started_at"]:
            raise RuntimeError("positives/ did not come from the sample's export; "
                               "move it away and run derive-positives")
        # A model fits the sample it was trained on; a new sample means a new model.
        self.fingerprint = {key: meta[key] for key in
                            ("method", "sample_percent", "sample_salt", "agg_share", "agg_salt",
                             "export_started_at")}
        sampled = float(meta["sample_percent"]) / 100.0
        # The aggregate half feeds the domain statistics; the rest is the pool sample.
        self.agg_rate = sampled * float(meta["agg_share"])
        self.rate = sampled - self.agg_rate
        self.snapshot_at = datetime.fromisoformat(meta["snapshot_at"])
        self.batches: list[Batch] = []
        self._keyer: DomainKeyer | None = None
        self._prepare(next_batch)

    def _files(self, pattern: str) -> list[str]:
        return sorted(str(p) for p in self.work_dir.glob(pattern))

    def _prepare(self, next_batch: bool) -> None:
        con, cfg = self.con, self.cfg
        sample = self._files("sample/shard=*.parquet")
        if not sample:
            raise RuntimeError("no sample files; run derive-sample")
        cols = ", ".join(ROW_COLUMNS)
        parts = [f"SELECT {cols}, NOT in_agg AS is_sample, in_agg FROM read_parquet({sql_str(f)})"
                 for f in sample]
        parts += [f"SELECT {cols}, false AS is_sample, false AS in_agg "
                  f"FROM read_parquet({sql_str(f)})"
                  for f in self._files("positives/batch=*.parquet")]
        con.execute(
            "CREATE OR REPLACE TEMP TABLE crawl0 AS SELECT *, "
            f"lower(regexp_extract(url, {sql_str(HOST_RE)}, 1)) AS host, "
            f"regexp_extract(url, {sql_str(PATH_RE)}, 1) AS path, md5_number_lower(url) AS h "
            f"FROM ({' UNION ALL BY NAME '.join(parts)}) "
            "QUALIFY row_number() OVER (PARTITION BY url, shard "
            "ORDER BY is_sample OR in_agg DESC) = 1")
        files = golden_files(self.work_dir, cfg.golden.use_pseudo)
        golden = f"[{', '.join(sql_str(str(f)) for f in files)}]"
        excluded = list(cfg.golden.exclude_batches) or [-1]
        rows = con.execute(
            f"SELECT batch_id, min(batch_created_at), max(available_at), "
            f"bool_and(label_source = {sql_str(OFFICIAL)}), count(DISTINCT label_source) "
            f"FROM read_parquet({golden}) WHERE batch_id NOT IN ({', '.join(map(str, excluded))}) "
            "GROUP BY batch_id ORDER BY batch_id").fetchall()
        mixed = [int(r[0]) for r in rows if r[4] > 1]
        if mixed:
            raise RuntimeError(f"batch ids {mixed} hold both official and pseudo rows; "
                               "raise pseudo.id_offset")
        lead = timedelta(hours=cfg.golden.lead_h)
        self.batches = [Batch(int(b), t, min(t - lead, self.snapshot_at), a, bool(o))
                        for b, t, a, o, _ in rows]
        missing = [b.batch_id for b in self.batches
                   if not (self.work_dir / "positives" / f"batch={b.batch_id}.parquet").exists()]
        if missing:
            raise RuntimeError(f"positives are missing for batches {missing}; run derive-positives")
        if next_batch:
            official = [b.batch_id for b in self.batches if b.official]
            t = self.snapshot_at + lead
            self.batches.append(Batch(max(official) + 1 if official else 1, t, self.snapshot_at,
                                      t, True))
        con.execute("CREATE OR REPLACE TEMP TABLE batches (batch_id BIGINT, "
                    "created_at TIMESTAMPTZ, as_of TIMESTAMPTZ, available_at TIMESTAMPTZ)")
        con.executemany("INSERT INTO batches VALUES (?, ?, ?, ?)",
                        [(b.batch_id, b.created_at, b.as_of, b.available_at)
                         for b in self.batches])
        con.execute(
            "CREATE OR REPLACE TEMP TABLE gold0 AS SELECT g.batch_id, g.query_id, g.raw_url, "
            "g.canonical_url, list_contains(g.tags, 'head') AS in_head, "
            "list_contains(g.tags, 'random') AS in_random, "
            f"lower(regexp_extract(g.canonical_url, {sql_str(HOST_RE)}, 1)) AS host, "
            f"regexp_extract(g.canonical_url, {sql_str(PATH_RE)}, 1) AS path "
            f"FROM read_parquet({golden}) g SEMI JOIN batches b USING (batch_id)")
        self._host_keys("SELECT DISTINCT host FROM crawl0 UNION SELECT DISTINCT host FROM gold0")
        self._key_columns("crawl", "crawl0")
        self._key_columns("gold", "gold0")
        con.execute("CREATE OR REPLACE TEMP TABLE sample_keys AS "
                    "SELECT DISTINCT url, shard, in_agg FROM crawl WHERE is_sample OR in_agg")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE spell AS SELECT batch_id, url, canonical_url, "
            "bool_or(in_head) AS in_head, bool_or(in_random) AS in_random FROM ("
            "SELECT batch_id, raw_url AS url, canonical_url, in_head, in_random FROM gold "
            "UNION ALL SELECT batch_id, canonical_url, canonical_url, in_head, in_random FROM gold"
            ") GROUP BY ALL")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE label_urls AS SELECT batch_id, url, "
            "bool_or(in_head) AS in_head, bool_or(in_random) AS in_random FROM spell GROUP BY ALL")
        half_life = float(cfg.features.half_life_days)
        con.execute(
            "CREATE OR REPLACE TEMP TABLE pairs AS SELECT s.batch_id AS src, d.batch_id AS dst, "
            f"pow(0.5, date_diff('second', s.created_at, d.as_of) / 86400.0 / {half_life}) AS w "
            "FROM batches s JOIN batches d "
            "ON s.created_at < d.created_at AND s.available_at <= d.as_of")
        self._aggregates()

    def _domain_keyer(self) -> DomainKeyer:
        if self._keyer is None:
            split = self.work_dir / "dims" / "split_hosts.parquet"
            hosts = [] if not split.exists() else self.con.execute(
                f"SELECT host FROM read_parquet({sql_str(str(split))})").fetchall()
            self._keyer = DomainKeyer([h for (h,) in hosts],
                                      offline=self.cfg.routing.tldextract_offline)
        return self._keyer

    def _host_keys(self, hosts_sql: str) -> None:
        keyer = self._domain_keyer()
        distinct = [h for (h,) in self.con.execute(hosts_sql).fetchall()]
        keys = [keyer.key_for_host(h or "") for h in distinct]
        # Typed, so an empty export file still joins with the VARCHAR aggregate keys.
        table = pa.table({"host": pa.array(distinct, pa.string()),
                          "dkey": pa.array(keys, pa.string())})
        self.con.register("host_keys_arrow", table)
        self.con.execute("CREATE OR REPLACE TEMP TABLE host_keys AS SELECT * FROM host_keys_arrow")
        self.con.unregister("host_keys_arrow")

    def _key_columns(self, name: str, source: str) -> None:
        """Moves `source` into `name` with the key columns; `source` is dropped to free memory."""
        self.con.execute(
            f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT s.*, k.dkey, "
            "s.host || '/' || regexp_extract(s.path, '^/([^/]*)', 1) AS hp, "
            "regexp_extract(s.host, '([^.]+)$', 1) AS tld "
            f"FROM {source} s LEFT JOIN host_keys k USING (host)")
        self.con.execute(f"DROP TABLE {source}")

    def _aggregates(self) -> None:
        """Per-level exposure, hits and demand, decayed into each destination batch."""
        con = self.con
        con.execute(
            "CREATE OR REPLACE TEMP TABLE matches AS SELECT s.batch_id, s.canonical_url, "
            "c.host, c.hp, c.dkey, c.tld FROM spell s JOIN crawl c ON c.url = s.url "
            "JOIN batches b ON b.batch_id = s.batch_id WHERE c.last_fetch_ok < b.as_of")
        for lvl in LEVELS:
            extra = (f", sum(ln(1 + c.inlink_count_approx)::{EXACT}) AS sum_li, "
                     f"sum(ln(1 + c.inlink_count_external)::{EXACT}) AS sum_le, "
                     "count(*) FILTER (WHERE c.inlink_count_external > 0) AS n_ext"
                     if lvl == "dkey" else "")
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE expo_{lvl} AS SELECT c.{lvl} AS k, b.batch_id, "
                f"count(*) AS n{extra} FROM crawl c JOIN batches b ON c.last_fetch_ok < b.as_of "
                "WHERE c.in_agg GROUP BY ALL")
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE hits_{lvl} AS SELECT {lvl} AS k, batch_id, "
                "count(DISTINCT canonical_url) AS g FROM matches GROUP BY ALL")
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE lvl_{lvl} AS SELECT k, dst AS batch_id, "
                f"sum(w::{EXACT} * n) AS ex, {_exact_sum('w * g')} AS g FROM ("
                f"SELECT e.k, p.dst, p.w, e.n, 0 AS g FROM expo_{lvl} e "
                "JOIN pairs p ON p.src = e.batch_id UNION ALL "
                f"SELECT h.k, p.dst, p.w, 0, h.g FROM hits_{lvl} h "
                "JOIN pairs p ON p.src = h.batch_id) GROUP BY ALL")
        for lvl in ("dkey", "host"):
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE dem_{lvl} AS SELECT d.k, p.dst AS batch_id, "
                f"{_exact_sum('p.w * d.n')} AS dem FROM (SELECT {lvl} AS k, batch_id, "
                "count(DISTINCT canonical_url) AS n FROM gold GROUP BY ALL) d "
                "JOIN pairs p ON p.src = d.batch_id GROUP BY ALL")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE glob_stats AS SELECT p.dst AS batch_id, "
            f"sum(p.w::{EXACT} * coalesce(x.n, 0)) AS ex, "
            f"{_exact_sum('p.w * coalesce(m.g, 0)')} AS g, "
            "count(*) AS hb FROM pairs p "
            "LEFT JOIN (SELECT batch_id, sum(n) AS n FROM expo_tld GROUP BY ALL) x "
            "ON x.batch_id = p.src "
            "LEFT JOIN (SELECT batch_id, count(DISTINCT canonical_url) AS g FROM matches "
            "GROUP BY ALL) m ON m.batch_id = p.src GROUP BY ALL")
        con.execute(
            "CREATE OR REPLACE TEMP TABLE uhist AS SELECT l.url, p.dst AS batch_id, "
            f"{_exact_sum('p.w')} AS hist_decayed, max(l.in_head::INT) AS hist_head, "
            "max(l.in_random::INT) AS hist_random, "
            "min(date_diff('second', s.created_at, d.as_of)) / 86400.0 AS hist_days_since "
            "FROM label_urls l JOIN pairs p ON p.src = l.batch_id "
            "JOIN batches s ON s.batch_id = p.src JOIN batches d ON d.batch_id = p.dst "
            "GROUP BY ALL")

    def load_chunk(self, path: Path) -> None:
        """Replaces the crawl rows with one full-export file and keeps the sample aggregates.

        The aggregate tables were built from the sample's aggregate half in
        `_prepare`, as for training. The sample files mark each row of the file
        as an aggregate row, a pool sample row or neither, and `frame_sql` counts
        every row outside the aggregate half into its own domain statistics, so
        each row gets the features it would get in training.
        """
        # The file sits under a shard=NNN/ directory; its own shard column is the truth.
        source = f"read_parquet({sql_str(str(path))}, hive_partitioning = false)"
        self.con.execute(
            "CREATE OR REPLACE TEMP TABLE crawl0 AS SELECT e.*, "
            "k.url IS NOT NULL AND NOT k.in_agg AS is_sample, coalesce(k.in_agg, false) AS in_agg, "
            f"lower(regexp_extract(e.url, {sql_str(HOST_RE)}, 1)) AS host, "
            f"regexp_extract(e.url, {sql_str(PATH_RE)}, 1) AS path, "
            f"md5_number_lower(e.url) AS h FROM {source} e "
            "LEFT JOIN (SELECT url, shard, in_agg FROM sample_keys "
            f"WHERE shard IN (SELECT DISTINCT shard FROM {source})) k "
            "ON k.url = e.url AND k.shard = e.shard")
        self._host_keys("SELECT DISTINCT host FROM crawl0")
        self._key_columns("crawl", "crawl0")

    def batch(self, batch_id: int) -> Batch:
        for b in self.batches:
            if b.batch_id == batch_id:
                return b
        raise KeyError(f"batch {batch_id} is not in the golden set")

    def _static_sql(self) -> str:
        f = self.cfg.features
        markers = " ".join(f"WHEN contains(c.path, {sql_str(m)}) THEN {i + 1}"
                           for i, m in enumerate(f.path_markers))
        marker_sql = f"CASE {markers} ELSE 0 END" if markers else "0"
        ext_cases = " ".join(f"WHEN {sql_str(k)} THEN {int(v)}"
                             for k, v in sorted(f.ext_classes.items()))
        ext = "regexp_extract(lower(c.path), '\\.([a-z0-9]{1,5})$', 1)"
        ext_other = max(f.ext_classes.values(), default=0) + 1
        query = "regexp_extract(c.url, '\\?([^#]*)', 1)"
        return (
            f"least(length(c.url), {int(f.url_len_cap)}) AS url_len, "
            "len(list_filter(string_split(c.path, '/'), x -> x <> '')) AS path_depth, "
            "(c.path IN ('', '/') AND NOT contains(c.url, '?'))::INT AS is_root, "
            "contains(c.url, '?')::INT AS has_query, "
            f"CASE WHEN {query} = '' THEN 0 ELSE length({query}) "
            f"- length(replace({query}, '&', '')) + 1 END AS n_params, "
            "regexp_matches(c.path, '[0-9]{5,}')::INT AS path_long_number, "
            f"regexp_matches(c.path, {sql_str(f.date_regex)})::INT AS path_date, "
            f"{marker_sql} AS path_marker, "
            f"CASE {ext} WHEN '' THEN 0 {ext_cases} ELSE {ext_other} END AS ext_class, "
            "starts_with(c.url, 'https://')::INT AS is_https, "
            "starts_with(c.host, 'www.')::INT AS sub_www, "
            "(starts_with(c.host, 'm.') OR starts_with(c.host, 'mobile.'))::INT AS sub_mobile, "
            "(starts_with(c.host, 'amp.') OR contains(c.path, '/amp'))::INT AS sub_amp, "
            "length(c.host) - length(replace(c.host, '.', '')) + 1 AS host_labels, "
            "(c.title_len IS NULL)::INT AS title_null, c.title_len, "
            "c.has_anchor::INT AS has_anchor, c.discovery_source_type, c.robots_bits, "
            "c.is_redirect::INT AS is_redirect, c.redirect_hop_count, "
            "c.has_json_ld::INT AS has_json_ld, c.hreflang_count, "
            "c.has_last_modified::INT AS has_last_modified, "
            "c.has_cache_control::INT AS has_cache_control, "
            "ln(1 + c.inlink_count_approx) AS log_inlinks, "
            "ln(1 + c.inlink_count_external) AS log_ext, "
            "(c.inlink_count_external > 0)::INT AS has_ext")

    def _own_weight_sql(self, batch_id: int) -> str:
        """Exact decay weight of the row in every source pool of `batch_id`.

        `expo_*` counts a sample row in the pool of each source batch with
        `last_fetch_ok < as_of`; a row outside the sample adds the same weight
        for itself. DuckDB renders the DECIMAL weights, so they match `lvl_*` exactly.
        """
        rows = self.con.execute(
            f"SELECT s.as_of, (p.w::{EXACT})::VARCHAR FROM pairs p "
            f"JOIN batches s ON s.batch_id = p.src WHERE p.dst = {batch_id} ORDER BY 1").fetchall()
        terms = [f"CASE WHEN c.last_fetch_ok < {_ts(t)} THEN {sql_str(w)}::{EXACT} ELSE 0 END"
                 for t, w in rows]
        return f"({' + '.join(terms)})::{EXACT}" if terms else f"0::{EXACT}"

    def frame_sql(self, batch_id: int, mode: str) -> str:
        """SQL for the rows of batch `batch_id` with ids, label and every feature.

        Args:
            batch_id: The batch whose pool, label and as-of time apply.
            mode: "eval" keeps every pool sample row plus the batch's golden
                rows; "train" keeps the golden rows plus a hashed subsample of the
                other pool sample rows at rate `dataset.neg_rate`, never an
                aggregate row, ordered by (url, shard) because LightGBM bagging
                picks rows by position; "score" keeps every crawl row in the pool
                (after `load_chunk`).
        """
        if mode not in ("eval", "train", "score"):
            raise ValueError(f"unknown mode {mode!r}")
        order = "ORDER BY c.url, c.shard" if mode == "train" else ""
        b = self.batch(batch_id)
        a = float(self.cfg.features.eb_pseudo_hits)
        ppm = round(self.cfg.dataset.neg_rate * 1_000_000)
        salt = sql_str(f"{self.cfg.dataset.neg_salt}:{batch_id}")
        keep = {"eval": "(l.url IS NOT NULL OR c.is_sample)",
                "train": f"NOT c.in_agg AND (l.url IS NOT NULL OR (c.is_sample AND "
                         f"md5_number_lower(c.url || {salt}) % 1000000 < {ppm}))",
                "score": "true"}[mode]
        rate = self.agg_rate
        # Every row counts in its own domain: aggregate rows through expo_*, others by `own`.
        n_dkey = "(coalesce(ds.n, 0) + c.own)"

        def exposure(alias: str) -> str:
            # Aggregate rows count themselves in `ex`; any other row adds itself here.
            return f"(coalesce({alias}.ex, 0) + base.own * base.own_w)::DOUBLE / {rate!r}"

        return f"""
WITH base AS (
  SELECT c.*, l.url IS NOT NULL AS label,
    coalesce(l.in_head, false) AS in_head, coalesce(l.in_random, false) AS in_random,
    CASE WHEN c.in_agg THEN 0 ELSE 1 END AS own, {self._own_weight_sql(batch_id)} AS own_w
  FROM crawl c
  LEFT JOIN (SELECT * FROM label_urls WHERE batch_id = {batch_id}) l ON l.url = c.url
  WHERE c.last_fetch_ok < {_ts(b.as_of)} AND {keep}
), g AS (
  SELECT ex, g, hb FROM glob_stats WHERE batch_id = {batch_id}
),
s0 AS (
  SELECT base.*, nullif(g.g, 0) / nullif({exposure("g")}, 0) AS rg, g.hb
  FROM base LEFT JOIN g ON true
),
s1 AS (
  SELECT base.*,
    (coalesce(t.g, 0) + {a}) / ({exposure("t")} + {a} / base.rg) AS r_tld,
    t.g AS g_tld, d.g AS g_dkey, {exposure("d")} AS e_dkey, ho.g AS g_host,
    {exposure("ho")} AS e_host, hp.g AS g_hp, {exposure("hp")} AS e_hp
  FROM s0 base
  LEFT JOIN (SELECT * FROM lvl_tld WHERE batch_id = {batch_id}) t ON t.k = base.tld
  LEFT JOIN (SELECT * FROM lvl_dkey WHERE batch_id = {batch_id}) d ON d.k = base.dkey
  LEFT JOIN (SELECT * FROM lvl_host WHERE batch_id = {batch_id}) ho ON ho.k = base.host
  LEFT JOIN (SELECT * FROM lvl_hp WHERE batch_id = {batch_id}) hp ON hp.k = base.hp
), s2 AS (
  SELECT s1.*, (coalesce(g_dkey, 0) + {a}) / (coalesce(e_dkey, 0) + {a} / r_tld) AS r_dkey
  FROM s1
), s3 AS (
  SELECT s2.*, (coalesce(g_host, 0) + {a}) / (coalesce(e_host, 0) + {a} / r_dkey) AS r_host
  FROM s2
), s4 AS (
  SELECT s3.*, (coalesce(g_hp, 0) + {a}) / (coalesce(e_hp, 0) + {a} / r_host) AS r_hp
  FROM s3
)
SELECT c.url, c.shard, c.h, c.dkey, c.is_sample, c.in_agg, c.label::INT AS label,
  c.in_head, c.in_random, c.url_score, c.source,
  {self._static_sql()},
  date_diff('second', c.last_fetch_ok, {_ts(b.as_of)}) / 86400.0 AS fetch_age_days,
  (ln(1 + c.inlink_count_approx)::{EXACT} * {n_dkey} - coalesce(ds.sum_li, 0)
   - c.own * coalesce(ln(1 + c.inlink_count_approx), 0)::{EXACT})::DOUBLE
   / nullif({n_dkey}, 0) AS rel_inlinks,
  (ln(1 + c.inlink_count_external)::{EXACT} * {n_dkey} - coalesce(ds.sum_le, 0)
   - c.own * coalesce(ln(1 + c.inlink_count_external), 0)::{EXACT})::DOUBLE
   / nullif({n_dkey}, 0) AS rel_ext,
  coalesce(u.hist_decayed, 0) AS hist_decayed, coalesce(u.hist_head, 0) AS hist_head,
  coalesce(u.hist_random, 0) AS hist_random, u.hist_days_since,
  ln(c.r_tld / c.rg) AS lift_tld, ln(c.r_dkey / c.rg) AS lift_dkey,
  ln(c.r_host / c.rg) AS lift_host, ln(c.r_hp / c.rg) AS lift_hp,
  ln(1 + coalesce(dd.dem, 0)) AS dem_dkey, ln(1 + coalesce(dh.dem, 0)) AS dem_host,
  ln(1 + {n_dkey} / {rate!r}) AS log_size_dkey,
  ln(1 + (coalesce(hs.n, 0) + c.own) / {rate!r}) AS log_size_host,
  (coalesce(ds.n_ext, 0) + c.own * coalesce((c.inlink_count_external > 0)::INT, 0))
   / nullif({n_dkey}, 0) AS dkey_ext_share, c.hb AS history_batches
FROM s4 c
LEFT JOIN (SELECT * FROM expo_dkey WHERE batch_id = {batch_id}) ds ON ds.k = c.dkey
LEFT JOIN (SELECT * FROM expo_host WHERE batch_id = {batch_id}) hs ON hs.k = c.host
LEFT JOIN (SELECT * FROM dem_dkey WHERE batch_id = {batch_id}) dd ON dd.k = c.dkey
LEFT JOIN (SELECT * FROM dem_host WHERE batch_id = {batch_id}) dh ON dh.k = c.host
LEFT JOIN (SELECT * FROM uhist WHERE batch_id = {batch_id}) u ON u.url = c.url
{order}
"""

    def frame(self, batch_id: int, mode: str) -> pa.Table:
        return self.con.execute(self.frame_sql(batch_id, mode)).to_arrow_table()

    def pool_size(self, batch_id: int) -> int:
        """Sample rows in the pool of `batch_id` (deduplicated by URL)."""
        (n,) = self.con.execute(
            f"SELECT count(DISTINCT url) FROM crawl WHERE is_sample "
            f"AND last_fetch_ok < {_ts(self.batch(batch_id).as_of)}").fetchone()
        return int(n)

    def golden_spellings(self, batch_ids: list[int]) -> pa.Array:
        """Every URL that labels a row positive in any of `batch_ids` (raw or canonical)."""
        ids = ", ".join(map(str, batch_ids)) or "NULL"
        return self.con.execute(
            f"SELECT DISTINCT url FROM label_urls WHERE batch_id IN ({ids})"
        ).to_arrow_table().column(0).combine_chunks()

    def label_audit(self, batch_id: int) -> dict[str, int | float | None]:
        """Counts sample negatives of `batch_id` that look like golden URLs.

        `neg_later_golden` and `neg_next_golden` count negatives whose URL is
        golden in any later official batch or in the next one. `neg_variant_golden`
        counts negatives that differ from a golden spelling of this batch only by
        scheme, a leading `www.` or a trailing `/`; the official metric compares
        raw strings, so those rows are true negatives. Later batches are future
        information, so this is a diagnostic and never feeds a model.
        """
        b = self.batch(batch_id)
        later = sorted((x for x in self.batches if x.official and x.created_at > b.created_at),
                       key=lambda x: x.created_at)
        ids = ", ".join(str(x.batch_id) for x in later) or "NULL"
        nxt = later[0].batch_id if later else "NULL"

        def loose(col: str) -> str:
            return f"regexp_replace(regexp_replace({col}, '^https?://(www\\.)?', ''), '/$', '')"

        neg, later_n, next_n, variant = self.con.execute(f"""
SELECT count(*), count(f.url), count(*) FILTER (WHERE f.nxt), count(v.k)
FROM crawl c
LEFT JOIN (SELECT url, bool_or(batch_id = {nxt}) AS nxt FROM label_urls
           WHERE batch_id IN ({ids}) GROUP BY url) f ON f.url = c.url
LEFT JOIN (SELECT DISTINCT {loose('url')} AS k FROM label_urls WHERE batch_id = {batch_id}) v
  ON v.k = {loose('c.url')}
WHERE c.is_sample AND c.last_fetch_ok < {_ts(b.as_of)}
  AND NOT EXISTS (SELECT 1 FROM label_urls l WHERE l.batch_id = {batch_id} AND l.url = c.url)
""").fetchone()
        return {"neg_rows": neg, "neg_later_golden": later_n,
                "neg_next_golden": next_n if later else None,
                "later_golden_share": later_n / neg if neg else None,
                "neg_variant_golden": variant}

    def has_history(self, batch_id: int) -> bool:
        """True when some earlier batch with crawled golden feeds this batch's priors."""
        row = self.con.execute(
            f"SELECT g, hb FROM glob_stats WHERE batch_id = {batch_id}").fetchone()
        return bool(row) and row[0] > 0 and row[1] >= 1 and not math.isnan(row[0])


def open_store(cfg: Config, work_dir: Path, next_batch: bool = False) -> FeatureStore:
    return FeatureStore(cfg, open_duckdb(cfg, work_dir / "duckdb_tmp"), work_dir, next_batch)
