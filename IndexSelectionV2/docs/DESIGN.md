# IndexSelectionV2 design

- Code: `src/isel/`. Each section names its modules.
- Parameters: `config/isel.toml`. Nothing tunable is hard-coded.
- Results: [`BENCHMARK.md`](BENCHMARK.md).

## 1. Goals and non-goals

**Goals**

- Every two weeks, pick K = 128,000,000 of the ~1.66 billion crawled URLs so that the
  next golden batch's `indexed_rate` is as high as possible.
- Keep the method explainable, the evaluation free of leaks, and every number
  reproducible.
- Publish the list where the official metric reads it, with the metric's own query.

**Non-goals**

- No scheduler and no daemon. Each command is one run.
- No writes to crawlerdb, metricdb or the v1 selectdb (`:5444`).
- No page fetching and no index building.

## 2. Configuration (`config.py`)

- `load_config` reads one or more TOML files; later files override earlier ones.
- It fails on unknown keys, missing keys, wrong types and inconsistent values, so a
  typo never falls back to a default.
- `Config.sha256()` is a digest of the effective config. Models, runs and selectdb rows
  record it.
- DSNs never live in the config. The config names environment variables; each one may
  instead point to a secret file through `<NAME>_FILE`.

## 3. URL rules (`urlnorm.py`)

- `canonicalize` is the crawler's `w3lib.canonicalize_url`. If a URL cannot be parsed,
  it is returned unchanged.
- `DomainKeyer` gives the crawler's shard key: a host from the crawler's split list, or
  else the eTLD+1 (`tldextract` 5.1.1, offline).
- `ShardRouter` maps a key to one of 256 shards with the crawler's MD5 rule and its
  `domain_overrides` (`[routing.overrides]`).
- `md5_number_lower(text)` is the low 64 bits of MD5. It is the URL hash for sampling,
  tie breaks and `doc_id`.

## 4. Production safety (`pg.py`, `pull.py`)

| Risk | Control |
|---|---|
| Writing crawlerdb (the role is a superuser) | Every session sets `default_transaction_read_only = on`. On top of that, `assert_read_only_sql` lexes each statement like PostgreSQL and allows one read statement only. `COPY` must be `COPY (SELECT ...) TO STDOUT`. Functions that change server state or run SQL text (`set_config`, `pg_terminate_backend`, advisory locks, `lo_*`, `query_to_xml`, ...) are refused. |
| Long statements | `statement_timeout` defaults to 120 s and never exceeds `guard.timeout_cap_s`. A shard that hits a timeout is retried on the next run. |
| Server temp files | No server-side sort or aggregate; `temp_file_limit = 0`, `work_mem = 16MB`, no parallel workers. |
| Reading while the metric works | `ReadGuard` runs before every statement. A statement may not reach into the 1st and 16th 00:00–20:00 UTC or any hour's :58–:10, even at its full timeout. It pauses while more than `guard.max_io_waiters` other sessions wait on IO (median of 3 probes). |
| Read load | `pull-full` reads `full.chunk_blocks` heap blocks per `COPY` (a TID range scan). All sessions share one byte budget of `full.max_heap_mb_s` (2,000 MB/s, about a quarter of the metric's hourly scan). |
| Leaking credentials | DSNs come from env variables or secret files; JSON output passes `assert_no_secrets`. |

**Full export.** `pull-full` opens `full.connections` sessions. Each takes whole shards
from a shared queue and writes `full/export/shard=NNN/part-<lo>-<hi>.parquet`. Output is
`FORMAT CSV`, parsed by pyarrow. The session settings `datestyle`, `extra_float_digits`
and `client_encoding` fix the CSV text, and the config check refuses other values. The
run is resumable: a rerun reads only the missing block ranges.

**S0.** S0 is the start of `pull-full`. The sample, the positives and the scored rows
all come from this one export, so they describe one crawl state.

**Golden.** `pull-golden` copies metricdb's batches, queries and URLs to
`golden/golden.parquet`. Each row keeps the raw URL and its canonical spelling.

## 5. Point in time and features (`features.py`, `derive.py`, `dataset.py`)

**Point in time**

- Batch b has `as_of_b = min(created_b − lead_h, S0)`, with `lead_h` = 30 h.
- The candidate pool is `pool_b = {rows with last_fetch_ok < as_of_b}`. The crawler
  fetches a URL once, so this rebuilds the crawl state before the batch.
- Golden-derived features for batch b use only batches created before b and available
  by `as_of_b`. Older batches weigh `0.5 ^ (age_days / half_life_days)`.

**Sample**

- `derive-sample` keeps an export row when
  `md5_number_lower(url:shard:salt) mod 10⁶ < p × 10⁶`, with p = `full.sample_percent`
  (2 %). Each row's membership is independent of its neighbours.
- `TABLESAMPLE SYSTEM` is not used. It takes whole heap pages, and rows on one page
  usually share a domain: adjacent rows share `domain_id` 32.6 % of the time, against
  0.012 % at random.
- A second salted hash splits the sample into halves of `full.agg_share` (0.5):
  - the **aggregate half** only feeds domain statistics;
  - the **pool half** gives negatives, the evaluation pool, τ and the scoring checks.
- `derive-positives` takes every export row whose URL equals a golden raw or canonical
  spelling, from every shard.

**Features.** The model reads only columns registered in `FEATURES`. A feature set that
names a column in `FORBIDDEN` (`source`, `url_score`, `should_crawl`, `*_90d`, ...)
stops at `resolve_set`.

| Group | Examples | Leak risk |
|---|---|---|
| URL shape | `url_len`, `path_depth`, `has_query`, `ext_class`, `sub_www` | none |
| Fetch facts | `title_len`, `robots_bits`, `is_redirect`, `fetch_age_days` | none |
| Link counts | `log_inlinks`, `log_ext`, `rel_inlinks`, `rel_ext` | inlinks grow after the batch |
| URL history | `hist_decayed`, `hist_head`, `hist_days_since` | batches before `as_of` only |
| Domain | `lift_tld`, `lift_dkey`, `lift_host`, `lift_hp`, `dem_dkey`, `log_size_dkey` | batches and pool before `as_of` only |

**Domain rates (empirical Bayes).** Levels from fine to coarse: `hp` (host + first path
segment), `host`, `dkey` (the shard key), `tld`, global. Each level shrinks toward the
next coarser one:

```
r_tld  = (g_tld  + a) / (e_tld  + a / r_global)
r_dkey = (g_dkey + a) / (e_dkey + a / r_tld)
...
lift_x = ln(r_x / r_global)
```

`e` is the estimated crawled pages, `g` the crawled golden pages, `a` =
`features.eb_pseudo_hits`. A key with no data equals its parent, so small domains never
get extreme values.

**Membership invariant.** A row's features must not depend on whether the row is in the
sample.

- Domain statistics come from the aggregate half only.
- Every other row adds itself to its domain's counts (`own = 1`).
- Training rows never come from the aggregate half.
- `tests/test_membership.py` checks that one page gets identical features inside and
  outside the aggregate half, and that training rows share group values whatever their
  label.

**Exact sums.** Cross-row float sums are taken as `DECIMAL(38, 18)`. DuckDB adds DOUBLE in
no fixed order, and LightGBM bins 0 apart from ±4·10⁻¹⁶, so DOUBLE sums would train a
different model on every run.




