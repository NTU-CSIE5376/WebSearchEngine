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





