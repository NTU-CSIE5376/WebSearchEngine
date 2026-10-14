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






