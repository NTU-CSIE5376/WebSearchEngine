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

- No scheduler. Each command is one run, except the two opt-in recrawl services
  ([`RECRAWL.md`](RECRAWL.md)).
- No writes to crawlerdb, metricdb, the v1 selectdb (`:5444`) or `/data/ipc`.
- No page fetching of our own and no index building yet. The recrawl asks the crawler
  to fetch the list and keeps the extracted text; the index is only planned.

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
- Golden-derived features for batch b use only batches created before b and no later
  than `as_of_b`. Older batches weigh `0.5 ^ (age_days / half_life_days)`.

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

## 6. Model and evaluation (`model.py`, `backtest.py`, `evaluate.py`)

**Training data for batch b**

- Positives: rows of `pool_b` whose URL equals a golden(b) spelling.
- Negatives: other pool-half sample rows in `pool_b`, sampled at `dataset.neg_rate`.
- Head and random positives get equal total weight.
- The model estimates P(url ∈ golden(b) | features at `as_of_b`).

**Training**

- The train frame is sorted by (url, shard), because LightGBM bagging samples by row
  position.
- Early stopping looks only at the last official training batch. With
  `refit_on_all = true`, the model is refit on all training batches with the best
  round count.
- A test batch never takes part in tuning.

**Recall at K.** The sample rate is p, so population rank K sits near sample rank
round(K · p). A golden row at or above that boundary counts as selected. With p = 1 this
is the exact top K.

**Rates**

- `indexed_exact_rate`: raw-string match. It is the backtest headline, kept comparable
  with earlier runs.
- `indexed_canonical_rate`: the raw or the canonical spelling is listed.
- `crawled_*_rate`: the share that was crawled by `as_of`, an upper bound.

**Confidence intervals.** A bootstrap resamples queries. A URL in c queries weighs 1/c
in each. Each scorer is compared with the reference model as a paired difference.

**Adoption rule (fixed in advance).** Tune on batch t − 1 and report on t. Adopt a
change only when its paired 95 % CI against the strongest baseline is above 0.

**Baselines:** `random`, `inlinks`, `ext`, `domain_prior`, `logreg`, `v1_features`
(the v1 feature set retrained), and, in forward evaluation only, `url_score` and the
V2 rule formula.

**Frozen model.** `train` writes `models/<id>/` with the model, its SHA-256 and a
`sample_fingerprint` (method, percent, salt, export start). `forward-eval` and
`score-full` refuse a model whose fingerprint differs from the current sample.

## 7. Output contract (`manifest.py`)

```
selection/run=<id>/
  manifest/index_shard=NN.parquet   # one row per URL
  deltas/added.parquet              # doc_ids not in the parent run
  deltas/removed.parquet            # parent doc_ids no longer selected
  _MANIFEST.json                    # written last: parent, rows, sha256 per file
```

| Column | Meaning |
|---|---|
| `doc_id` | low 64 bits of MD5 of the canonical URL; stable across runs |
| `url` | the spelling the metric matches (the crawler's spelling) |
| `canonical_url` | `canonicalize(url)`; spellings of one page share a `doc_id` |
| `host`, `domain_id`, `crawl_shard` | where the crawler keeps the page |
| `index_shard` | `doc_id mod index_shards`, for a future indexer |
| `score`, `rank`, `reason` | model score, rank (score desc, URL hash asc), top feature contributions |

- Every file is written as `.tmp`, fsynced, then renamed (`io.py`).
- `validate-manifest` checks each file's sha256 and row count. `publish` runs it first.

## 8. Full scoring and exact top K (`score.py`)

**τ.** The pool-half sample gives a score floor τ at sample rank
round(K · p · (1 + `tau_margin`)). `score-full` keeps only rows with score ≥ τ, so it
writes about 147M rows instead of 1.66B.

**Checks.** `score-full` stops, and `select-full` refuses its output, when:

- a sample row scores differently in the full pass than in the sample pass;
- the kept share differs from the sample's estimate by more than `kept_share_max_z` SDs;
- the model's sample fingerprint differs from the current sample.

**Selection.** `select-full` takes the exact top K distinct URLs, ties broken by URL
hash. A URL found in two shards keeps its best score. The result is a manifest run under
`full/selection/`.

**Resume.** `score-full` writes one file per shard and skips finished shards on a rerun.

## 9. selectdb contract (`schema.py`, `selectdb.py`, `publish.py`)

The schema lives only in `schema.py` (SQLAlchemy Core). Alembic revisions in
`migrations/versions/` apply it. `migrate-selectdb` applies them in one transaction and
then runs `alembic check` against `schema.py`; any difference rolls the whole run back.

| Table | Columns and constraints |
|---|---|
| `public.selected_urls_current` | `url` PK, `score`, `first_seen` (NULL), `run_id` FK `ON DELETE RESTRICT`, `selected_at`; the v1 columns |
| `public.model_runs` | the v1 columns, plus `status` (loading, validated, live, retired, abandoned, rolled_back; at most one live), `activated_at`, `config_sha256`, `code_version` |
| `recrawl.host`, `recrawl.doc`, `recrawl.log`, `recrawl.batch_seq` | the recrawl frontier (revision 0003): one row per hostname and per doc, and the fetch logs already applied. See [`RECRAWL.md`](RECRAWL.md) |

**How the metric reads it.** `SELECT url FROM public.selected_urls_current WHERE url =
ANY(:urls)`, 10,000 URLs at a time, as the hard-coded `select` role. Since
WebSearchEngine #7, the metric canonicalizes each golden URL and looks up only that key.

**Load (transaction 1).** `publish` validates the manifest, then creates
`selected_urls_r<id>` `LIKE selected_urls_current`, runs `COPY FREEZE`, adds the PK and
FK, runs `ANALYZE`, and checks that the row count equals K. Readers cannot see the new
table yet. `publish` refuses a database that is not at the newest revision.

**Swap (transaction 2).** Within `lock_timeout` 5 s, `swap` renames current to prev and
the new table to current, constraints included. A reader waits milliseconds and never
sees half a list. Swaps are refused on the 1st and 16th, 11:30–20:00 UTC, because the
metric builds the random set at 12:00 and the head set at 18:00.

**Rollback.** `rollback` exchanges current and prev.

**Write guard.** Every write goes through `selectdb.connect`. It refuses ports 5432,
5433 and 5444, and any database that holds `url_state_current_000` or `metric_url`.

**Reader role.** `migrate-selectdb` creates the `select` role with the password the
metric hard-codes. The role can only `SELECT` from `public`; it has no access to the
`recrawl` schema.

**Self-check.** `verify-live --batch <b>` sends the metric's query as `select`.
`indexed_metric_rate` equals the metric's `indexed_rate`: canonical golden URLs found ÷
distinct canonical golden URLs.

**First run on the live :5445.** That database predates Alembic. The first
`migrate-selectdb` needs `--stamp-baseline`: it records revision 0001, then applies
0002 and 0003. Review the DDL with `--sql` first. Revision 0002 changes the list's FK from
`CASCADE` to `RESTRICT`, so a deleted run can no longer take its list with it.

## 10. Deployment (`Dockerfile`, `deploy/`)

| Part | Host | Why |
|---|---|---|
| isel jobs (`deploy/compose.compute.yaml`) | Host2 | free RAM and disk; reads Host1's databases over the LAN |
| selectdb `:5445` (`deploy/compose.selectdb.yaml`) | Host1 | data on the root NVMe, not on crawlerdb's `/data` |

**Image**

- Stages: `uv` → `builder` (`uv sync --locked --no-dev`) → `test` (pytest with
  `--network=none`) → `runtime`.
- Base images are pinned by digest. `libgomp1` is installed for LightGBM.
- `USER 1014:1015` matches the host account, so files in the work dir belong to it.
- `--build-arg GIT_SHA=<sha>` becomes `ISEL_CODE_VERSION`; models, runs and
  `model_runs.code_version` record it.

**Compose**

| Setting | Reason |
|---|---|
| `profiles: [job]`, used with `run --rm` | one container per command; `up` starts nothing |
| except `recrawl-plan`, `recrawl-archive` (`profiles: [recrawl]`) | long-running loops with `restart: unless-stopped`, one instance each (advisory lock). `recrawl-plan` uses `network_mode: host` to reach the gateway on `127.0.0.1:4000`; both mount `/data/ipc/recrawl_result` read-only and `/data/isel` writable |
| `read_only: true`, `tmpfs: /tmp` | only `/work` is writable |
| secrets as `<NAME>_FILE` | DSNs never appear in `docker inspect`, env or logs |
| `mem_limit` 128g, `cpus` 32 | DuckDB 96 GB and LightGBM 32 threads fit below the limit |
| `cpu_shares: 256`, `oom_score_adj: 1000` | the crawler wins any contention; we are killed first |
| `network_mode: bridge` | jobs only connect out to Host1 |

The env file `~/isel/compose.env` (not in the repo) sets `ISEL_PROJECT`, `ISEL_IMAGE`,
`ISEL_WORK_DIR` and `ISEL_SECRETS`.

## 11. Runbook

Each step below needs the owner's approval. `pull-full` also needs the TA's approval.

```bash
D="docker compose --env-file $HOME/isel/compose.env -f deploy/compose.compute.yaml"
GIT_SHA=$SHA $D build --build-arg GIT_SHA=$SHA isel
J="$D run --rm isel"
$J check-config
$J pull-golden
$J pull-dims
$J pull-full --dry-run                 # catalog only: heap bytes, chunks, free disk
$J pull-full                           # rerun until shards_missing = 0
$J derive-sample
$J derive-positives
$J build-dataset
$J backtest --feature-sets full --baselines random,v1_features,domain_prior
$J train                               # freeze before the batch's as_of
$J score-full --model-dir /work/models/<id>
$J select-full
$J migrate-selectdb --sql --stamp-baseline   # review the DDL
$J migrate-selectdb --stamp-baseline         # first run on the pre-Alembic :5445 only
$J publish --run-dir /work/full/selection/run=<id>
$J swap --db-run-id <n>
$J verify-live --batch <b>
# recrawl (docs/RECRAWL.md), only after the crawler serves the recrawl lane
$J recrawl-sync                        # after each swap; refused in the freeze window
$J recrawl-plan --dry-run              # per-host capacity report; sends nothing
$D --profile recrawl up -d recrawl-plan recrawl-archive
$D --profile recrawl stop recrawl-plan # stops sending new batches at once
```

**Recovery**

- A bad list: `$J rollback` restores the previous list at once.
- The metric back on v1: the TA sets `--select_db_url` back to `:5444`.
- Every command resumes: rerun it after an interruption.
