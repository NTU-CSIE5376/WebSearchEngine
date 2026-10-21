# IndexSelection_v1

URL selection pipeline using LightGBM binary classifier to select high-quality URLs from a large crawler database.

## Overview

- **Goal**: Select top 32M high-quality URLs from ~767M URL database using LightGBM model
- **Database**: PostgreSQL (crawlerdb + metricdb)
- **Model**: LightGBM binary classifier

## Insel Repo Lifecycle

The index selection report, or "insel repo", is the current `selected_urls_current` snapshot in selectdb.

- Every refresh fully replaces the current insel contents with a new 32M URL selection.
- The expected operating cadence is biweekly: every 14 days, select 32M URLs from the discovery/disc report and load them into selectdb.
- One week before each golden-set availability date, run a reset that clears `selected_urls_current`.
- Reset only clears the live insel contents; `model_runs` keeps the audit trail for both refresh and reset events.

The Docker entrypoint supports two scheduling modes:

- Cycle-aware mode: set `GOLDEN_SET_ANCHOR_DATE`, `GOLDEN_SET_CADENCE_DAYS`, and `PRE_GOLDEN_RESET_DAYS`; the container runs a daily scheduler tick and decides whether today is a refresh day or reset day.
- Legacy cron mode: set `REFRESH_SCHEDULE` and optionally `RESET_SCHEDULE` directly.

Example UTC configuration:

```bash
GOLDEN_SET_ANCHOR_DATE=2026-05-08
GOLDEN_SET_CADENCE_DAYS=14
PRE_GOLDEN_RESET_DAYS=7
SCHEDULER_TICK_SCHEDULE="0 0 * * *"
```

With that anchor:

- `2026-05-01` UTC triggers the pre-golden reset
- `2026-05-08` UTC refreshes and fills the insel repo with the next 32M URLs
- `2026-05-15` UTC triggers the next reset
- `2026-05-22` UTC triggers the next biweekly refresh

## Workflow

```
prepare_live_training_data.py → train_pipeline.py → score_live_crawler.py → load_selection_into_selectdb.py
```

## Scripts

### 1. prepare_live_training_data.py

Prepare training data from live metricdb/crawlerdb.

```bash
python3 prepare_live_training_data.py \
  --metric-db-url "postgresql://user:pass@host/metricdb" \
  --crawler-db-url "postgresql://user:pass@host/crawlerdb" \
  --output-dir /path/to/output \
  --train-batches 10 \
  --test-batches 1
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--metric-db-url` | required | Live metricdb URL |
| `--crawler-db-url` | required | Live crawlerdb URL |
| `--output-dir` | required | Output directory |
| `--train-batches` | 10 | Training batch count |
| `--test-batches` | 0 | Test batch count (0 = train only) |
| `--negative-sample-target` | 100000 | Negative sample target |
| `--worker-count` | 16 | Parallel workers |
| `--db-retry-attempts` | 3 | DB retry attempts |
| `--seed` | random | Random seed |

### 2. train_pipeline.py

Train the model from extracted features.

```bash
python3 train_pipeline.py \
  --pos-csv output/metric_url_train.csv \
  --neg-csv output/crawler_negative_train.csv \
  --model-path output/model.txt \
  --domain-freq-path output/domain_freq.tsv
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--pos-csv` | required | Training positive source |
| `--neg-csv` | required | Training negative source |
| `--pos-test-csv` | None | Test positive source (optional) |
| `--neg-test-csv` | None | Test negative source (optional) |
| `--model-path` | model.txt | Model output path |
| `--domain-freq-path` | domain_freq.tsv | Domain frequency TSV |
| `--train-feature-csv` | training_features_ext_train.csv | Training feature CSV |
| `--test-feature-csv` | None | Test feature CSV |
| `--feature-list-path` | features_ext.txt | Feature list path |
| `--seed` | 42 | Random seed |
| `--random-test-frac` | 0.0 | Random test fraction (0 = no random test) |
| `--train-pos-frac` | 1.0 | Training positive fraction |
| `--test-pos-frac` | 1.0 | Test positive fraction |
| `--neg-ratio` | None | Negative-to-positive ratio |
| `--skip-extract` | False | Skip feature extraction |
| `--skip-binary-train` | False | Skip model training |
| `--skip-binary-eval` | False | Skip model evaluation |
| `--positive-source` | metric_url_csv | Positive source (metric_url_csv/metric_data) |
| `--metric-data-dir` | Metric/metricData | Metric data directory |
| `--label-snapshot-count` | 2 | Label snapshot count |
| `--label-start-date` | None | Label start date (YYYYMMDD) |

### 3. score_live_crawler.py

Score the crawler database and select top-K URLs.

```bash
python3 score_live_crawler.py \
  --crawler-db-url "postgresql://user:pass@host/crawlerdb" \
  --model-path model.txt \
  --domain-freq-path domain_freq.tsv \
  --selection-top-k 32000000 \
  --selection-output-path top_32m.tsv \
  --score-shard-dir /path/to/shards \
  --db-stream-method connectorx
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--crawler-db-url` | required | crawlerdb URL |
| `--model-path` | required | LightGBM model path |
| `--domain-freq-path` | required | Domain frequency TSV |
| `--selection-top-k` | 32000000 | URLs to keep |
| `--max-urls-per-domain` | None | Max URLs per domain (optional) |
| `--selection-output-path` | selection_top_k.tsv | Output TSV path |
| `--summary-path` | selection_summary.json | Summary JSON path |
| `--score-shard-dir` | runtime/shards | Score shard directory |
| `--score-batch-size` | 100000 | Batch size |
| `--shard-start` | 0 | First shard |
| `--shard-end` | 255 | Last shard |
| `--worker-count` | 16 | Parallel workers |
| `--chunk-worker-count` | 4 | Chunk workers |
| `--db-retry-attempts` | 3 | DB retry attempts |
| `--first-seen-before` | None | UTC cutoff timestamp |
| `--progress-log-path` | None | Progress log path |
| `--progress-every-rows` | 100000 | Progress interval |
| `--verify-row-counts` | False | Verify row counts |
| `--intra-shard-workers` | 0 | Intra-shard workers |
| `--merge-threads` | 0 | Merge threads (0=auto) |
| `--db-stream-method` | connectorx | DB stream (`connectorx`) |

### 4. load_selection_into_selectdb.py

Load selected URLs into selectdb.

```bash
python3 load_selection_into_selectdb.py \
  --selectdb-url "postgresql://user:pass@host/selectdb" \
  --selection-tsv top_32m.tsv \
  --run-id 20260401
```

### 5. sync_selectdb_selected_urls_to_crawlerdb.py

Notify crawlerdb after IndexSelection finishes loading `selectdb.selected_urls_current`.
This mirrors selectdb-selected URLs into `crawlerdb.url_state_current_*` using
`is_selectdb_selected`, `selectdb_score`, `selectdb_run_id`, `selectdb_selected_at`,
and `selectdb_synced_at`. URLs already present in crawlerdb are reopened with
`should_crawl=TRUE` when the selectdb `run_id` changes; missing crawlerdb URLs
are counted but not inserted.

```bash
python3 sync_selectdb_selected_urls_to_crawlerdb.py \
  --crawler-db-url "postgresql://user:pass@host/crawlerdb" \
  --select-db-url "postgresql://user:pass@host/selectdb" \
  --summary-path /path/to/selected_url_crawlerdb_sync_summary.json
```

### 6. run_monthly_refresh.py

Full refresh wrapper (all-in-one). Despite the historical file name, this is the entry point typically used for the biweekly insel fill. It now notifies crawlerdb to schedule the freshly loaded selectdb URLs, then also syncs `crawlerdb.metric_url.is_indexed` from the same `selectdb.selected_urls_current` snapshot.

```bash
python3 run_monthly_refresh.py \
  --metric-db-url "postgresql://user:pass@host/metricdb" \
  --crawler-db-url "postgresql://user:pass@host/crawlerdb" \
  --selectdb-url "postgresql://user:pass@host/selectdb" \
  --run-root /path/to/run \
  --selection-top-k 32000000
```

### 7. reset_selectdb.py

Clear the live insel contents ahead of the next golden-set window.

```bash
python3 reset_selectdb.py \
  --select-db-url "postgresql://user:pass@host/selectdb" \
  --reason pre_golden_reset \
  --summary-path /path/to/reset_summary.json
```

## Native Extension (C++)

For faster domain extraction, build the C++ extension:

```bash
cd IndexSelection_v1
python3 build_native_extension.py build_ext --inplace
```

This provides:
- `extract_domains(urls)`: Fast domain extraction
- `extract_domain_hashes(urls)`: Domain hash extraction

## Feature Columns

55 features total:
- Domain frequency (from training data)
- URL length, path length, query length, path depth
- Has query, num params, has fragment
- HTTPS, is homepage
- Num digits, digit ratio, hyphen, underscore
- Domain length, subdomain count, TLD length, file ext
- Date patterns
- TLD one-hot features (34 TLDs)

## Database Schema

### metricdb

- `metric_queries`: query_id, batch_id, keyword, geo, frequency, tags
- `metric_url`: id, query_id, url, rank, is_discovered, is_crawled, is_indexed, is_ranked
- `metric_batches`: id, created_at, meta_total_queries, meta_total_urls

### crawlerdb

- `url_state_current_000..255`: partitioned URL table
- Fields: url, first_seen, source_table, sample_attempt
- `metric_url.is_indexed`: synced from the current `selectdb.selected_urls_current` snapshot after each refresh

### selectdb

- `selected_urls_current`: url, score, first_seen, run_id

## Examples

### Prepare Training Data

```bash
python3 prepare_live_training_data.py \
  --metric-db-url "postgresql://metric:metric@172.16.191.1:5433/metricdb" \
  --crawler-db-url "postgresql://crawler:crawler@172.16.191.1:5432/crawlerdb" \
  --output-dir training_data \
  --train-batches 9 \
  --test-batches 1
```

### Train Model

```bash
python3 train_pipeline.py \
  --pos-csv training_data/metric_url_train.csv \
  --neg-csv training_data/crawler_negative_train.csv \
  --model-path model_413/model.txt \
  --domain-freq-path model_413/domain_freq.tsv
```

### Score and Select (Full)

```bash
python3 score_live_crawler.py \
  --crawler-db-url "postgresql+psycopg2://crawler:crawler@172.16.191.1:5432/crawlerdb" \
  --model-path model_413/model.txt \
  --domain-freq-path model_413/domain_freq.tsv \
  --selection-output-path top_32m_pipelined.tsv \
  --summary-path top_32m_pipelined_summary.json \
  --score-shard-dir score_shards_pipelined \
  --chunk-worker-count 4 \
  --worker-count 8 \
  --score-batch-size 2000000 \
  --db-stream-method connectorx
```

### Score and Select (With Time Filter)

```bash
python3 score_live_crawler.py \
  --crawler-db-url "postgresql+psycopg2://crawler:crawler@172.16.191.1:5432/crawlerdb" \
  --model-path model_413/model.txt \
  --domain-freq-path model_413/domain_freq.tsv \
  --selection-output-path top_32m_pipelined.tsv \
  --summary-path top_32m_pipelined_summary.json \
  --score-shard-dir score_shards_pipelined \
  --chunk-worker-count 4 \
  --worker-count 8 \
  --score-batch-size 2000000 \
  --db-stream-method connectorx \
  --first-seen-before 2026-04-10
```

### With Per-Domain Limit

```bash
python3 score_live_crawler.py \
  --crawler-db-url "postgresql://user:pass@host/crawlerdb" \
  --model-path model.txt \
  --domain-freq-path domain_freq.tsv \
  --selection-top-k 32000000 \
  --max-urls-per-domain 100000 \
  --selection-output-path top_32m.tsv
```

### With Time Filter

```bash
python3 score_live_crawler.py \
  --crawler-db-url "postgresql://user:pass@host/crawlerdb" \
  --model-path model.txt \
  --domain-freq-path domain_freq.tsv \
  --selection-top-k 32000000 \
  --first-seen-before 2025-01-01 \
  --selection-output-path top_32m.tsv
```

## File Structure

- `train_model.py`: training and top-k merge
- `score_live_crawler.py`: scoring pipeline
- `polars_merge_helper.py`: helper for subprocess merge
- `feature_extract_ext.py`: feature extraction
- `train_pipeline.py`: training orchestration
- `training_common.py`: shared utilities
- `live_db_common.py`: database utilities
- `prepare_live_training_data.py`: data preparation
- `load_selection_into_selectdb.py`: load into selectdb
- `sync_selectdb_selected_urls_to_crawlerdb.py`: notify crawlerdb to prioritize selectdb URLs
- `bootstrap_selectdb.py`: bootstrap selectdb
- `selectdb_common.py`: selectdb utilities
- `run_monthly_refresh.py`: full refresh workflow
- `native_feature_matrix.py`: native feature matrix wrapper
- `native/url_features_pybind.cpp`: C++ feature extraction
- `build_native_extension.py`: build script for C++ extension
