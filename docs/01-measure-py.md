# measure.py - Full Technical Document

## 1. Scope and Role

`measure.py` is the main orchestrator for:

- Creating metric tables.
- Generating golden datasets from trending keyword sources.
- Executing measurement jobs (crawler status and coverage metrics).

It coordinates three layers:

- Data source ingestion (`Metric.RawDataReader`).
- Golden set generation (`Metric.Query`).
- Metric computation and persistence (`Metric.Measure`).

## 2. CLI Contract

`measure.py` exposes the following arguments:

- `--strategy [random|head]...`: Which golden-set strategy(ies) to run.
- `--crawler_db_url`: Host:port for crawler PostgreSQL.
- `--metric_db_url`: Host:port for metric PostgreSQL.
- `--select_db_url`: Host:port for selectdb PostgreSQL (required for `crawler_all` index coverage).
- `--create`: Build/update golden sets.
- `--rawdatareader db`: Read trending raw data via DB-backed reader.
- `--update <days>`: Raw-data cache TTL in days (default: 14).
- `--keywordNums <N>`: Number of keywords for strategy (default: 100).
- `--test`: Execute measure phase.
- `--typesense_url`: Reserved/not actively used by current active measures.
- `--measure [status|rank|crawler_all|all]...`: Which measure(s) to execute.
- `--createtable`: Create metric tables before execution.

## 3. Runtime Modes

### 3.1 Table creation mode

- If `--createtable` is set:
  - Calls `createAllMetricModel()` to register dynamic ORM models:
    - `crawler_stat_total`, `crawler_stat_a`, `crawler_stat_b`
    - `metric_headset_total`, `metric_headset_a`, `metric_headset_b`
    - `metric_randomset_total`, `metric_randomset_a`, `metric_randomset_b`
  - Calls `createDB(..., createTable=True, base=MetricBase)` for metric DB.

### 3.2 Dataset creation mode (`--create`)

Flow:

1. Build `DatabaseRawDataReader(metricDB, modelFactory, update_day)`.
2. Check the SerpApi key and quota with `require_searches()` (`Metric/Query/SerpApi.py`).
   It calls `account.json`, which costs no search, and needs
   `keywordNums × strategies + one per trending geo` searches left.
   A missing or invalid key, or too few searches, stops the run before any batch is written.
3. `readData()` returns trending keywords:
   - Uses the latest non-empty `metric_batches` entry (`meta_total_queries > 0`) if it is under `update_day` days old.
   - Otherwise fetches from SerpApi and persists new batch/query rows.
   - If any geo still fails after its retries, it raises and saves no batch, so the next run fetches again.
4. Read latest non-empty batch id via `get_latest_batch_id()`.
5. For each strategy:
   - `random`: sample `keywordNums` keywords.
   - `head`: pick top `keywordNums` by frequency.
6. For each chosen keyword:
   - Query Google organic results via SerpApi.
   - Upsert `metric_queries.tags` with strategy tag.
   - Replace associated rows in `metric_url`.
7. Recompute batch metadata (`meta_total_queries`, `meta_total_urls`, `meta_tag_stats`).

### 3.3 Measure execution mode (`--test`)

- `status`: runs `CrawlerStatusMeasure`.
- `crawler_all`: for each selected strategy tag (`head`/`random`), runs `CrawlerAllMetricMeasure`.
  - Queries `selectdb.selected_urls_current` to determine `is_indexed` flag per golden URL.
  - Without `--select_db_url`, indexed always reports `False`.
- `rank`: currently no-op in active implementation.

## 4. DB Initialization

`createDB(user, password, url, name)` composes:

- `postgresql+psycopg2://{user}:{password}@{url}/{name}`

Effective defaults from `measure.py`:

- Crawler DB credentials: `crawler:crawler`, DB name `crawlerdb`.
- Metric DB credentials: `metric:metric`, DB name `metricdb`.
- Select DB credentials: `select:select`, DB name `selectdb`.

## 5. Key Internal Functions

### 5.1 `get_latest_batch_id`

- SQLAlchemy `select(MetricBatch.id).order_by(id desc).limit(1)`.
- Returns latest batch id or `None`.
- Used by dataset generation and coverage measuring.

### 5.2 `createDataset`

Responsibilities:

- Read or refresh raw trending keyword data.
- Run one or more golden-set strategies.
- Persist/refresh query->URL mapping in metric DB.

### 5.3 `test`

Responsibilities:

- Run measurement classes against crawler + metric + select DBs.
- Persist computed KPI tables via UPSERT.

## 6. Control Flow Diagram

```mermaid
flowchart TD
    A[CLI args] --> B{--createtable?}
    B -->|yes| C[Create dynamic metric models + create tables]
    B -->|no| D[Connect crawlerDB + metricDB + selectDB]
    C --> D

    D --> E{--create?}
    E -->|yes| F[DatabaseRawDataReader.readData]
    F --> G{strategy contains random/head}
    G --> H[RandomQueryStrategy.getGoldenSet]
    G --> I[HeadQueryStrategy.getGoldenSet]

    D --> J{--test?}
    J -->|yes| K{measure contains status/crawler_all}
    K --> L[CrawlerStatusMeasure.test]
    K --> M[CrawlerAllMetricMeasure.test per strategy tag]
    M --> N[Query selectdb.selected_urls_current for is_indexed]
```

## 7. Operational Notes

- `rank` pathway is not active; do not assume search ranking KPIs are written.
- `indexed` is now populated from `selectdb.selected_urls_current` when `--select_db_url` is provided.
- `--measure all` appears in choices but no explicit branch handles it in current code.

