# IndexSelectionV2

IndexSelectionV2 picks the 128,000,000 crawled URLs that go into the search index.
It learns which pages tend to become golden URLs, scores every crawled page, and
publishes the exact top K to its own selectdb. The official metric reads that list
with its existing query.

- Design: [`docs/DESIGN.md`](docs/DESIGN.md)
- Backtest and full-scale results: [`docs/BENCHMARK.md`](docs/BENCHMARK.md)
- Recrawl of the list (not deployed): [`docs/RECRAWL.md`](docs/RECRAWL.md), which links the
  crawler-side proposal

## How the metric reads the list

The metric does not change. It runs the same query against a different port:

```sql
-- role select, database selectdb, 172.16.191.1:5445
SELECT url FROM public.selected_urls_current WHERE url = ANY(:urls)
```

`public.selected_urls_current` keeps the v1 columns (`url`, `score`, `first_seen`,
`run_id`, `selected_at`). A golden URL counts as indexed when its canonical spelling
is in the table (WebSearchEngine #7).

## Pipeline

```mermaid
flowchart LR
    C[(crawlerdb<br/>read-only, rate-limited)] --> E[export<br/>every crawled row]
    G[(metricdb<br/>golden batches)] --> F
    E --> F[point-in-time features]
    F --> T[LightGBM<br/>frozen before the batch]
    T --> S[score the full pool]
    S --> K[exact top-K]
    K --> D[(selectdb :5445)]
    D --> M[metric]
```

| Step | Command | Reads | Writes |
|---|---|---|---|
| Export | `pull-golden`, `pull-dims`, `pull-full` | metricdb, crawlerdb (read-only) | Parquet in the work dir |
| Sample | `derive-sample`, `derive-positives` | the export | 2 % hash sample, golden rows |
| Model | `backtest`, `train` | the sample | backtest report, frozen model |
| Select | `score-full`, `select-full` | the export, the model | the 128M-URL manifest |
| Publish | `migrate-selectdb`, `publish`, `swap`, `verify-live` | the manifest | selectdb :5445 only |

## What it never does

- It never writes crawlerdb, metricdb or the v1 selectdb (`:5444`). Every session to
  crawlerdb and metricdb is read-only on the server and behind a client-side SQL gate.
- It never reads crawlerdb during the measurement days (1st and 16th, 00:00–20:00 UTC)
  or in each hour's :58–:10 window, when the metric scans the crawl tables.
- It never swaps the list on the 1st and 16th between 11:30 and 20:00 UTC, so the head
  and random sets of one batch see the same list.
- It has no scheduler. Each command is one container run, except the two opt-in recrawl
  services below.
- It never writes `/data/ipc`. Recrawl batches go through the crawler's API, and the
  crawler's result files are read-only for us.

## Recrawl (not deployed)

The list needs page text to build an inverted index, but `crawl_result` keeps HTML for
only 8 hours and the crawler never re-fetches a page. `recrawl.py` re-fetches the list
through a second crawler lane that the crawler team has not built yet:

| Command | Does |
|---|---|
| `recrawl-sync` | loads the live run into the `recrawl` schema of selectdb (one row per host and per doc) |
| `recrawl-plan` | loop: sends up to 100 due URLs per hostname to `POST /scheduler_ingest/v1/recrawl-batches` (WebCrawler PR #84's API, proposal C10) |
| `recrawl-archive` | loop: reads the crawler's WARC and fetch logs read-only, keeps the extracted text as Parquet under `/data/isel/text`, and updates the frontier |

The two loops run only with `--profile recrawl`, and only after the crawler serves the
recrawl lane (proposal C5, C6, C10). `tests/recrawl_fakes.py` holds a reference stub of
the C10 API and a fake crawler; the integration tests run the whole loop against them.

## Rollout and rollback

| Step | Who | Undo |
|---|---|---|
| Publish and swap a list on :5445 | us | `rollback` restores the previous list at once |
| Point the metric's `--select_db_url` at :5445 | TA (Dockerfile cron) | set it back to `:5444` |

## Code arrives in these pull requests

| Part | Adds |
|---|---|
| 1 | this README and the benchmark |
| 2 | package, strict config loader, URL rules |
| 3 | read-only crawlerdb and metricdb access, full export |
| 4 | point-in-time features, hash sample |
| 5 | model training, backtest, manifest |
| 6 | full scoring, exact top-K |
| 7 | selectdb schema, publish, swap, rollback, verify-live |
| 8 | container image, compose files, runbook |
| 9 | recrawl frontier, crawler API client, text extraction (not deployed) |
