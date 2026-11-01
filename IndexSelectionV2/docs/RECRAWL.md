# Recrawl design

- Code (implemented, tested, not deployed): `src/isel/recrawl.py`, `src/isel/extract.py`.
  Parameters: `[recrawl]` in `config/isel.toml`.
- Crawler side: the bulk crawl v2 API of WebCrawler (branch `bulk-crawl-v2`, on top of
  PR #106). It replaces PR #84's job API; the contract is in WebCrawler `docs/03` and in
  the spec `bulk-crawl-v2-server-spec.md`. The client spec is
  `bulk-crawl-v2-client-spec.md`.
- isel keeps no database for the recrawl. Every piece of state is a file with one
  writer (§5). selectdb is read only for the live run.

## 1. Why recrawl

- An inverted index needs page text. The crawler keeps HTML only in
  `/data/ipc/crawl_result`, and a root cron on Host2 deletes those files after 8 hours.
- crawlerdb keeps only `content_hash`, `content_length` and `title`. The crawler never
  re-fetches a page: nothing sets `should_crawl` back to TRUE.
- 55 % of the 128M list was last fetched before June 2026.

## 2. Who decides what

| Decision | Owner |
|---|---|
| Which pages, in which order (rank) | isel: the live run's manifest |
| When a page is due again (revisit, retry backoff, robots, gone) | isel: the outcome files (§6) |
| When a host is fetched and how fast | the crawler: per-slot queues, LPT on the whole backlog, PR #106's per-host cap and weighted share, AutoThrottle, 429/503 blocks |
| How much the crawler may hold for isel | the crawler: 503 + `Retry-After` on POST, a result quota on unacked segments |

isel sets no rate, packs no jobs and locks no host. robots.txt is always honoured.

## 3. Capacity

The list is 64M (`eval.k_headline`), as TY Huang asked on 2026-10-27. Figures are for
the batch-20 list.

- The top 64M are 64,000,005 URLs on 1,204,079 hostnames. 14 days need 52.9 req/s,
  9.4 % of the crawler's ~560 req/s before the YugabyteDB cutover.
- `select-full` takes the top 64M by score with no per-hostname cap; the crawler's pace
  decides how far each host gets. `www.espn.com`, the largest, has 638,981 URLs: 14 days
  need 0.53 req/s. Below that, its lowest-ranked URLs are fetched after the 14 days,
  not dropped.
- Under v1 a host had one open job of 240 docs, and its next job waited for the whole
  job, so a big host was capped near 240 ÷ the job's lifetime (main7b: p50 1,175 s,
  about 0.20 docs/s). v2 has no jobs: the crawler sees the whole backlog of each slot.
- Batch-20 golden coverage (the official `indexed_rate`):

  | List | head | random |
  |---|---|---|
  | 128M | 24.75 % | 23.06 % |
  | 64M, no cap | 23.69 % | 21.99 % |
  | 64M, 241,920 per hostname | 23.58 % | 21.85 % |

- `recrawl-report` shows progress from the outcome files: docs left, docs fetched in
  the last day and the days left at that pace, next to the crawler's own counts.

## 4. Flow

```
selectdb public.model_runs (live run) ──► work/full/selection/run=<label>/manifest/*.parquet
                                              │ recrawl-push (loop)
                                              ▼
  due/<request>/due-<ts>.parquet ──► push/<request>/<ts>-<n>.parquet ──► POST /urls (<= 10k)
                                              │ the crawler fetches by its own schedule
                                              ▼
  GET /segments (long poll) ──► recrawl-pull ──► text/ + outcome/ ──► POST /ack
                                              │
                                              ▼
                               due_query (next push), text-index-build
```

- One crawl request per live run (`isel-run-<id>`, `expires_days` 30). A new live run
  `DELETE`s the old request, which cancels its queue; its unacked segments, including
  the `cancelled` records, stay until pull acks them.
- Each URL goes out as `{id: str(doc_id), url: canonical_url, priority: rank}`. The
  crawler canonicalizes URLs; results map back by `id` only.
- The client never seals a request: retries of the same list keep going into it.

## 5. Files and writers

All under `recrawl.root` (`/data/isel/recrawl` in the container); every file is written
through a tmp file and a rename.

| File | Content | Writer |
|---|---|---|
| `request.json` | `request_id`, `model_run_id`, `run_label`, `created_at` | push |
| `due/<request>/due-<ts>.parquet` | `doc_id`, `url`, `rank` of the docs to send, by rank; `due-<ts>.done` once all batches are sent | push |
| `push/<request>/<ts>-<n>.parquet` | `doc_id`, `pushed_at` of batch n, written before its POST | push |
| `outcome/fetch_date=D/push-*.parquet` | URLs the API skipped as invalid (`gone`, `invalid_url`) | push |
| `outcome/fetch_date=D/pull-*.parquet` | one row per result record | pull |
| `text/fetch_date=D/pull-*.parquet` | `TEXT_SCHEMA`: `doc_id`, `source`, `url`, `final_url`, `fetched_at`, `http_status`, `lang`, `title`, `description`, `body`, `text_len`, `payload_sha1` | pull |
| `outcome/`, `text/` `compact-*.parquet` | merged days before yesterday | compact |

- Outcome rows: `doc_id` (u64), `request_id`, `outcome`, `fail_reason`, `http_status`,
  `final_url`, `fetched_at`, `finished_at` (the crawler's time), `payload_sha1`,
  `text_len`, `lang`.
- A pull file is named by a hash of its segment names, so the same segments read twice
  rewrite the same file. Readers take distinct `(doc_id, request_id, outcome,
  finished_at)`, so a duplicate row from a cut-short compaction or a re-batched segment
  changes nothing. The text index takes each doc's newest `fetched_at`.
- `doc_id` is u64 everywhere: manifest, outcome, text and OpenSearch `_id`.

## 6. When a doc is due (`due_query`)

| Last outcome (by `finished_at`) | From the record | Due again |
|---|---|---|
| none | — | now |
| `fetched` | no `error`, HTML text extracted | `finished_at` + `stale_days` (30) |
| `retry` | `http_<code>` (not 404/410), `dns`, `timeout`, `connection`, `retries_exhausted`, no HTML, extraction failure | `finished_at` + `min(retry_base_s · 2^(n−1), retry_max_s)`; `stale_days` once n ≥ `max_attempts` (5). n counts the retries since the last `fetched` |
| `robots` | `robots` | `finished_at` + `robots_retry_days` (30) |
| `gone` | `non_html`, `http_404`, `http_410`, skipped `invalid` | never |
| `requeue` | `expired`, `cancelled` | now; not counted in n |

A doc is in flight, and not due, while the current request's newest push of it has no
outcome of the same request with `finished_at` ≥ `pushed_at` − `clock_skew_s` (300 s,
validated below `retry_base_s`). Only the current request counts: a late `cancelled` of
the old request cannot free a doc already queued in the new one. If the crawler ever
lost a record, the doc stays in flight until the next request, at most one list cycle.

## 7. `recrawl-push`

A loop; each cycle names its own wait.

1. Read the live run (admin, read-only session). If it differs from `request.json`:
   validate the run's manifest, `DELETE` the old request, create a new one.
2. If the newest due file is sent (`.done`) and younger than `due_interval_h` (24 h),
   wait. Otherwise compute a new due file, or continue the unsent one.
3. Send batches of `batch_urls` (10,000) in order. Before each POST, wait out the
   `[guard]` windows (`pg.next_allowed_start`: the 1st and 16th 00:00–20:00 and every
   hour :58–:10), because each POST makes the API look up `domain_state` on YugabyteDB.
4. Write the push file (only if it does not exist: a batch sent again keeps its first
   time), then POST with `batch_id` `<ts>-<n>`.

| Answer | Verdict |
|---|---|
| 202 | next batch; `skipped` `invalid` items become `gone` outcomes, `duplicate` items need nothing |
| 503, 5xx, no answer | stop; wait `Retry-After` or `api_backoff_s` (300 s); the same batch goes again with the same `batch_id`, and the API answers it once |
| 401, 404, 409, 400 | delete that batch's push file (it never reached the crawler) and fail |

On restart, the last batch with a push file is sent again, then the rest.

## 8. `recrawl-pull`

A loop. It stops below `min_free_gb` (800 GB) free under `root`; the crawler's result
quota then fills and its manual dispatch for isel stops.

1. List this tenant's requests; take those with unacked segments and the current one.
   Long-poll (`long_poll_s`, 30 s) only when the current request is the only one.
2. Take segments in order up to `pull_batch_segments` (64) or `pull_batch_mb` (256 MB
   of gzip; a segment unpacks to several times its size and the pull has 8 GB). Wait
   until the batch is full or its oldest segment has waited `pull_batch_s` (300 s);
   `--once` takes what is there.
3. Download them, check every record (keys, `id` a u64, status, types, UTC times),
   extract the text of HTML records in `archive_workers` (8) spawn processes
   (Resiliparse: title, description, main-content body up to 200,000 chars, language),
   write text, then outcome, then ack the segments by name.
4. A record that breaks the contract stops the pull before the ack; the segment stays.

## 9. `recrawl-compact` and `recrawl-report`

- `recrawl-compact` (monthly) merges the outcome and text files of days before
  yesterday into one file each. Outcome keeps each doc's latest `fetched` row and every
  row after it, which is all `due_query` reads; text keeps each doc's newest row. It
  writes the new file before deleting the old ones and skips a kind when free space is
  below the inputs' size plus `min_free_gb`. It also deletes push dirs of requests other
  than the current one.
- `recrawl-report` prints `request.json`, the crawler's status of the current request,
  and from the outcome files: the list size, never fetched, fetched within `stale_days`,
  retry, gone, robots, fetched in the last day and `days_left`.

## 10. Placement and storage

| Data | Size | Location |
|---|---|---|
| `work_dir` (manifests, golden) and the compute jobs | live manifest 8.9 GB | Host3 `/data/isel/work` |
| due, push, outcome | ~8 GB a cycle before compaction | Host3 `/data/isel/recrawl` |
| Text Parquet | ~0.2 TB per copy of the 64M list; compaction keeps one | Host3 `/data/isel/recrawl/text` |
| OpenSearch index | 0.07–0.12 TB at 64M, twice that while building | Host3 |
| Results at the crawler | at most `result_quota_gb` (100 GB) unacked | Host2 `/data/manual_bulk_crawls` |
| selectdb | unchanged; no recrawl tables | Host1 `:5445` |

- Host3 `/data` also holds a YugabyteDB tserver of crawlerdb (RF 1), so a full disk
  stops crawlerdb; `min_free_gb` 800 protects it.
- Extraction, measured on 9 real pages of 590 KB on average: 6.5 ms per page per process,
  so 8 workers handle about 45.6M pages a day.

## 11. Deployment (`deploy/compose.compute.yaml`)

- `recrawl-push`: 4 CPUs, 8 GB; `recrawl-pull`: 8 CPUs, 8 GB. Profile `recrawl`,
  `restart: unless-stopped`, `/data/isel` read-write, secret `recrawl_api_token`.
  Both reach the API over HTTP at `http://172.16.191.2:4000`; no host network, no mount
  of the crawler's files.
- Run `mkdir -p /data/isel/recrawl` as the isel user before `up`.
- `tests/recrawl_fakes.py` serves the v2 contract over HTTP; `tests/test_recrawl.py`
  and the integration test run push, pull, compact and report against it.

## 12. Rollout

| Phase | Content | Precondition |
|---|---|---|
| R0 | push, pull, compact, report, tests (done) | this repository only |
| R1 (Hard Stop) | WebCrawler v2 deployed with `MANUAL_BULK_CRAWL_SHARE_PERCENT=20`; the API on Host2's LAN address with a token for isel | the crawler owners agree |
| R2 (Hard Stop) | `work_dir` and the compute jobs on Host3; `recrawl-push --once` on a trial list | R1 |
| R3 (Hard Stop) | both loops on the full list; measure text size, time from push to result, both sources' rates and `days_left` | R2 |

To remove the recrawl: `git revert`; stop the loops. The files under `recrawl.root` are
not touched, and selectdb never changed.

## 13. Rejected alternatives

| Alternative | Reason |
|---|---|
| A `recrawl` schema in selectdb (v1, migration `0003`) | its host lock, job states and indexes served client-side scheduling, which v2 gives the crawler; what is left is derivable from append-only files, and 64M rows of updates per cycle would share the Postgres the metric reads |
| A separate Postgres for the recrawl | a second service for the same purpose |
| Upload the whole list at once and fetch results per job | the crawler holds results until a job ends, ~12.7 TB of raw HTML for 64M pages against ~2.5 TB free on Host2 |
| Ack by cursor | 16 crawler processes write segments in parallel, so there is no single order; ack by name |
| Map results by URL string | the crawler canonicalizes URLs; map by the item `id` |
| Track "submitted" per doc in a table | the push files record it, and the crawler sends a final record for every accepted URL |
| Seal the request after the first upload | retries of the same list go into the same request |
| A per-hostname list cap from an assumed rate (241,920 at 4.8 s) | the rate is the crawler's to decide and is unknown before the crawl |
| Permanent raw HTML | 5–6 TB against the free space; the index needs only text |

## 14. Inverted index

`text-index-build` and `text-index-eval` build and measure it; the design, the engine
choice and the trial are in [`TEXT_INDEX.md`](TEXT_INDEX.md).

- Source: the text Parquet only (`**/*.parquet`; skip `.parquet.tmp`). Per doc, keep the
  latest `fetched_at`.
- Engine: OpenSearch with ICU analysis. Quickwit ranked lower on Japanese, Hindi and
  Bengali in the trial, and Typesense keeps the whole index in RAM: about 1.1–1.65 TB for
  128M pages of body text.

| Field | Use |
|---|---|
| `_id` | the doc id; a hit matches a golden URL only by it. A golden URL that redirects is on the list under its own URL, so it still matches |
| `doc_id` | the same id as doc values only; it breaks score ties, so every build returns the same top 10 |
| `url` | displayed; its words are searchable as `url.text` |
| `title`, `description`, `body` | full text, one ICU analyzer; `best_fields` scores a doc by its best field, `url.text` included. The body has no positions (`index_options: freqs`): the query never uses them, and they were 45 % of the index |
| `lang` | stored; the trial measured what per-language analyzers would add |

Not built yet:

- **Live docs only.** Index only docs on the live run's manifest, so a dropped doc leaves
  the next build.
- **Words of percent-escaped URLs.** Most CJK and Indic URLs are percent escaped, and
  `url.text` drops the escapes, so those URLs give no words.
- **Fallback documents.** A doc without text is indexed by its URL plus crawlerdb's title
  and anchor text. The full export must then pull `title` and `anchor_text`.
- **Recall@10 in the metric.** The official metric hard-codes `ranked_num` to 0;
  `text-index-eval` measures it offline the same way; the official metric needs a
  `RankMeasure` PR.
