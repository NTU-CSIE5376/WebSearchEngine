# Recrawl design

- Code (implemented, tested, not deployed): `src/isel/recrawl.py`, `src/isel/extract.py`,
  `src/isel/schema.py`. Parameters: `[recrawl]` in `config/isel.toml`.
- Crawler side: the manual bulk crawl API of
  [WebCrawler PR #84](https://github.com/NTU-CSIE5376/WebCrawler/pull/84) at `bd40191`,
  used as is. The one open crawler-side problem, API requests on busy hosts, is
  [WebCrawler#85](https://github.com/NTU-CSIE5376/WebCrawler/issues/85).

## 1. Why recrawl

- An inverted index needs page text. The crawler keeps HTML only in
  `/data/ipc/crawl_result`, and a root cron on Host2 deletes those files after 8 hours.
- crawlerdb keeps only `content_hash`, `content_length` and `title`. The crawler never
  re-fetches a page: nothing sets `should_crawl` back to TRUE.
- 55 % of the 128M list was last fetched before June 2026.

## 2. Rule

| Rule | Value |
|---|---|
| Unit | the full hostname; `en.wikipedia.org` and `zh.wikipedia.org` count separately |
| Rate | at most 0.2 requests per second per host, so requests at least 5 s apart |
| Scope | the sum of every fetcher: the crawler (normal and API requests), sitemap patrol, golden patrol |
| robots.txt | always honoured; its requests count against the same budget |

## 3. Capacity

- The list spans 1,677,068 hosts; a host allows 17,280 requests a day. The largest,
  `www.espn.com`, has 656,637 URLs and needs 38 days.
- At a full 0.2 qps everywhere, 14 days would cover 123.0M URLs (96.1 %). A real cycle
  is max(5 s, latency) with a 6.6 s median latency, so about 0.15 qps. With robots.txt
  requests and ~10 % failures, that gives ~108M pages in 14 days.
- 93.8 % of the normal crawl's requests hit listed hosts, and API requests yield to them
  on a shared host (WebCrawler#85). Large hosts are the bottleneck, not bandwidth.
- So 128M cannot finish in 14 days. The goal is as many pages as possible, best rank
  first.

## 4. Architecture: the crawler's job API

- Recrawl is not a second crawler. isel hands URL lists to the production crawler's job
  API, and the crawler fetches them with its normal spider at priority 0. Each crawler
  process keeps at most 128 API requests in flight (`max_concurrency_ratio` 0.5 of 256),
  queued requests included. That is a ceiling, not a floor.
- Politeness stays with the crawler's AutoThrottle. On its own side, isel keeps the
  recrawl within 0.2 qps per host: a host's next job waits `docs × min_gap_s` (5 s).
- isel never writes crawlerdb, `/data/ipc` or `/data/manual_bulk_crawls`. It reads
  results read-only and deletes them only through the API.

```
selection run ──recrawl-sync──► selectdb recrawl.{host,doc}
                                   │ recrawl-plan (loop)
                                   ▼
  POST 127.0.0.1:4000/v1/manual-bulk-crawls {"urls": [...], "ttl_days": 3} -> job_id
                                   ▼
  /data/manual_bulk_crawls/pending/{job_id}.json + pending/crawler_NN/{job_id}.json
                                   │ each crawler takes its part, FIFO
                                   ▼
  /data/manual_bulk_crawls/results/{job_id}/request.json
  /data/manual_bulk_crawls/results/{job_id}/crawler_NN/{NNNNN.json,NNNNN.html,progress.json}
          │ recrawl-plan: GET until completed, DELETE once applied
          │ recrawl-archive (loop, read-only, completed jobs only)
          ▼
  /data/isel/text/fetch_date=YYYYMMDD/job-<batch_id>.parquet   (text, permanent)
  /data/isel/log/YYYYMMDD/job-<batch_id>.jsonl.gz              (per-URL results, permanent)
```

- The API routes each URL to the crawler that owns its domain. At POST time it creates
  `results/{job_id}/`, `request.json` and one empty partition per crawler, so a directory
  does not mean the job is done.
- Each crawler writes only its own partition and sets `completed_at` in its
  `progress.json` when its part is done. Only the API's `GET` decides that the job is
  `completed`: every partition must be done. There is no `status.json` on disk (PR #84
  `docs/03` §3.7).
- `request.json` lists the URLs as the API canonicalized and deduplicated them, in the
  order sent, and `crawler_ids`, the crawler of each URL. Result N is
  `crawler_{crawler_ids[N]:02d}/NNNNN.json`, with its HTML in the same partition. The
  API's download route resolves files the same way.
- A job expires `ttl_days` after the POST. Past that, the crawler drops it, `GET` answers
  404, and the API's hourly GC deletes its files.

## 5. Frontier schema (`schema.py`, revision `0003_recrawl`)

| Table | One row per | Main columns |
|---|---|---|
| `recrawl.host` | hostname (a back queue) | `host_id` (low 63 bits of `md5_number_lower(host)`), `host`, `best_rank`, `next_at`, `batch_id` (its open job) |
| `recrawl.doc` | `doc_id` (signed BIGINT) | `url` (canonical), `host_id`, `rank`, `model_run_id`, `state`, `due_at`, `attempts`, `batch_id`, fetch fields, `text_file`, `text_len`, `lang` |
| `recrawl.job` | job sent to the API | `batch_id`, `job_id`, `doc_ids` (in the order sent), `state`, timestamps, `deleted_at`, `records`, `error` |
| `recrawl.batch_seq` | (sequence) | source of `batch_id` |

- Doc `state` is `pending`, `dispatched`, `fetched`, `failed` or `gone`; `pending` and
  `failed` docs are due after `due_at`. A doc is live while `rank IS NOT NULL`. Sync
  writes only `rank` and `model_run_id`; archive writes only fetch fields.
- Job `state`:

| State | Meaning | Its docs |
|---|---|---|
| `claimed` | committed; the POST has no clear answer yet | `dispatched` |
| `submitted` | the API returned a `job_id` | `dispatched` |
| `completed` | the API's `GET` reported it done; `completed_at` is the API's value | `dispatched` |
| `error` | the results break the contract (e.g. a new order); the planner halts | `dispatched` |
| `applied` | results applied; `deleted_at` is set once the API deleted them | — |
| `lost` | no results will come: refused, no reply, or unknown to the API (TTL passed) | back to `pending` |

- A job's only deadline is `result_ttl_days` (3), sent as `ttl_days`. The server stops
  an expired job; isel has no timeout of its own.
- Partial indexes: `host_ready (best_rank)` and `host_queued (batch_id)` split hosts by
  open job; `doc_due (host_id, rank)` covers due live docs; `doc_inflight (batch_id)`
  covers `dispatched`; `doc_stale (due_at)` covers live `fetched` docs; `job_open
  (state)` and `job_undeleted (batch_id)` cover jobs still to watch or delete.
- Every frontier transaction takes one advisory lock, so sync, plan and archive never
  interleave or deadlock. An instance lock keeps each command to one copy.

## 6. `recrawl-sync`

Run once after each swap; it refuses to run in the swap freeze window. It reads the live
run's manifest (one row per `doc_id`, its best rank) and upserts `host` and `doc`, one
transaction per 1M rows. Docs that left the list get `rank = NULL`, `best_rank` is
recomputed, and hosts of new or re-ranked docs lose their backoff `next_at`.

## 7. `recrawl-plan`

A loop, every 30 s. No HTTP call runs inside a transaction: jobs are claimed and
committed, then sent.

1. **Recover.** A `claimed` job older than `claim_timeout_s` (600 s) is `lost` and its
   docs return to `pending`. Docs and hosts held by a job that is no longer open are
   released. Up to `sweep_rows` (100,000) `fetched` docs past `due_at` return to
   `pending`.
2. **Poll.** Each `submitted`, `completed` or `error` job gets a `GET` at most every
   `status_poll_s` (60 s); 48 jobs make about one request a second.
   - `completed` while the job is `submitted`: mark it `completed` with the API's
     `completed_at`.
   - `completed` but no results directory: the job stays `submitted`; report
     `config_error` (wrong mount) and claim nothing.
   - 404: `lost`, docs back to `pending`. This covers an expired job, an `error` job left
     unfixed past its TTL, and a `completed` job the archive did not apply in time.
   - `pending` or `running`: record `checked_at`.
   - An answer that cannot be used (5xx, a bad body): report `api_error` and skip only
     this job, so one broken job cannot hold back the others.
   - No connection: report `api_error` and end the round, so a dead API costs one
     timeout.
3. **Delete.** Every `applied` job without `deleted_at` gets a `DELETE`. 204 or 404 sets
   `deleted_at`; anything else reports `api_error` and is tried next cycle. This runs even
   while the planner is halted.
4. **Stop** claiming if a job is in `error`, if a completed job's results are not
   mounted, during the `api_backoff_s` (300 s) pause, if `max_jobs_inflight` (48) jobs are
   at the crawler (`claimed` + `submitted`), if as many `completed` jobs wait for the
   archive (`archive_lag`), or if less than `min_free_gb` (800 GB) is free under
   `text_root`.
5. **Claim.** Ready hosts (no open job, `next_at` passed) by `best_rank`. Each gives up to
   `host_docs_per_job` (120) due docs by rank; jobs hold up to `job_size` (3,000) URLs,
   at most `plan_max_jobs` (8) per cycle. Inside a job the hosts' URLs alternate, so the
   crawler's 128 API slots per process spread over many hosts. Docs become `dispatched`,
   each host gets `batch_id` and `next_at = now + docs × min_gap_s`, and the claim
   commits.
6. **Send** each job and record the answer:

| Response | Verdict |
|---|---|
| 200 with a `job_id` | `submitted` |
| 400 "Invalid HTTP(S) URL at index N" | doc N becomes `gone` (`invalid_url`); the rest is sent again, at most `max_resend` (20) times |
| other 400, 415, 500, 503 | nothing accepted: `lost`, docs released; pause `api_backoff_s`; unsent jobs released |
| no reply, or 200 without a usable `job_id` | unknown: the job stays `claimed` until the claim timeout; unsent jobs released |

An unknown job may have been accepted. isel never learns its `job_id`, so the TTL
removes its results. `--dry-run` only prints the per-host capacity report.

## 8. `recrawl-archive`

A loop, every 60 s. It never changes `result_root`. A spawn pool of `archive_workers`
(8) processes does each job's file work; the main process applies results in order.

1. Take each `completed` or `error` job whose `result_root/{job_id}` exists. The archive
   never calls the API.
2. Read `request.json` (`urls` and `crawler_ids`, one crawler per URL) and, for every
   listed position N, `NNNNN.json` in partition `crawler_{crawler_ids[N]:02d}`. Check
   each record: required keys and types, `url` equal to `request.json`'s N-th URL,
   `html_file` only `NNNNN.html`, timestamps with a UTC offset.
3. Map results to docs (`map_results`):
   - Same length: result i is doc i. If a URL differs and equals another sent URL, the
     order changed: a contract breach. Canonicalization alone is fine (PR #84's w3lib
     2.3.1 turns `%25C3%25A9` into `%C3%A9`, so strings cannot be used).
   - Shorter (the API dropped duplicates): pair only identical strings. An unmatched doc
     is retried (`unmatched`); alone in its next job, it pairs by position.
4. A contract breach marks the job `error` and writes nothing; the planner halts until a
   fix lets a later cycle apply it. An `OSError` (permission, EIO, directory gone)
   changes nothing, and the job is retried next cycle.
5. For each 2xx HTML result, Resiliparse extracts the title, `<meta name=description>`,
   the main-content body (full text if all boilerplate; at most 200,000 chars) and the
   language, from the raw bytes or their `<meta charset>`.
6. Write one zstd Parquet per job under `text_root` and a gzipped copy of the per-URL
   results under `log_root`, dated by the job's `completed_at`, so a rerun
   overwrites the same files. Then apply in one transaction, marking the job `applied`
   last:

| Result | New state | Next attempt |
|---|---|---|
| 2xx HTML | `fetched`; `text_file`, `text_len`, `lang`, `payload_sha1` set | after `stale_days` (30) |
| 2xx non-HTML, 404, 410 | `gone` | never |
| `IgnoreRequest` (robots.txt) | `failed` | after `robots_retry_days` (30) |
| 429, 503 | `failed`, `attempts + 1`; host paused `retry_base_s` | exponential backoff |
| other failure, `unmatched`, extraction failure | `failed`, `attempts + 1` | `retry_base_s · 2^attempts`, cap 24 h; `stale_days` after `max_attempts` (5) |

- A 2xx applies whenever it is newer than the stored fetch. A failure applies only while
  the doc still waits on this job. A doc of the job with no result is retried
  (`no_result`).

**Text Parquet.** `doc_id` (uint64, as in the manifest), `batch_id`, `url`, `final_url`,
`fetched_at`, `http_status`, `lang`, `title`, `description`, `body`, `text_len`,
`payload_sha1`. `recrawl.doc` stores the same `doc_id` as a signed BIGINT.

## 9. Storage

| Data | Size | Location | Kept |
|---|---|---|---|
| Pending job files | one manifest per open job, plus one marker per crawler in it | Host2 `/data/manual_bulk_crawls/pending` | a marker until its crawler finishes; the manifest until DELETE or the TTL |
| Raw HTML + per-URL JSON | 128M pages are 5–6 TB even gzipped; ≤ ~288k pages at once (48 jobs at the crawler, 48 waiting for the archive) | Host2 `/data/manual_bulk_crawls/results` | deleted by isel once applied; the 3-day TTL removes the rest |
| Text Parquet | ~0.35–0.4 TB | Host2 `/data/isel/text` | permanent |
| Gzipped per-URL results | ~8 GB | Host2 `/data/isel/log` | permanent; selectdb has no backup |
| `recrawl` schema | ~37 GB (not measured) | Host1 selectdb `:5445` | resident |

- Host2 `/data` has about 2 TB free while the crawler runs, so raw HTML cannot be kept.
  The text estimate uses Common Crawl WET (~2.8 KiB per page).
- Extraction, measured on 9 real pages of 590 KB on average: 6.5 ms per page per
  process, so 8 workers handle about 45.6M pages a day.

## 10. Deployment (`deploy/compose.compute.yaml`)

- `recrawl-plan`: `network_mode: host`, because the API listens on `127.0.0.1:4000`
  only; the API has no auth, so no token. It also sends the DELETEs. 2 CPUs, 4 GB.
- `recrawl-archive`: 8 CPUs for `archive_workers`, 8 GB.
- Both: profile `recrawl`, `restart: unless-stopped`, `/data/manual_bulk_crawls/results`
  read-only, `/data/isel` read-write. Run `mkdir -p /data/isel` as the isel user before
  `up`, or Docker creates it as root. Start them only after WebCrawler#84 (`bd40191` or
  later) is merged and deployed.
- `tests/recrawl_fakes.py` holds a stub of the API and a fake crawler that write
  per-crawler partitions like PR #84; the integration tests run the whole loop against
  them. A separate local run against PR #84's own FastAPI app and partition writer at
  `bd40191`, with two crawlers, also passed. It covered a job that only one crawler had
  finished, which was not applied.

## 11. Rollout

A **Hard Stop** phase needs the owner's approval before it starts. The R3 audit checks
Loki: at most 120 requests per host in any 10 minutes.

| Phase | Content | Precondition |
|---|---|---|
| R0 | schema, sync, plan, archive, extraction, tests, local end-to-end runs (done) | this repository only |
| R1 (Hard Stop) | check `:5445` is at 0002, migrate to 0003; `recrawl-sync` on the live run; `recrawl-plan --dry-run` | the batch-20 swap is done; Host1 has space for the schema |
| R2 | WebCrawler#84 merged and deployed | crawler running |
| R3 (Hard Stop) | start both loops; trial of 1–5M pages; measure HTML and text size and the time from POST to results | R2 |
| R4 | the full list, then only new, due and unfinished pages each period | R3's measurements |

To remove the recrawl: `git revert`, then `alembic downgrade 0002`, which drops the
`recrawl` schema. The text Parquet is not touched.

Known risks:

- **A stopped crawler.** A job's URLs span many crawlers, so nearly every job waits for
  it. Once 48 jobs are open, the planner claims nothing. If the crawler is back within
  the TTL, the jobs complete. If not, they become `lost`, and the results the other
  partitions already fetched are dropped with them.
- **Head-of-line in the API.** The 128 slots count requests queued in the scheduler,
  fill by job and index order, and have no per-host cap. By rank, each of the first 50
  jobs holds only ~29 hosts, about 2 per crawler over 16 crawlers. If AutoThrottle holds
  a large host near 0.3 qps, the total is about 10 req/s, far below the 106 req/s that
  14 days need. A smaller `host_docs_per_job` alone leaves the large hosts unfinished
  under the rule of one open job per host. The candidate fix lets a host sit in several
  jobs, paced by `next_at`; it waits for Loki or trial measurements.
- **Lane allocation** between normal and API requests is WebCrawler#85.

## 12. Rejected alternatives

| Alternative | Reason |
|---|---|
| `crawl-requests` for recrawl | ~128,000 calls, each writing crawlerdb; HTML deleted after 8 h; nothing to track |
| Map results by URL string | the API canonicalizes URLs, so strings miss docs; map by position and check the order |
| A threshold on rewritten URLs to detect a new order | one host fills 120 of 3,000 URLs (4 %), so a fully rewritten host trips it; an exact check is used |
| Read each partition's `progress.json` to decide completion | a copy of the API's `job_status`; PR #84 says only the API decides, the format changed twice in one week, and it would miss the API's expiry check |
| Download results over HTTP | ~6,000 GETs per 3,000-URL job, ~60 MB/s through FastAPI at full rate, and the archive would need the API; isel sits next to the files |
| Scan `crawler_*` directories for result files | they also hold `.json.tmp` and `progress.json`, and crawler ids above 99 have 3 digits; `crawler_ids` in `request.json` locates each file, as the API's download route does |
| Count `completed` jobs in `max_jobs_inflight` | they no longer hold the crawler, so the archive's delay would slow dispatch; a separate cap of the same size is used |
| Our own job timeout plus `DELETE` to cancel | two deadlines to keep in step; the server's TTL already stops a job and answers 404 |
| Apply late results of a `lost` job | after the TTL the server drops them, so none arrive; the code is simpler without it |
| The default 14-day TTL | unapplied results would stay 14 days on `/data`; 3 days covers a job under FIFO, and a longer archive outage only costs a refetch |
| Wait for a whole wave of jobs before the next | the slowest host holds the wave |
| Permanent raw HTML | 5–6 TB against ~2 TB free; the index needs only text |

## 13. Inverted index (planned)

This round only makes sure recrawl keeps enough data to build one.

- Source: the text Parquet only (`**/*.parquet`; skip `.parquet.tmp`). Per doc, keep the
  latest `fetched_at`. Index only live docs (`rank IS NOT NULL`), so ranked ≤ indexed.
- Engine: Quickwit, which keeps the index on disk instead of RAM. Typesense would need an
  estimated 100–200 GB RAM for 128M documents. Size: ~0.2 TB compact BM25, ~1.2 TB full.

| Field | Use |
|---|---|
| `url`, `canonical_url`, `final_url` | aliases; ~23 % of crawled golden pages are redirect sources |
| `title`, `description`, `body` | full text, BM25 |
| `lang` | tokenizer choice; golden keywords come from 10 regions, CJK included |

- **Fallback documents.** A doc without text (`gone`, `failed`, not yet fetched) is
  indexed by its URL tokens plus crawlerdb's title and anchor text. The full export must
  then pull `title` and `anchor_text`. The index covers all 128M URLs from day one.
- **Recall@10.** The official metric hard-codes `ranked_num` to 0; `TypesenseRankMeasure`
  is a stub. So measure offline: query each batch's golden keywords and match the top
  10 canonical URLs against golden URLs. The official metric needs a `RankMeasure` PR.
