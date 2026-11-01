# Recrawl design

- Code (implemented, tested, not deployed): `src/isel/recrawl.py`, `src/isel/extract.py`,
  `src/isel/schema.py`. Parameters: `[recrawl]` in `config/isel.toml`.
- Crawler side: the manual bulk crawl API of
  [WebCrawler PR #84](https://github.com/NTU-CSIE5376/WebCrawler/pull/84) at `cc96500`,
  used as is. Its HTTP API and result files are the same as at `bd40191`; the later
  commits change only how each crawler dispatches API requests (§4). That dispatch
  answers both problems of
  [WebCrawler#85](https://github.com/NTU-CSIE5376/WebCrawler/issues/85), and opens two
  new risks (§11).

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
| Crawler limit | the production crawler runs AutoThrottle (target concurrency 2, delay 0.25–10 s, 16 per hostname). API and normal requests for a hostname share one downloader slot, so the recrawl adds no second rate |
| Recrawl rate | isel adds at most 0.208 requests per second per hostname on average: a host's next job waits `docs × min_gap_s` (4.8 s) |
| Crawler pacing | the crawler sends a host's batch at its AutoThrottle slot rate, as it does normal URLs. It has no per-host interval, so the 0.208 req/s is an average over a job cycle, not a cap on any one minute (§4) |
| Audit | per hostname, a 10-minute rate of at most the normal crawl's rate + 0.22 req/s, and at most 67 requests in any 60 s (1.2 × the 56 seen on 2026-10-30) |
| robots.txt | always honoured |

`domain_qps.json` (default delay 5 s) applies only when `CRAWLER_USE_AUTOTHROTTLE` is
off. Every production process sets it on in `supervisord.conf`.

## 3. Capacity

The list is 64M (`eval.k_headline`), as TY Huang asked on 2026-10-27. Figures are for
the batch-20 list.

- The top 64M are 64,000,005 URLs on 1,204,079 hostnames. 14 days need 52.9 req/s,
  9.4 % of the crawler's ~560 req/s since 2026-10-29.
- At 0.2 qps a hostname gets 241,920 requests in 14 days. `www.espn.com` alone has
  638,981 URLs and would need 37 days.
- So `select-full` keeps at most `full.max_host_days` × 86,400 / `min_gap_s` = 13.44 ×
  86,400 / 4.8 = 241,920 URLs per hostname.
- A host waits between its jobs: for the job to complete, the status poll, the archive
  and the next plan. At `min_gap_s` 5 s and 14 days there was no time for that, so a
  capped host needed 14.6–15.3 days. At 4.8 s a host sends 241,920 in 13.44 days of
  sending. In the round-5 simulation (2026-10-31, `index-selection` experiment
  2026-10-29, §9.8) capped hosts got 0.193–0.200 req/s: 14.0–14.5 days. 0.208 qps stays
  within the audit's 0.22. Round 5 ran with the crawler's per-host interval, which is
  now removed. Round 6 (2026-11-01, without it): two capped hosts got 0.196-0.202 req/s
  (13.8-14.5 days); one whose simulated slot is 0.33 req/s got 0.159-0.171 (16.4-17.6
  days). Host2's capped hostnames have slots of 0.54-1.39 req/s. Recrawl used its whole 20 %
  share (7.6 req/s per process against 3.78 needed) and normal kept 91 %. The cap hits 23 hostnames, and the next URLs take their
  places.
- Batch-20 golden coverage (the official `indexed_rate`):

  | List | head | random |
  |---|---|---|
  | 128M | 24.75 % | 23.06 % |
  | 64M, no cap | 23.69 % | 21.99 % |
  | 64M, 241,920 per hostname | 23.58 % | 21.85 % |

- Each of the 20 capped hostnames with normal traffic has at least 0.34 req/s of slot
  rate left, more than the 0.2 the recrawl takes. Per process, the recrawl needs
  2.75–3.96 req/s, 8–12 % of the process's ~33 req/s.

## 4. Architecture: the crawler's job API

- Recrawl is not a second crawler. isel hands URL lists to the production crawler's job
  API, and the crawler fetches them with its normal spider and downloader. Each crawler
  process (PR #84 `docs/03` §3.7):
  - keeps API requests in a second scheduler queue. The crawler env
    `MANUAL_BULK_CRAWL_SCHEDULING_POLICY` picks between the queues: `manual_first` (the
    default as merged) dispatches API requests before normal ones, `balanced` alternates,
    and `normal_first` does the reverse. Jobs carry no policy.
  - admits up to 64 jobs (`MANUAL_BULK_CRAWL_MAX_ACTIVE_JOBS`) in submission order and
    groups their URLs by hostname. A host's URLs keep submission order, by job and index.
  - holds at most the host's slot concurrency of API requests per host (16 in
    production), and at most `CONCURRENT_REQUESTS` in all (256; 384 on the canary
    process 07). Requests that wait for a host's delay count.
  - feeds first the host with the longest estimated remaining time: its URLs left ÷ its
    estimated QPS.
- PR #84 was merged on 2026-10-29 (`5485bcd`) with the rules above. Host2 runs it since
  2026-10-29 17:29Z (`39e1363`): the `api` container listens on `127.0.0.1:4000`, and the
  policy env is unset, so `manual_first` applies. The image rebuild also moved Scrapy to
  2.19.0 (`requirements.txt` pins only `>=2.13`).
- A follow-up fixes the starvation that §11 describes. It is the local WebCrawler branch
  `pr84-weighted-dispatch` (`6d23789`, tests in `f5a7b93`, pacing in `e1ae98d` and `f24189c`) and is
  not proposed upstream yet. It changes four rules and adds logs:
  - `weighted`, the new default policy: while both queues have requests, API requests get
    `MANUAL_BULK_CRAWL_SHARE_PERCENT` (1–99, default 50) of the dispatches. A queue with
    nothing to send gives its turns away and does not bank them.
  - a host holds at most `ceil(latency / delay)` + `MANUAL_BULK_CRAWL_HOST_HEADROOM`
    (default 1) API requests. AutoThrottle keeps the delay at least latency /
    `TARGET_CONCURRENCY`, so the cap follows the target: 3 at Host2's 2.0, 5 at 4.0.
    Few of them wait for the delay.
  - a host's priority adds the seconds its oldest job has waited, so newer, larger hosts
    cannot pass over the small hosts of an old job forever.
  - no per-host interval. A fixed interval (4.8 s, `e1ae98d`) was dropped after review
    (`498a195`): it capped fast hosts below what their slot allows. A batch now goes out
    at the slot's rate, so a host's busiest minute can reach that rate (`www.espn.com`'s
    slot is about 83 a minute). Redirect hops and retries stay on the original slot and
    are not counted in `min_gap_s`: at 0.378 extra requests per doc
    (`www.cnnbrasil.com.br` on the normal crawl) that host sees about 0.29 req/s on
    average, and its 241,920 docs still fit the 13.44 days.
  - `request.start`, `request.end` and `request.fail` carry `source` (`normal` or
    `manual`). The heartbeat adds `dequeued_normal`, `dequeued_manual`, `manual_active`
    and `manual_save_ms`.
- Politeness stays with the crawler's AutoThrottle. On its own side, isel keeps the
  recrawl within 0.208 qps per host: a host's next job waits `docs × min_gap_s` (4.8 s).
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
   `status_poll_s` (15 s); 48 jobs make about three requests a second.
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
   `host_docs_per_job` (240) due docs by rank; jobs hold up to `job_size` (3,000) URLs,
   at most `plan_max_jobs` (8) per cycle. A job lists one host's docs after another; the
   crawler regroups them by host anyway. Docs become `dispatched`, each host gets
   `batch_id` and `next_at = now + docs × min_gap_s`, and the claim commits.
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

A loop, every 15 s. It never changes `result_root`. A spawn pool of `archive_workers`
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
| Text Parquet | ~0.35–0.4 TB per copy of the list; each `stale_days` cycle adds one until compaction exists (§13) | Host2 `/data/isel/text` | permanent |
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
  them. A separate local run against PR #84's own FastAPI app, with its pinned
  requirements (w3lib 2.3.1), and its partition writer at `cc96500`, with two crawlers,
  also passed. It covered a URL the API refused by index, a job that only one crawler
  had finished, which was not applied, and the `DELETE` after the apply.

## 11. Rollout

A **Hard Stop** phase needs the owner's approval before it starts. The R3 audit checks
Loki against the audit row of §2.

| Phase | Content | Precondition |
|---|---|---|
| R0 | schema, sync, plan, archive, extraction, tests, local end-to-end runs (done) | this repository only |
| R1 (Hard Stop) | check `:5445` is at 0002, migrate to 0003; `recrawl-sync` on the live run; `recrawl-plan --dry-run` | the batch-20 swap is done; Host1 has space for the schema |
| R2 | the follow-up of §4 deployed with `MANUAL_BULK_CRAWL_SHARE_PERCENT=20`. #84 alone runs since 2026-10-29 under `manual_first`, so isel must not send jobs before this | crawler running |
| R3 (Hard Stop) | start both loops; a trial on process 03 first, then 1–5M pages; measure HTML and text size, the time from POST to results, and both sources' rates (§11) | R2 |
| R4 | the full list, then only new, due and unfinished pages each period | R3's measurements |

To remove the recrawl: `git revert`, then `alembic downgrade 0002`, which drops the
`recrawl` schema. The text Parquet is not touched.

Known risks:

- **A stopped crawler.** A job's URLs span many crawlers, so nearly every job waits for
  it. Once 48 jobs are open, the planner claims nothing. If the crawler is back within
  the TTL, the jobs complete. If not, they become `lost`, and the results the other
  partitions already fetched are dropped with them.
- **Slots held by waiting requests (fix proposed, not adopted).** As merged, a host may
  hold 16 API requests, but AutoThrottle starts one per delay, so most of them wait. 16
  such hosts fill a process's 256. In a local simulation of one process with production
  settings, 88 % of the slots waited and API requests ran at 6.1 req/s. The per-host cap
  of §4 cut the waiting share to 13 % and raised API requests to 28.3 req/s.
- **Normal crawl starvation (fix proposed, not adopted).** Under `manual_first`, a
  process dispatches a normal request only when no API request waits in its scheduler.
  In the simulation the normal crawl fell to 1 % of its rate; under `balanced` it fell
  to 13 %, because waiting API requests held the slots. On PR #84 the TA asked that
  neither side starve. With the follow-up at a share of 50, the normal crawl kept 56 %
  and API requests got 50.0 % of the dispatches. The simulation and its numbers are in
  index-selection `docs/experiments/2026-10-29-pr84-starvation.md`.
- **R3 trial on process 03.** The API routes a URL to crawler
  `domain_state.shard_id // shards_per_ingestor`. isel reads crawlerdb, so the first
  trial can send only URLs that route to process 03. Then 03 carries all API work and
  the other 15 processes are the control. Compare by process the rates of
  `request.start` with `source` `normal` and `manual` before and after, and the
  heartbeat's `dequeued_*` counters. If the normal crawl on 03 drops below its share,
  lower `MANUAL_BULK_CRAWL_SHARE_PERCENT` or isel's `max_jobs_inflight`.
  - Not 07: it runs 384 concurrent requests and a 150M scraper slot, unlike the other
    15, and it sat idle from 2026-10-26 until the 10-29 restart.
  - 03 has a recrawl demand of 3.78 req/s, near the highest, and three capped hostnames,
    each with normal traffic.

## 12. Rejected alternatives

| Alternative | Reason |
|---|---|
| `crawl-requests` for recrawl | ~128,000 calls, each writing crawlerdb; HTML deleted after 8 h; nothing to track |
| Map results by URL string | the API canonicalizes URLs, so strings miss docs; map by position and check the order |
| A threshold on rewritten URLs to detect a new order | one host fills 240 of 3,000 URLs (8 %), so a fully rewritten host trips it; an exact check is used |
| Read each partition's `progress.json` to decide completion | a copy of the API's `job_status`; PR #84 says only the API decides, the format changed twice in one week, and it would miss the API's expiry check |
| Download results over HTTP | ~6,000 GETs per 3,000-URL job, ~60 MB/s through FastAPI at full rate, and the archive would need the API; isel sits next to the files |
| Scan `crawler_*` directories for result files | they also hold `.json.tmp` and `progress.json`, and crawler ids above 99 have 3 digits; `crawler_ids` in `request.json` locates each file, as the API's download route does |
| Count `completed` jobs in `max_jobs_inflight` | they no longer hold the crawler, so the archive's delay would slow dispatch; a separate cap of the same size is used |
| Our own job timeout plus `DELETE` to cancel | two deadlines to keep in step; the server's TTL already stops a job and answers 404 |
| Apply late results of a `lost` job | after the TTL the server drops them, so none arrive; the code is simpler without it |
| The default 14-day TTL | unapplied results would stay 14 days on `/data`; 3 days covers a job under FIFO, and a longer archive outage only costs a refetch |
| Wait for a whole wave of jobs before the next | the slowest host holds the wave |
| Permanent raw HTML | 5–6 TB against ~2 TB free; the index needs only text |

## 13. Inverted index

`text-index-build` and `text-index-eval` build and measure it locally; the design, the
engine choice and the trial are in [`TEXT_INDEX.md`](TEXT_INDEX.md).

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

Not built yet, for Host2 after WebCrawler#84 is deployed:

- **Live docs only.** Index only docs on the live list (`rank IS NOT NULL`), so a
  dropped doc leaves the next build. Needed before the first list swap after the
  recrawl has run: until then a dropped doc stays searchable, and a golden URL that is
  no longer listed can count as ranked.
- **Compact the text Parquet.** `recrawl-archive` appends one row per fetch and never
  drops the older rows, so every `stale_days` cycle adds about one more copy of the
  text (0.22–0.4 TB at 128M). Needed before the first cycle ends, 30 days after the
  recrawl starts; the text index build reads only each doc's newest row.
- **Words of percent-escaped URLs.** Most CJK and Indic URLs are percent escaped, and
  `url.text` drops the escapes, so those URLs give no words. Indexing the decoded URL
  could add them. Golden batch 20 already chose the query, so this waits for batch 21
  (11-01) to tune and test it.
- **Fallback documents.** A doc without text (`gone`, `failed`, not yet fetched) is
  indexed by its URL (`url.text`) plus crawlerdb's title and anchor text. The full
  export must then pull `title` and `anchor_text`.
- **Recall@10 in the metric.** The official metric hard-codes `ranked_num` to 0;
  `TypesenseRankMeasure` is a stub. `text-index-eval` measures it offline the same way;
  the official metric needs a `RankMeasure` PR.
