# Recrawl design

- Code (implemented, tested, not deployed): `src/isel/recrawl.py`, `src/isel/extract.py`,
  `src/isel/schema.py`. Parameters: `[recrawl]` in `config/isel.toml`.
- Crawler side: the crawler proposal (NTU-CSIE5376/WebCrawler issue, items C1–C10),
  https://github.com/NTU-CSIE5376/WebCrawler/issues/85. Items are cited below as C1 … C10.
- Entry point: the API of
  [WebCrawler PR #84](https://github.com/NTU-CSIE5376/WebCrawler/pull/84).

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
| Scope | the sum of every fetcher: the crawler, recrawl, sitemap patrol, golden patrol |
| robots.txt | always honoured; its requests count against the same budget |

## 3. Capacity

- The list spans 1,677,068 hosts; a host allows 17,280 requests a day. The largest,
  `www.espn.com`, has 656,637 URLs and needs 38 days.
- At a full 0.2 qps everywhere, 14 days would cover 123.0M URLs (96.1 %). A real cycle
  is max(5 s, latency) with a 6.6 s median latency, so about 0.15 qps. With robots.txt
  requests and ~10 % failures, that gives ~108M pages in 14 days.
- 93.8 % of the normal crawl's requests hit listed hosts. A day-by-day simulation with
  C9 pacing gives 112.8M pages in 14 days.
- Expect 108–113M pages in 14 days. Large hosts are the bottleneck, not bandwidth.

## 4. Architecture: one fetch plane, one host gate

- A 5 s gap needs the time of the host's last request from any process. Per-process
  Scrapy slots miss it after a cross-host redirect (Scrapy 2.16 keeps the original
  slot), a routing change, a stale routing snapshot or a restart.
- So recrawl is a second input lane of the crawler, not a second crawler. Both lanes
  pass one cross-process gate: an atomic check-and-set per hostname in SQLite on tmpfs
  (C2). isel's routing only picks the process.
- isel never writes `/data/ipc` or crawlerdb. The API writes queue files; the crawler
  writes and prunes result files; isel only reads them.

```
selection run ──recrawl-sync──► selectdb recrawl.{host,doc,log}
                                   │ recrawl-plan (loop)
                                   ▼
  POST 127.0.0.1:4000/scheduler_ingest/v1/recrawl-batches   (PR #84 gateway, C10)
                                   ▼
  /data/ipc/recrawl_queue/crawler_NN/domain_<host_id>.json
                                   ▼
  crawler process NN: normal lane + recrawl lane, both behind the host gate
                                   ▼
  /data/ipc/recrawl_result/crawler_NN/recrawl-NN-<ts>Z-<seq>.{warc.gz,jsonl}
          │ recrawl-archive (loop, read-only)          │ crawler deletes after 24 h
          ▼
  /data/isel/text/fetch_date=YYYYMMDD/crawler_NN/<base>.parquet   (text, permanent)
  /data/isel/log/YYYYMMDD/crawler_NN/<base>.jsonl.gz              (fetch log, permanent)
```

| Items | Crawler-side change |
|---|---|
| C1–C4 | 1 request in flight and 5 s per host, no AutoThrottle (C1); the host gate, slots keyed by the URL's hostname (C2); robots.txt with `Crawl-delay` ≤ 60 s, 5xx as disallow, 24 h cache (C3); 429/503 slow the host (C4) |
| C5–C6 | recrawl lane: its own queue directory, watermark and exclusion set; output as WARC (one gzip member per record) plus a fetch log |
| C7–C8 | every other fetcher uses the gate; `recrawl_result` kept 24 h under a size cap |
| C9 | per host, weighted round robin between the lanes by the batch's `recrawl_share` |
| C10 | `recrawl-batches`, a new resource on PR #84's API that writes queue files for isel |

URLs from PR #84's `crawl-requests` use the normal lane and pass the same gate.

## 5. Frontier schema (`schema.py`, revision `0003_recrawl`)

| Table | One row per | Main columns |
|---|---|---|
| `recrawl.host` | hostname (a back queue) | `host_id` (low 63 bits of `md5_number_lower(host)`), `host`, `crawl_shard`, `best_rank`, `next_at`, `batch_id` |
| `recrawl.doc` | `doc_id` (signed BIGINT) | `url` (canonical), `host_id`, `rank`, `model_run_id`, `state`, `due_at`, `attempts`, `batch_id`, `dispatched_at`, fetch fields, `text_file`, `text_len`, `lang` |
| `recrawl.log` | applied fetch log | `name`, `processed_at`, `records`, `error` |
| `recrawl.batch_seq` | (sequence) | source of `batch_id` |

- `state` is `pending`, `dispatched`, `fetched`, `failed` or `gone`; `pending` and
  `failed` docs are due after `due_at`. A doc is live while `rank IS NOT NULL`. Sync
  writes only `rank` and `model_run_id`; archive writes only fetch fields.
- Partial indexes: `host_ready (best_rank)` and `host_queued (host_id)` split hosts by
  `batch_id`; `doc_due (host_id, rank)` covers due live docs; `doc_inflight (batch_id)`
  covers `dispatched`; `doc_stale (due_at)` covers live `fetched` docs.
- Every frontier transaction takes one advisory lock, so sync, plan and archive never
  interleave or deadlock. An instance lock keeps each command to one copy.

## 6. `recrawl-sync`

Run once after each swap; it refuses to run in the swap freeze window. It reads the live
run's manifest (one row per `doc_id`, its best rank) and upserts `host` and `doc`, one
transaction per 1M rows. Docs that left the list get `rank = NULL`, `best_rank` is
recomputed, and hosts of new or re-ranked docs lose their backoff `next_at` (as in §7).
A new doc's `due_at` is the sync time, which starts its pacing deadline, so run sync
shortly before the planner starts.

## 7. `recrawl-plan`

A loop, every 30 s. No HTTP call runs inside a lock or a transaction (outbox order):
batches are claimed and committed, then sent, then released if refused.

1. `GET recrawl-batches` returns the queued `host_id`s. On any error the cycle changes
   nothing; an error never means "nothing queued".
2. Transaction T1:
   - **Taken.** A host with `batch_id` that is no longer queued: the crawler took the
     batch. Stamp its docs' `dispatched_at` and clear `host.batch_id`.
   - **Timeout.** Docs taken over `dispatch_timeout_s` (8 h) ago without a result return
     to `pending`; `attempts` does not grow. 100 URLs at a 60 s `Crawl-delay` and share
     0.25 take 24,000 s, so a shorter timeout would resend running batches.
   - **Sweep.** Up to `sweep_rows` (100,000) `fetched` docs past `due_at` return to
     `pending`. For those hosts, clear a `next_at` later than `now + host_pause_max_s`:
     no throttle pause lasts that long, so it only guarded docs in backoff.
   - **Backpressure.** Claim nothing while the API pause (`api_backoff_s`, 300 s) runs,
     while less than `min_free_gb` (800 GB) is free under `text_root`, or while
     `recrawl_result` holds more than `result_cap_gb` (1,000 GB).
   - **Claim.** Each process has `queue_files_per_dir` (384) minus its queued batches
     free slots, from the GET list and `crawl_shard`. It takes up to that many ready
     hosts by `best_rank`; all processes together take at most `plan_max_batches`
     (1,000). A full process thus draws no 429s and blocks no other process.
   - Each host gets up to `batch_size` (100) due docs by rank. They become `dispatched`,
     `host.batch_id` is set, and T1 commits.
3. `POST recrawl-batches` with `{batch_id, crawler_id, host_id, recrawl_share, docs:
   [{doc_id, url}]}`. isel computes `crawler_id` (`crawl_shard // 16`) and `host_id`.
   Transaction T2 then returns refused and unsent batches' docs to `pending`.

| Response | Verdict | Rest of the cycle |
|---|---|---|
| 2xx | queued | continue |
| 409 | released: the host already has a queued batch | continue |
| 429 | released: the process is full | skip that `crawler_id`; others continue |
| 400 with non-empty `rejected` | released; rejected docs become `gone` (`invalid_url`) | continue |
| other 4xx (incl. 400 with empty `rejected`), 503 | released; `api_error`; pause `api_backoff_s` | stop; unsent released |
| other 5xx, other non-2xx below 400, timeout, connection error | unknown: the batch stays claimed | stop; unsent released |

The next GET or the timeout settles an unknown batch, so nothing is sent twice.

**Pacing.** `pace_share` asks for the share that finishes a host within `pace_days` of
its oldest due doc. `backlog` is the host's due docs; `since` is their oldest `due_at`.

```
share = clamp(backlog × pace_cycle_s / max(since + pace_days − now, plan_interval_s),
              share_min, share_max)        # 6.7 s, 14 days, 0.25, 0.75
```

Past the deadline the share is 0.75. The count stops at `pace_count_cap`, which already
yields 0.75, so a large host never counts 656k rows. `--dry-run` only prints capacity.

## 8. `recrawl-archive`

A loop, every 60 s. It never changes `result_root`. A spawn pool of `archive_workers`
(8) processes does each log's file work; the main process applies results in order.

1. Take each `.jsonl` not in `recrawl.log`. Skip it while its WARC is missing.
2. Check every line: exactly the 16 C6 keys, their types, timestamps with a UTC offset,
   `http_status` in 100–599, `doc_id = md5(source_url)`. One bad line records the log in
   `recrawl.log` with `error`, and the archive moves on instead of crash-looping.
3. An `OSError` on the log or the WARC (permission, EIO, pruned mid-read) records
   nothing. The log is retried next cycle, so no text is lost.
4. Per `doc_id`, a 2xx record beats a failure, then the latest `fetched_at` wins.
5. For each 2xx HTML record, read the gzip member at `(warc_offset, warc_length)` and
   check that it is a `response` record with `WARC-Target-URI = final_url`. With a wrong
   offset, FastWARC silently reads a neighbouring or truncated record.
6. Resiliparse extracts the title, `<meta name=description>`, the main-content body
   (full text if all boilerplate; at most 200,000 chars) and the language. A parse
   failure (`extract_error`) or missing WARC fields (`no_warc_record`) retry that record.
7. Write one zstd Parquet per log under `text_root`, then a gzipped log copy under
   `log_root`, the source for rebuilding the frontier. Then apply the results and insert
   the `recrawl.log` row in one transaction:

| Result | New state | Next attempt |
|---|---|---|
| 2xx HTML | `fetched`; `text_file`, `text_len`, `lang` set | after `stale_days` (30) |
| 2xx non-HTML, 404, 410 | `gone` | never |
| robots disallow, `Crawl-delay` too long | `failed` | after `robots_retry_days` (30) |
| 429, 503 | `failed`, `attempts + 1`; host paused for `Retry-After` (default `retry_base_s`, cap `host_pause_max_s` = 24 h) | exponential backoff |
| other failure, extraction failure | `failed`, `attempts + 1` | `retry_base_s · 2^attempts`, cap 24 h; `stale_days` after `max_attempts` (5) |

- A 2xx applies whenever it is newer than the stored fetch, whatever batch the doc waits
  on. A failure applies only to the batch the doc waits on.
- Files come before the DB, and the log row comes last, so a rerun of a log is a no-op.

**Text Parquet.** `doc_id` (uint64, as in the manifest), `batch_id`, `url`, `final_url`,
`fetched_at`, `http_status`, `lang`, `title`, `description`, `body`, `text_len`,
`payload_sha1`. `recrawl.doc` stores the same `doc_id` as a signed BIGINT.

## 9. Storage

| Data | Size | Location | Kept |
|---|---|---|---|
| Queue files | ≤ 384 per directory | Host2 `/data/ipc/recrawl_queue` (API) | until the crawler reads them |
| WARC + fetch logs | ~0.9 TB/day at peak (~21M pages/day), 0.33 TB/day average | Host2 `/data/ipc/recrawl_result` | 24 h staging; log deleted before WARC |
| Text Parquet | ~0.35–0.4 TB | Host2 `/data/isel/text` | permanent |
| Gzipped fetch logs | ~8 GB | Host2 `/data/isel/log` | permanent; selectdb has no backup |
| `recrawl` schema | ~37 GB (not measured) | Host1 selectdb `:5445` | resident |

- Host2 `/data` has about 2 TB free while the crawler runs, so a 5–6 TB WARC archive
  does not fit. The text estimate uses Common Crawl WET (~2.8 KiB per page).
- Extraction, measured on 9 real pages of 590 KB on average: 6.5 ms per page per
  process, about 13.2M pages a day. That is below the 21M peak, so 8 workers run,
  about 45.6M pages a day. A backlog over 24 h loses logs to C8 pruning.

## 10. Deployment (`deploy/compose.compute.yaml`)

- `recrawl-plan`: `network_mode: host`, because the gateway listens on `127.0.0.1:4000`
  only; API token from the secret `recrawl_api_token`; 2 CPUs, 4 GB.
- `recrawl-archive`: 8 CPUs for `archive_workers`, 8 GB.
- Both: profile `recrawl`, `restart: unless-stopped`, `/data/ipc/recrawl_result`
  read-only, `/data/isel` read-write. Run `mkdir -p /data/isel` as the isel user before
  `up`, or Docker creates it as root. Start them only after the crawler serves C5, C6, C10.
- `tests/recrawl_fakes.py` holds a reference stub of the C10 API and a fake crawler;
  the integration tests run the whole loop against them.

## 11. Rollout

A **Hard Stop** phase needs the owner's approval before it starts. The R2 audit checks
Loki: at most 120 requests per host in any 10 minutes.

| Phase | Content | Precondition |
|---|---|---|
| R0 | schema, sync, plan, archive, extraction, tests, proposal, local end-to-end run (done) | this repository only |
| R1 (Hard Stop) | `recrawl-sync` on the live run; `recrawl-plan --dry-run` capacity report | the batch-20 swap is done; Host1 has space for the schema |
| R2 | the crawler team builds C1–C7 and C10; an audit follows | crawler team agrees; crawler running; PR #84 merged |
| R3 (Hard Stop) | start both loops; trial of 1–5M pages; measure WARC and text size | R2 |
| R4 | the full list, then only new, due and unfinished pages each period | C8 retention and cap agreed |

## 12. Rejected alternatives

| Alternative | Reason |
|---|---|
| `crawl-requests` for recrawl | ~128,000 calls, each writing crawlerdb; HTML deleted after 8 h; nothing to track |
| A mode flag on `crawl-requests` | different semantics: one edits crawlerdb's frontier untracked; the other must not touch crawlerdb and returns `batch_id`, `doc_id` |
| isel writes queue files | `/data/ipc` is root 755; no shared entry point with PR #84 |
| The API computes routing and `host_id` | routing needs crawlerdb's split list; two MD5 byte-order implementations drift |
| Permanent WARC | 5–6 TB against ~2 TB free; the index needs only text |
| C10 normalizes URLs | breaks `doc_id = md5(source_url)`; `validators.url` rejects canonical `\|` and `[ ]`; w3lib 2.3.1 vs 2.5.0 |
| A fixed lane ratio (3:1) | same 14-day total (113.0M vs 112.8M), but the normal lane's peak loss is 14.0 % vs 8.5 % |
| Strict priority for recrawl | the normal lane stalls for 14+ days on the 10 largest hosts |

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
