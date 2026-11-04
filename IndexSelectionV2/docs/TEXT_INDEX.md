# Text index

The text index makes the selected list searchable, so the metric's `ranked_rate`
(Recall@10 on golden keywords) can stop being 0. This page records the engine choice,
the design, and the local trial that backs both.

Status: engine decided (OpenSearch, see [Decision](#decision)); built and measured
locally on 2026-10-23. The query and a URL field were then tuned on golden batch 20's
own pages ([Golden batch 20 with its own pages](#golden-batch-20-with-its-own-pages)).
Not deployed. A real Recall@10 needs the list's page text, which arrives only after
WebCrawler#84 is deployed and `recrawl-archive` runs (`docs/RECRAWL.md`). A review the
same evening indexed the body without positions, fixed the build's disk peak and its
failure handling, added the Host2 services, and measured batch 20 as the official
metric would count it ([Golden batch 20 as the official metric counts
it](#golden-batch-20-as-the-official-metric-counts-it)).

## Commands

| Command | Does |
|---|---|
| `text-index-build [--text-root DIR]` | loads the newest text of each doc into a new index `isel-text-<utc id>`, checks the count, merges it, blocks writes, then moves the alias `isel-text` to it in one call |
| `text-index-eval --batch N` | searches each golden keyword of batch N on the alias and reports Recall@10 against its golden URLs |

Local engine: `OS_HEAP=3g docker compose -f deploy/compose.opensearch.yaml up -d --build`.

## Design

- **Input.** The text Parquet that `recrawl-archive` writes (`TEXT_SCHEMA`). A first
  DuckDB pass picks each doc's newest row without the bodies, so 128M page bodies are
  never sorted; a second pass reads only the winners. A row with text beats any row
  without it: about 620,000 rows hold an empty WAF page from before revision `0004`. Such
  a doc stays in the index, where the words of its URL can find it.
- **Document.** `_id` is the doc id in decimal. `url` and `lang` are stored; `title`,
  `description` and `body` are full text, and `body` is left out of `_source`. The body
  is indexed without positions (`index_options: freqs`): BM25 needs only term counts,
  and positions were 45 % of the index. The URL's words are searchable as `url.text`.
  `doc_id` holds the same id as doc values only, for sorting.
  `config/text_index/icu.json` holds the settings and mappings.
- **Analysis.** One analyzer for every text field and language: `icu_tokenizer` (Unicode
  word breaks with dictionaries for CJK and Thai) and `icu_folding` (case, accents,
  width). A keyword's language is unknown, so per-language analyzers would need routing;
  the trial measures what they would add. `url.text` uses the same two steps after a
  character filter turns each run of punctuation and percent escapes into one space.
- **Query.** `best_fields` over `title^2`, `description`, `body` and `url.text`, top 10:
  a doc scores by its best-matching field. The type and weights were chosen on half of
  golden batch 20's keywords and confirmed on the other half ([Tuning on batch
  20](#tuning-on-batch-20)). The keyword is never parsed as query syntax. Equal scores
  are ordered by `doc_id`.
  Lucene would order them by internal doc id, which concurrent loading assigns
  differently in every build. Anserini fixed the same problem the same way ([Lin and
  Yang, SIGIR 2019](https://arxiv.org/abs/1807.05798)).
- **Build.** A new index per build, in bulk-load mode (no replica, no refresh, async
  translog).
  - One build runs at a time: it holds a lock file in the work dir. It first deletes
    indices that killed builds left, and checks that the disk can take twice the
    expected index and stay out of the flood stage.
  - `_bulk` requests of 8 MiB, two in flight, resent whole on 429, 503 or a lost reply.
    Reads are resent the same way; requests that must not run twice (create, delete,
    alias move, merge) are not.
  - After the load: refresh, check that the index holds every doc, flush, wait until the
    retention leases pass the last operation (or stop), check the disk again for one
    more copy of the index, commit again, force merge to one segment and check the merge
    task. Every poll of the task refreshes, so each merged shard frees its old segments
    at once. Then restore refresh and durability, and block writes.
  - One `_aliases` call moves the alias. If its reply is lost, the alias shows whether it
    moved. A failure before the move deletes the new index and leaves the alias as it
    was. With `keep_prev`, the previous index stays for rollback; older ones are
    deleted, and newer ones (another build's) never.
- **Eval.** For each golden keyword, the top 10 ids are compared with the doc ids of its
  golden URLs. `recall_pairs` counts (keyword, URL) pairs, as the metric's old rank
  code did, with never-crawled URLs in the denominator; `recall_unique` counts distinct
  URLs, as the coverage tables do. Both have a 95 % interval that resamples keywords; in
  the distinct-URL one, a URL golden for c keywords weighs 1/c in each. `presence` is
  the share of golden pairs whose page is in the index, the ceiling of both.

## Decision rule (written before the NanoMIRACL runs, 2026-10-23 05:40 UTC)

OpenSearch is chosen if Q, S and M all hold; otherwise Quickwit v0.9.3.

- **Q, quality.** The one primary comparison is OpenSearch with `config/text_index/icu.json`
  (OS-ICU) against Quickwit with the `chinese_compatible` tokenizer (QW-CC), on
  NanoMIRACL nDCG@10. The languages are weighted by the scripts of batch 20's golden
  keywords: en 0.745, zh 0.092, ja 0.055, de 0.029, fr 0.029, es 0.029, hi 0.020,
  bn 0.002 (Tamil, Arabic, Cyrillic and Thai keywords, 1.3 %, have no NanoMIRACL
  split). Q holds if the 95 % interval of weighted nDCG@10(OS-ICU) − nDCG@10(QW-CC)
  has a lower bound ≥ −0.02. The interval comes from a paired bootstrap that resamples
  queries within each language, 10,000 draws.
- **S, size.** OpenSearch's projected size for 128M documents fits Host2's budget B,
  computed on the decision day: free space on `/data` before the hourly prune, minus
  0.4 TB of text Parquet, minus the recrawl's HTML staging, minus 10 % headroom.
  Indexing the body without positions ranks the same, so it may be used to pass S.
- **M, memory.** Host2 can give OpenSearch a 16–31 GB heap.

Other variants (per-language analyzers, `cjk_bigram`, folding settings) are
exploratory. One is adopted only if it gains on its target languages and loses on none.

## Results

### Quality: NanoMIRACL

- **Data.** `hakari-bench/NanoMIRACL` at revision `ea97b8c`: per language 200 MIRACL dev
  questions and 10,000 Wikipedia passages (all judged positives, hard negatives, random
  filler). The passage text starts with its article title, so it is indexed as `body`.
  The data is not committed; its card defers to MIRACL's license (Apache-2.0).
- **Gate.** `rankeval.ndcg_at_k` on the split's own BM25 run reproduces the published
  nDCG@10 of all 8 languages to 6 decimals, so the metric code matches theirs.
- **Runs.** One index per variant and language, built by `textindex.build` and searched
  by `textindex.search` (OpenSearch 3.9.0), or by Quickwit 0.9.3 with `sort_by: _score`
  and the question split into quoted terms joined by OR. The runs predate the `doc_id`
  tie-break and the build fixes in [Found on the way](#found-on-the-way); `icu` rerun
  with the final code scores the same on 1,598 of 1,600 questions.

nDCG@10:

| Variant | en | zh | ja | de | fr | es | hi | bn | weighted | macro | weighted R@10 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| OS `standard` (Lucene default) | 0.640 | 0.519 | 0.711 | 0.483 | 0.432 | 0.640 | 0.530 | 0.544 | 0.620 | 0.562 | 0.870 |
| **OS `icu` (chosen)** | 0.640 | 0.487 | 0.686 | 0.481 | 0.452 | 0.654 | 0.554 | 0.560 | **0.617** | 0.564 | 0.869 |
| OS `icu`, no folding of Indic | 0.640 | 0.487 | 0.686 | 0.481 | 0.452 | 0.654 | 0.533 | 0.544 | 0.617 | 0.560 | 0.868 |
| OS `icu` + `cjk_bigram` | 0.640 | 0.471 | 0.648 | 0.481 | 0.452 | 0.654 | 0.554 | 0.560 | 0.614 | 0.558 | 0.864 |
| OS per-language analyzer | 0.681 | 0.470 | 0.648 | 0.534 | 0.486 | 0.697 | 0.591 | 0.613 | 0.649 | 0.590 | 0.914 |
| **QW `chinese_compatible`** | 0.645 | 0.519 | 0.426 | 0.471 | 0.443 | 0.639 | 0.517 | 0.534 | **0.607** | 0.524 | 0.853 |
| QW `default` | 0.645 | 0.000 | 0.017 | 0.471 | 0.443 | 0.639 | 0.528 | 0.534 | 0.537 | 0.410 | 0.750 |
| Published BM25 (bm25s, tokenizer per language) | 0.677 | 0.402 | 0.660 | 0.517 | 0.466 | 0.686 | 0.304 | 0.503 | | | |

- **Primary comparison: Q holds.** Weighted nDCG@10 of OS-ICU minus QW-CC is +0.010,
  95 % interval [+0.001, +0.017]. Per language: ja +0.260 [0.202, 0.319], hi +0.037,
  bn +0.026, es +0.016 favor OpenSearch; zh −0.032 [−0.064, −0.001] favors Quickwit;
  en, de and fr are within noise.
- **Japanese decides it.** Quickwit's `chinese_compatible` splits Han characters one by
  one but keeps a run of kana as one token up to the next Han character, so
  `オリンピックの` is a single term that a query for `オリンピック` never matches. Its
  `default` tokenizer keeps whole CJK runs and finds nothing in zh and ja.
- **Per-language analyzers would add +0.031** [+0.012, +0.051], from stemming in en, de,
  fr, es, hi and bn; they lose on ja (−0.038). English alone has weight 0.745, so an
  English-stemmed subfield next to the ICU fields is the next thing to try.
- **Not adopted.** `cjk_bigram` loses on ja and zh. Keeping Indic marks unfolded loses on
  hi (−0.021) and bn (−0.016). Folding drops the virama (`प्रधान` is indexed as
  `परधान`); on this data that helps more than it hurts. `standard` ties overall
  (−0.003, interval spans 0) but loses on fr, es, hi and bn.
- **Weights.** The weights came from a script count that put 34 Telugu, Malayalam,
  Kannada, Gujarati and Greek keywords under Latin, so de, fr and es got 165 keywords
  instead of 131. With the corrected count the primary difference is still +0.010,
  95 % interval [+0.001, +0.017], so Q holds either way.
- **Zero hits and latency.** No OpenSearch variant left a question without hits. All
  engines answer in 1–3 ms at the median on 10,000 passages, which says nothing about
  128M documents.

### Size and speed: 50k crawled pages

TB is 10^12 bytes; `df -h` on Host2 prints TiB.

- **Corpus.** 36 crawl_result files from Host2, two or three per crawler and 70 min to
  6 h old, streamed read-only on 2026-10-23 between 05:30 and 06:04 UTC. All 36 matched
  the server's sha256. Of 48,536 records, 4,108 were not `ok`; the rest gave 44,428
  rows and 44,028 docs, as 400 URLs were fetched twice.
- **Text per doc.** Mean 4,114 bytes, median 2,061, p90 8,460, p99 36,990. 97.5 % of the
  pages have a title and 70 % a description; 0.07 % reach the 200,000-character cap.
  Languages: en 41 %, ja 9.6 %, zh 8.2 %, de 6.3 %, fr 6.3 %, undetected 4.3 %, pt 3.1 %,
  es 2.4 %. The text Parquet takes 1,729 bytes per row.
- **Index size.** One segment per index, measured from the live segments' files. Every
  variant but the last dropped the recovery source.

| Variant | Docs | Bytes/doc | Per text byte | 128M docs |
|---|---|---|---|---|
| **body `freqs`, with `url.text`: the settings now** | 44,028 | 1,003 | 0.244 | **0.13 TB** |
| `icu` with `url.text`, body with positions | 44,028 | 1,840 | 0.447 | 0.24 TB |
| `icu` before `url.text`, the settings of the decision | 44,028 | 1,791 | 0.435 | 0.23 TB |
| `icu`, every 4th file | 10,270 | 1,841 | 0.479 | |
| `icu`, every 2nd file | 21,978 | 1,860 | 0.453 | |
| body without positions (`index_options: freqs`) | 44,028 | 953 | 0.232 | 0.12 TB |
| `zstd` instead of `best_compression` | 44,028 | 1,796 | 0.437 | 0.23 TB |
| body kept in `_source` | 44,028 | 3,232 | 0.786 | 0.41 TB |
| no wait for the leases | 44,028 | 3,263 | 0.793 | 0.42 TB |

- **Projection.** 128M × T × r, with T = max(4,114 sample, 2,867 WET) = 4,114 bytes, gives
  0.23 TB for `icu`, the same as bytes per doc × 128M. Bytes per text byte fall as the
  sample grows (0.479, 0.453, 0.435), so this is an upper estimate.
- **Positions** are 47 % of the index. Without them, all 1,948 batch-20 keywords return
  the same top-10 ids and scores (`c50k_freqs_rank.py`); only phrase queries need them.
  The body has been indexed without them since the 10-23 review, after the same check
  with the tuned query on the 54k golden index (`freqs_check.py`): same top-10 ids and
  scores for all 1,948 keywords, at 1,072 bytes per doc against 2,165. NanoMIRACL scores
  the same on all 1,600 questions (`nm_freqs_check.py`). The first such build after a
  restart of the local Docker VM kept every id but moved the scores of 955 keywords
  slightly and was 0.13 % larger; three later builds matched exactly
  (`freqs_scores.py`), and the cause was not found.
- **`url.text`** adds 55 bytes per doc (+3.1 %), measured again with and without it on
  the same 44,028 docs (1,785 → 1,840).
- **Other settings.** `zstd` gains nothing over `best_compression`. Keeping the body in
  `_source` adds 80 %, and skipping the lease wait adds 82 %. `doc_id` adds 8 bytes per
  doc.
- **Speed.** The Mac loads 3,000–6,000 docs/s (Docker, 3 GB heap, two bulk requests in
  flight). A 44k build takes 32–64 s, mostly the lease wait. At 4,800 docs/s one node
  would need about 7.4 h for 128M docs. Host2 was not measured.

### S and the shared disk

- **B = 0.82 TB** = 0.9 × (1.32 − 0.40 − 0.014):
  - Free space before the hourly prune: the lowest `fs_avail` before a prune in
    `/data/ipc/logs/prune_crawl_result.log` from 2026-10-22 07:00 to 2026-10-23
    06:00 UTC, 1.2 TiB (1.32 TB).
  - Text Parquet: 0.4 TB, as in `RECRAWL.md`. This sample's Parquet would take 0.22 TB.
  - HTML staging: at most ~288k pages at once, about 0.014 TB.
- **S holds**: `icu` needs 0.23 TB, and 0.24 TB with `url.text`; the body without
  positions now needs 0.13 TB.
- **A rebuild needs more than one index**, which the rule did not cover. A build holds
  the serving index, the new index as loaded, and the merged copy that the force merge
  writes. The loaded index may still carry the recovery source: 0.19 TB at 128M as an
  upper bound, as at 44k docs. Over a load of hours the leases advance and natural
  merges drop most of it, which gives the lower bound.
- **Until the 10-23 review, the merge held a whole extra copy.** Each merged shard kept
  its old segments until the one refresh at the end, so the peak was three indices, and
  the table here first missed the merged copy. Now every poll of the merge refreshes,
  and Host2 merges one shard at a time, so the merge adds one shard (1/8 of the index).

| TB at 128M docs, no `keep_prev` | One index | First build | Rebuild before the review | Rebuild now |
|---|---|---|---|---|
| **body `freqs`, with `url.text`: the settings now** | 0.13 | 0.14–0.33 | 0.38–0.57 | **0.27–0.46** |
| `icu` with `url.text`, body with positions | 0.24 | 0.27–0.45 | 0.71–0.90 | 0.50–0.69 |

`keep_prev` adds one more index to every column but the first.

- **Host2 on 10-23 at 15:00 UTC** (read-only):
  - `/data` holds 7.62 TB, is 77 % used and has 1.73 TB free; ext4 also keeps 0.38 TB
    for root.
  - 5.42 TB of it is the crawler's last 8 hours in `/data/ipc/crawl_result`: 2.48 TB of
    crawl results and 2.94 TB of router output for the ingestor. The hourly prune
    deletes about 0.66 TB.
  - Free space before the prune since the crawler restarted on 10-20: 1.1–1.3 TiB from
    10-20 to 10-22, 1.3–1.6 TiB on 10-23. After the flood stage, the text and the
    staging, that leaves 0.41 TB for the index on the worst day and 0.63 TB at 10-23's
    low.
  - 380 GB of 811 GB memory available, swap 7.5 of 8 GB used, 144 cores,
    `vm.max_map_count` 1,048,576. Docker's root is on `/` with 367 GiB free, so the
    index needs a bind mount on `/data`.
- **The flood stage is the hard limit.** It applies to a single node and counts the
  crawler's files on `/data`. No build fits with `keep_prev`. The body without positions
  fits even on the worst day unless more than 0.14 TB of recovery source survives the
  load; `icu` with positions fits only at 10-23's level. The flood stage blocks writes
  but not a merge that is running, so the build checks the free space itself before it
  loads and before it merges, and stops instead of crossing it. The serving index stays
  readable either way.

### Golden batch 20 on the crawl sample

- **Presence.** The 44k pages hold 3 of the 15,328 golden pairs (0.02 %, all random).
  None of the 3 is in its keyword's top 10, so Recall@10 is 0. This is expected for
  pages not chosen from the list; the run checks the pipeline, zero hits and latency.
- **Zero hits.** 5.1 % of the 1,948 keywords get no hit (head 2.4 %, random 7.6 %):

| Script | Keywords | No hit |
|---|---|---|
| ASCII | 1,433 | 3.8 % |
| Han | 177 | 1.1 % |
| Latin with diacritics | 131 | 3.8 % |
| Kana | 106 | 7.5 % |
| Devanagari | 38 | 13 % |
| Tamil | 20 | 35 % |
| Telugu | 14 | 71 % |
| Malayalam | 10 | 40 % |
| Kannada | 6 | 50 % |
| Bengali, Gujarati, Arabic, Cyrillic, Greek, Thai | 13 | 1 of 13 |

- The misses gather in South Indian scripts, which the sample barely covers: of 44,428
  rows, 197 are Tamil, 51 Telugu, 54 Malayalam and 44 Kannada. Every missed keyword
  checked is analyzed into whole words, for example `帝国ホテル` → `帝国ホテル`,
  `tubarões` → `tubaroes`, and `సునీల్ గవాస్కర్` → two tokens.
- **Latency.** p50 1 ms and p95 2 ms on 44k docs, with the `doc_id` sort.

### Golden batch 20 with its own pages

The crawl sample holds almost no golden page, so it cannot rank one. Here the golden
pages themselves are fetched and indexed among the crawl pages.

- **Fetch.** A Mac fetched each of batch 20's 14,822 golden URLs once on 2026-10-23,
  08:01–10:01 UTC (`golden_fetch.py`). It sent at most one request per host every 5 s
  (0.2 qps, robots.txt included, as the recrawl rule), obeyed robots.txt, made no
  retries, and named the course in its User-Agent. A 2xx HTML page became one
  `TEXT_SCHEMA` row through `isel.extract`. Its `url` and `doc_id` come from the golden
  canonical URL, so a hit matches as in `text-index-eval`.

| Outcome | URLs |
|---|---|
| fetched | 10,345 |
| disallowed by robots.txt | 3,628: facebook 737, instagram 694, x 304, reddit 262, imdb 216, … |
| HTTP error | 547, of them 403: 322 |
| network or TLS error | 264 |
| not HTML | 38 |

- **Pages.** Median text 2,196 characters; 2,263 pages have fewer than 200. YouTube's
  1,150 pages are script shells with the title and description only (median 49).
- **Index.** The 10,345 pages plus the 44,028 crawl pages, 54,370 docs. A golden page
  competes with every other page, the golden pages of other keywords included.
- **Metric.** Recall given present: found pairs / pairs whose page is in the index. The
  fetched pages cover 10,702 of the 15,328 pairs (69.8 %; head 70.1 %, random 69.6 %).
  The official pairs recall is this recall × that coverage.

First runs (`golden_eval.py`), with the settings before tuning unless the row says
otherwise. The interval is a paired bootstrap over keywords against the first row,
10,000 draws:

| Variant | Docs | Recall given present | head | random | nDCG@10 | Δ |
|---|---|---|---|---|---|---|
| `icu`, `combined_fields` (before tuning) | 54,370 | **0.769** | 0.734 | 0.801 | 0.609 | |
| no crawl pages | 10,345 | 0.806 | 0.774 | 0.836 | 0.642 | +0.037 [+0.032, +0.043] |
| every 4th crawl file | 20,614 | 0.798 | 0.764 | 0.830 | 0.634 | +0.028 [+0.024, +0.033] |
| every 2nd crawl file | 32,320 | 0.785 | 0.750 | 0.817 | 0.623 | +0.016 [+0.013, +0.019] |
| `standard` analyzer | 54,370 | 0.755 | 0.720 | 0.788 | 0.600 | −0.014 [−0.018, −0.010] |
| `most_fields` | 54,370 | 0.780 | 0.750 | 0.808 | 0.624 | +0.011 [+0.004, +0.018] |
| `most_fields`, English subfields on all three fields | 54,370 | 0.785 | 0.758 | 0.811 | 0.628 | +0.016 [+0.009, +0.023] |

- **Size of the index.** Recall falls by 0.8 points over the first doubling of the docs
  and by about 2 points per doubling after it. The absolute numbers hold for 54k docs,
  not for 128M.
- **ICU** beats `standard` here as well.
- **English subfields** on the body make the index 1.88 times as large (3,977 bytes per
  doc against 2,115), out of reach of the disk budget.
- **Where `icu` misses.** By text length: 0.834 at 2,000 characters or more, 0.594
  under 200. Of the top-10 slots, 43.8 % hold a golden page of the keyword, 23.8 % a
  golden page of another keyword, and 32.4 % a crawl page. Typical misses: a long page
  that repeats one keyword word (`champions league` → a club page on ESPN, not
  uefa.com), a video page with no text, and Google results that are not the only right
  answer (`oregon school shooting` → the Wikipedia article of that name).

#### Tuning on batch 20

The rule was written at 10:26 UTC, before any split numbers (`tune_rule.md`):

- **Split.** A keyword is in `test` if `int(sha256(keyword), 16)` is odd, else in `dev`
  (988 and 960 query ids with a present pair; head and random ids of one keyword stay
  together).
- **Selection on dev only.** Query types and title weights; then index candidates, each
  with the best query: light stemming (`kstem`, `porter_stem`) in the analyzer,
  English-stemmed `title` and `description` subfields, and the URL's words. An index
  candidate may add at most 10 % bytes per doc.
- **One comparison on test.** The final settings minus those before tuning. They are
  adopted if the 95 % interval (paired bootstrap over keywords, 10,000 draws) is above
  0 and no keyword script group has its interval entirely below 0.
- **Deviation.** `best_fields` with title weights 1 and 3 were added after the dev
  numbers showed that the title weight matters. The test half was not read until the
  final choice.

Recall given present (`tune_report.py`; Δ on all keywords is descriptive):

| Index | Query | Size | dev | test | all | Δ all |
|---|---|---|---|---|---|---|
| `icu` | `combined_fields`, `title^2` (before tuning) | | 0.777 | 0.761 | 0.769 | |
| `icu` | `combined_fields`, `title^3` | | 0.783 | 0.767 | 0.775 | +0.006 |
| `icu` | `most_fields`, `title` | | 0.769 | 0.751 | 0.760 | −0.009 |
| `icu` | `most_fields`, `title^2` | | 0.787 | 0.773 | 0.780 | +0.011 |
| `icu` | `most_fields`, `title^3` | | 0.796 | 0.783 | 0.790 | +0.021 |
| `icu` | `most_fields`, `title^2`, `description^0.5` | | 0.794 | 0.782 | 0.788 | +0.019 |
| `icu` | `best_fields`, `title` | | 0.746 | 0.730 | 0.738 | −0.031 |
| `icu` | **`best_fields`, `title^2`** | | **0.803** | 0.800 | 0.802 | +0.032 |
| `icu` | `best_fields`, `title^3` | | 0.797 | 0.792 | 0.794 | +0.025 |
| `kstem` | `best_fields`, `title^2` | −1.7 % | 0.803 | 0.802 | 0.802 | +0.033 |
| `porter_stem` | `best_fields`, `title^2` | −2.9 % | 0.803 | 0.798 | 0.800 | +0.031 |
| English `title`, `description` subfields | `best_fields`, `title^2`, `title.en^2`, `description.en` | +5.7 % | 0.803 | 0.801 | 0.802 | +0.033 |
| **URL words** | **`best_fields`, `title^2`, `url.text`** | **+2.3 %** | **0.815** | **0.811** | **0.813** | **+0.044** |

- **Dev.** `best_fields` with `title^2` wins the query step (+0.026 [+0.017, +0.036]
  over the query before tuning). With it, stemming changes nothing (−0.001), so the
  analyzer stays as NanoMIRACL chose it. The URL words add +0.011 [+0.008, +0.014] on
  top, at 2.3 % more bytes per doc on these pages.
- **Test: adopted.** +0.050 [+0.040, +0.061]. Per script: ASCII +0.058, Han +0.042,
  Latin with diacritics +0.039, kana +0.018 [−0.009, +0.051], others +0.008
  [0, +0.021]; no interval lies below 0.
- **All keywords**, through `text-index-build` and `text-index-eval` with the new
  config: recall given present 0.769 → 0.813 (head 0.734 → 0.787, random 0.801 →
  0.836), pairs recall 0.537 → 0.568, nDCG@10 0.609 → 0.647. Zero hits stay at 1 of
  1,948 keywords, latency at p50 1 ms and p95 2 ms.
- **Why it works.** `best_fields` scores a doc by its best field, so a page whose title
  states the keyword beats a long page that repeats one of its words. News and sports
  URLs carry the slug (`dodgers-brewers`, `bills-vs-rams-highlights-week-5`), and
  navigational keywords match the host (`uefa.com`). Most CJK and Indic URLs are percent
  escaped and give no words, so Han, kana and the other scripts gain from the query
  type alone.
- **Pair changes.** 782 pairs entered the top 10 and 313 left it (`tune_breakdown.py`):

| Group | Pairs | Before | After |
|---|---|---|---|
| text ≥ 2,000 characters | 5,556 | 0.834 | 0.863 |
| text 200–2,000 | 2,826 | 0.785 | 0.824 |
| text < 200 | 2,320 | 0.594 | 0.680 |
| en.wikipedia.org | 883 | 0.818 | 0.881 |
| www.youtube.com | 1,178 | 0.701 | 0.739 |
| other hosts | 8,641 | 0.773 | 0.816 |

- **Limits.** One batch chose and confirmed the settings, on disjoint keywords. The index
  has 54k docs, not 128M. Sites that disallow robots are not measured. Google also
  ranks by links and clicks, which a text index does not have. The batch was taken on
  2026-10-16 and its pages were fetched seven days later.

### Golden batch 20 as the official metric counts it

The official coverage table counts distinct golden URLs per tag. A URL is indexed when
it is on our list (`selected_urls_current`), and `ranked_rate` is hard-coded to 0. This
run measures what the ranked rate would be, through the product build and search
(`golden_official.py`), with the settings now (body `freqs`).

- **Flags.** Batch 20's `is_crawled` and `is_indexed`, as the official run of 10-18 wrote
  them into `metric_url`, read once and read-only (`golden_flags.py`). They reproduce
  its numbers: head 7,329 URLs with 1,814 indexed (24.75 %), random 8,031 with 1,852
  (23.06 %). Every indexed URL is also crawled.
- **Index.** Only the on-list golden pages that the Mac fetched (3,137 docs), plus 0,
  10k, 22k or 44k crawl pages. A page that is not on the list would not be in the
  production index, so it is left out here, unlike the tuning runs.
- **Counting.** Per tag: N golden URLs, I of them on the list, F on the list with a page
  in the index, and H in the top 10 of at least one of the tag's keywords. Only on-list
  ids count, so H/N, the ranked rate, can never pass I/N, the indexed rate. Intervals
  resample keywords, 10,000 draws; a URL golden for c keywords weighs 1/c in each. The
  query was chosen on the dev half of the keywords, so the test half is the headline.

Test half, 44k crawl pages:

| Tag | N | Crawled | On the list (I/N) | With a page (F/I) | Top 10 of those (H/F) | **Ranked rate H/N** |
|---|---|---|---|---|---|---|
| head | 3,636 | 27.9 % | 965 (26.5 %) | 90.2 % | 79.4 % | **0.190** [0.173, 0.207] |
| random | 3,974 | 25.5 % | 924 (23.3 %) | 88.9 % | 85.0 % | **0.176** [0.160, 0.191] |

- **All keywords**, descriptive since the dev half chose the query: head 0.182 [0.171,
  0.193] = 0.248 × 0.897 × 0.819, random 0.175 [0.164, 0.186] = 0.231 × 0.895 × 0.849.
  `text-index-eval` on the same index gives the same distinct-URL recall (0.1819 and
  0.1752), so no off-list golden page reached a top 10. 2.0 % of the keywords get no
  hit, as the index holds fewer golden pages.
- **What limits it.** Three quarters of the golden URLs are not on the list: that ceiling
  belongs to the selection, not to the index. Of the on-list URLs, about 10 % had no
  page, mostly robots.txt for the Mac's user agent and 403s; the crawler fetched all of
  them once, so the recrawl may get some. Of the on-list pages in the index, 79–85 %
  reach the top 10.
- **The tuned query** raises the test-half ranked rate over `combined_fields` before
  tuning: head 0.175 → 0.190 (+0.009 to +0.022), random 0.167 → 0.176 (+0.004 to
  +0.013).
- **Size of the index.** On the test half, H/F falls from 0.853 with no crawl pages to
  0.794 with 44k for head, and from 0.890 to 0.850 for random (paired, −0.079 to −0.040
  and −0.057 to −0.026): about 1.5 points per doubling from 10k on. 128M is about 11
  more doublings, and its pages are chosen to look like golden pages. So these rates
  are upper bounds, and this run gives no number for 128M.
- **The first weeks.** Until the recrawl has fetched the list, fewer on-list pages have
  text, so F/I and the ranked rate start lower.

## Decision

**OpenSearch 3.9.0 with `config/text_index/icu.json`.** Q, S and M hold:

- **Q.** +0.010, 95 % interval [+0.001, +0.017], above −0.02. It is the same with the
  corrected weights, and with the final build code: 1,598 of 1,600 NanoMIRACL questions
  score the same, and the other two moved by a tie.
- **S.** 0.23 TB ≤ B = 0.82 TB; 0.24 TB with the later `url.text`.
- **M.** Host2 had 231 GiB of memory available on 2026-10-23 06:06 UTC, so a 16–31 GB heap
  fits. Swap was full (7 of 7 GiB), available memory was 268 GB on 10-22, and new DB
  benchmark containers were running.

The rule did not cover rebuilds on the shared disk. They add two conditions for Host2:
no `keep_prev`, and a build peak measured before the first full build (next section).

**Query, after the engine: `best_fields` over `title^2`, `description`, `body` and
`url.text`.** It passed the tuning rule on the test half of batch 20, +0.050 [+0.040,
+0.061] in recall given present over `combined_fields`. The text analyzer did not
change, so the NanoMIRACL result stands.

**Body without positions, after the 10-23 review.** It returns the same top 10 with
the tuned query and on NanoMIRACL, and it halves the index to 0.13 TB at 128M. With
the merge fix, a rebuild then fits Host2's `/data` even on the worst day since the
crawler restarted, unless much recovery source survives the load.

## Found on the way

- Quickwit sorts hits by document id unless the query asks for `sort_by: _score`.
- Quickwit's official image still has no `multilang` tokenizer in 0.9.3 (`unknown
  tokenizer`), and its `raw` tokenizer drops tokens over 255 bytes, so long URLs cannot
  be looked up there.
- OpenSearch keeps a `_recovery_source` copy of each document, the excluded body
  included, and a merge drops it only below `min(retention leases, last safe commit)`
  (`SoftDeletesPolicy`). The leases sync about every 30 s, and a commit is safe only
  once the global checkpoint is persisted. So a build waits for the leases, commits
  again, then merges; `recovery_source_pruned` in the manifest reads the bound from the
  last commit. Without the wait the 50k index is 1.8 times as large.
- A force merge only commits the merged segment. Searches keep the old segments, and
  their files, until a refresh, and a shard without searches for 30 s skips the
  scheduled refreshes. The first 50k build measured 223 MB in 3 segments for a 78 MB
  index, so the build now refreshes after the merge.
- Right after the merge, the store can still hold the unmerged segments' files for a
  few seconds: one build read 170 MB for an index of 79 MB on disk. The manifest takes
  the sizes from the live segments' files and keeps the store reading as `store_bytes`.
- Lucene orders equal scores by internal doc id, which concurrent loading assigns
  differently in every build. In the 50k index, 310 of the 1,948 batch-20 keywords
  tie at rank 10, and two builds of the same text disagreed on the top-10 set for
  about 10 % of the keywords. With the `doc_id` tie-break they agree on all of them.
- The flood-stage watermark (95 % of the disk) also applies to a single-node cluster.
  The low and high watermarks do not by default
  (`cluster.routing.allocation.disk.watermark.enable_for_single_data_node`). Past the
  flood stage, every index on the node gets `read_only_allow_delete`, writes fail with
  HTTP 429, and the cluster gets `cluster.blocks.create_index`. In a local test with
  byte-valued watermarks set above the free space, the read-only block came within
  10 s. The create-index block was still set 90 s after the watermarks were reset and
  gone at the next check, less than a minute later.
- OpenSearch has `combined_fields` since 3.2; it needs one analyzer across the fields
  and field weights of at least 1. So it cannot search `url.text`, whose analyzer
  differs.
- In a `pattern_replace` filter, `%[0-9A-F]{2}|[^\p{L}\p{N}]+` leaves hex fragments:
  after a `/`, the second branch also takes the `%` of the next escape, so `/%E0` keeps
  `E0`. The filter now repeats one group of both, with `%` out of the second branch.
- A force merge commits shard by shard, but each shard's old segments stay on disk until
  a refresh, and the build refreshed only when the whole task had ended. So a rebuild
  held three indices, not two. OpenSearch also merges several shards at once (the
  `force_merge` pool has 1/8 of the cores, 18 threads on Host2). The build now refreshes
  at every poll, and the Host2 node sets `thread_pool.force_merge.size: 1`.
- The flood stage blocks writes, not a merge that is running: OpenSearch checks the
  block only when the merge is requested. A merge that starts below the flood stage can
  then fill the disk, under the crawler too. Hence the build's own checks.
- urllib raises `RemoteDisconnected`, `IncompleteRead` and timeouts as they are, not as
  `HTTPError`. The client turned only `HTTPError` into a `TextIndexError`, so one lost
  reply among the ~68k bulk requests of a full build would have deleted its index.

## Before Host2

The Host2 pieces are in the repo, and nothing runs there yet:
- `deploy/compose.compute.yaml`: the `opensearch` service (profile `search`) and the
  `text-index` job service;
- `config/text_index/host2.toml`: `settings_file` under `/app` (the image runs in
  `/work`), 8 shards, no `keep_prev`. The `text-index` service passes it after
  `container.toml`.

Steps:
1. Recompute the disk room on the day. Take the lowest free space before a prune over
   the last days in `/data/ipc/logs/prune_crawl_result.log`. Subtract the flood stage
   (5 % of 7.62 TB), the text Parquet and the staging.
2. As the isel user: `mkdir -p /data/isel && mkdir -m 2770 /data/isel/opensearch`. The
   node's user (uid 1000) joins the isel group (`group_add`), so it can write there; the
   files it makes belong to uid 1000, so the isel user removes them through a container.
   `host2_smoke.sh` checked this setup on the local Docker VM.
3. `docker compose ... --profile search up -d opensearch`. Check
   `curl 127.0.0.1:9200/_cluster/health`, and check that `_nodes` reports
   `mlockall: true`: swap is full on Host2.
4. Once the recrawl has text, build about 10M docs first (`docker compose ... run --rm
   text-index text-index-build --text-root` on a few partitions) and record the peak
   disk use. Then build all of it.
5. Run `text-index-eval --batch N` on the next golden batch: batch 20 alone chose the
   query.

Choices and limits:
- **No security plugin.** Like the crawler API, the node listens on 127.0.0.1 only. Any
  Host2 user can reach it. A lost index is rebuilt from the Parquet, in hours.
- **Default flood stage.** The crawler runs as root and can also use ext4's 5 % root
  reserve. When OpenSearch stops at 95 %, the crawler still has about 0.77 TB, a little
  more than the hour of output between two prunes.
- **8 shards**: 0.13 TB in shards of about 16 GB, inside the 10–30 GiB range that [AWS
  suggests](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/bp-sharding.html)
  when search latency matters.
- **Still open** (`docs/RECRAWL.md` §13):
  - index only live docs, before the first list swap after the recrawl has run;
  - compact the text Parquet, before its first 30-day cycle ends;
  - fallback documents for docs without text;
  - a `RankMeasure` for the metric.

## Reproduction

Scripts (sha256 prefix), kept outside the repo:

| Script | sha256 | Does |
|---|---|---|
| `crawl_to_text.py` | `17cd2f0e7f38f22d` | one crawl_result jsonl from stdin → `TEXT_SCHEMA` Parquet, hashing the stream |
| `pull_50k.sh` | `36662f2953a8733e` | `ssh ntu-sa2 'cat F'` per file into the converter; compares with the server's `sha256sum` |
| `nm_prepare.py` | `f024cd9cc3023bf8` | NanoMIRACL → `TEXT_SCHEMA` and judgments; the metric gate |
| `nm_os.py` | `4d4f57dd6e00a6c2` | OpenSearch variants through `textindex.build` and `search` |
| `nm_qw.py` | `226ec4ef1cda6fe8` | Quickwit variants, scored with `rankeval` |
| `nm_analyze.py` | `f282fb61cf606967` | tables and the stratified paired bootstrap |
| `nm_sensitivity.py` | `d53f96d10464a1ac` | the primary comparison with the corrected weights |
| `c50k_build.py` | `8d55a35a94da29ea` | size and speed variants through `textindex.build` |
| `c50k_stats.py` | `9035ebe4cceea83c` | text bytes per doc, fields and languages of the indexed docs |
| `c50k_project.py` | `25e888db0fbba035` | the 128M projection, budget B and the disk footprint |
| `c50k_freqs_rank.py` | `bcc5ef143d9bd9fd` | top-10 agreement of the `freqs` variant and of a rebuild with the live index |
| `golden_scripts.py` | `bff12e47735bcfb7` | zero hits and latency of a golden eval by keyword script |
| `golden_fetch.py` | `d451da13f1b710a0` | one polite fetch of each golden URL → `TEXT_SCHEMA` Parquet and a log line per URL |
| `golden_eval.py` | `5cc95f0ab685c09e` | the first golden runs through `textindex.build` and `evaluate_golden` |
| `golden_analyze.py` | `2745b5285af58aaf` | paired bootstrap of the first runs; misses of `icu` by length, host and tag |
| `tune_rule.md` | `0e2fd42b0df30b59` | the tuning rule, written before the split numbers |
| `golden_tune.py` | `708fe18a0a34c538` | index and query candidates; one build per index, many queries |
| `tune_report.py` | `5cf3b3f4d6182cd4` | recall given present on dev, test or all, with paired bootstrap per script |
| `tune_breakdown.py` | `6fd4c7b0e19f20da` | pairs that entered or left the top 10, by length, host and tag |
| `freqs_check.py` | `4af8cc26ffd4f44f` | top-10 ids and scores of a body-`freqs` build and of a rebuild against the live index, with the tuned query |
| `nm_freqs_check.py` | `d988badb77fcfaed` | NanoMIRACL with body `freqs`, question by question against the recorded `icu` run |
| `freqs_scores.py` | `f96f2dc0de45a73a` | score differences of another body-`freqs` build; its `scores_compared` field is unused |
| `freqs_scores2.py` | `b83154732b3da96e` | the same at 10 and 11 hits |
| `golden_flags.py` | `4c194f64dfb7c616` | batch 20's official `is_crawled` and `is_indexed` flags, read once from metricdb |
| `golden_official.py` | `92a6f07cf46d3320` | batch 20 as the official metric counts it: on-list pages, four background sizes, paired bootstrap |
| `host2_smoke.sh` | `d9ad011c446507fa` | the Host2 node's settings and data directory permissions on the local Docker VM |
