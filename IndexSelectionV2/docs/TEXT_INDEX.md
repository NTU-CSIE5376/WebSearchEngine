# Text index

The text index makes the selected list searchable, so the metric's `ranked_rate`
(Recall@10 on golden keywords) can stop being 0. This page records the engine choice,
the design, and the local trial that backs both.

Status: engine decided (OpenSearch, see [Decision](#decision)); built and measured
locally on 2026-10-23. Not deployed. A real Recall@10 needs the list's page text,
which arrives only after WebCrawler#84 is deployed and `recrawl-archive` runs
(`docs/RECRAWL.md`).

## Commands

| Command | Does |
|---|---|
| `text-index-build [--text-root DIR]` | loads the newest text of each doc into a new index `isel-text-<utc id>`, checks the count, merges it, blocks writes, then moves the alias `isel-text` to it in one call |
| `text-index-eval --batch N` | searches each golden keyword of batch N on the alias and reports Recall@10 against its golden URLs |

Local engine: `OS_HEAP=3g docker compose -f deploy/compose.opensearch.yaml up -d --build`.

## Design

- **Input.** The text Parquet that `recrawl-archive` writes (`TEXT_SCHEMA`). A first
  DuckDB pass picks each doc's newest row from (`doc_id`, `fetched_at`, file name)
  alone, so 128M page bodies are never sorted; a second pass reads only the winners.
- **Document.** `_id` is the doc id in decimal. `url` and `lang` are stored; `title`,
  `description` and `body` are full text, and `body` is left out of `_source`. `doc_id`
  holds the same id as doc values only, for sorting. `config/text_index/icu.json` holds
  the settings and mappings.
- **Analysis.** One analyzer for every field and language: `icu_tokenizer` (Unicode word
  breaks with dictionaries for CJK and Thai) and `icu_folding` (case, accents, width).
  A keyword's language is unknown, so per-language analyzers would need routing; the
  trial measures what they would add.
- **Query.** `combined_fields` (BM25F) over `title^2`, `description` and `body`, top 10.
  The weights are untuned: NanoMIRACL has no titles, so they wait for real golden data.
  The keyword is never parsed as query syntax. Equal scores are ordered by `doc_id`.
  Lucene would order them by internal doc id, which concurrent loading assigns
  differently in every build. Anserini fixed the same problem the same way ([Lin and
  Yang, SIGIR 2019](https://arxiv.org/abs/1807.05798)).
- **Build.** A new index per build, in bulk-load mode (no replica, no refresh, async
  translog). `_bulk` requests of 8 MiB, two in flight, resent whole on 429 or 503.
  After the load: refresh, check that the index holds every doc, flush, wait until the
  retention leases pass the last operation, commit again, force merge to one segment and
  check the merge task, refresh so that searches use the merged segment, restore refresh
  and durability, block writes. Then one `_aliases` call moves the alias. A failure
  before the move deletes the new index and leaves the alias as it was. With
  `keep_prev`, the previous index stays for rollback; older ones are deleted.
- **Eval.** For each golden keyword, the top 10 ids are compared with the doc ids of its
  golden URLs. `recall_pairs` counts (keyword, URL) pairs, as the metric's old rank
  code did, with never-crawled URLs in the denominator; `recall_unique` counts distinct
  URLs, as the coverage tables do. `presence` is the share of golden pairs whose page
  is in the index, the ceiling of both.

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
| **`icu`, the chosen settings** | 44,028 | 1,791 | 0.435 | **0.23 TB** |
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
- **S holds**: `icu` needs 0.23 TB.
- **A rebuild needs more than one index**, which the rule did not cover. A build holds the
  serving index and the new one. Upper bound: the new index's unmerged segments still
  carry the recovery source (0.19 TB at 128M) while the merged copy is written, as at
  44k docs. Lower bound: over a load of hours the leases advance, and natural merges
  drop most of it.

| TB at 128M docs | One index | Two kept (`keep_prev`) | Build peak | Build peak, `keep_prev` |
|---|---|---|---|---|
| `icu` | 0.23 | 0.46 | 0.46–0.88 | 0.69–1.11 |
| body `freqs` | 0.12 | 0.24 | 0.24–0.55 | 0.37–0.68 |

- **The flood stage is the hard limit.** It applies to a single node and counts the
  crawler's files on `/data`. At the pre-prune low, after the text and the staging, it
  leaves 0.52 TB. So no build fits with `keep_prev`. Without it, `icu` fits only near
  the lower bound, and the body without positions fits unless the peak is near its upper
  bound. A build that crosses the flood stage fails and deletes its index; the serving
  index stays readable.

### Golden batch 20

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

## Decision

**OpenSearch 3.9.0 with `config/text_index/icu.json`.** Q, S and M hold:

- **Q.** +0.010, 95 % interval [+0.001, +0.017], above −0.02. It is the same with the
  corrected weights, and with the final build code: 1,598 of 1,600 NanoMIRACL questions
  score the same, and the other two moved by a tie.
- **S.** 0.23 TB ≤ B = 0.82 TB.
- **M.** Host2 had 231 GiB of memory available on 2026-10-23 06:06 UTC, so a 16–31 GB heap
  fits. Swap was full (7 of 7 GiB), available memory was 268 GB on 10-22, and new DB
  benchmark containers were running.

The rule did not cover rebuilds on the shared disk. They add two conditions for Host2:
no `keep_prev`, and a build peak measured before the first full build (next section).

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
  and field weights of at least 1.

## Before Host2

- `config/container.toml` must set `text_index.settings_file` to
  `/app/config/text_index/icu.json` (the image runs in `/work`) and `text_index.url`.
- The node needs the security plugin, TLS and credentials as secrets, a 16–31 GB heap,
  and a data path on Host2 `/data` within the budget above.
- Set `text_index.keep_prev = false`; rollback is a rebuild from the Parquet.
- Build about 10M docs first and record the peak of the shard's disk use. Scaled to
  128M, it decides between `icu` and a body without positions, which ranks the same.
- Set `text_index.shards` to about 8: 0.23 TB in shards of about 30 GiB, the top of the
  10–30 GiB range that [AWS suggests](https://docs.aws.amazon.com/opensearch-service/latest/developerguide/bp-sharding.html)
  when search latency matters.
- Keep the default flood stage. It stops OpenSearch before the crawler runs out of
  disk.
- Index only live docs, add fallback documents for docs without text, and give the
  metric a `RankMeasure` (`docs/RECRAWL.md` §13).

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
