# Benchmark

This page reports how well the selected list covers golden URLs, before the metric
measures it. All numbers come from the run of 2026-10-11 (UTC) on Host2.

## Question

At K = 128,000,000 URLs, what share of a batch's golden URLs does the list contain?
The metric calls this share `indexed_rate`.

## Method

- **One read of the crawl.** `pull-full` exported every crawled row once:
  1,657,046,602 rows from 256 shards. The crawler was stopped, so the crawl state is
  frozen at 2026-10-05 12:39 UTC.
- **Point in time.** For batch b, features and the candidate pool use only data from
  before `as_of_b` = batch creation − 30 h. A page is a candidate when it was crawled
  before `as_of_b`.
- **Held-out batches.** Each test batch is scored by a model trained only on earlier
  batches. Batches 6, 7, 8, 18 and 19 are the test batches. Batches 10–17 have no
  golden URLs.
- **Recall at K from a sample.** A 1 % hash sample of the pool stands in for the full
  pool. The row at sample rank round(K · p) is the boundary; a golden page at or above
  it counts as selected. With p = 1 the estimate equals the exact top K (unit test).
- **Rate.** `indexed exact rate` = distinct raw golden URLs in the list ÷ distinct raw
  golden URLs. 1.35 % of golden URLs change under `canonicalize_url`, so this rate and
  the metric's canonical-key rate differ by at most that share.
- **Uncertainty.** 95 % CIs come from a bootstrap that resamples queries.

## Results

### Pooled over the five test batches

| Ranker | What it is | Head | Random |
|---|---|---|---|
| **`lgbm:full`** | **IndexSelectionV2** | **22.36 %** (21.82–22.93) | **17.82 %** (17.31–18.31) |
| `lgbm:no_domain` | same model without domain features | 21.06 % | 16.50 % |
| `logreg` | logistic regression on the same features | 21.86 % | 17.35 % |
| `v1_features` | the v1 feature set (URL shape + domain demand), retrained on the same data | 20.52 % | 16.18 % |
| `domain_prior` | domain golden lift only | 17.70 % | — |

- IndexSelectionV2 beats the v1 feature set by 1.84 points (head) and 1.64 points
  (random) on the same pool, K and labels.
- Domain features add 1.16–1.45 points (head, paired 95 % CI).

### Per batch (`lgbm:full`)

| Batch | Best rounds | Head | Random |
|---|---|---|---|
| 6 | 129 | 17.94 % | 16.30 % |
| 7 | 142 | 19.82 % | 18.61 % |
| 8 | 229 | 25.15 % | 23.32 % |
| 18 | 1,343 | 26.13 % | 19.92 % |
| 19 | 239 | 24.62 % | 12.06 % |

## Full-scale run

| Check | Result |
|---|---|
| Rows scored | 1,657,046,602 |
| Rows kept above τ | 147,021,239; the sample predicted 147,181,326 (z = −1.42, limit 6) |
| Sample rows scored again in the full pass | every score identical |
| URLs selected | 128,000,000 |
| Boundary score | −6.0465; the sample predicted −6.0437 |
| `score-full` time | 148.6 min on 32 CPUs |

## Leaks found and fixed before these numbers

| Leak | Effect | Fix |
|---|---|---|
| A positive did not count itself in its domain statistics | a +17 logit boost for pages of small domains; 38M extra rows kept | every row adds itself to its domain statistics |
| `TABLESAMPLE SYSTEM` takes whole pages, so sample rows saw their page neighbours | sample and non-sample rows saw different domain statistics | per-row salted hash sample |
| Sample rows shared one group value that positives never had | early stopping at 33 rounds; train and valid loss diverged | domain statistics come from one half of the sample, training rows from the other |

Each fix has a unit test that fails without it.

## Limits

- These are offline numbers. The first out-of-sample number is batch 20
  (2026-10-16), read by the metric itself.
- Inlink counts are measured at export time, after most test batches. Inlinks may
  have grown since then.
- Only crawled pages can be selected. A golden page the crawler never fetched cannot
  count.
