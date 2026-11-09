# Batch 21 google.com/goto write-back

From batch 21 (2026-11-01) SerpApi returned most organic results at rank 4-10 as
`https://www.google.com/goto?url=<token>` redirect links. 9,041 of the 15,008
`metric_url` rows in batch 21 hold such a link. The crawler never has these URLs,
so batch 21 coverage reads about 20% instead of about 45%.

Each link was requested once on 2026-11-05/06 (5 s apart, redirects not followed),
with `scripts/resolve_golden_goto.py` on branch `ops/resolve-golden-goto-batch21`.
All 9,041 answered with a 302 to a non-Google page: 0 blocked, 0 errors.

| file | content |
|---|---|
| `batch21_goto_resolved.csv` | `metric_url_id, query_id, tags, rank, old_url, new_url` for the 9,041 rows |
| `batch21_writeback.sql` | sets `metric_url.url = new_url` for those rows |
| `batch21_rollback.sql` | puts the goto links back |
| `build_writeback.py` | generates the three files above from the resolver's SQLite state |

## Run

```sh
psql -v ON_ERROR_STOP=1 -h 172.16.191.1 -p 5433 -U metric -d metricdb \
     -f scripts/golden_goto_writeback/batch21_writeback.sql
```

Both SQL files run in one transaction and abort, changing nothing, unless every row
still holds the expected URL. A second run of the same file therefore fails
instead of doing anything. Only `url` changes.

The flags (`is_discovered`, `is_crawled`, `is_indexed`, `shard_id`) and the
coverage tables are not recomputed on their own: cron runs `crawler_all` only on
the 1st and 16th together with `--create`, so it measures the new batch, not 21.
Measure batch 21 once by hand after the write-back, before the next batch is
created (2026-11-16 12:00). Without `--create`, `measure.py` measures the latest
batch, and the coverage rows are upserted for today's date:

```sh
cd /root/WebSearchEngine && /root/system-venv/bin/python3 measure.py --test \
    --metric_db_url 172.16.191.1:5433 --crawler_db_url 172.16.191.3:5433 \
    --select_db_url 172.16.191.1:5445 --strategy random head --measure crawler_all
```

Tested on a copy of batch 21 in Postgres 16: write-back leaves 0 goto rows, a rerun
aborts, and rollback restores the original URLs byte for byte.

## Expected effect

Coverage of batch 21 recomputed with the resolved URLs, from YB crawlerdb and
selectdb on 2026-11-06:

| set | discovered | crawled | indexed |
|---|---|---|---|
| head, goto as is | 22.5% | 14.1% | 9.7% |
| head, resolved | 48.0% | 29.5% | 19.6% |
| random, goto as is | 18.9% | 11.5% | 8.0% |
| random, resolved | 41.6% | 25.3% | 16.9% |

The write-back also means golden_inject injects the real pages instead of goto
links once batch 21 is four weeks old (around 2026-11-29).
