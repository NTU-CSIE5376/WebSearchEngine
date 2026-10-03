"""Exact, deterministic top-K over ~1.5B scored rows without a global sort.

Order: tier DESC, key52 DESC, h ASC, url ASC, shard ASC. The sort key is split
into digits (most significant first). Each pass counts one digit inside the
current bucket, walks the counts from the top to find the digit holding the
K-th row, and narrows the bucket; the boundary digit's count is the new bucket
size. Once the bucket is small it is sorted exactly. Typically 2-3 GROUP BY
passes over three narrow columns are enough.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from idxsel.duck import copy_atomic, sql_str

# hinv = 2^64 - 1 - h, so descending hinv digits mean ascending h.
DIGITS: tuple[str, ...] = (
    "tier",
    "(key52 >> 36)",
    "((key52 >> 20) & 65535)",
    "((key52 >> 4) & 65535)",
    "(key52 & 15)",
    "(hinv >> 48)",
    "((hinv >> 32) & 65535)",
    "((hinv >> 16) & 65535)",
    "(hinv & 65535)",
)
FINAL_ORDER = "tier DESC, key52 DESC, h ASC, url ASC, shard ASC"


class NotEnoughCandidates(RuntimeError):
    """Raised when fewer than K rows are eligible."""


def _create_candidates(con: duckdb.DuckDBPyConnection, scored_glob: str) -> None:
    con.execute("CREATE TEMP TABLE IF NOT EXISTS excluded (url VARCHAR, shard SMALLINT)")
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW cand AS "
        f"SELECT s.url, CAST(s.shard AS SMALLINT) AS shard, s.first_seen, s.tier, s.key52, s.h,"
        f" s.score, (18446744073709551615::UBIGINT - s.h) AS hinv "
        f"FROM read_parquet({sql_str(scored_glob)}, hive_partitioning = true) s "
        f"ANTI JOIN excluded e ON e.url = s.url AND e.shard = CAST(s.shard AS SMALLINT)"
    )


def _radix(con: duckdb.DuckDBPyConnection, k: int, total: int, exact_sort_max_rows: int
           ) -> tuple[list[str], list[str], int, int]:
    """Returns (strictly_above predicates, bucket prefix, rows still needed, passes)."""
    need, bucket, prefix, above, passes = k, total, [], [], 0
    for digit in DIGITS:
        if bucket <= exact_sort_max_rows:
            break
        where = " AND ".join(prefix) or "TRUE"
        counts = con.execute(
            f"SELECT {digit} AS d, count(*) FROM cand WHERE {where} GROUP BY d ORDER BY d DESC"
        ).fetchall()
        passes += 1
        cum, boundary = 0, None
        for d, n in counts:
            if cum + n >= need:
                boundary, bucket = d, n
                break
            cum += n
        if boundary is None:
            raise RuntimeError("radix walk did not find the boundary bucket")
        if cum:
            above.append(f"({where}) AND {digit} > {boundary}")
        need -= cum
        prefix.append(f"{digit} = {boundary}")
    return above, prefix, need, passes


def _write_selection(con: duckdb.DuckDBPyConnection, above: list[str], prefix: list[str],
                     need: int, out: Path) -> None:
    cols = "url, score, first_seen, shard, tier, key52, h"
    bucket = " AND ".join(prefix) or "TRUE"
    above_sql = " OR ".join(f"({p})" for p in above) or "FALSE"
    copy_atomic(
        con,
        f"SELECT {cols} FROM cand WHERE {above_sql} "
        f"UNION ALL "
        f"SELECT {cols} FROM (SELECT * FROM cand WHERE {bucket} ORDER BY {FINAL_ORDER} "
        f"LIMIT {int(need)})",
        out)


def _exclude_non_best(con: duckdb.DuckDBPyConnection, dup_source: str) -> None:
    """Excludes every copy except the best-ranked one for URLs in `dup_source`."""
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE to_exclude AS SELECT url, shard FROM ("
        f"SELECT c.url, c.shard, row_number() OVER (PARTITION BY c.url ORDER BY {FINAL_ORDER})"
        f" AS rn FROM cand c SEMI JOIN ({dup_source}) d ON d.url = c.url) WHERE rn > 1"
    )
    con.execute("INSERT INTO excluded SELECT url, shard FROM to_exclude")


def select_top_k(con: duckdb.DuckDBPyConnection, scored_glob: str, k: int,
                 exact_sort_max_rows: int, max_dedupe_rounds: int, out: Path) -> dict[str, Any]:
    """Writes exactly `k` distinct URLs to `out` (Parquet) and returns stats.

    A URL can exist in more than one shard after reshards. When the selection
    holds such a URL twice, all but its best-ranked copy are excluded and the
    selection is recomputed. If duplicates keep appearing after
    `max_dedupe_rounds`, one global pass groups by the 64-bit URL hash to drop
    every cross-shard copy at once, which guarantees termination.

    Raises:
        NotEnoughCandidates: If fewer than `k` distinct URLs are eligible.
    """
    _create_candidates(con, scored_glob)
    (total,) = con.execute("SELECT count(*) FROM cand").fetchone()
    if total < k:
        raise NotEnoughCandidates(f"only {total} candidates for k={k}")
    sel = sql_str(str(out))
    left, rounds, passes, global_pass = total, 0, 0, False
    while True:
        rounds += 1
        above, prefix, need, n = _radix(con, k, left, exact_sort_max_rows)
        passes += n
        _write_selection(con, above, prefix, need, out)
        dup_urls = f"SELECT url FROM read_parquet({sel}) GROUP BY url HAVING count(*) > 1"
        (dups,) = con.execute(f"SELECT count(*) FROM ({dup_urls})").fetchone()
        if not dups:
            break
        if global_pass:
            raise RuntimeError("duplicates remain after the global dedupe pass")
        if rounds > max_dedupe_rounds:
            _exclude_non_best(con, "SELECT url FROM cand WHERE h IN "
                                   "(SELECT h FROM cand GROUP BY h HAVING count(*) > 1)")
            global_pass = True
        else:
            _exclude_non_best(con, dup_urls)
        (left,) = con.execute("SELECT count(*) FROM cand").fetchone()
        if left < k:
            raise NotEnoughCandidates(f"only {left} distinct candidates for k={k}")
    # The duplicate check above already proved the URLs distinct.
    n, tiers = con.execute(
        f"SELECT count(*), histogram(tier) FROM read_parquet({sel})").fetchone()
    if n != k:
        raise RuntimeError(f"selection has {n} rows, expected {k}")
    (excluded,) = con.execute("SELECT count(*) FROM excluded").fetchone()
    return {"k": k, "candidates": total, "rounds": rounds, "radix_passes": passes,
            "global_dedupe": global_pass, "excluded_duplicates": excluded,
            "tier_counts": {str(t): c for t, c in (tiers or {}).items()},
            "boundary_prefix": prefix}
