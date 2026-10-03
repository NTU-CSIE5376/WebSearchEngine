import random

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from idxsel.select_topk import NotEnoughCandidates, select_top_k
from idxsel.urlnorm import md5_number_lower


def _scored(tmp_path, n=3000, n_urls=2500, seed=1):
    rng = random.Random(seed)
    urls = [f"https://d{rng.randrange(40)}.com/p/{i}" for i in range(n_urls)]
    rows, used = [], set()
    for i in range(n):
        # Tail rows repeat a URL in another shard, like rows left behind by a reshard.
        # Within one shard a URL is unique (features are deduplicated per shard).
        url = urls[i] if i < n_urls else rng.choice(urls)
        shard = rng.choice([s for s in range(4) if (url, s) not in used] or [None])
        if shard is None:
            continue
        used.add((url, shard))
        tier = rng.choice([0, 0, 0, 1, 2])
        key52 = rng.choice([0, 5, 5, 5, 2**51, 2**52 - 1, rng.randrange(2**52)])  # heavy ties
        rows.append((url, shard, tier, key52, md5_number_lower(url), tier + key52 / 2**52))
    for shard in range(4):
        part = [r for r in rows if r[1] == shard]
        d = tmp_path / "scored" / f"shard={shard}"
        d.mkdir(parents=True)
        pq.write_table(pa.table({
            "url": [r[0] for r in part],
            "first_seen": pa.array([None] * len(part), pa.timestamp("us", tz="UTC")),
            "tier": pa.array([r[2] for r in part], pa.int32()),
            "key52": pa.array([r[3] for r in part], pa.int64()),
            "h": pa.array([r[4] for r in part], pa.uint64()),
            "score": [r[5] for r in part],
        }), d / "data_0.parquet")
    return rows, str(tmp_path / "scored" / "**" / "*.parquet")


def _brute_force(rows, k):
    ordered = sorted(rows, key=lambda r: (-r[2], -r[3], r[4], r[0], r[1]))
    seen, out = set(), []
    for r in ordered:
        if r[0] in seen:
            continue
        seen.add(r[0])
        out.append((r[0], r[1]))
        if len(out) == k:
            break
    return out


@pytest.mark.parametrize("exact_rows", [1, 7, 10**9])
@pytest.mark.parametrize("k", [1, 999, 2499, 2500])
def test_matches_brute_force(tmp_path, k, exact_rows):
    rows, glob = _scored(tmp_path)
    out = tmp_path / "selected.parquet"
    stats = select_top_k(duckdb.connect(), glob, k, exact_rows, 5, out)
    got = pq.read_table(out, columns=["url", "shard"]).to_pylist()
    assert sorted((r["url"], r["shard"]) for r in got) == sorted(_brute_force(rows, k))
    assert stats["k"] == k
    if exact_rows == 10**9:
        assert stats["radix_passes"] == 0  # the whole candidate set is sorted directly
    else:
        assert stats["radix_passes"] >= stats["rounds"]


def test_deterministic_across_threads(tmp_path):
    _, glob = _scored(tmp_path)
    picks = []
    for threads in (1, 8):
        con = duckdb.connect()
        con.execute(f"SET threads = {threads}")
        out = tmp_path / f"sel{threads}.parquet"
        select_top_k(con, glob, 1234, 50, 5, out)
        picks.append(sorted(pq.read_table(out, columns=["url"])["url"].to_pylist()))
    assert picks[0] == picks[1]


def test_not_enough_candidates(tmp_path):
    _, glob = _scored(tmp_path)
    with pytest.raises(NotEnoughCandidates):
        select_top_k(duckdb.connect(), glob, 2501, 10, 5, tmp_path / "s.parquet")


def test_global_dedupe_fallback_terminates(tmp_path):
    rows, glob = _scored(tmp_path)
    out = tmp_path / "s.parquet"
    stats = select_top_k(duckdb.connect(), glob, 2500, 7, 0, out)
    assert stats["global_dedupe"]
    got = pq.read_table(out, columns=["url", "shard"]).to_pylist()
    assert sorted((r["url"], r["shard"]) for r in got) == sorted(_brute_force(rows, 2500))
