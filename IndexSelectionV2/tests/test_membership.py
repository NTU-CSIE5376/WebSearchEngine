"""A page's features must not depend on whether the sample holds it.

The aggregate half of the sample feeds the domain statistics; positives and
almost every scored row are outside it. Any feature that moves with that
membership becomes a label proxy in training and a bias at scoring time
(docs/DESIGN.md §4.2). Each test builds the same world twice: W1 puts page u
in the aggregate half, W2 is W1 with u taken out of the sample files, and u
must get the same feature values in both.
"""

from __future__ import annotations

import dataclasses
import shutil
from datetime import timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from conftest import small_cfg
from synth import FIRST_BATCH, make_world, write_work_dir

from isel.features import FEATURES, open_store
from isel.io import write_parquet_atomic
from isel.urlnorm import md5_number_lower

PERCENT = 50.0     # below 100, so a wrong 1/rate scale shows
LABEL_BATCH = 8


def _in_agg(url: str, shard: int, full) -> bool:
    sampled = md5_number_lower(f"{url}:{shard}:{full.sample_salt}") % 1_000_000
    agg = md5_number_lower(f"{url}:{shard}:{full.agg_salt}") % 1_000_000
    return sampled < PERCENT * 10_000 and agg < full.agg_share * 1_000_000


def _lone_page(world, full):
    """A golden page alone in its domain, fetched between two batches' as-of times."""
    base = world.pages[0]
    url = next(u for u in (f"https://www.lonely{i}.org/" for i in range(1000))
               if _in_agg(u, base["shard"], full))
    fetched = FIRST_BATCH + timedelta(days=14 * 3 + 5)
    page = {**base, "url": url, "domain_id": 10_000, "last_fetch_ok": fetched}
    created = world.batches[LABEL_BATCH]
    row = (LABEL_BATCH, created, 999_999, "q999999", 100, ["head"], 1, url)
    return dataclasses.replace(world, pages=[*world.pages, page],
                               golden_rows=[*world.golden_rows, row]), page


def _crowded_page(world, full):
    """A golden page of the label batch in the aggregate half, in a domain of many rows."""
    golden = {r[7] for r in world.golden_rows if r[0] == LABEL_BATCH}
    page = next(p for p in world.pages if p["url"] in golden
                and p["last_fetch_ok"] < world.batches[LABEL_BATCH] - timedelta(days=3)
                and _in_agg(p["url"], p["shard"], full))
    return world, page


@pytest.fixture(scope="module", params=["lone", "crowded"])
def worlds(request, tmp_path_factory):
    full = small_cfg(tmp_path_factory.mktemp("cfg")).full
    pick = _lone_page if request.param == "lone" else _crowded_page
    world, page = pick(make_world(), full)
    w1 = tmp_path_factory.mktemp("w1")
    write_work_dir(world, w1, PERCENT)
    w2 = tmp_path_factory.mktemp("w2")
    shutil.copytree(w1, w2, dirs_exist_ok=True)
    path = w2 / "sample" / f"shard={page['shard']:03d}.parquet"
    sample = pq.read_table(path)
    assert page["url"] in sample.column("url").to_pylist()
    write_parquet_atomic(sample.filter(pc.not_equal(sample.column("url"), page["url"])), path)
    return w1, w2, page


def _row(frame: pa.Table, url: str) -> dict:
    rows = frame.filter(pc.equal(frame.column("url"), url)).to_pylist()
    assert len(rows) == 1
    return rows[0]


def _features(row: dict) -> dict:
    return {name: row[name] for name in FEATURES}


def test_a_positive_gets_the_features_it_would_get_in_the_aggregate_half(worlds):
    w1, w2, page = worlds
    rows = [_row(open_store(small_cfg(w), w).frame(LABEL_BATCH, "eval"), page["url"])
            for w in (w1, w2)]
    assert [r["in_agg"] for r in rows] == [True, False]
    assert all(r["label"] == 1 for r in rows)
    assert _features(rows[1]) == _features(rows[0])


def test_a_scored_row_gets_the_features_it_would_get_in_the_aggregate_half(worlds):
    w1, w2, page = worlds
    rows = []
    for w in (w1, w2):
        store = open_store(small_cfg(w), w, next_batch=True)
        part = next(f for f in sorted((w / "full" / "export").glob("shard=*/part-*.parquet"))
                    if page["url"] in pq.read_table(f, columns=["url"]).column("url").to_pylist())
        store.load_chunk(part)
        rows.append(_row(store.frame(store.batches[-1].batch_id, "score"), page["url"]))
    assert [r["in_agg"] for r in rows] == [True, False]
    assert _features(rows[1]) == _features(rows[0])


def test_training_rows_share_group_values_whatever_their_label(tmp_path):
    # Sample rows used to share a TLD's lift value that no other row could reach, so the
    # value alone told the sample negatives from the positives (2026-10-11, M0').
    write_work_dir(make_world(), tmp_path, PERCENT)
    store = open_store(small_cfg(tmp_path), tmp_path)
    frame = store.frame(LABEL_BATCH, "train")
    assert not any(frame.column("in_agg").to_pylist())
    first = min(b.as_of for b in store.batches if b.created_at < store.batch(LABEL_BATCH).created_at)
    # Rows fetched before every source batch count in every source pool: same own weight.
    age = (store.batch(LABEL_BATCH).as_of - first).total_seconds() / 86400.0
    old = frame.filter(pc.greater(frame.column("fetch_age_days"), age))
    labels = set(old.column("label").to_pylist())
    assert labels == {0, 1}
    assert len(set(old.column("lift_tld").to_pylist())) == 1
