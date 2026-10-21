from __future__ import annotations

from datetime import timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import replace, small_cfg
from synth import SNAPSHOT, crawl_rows, make_world, write_export, write_work_dir

from isel.derive import derive_positives, derive_sample
from isel.features import open_store
from isel.golden import golden_table
from isel.io import atomic_write_json, read_json, write_parquet_atomic
from isel.urlnorm import canonicalize


@pytest.fixture(scope="module")
def world():
    return make_world()


def _cfg(work, percent=50.0, salt="sample-v1"):
    return replace(small_cfg(work), "full", sample_percent=percent, sample_salt=salt)


def _rows(directory, pattern):
    files = sorted(directory.glob(pattern))
    return pa.concat_tables([pq.read_table(f) for f in files]).to_pylist() if files else []


def _keys(rows):
    return [(r["url"], r["shard"]) for r in rows]


def test_derive_sample_keeps_a_fixed_salted_share_of_the_export(tmp_path, world):
    exported = set(_keys(crawl_rows(world)))
    picks = {}
    for name, salt in (("a", "sample-v1"), ("b", "sample-v1"), ("c", "other")):
        work = tmp_path / name
        write_export(world, work)
        report = derive_sample(_cfg(work, salt=salt), work)
        picks[name] = _keys(_rows(work / "sample", "shard=*.parquet"))
        meta = read_json(work / "sample" / "_meta.json")
        assert report["rows"] == len(picks[name])
        assert meta["method"] == "hash" and meta["snapshot_at"] == SNAPSHOT.isoformat()
    agg = [r["in_agg"] for r in _rows(tmp_path / "a" / "sample", "shard=*.parquet")]
    assert 0.4 < sum(agg) / len(agg) < 0.6
    meta = read_json(tmp_path / "a" / "sample" / "_meta.json")
    assert (meta["agg_share"], meta["agg_salt"]) == (0.5, "agg-v1")
    sample = set(picks["a"])
    assert len(sample) == len(picks["a"]) and sample <= exported
    assert 0.45 < len(sample) / len(exported) < 0.55
    assert picks["b"] == picks["a"]
    assert set(picks["c"]) != sample


def test_derive_sample_keeps_one_copy_of_a_row_read_twice(tmp_path, world):
    # A row the rescoring job moves during the export can be read twice.
    twice = {**crawl_rows(world)[0], "url_score": 0.5}
    write_export(world, tmp_path, extra=[twice])
    derive_sample(_cfg(tmp_path, percent=100.0), tmp_path)
    keys = _keys(_rows(tmp_path / "sample", "shard=*.parquet"))
    assert keys.count((twice["url"], twice["shard"])) == 1
    assert len(keys) == len(world.pages)


def test_derive_sample_counts_copies_whose_features_differ(tmp_path, world):
    rows = crawl_rows(world)
    # url_score is no feature input; title_len is, so score-full would score the copies apart.
    extra = [{**rows[0], "url_score": 0.5}, {**rows[1], "title_len": 999}]
    write_export(world, tmp_path, extra=extra)
    derive_sample(_cfg(tmp_path, percent=100.0), tmp_path)
    meta = read_json(tmp_path / "sample" / "_meta.json")
    assert meta["duplicate_keys"] == 2
    assert meta["differing_duplicates"] == 1


def test_derive_sample_needs_a_finished_export_and_no_older_sample(tmp_path, world):
    atomic_write_json(tmp_path / "full" / "export" / "_meta.json", {"started_at": "x"})
    with pytest.raises(RuntimeError, match="export is unfinished"):
        derive_sample(_cfg(tmp_path), tmp_path)
    write_export(world, tmp_path)
    derive_sample(_cfg(tmp_path), tmp_path)
    with pytest.raises(RuntimeError, match="already holds a sample"):
        derive_sample(_cfg(tmp_path), tmp_path)


def _write_golden(world, work):
    write_parquet_atomic(golden_table(world.golden_rows), work / "golden" / "golden.parquet")


def test_derive_positives_takes_every_golden_spelling_from_every_shard(tmp_path, world):
    golden_page = next(p for p in crawl_rows(world)
                       if any(r[7] == p["url"] for r in world.golden_rows))
    # The same URL in a second shard, which the old shard router never probed.
    moved = {**golden_page, "shard": (golden_page["shard"] + 1) % 4}
    write_export(world, tmp_path, extra=[moved])
    _write_golden(world, tmp_path)
    report = derive_positives(_cfg(tmp_path), tmp_path)
    exported = [*crawl_rows(world), moved]
    for b in world.batches:
        spellings = {u for r in world.golden_rows if r[0] == b for u in (r[7], canonicalize(r[7]))}
        expected = sorted(_keys(r for r in exported if r["url"] in spellings))
        got = sorted(_keys(_rows(tmp_path / "positives", f"batch={b}.parquet")))
        assert got == expected and report[str(b)] == len(expected)
    assert any(set(_keys(_rows(tmp_path / "positives", f"batch={b}.parquet")))
               >= {(moved["url"], moved["shard"]), (moved["url"], golden_page["shard"])}
               for b in world.batches)
    assert read_json(tmp_path / "positives" / "_meta.json")["export_started_at"] \
        == SNAPSHOT.isoformat()


def test_derive_positives_refuses_positives_from_another_export(tmp_path, world):
    write_export(world, tmp_path)
    _write_golden(world, tmp_path)
    derive_positives(_cfg(tmp_path), tmp_path, [1])
    write_export(world, tmp_path, started_at=SNAPSHOT + timedelta(days=1))
    with pytest.raises(RuntimeError, match="another export"):
        derive_positives(_cfg(tmp_path), tmp_path, [2])


def test_derive_positives_refuses_positives_without_a_meta(tmp_path, world):
    # The old pull-positives wrote batch files but no _meta.json.
    write_export(world, tmp_path)
    _write_golden(world, tmp_path)
    derive_positives(_cfg(tmp_path), tmp_path, [1])
    (tmp_path / "positives" / "_meta.json").unlink()
    with pytest.raises(RuntimeError, match=r"no _meta\.json"):
        derive_positives(_cfg(tmp_path), tmp_path, [2])


def test_derive_positives_refuses_a_batch_the_golden_set_lacks(tmp_path, world):
    write_export(world, tmp_path)
    _write_golden(world, tmp_path)
    with pytest.raises(ValueError, match=r"\[999\]"):
        derive_positives(_cfg(tmp_path), tmp_path, [1, 999])
    assert not (tmp_path / "positives").exists()


def test_the_store_takes_only_a_hash_sample_and_positives_of_the_same_export(tmp_path, world):
    write_work_dir(world, tmp_path)
    cfg = small_cfg(tmp_path)
    open_store(cfg, tmp_path)
    positives = read_json(tmp_path / "positives" / "_meta.json")
    atomic_write_json(tmp_path / "positives" / "_meta.json",
                      {**positives, "export_started_at": "2026-01-01T00:00:00+00:00"})
    with pytest.raises(RuntimeError, match="did not come from the sample's export"):
        open_store(cfg, tmp_path)
    atomic_write_json(tmp_path / "positives" / "_meta.json", positives)
    meta = read_json(tmp_path / "sample" / "_meta.json")
    # A TABLESAMPLE SYSTEM sample: whole heap pages, so domain neighbours come along.
    atomic_write_json(tmp_path / "sample" / "_meta.json", {**meta, "method": "system"})
    with pytest.raises(RuntimeError, match="not drawn row by row"):
        open_store(cfg, tmp_path)
