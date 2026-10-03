from helpers import example_config, with_changes

from idxsel.manifest import atomic_write_json, save_state
from idxsel.pipeline import Runtime, gc_storage


def _dir(path, marker=None):
    path.mkdir(parents=True)
    if marker:
        (path / marker).write_text("{}")


def test_gc_follows_compaction_completion_and_retention(tmp_path):
    rt = Runtime(with_changes(example_config(), run__work_dir=str(tmp_path)))
    root, ex = tmp_path, tmp_path / "exports"
    save_state(rt.layout, {
        "base": {"id": "B1", "as_of": "2026-10-01T00:00:00+00:00"}, "dims": "B1",
        "compacted": {"features": "f1", "as_of": "2026-10-02T00:00:00+00:00"},
        "deltas": [{"id": "D9", "as_of": "2026-10-03T00:00:00+00:00"}],
        "in_progress": {"full": {"id": "J", "shards": [0]}, "delta": {"id": "P"}},
        "watermarks": {}, "shards": [0], "cycles_since_full": 1})
    atomic_write_json(root / "state" / "pointers.json", {"golden": "g1", "features": "f3"})
    for name in ("base_B0", "base_B1", "base_J", "delta_D1", "delta_D9", "delta_P", "delta_Jc",
                 "dims_B0", "dims_B1", "dims_J"):
        _dir(ex / name)
    for name in ("f1", "f2", "f3"):
        _dir(root / "features" / name, "meta.json")
    _dir(root / "features" / "f4")
    _dir(root / "features" / "f5", "meta.json")  # newest, but a crash orphan nobody names
    for i in range(1, 9):
        _dir(root / "golden" / f"g{i}", "meta.json")
    _dir(root / "golden" / "g9")
    _dir(root / "runs" / "r1")
    _dir(root / "runs" / "r2", "manifest.json")
    for i in range(1, 8):
        _dir(root / "backtests" / f"t{i}", "backtest.json")
    _dir(root / "duckdb_tmp")
    (root / "duckdb_tmp" / "duckdb_temp_storage_S32K-0.tmp").write_bytes(b"x")

    gc_storage(rt)

    def names(sub):
        return sorted(p.name for p in (root / sub).iterdir())

    # The compacted set replaced base B1 as the merge source; merged delta D1 is gone.
    assert names("exports") == ["base_J", "delta_D9", "delta_Jc", "delta_P", "dims_B1", "dims_J"]
    assert names("features") == ["f1", "f3"]  # only the compacted set and the pointer
    assert names("golden") == ["g1", "g3", "g4", "g5", "g6", "g7", "g8"]  # g1 is the pointer
    assert names("runs") == ["r2"]
    assert names("backtests") == [f"t{i}" for i in range(2, 8)]
    assert not (root / "duckdb_tmp").exists()


def test_gc_keeps_complete_labels_and_drops_partial_ones(tmp_path):
    rt = Runtime(with_changes(example_config(), run__work_dir=str(tmp_path)))
    labels = tmp_path / "labels"
    _dir(labels / "18", "meta.json")
    _dir(labels / "19", "meta.json")
    _dir(labels / "20")  # crashed before its meta was written
    _dir(labels / ".21.20261003T000000000000Z", "meta.json")  # an unrenamed build
    for i in range(1, 9):
        _dir(tmp_path / "forward_evals" / f"e{i}", "forward_eval.json")
    gc_storage(rt)
    assert sorted(p.name for p in labels.iterdir()) == ["18", "19"]
    assert sorted(p.name for p in (tmp_path / "forward_evals").iterdir()) == [
        f"e{i}" for i in range(3, 9)]
