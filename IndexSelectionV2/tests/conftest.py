from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from isel.config import Config, load_config

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "config" / "isel.toml"


def replace(cfg: Config, section: str, **values) -> Config:
    return dataclasses.replace(cfg, **{section: dataclasses.replace(getattr(cfg, section),
                                                                    **values)})


def small_cfg(work_dir: Path) -> Config:
    cfg = load_config([BASE])
    cfg = replace(cfg, "run", work_dir=str(work_dir))
    cfg = replace(cfg, "dataset", neg_rate=0.5, first_label_batch=2)
    cfg = replace(cfg, "model", num_boost_round=150, early_stopping_rounds=20,
                  params={**cfg.model.params, "num_leaves": 15, "min_data_in_leaf": 20,
                          "num_threads": 2, "learning_rate": 0.1})
    cfg = replace(cfg, "eval", test_batches=[5, 6, 7, 8], k_values=[500, 1000],
                  k_headline=1000, fractions=[0.1], gain_curve_k=[250, 1000, 4000],
                  bootstrap_samples=50)
    cfg = replace(cfg, "duckdb", threads=2, memory_limit="2GB",
                  max_temp_directory_size="2GB")
    cfg = replace(cfg, "manifest", index_shards=4)
    return cfg


@pytest.fixture
def base_cfg() -> Config:
    return load_config([BASE])


class FakeConn:
    """Records statements; returns canned rows keyed by a substring of the SQL."""

    def __init__(self, rows: dict[str, list[tuple]] | None = None, clock=None):
        self.rows = rows or {}
        self.clock = clock
        self.executed: list[tuple[str, object]] = []
        self.executed_at: list[object] = []

    def cursor(self):
        conn = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, text, params=None):
                conn.executed.append((text, params))
                conn.executed_at.append(conn.clock() if conn.clock else None)
                self._rows = next((r for k, r in conn.rows.items() if k in text), [])

            def fetchall(self):
                return self._rows

        return Cursor()
