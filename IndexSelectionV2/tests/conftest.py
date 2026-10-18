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
    cfg = replace(cfg, "duckdb", threads=2, memory_limit="2GB",
                  max_temp_directory_size="2GB")
    return cfg


@pytest.fixture
def base_cfg() -> Config:
    return load_config([BASE])


