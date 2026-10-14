from __future__ import annotations

from pathlib import Path

import pytest
from conftest import BASE

from isel.config import load_config
from isel.features import LeakageError, resolve_set


def _overlay(tmp_path: Path, text: str) -> list[Path]:
    path = tmp_path / "overlay.toml"
    path.write_text(text)
    return [BASE, path]


def test_feature_set_with_forbidden_column_is_rejected(tmp_path):
    cfg = load_config(_overlay(tmp_path, "[features.sets]\nbad = ['log_ext', 'url_score']\n"))
    with pytest.raises(LeakageError, match="forbidden"):
        resolve_set(cfg, "bad")
    cfg = load_config(_overlay(tmp_path, "[features.sets]\nbad = ['nope']\n"))
    with pytest.raises(LeakageError, match="unregistered"):
        resolve_set(cfg, "bad")
