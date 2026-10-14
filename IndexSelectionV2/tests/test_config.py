from __future__ import annotations

from pathlib import Path

import pytest
from conftest import BASE

from isel.config import ConfigError, load_config


def _overlay(tmp_path: Path, text: str) -> list[Path]:
    path = tmp_path / "overlay.toml"
    path.write_text(text)
    return [BASE, path]


def test_base_config_loads(base_cfg):
    assert len(base_cfg.sha256()) == 64


@pytest.mark.parametrize("text, message", [
    ("[run]\nunknown = 1\n", "unknown keys"),
    ("[crawlerdb.session]\ndefault_transaction_read_only = 'off'\n", "read_only"),
    ("[crawlerdb.session]\nstatement_timeout = '2min'\n", "statement_timeout"),
    ("[metricdb.session]\nstatement_timeout = '1000s'\n", "timeout_cap_s"),
    ("[metricdb.session]\nstandard_conforming_strings = 'off'\n", "standard_conforming"),
    ("[guard]\ntimeout_cap_s = 2880\n", "hourly open gap"),
    ("[guard]\nmonth_window = ['20:00', '00:00']\n", "wrap"),
    ("[guard]\nmonth_window = ['00:00', '25:00']\n", "HH:MM"),
])
def test_config_rejects_bad_values(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load_config(_overlay(tmp_path, text))


def test_config_rejects_missing_key(tmp_path):
    text = BASE.read_text().replace('app_name = "isel"', "")
    path = tmp_path / "full.toml"
    path.write_text(text)
    with pytest.raises(ConfigError, match="missing keys"):
        load_config([path])


