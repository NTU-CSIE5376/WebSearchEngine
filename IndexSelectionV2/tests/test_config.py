from __future__ import annotations

from pathlib import Path

import pytest
from conftest import BASE, ROOT

from isel.config import ConfigError, load_config


def _overlay(tmp_path: Path, text: str) -> list[Path]:
    path = tmp_path / "overlay.toml"
    path.write_text(text)
    return [BASE, path]


def test_base_config_loads(base_cfg):
    assert base_cfg.eval.k_headline == 128_000_000
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
    ("[full]\nchunk_blocks = 1000000\n", "readable at guard.min_mb_s"),
    ("[full]\nparse_threads = 0\n", "full.parse_threads must be > 0"),
    ("[full]\nconnections = 0\n", "full.connections and full.max_heap_mb_s must be > 0"),
    ("[full]\nmax_heap_mb_s = 0.0\n", "full.connections and full.max_heap_mb_s must be > 0"),
    # extra_float_digits = 0 would round url_score in the CSV text the full export parses.
    ("[crawlerdb.session]\nextra_float_digits = '0'\n", "CSV text"),
    ("[crawlerdb.session]\ndatestyle = 'SQL'\n", "CSV text"),
    ("[full]\nsample_percent = 'x'\n", "expected float"),
    ("[full]\nsample_percent = 0.00001\n", "at most 4 decimals"),
    ("[full]\nsample_salt = 'isel'\n", "differ from dataset.neg_salt"),
    ("[crawlerdb]\nengine = 'cockroach'\n", "crawlerdb.engine must be one of"),
    ("[crawlerdb]\nio_wait_types = []\n", "io_wait_types must not be empty"),
    ("[full]\nhash_chunks = 3\n", "divide 65536"),
    ("[full]\nchunk_retries = -1\n", "chunk_retries must be >= 0"),
    ("[full]\nagg_share = 1.0\n", "agg_share must be in"),
    ("[full]\nagg_salt = 'sample-v1'\n", "agg_salt must be set and differ"),
    ("[eval]\nk_headline = 5\n", "k_headline"),
    # num_threads = 0 lets OpenMP start one thread per host CPU, past the container quota.
    ("[model.params]\nnum_threads = 0\n", "num_threads must be an integer > 0"),
    ("[model.params]\nnum_threads = 2.5\n", "num_threads must be an integer > 0"),
    ("[model.params]\nn_jobs = 4\n", r"aliases \['n_jobs'\]"),
    ("[selectdb]\nadmin_dsn_env = 'ISEL_CRAWLERDB_DSN'\n", "must differ"),
    ("[selectdb]\nforbidden_ports = []\n", "forbidden_ports"),
    ("[schedule]\nswap_freeze_window = ['20:00', '11:30']\n", "swap_freeze_window"),
])
def test_config_rejects_bad_values(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load_config(_overlay(tmp_path, text))


def test_yugabyte_overlay_switches_the_crawlerdb_engine(base_cfg):
    cfg = load_config([BASE, ROOT / "config/container.toml",
                       ROOT / "config/crawlerdb/yugabyte.toml"])
    assert base_cfg.crawlerdb.engine == "postgres"
    assert cfg.crawlerdb.engine == "yugabyte" and cfg.full.connections == 3
    assert cfg.crawlerdb.dsn_env != base_cfg.crawlerdb.dsn_env
    # The overlay adds follower reads on top of every base session setting.
    assert cfg.crawlerdb.session == {**base_cfg.crawlerdb.session, "yb_read_from_followers": "on"}


def test_config_rejects_missing_key(tmp_path):
    text = BASE.read_text().replace('app_name = "isel"', "")
    path = tmp_path / "full.toml"
    path.write_text(text)
    with pytest.raises(ConfigError, match="missing keys"):
        load_config([path])


def test_config_needs_the_lightgbm_thread_count(tmp_path):
    text = BASE.read_text().replace("\nnum_threads = 8\n", "\n")
    path = tmp_path / "full.toml"
    path.write_text(text)
    with pytest.raises(ConfigError, match="num_threads must be an integer > 0"):
        load_config([path])
