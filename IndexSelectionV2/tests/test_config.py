from __future__ import annotations

import re
from pathlib import Path

import pytest
from conftest import BASE, ROOT

from isel.config import ConfigError, load_config


def _overlay(tmp_path: Path, text: str) -> list[Path]:
    path = tmp_path / "overlay.toml"
    path.write_text(text)
    return [BASE, path]


def test_base_config_loads(base_cfg):
    assert base_cfg.eval.k_headline == 64_000_000
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
    ("[full]\nparse_threads = 0\n", "full.parse_threads must be > 0"),
    ("[full]\nconnections = 0\n", "full.connections and full.max_mb_s must be > 0"),
    ("[full]\nmax_mb_s = 0.0\n", "full.connections and full.max_mb_s must be > 0"),
    # extra_float_digits = 0 would round url_score in the CSV text the full export parses.
    ("[crawlerdb.session]\nextra_float_digits = '0'\n", "CSV text"),
    ("[crawlerdb.session]\ndatestyle = 'SQL'\n", "CSV text"),
    ("[full]\nsample_percent = 'x'\n", "expected float"),
    ("[full]\nsample_percent = 0.00001\n", "at most 4 decimals"),
    ("[full]\nsample_salt = 'isel'\n", "differ from dataset.neg_salt"),
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
    ("[text_index]\nurl = 'http://127.0.0.1:9200/'\n", "without a trailing slash"),
    ("[text_index]\nalias = 'Isel-Text'\n", "alias must be lowercase"),
    ("[text_index]\nquery_type = 'query_string'\n", "query_type must be one of"),
    ("[text_index]\nfields = ['title^x']\n", "weight > 0"),
    ("[text_index]\nfields = []\n", "must be non-empty"),
    # Lucene's CombinedFieldQuery rejects a field weight below 1.
    ("[text_index]\nquery_type = 'combined_fields'\nfields = ['title', 'body^0.5']\n",
     "weights must be >= 1"),
    ("[text_index]\nbulk_bytes = 0\n", "text_index sizes and counts must be > 0"),
    ("[text_index]\nretries = -1\n", "retries, backoff_s and warmup_queries"),
    ("[text_index]\nexpected_bytes_per_doc = 0\n", "text_index sizes and counts must be > 0"),
    ("[text_index]\nprogress_every = 0\n", "text_index sizes and counts must be > 0"),
    ("[text_index]\nflood_free_ratio = 1.0\n", "flood_free_ratio must be in"),
    ("[text_index]\nfetch_k = 5\n", "fetch_k must be >= k"),
    ("[text_index]\nhost_cap = -1\n", "host_cap must be >= 0"),
    ("[text_index]\nprior_boost = -1.0\n", "prior_boost must be >= 0"),
    ("[text_index]\nprior_score_mode = \"max\"\n", "prior_score_mode must be one of"),
    ("[text_index]\nprior_depth = 100\n", "prior_depth must be >= fetch_k"),
    ("[text_index]\nprior_scale = 0.0\n", "prior_scale and prior_scaling_factor must be > 0"),
    ("[text_index]\nprior_scaling_factor = 0.0\n", "prior_scale and prior_scaling_factor"),
    ("[schedule]\nswap_freeze_days = []\n", "swap_freeze_days must not be empty"),
    # Feb 16 to Mar 1 is 13 days: a cycle that starts 13 days early overlaps the last one.
    ("[refresh]\nstart_days_before = 13\n", "start_days_before is too large"),
    ("[schedule]\nswap_freeze_days = [1, 2]\n", "start_days_before is too large"),
    ("[refresh]\nstart_days_before = 0\n", "start_days_before must be >= 1"),
    ("[refresh]\nstart_time = '24:00'\n", "start_time must be before 24:00"),
    ("[refresh]\nstart_time = '2:15pm'\n", "HH:MM"),
    # 2 minutes is shorter than 5 swap retries x 30 s.
    ("[refresh]\nswap_margin_min = 2\n", "swap_margin_min must exceed"),
    ("[refresh]\nswap_margin_min = 3500\n", "no time between the cycle start and swap_by"),
    ("[refresh]\nsync_budget_min = 0\n", "sync_budget_min must be 1 to 1439"),
    ("[refresh]\nsync_budget_min = 1440\n", "sync_budget_min must be 1 to 1439"),
    ("[refresh]\nmin_overlap_share = 1.5\n", "min_overlap_share and min_coverage_ratio"),
    ("[refresh]\nmin_coverage_ratio = -0.1\n", "min_overlap_share and min_coverage_ratio"),
    ("[refresh]\nmax_attempts = 0\n", "max_attempts must be >= 1"),
    ("[refresh]\npoll_s = 0.0\n", "poll_s must be > 0"),
    ("[refresh]\nkill_grace_s = -1.0\n", "kill_grace_s must be >= 0"),
    ("[refresh]\nfirst_golden_day = '2026-12-01'\n", "unknown keys"),
])
def test_config_rejects_bad_values(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load_config(_overlay(tmp_path, text))


@pytest.mark.parametrize("overlays", [
    ["config/container.toml"],
    ["config/container.toml", "config/text_index/host2.toml"],
])
def test_the_deployed_config_layers_load_with_the_refresh_cycle(base_cfg, overlays):
    cfg = load_config([BASE, *(ROOT / o for o in overlays)])
    assert cfg.refresh == base_cfg.refresh
    assert cfg.schedule == base_cfg.schedule


def test_config_without_the_refresh_table_is_refused(tmp_path):
    text = re.sub(r"\n\[refresh\].*?\n(?=\[recrawl\])", "\n", BASE.read_text(), flags=re.S)
    assert "start_days_before" not in text and "[recrawl]" in text
    path = tmp_path / "old.toml"
    path.write_text(text)
    with pytest.raises(ConfigError, match=r"missing keys \['refresh'\]"):
        load_config([path])


def test_host2_overlay_sets_the_text_index():
    cfg = load_config([BASE, ROOT / "config/container.toml",
                       ROOT / "config/text_index/host2.toml"])
    t = cfg.text_index
    assert (ROOT / Path(t.settings_file).relative_to("/app")).is_file()   # the image's copy
    assert t.keep_prev is False and t.shards == 8 and t.prior_file == "live"
    assert cfg.duckdb.threads == 4 and cfg.duckdb.memory_limit == "16GB"


def test_crawlerdb_reads_yugabyte_followers(base_cfg):
    assert base_cfg.crawlerdb.session["yb_read_from_followers"] == "on"
    assert base_cfg.full.connections == 3       # one session per tserver



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
