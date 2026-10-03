import pytest
from helpers import EXAMPLE, example_config

from idxsel.config import ConfigError, load_config


def _variant(tmp_path, old, new):
    text = EXAMPLE.read_text()
    assert old in text
    path = tmp_path / "variant.toml"
    path.write_text(text.replace(old, new, 1))
    return path


def test_example_config_loads():
    cfg = example_config()
    assert cfg.run.k == 128_000_000
    assert cfg.active_profile.tiers == ["past_golden"]
    assert cfg.sha256() == example_config().sha256()


def test_overlay_overrides_values(tmp_path):
    overlay = tmp_path / "o.toml"
    overlay.write_text("[run]\nk = 10\n[selection]\nprofile = 'ext_tier_v1'\n")
    cfg = load_config([EXAMPLE, overlay])
    assert cfg.run.k == 10
    assert cfg.active_profile.tiers == ["past_golden", "external_inlink"]


def test_unknown_key_rejected(tmp_path):
    overlay = tmp_path / "o.toml"
    overlay.write_text("[run]\nkk = 10\n")
    with pytest.raises(ConfigError, match="unknown keys"):
        load_config([EXAMPLE, overlay])


def test_missing_key_rejected(tmp_path):
    path = _variant(tmp_path, "k = 128_000_000", "")
    with pytest.raises(ConfigError, match="missing keys"):
        load_config([path])


def test_wrong_type_rejected(tmp_path):
    path = _variant(tmp_path, "k = 128_000_000", "k = 'many'")
    with pytest.raises(ConfigError, match="expected int"):
        load_config([path])


@pytest.mark.parametrize(("old", "new", "message"), [
    ("rate_mb_s_min = 20", "rate_mb_s_min = 200", "rate_mb_s_min"),
    ("profile = \"baseline_v1\"", "profile = \"nope\"", "not in selection.profiles"),
    ("tiers = [\"past_golden\"]", "tiers = [\"vip\"]", "unknown tiers"),
    ("target_batch = \"next\"", "target_batch = \"later\"", "target_batch"),
    ("shard_order = \"ingestor_round_robin\"", "shard_order = \"random\"", "shard_order"),
])
def test_semantic_validation(tmp_path, old, new, message):
    with pytest.raises(ConfigError, match=message):
        load_config([_variant(tmp_path, old, new)])


@pytest.mark.parametrize(("section", "body", "message"), [
    ("run", "k = 0", "run.k must be >= 1"),
    ("storage", "keep_runs = 0", "storage.keep_runs"),
    ("guard", "build_reserve_gb = -1", "build_reserve_gb"),
    ("eval", "live_check_sample_size = 0", "live_check_sample_size"),
    ("schedule", "delta_max_h = 0", "schedule.delta_max_h"),
    ("guard", "work_dir_scan_interval_s = -1", "work_dir_scan_interval_s"),
    ("storage", "keep_base_exports = 1", "unknown keys"),
    ("storage", "keep_feature_sets = 1", "unknown keys"),
    ("schedule", "forward_eval_delay_h = 0", "forward_eval_delay_h"),
    ("labels", "sample_mod = 0", "labels.sample_mod"),
    ("labels", "top_domains = 0", "labels.top_domains"),
    ("labels", "sweep_memory_limit = ''", "sweep_memory_limit"),
    ("golden", "prior_rate_pseudo_golden = 0", "prior_rate_pseudo_golden"),
    ("selection.caps", "prior_rate_lift = 1", "prior_rate_lift"),
    ("selection.caps", "fetch_ok_90d = 10", "unknown keys"),
    ("selection.profiles.baseline_v1.weights", "fetch_ok = 1", "unknown keys"),
    ("selection", "url_date_regex = '/(20[0-9]{2})/'", "needs 3 groups"),
    ("selection", "url_date_regex = '/(20'", "does not compile"),
])
def test_bounds_rejected(tmp_path, section, body, message):
    overlay = tmp_path / "o.toml"
    overlay.write_text(f"[{section}]\n{body}\n")
    with pytest.raises(ConfigError, match=message):
        load_config([EXAMPLE, overlay])


def test_all_zero_weights_rejected(tmp_path):
    overlay = tmp_path / "o.toml"
    overlay.write_text(
        "[selection.profiles.baseline_v1.weights]\n"
        "inlinks = 0\next = 0\nurl_score = 0\nprior = 0\n")  # pageview is 0 already
    with pytest.raises(ConfigError, match="must not all be zero"):
        load_config([EXAMPLE, overlay])


def test_local_overlay_example_loads():
    cfg = load_config([EXAMPLE, EXAMPLE.parent / "idxsel.local.example.toml"])
    assert cfg.run.k == 1000
    assert not cfg.guard.remote.enabled
