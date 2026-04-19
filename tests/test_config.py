from pathlib import Path

from horse_bet_lab.config import load_experiment_config


def test_load_experiment_config() -> None:
    config = load_experiment_config(Path("configs/default.toml"))

    assert config.name == "baseline_dummy"
    assert config.model == "dummy"
    assert config.feature_set == "minimal"
    assert config.strategy == "flat"
    assert config.period == "2020-01-01_to_2020-12-31"

