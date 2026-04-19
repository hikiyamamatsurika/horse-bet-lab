from pathlib import Path

from horse_bet_lab.config import load_experiment_config
from horse_bet_lab.runner import render_run_summary


def test_render_run_summary() -> None:
    config = load_experiment_config(Path("configs/default.toml"))

    output = render_run_summary(config)

    assert "Running dummy experiment:" in output
    assert "name: baseline_dummy" in output
    assert "model: dummy" in output
