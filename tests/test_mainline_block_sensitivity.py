from __future__ import annotations

from pathlib import Path

from horse_bet_lab.config import load_mainline_block_sensitivity_config


def test_mainline_block_sensitivity_loader(tmp_path: Path) -> None:
    config_path = tmp_path / "mainline_block_sensitivity.toml"
    config_path.write_text(
        """
[analysis]
name = "mainline_block_sensitivity_test"
reference_label_guard_uncertainty_config_path = "configs/a.toml"
reference_bankroll_simulation_uncertainty_config_path = "configs/b.toml"
output_dir = "data/artifacts/mainline_block_sensitivity_test"
bootstrap_block_units = ["race_date", "week", "month"]
stateful_stake_variant = "capped_fractional_kelly_like_per_race_cap"
stateful_initial_bankrolls = [10000, 30000]
""".strip(),
        encoding="utf-8",
    )

    config = load_mainline_block_sensitivity_config(config_path)

    assert config.name == "mainline_block_sensitivity_test"
    assert config.bootstrap_block_units == ("race_date", "week", "month")
    assert config.stateful_stake_variant == "capped_fractional_kelly_like_per_race_cap"
    assert config.stateful_initial_bankrolls == (10000.0, 30000.0)
