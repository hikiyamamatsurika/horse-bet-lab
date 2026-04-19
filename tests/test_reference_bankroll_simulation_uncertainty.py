from __future__ import annotations

from pathlib import Path

from horse_bet_lab.config import load_reference_bankroll_simulation_uncertainty_config


def test_reference_bankroll_simulation_uncertainty_loader(tmp_path: Path) -> None:
    simulation_config = tmp_path / "reference_bankroll_simulation.toml"
    simulation_config.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_bankroll_simulation_test'",
                (
                    "reference_label_guard_compare_config_path = "
                    "'configs/reference_label_guard_compare_dual_market_logreg_2023_2025.toml'"
                ),
                f"output_dir = '{tmp_path / 'simulation_output'}'",
                "stake_variants = ['flat_100']",
                "initial_bankrolls = [5000]",
                "",
            ],
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "reference_bankroll_simulation_uncertainty.toml"
    config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_bankroll_simulation_uncertainty_test'",
                f"reference_bankroll_simulation_config_path = '{simulation_config}'",
                f"output_dir = '{tmp_path / 'output'}'",
                "stake_variants = ['flat_100', 'capped_fractional_kelly_like_per_race_cap']",
                "initial_bankrolls = [5000, 10000]",
                "bootstrap_iterations = 20",
                "random_seed = 11",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_reference_bankroll_simulation_uncertainty_config(config_path)
    assert config.stake_variants == ("flat_100", "capped_fractional_kelly_like_per_race_cap")
    assert config.initial_bankrolls == (5000.0, 10000.0)
    assert config.bootstrap_iterations == 20
    assert config.random_seed == 11
    assert config.bootstrap_block_unit == "race_date"
