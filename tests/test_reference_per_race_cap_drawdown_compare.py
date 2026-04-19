from __future__ import annotations

from pathlib import Path

from horse_bet_lab.config import load_reference_per_race_cap_drawdown_compare_config


def test_reference_per_race_cap_drawdown_compare_loader(tmp_path: Path) -> None:
    bankroll_config = tmp_path / "reference_bankroll_simulation.toml"
    bankroll_config.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_bankroll_simulation_test'",
                (
                    "reference_label_guard_compare_config_path = "
                    "'configs/reference_label_guard_compare_dual_market_logreg_2023_2025.toml'"
                ),
                f"output_dir = '{tmp_path / 'simulation_output'}'",
                "stake_variants = ['capped_fractional_kelly_like_per_race_cap']",
                "initial_bankrolls = [10000]",
                "",
            ],
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "reference_per_race_cap_drawdown_compare.toml"
    config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_per_race_cap_drawdown_compare_test'",
                f"reference_bankroll_simulation_config_path = '{bankroll_config}'",
                f"output_dir = '{tmp_path / 'output'}'",
                "initial_bankrolls = [10000, 30000]",
                "per_race_cap_values = [200, 300]",
                "bootstrap_iterations = 20",
                "random_seed = 19",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_reference_per_race_cap_drawdown_compare_config(config_path)
    assert config.initial_bankrolls == (10000.0, 30000.0)
    assert config.per_race_cap_values == (200.0, 300.0)
    assert config.bootstrap_iterations == 20
    assert config.random_seed == 19
