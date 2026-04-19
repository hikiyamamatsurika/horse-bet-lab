from __future__ import annotations

from pathlib import Path

from horse_bet_lab.config import load_reference_stake_sizing_uncertainty_config


def test_reference_stake_sizing_uncertainty_loader(tmp_path: Path) -> None:
    compare_config = tmp_path / "reference_stake_sizing_compare.toml"
    compare_config.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_stake_sizing_compare_test'",
                (
                    "reference_label_guard_compare_config_path = "
                    "'configs/reference_label_guard_compare_dual_market_logreg_2023_2025.toml'"
                ),
                f"output_dir = '{tmp_path / 'compare_output'}'",
                "stake_variants = ['flat_100', 'flat_200']",
                "",
            ],
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "reference_stake_sizing_uncertainty.toml"
    config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_stake_sizing_uncertainty_test'",
                f"reference_stake_sizing_compare_config_path = '{compare_config}'",
                f"output_dir = '{tmp_path / 'output'}'",
                "bootstrap_iterations = 10",
                "random_seed = 7",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_reference_stake_sizing_uncertainty_config(config_path)
    assert config.bootstrap_iterations == 10
    assert config.random_seed == 7
    assert config.bootstrap_block_unit == "race_date"
    assert config.output_dir == tmp_path / "output"
