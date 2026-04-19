from __future__ import annotations

from pathlib import Path

from horse_bet_lab.config import load_reference_label_guard_uncertainty_config


def test_reference_label_guard_uncertainty_outputs_expected_artifacts(
    tmp_path: Path,
) -> None:
    compare_config = tmp_path / "reference_label_guard_compare.toml"
    compare_config.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_label_guard_compare_test'",
                (
                    "second_guard_selection_config_path = "
                    "'configs/second_guard_selection_dual_market_logreg_2023_2025.toml'"
                ),
                f"output_dir = '{tmp_path / 'compare_output'}'",
                "extra_guard_variants = [",
                "  'no_extra_label_guard',",
                "  'popularity_3_4_excluded',",
                "]",
                "",
            ],
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "reference_label_guard_uncertainty.toml"
    config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_label_guard_uncertainty_test'",
                f"reference_label_guard_compare_config_path = '{compare_config}'",
                f"output_dir = '{tmp_path / 'output'}'",
                "bootstrap_iterations = 10",
                "random_seed = 7",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_reference_label_guard_uncertainty_config(config_path)
    assert config.bootstrap_iterations == 10
    assert config.random_seed == 7
    assert config.bootstrap_block_unit == "race_date"
    assert config.output_dir == tmp_path / "output"
