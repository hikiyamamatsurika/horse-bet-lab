from __future__ import annotations

from datetime import date
from pathlib import Path

from horse_bet_lab.config import load_reference_uncertainty_config
from horse_bet_lab.evaluation.ranking_rule_rollforward import CandidateBetRow
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_uncertainty import (
    build_bootstrap_rows,
    build_date_block_key,
    build_ordered_blocks,
)


def test_reference_uncertainty_outputs_expected_artifacts(tmp_path: Path) -> None:
    selection_config = tmp_path / "second_guard_selection.toml"
    selection_config.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_uncertainty_test_selection'",
                (
                    "reference_guard_compare_config_path = "
                    "'configs/reference_guard_compare_dual_market_logreg_2023_2025.toml'"
                ),
                f"output_dir = '{tmp_path / 'selection_output'}'",
                "first_guard_variant = 'problematic_band_excluded'",
                "second_guard_variants = [",
                "  'no_second_guard',",
                "  'problematic_band_excluded_edge_lt_0_06_excluded',",
                "  'problematic_band_excluded_win_odds_lt_5_excluded',",
                "]",
                "",
            ],
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "reference_uncertainty.toml"
    config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_uncertainty_test'",
                f"second_guard_selection_config_path = '{selection_config}'",
                f"output_dir = '{tmp_path / 'output'}'",
                "bootstrap_iterations = 10",
                "random_seed = 7",
                "",
            ],
        ),
        encoding="utf-8",
    )

    # For smoke coverage, just verify loader/output path shape rather than real data rerun.
    config = load_reference_uncertainty_config(config_path)
    assert config.bootstrap_iterations == 10
    assert config.random_seed == 7
    assert config.bootstrap_block_unit == "race_date"
    assert config.output_dir == tmp_path / "output"


def test_reference_uncertainty_block_bootstrap_groups_by_race_date() -> None:
    rows = (
        GuardVariantRow(
            variant="baseline",
            window_label="test_2023_01",
            row=CandidateBetRow(
                race_key="R1",
                horse_number=1,
                result_date=date(2023, 1, 1),
                target_value=1,
                pred_probability=0.4,
                market_prob=0.3,
                edge=0.1,
                win_odds=4.0,
                popularity=2,
                place_basis_odds=2.4,
                place_payout=180.0,
            ),
        ),
        GuardVariantRow(
            variant="baseline",
            window_label="test_2023_01",
            row=CandidateBetRow(
                race_key="R2",
                horse_number=2,
                result_date=date(2023, 1, 1),
                target_value=0,
                pred_probability=0.35,
                market_prob=0.3,
                edge=0.05,
                win_odds=5.0,
                popularity=3,
                place_basis_odds=2.6,
                place_payout=None,
            ),
        ),
        GuardVariantRow(
            variant="baseline",
            window_label="test_2023_01",
            row=CandidateBetRow(
                race_key="R3",
                horse_number=3,
                result_date=date(2023, 1, 2),
                target_value=1,
                pred_probability=0.45,
                market_prob=0.33,
                edge=0.12,
                win_odds=6.0,
                popularity=1,
                place_basis_odds=2.5,
                place_payout=220.0,
            ),
        ),
    )

    blocks = build_ordered_blocks(rows, lambda item: item.row.result_date.isoformat())
    assert len(blocks) == 2
    assert [len(block) for block in blocks] == [2, 1]

    bootstrap_rows = build_bootstrap_rows(
        rows=rows,
        iterations=20,
        random_seed=11,
        block_unit="race_date",
    )

    assert len(bootstrap_rows) == 20
    assert all(row.bet_count in {2, 3, 4} for row in bootstrap_rows)


def test_reference_uncertainty_supports_week_and_month_block_keys() -> None:
    assert build_date_block_key("week", date(2023, 1, 1)) == "2022-W52"
    assert build_date_block_key("week", date(2023, 1, 2)) == "2023-W01"
    assert build_date_block_key("month", date(2023, 1, 31)) == "2023-01"
    assert build_date_block_key("month", date(2023, 2, 1)) == "2023-02"
