from __future__ import annotations

from datetime import date
from pathlib import Path
from random import Random

import duckdb

from horse_bet_lab.config import load_reference_label_guard_null_test_config
from horse_bet_lab.evaluation.ranking_rule_rollforward import CandidateBetRow
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_label_guard_compare import (
    permute_guard_row_labels_within_race,
    shuffle_guard_row_labels,
)
from horse_bet_lab.evaluation.reference_label_guard_null_test import (
    run_reference_label_guard_null_test,
    shuffle_valid_rows_for_null_mode,
)


def test_shuffle_guard_row_labels_preserves_multiset_and_reassigns_outcomes() -> None:
    rows = (
        GuardVariantRow(
            variant="baseline",
            window_label="test_1",
            row=CandidateBetRow(
                race_key="race_1",
                horse_number=1,
                result_date=date(2023, 1, 1),
                target_value=1,
                pred_probability=0.5,
                market_prob=0.4,
                edge=0.1,
                win_odds=4.0,
                popularity=2,
                place_basis_odds=2.3,
                place_payout=180.0,
            ),
        ),
        GuardVariantRow(
            variant="baseline",
            window_label="test_1",
            row=CandidateBetRow(
                race_key="race_2",
                horse_number=2,
                result_date=date(2023, 1, 2),
                target_value=0,
                pred_probability=0.4,
                market_prob=0.4,
                edge=0.0,
                win_odds=5.0,
                popularity=3,
                place_basis_odds=2.6,
                place_payout=None,
            ),
        ),
        GuardVariantRow(
            variant="baseline",
            window_label="test_1",
            row=CandidateBetRow(
                race_key="race_3",
                horse_number=3,
                result_date=date(2023, 1, 3),
                target_value=1,
                pred_probability=0.6,
                market_prob=0.5,
                edge=0.1,
                win_odds=3.5,
                popularity=1,
                place_basis_odds=2.1,
                place_payout=240.0,
            ),
        ),
    )

    shuffled = shuffle_guard_row_labels(rows, Random(7))

    assert [row.row.place_payout for row in shuffled].count(None) == 1
    assert sorted(
        value for value in (row.row.place_payout for row in shuffled) if value is not None
    ) == [180.0, 240.0]
    assert sorted(row.row.target_value for row in shuffled) == [0, 1, 1]
    assert any(
        left.row.place_payout != right.row.place_payout
        for left, right in zip(rows, shuffled, strict=True)
    )


def test_permute_guard_row_labels_within_race_preserves_race_multiset() -> None:
    rows = (
        GuardVariantRow(
            variant="baseline",
            window_label="test_1",
            row=CandidateBetRow(
                race_key="race_1",
                horse_number=1,
                result_date=date(2023, 1, 1),
                target_value=1,
                pred_probability=0.5,
                market_prob=0.4,
                edge=0.1,
                win_odds=4.0,
                popularity=2,
                place_basis_odds=2.3,
                place_payout=180.0,
            ),
        ),
        GuardVariantRow(
            variant="baseline",
            window_label="test_1",
            row=CandidateBetRow(
                race_key="race_1",
                horse_number=2,
                result_date=date(2023, 1, 1),
                target_value=0,
                pred_probability=0.4,
                market_prob=0.4,
                edge=0.0,
                win_odds=5.0,
                popularity=3,
                place_basis_odds=2.6,
                place_payout=None,
            ),
        ),
        GuardVariantRow(
            variant="baseline",
            window_label="test_1",
            row=CandidateBetRow(
                race_key="race_2",
                horse_number=3,
                result_date=date(2023, 1, 2),
                target_value=1,
                pred_probability=0.6,
                market_prob=0.5,
                edge=0.1,
                win_odds=3.5,
                popularity=1,
                place_basis_odds=2.1,
                place_payout=240.0,
            ),
        ),
    )

    permuted = permute_guard_row_labels_within_race(rows, Random(7))

    assert sorted(row.row.target_value for row in permuted[:2]) == [0, 1]
    assert sorted(
        value for value in (row.row.place_payout for row in permuted[:2]) if value is not None
    ) == [180.0]
    assert permuted[2].row.place_payout == 240.0
    assert shuffle_valid_rows_for_null_mode(
        rows,
        Random(7),
        "race_internal_permutation",
    ) == permuted


def test_reference_label_guard_null_test_outputs_expected_artifacts(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    rolling_predictions_path = tmp_path / "rolling_predictions.csv"
    ranking_config_path = tmp_path / "ranking_rule_compare.toml"
    reference_config_path = tmp_path / "reference_strategy.toml"
    guard_config_path = tmp_path / "reference_guard.toml"
    selection_config_path = tmp_path / "second_guard_selection.toml"
    compare_config_path = tmp_path / "reference_label_guard_compare.toml"
    null_config_path = tmp_path / "reference_label_guard_null.toml"

    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                result_date DATE,
                win_odds DOUBLE,
                popularity INTEGER
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('race_v1', 1, '2023-01-10', 4.5, 3),
                ('race_v1', 2, '2023-01-10', 6.5, 3),
                ('race_t1', 1, '2023-02-10', 4.5, 3),
                ('race_t1', 2, '2023-02-10', 6.5, 3),
                ('race_v2', 1, '2024-01-10', 4.5, 3),
                ('race_v2', 2, '2024-01-10', 6.5, 3),
                ('race_t2', 1, '2024-08-10', 4.5, 3),
                ('race_t2', 2, '2024-08-10', 6.5, 3)
            """,
        )
        connection.execute(
            """
            CREATE TABLE jrdb_oz_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                place_basis_odds DOUBLE
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_oz_staging VALUES
                ('race_v1', 1, 2.5),
                ('race_v1', 2, 2.2),
                ('race_t1', 1, 2.5),
                ('race_t1', 2, 2.2),
                ('race_v2', 1, 2.5),
                ('race_v2', 2, 2.2),
                ('race_t2', 1, 2.5),
                ('race_t2', 2, 2.2)
            """,
        )
        connection.execute(
            """
            CREATE TABLE jrdb_hjc_staging (
                race_key VARCHAR,
                place_horse_number_1 INTEGER,
                place_payout_1 DOUBLE,
                place_horse_number_2 INTEGER,
                place_payout_2 DOUBLE,
                place_horse_number_3 INTEGER,
                place_payout_3 DOUBLE
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_hjc_staging VALUES
                ('race_t1', 2, 230.0, NULL, NULL, NULL, NULL),
                ('race_t2', 2, 170.0, NULL, NULL, NULL, NULL)
            """,
        )
    finally:
        connection.close()

    rolling_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,target_value,pred_probability,window_label",
                "race_v1,1,valid,0,0.43,2023_01_to_02",
                "race_v1,2,valid,1,0.52,2023_01_to_02",
                "race_t1,1,test,0,0.43,2023_01_to_02",
                "race_t1,2,test,1,0.52,2023_01_to_02",
                "race_v2,1,valid,0,0.43,2024_01_to_08",
                "race_v2,2,valid,1,0.52,2024_01_to_08",
                "race_t2,1,test,0,0.43,2024_01_to_08",
                "race_t2,2,test,1,0.52,2024_01_to_08",
                "",
            ],
        ),
        encoding="utf-8",
    )

    (tmp_path / "base_backtest.toml").write_text(
        "\n".join(
            [
                "[backtest]",
                "name = 'base'",
                f"predictions_path = '{rolling_predictions_path}'",
                f"duckdb_path = '{duckdb_path}'",
                f"output_dir = '{tmp_path / 'ranking_output'}'",
                "selection_metric = 'edge'",
                "market_prob_method = 'oz_place_basis_inverse'",
                "thresholds = [0.04]",
                "stake_per_bet = 100",
                "min_bets_valid = 1",
                "",
                "[[backtest.popularity_bands]]",
                "min = 1",
                "max = 3",
                "",
                "[[backtest.place_basis_bands]]",
                "min = 2.0",
                "max = 2.8",
                "",
                "[[backtest.evaluation_window_pairs]]",
                "label = '2023_01_to_02'",
                "valid_start_date = '2023-01-01'",
                "valid_end_date = '2023-01-31'",
                "test_start_date = '2023-02-01'",
                "test_end_date = '2023-02-28'",
                "",
                "[[backtest.evaluation_window_pairs]]",
                "label = '2024_01_to_08'",
                "valid_start_date = '2024-01-01'",
                "valid_end_date = '2024-01-31'",
                "test_start_date = '2024-08-01'",
                "test_end_date = '2024-08-31'",
                "",
                "[[backtest.selection_window_groups]]",
                "label = 'test_2023_02'",
                "valid_window_labels = ['2023_01_to_02']",
                "test_window_label = '2023_01_to_02'",
                "",
                "[[backtest.selection_window_groups]]",
                "label = 'test_2024_08'",
                "valid_window_labels = ['2023_01_to_02', '2024_01_to_08']",
                "test_window_label = '2024_01_to_08'",
                "",
            ],
        ),
        encoding="utf-8",
    )
    ranking_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'ranking_rule_compare_test'",
                f"base_backtest_config_path = '{tmp_path / 'base_backtest.toml'}'",
                f"rolling_predictions_path = '{rolling_predictions_path}'",
                f"output_dir = '{tmp_path / 'ranking_output'}'",
                (
                    "aggregate_selection_score_rule = "
                    "'positive_window_count_then_mean_roi_then_min_roi'"
                ),
                "min_bets_valid = 1",
                "ranking_score_rules = ['edge', 'pred_times_place_basis_odds']",
                "",
            ],
        ),
        encoding="utf-8",
    )
    reference_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_strategy_test'",
                f"ranking_rule_comparison_config_path = '{ranking_config_path}'",
                f"output_dir = '{tmp_path / 'reference_output'}'",
                "",
            ],
        ),
        encoding="utf-8",
    )
    guard_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_guard_test'",
                f"reference_strategy_config_path = '{reference_config_path}'",
                f"output_dir = '{tmp_path / 'guard_output'}'",
                "problematic_min_popularity = 3",
                "problematic_max_popularity = 3",
                "problematic_min_place_basis_odds = 2.4",
                "problematic_max_place_basis_odds = 2.8",
                "edge_surcharges = [0.02]",
                "exclude_win_odds_below = 5.0",
                "exclude_edge_below = 0.06",
                "",
            ],
        ),
        encoding="utf-8",
    )
    selection_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'second_guard_selection_test'",
                f"reference_guard_compare_config_path = '{guard_config_path}'",
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
    compare_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_label_guard_compare_test'",
                f"second_guard_selection_config_path = '{selection_config_path}'",
                f"output_dir = '{tmp_path / 'analysis_output'}'",
                "extra_guard_variants = [",
                "  'no_extra_label_guard',",
                "  'popularity_3_4_excluded',",
                "  'month_07_08_excluded',",
                "  'popularity_3_4_or_month_07_08_excluded',",
                "]",
                "",
            ],
        ),
        encoding="utf-8",
    )
    null_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_label_guard_null_test'",
                f"reference_label_guard_compare_config_path = '{compare_config_path}'",
                f"output_dir = '{tmp_path / 'null_output'}'",
                "null_iterations = 16",
                "random_seed = 7",
                "null_modes = ['current_shuffle', 'race_internal_permutation']",
                "",
            ],
        ),
        encoding="utf-8",
    )

    result = run_reference_label_guard_null_test(
        load_reference_label_guard_null_test_config(null_config_path),
    )

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "null_distribution.csv").exists()
    assert (result.output_dir / "null_selected_guards.csv").exists()
    assert len(result.summaries) == 2
    assert {row.null_mode for row in result.summaries} == {
        "current_shuffle",
        "race_internal_permutation",
    }
    assert len(result.null_distribution_rows) == 32
    assert len(result.null_selection_rows) > 0
    assert all(0.0 <= row.observed_test_roi_percentile <= 1.0 for row in result.summaries)
    assert all(0.0 <= row.observed_test_profit_percentile <= 1.0 for row in result.summaries)
