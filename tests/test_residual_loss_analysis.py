from __future__ import annotations

from pathlib import Path

import duckdb

from horse_bet_lab.config import load_residual_loss_analysis_config
from horse_bet_lab.evaluation.residual_loss_analysis import run_residual_loss_analysis


def test_residual_loss_analysis_outputs_expected_artifacts(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    rolling_predictions_path = tmp_path / "rolling_predictions.csv"
    ranking_config_path = tmp_path / "ranking_rule_compare.toml"
    reference_config_path = tmp_path / "reference_strategy.toml"
    guard_config_path = tmp_path / "reference_guard.toml"
    analysis_config_path = tmp_path / "residual_loss.toml"
    output_dir = tmp_path / "output"

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
                ('race_v1', 1, '2023-01-10', 5.0, 3),
                ('race_v1', 2, '2023-01-10', 7.0, 3),
                ('race_t1', 1, '2023-02-10', 5.0, 3),
                ('race_t1', 2, '2023-02-10', 7.0, 3),
                ('race_t2', 1, '2024-02-10', 5.0, 2),
                ('race_t2', 2, '2024-02-10', 7.0, 3)
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
                ('race_v1', 2, 2.5),
                ('race_t1', 1, 2.5),
                ('race_t1', 2, 2.5),
                ('race_t2', 1, 2.2),
                ('race_t2', 2, 2.5)
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
                "race_v1,1,valid,0,0.50,2023_01_to_02",
                "race_v1,2,valid,1,0.47,2023_01_to_02",
                "race_t1,1,test,0,0.50,2023_01_to_02",
                "race_t1,2,test,1,0.47,2023_01_to_02",
                "race_t2,1,test,0,0.46,2024_01_to_02",
                "race_t2,2,test,1,0.45,2024_01_to_02",
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
                "label = '2024_01_to_02'",
                "valid_start_date = '2024-01-01'",
                "valid_end_date = '2024-01-31'",
                "test_start_date = '2024-02-01'",
                "test_end_date = '2024-02-29'",
                "",
                "[[backtest.selection_window_groups]]",
                "label = 'test_2023_02'",
                "valid_window_labels = ['2023_01_to_02']",
                "test_window_label = '2023_01_to_02'",
                "",
                "[[backtest.selection_window_groups]]",
                "label = 'test_2024_02'",
                "valid_window_labels = ['2023_01_to_02', '2024_01_to_02']",
                "test_window_label = '2024_01_to_02'",
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
                "",
            ],
        ),
        encoding="utf-8",
    )
    analysis_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'residual_loss_test'",
                f"reference_guard_compare_config_path = '{guard_config_path}'",
                f"output_dir = '{output_dir}'",
                "baseline_variant = 'baseline'",
                "guarded_variant = 'problematic_band_excluded'",
                "representative_examples_per_regime = 1",
                "",
            ],
        ),
        encoding="utf-8",
    )

    result = run_residual_loss_analysis(load_residual_loss_analysis_config(analysis_config_path))

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "bucket_summary.csv").exists()
    assert (result.output_dir / "top_loss_bands.csv").exists()
    assert (result.output_dir / "representative_examples.csv").exists()
    assert any(row.variant == "baseline" for row in result.summaries)
    assert any(row.variant == "problematic_band_excluded" for row in result.summaries)
    assert any(row.bucket_type == "popularity" for row in result.bucket_rows)
    assert any(row.bucket_type == "place_basis_odds" for row in result.bucket_rows)
    assert any(row.bucket_type == "win_odds" for row in result.bucket_rows)
    assert any(row.bucket_type == "edge" for row in result.bucket_rows)
    assert (result.output_dir / "top_loss_bands.csv").exists()
    assert len(result.representative_rows) > 0
