from __future__ import annotations

from pathlib import Path

import duckdb

from horse_bet_lab.config import load_calibration_drift_config
from horse_bet_lab.evaluation.calibration_drift import run_calibration_drift_analysis


def test_calibration_drift_analysis_writes_expected_outputs(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    rolling_predictions_path = tmp_path / "rolling_predictions.csv"
    ranking_config_path = tmp_path / "ranking_rule_compare.toml"
    reference_config_path = tmp_path / "reference_strategy.toml"
    analysis_config_path = tmp_path / "calibration_drift.toml"
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
                popularity INTEGER,
                finish_position INTEGER
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('race_v23', 1, '2023-01-10', 5.0, 3, 2),
                ('race_t23a', 1, '2023-02-10', 6.0, 3, 4),
                ('race_t23a', 2, '2023-02-10', 6.2, 3, 2),
                ('race_v24', 1, '2024-01-10', 5.0, 3, 1),
                ('race_t24a', 1, '2024-02-10', 6.0, 3, 1),
                ('race_t24a', 2, '2024-02-10', 6.1, 3, 5)
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
                ('race_v23', 1, 2.6),
                ('race_t23a', 1, 2.5),
                ('race_t23a', 2, 2.7),
                ('race_v24', 1, 2.6),
                ('race_t24a', 1, 2.5),
                ('race_t24a', 2, 2.7)
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
                ('race_t23a', 2, 220.0, NULL, NULL, NULL, NULL),
                ('race_t24a', 1, 180.0, NULL, NULL, NULL, NULL)
            """,
        )
    finally:
        connection.close()

    rolling_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,target_value,pred_probability,window_label",
                "race_v23,1,valid,1,0.46,2023_01_to_02",
                "race_t23a,1,test,0,0.49,2023_01_to_02",
                "race_t23a,2,test,1,0.44,2023_01_to_02",
                "race_v24,1,valid,1,0.46,2024_01_to_02",
                "race_t24a,1,test,1,0.50,2024_01_to_02",
                "race_t24a,2,test,0,0.45,2024_01_to_02",
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
                "min = 3",
                "max = 3",
                "",
                "[[backtest.place_basis_bands]]",
                "min = 2.4",
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
    analysis_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'calibration_drift_test'",
                f"reference_strategy_config_path = '{reference_config_path}'",
                f"output_dir = '{output_dir}'",
                "min_popularity = 3",
                "max_popularity = 3",
                "min_place_basis_odds = 2.4",
                "max_place_basis_odds = 2.8",
                "representative_examples_per_regime = 1",
                "",
                "[[analysis.regimes]]",
                "label = '2023'",
                "start_date = '2023-01-01'",
                "end_date = '2023-12-31'",
                "",
                "[[analysis.regimes]]",
                "label = '2024_2025'",
                "start_date = '2024-01-01'",
                "end_date = '2025-12-31'",
                "",
            ],
        ),
        encoding="utf-8",
    )

    result = run_calibration_drift_analysis(load_calibration_drift_config(analysis_config_path))

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "bucket_summary.csv").exists()
    assert (result.output_dir / "representative_examples.csv").exists()
    assert any(row.regime_label == "2023" for row in result.summaries)
    assert any(row.regime_label == "2024_2025" for row in result.summaries)
    assert any(row.metric == "market_prob" for row in result.bucket_rows)
    assert any(row.metric == "pred_probability" for row in result.bucket_rows)
    assert any(row.metric == "edge" for row in result.bucket_rows)
    assert len(result.representative_rows) > 0
