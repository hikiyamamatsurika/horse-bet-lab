from __future__ import annotations

from pathlib import Path

import duckdb

from horse_bet_lab.config import load_reference_strategy_diagnostics_config
from horse_bet_lab.evaluation.reference_strategy import run_reference_strategy_diagnostics


def test_reference_strategy_diagnostics_outputs_equity_and_group_summaries(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    rolling_predictions_path = tmp_path / "rolling_predictions.csv"
    base_config_path = tmp_path / "ranking_rule_compare.toml"
    diagnostics_config_path = tmp_path / "reference_strategy.toml"
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
                ('race_v1', 1, '2025-01-10', 5.0, 2),
                ('race_v1', 2, '2025-01-10', 8.0, 3),
                ('race_t1', 1, '2025-02-10', 5.0, 2),
                ('race_t2', 2, '2025-02-11', 8.0, 3)
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
                ('race_v1', 1, 2.0),
                ('race_v1', 2, 2.8),
                ('race_t1', 1, 2.0),
                ('race_t2', 2, 2.8)
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
                ('race_t1', NULL, NULL, NULL, NULL, NULL, NULL),
                ('race_t2', 2, 260.0, NULL, NULL, NULL, NULL)
            """,
        )
    finally:
        connection.close()

    rolling_predictions_path.write_text(
        "race_key,horse_number,split,target_value,pred_probability,window_label\n"
        "race_v1,1,valid,1,0.60,2025_01_to_02\n"
        "race_v1,2,valid,0,0.43,2025_01_to_02\n"
        "race_t1,1,test,0,0.60,2025_01_to_02\n"
        "race_t2,2,test,1,0.43,2025_01_to_02\n",
        encoding="utf-8",
    )

    base_config_path.write_text(
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
                "label = '2025_01_to_02'",
                "valid_start_date = '2025-01-01'",
                "valid_end_date = '2025-01-31'",
                "test_start_date = '2025-02-01'",
                "test_end_date = '2025-02-28'",
                "",
                "[[backtest.selection_window_groups]]",
                "label = 'test_2025_02'",
                "valid_window_labels = ['2025_01_to_02']",
                "test_window_label = '2025_01_to_02'",
                "",
            ],
        ),
        encoding="utf-8",
    )
    diagnostics_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_strategy_test'",
                f"ranking_rule_comparison_config_path = '{base_config_path}'",
                f"output_dir = '{output_dir}'",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_reference_strategy_diagnostics_config(diagnostics_config_path)
    result = run_reference_strategy_diagnostics(config)

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "equity_curve.csv").exists()
    assert (result.output_dir / "monthly_profit.csv").exists()
    assert (result.output_dir / "window_profit.csv").exists()
    assert result.summary.bet_count == 2
    assert result.summary.hit_count == 1
    assert result.summary.roi == 1.3
    assert result.summary.total_profit == 60.0
    assert result.summary.max_drawdown == 100.0
    assert result.summary.max_losing_streak == 1
    assert len(result.equity_curve) == 2
    assert result.monthly_profit[0].group_key == "2025-02"
    assert result.window_profit[0].group_key == "test_2025_02"
