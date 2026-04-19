from __future__ import annotations

from pathlib import Path

import duckdb

from horse_bet_lab.config import load_ranking_rule_comparison_config
from horse_bet_lab.evaluation.ranking_rule_rollforward import run_ranking_rule_comparison


def test_ranking_rule_rollforward_compares_edge_and_pred_times(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    rolling_predictions_path = tmp_path / "rolling_predictions.csv"
    base_config_path = tmp_path / "base_backtest.toml"
    analysis_config_path = tmp_path / "ranking_rule_compare.toml"
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
                ('race_t1', 2, '2025-02-10', 8.0, 3)
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
                ('race_t1', 2, 2.8)
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
                ('race_v1', 1, 140.0, NULL, NULL, NULL, NULL),
                ('race_t1', 2, 260.0, NULL, NULL, NULL, NULL)
            """,
        )
    finally:
        connection.close()

    rolling_predictions_path.write_text(
        "race_key,horse_number,split,target_value,pred_probability,window_label\n"
        "race_v1,1,valid,1,0.60,2025_01_to_02\n"
        "race_v1,2,valid,0,0.43,2025_01_to_02\n"
        "race_t1,1,test,0,0.60,2025_01_to_02\n"
        "race_t1,2,test,1,0.43,2025_01_to_02\n",
        encoding="utf-8",
    )

    base_config_path.write_text(
        "\n".join(
            [
                "[backtest]",
                "name = 'base'",
                f"predictions_path = '{rolling_predictions_path}'",
                f"duckdb_path = '{duckdb_path}'",
                f"output_dir = '{output_dir}'",
                "selection_metric = 'edge'",
                "market_prob_method = 'oz_place_basis_inverse'",
                "thresholds = [0.08]",
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

    analysis_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'ranking_rule_compare_test'",
                f"base_backtest_config_path = '{base_config_path}'",
                f"rolling_predictions_path = '{rolling_predictions_path}'",
                f"output_dir = '{output_dir}'",
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

    config = load_ranking_rule_comparison_config(analysis_config_path)
    result = run_ranking_rule_comparison(config)

    assert (result.output_dir / "selected_test_rollup.csv").exists()
    assert (result.output_dir / "diff_summary.csv").exists()
    assert (result.output_dir / "strategy_variant_rollup.csv").exists()
    assert (result.output_dir / "strategy_variant_diff_summary.csv").exists()
    assert {row.ranking_score_rule for row in result.rollups} == {
        "edge",
        "pred_times_place_basis_odds",
    }
    assert {row.strategy_variant for row in result.strategy_variant_rollups} == {
        "consensus",
        "edge",
        "pred_times_place_basis_odds",
    }
    pred_rollup = next(
        row
        for row in result.rollups
        if row.ranking_score_rule == "pred_times_place_basis_odds"
    )
    assert pred_rollup.bet_count == 1
    assert pred_rollup.roi == 2.6
    consensus_rollup = next(
        row
        for row in result.strategy_variant_rollups
        if row.strategy_variant == "consensus"
    )
    assert consensus_rollup.bet_count == 0
    assert consensus_rollup.roi == 0.0
    diff_row = next(
        row
        for row in result.diff_summaries
        if row.ranking_score_rule == "pred_times_place_basis_odds"
        and row.set_group == "score_variant_only"
    )
    assert diff_row.bet_count == 1
    assert diff_row.roi == 2.6
    pred_only_row = next(
        row
        for row in result.strategy_variant_diff_summaries
        if row.set_group == "pred_only"
    )
    edge_only_row = next(
        row
        for row in result.strategy_variant_diff_summaries
        if row.set_group == "edge_only"
    )
    assert pred_only_row.bet_count == 1
    assert pred_only_row.roi == 2.6
    assert edge_only_row.bet_count == 1
    assert edge_only_row.roi == 0.0
