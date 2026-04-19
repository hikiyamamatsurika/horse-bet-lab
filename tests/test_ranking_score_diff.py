from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from horse_bet_lab.config import load_ranking_score_diff_config
from horse_bet_lab.evaluation.ranking_score_diff import analyze_ranking_score_diff


def test_ranking_score_diff_reports_frontier_swap(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    backtest_dir = tmp_path / "backtest"
    backtest_dir.mkdir()
    output_dir = tmp_path / "output"
    prediction_path = backtest_dir / "rolling_predictions.csv"
    selected_summary_path = backtest_dir / "selected_summary.csv"
    config_path = tmp_path / "ranking_score_diff.toml"

    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                win_odds DOUBLE,
                popularity INTEGER
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('race_a', 1, 6.0, 2),
                ('race_a', 2, 6.5, 3),
                ('race_b', 3, 8.0, 3)
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
                ('race_a', 1, 2.0),
                ('race_a', 2, 2.2),
                ('race_b', 3, 3.5)
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
                ('race_a', 1, 140.0, NULL, NULL, NULL, NULL),
                ('race_b', NULL, NULL, NULL, NULL, NULL, NULL)
            """,
        )
    finally:
        connection.close()

    prediction_path.write_text(
        "race_key,horse_number,split,target_value,pred_probability,window_label\n"
        "race_a,1,test,1,0.61,test_1\n"
        "race_a,2,test,0,0.57,test_1\n"
        "race_b,3,test,0,0.376,test_1\n",
        encoding="utf-8",
    )
    with selected_summary_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=(
                "window_label",
                "selection_score_rule",
                "aggregate_selection_score_rule",
                "min_bets_valid",
                "selected_on_split",
                "applied_to_split",
                "selection_mode",
                "valid_window_labels",
                "test_window_label",
                "valid_aggregate_score",
                "valid_positive_window_count",
                "valid_mean_roi",
                "valid_min_roi",
                "valid_roi_std",
                "valid_window_rois",
                "selection_metric",
                "threshold",
                "min_win_odds",
                "max_win_odds",
                "min_place_basis_odds",
                "max_place_basis_odds",
                "min_popularity",
                "max_popularity",
                "bet_count",
                "hit_count",
                "hit_rate",
                "roi",
                "total_profit",
                "avg_payout",
                "avg_edge",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "window_label": "group_1",
                "selection_score_rule": "aggregate_valid_windows",
                "aggregate_selection_score_rule": (
                    "positive_window_count_then_mean_roi_then_min_roi"
                ),
                "min_bets_valid": "10",
                "selected_on_split": "valid_aggregate",
                "applied_to_split": "test",
                "selection_mode": "aggregate_valid_windows",
                "valid_window_labels": "valid_1",
                "test_window_label": "test_1",
                "valid_aggregate_score": "1.0",
                "valid_positive_window_count": "1",
                "valid_mean_roi": "1.0",
                "valid_min_roi": "1.0",
                "valid_roi_std": "0.0",
                "valid_window_rois": "1.000000",
                "selection_metric": "edge",
                "threshold": "0.10",
                "min_win_odds": "",
                "max_win_odds": "",
                "min_place_basis_odds": "2.0",
                "max_place_basis_odds": "3.5",
                "min_popularity": "1",
                "max_popularity": "3",
                "bet_count": "2",
                "hit_count": "1",
                "hit_rate": "0.5",
                "roi": "0.7",
                "total_profit": "-60",
                "avg_payout": "140.0",
                "avg_edge": "0.1077",
            },
        )

    config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'ranking_score_diff_test'",
                f"duckdb_path = '{duckdb_path}'",
                f"backtest_dir = '{backtest_dir}'",
                f"output_dir = '{output_dir}'",
                "selection_metric = 'edge'",
                (
                    "aggregate_selection_score_rule = "
                    "'positive_window_count_then_mean_roi_then_min_roi'"
                ),
                "min_bets_valid = 10",
                "split_column = 'split'",
                "target_column = 'target_value'",
                "probability_column = 'pred_probability'",
                "stake_per_bet = 100.0",
                "ranking_scores = ['edge', 'pred_times_place_basis_odds']",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_ranking_score_diff_config(config_path)
    result = analyze_ranking_score_diff(config)

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "diff_summary.csv").exists()
    assert (result.output_dir / "distribution.json").exists()
    assert (result.output_dir / "race_examples.csv").exists()

    baseline_summary = next(
        row for row in result.summaries if row.score_name == "baseline_edge_threshold"
    )
    assert baseline_summary.bet_count == 2

    variant_only_summary = next(
        row
        for row in result.diff_summaries
        if row.score_name == "pred_times_place_basis_odds"
        and row.set_group == "score_variant_only"
    )
    assert variant_only_summary.bet_count == 1
    assert variant_only_summary.roi == 0.0

    edge_distribution_exists = any(
        row.score_name == "pred_times_place_basis_odds"
        and row.metric == "edge"
        and row.set_group == "score_variant_only"
        for row in result.distributions
    )
    assert edge_distribution_exists
