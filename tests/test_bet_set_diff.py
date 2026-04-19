from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb

from horse_bet_lab.config import load_bet_set_diff_analysis_config
from horse_bet_lab.evaluation.bet_set_diff import analyze_bet_set_diff


def test_analyze_bet_set_diff_writes_summary(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    comparison_root_dir = tmp_path / "comparison"
    output_dir = tmp_path / "artifacts"
    comparison_root_dir.mkdir(parents=True, exist_ok=True)

    prepare_backtest_database(duckdb_path)
    prepare_backtest_dir(
        comparison_root_dir / "baseline" / "backtest",
        probabilities=(0.65, 0.35, 0.20),
    )
    prepare_backtest_dir(
        comparison_root_dir / "variant" / "backtest",
        probabilities=(0.65, 0.50, 0.20),
    )

    config_path = tmp_path / "bet_set_diff.toml"
    config_path.write_text(
        (
            "[analysis]\n"
            "name = 'bet_set_diff_smoke'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"comparison_root_dir = '{comparison_root_dir}'\n"
            f"output_dir = '{output_dir}'\n"
            "selection_metric = 'edge'\n"
            "market_prob_method = 'oz_place_basis_inverse'\n"
            "split_column = 'split'\n"
            "target_column = 'target_value'\n"
            "probability_column = 'pred_probability'\n"
            "stake_per_bet = 100\n"
            "[[analysis.comparisons]]\n"
            "baseline_feature_set = 'baseline'\n"
            "variant_feature_set = 'variant'\n"
        ),
        encoding="utf-8",
    )

    result = analyze_bet_set_diff(load_bet_set_diff_analysis_config(config_path))

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "distribution.csv").exists()
    assert (result.output_dir / "race_examples.csv").exists()

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    rows = payload["analysis"]["rows"]
    assert len(rows) == 3
    common = next(row for row in rows if row["set_group"] == "common")
    baseline_only = next(row for row in rows if row["set_group"] == "baseline_only")
    variant_only = next(row for row in rows if row["set_group"] == "variant_only")
    assert common["bet_count"] == 1
    assert baseline_only["bet_count"] == 0
    assert variant_only["bet_count"] == 1
    assert variant_only["hit_count"] == 1
    assert variant_only["roi"] == 1.4

    distribution_payload = json.loads(
        (result.output_dir / "distribution.json").read_text(encoding="utf-8"),
    )
    assert any(row["metric"] == "edge" for row in distribution_payload["analysis"]["rows"])


def prepare_backtest_database(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                registration_id VARCHAR,
                result_date DATE,
                horse_name VARCHAR,
                distance_m INTEGER,
                finish_position INTEGER,
                win_odds VARCHAR,
                popularity INTEGER,
                source_file_path VARCHAR,
                source_file_hash VARCHAR,
                ingestion_run_id BIGINT,
                ingested_at TIMESTAMP
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('r1', 1, 'a', DATE '2025-02-01', 'h1', 1400, 2, '10.0', 1, 's', 'h', 1, NOW()),
                ('r1', 2, 'b', DATE '2025-02-01', 'h2', 1400, 3, '20.0', 2, 's', 'h', 1, NOW()),
                ('r1', 3, 'c', DATE '2025-02-01', 'h3', 1400, 8, '30.0', 3, 's', 'h', 1, NOW())
            """,
        )
        connection.execute(
            """
            CREATE TABLE jrdb_oz_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                headcount INTEGER,
                win_basis_odds DOUBLE,
                place_basis_odds DOUBLE,
                source_file_path VARCHAR,
                source_file_hash VARCHAR,
                ingestion_run_id BIGINT,
                ingested_at TIMESTAMP
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_oz_staging VALUES
                ('r1', 1, 3, 10.0, 2.0, 's', 'h', 1, NOW()),
                ('r1', 2, 3, 20.0, 2.5, 's', 'h', 1, NOW()),
                ('r1', 3, 3, 30.0, 3.0, 's', 'h', 1, NOW())
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
                ('r1', 1, 180.0, 2, 140.0, NULL, NULL)
            """,
        )
    finally:
        connection.close()


def prepare_backtest_dir(backtest_dir: Path, *, probabilities: tuple[float, float, float]) -> None:
    backtest_dir.mkdir(parents=True, exist_ok=True)
    with (backtest_dir / "rolling_predictions.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "race_key",
                "horse_number",
                "split",
                "target_value",
                "pred_probability",
                "window_label",
            ],
        )
        writer.writerow(["r1", 1, "test", 1, probabilities[0], "2025_01_to_02"])
        writer.writerow(["r1", 2, "test", 1, probabilities[1], "2025_01_to_02"])
        writer.writerow(["r1", 3, "test", 0, probabilities[2], "2025_01_to_02"])

    with (backtest_dir / "selected_summary.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
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
            ],
        )
        writer.writerow(
            [
                "test_2025_02",
                "aggregate_valid_windows",
                "positive_window_count_then_mean_roi_then_min_roi",
                10,
                "valid_aggregate",
                "test",
                "aggregate_valid_windows",
                "2025_01_to_02",
                "2025_01_to_02",
                1.0,
                1,
                1.0,
                1.0,
                0.0,
                "1.0",
                "edge",
                0.08,
                "",
                "",
                1.8,
                3.0,
                1,
                3,
                1,
                1,
                1.0,
                1.8,
                80.0,
                0.1,
            ],
        )
