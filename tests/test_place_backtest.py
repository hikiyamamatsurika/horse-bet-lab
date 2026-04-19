from __future__ import annotations

import json
from pathlib import Path

import duckdb

from horse_bet_lab.config import load_place_backtest_config
from horse_bet_lab.evaluation.place_backtest import run_place_backtest


def test_run_place_backtest_writes_summary_artifacts(tmp_path: Path) -> None:
    predictions_path = tmp_path / "predictions.csv"
    predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,target_value,pred_probability",
                "r1,1,train,1,0.70",
                "r1,2,train,0,0.40",
                "r2,3,valid,1,0.80",
                "r3,5,test,1,0.90",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    duckdb_path = tmp_path / "jrdb.duckdb"
    prepare_hjc_table(duckdb_path)
    prepare_sed_table(duckdb_path)
    prepare_oz_table(duckdb_path)

    config_path = tmp_path / "place_backtest.toml"
    config_path.write_text(
        (
            "[backtest]\n"
            "name = 'place_backtest_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_dir = '{tmp_path / 'artifacts'}'\n"
            "thresholds = [0.50, 0.75]\n"
            "stake_per_bet = 100\n"
        ),
        encoding="utf-8",
    )

    result = run_place_backtest(load_place_backtest_config(config_path))

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "summary.json").exists()
    assert (result.output_dir / "feature_provenance.json").exists()
    assert (result.output_dir / "candidate_summary.csv").exists()
    assert (result.output_dir / "candidate_summary.json").exists()
    assert (result.output_dir / "uncertainty_summary.csv").exists()
    assert (result.output_dir / "uncertainty_summary.json").exists()
    assert (result.output_dir / "yearly_summary.csv").exists()
    assert (result.output_dir / "yearly_summary.json").exists()
    assert (result.output_dir / "monthly_summary.csv").exists()
    assert (result.output_dir / "monthly_odds_bands.csv").exists()
    assert (result.output_dir / "monthly_place_basis_buckets.csv").exists()

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["provenance"]["feature_contract_version"] == "v1"
    assert payload["backtest"]["selection_metric"] == "probability"
    assert payload["backtest"]["thresholds"] == [0.5, 0.75]
    assert len(payload["summaries"]) == 6
    assert result.summaries[0].bet_count == 1
    assert result.summaries[1].bet_count == 1
    assert result.summaries[2].bet_count == 1


def test_run_place_backtest_supports_oz_edge_filters(tmp_path: Path) -> None:
    predictions_path = tmp_path / "predictions.csv"
    predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,target_value,pred_probability",
                "r1,1,valid,1,0.95",
                "r1,2,valid,0,0.20",
                "r2,3,test,1,0.80",
                "r3,5,test,1,0.80",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    duckdb_path = tmp_path / "jrdb.duckdb"
    prepare_hjc_table(duckdb_path)
    prepare_sed_table(duckdb_path)
    prepare_oz_table(duckdb_path)

    config_path = tmp_path / "place_backtest_edge_oz_filters.toml"
    config_path.write_text(
        (
            "[backtest]\n"
            "name = 'place_backtest_edge_oz_filters_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_dir = '{tmp_path / 'artifacts_edge_oz_filters'}'\n"
            "selection_metric = 'edge'\n"
            "market_prob_method = 'oz_place_basis_inverse'\n"
            "thresholds = [0.02]\n"
            "stake_per_bet = 100\n"
            "min_popularity = 1\n"
            "max_popularity = 3\n"
            "min_place_basis_odds = 1.0\n"
            "max_place_basis_odds = 2.5\n"
        ),
        encoding="utf-8",
    )

    result = run_place_backtest(load_place_backtest_config(config_path))

    assert result.summaries[0].selection_metric == "edge"
    assert result.summaries[0].min_popularity == 1
    assert result.summaries[0].max_popularity == 3
    assert result.summaries[0].min_place_basis_odds == 1.0
    assert result.summaries[0].max_place_basis_odds == 2.5
    assert result.summaries[0].bet_count == 0
    assert result.summaries[1].bet_count == 1
    assert result.summaries[2].bet_count == 0


def test_run_place_backtest_supports_rolling_retrain_aggregate_selection(
    tmp_path: Path,
) -> None:
    predictions_path = tmp_path / "predictions.csv"
    predictions_path.write_text(
        "race_key,horse_number,split,target_value,pred_probability\n",
        encoding="utf-8",
    )
    dataset_path = tmp_path / "rolling_dataset.parquet"
    prepare_rolling_dataset_parquet(dataset_path)
    duckdb_path = tmp_path / "jrdb.duckdb"
    prepare_rolling_backtest_tables(duckdb_path)

    config_path = tmp_path / "place_backtest_aggregate_valid_selection.toml"
    config_path.write_text(
        (
            "[backtest]\n"
            "name = 'place_backtest_aggregate_valid_selection_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_dir = '{tmp_path / 'artifacts_aggregate_valid_selection'}'\n"
            "selection_metric = 'edge'\n"
            "market_prob_method = 'oz_place_basis_inverse'\n"
            "thresholds = [0.00, 0.05]\n"
            "stake_per_bet = 100\n"
            "min_bets_valid = 1\n"
            "aggregate_selection_score_rules = [\n"
            "  'mean_valid_roi_minus_std',\n"
            "  'positive_window_count_then_mean_roi_then_min_roi',\n"
            "  'min_valid_roi_then_mean_roi'\n"
            "]\n"
            "[[backtest.popularity_bands]]\n"
            "min = 1\n"
            "max = 3\n"
            "[[backtest.place_basis_bands]]\n"
            "min = 2.0\n"
            "max = 5.0\n"
            "[[backtest.evaluation_window_pairs]]\n"
            "label = 'pair_a'\n"
            "valid_start_date = '2025-02-01'\n"
            "valid_end_date = '2025-02-28'\n"
            "test_start_date = '2025-03-01'\n"
            "test_end_date = '2025-03-31'\n"
            "[[backtest.selection_window_groups]]\n"
            "label = 'test_pair_a'\n"
            "valid_window_labels = ['pair_a']\n"
            "test_window_label = 'pair_a'\n"
            "\n"
            "[rolling_retrain]\n"
            f"dataset_path = '{dataset_path}'\n"
            "feature_columns = ['win_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'identity']\n"
            "race_date_column = 'race_date'\n"
            "max_iter = 1000\n"
        ),
        encoding="utf-8",
    )

    result = run_place_backtest(load_place_backtest_config(config_path))

    assert (result.output_dir / "rolling_predictions.csv").exists()
    assert (result.output_dir / "selected_summary.json").exists()
    assert (result.output_dir / "selected_test_rollup.json").exists()
    assert (result.output_dir / "yearly_summary.json").exists()
    assert len(result.selected_summaries) == 6
    assert {summary.aggregate_selection_score_rule for summary in result.selected_summaries} == {
        "mean_valid_roi_minus_std",
        "positive_window_count_then_mean_roi_then_min_roi",
        "min_valid_roi_then_mean_roi",
    }
    yearly_payload = json.loads((result.output_dir / "yearly_summary.json").read_text(encoding="utf-8"))
    assert len(yearly_payload["yearly_summaries"]) > 0


def test_run_place_backtest_supports_win_payouts(tmp_path: Path) -> None:
    predictions_path = tmp_path / "predictions.csv"
    predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,target_value,pred_probability",
                "r1,1,valid,1,0.75",
                "r2,3,test,1,0.80",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    duckdb_path = tmp_path / "jrdb.duckdb"
    prepare_hjc_table(duckdb_path)
    prepare_sed_table(duckdb_path)
    prepare_oz_table(duckdb_path)

    config_path = tmp_path / "win_backtest.toml"
    config_path.write_text(
        (
            "[backtest]\n"
            "name = 'win_backtest_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_dir = '{tmp_path / 'artifacts_win'}'\n"
            "payout_type = 'win'\n"
            "thresholds = [0.70]\n"
            "stake_per_bet = 100\n"
        ),
        encoding="utf-8",
    )

    result = run_place_backtest(load_place_backtest_config(config_path))

    summary_by_split = {summary.split: summary for summary in result.summaries}
    assert summary_by_split["valid"].hit_count == 1
    assert summary_by_split["valid"].avg_payout == 250.0
    assert summary_by_split["test"].hit_count == 1
    assert summary_by_split["test"].avg_payout == 1500.0

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["backtest"]["payout_type"] == "win"


def test_run_place_backtest_supports_dual_market_robustness_smoke(
    tmp_path: Path,
) -> None:
    predictions_path = tmp_path / "predictions.csv"
    predictions_path.write_text(
        "race_key,horse_number,split,target_value,pred_probability\n",
        encoding="utf-8",
    )
    dataset_path = tmp_path / "rolling_dual_market_dataset.parquet"
    prepare_rolling_dataset_parquet(dataset_path, include_place_basis_odds=True)
    duckdb_path = tmp_path / "jrdb.duckdb"
    prepare_rolling_backtest_tables(duckdb_path)

    config_path = tmp_path / "place_backtest_dual_market_robustness.toml"
    config_path.write_text(
        (
            "[backtest]\n"
            "name = 'place_backtest_dual_market_robustness_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_dir = '{tmp_path / 'artifacts_dual_market_robustness'}'\n"
            "selection_metric = 'edge'\n"
            "market_prob_method = 'oz_place_basis_inverse'\n"
            "thresholds = [0.04, 0.08]\n"
            "stake_per_bet = 100\n"
            "min_bets_valid = 1\n"
            "aggregate_selection_score_rules = [\n"
            "  'mean_valid_roi_minus_std',\n"
            "  'positive_window_count_then_mean_roi_then_min_roi',\n"
            "  'min_valid_roi_then_mean_roi'\n"
            "]\n"
            "[[backtest.popularity_bands]]\n"
            "min = 1\n"
            "max = 3\n"
            "[[backtest.place_basis_bands]]\n"
            "min = 2.0\n"
            "max = 2.8\n"
            "[[backtest.place_basis_bands]]\n"
            "min = 2.0\n"
            "max = 3.0\n"
            "[[backtest.evaluation_window_pairs]]\n"
            "label = '2025_01_to_02'\n"
            "valid_start_date = '2025-02-01'\n"
            "valid_end_date = '2025-02-28'\n"
            "test_start_date = '2025-03-01'\n"
            "test_end_date = '2025-03-31'\n"
            "[[backtest.evaluation_window_pairs]]\n"
            "label = '2025_02_to_03'\n"
            "valid_start_date = '2025-03-01'\n"
            "valid_end_date = '2025-03-31'\n"
            "test_start_date = '2025-04-01'\n"
            "test_end_date = '2025-04-30'\n"
            "[[backtest.selection_window_groups]]\n"
            "label = 'test_2025_03'\n"
            "valid_window_labels = ['2025_01_to_02']\n"
            "test_window_label = '2025_01_to_02'\n"
            "[[backtest.selection_window_groups]]\n"
            "label = 'test_2025_04'\n"
            "valid_window_labels = ['2025_01_to_02', '2025_02_to_03']\n"
            "test_window_label = '2025_02_to_03'\n"
            "\n"
            "[rolling_retrain]\n"
            f"dataset_path = '{dataset_path}'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "race_date_column = 'race_date'\n"
            "max_iter = 1000\n"
        ),
        encoding="utf-8",
    )

    result = run_place_backtest(load_place_backtest_config(config_path))

    assert (result.output_dir / "rolling_predictions.csv").exists()
    assert (result.output_dir / "selected_test_rollup.csv").exists()
    assert (result.output_dir / "candidate_summary.csv").exists()
    assert (result.output_dir / "uncertainty_summary.csv").exists()
    assert (result.output_dir / "yearly_summary.csv").exists()
    assert len(result.selected_summaries) == 12
    assert {summary.aggregate_selection_score_rule for summary in result.selected_summaries} == {
        "mean_valid_roi_minus_std",
        "positive_window_count_then_mean_roi_then_min_roi",
        "min_valid_roi_then_mean_roi",
    }
    assert {summary.min_place_basis_odds for summary in result.selected_summaries} <= {
        2.0,
    }
    assert {summary.max_place_basis_odds for summary in result.selected_summaries} <= {
        2.8,
        3.0,
    }
    rollup_payload = json.loads(
        (result.output_dir / "selected_test_rollup.json").read_text(encoding="utf-8")
    )
    assert len(rollup_payload["rollups"]) == 3
    assert {"roi_ci_lower", "roi_ci_upper", "max_drawdown", "max_losing_streak"} <= set(
        rollup_payload["rollups"][0].keys()
    )


def prepare_hjc_table(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_hjc_staging (
                race_key VARCHAR,
                win_horse_number INTEGER,
                win_payout INTEGER,
                place_horse_number_1 INTEGER,
                place_payout_1 INTEGER,
                place_horse_number_2 INTEGER,
                place_payout_2 INTEGER,
                place_horse_number_3 INTEGER,
                place_payout_3 INTEGER,
                source_file_path VARCHAR,
                source_file_hash VARCHAR,
                ingestion_run_id BIGINT,
                ingested_at TIMESTAMP
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_hjc_staging VALUES
                ('r1', 1, 250, 1, 120, 7, 0, 8, 0, 'sample', 'hash', 1, NOW()),
                ('r2', 3, 1500, 3, 150, 7, 0, 8, 0, 'sample', 'hash', 1, NOW()),
                ('r3', 5, 3000, 5, 300, 7, 0, 8, 0, 'sample', 'hash', 1, NOW())
            """,
        )
    finally:
        connection.close()


def prepare_sed_table(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                result_date DATE,
                win_odds VARCHAR,
                popularity INTEGER
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('r1', 1, '2025-01-05', '2.0', 1),
                ('r1', 2, '2025-01-05', '10.0', 5),
                ('r2', 3, '2025-01-20', '15.0', 7),
                ('r3', 5, '2025-02-10', '30.0', 12)
            """,
        )
    finally:
        connection.close()


def prepare_oz_table(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_oz_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                headcount INTEGER,
                win_basis_odds DOUBLE,
                place_basis_odds DOUBLE
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_oz_staging VALUES
                ('r1', 1, 2, 2.0, 1.2),
                ('r1', 2, 2, 10.0, 3.5),
                ('r2', 3, 1, 15.0, 2.0),
                ('r3', 5, 1, 30.0, 4.0)
            """,
        )
    finally:
        connection.close()


def prepare_rolling_dataset_parquet(path: Path, include_place_basis_odds: bool = False) -> None:
    connection = duckdb.connect()
    try:
        if include_place_basis_odds:
            connection.execute(
                """
                CREATE TABLE rolling_dataset AS
                SELECT * FROM (
                    VALUES
                        ('t1', 1, DATE '2025-01-05', 2.0, 2.2, 1, 1),
                        ('t1', 2, DATE '2025-01-05', 8.0, 4.1, 6, 0),
                        ('t2', 1, DATE '2025-01-12', 1.8, 2.0, 1, 1),
                        ('t2', 2, DATE '2025-01-12', 9.0, 4.4, 7, 0),
                        ('v1', 1, DATE '2025-02-10', 2.1, 2.4, 1, 1),
                        ('v1', 2, DATE '2025-02-10', 7.5, 4.2, 5, 0),
                        ('v2', 1, DATE '2025-02-17', 2.4, 2.6, 2, 1),
                        ('v2', 2, DATE '2025-02-17', 6.0, 4.0, 4, 0),
                        ('s1', 1, DATE '2025-03-10', 2.2, 2.5, 1, 1),
                        ('s1', 2, DATE '2025-03-10', 7.2, 4.3, 5, 0),
                        ('s2', 1, DATE '2025-03-17', 2.5, 2.8, 2, 1),
                        ('s2', 2, DATE '2025-03-17', 6.5, 4.1, 4, 0),
                        ('u1', 1, DATE '2025-04-10', 2.3, 2.6, 1, 1),
                        ('u1', 2, DATE '2025-04-10', 6.8, 4.0, 5, 0)
                ) AS t(
                    race_key,
                    horse_number,
                    race_date,
                    win_odds,
                    place_basis_odds,
                    popularity,
                    target_value
                )
                """,
            )
        else:
            connection.execute(
                """
                CREATE TABLE rolling_dataset AS
                SELECT * FROM (
                    VALUES
                        ('t1', 1, DATE '2025-01-05', 2.0, 1, 1),
                        ('t1', 2, DATE '2025-01-05', 8.0, 6, 0),
                        ('t2', 1, DATE '2025-01-12', 1.8, 1, 1),
                        ('t2', 2, DATE '2025-01-12', 9.0, 7, 0),
                        ('v1', 1, DATE '2025-02-10', 2.1, 1, 1),
                        ('v1', 2, DATE '2025-02-10', 7.5, 5, 0),
                        ('v2', 1, DATE '2025-02-17', 2.4, 2, 1),
                        ('v2', 2, DATE '2025-02-17', 6.0, 4, 0),
                        ('s1', 1, DATE '2025-03-10', 2.2, 1, 1),
                        ('s1', 2, DATE '2025-03-10', 7.2, 5, 0),
                        ('s2', 1, DATE '2025-03-17', 2.5, 2, 1),
                        ('s2', 2, DATE '2025-03-17', 6.5, 4, 0),
                        ('u1', 1, DATE '2025-04-10', 2.3, 1, 1),
                        ('u1', 2, DATE '2025-04-10', 6.8, 5, 0)
                ) AS t(race_key, horse_number, race_date, win_odds, popularity, target_value)
                """,
            )
        connection.execute("COPY rolling_dataset TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        connection.close()


def prepare_rolling_backtest_tables(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_hjc_staging (
                race_key VARCHAR,
                win_horse_number INTEGER,
                win_payout INTEGER,
                place_horse_number_1 INTEGER,
                place_payout_1 INTEGER,
                place_horse_number_2 INTEGER,
                place_payout_2 INTEGER,
                place_horse_number_3 INTEGER,
                place_payout_3 INTEGER,
                source_file_path VARCHAR,
                source_file_hash VARCHAR,
                ingestion_run_id BIGINT,
                ingested_at TIMESTAMP
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_hjc_staging VALUES
                ('v1', 1, 240, 1, 140, 7, 0, 8, 0, 'sample', 'hash', 1, NOW()),
                ('v2', 1, 260, 1, 150, 7, 0, 8, 0, 'sample', 'hash', 1, NOW()),
                ('s1', 1, 250, 1, 160, 7, 0, 8, 0, 'sample', 'hash', 1, NOW()),
                ('s2', 1, 280, 1, 170, 7, 0, 8, 0, 'sample', 'hash', 1, NOW()),
                ('u1', 1, 230, 1, 180, 7, 0, 8, 0, 'sample', 'hash', 1, NOW())
            """,
        )
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                result_date DATE,
                win_odds VARCHAR,
                popularity INTEGER
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('v1', 1, '2025-02-10', '2.1', 1),
                ('v1', 2, '2025-02-10', '7.5', 5),
                ('v2', 1, '2025-02-17', '2.4', 2),
                ('v2', 2, '2025-02-17', '6.0', 4),
                ('s1', 1, '2025-03-10', '2.2', 1),
                ('s1', 2, '2025-03-10', '7.2', 5),
                ('s2', 1, '2025-03-17', '2.5', 2),
                ('s2', 2, '2025-03-17', '6.5', 4),
                ('u1', 1, '2025-04-10', '2.3', 1),
                ('u1', 2, '2025-04-10', '6.8', 5)
            """,
        )
        connection.execute(
            """
            CREATE TABLE jrdb_oz_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                headcount INTEGER,
                win_basis_odds DOUBLE,
                place_basis_odds DOUBLE
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_oz_staging VALUES
                ('v1', 1, 2, 2.1, 2.4),
                ('v1', 2, 2, 7.5, 4.2),
                ('v2', 1, 2, 2.4, 2.6),
                ('v2', 2, 2, 6.0, 4.0),
                ('s1', 1, 2, 2.2, 2.5),
                ('s1', 2, 2, 7.2, 4.3),
                ('s2', 1, 2, 2.5, 2.8),
                ('s2', 2, 2, 6.5, 4.1),
                ('u1', 1, 2, 2.3, 2.6),
                ('u1', 2, 2, 6.8, 4.0)
            """,
        )
    finally:
        connection.close()
