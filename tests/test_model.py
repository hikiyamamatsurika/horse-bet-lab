from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from horse_bet_lab.config import load_model_train_config
from horse_bet_lab.model.service import train_logistic_regression_baseline


def test_train_logistic_regression_baseline_writes_metrics_and_predictions(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset.parquet"
    create_smoke_dataset(dataset_path)

    config_path = tmp_path / "model.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'odds_only_logreg_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds']\n"
            "feature_transforms = ['identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts'}'\n"
            "max_iter = 200\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))

    metrics_path = summary.output_dir / "metrics.json"
    predictions_path = summary.output_dir / "predictions.csv"
    calibration_path = summary.output_dir / "calibration.csv"

    assert metrics_path.exists()
    assert predictions_path.exists()
    assert calibration_path.exists()
    assert (predictions_path.with_name(f"{predictions_path.name}.provenance.json")).exists()
    assert (calibration_path.with_name(f"{calibration_path.name}.provenance.json")).exists()

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert metrics["provenance"]["feature_contract_version"] == "v1"
    assert metrics["provenance"]["model_feature_columns"] == ["win_odds"]
    assert metrics["training"]["feature_columns"] == ["win_odds"]
    assert metrics["training"]["feature_transforms"] == ["identity"]
    assert set(metrics["metrics"]) == {"train", "valid", "test"}

    prediction_rows = predictions_path.read_text(encoding="utf-8").strip().splitlines()
    assert (
        prediction_rows[0]
        == "race_key,horse_number,split,target_value,pred_probability,window_label"
    )
    assert len(prediction_rows) == 7


def test_train_logistic_regression_baseline_supports_popularity_feature(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_with_popularity.parquet"
    create_smoke_dataset(dataset_path, include_popularity=True)

    config_path = tmp_path / "model_with_popularity.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'odds_only_plus_popularity_logreg_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'popularity']\n"
            "feature_transforms = ['identity', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_with_popularity'}'\n"
            "max_iter = 200\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))

    metrics_path = summary.output_dir / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics["training"]["feature_columns"] == ["win_odds", "popularity"]
    assert metrics["training"]["feature_transforms"] == ["identity", "identity"]
    assert set(metrics["metrics"]) == {"train", "valid", "test"}
    assert len(metrics["model"]["coefficients"]) == 2


def test_train_logistic_regression_baseline_supports_log1p_win_odds(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_log.parquet"
    create_smoke_dataset(dataset_path)

    config_path = tmp_path / "model_log.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'odds_only_log1p_logreg_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds']\n"
            "feature_transforms = ['log1p']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_log'}'\n"
            "max_iter = 200\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))

    metrics_path = summary.output_dir / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics["training"]["feature_columns"] == ["win_odds"]
    assert metrics["training"]["feature_transforms"] == ["log1p"]
    assert set(metrics["metrics"]) == {"train", "valid", "test"}


def test_train_logistic_regression_baseline_supports_market_plus_workout_features(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_market_workout.parquet"
    create_smoke_dataset(dataset_path, include_popularity=True, include_workout=True)

    config_path = tmp_path / "model_market_workout.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'market_plus_workout_minimal_logreg_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'popularity', 'workout_gap_days', "
            "'workout_weekday_code']\n"
            "feature_transforms = ['log1p', 'identity', 'identity', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_market_workout'}'\n"
            "max_iter = 200\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))

    metrics_path = summary.output_dir / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics["training"]["feature_columns"] == [
        "win_odds",
        "popularity",
        "workout_gap_days",
        "workout_weekday_code",
    ]
    assert metrics["training"]["feature_transforms"] == [
        "log1p",
        "identity",
        "identity",
        "identity",
    ]
    assert len(metrics["model"]["coefficients"]) == 4


def test_train_logistic_regression_baseline_supports_dual_market_features(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_dual_market.parquet"
    create_smoke_dataset(dataset_path, include_popularity=True, include_place_basis=True)

    config_path = tmp_path / "model_dual_market.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'dual_market_logreg_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_dual_market'}'\n"
            "max_iter = 200\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))

    metrics_path = summary.output_dir / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics["training"]["feature_columns"] == [
        "win_odds",
        "place_basis_odds",
        "popularity",
    ]
    assert metrics["training"]["feature_transforms"] == [
        "log1p",
        "log1p",
        "identity",
    ]
    assert len(metrics["model"]["coefficients"]) == 3


def test_train_logistic_regression_baseline_supports_dual_market_derived_features(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_dual_market_derived.parquet"
    create_smoke_dataset(
        dataset_path,
        include_popularity=True,
        include_place_basis=True,
        include_dual_derived=True,
    )

    config_path = tmp_path / "model_dual_market_derived.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'dual_market_plus_ratio_logreg_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = [\n"
            "  'win_odds',\n"
            "  'place_basis_odds',\n"
            "  'popularity',\n"
            "  'place_to_win_ratio',\n"
            "]\n"
            "feature_transforms = ['log1p', 'log1p', 'identity', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_dual_market_derived'}'\n"
            "max_iter = 200\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))

    metrics_path = summary.output_dir / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics["training"]["feature_columns"] == [
        "win_odds",
        "place_basis_odds",
        "popularity",
        "place_to_win_ratio",
    ]
    assert len(metrics["model"]["coefficients"]) == 4


def test_train_logistic_regression_baseline_rejects_null_model_feature_values(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_with_null_win_odds.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT * FROM (
                    VALUES
                        ('r1', 1, DATE '2025-01-05', 'train', 'is_place', 1.2, 1),
                        ('r2', 1, DATE '2025-01-12', 'train', 'is_place', NULL, 0),
                        ('r3', 1, DATE '2025-01-19', 'valid', 'is_place', 1.5, 1),
                        ('r4', 1, DATE '2025-01-26', 'valid', 'is_place', 10.0, 0),
                        ('r5', 1, DATE '2025-02-02', 'test', 'is_place', 2.0, 1),
                        ('r6', 1, DATE '2025-02-09', 'test', 'is_place', 12.0, 0)
                ) AS t(
                    race_key,
                    horse_number,
                    race_date,
                    split,
                    target_name,
                    win_odds,
                    target_value
                )
            ) TO ? (FORMAT PARQUET)
            """,
            [str(dataset_path)],
        )
    finally:
        connection.close()

    config_path = tmp_path / "model_with_null.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'odds_only_null_rejection'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds']\n"
            "feature_transforms = ['identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_null'}'\n"
            "max_iter = 200\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="training dataset split 'train'.*win_odds"):
        train_logistic_regression_baseline(load_model_train_config(config_path))


def test_train_hist_gradient_boosting_small_supports_dual_market_features(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_dual_market_tree.parquet"
    create_smoke_dataset(dataset_path, include_popularity=True, include_place_basis=True)

    config_path = tmp_path / "model_dual_market_tree.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'dual_market_histgb_small_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'hist_gradient_boosting_small'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_dual_market_tree'}'\n"
            "max_iter = 50\n"
            "[training.model_params]\n"
            "random_state = 7\n"
            "min_samples_leaf = 30\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))

    metrics_path = summary.output_dir / "metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))

    assert metrics["training"]["model_name"] == "hist_gradient_boosting_small"
    assert metrics["model"]["max_iter"] == 50
    assert metrics["training"]["model_params"] == {"min_samples_leaf": 30, "random_state": 7}
    assert metrics["model"]["min_samples_leaf"] == 30


def test_train_hist_gradient_boosting_small_ensemble_supports_dual_market_features(
    tmp_path: Path,
) -> None:
    dataset_path = tmp_path / "dataset_dual_market_ensemble.parquet"
    create_smoke_dataset(dataset_path, include_popularity=True, include_place_basis=True)

    config_path = tmp_path / "model_dual_market_ensemble.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'dual_market_histgb_ensemble_is_place'\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'hist_gradient_boosting_small_ensemble'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            f"output_dir = '{tmp_path / 'artifacts_dual_market_ensemble'}'\n"
            "max_iter = 50\n"
            "[training.model_params]\n"
            "seeds = [42, 7, 99]\n"
        ),
        encoding="utf-8",
    )

    summary = train_logistic_regression_baseline(load_model_train_config(config_path))
    metrics = json.loads((summary.output_dir / "metrics.json").read_text(encoding="utf-8"))

    assert metrics["training"]["model_name"] == "hist_gradient_boosting_small_ensemble"
    assert metrics["training"]["model_params"] == {"seeds": [42, 7, 99]}
    assert metrics["model"]["ensemble_size"] == 3


def create_smoke_dataset(
    path: Path,
    *,
    include_popularity: bool = False,
    include_place_basis: bool = False,
    include_dual_derived: bool = False,
    include_workout: bool = False,
) -> None:
    connection = duckdb.connect()
    try:
        if include_workout:
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            ('r1', 1, DATE '2025-01-05', 'train', 'is_place', 1.2, 1, 3, 1, 1),
                            ('r2', 1, DATE '2025-01-12', 'train', 'is_place', 8.0, 8, 5, 2, 0),
                            ('r3', 1, DATE '2025-01-19', 'valid', 'is_place', 1.5, 2, 4, 2, 1),
                            ('r4', 1, DATE '2025-01-26', 'valid', 'is_place', 10.0, 10, 6, 3, 0),
                            ('r5', 1, DATE '2025-02-02', 'test', 'is_place', 2.0, 3, 2, 4, 1),
                            ('r6', 1, DATE '2025-02-09', 'test', 'is_place', 12.0, 11, 7, 5, 0)
                    ) AS t(
                        race_key,
                        horse_number,
                        race_date,
                        split,
                        target_name,
                        win_odds,
                        popularity,
                        workout_gap_days,
                        workout_weekday_code,
                        target_value
                    )
                ) TO ? (FORMAT PARQUET)
                """,
                [str(path)],
            )
        elif include_dual_derived:
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            (
                                'r1', 1, DATE '2025-01-05', 'train', 'is_place',
                                1.2, 1.4, 1, 0.154151, 0.714286, 0.833333, -0.119048, 1.166667, 1
                            ),
                            (
                                'r2', 1, DATE '2025-01-12', 'train', 'is_place',
                                8.0, 2.8, 8, -1.049822, 0.357143, 0.125000, 0.232143, 0.350000, 0
                            ),
                            (
                                'r3', 1, DATE '2025-01-19', 'valid', 'is_place',
                                1.5, 1.6, 2, 0.064539, 0.625000, 0.666667, -0.041667, 1.066667, 1
                            ),
                            (
                                'r4', 1, DATE '2025-01-26', 'valid', 'is_place',
                                10.0, 3.2, 10, -1.139434, 0.312500, 0.100000, 0.212500, 0.320000, 0
                            ),
                            (
                                'r5', 1, DATE '2025-02-02', 'test', 'is_place',
                                2.0, 1.8, 3, -0.105361, 0.555556, 0.500000, 0.055556, 0.900000, 1
                            ),
                            (
                                'r6', 1, DATE '2025-02-09', 'test', 'is_place',
                                12.0, 3.5, 11, -1.232144, 0.285714, 0.083333, 0.202381, 0.291667, 0
                            )
                    ) AS t(
                        race_key,
                        horse_number,
                        race_date,
                        split,
                        target_name,
                        win_odds,
                        place_basis_odds,
                        popularity,
                        log_place_minus_log_win,
                        implied_place_prob,
                        implied_win_prob,
                        implied_place_prob_minus_implied_win_prob,
                        place_to_win_ratio,
                        target_value
                    )
                ) TO ? (FORMAT PARQUET)
                """,
                [str(path)],
            )
        elif include_place_basis:
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            ('r1', 1, DATE '2025-01-05', 'train', 'is_place', 1.2, 1.4, 1, 1),
                            ('r2', 1, DATE '2025-01-12', 'train', 'is_place', 8.0, 2.8, 8, 0),
                            ('r3', 1, DATE '2025-01-19', 'valid', 'is_place', 1.5, 1.6, 2, 1),
                            ('r4', 1, DATE '2025-01-26', 'valid', 'is_place', 10.0, 3.2, 10, 0),
                            ('r5', 1, DATE '2025-02-02', 'test', 'is_place', 2.0, 1.8, 3, 1),
                            ('r6', 1, DATE '2025-02-09', 'test', 'is_place', 12.0, 3.5, 11, 0)
                    ) AS t(
                        race_key,
                        horse_number,
                        race_date,
                        split,
                        target_name,
                        win_odds,
                        place_basis_odds,
                        popularity,
                        target_value
                    )
                ) TO ? (FORMAT PARQUET)
                """,
                [str(path)],
            )
        elif include_popularity:
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            ('r1', 1, DATE '2025-01-05', 'train', 'is_place', 1.2, 1, 1),
                            ('r2', 1, DATE '2025-01-12', 'train', 'is_place', 8.0, 8, 0),
                            ('r3', 1, DATE '2025-01-19', 'valid', 'is_place', 1.5, 2, 1),
                            ('r4', 1, DATE '2025-01-26', 'valid', 'is_place', 10.0, 10, 0),
                            ('r5', 1, DATE '2025-02-02', 'test', 'is_place', 2.0, 3, 1),
                            ('r6', 1, DATE '2025-02-09', 'test', 'is_place', 12.0, 11, 0)
                    ) AS t(
                        race_key,
                        horse_number,
                        race_date,
                        split,
                        target_name,
                        win_odds,
                        popularity,
                        target_value
                    )
                ) TO ? (FORMAT PARQUET)
                """,
                [str(path)],
            )
        else:
            connection.execute(
                """
                COPY (
                    SELECT * FROM (
                        VALUES
                            ('r1', 1, DATE '2025-01-05', 'train', 'is_place', 1.2, 1),
                            ('r2', 1, DATE '2025-01-12', 'train', 'is_place', 8.0, 0),
                            ('r3', 1, DATE '2025-01-19', 'valid', 'is_place', 1.5, 1),
                            ('r4', 1, DATE '2025-01-26', 'valid', 'is_place', 10.0, 0),
                            ('r5', 1, DATE '2025-02-02', 'test', 'is_place', 2.0, 1),
                            ('r6', 1, DATE '2025-02-09', 'test', 'is_place', 12.0, 0)
                    ) AS t(
                        race_key,
                        horse_number,
                        race_date,
                        split,
                        target_name,
                        win_odds,
                        target_value
                    )
                ) TO ? (FORMAT PARQUET)
                """,
                [str(path)],
            )
    finally:
        connection.close()
