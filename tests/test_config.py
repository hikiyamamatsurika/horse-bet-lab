from pathlib import Path

import pytest

from horse_bet_lab.config import (
    load_dataset_build_config,
    load_experiment_config,
    load_market_feature_comparison_config,
    load_model_train_config,
    load_place_backtest_config,
)


def test_load_experiment_config() -> None:
    config = load_experiment_config(Path("configs/default.toml"))

    assert config.name == "baseline_dummy"
    assert config.model == "dummy"
    assert config.feature_set == "minimal"
    assert config.strategy == "flat"
    assert config.period == "2020-01-01_to_2020-12-31"


def test_load_dataset_build_config_accepts_known_feature_set(tmp_path: Path) -> None:
    config_path = tmp_path / "dataset.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'dataset_dual_market'\n"
            "start_date = '2025-01-01'\n"
            "end_date = '2025-12-31'\n"
            "feature_set = 'dual_market'\n"
            "target_name = 'is_place'\n"
            "duckdb_path = 'data/artifacts/jrdb.duckdb'\n"
            "output_path = 'data/processed/dataset.parquet'\n"
        ),
        encoding="utf-8",
    )

    config = load_dataset_build_config(config_path)

    assert config.feature_set == "dual_market"


def test_load_dataset_build_config_rejects_unknown_feature_set(tmp_path: Path) -> None:
    config_path = tmp_path / "dataset_invalid.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'dataset_invalid'\n"
            "start_date = '2025-01-01'\n"
            "end_date = '2025-12-31'\n"
            "feature_set = 'unknown_feature_set'\n"
            "target_name = 'is_place'\n"
            "duckdb_path = 'data/artifacts/jrdb.duckdb'\n"
            "output_path = 'data/processed/dataset.parquet'\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported dataset_feature_set"):
        load_dataset_build_config(config_path)


def test_load_model_train_config_rejects_unknown_feature(tmp_path: Path) -> None:
    config_path = tmp_path / "model_unknown.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'invalid_unknown_feature'\n"
            "dataset_path = 'data/processed/dataset.parquet'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'mystery_feature']\n"
            "feature_transforms = ['identity', 'identity']\n"
            "output_dir = 'data/artifacts/invalid'\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown features"):
        load_model_train_config(config_path)


def test_load_model_train_config_rejects_dataset_only_feature_in_model_path(tmp_path: Path) -> None:
    config_path = tmp_path / "model_dataset_only.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'invalid_dataset_only_feature'\n"
            "dataset_path = 'data/processed/dataset.parquet'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'race_name']\n"
            "feature_transforms = ['identity', 'identity']\n"
            "output_dir = 'data/artifacts/invalid'\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="features not allowed in model parity path"):
        load_model_train_config(config_path)


def test_load_model_train_config_rejects_forbidden_leakage_feature(tmp_path: Path) -> None:
    config_path = tmp_path / "model_forbidden.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'invalid_forbidden_feature'\n"
            "dataset_path = 'data/processed/dataset.parquet'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'finish_position']\n"
            "feature_transforms = ['identity', 'identity']\n"
            "output_dir = 'data/artifacts/invalid'\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="forbidden post-race/result-side features"):
        load_model_train_config(config_path)


def test_load_model_train_config_accepts_valid_ordered_parity_sequence(tmp_path: Path) -> None:
    config_path = tmp_path / "model_valid.toml"
    config_path.write_text(
        (
            "[training]\n"
            "name = 'valid_dual_market'\n"
            "dataset_path = 'data/processed/dataset.parquet'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "output_dir = 'data/artifacts/valid'\n"
        ),
        encoding="utf-8",
    )

    config = load_model_train_config(config_path)

    assert config.feature_columns == ("win_odds", "place_basis_odds", "popularity")


def test_load_market_feature_comparison_config_rejects_dataset_only_mismatch(tmp_path: Path) -> None:
    config_path = tmp_path / "comparison_invalid.toml"
    config_path.write_text(
        (
            "[comparison]\n"
            "name = 'invalid_comparison'\n"
            "duckdb_path = 'data/artifacts/jrdb.duckdb'\n"
            "output_dir = 'data/artifacts/out'\n"
            "start_date = '2025-01-01'\n"
            "end_date = '2025-12-31'\n"
            "target_name = 'is_place'\n"
            "backtest_template_config_path = 'configs/place_backtest_odds_log1p_plus_popularity.toml'\n"
            "aggregate_selection_score_rule = 'mean_valid_roi'\n"
            "\n"
            "[[comparison.feature_sets]]\n"
            "name = 'invalid_minimal'\n"
            "dataset_feature_set = 'minimal'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['distance_m']\n"
            "feature_transforms = ['identity']\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="dataset-only and has no model parity path"):
        load_market_feature_comparison_config(config_path)


def test_load_market_feature_comparison_config_rejects_feature_set_mismatch(tmp_path: Path) -> None:
    config_path = tmp_path / "comparison_mismatch.toml"
    config_path.write_text(
        (
            "[comparison]\n"
            "name = 'comparison_mismatch'\n"
            "duckdb_path = 'data/artifacts/jrdb.duckdb'\n"
            "output_dir = 'data/artifacts/out'\n"
            "start_date = '2025-01-01'\n"
            "end_date = '2025-12-31'\n"
            "target_name = 'is_place'\n"
            "backtest_template_config_path = 'configs/place_backtest_odds_log1p_plus_popularity.toml'\n"
            "aggregate_selection_score_rule = 'mean_valid_roi'\n"
            "\n"
            "[[comparison.feature_sets]]\n"
            "name = 'dual_market_bad_order'\n"
            "dataset_feature_set = 'dual_market'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'identity']\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must match dataset_feature_set 'dual_market'"):
        load_market_feature_comparison_config(config_path)


def test_load_place_backtest_config_rejects_dataset_only_rolling_feature(tmp_path: Path) -> None:
    config_path = tmp_path / "backtest_invalid.toml"
    config_path.write_text(
        (
            "[backtest]\n"
            "name = 'backtest_invalid'\n"
            "predictions_path = 'data/artifacts/predictions.csv'\n"
            "duckdb_path = 'data/artifacts/jrdb.duckdb'\n"
            "output_dir = 'data/artifacts/out'\n"
            "threshold = 0.5\n"
            "\n"
            "[rolling_retrain]\n"
            "dataset_path = 'data/processed/dataset.parquet'\n"
            "feature_columns = ['race_name']\n"
            "feature_transforms = ['identity']\n"
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="features not allowed in model parity path"):
        load_place_backtest_config(config_path)
