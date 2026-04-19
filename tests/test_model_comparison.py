from __future__ import annotations

import json
from pathlib import Path

import pytest

from horse_bet_lab.config import load_market_feature_comparison_config
from horse_bet_lab.dataset.service import DatasetBuildSummary
from horse_bet_lab.evaluation.place_backtest import (
    PlaceBacktestResult,
    PlaceBacktestSelectionRollupSummary,
)
from horse_bet_lab.model.comparison import run_market_feature_comparison
from horse_bet_lab.model.service import SplitMetrics


def test_run_market_feature_comparison_writes_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    template_config_path = tmp_path / "template.toml"
    template_config_path.write_text(
        (
            "[backtest]\n"
            "name = 'template'\n"
            "predictions_path = 'predictions.csv'\n"
            f"duckdb_path = '{tmp_path / 'jrdb.duckdb'}'\n"
            f"output_dir = '{tmp_path / 'template_artifacts'}'\n"
            "selection_metric = 'edge'\n"
            "market_prob_method = 'oz_place_basis_inverse'\n"
            "thresholds = [0.02]\n"
            "aggregate_selection_score_rule = 'positive_window_count_then_mean_roi_then_min_roi'\n"
            "split_column = 'split'\n"
            "target_column = 'target_value'\n"
            "probability_column = 'pred_probability'\n"
            "stake_per_bet = 100\n"
            "[[backtest.popularity_bands]]\n"
            "min = 1\n"
            "max = 3\n"
            "[[backtest.place_basis_bands]]\n"
            "min = 2.0\n"
            "max = 3.0\n"
            "[[backtest.evaluation_window_pairs]]\n"
            "label = '2025_01_to_02'\n"
            "valid_start_date = '2025-01-01'\n"
            "valid_end_date = '2025-01-31'\n"
            "test_start_date = '2025-02-01'\n"
            "test_end_date = '2025-02-28'\n"
            "[[backtest.selection_window_groups]]\n"
            "label = 'test_2025_02'\n"
            "valid_window_labels = ['2025_01_to_02']\n"
            "test_window_label = '2025_01_to_02'\n"
            "[rolling_retrain]\n"
            f"dataset_path = '{tmp_path / 'placeholder.parquet'}'\n"
            "feature_columns = ['win_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'identity']\n"
            "race_date_column = 'race_date'\n"
            "max_iter = 1000\n"
        ),
        encoding="utf-8",
    )

    comparison_config_path = tmp_path / "comparison.toml"
    comparison_config_path.write_text(
        (
            "[comparison]\n"
            "name = 'market_feature_comparison_smoke'\n"
            f"duckdb_path = '{tmp_path / 'jrdb.duckdb'}'\n"
            f"output_dir = '{tmp_path / 'comparison_artifacts'}'\n"
            "start_date = '2024-01-01'\n"
            "end_date = '2025-12-31'\n"
            "target_name = 'is_place'\n"
            f"backtest_template_config_path = '{template_config_path}'\n"
            "aggregate_selection_score_rule = 'positive_window_count_then_mean_roi_then_min_roi'\n"
            "[[comparison.feature_sets]]\n"
            "name = 'current_win_market'\n"
            "dataset_feature_set = 'current_win_market'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'identity']\n"
            "[[comparison.feature_sets]]\n"
            "name = 'place_market_plus_popularity'\n"
            "dataset_feature_set = 'place_market_plus_popularity'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'identity']\n"
            "[[comparison.feature_sets]]\n"
            "name = 'dual_market_tree'\n"
            "dataset_feature_set = 'dual_market'\n"
            "model_name = 'hist_gradient_boosting_small'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "[comparison.feature_sets.model_params]\n"
            "random_state = 7\n"
            "min_samples_leaf = 30\n"
            "[[comparison.feature_sets]]\n"
            "name = 'dual_market_ensemble'\n"
            "dataset_feature_set = 'dual_market'\n"
            "model_name = 'hist_gradient_boosting_small_ensemble'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "[comparison.feature_sets.model_params]\n"
            "seeds = [42, 7, 99]\n"
            "[[comparison.feature_sets]]\n"
            "name = 'dual_market_headcount'\n"
            "dataset_feature_set = 'dual_market_plus_headcount_place_slots_distance'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = [\n"
            "  'win_odds', 'place_basis_odds', 'popularity',\n"
            "  'headcount', 'place_slot_count', 'distance_m'\n"
            "]\n"
            "feature_transforms = [\n"
            "  'log1p', 'log1p', 'identity',\n"
            "  'identity', 'identity', 'identity'\n"
            "]\n"
        ),
        encoding="utf-8",
    )

    def fake_build_horse_dataset(config):  # type: ignore[no-untyped-def]
        config.output_path.parent.mkdir(parents=True, exist_ok=True)
        config.output_path.write_text("dataset", encoding="utf-8")
        return DatasetBuildSummary(output_path=config.output_path, row_count=12)

    def fake_run_place_backtest(config):  # type: ignore[no-untyped-def]
        config.output_dir.mkdir(parents=True, exist_ok=True)
        (config.output_dir / "rolling_predictions.csv").write_text(
            "race_key,horse_number,split,target_value,pred_probability,window_label\n"
            "r1,1,test,1,0.7,2025_01_to_02\n",
            encoding="utf-8",
        )
        feature_name = config.output_dir.parent.name
        if feature_name == "dual_market_ensemble":
            roi = 1.18
        elif feature_name == "dual_market_headcount":
            roi = 1.21
        elif feature_name == "dual_market_tree":
            roi = 1.15
        elif feature_name == "place_market_plus_popularity":
            roi = 1.12
        else:
            roi = 0.98
        return PlaceBacktestResult(
            output_dir=config.output_dir,
            summaries=(),
            selected_summaries=(),
            selected_test_rollups=(
                PlaceBacktestSelectionRollupSummary(
                    selection_score_rule="aggregate_valid_windows",
                    aggregate_selection_score_rule=(
                        "positive_window_count_then_mean_roi_then_min_roi"
                    ),
                    selection_mode="aggregate_valid_windows",
                    min_bets_valid=10,
                    test_window_count=1,
                    test_window_labels="test_2025_02",
                    bet_count=10,
                    hit_count=6,
                    hit_rate=0.6,
                    roi=roi,
                    total_profit=120.0 if roi > 1.0 else -20.0,
                    avg_payout=186.0,
                    avg_edge=0.08,
                    max_drawdown=50.0,
                    max_losing_streak=2,
                    roi_gt_1_ratio=0.5,
                    roi_ci_lower=0.91,
                    roi_ci_upper=1.24,
                ),
            ),
            candidate_summaries=(),
            uncertainty_summaries=(),
            yearly_summaries=(),
            monthly_summaries=(),
            odds_band_summaries=(),
            place_basis_bucket_summaries=(),
        )

    def fake_load_prediction_metrics(*args, **kwargs):  # type: ignore[no-untyped-def]
        return SplitMetrics(
            auc=0.81,
            logloss=0.42,
            brier_score=0.13,
            positive_rate=0.22,
            prediction_mean=0.22,
            prediction_std=0.1,
            prediction_min=0.01,
            prediction_p10=0.05,
            prediction_p50=0.2,
            prediction_p90=0.4,
            prediction_max=0.8,
            row_count=100,
        )

    monkeypatch.setattr(
        "horse_bet_lab.model.comparison.build_horse_dataset",
        fake_build_horse_dataset,
    )
    monkeypatch.setattr(
        "horse_bet_lab.model.comparison.run_place_backtest",
        fake_run_place_backtest,
    )
    monkeypatch.setattr(
        "horse_bet_lab.model.comparison.load_prediction_metrics",
        fake_load_prediction_metrics,
    )

    summary = run_market_feature_comparison(
        load_market_feature_comparison_config(comparison_config_path),
    )

    summary_path = summary.output_dir / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    rows = payload["comparison"]["rows"]

    assert len(rows) == 5
    assert {row["feature_set_name"] for row in rows} == {
        "current_win_market",
        "place_market_plus_popularity",
        "dual_market_tree",
        "dual_market_ensemble",
        "dual_market_headcount",
    }
    assert {row["model_name"] for row in rows} == {
        "logistic_regression",
        "hist_gradient_boosting_small",
        "hist_gradient_boosting_small_ensemble",
    }
    assert rows[2]["model_params"] == '{"min_samples_leaf": 30, "random_state": 7}'
    assert rows[3]["model_params"] == '{"seeds": [42, 7, 99]}'
    assert rows[3]["aggregate_selection_score_rule"] == (
        "positive_window_count_then_mean_roi_then_min_roi"
    )


def test_run_market_feature_comparison_supports_single_win_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    template_config_path = tmp_path / "template_win.toml"
    template_config_path.write_text(
        (
            "[backtest]\n"
            "name = 'template_single_win'\n"
            "predictions_path = 'predictions.csv'\n"
            f"duckdb_path = '{tmp_path / 'jrdb.duckdb'}'\n"
            f"output_dir = '{tmp_path / 'template_single_win_artifacts'}'\n"
            "payout_type = 'win'\n"
            "selection_metric = 'edge'\n"
            "market_prob_method = 'inverse_win_odds'\n"
            "thresholds = [0.00, 0.02]\n"
            "aggregate_selection_score_rule = 'mean_valid_roi'\n"
            "split_column = 'split'\n"
            "target_column = 'target_value'\n"
            "probability_column = 'pred_probability'\n"
            "stake_per_bet = 100\n"
            "[[backtest.evaluation_window_pairs]]\n"
            "label = '2025_01_to_02'\n"
            "valid_start_date = '2025-01-01'\n"
            "valid_end_date = '2025-01-31'\n"
            "test_start_date = '2025-02-01'\n"
            "test_end_date = '2025-02-28'\n"
            "[[backtest.selection_window_groups]]\n"
            "label = 'test_2025_02'\n"
            "valid_window_labels = ['2025_01_to_02']\n"
            "test_window_label = '2025_01_to_02'\n"
            "[rolling_retrain]\n"
            f"dataset_path = '{tmp_path / 'placeholder.parquet'}'\n"
            "feature_columns = ['win_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'identity']\n"
            "race_date_column = 'race_date'\n"
            "max_iter = 1000\n"
        ),
        encoding="utf-8",
    )

    comparison_config_path = tmp_path / "comparison_single_win.toml"
    comparison_config_path.write_text(
        (
            "[comparison]\n"
            "name = 'single_win_market_comparison_smoke'\n"
            f"duckdb_path = '{tmp_path / 'jrdb.duckdb'}'\n"
            f"output_dir = '{tmp_path / 'comparison_single_win_artifacts'}'\n"
            "start_date = '2024-01-01'\n"
            "end_date = '2025-12-31'\n"
            "target_name = 'is_win'\n"
            f"backtest_template_config_path = '{template_config_path}'\n"
            "aggregate_selection_score_rule = 'mean_valid_roi'\n"
            "[[comparison.feature_sets]]\n"
            "name = 'win_market_only'\n"
            "dataset_feature_set = 'win_market_only'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'identity']\n"
            "[[comparison.feature_sets]]\n"
            "name = 'dual_market_for_win'\n"
            "dataset_feature_set = 'dual_market_for_win'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
        ),
        encoding="utf-8",
    )

    built_targets: list[tuple[str, str]] = []

    def fake_build_horse_dataset(config):  # type: ignore[no-untyped-def]
        built_targets.append((config.feature_set, config.target_name))
        config.output_path.parent.mkdir(parents=True, exist_ok=True)
        config.output_path.write_text("dataset", encoding="utf-8")
        return DatasetBuildSummary(output_path=config.output_path, row_count=24)

    def fake_run_place_backtest(config):  # type: ignore[no-untyped-def]
        config.output_dir.mkdir(parents=True, exist_ok=True)
        (config.output_dir / "rolling_predictions.csv").write_text(
            "race_key,horse_number,split,target_value,pred_probability,window_label\n"
            "r1,1,test,1,0.6,2025_01_to_02\n",
            encoding="utf-8",
        )
        return PlaceBacktestResult(
            output_dir=config.output_dir,
            summaries=(),
            selected_summaries=(),
            selected_test_rollups=(
                PlaceBacktestSelectionRollupSummary(
                    selection_score_rule="aggregate_valid_windows",
                    aggregate_selection_score_rule="mean_valid_roi",
                    selection_mode="aggregate_valid_windows",
                    min_bets_valid=None,
                    test_window_count=1,
                    test_window_labels="test_2025_02",
                    bet_count=4,
                    hit_count=1,
                    hit_rate=0.25,
                    roi=1.1,
                    total_profit=40.0,
                    avg_payout=440.0,
                    avg_edge=0.03,
                    max_drawdown=20.0,
                    max_losing_streak=1,
                    roi_gt_1_ratio=1.0,
                    roi_ci_lower=1.02,
                    roi_ci_upper=1.18,
                ),
            ),
            candidate_summaries=(),
            uncertainty_summaries=(),
            yearly_summaries=(),
            monthly_summaries=(),
            odds_band_summaries=(),
            place_basis_bucket_summaries=(),
        )

    def fake_load_prediction_metrics(*args, **kwargs):  # type: ignore[no-untyped-def]
        return SplitMetrics(
            auc=0.67,
            logloss=0.51,
            brier_score=0.18,
            positive_rate=0.09,
            prediction_mean=0.10,
            prediction_std=0.04,
            prediction_min=0.02,
            prediction_p10=0.03,
            prediction_p50=0.09,
            prediction_p90=0.16,
            prediction_max=0.22,
            row_count=80,
        )

    monkeypatch.setattr(
        "horse_bet_lab.model.comparison.build_horse_dataset",
        fake_build_horse_dataset,
    )
    monkeypatch.setattr(
        "horse_bet_lab.model.comparison.run_place_backtest",
        fake_run_place_backtest,
    )
    monkeypatch.setattr(
        "horse_bet_lab.model.comparison.load_prediction_metrics",
        fake_load_prediction_metrics,
    )

    summary = run_market_feature_comparison(
        load_market_feature_comparison_config(comparison_config_path),
    )

    rows = json.loads((summary.output_dir / "summary.json").read_text(encoding="utf-8"))[
        "comparison"
    ]["rows"]
    assert built_targets == [
        ("win_market_only", "is_win"),
        ("dual_market_for_win", "is_win"),
    ]
    assert [row["feature_set_name"] for row in rows] == [
        "win_market_only",
        "dual_market_for_win",
    ]
    assert all(row["bet_count"] == 4 for row in rows)
    assert all(row["roi_ci_lower"] == 1.02 for row in rows)
