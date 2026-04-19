from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from horse_bet_lab.config import (
    DatasetBuildConfig,
    MarketFeatureComparisonConfig,
    MarketFeatureComparisonFeatureSetConfig,
    PlaceBacktestConfig,
    load_place_backtest_config,
)
from horse_bet_lab.dataset.service import build_horse_dataset
from horse_bet_lab.evaluation.place_backtest import (
    PlaceBacktestSelectionRollupSummary,
    run_place_backtest,
)
from horse_bet_lab.model.service import SplitMetrics, load_prediction_metrics


@dataclass(frozen=True)
class MarketFeatureComparisonRow:
    feature_set_name: str
    dataset_feature_set: str
    model_name: str
    model_params: str
    feature_columns: str
    feature_transforms: str
    aggregate_selection_score_rule: str
    auc: float
    logloss: float
    brier_score: float
    positive_rate: float
    row_count: int
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    max_drawdown: float
    max_losing_streak: int
    roi_gt_1_ratio: float
    roi_ci_lower: float
    roi_ci_upper: float
    test_window_count: int
    test_window_labels: str
    dataset_path: str
    backtest_output_dir: str


@dataclass(frozen=True)
class MarketFeatureComparisonSummary:
    output_dir: Path
    rows: tuple[MarketFeatureComparisonRow, ...]


def run_market_feature_comparison(
    config: MarketFeatureComparisonConfig,
) -> MarketFeatureComparisonSummary:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    template = load_place_backtest_config(config.backtest_template_config_path)
    rows: list[MarketFeatureComparisonRow] = []

    for feature_set in config.feature_sets:
        dataset_summary = build_horse_dataset(
            build_dataset_config(config, feature_set),
        )
        backtest_config = build_backtest_config(
            template=template,
            comparison_config=config,
            feature_set=feature_set,
            dataset_path=dataset_summary.output_path,
        )
        backtest_result = run_place_backtest(backtest_config)
        test_metrics = load_prediction_metrics(
            backtest_config.output_dir / "rolling_predictions.csv",
            split_name="test",
        )
        rollup = select_test_rollup(
            backtest_result.selected_test_rollups,
            aggregate_selection_score_rule=config.aggregate_selection_score_rule,
        )
        rows.append(
            build_summary_row(
                feature_set=feature_set,
                metrics=test_metrics,
                rollup=rollup,
                dataset_path=dataset_summary.output_path,
                backtest_output_dir=backtest_config.output_dir,
            ),
        )

    summary = MarketFeatureComparisonSummary(output_dir=config.output_dir, rows=tuple(rows))
    write_market_feature_comparison_csv(config.output_dir / "summary.csv", summary.rows)
    write_market_feature_comparison_json(config.output_dir / "summary.json", summary.rows)
    return summary


def build_dataset_config(
    config: MarketFeatureComparisonConfig,
    feature_set: MarketFeatureComparisonFeatureSetConfig,
) -> DatasetBuildConfig:
    output_path = config.output_dir / feature_set.name / "dataset.parquet"
    return DatasetBuildConfig(
        name=f"{config.name}_{feature_set.name}",
        start_date=config.start_date,
        end_date=config.end_date,
        train_end_date=None,
        valid_end_date=None,
        feature_set=feature_set.dataset_feature_set,
        include_popularity=feature_set.include_popularity,
        target_name=config.target_name,
        duckdb_path=config.duckdb_path,
        output_path=output_path,
    )


def build_backtest_config(
    *,
    template: PlaceBacktestConfig,
    comparison_config: MarketFeatureComparisonConfig,
    feature_set: MarketFeatureComparisonFeatureSetConfig,
    dataset_path: Path,
) -> PlaceBacktestConfig:
    return replace(
        template,
        name=f"{comparison_config.name}_{feature_set.name}",
        duckdb_path=comparison_config.duckdb_path,
        output_dir=comparison_config.output_dir / feature_set.name / "backtest",
        aggregate_selection_score_rule=comparison_config.aggregate_selection_score_rule,
        aggregate_selection_score_rules=(comparison_config.aggregate_selection_score_rule,),
        rolling_retrain_dataset_path=dataset_path,
        rolling_model_name=feature_set.model_name,
        rolling_model_params=feature_set.model_params,
        rolling_feature_columns=feature_set.feature_columns,
        rolling_feature_transforms=feature_set.feature_transforms,
        rolling_max_iter=feature_set.max_iter,
    )


def select_test_rollup(
    rollups: tuple[PlaceBacktestSelectionRollupSummary, ...],
    *,
    aggregate_selection_score_rule: str,
) -> PlaceBacktestSelectionRollupSummary:
    for rollup in rollups:
        if rollup.aggregate_selection_score_rule == aggregate_selection_score_rule:
            return rollup
    return PlaceBacktestSelectionRollupSummary(
        selection_score_rule="aggregate_valid_windows",
        aggregate_selection_score_rule=aggregate_selection_score_rule,
        selection_mode="aggregate_valid_windows",
        min_bets_valid=None,
        test_window_count=0,
        test_window_labels="",
        bet_count=0,
        hit_count=0,
        hit_rate=0.0,
        roi=0.0,
        total_profit=0.0,
        avg_payout=0.0,
        avg_edge=0.0,
        max_drawdown=0.0,
        max_losing_streak=0,
        roi_gt_1_ratio=0.0,
        roi_ci_lower=0.0,
        roi_ci_upper=0.0,
    )


def build_summary_row(
    *,
    feature_set: MarketFeatureComparisonFeatureSetConfig,
    metrics: SplitMetrics,
    rollup: PlaceBacktestSelectionRollupSummary,
    dataset_path: Path,
    backtest_output_dir: Path,
) -> MarketFeatureComparisonRow:
    return MarketFeatureComparisonRow(
        feature_set_name=feature_set.name,
        dataset_feature_set=feature_set.dataset_feature_set,
        model_name=feature_set.model_name,
        model_params=json.dumps(feature_set.model_params, sort_keys=True),
        feature_columns=",".join(feature_set.feature_columns),
        feature_transforms=",".join(feature_set.feature_transforms),
        aggregate_selection_score_rule=(
            rollup.aggregate_selection_score_rule
            if rollup.aggregate_selection_score_rule is not None
            else ""
        ),
        auc=metrics.auc,
        logloss=metrics.logloss,
        brier_score=metrics.brier_score,
        positive_rate=metrics.positive_rate,
        row_count=metrics.row_count,
        bet_count=rollup.bet_count,
        hit_count=rollup.hit_count,
        hit_rate=rollup.hit_rate,
        roi=rollup.roi,
        total_profit=rollup.total_profit,
        avg_payout=rollup.avg_payout,
        avg_edge=rollup.avg_edge,
        max_drawdown=rollup.max_drawdown,
        max_losing_streak=rollup.max_losing_streak,
        roi_gt_1_ratio=rollup.roi_gt_1_ratio,
        roi_ci_lower=rollup.roi_ci_lower,
        roi_ci_upper=rollup.roi_ci_upper,
        test_window_count=rollup.test_window_count,
        test_window_labels=rollup.test_window_labels,
        dataset_path=str(dataset_path),
        backtest_output_dir=str(backtest_output_dir),
    )


def write_market_feature_comparison_csv(
    path: Path,
    rows: tuple[MarketFeatureComparisonRow, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=tuple(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_market_feature_comparison_json(
    path: Path,
    rows: tuple[MarketFeatureComparisonRow, ...],
) -> None:
    payload = {
        "comparison": {
            "rows": [asdict(row) for row in rows],
        },
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
