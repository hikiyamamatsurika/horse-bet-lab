from __future__ import annotations

import csv
import json
import math
import random
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from horse_bet_lab.config import PlaceBacktestConfig
from horse_bet_lab.features.provenance import build_feature_provenance_payload
from horse_bet_lab.model.service import RollingPairWindow, generate_rolling_pair_predictions

SPLIT_ORDER = ("train", "valid", "test")
PLACE_BACKTEST_COLUMNS = (
    "window_label",
    "window_start_date",
    "window_end_date",
    "split",
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
)
PLACE_BACKTEST_MONTHLY_COLUMNS = (
    "window_label",
    "window_start_date",
    "window_end_date",
    "month",
    "split",
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
)
PLACE_BACKTEST_ODDS_BAND_COLUMNS = (
    "window_label",
    "window_start_date",
    "window_end_date",
    "month",
    "split",
    "selection_metric",
    "threshold",
    "min_win_odds",
    "max_win_odds",
    "min_place_basis_odds",
    "max_place_basis_odds",
    "min_popularity",
    "max_popularity",
    "odds_metric",
    "bucket_label",
    "adopted_count",
)
PLACE_BACKTEST_PLACE_BASIS_BUCKET_COLUMNS = (
    "window_label",
    "window_start_date",
    "window_end_date",
    "month",
    "split",
    "selection_metric",
    "threshold",
    "min_win_odds",
    "max_win_odds",
    "min_place_basis_odds",
    "max_place_basis_odds",
    "min_popularity",
    "max_popularity",
    "bucket_label",
    "bet_count",
    "hit_count",
    "hit_rate",
    "roi",
    "total_profit",
    "avg_payout",
    "avg_edge",
)
PLACE_BACKTEST_SELECTION_COLUMNS = (
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
)
PLACE_BACKTEST_SELECTION_ROLLUP_COLUMNS = (
    "selection_score_rule",
    "aggregate_selection_score_rule",
    "selection_mode",
    "min_bets_valid",
    "test_window_count",
    "test_window_labels",
    "bet_count",
    "hit_count",
    "hit_rate",
    "roi",
    "total_profit",
    "avg_payout",
    "avg_edge",
    "max_drawdown",
    "max_losing_streak",
    "roi_gt_1_ratio",
    "roi_ci_lower",
    "roi_ci_upper",
)
PLACE_BACKTEST_CANDIDATE_COLUMNS = (
    "window_label",
    "selection_score_rule",
    "aggregate_selection_score_rule",
    "min_bets_valid",
    "selection_mode",
    "valid_window_labels",
    "test_window_label",
    "selection_metric",
    "threshold",
    "min_win_odds",
    "max_win_odds",
    "min_place_basis_odds",
    "max_place_basis_odds",
    "min_popularity",
    "max_popularity",
    "valid_aggregate_score",
    "valid_bet_count",
    "valid_hit_count",
    "valid_hit_rate",
    "valid_roi",
    "valid_total_profit",
    "valid_avg_payout",
    "valid_avg_edge",
    "valid_positive_window_count",
    "valid_mean_roi",
    "valid_min_roi",
    "valid_roi_std",
    "valid_window_rois",
    "test_bet_count",
    "test_hit_count",
    "test_hit_rate",
    "test_roi",
    "test_total_profit",
    "test_avg_payout",
    "test_avg_edge",
)
PLACE_BACKTEST_UNCERTAINTY_COLUMNS = (
    "selection_score_rule",
    "aggregate_selection_score_rule",
    "selection_mode",
    "min_bets_valid",
    "test_window_count",
    "bet_count",
    "hit_count",
    "hit_rate",
    "roi",
    "total_profit",
    "max_drawdown",
    "max_losing_streak",
    "roi_gt_1_ratio",
    "roi_ci_lower",
    "roi_ci_upper",
)
PLACE_BACKTEST_YEARLY_COLUMNS = (
    "selection_score_rule",
    "aggregate_selection_score_rule",
    "selection_mode",
    "min_bets_valid",
    "year",
    "bet_count",
    "hit_count",
    "hit_rate",
    "roi",
    "total_profit",
    "avg_payout",
    "avg_edge",
)


@dataclass(frozen=True)
class PlaceBacktestSummary:
    window_label: str
    window_start_date: str | None
    window_end_date: str | None
    split: str
    selection_metric: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class PlaceBacktestSelectionSummary:
    window_label: str
    selection_score_rule: str
    min_bets_valid: int | None
    selected_on_split: str
    applied_to_split: str
    selection_metric: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    aggregate_selection_score_rule: str | None = None
    selection_mode: str = "single_valid_window"
    valid_window_labels: str | None = None
    test_window_label: str | None = None
    valid_aggregate_score: float | None = None
    valid_positive_window_count: int | None = None
    valid_mean_roi: float | None = None
    valid_min_roi: float | None = None
    valid_roi_std: float | None = None
    valid_window_rois: str | None = None


@dataclass(frozen=True)
class AggregateValidSummary:
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    mean_valid_roi: float
    mean_valid_roi_with_bets_weight: float
    positive_window_count: int
    min_valid_roi: float
    valid_roi_std: float
    valid_window_rois: tuple[float, ...]


@dataclass(frozen=True)
class PlaceBacktestResult:
    output_dir: Path
    summaries: tuple[PlaceBacktestSummary, ...]
    selected_summaries: tuple["PlaceBacktestSelectionSummary", ...]
    selected_test_rollups: tuple["PlaceBacktestSelectionRollupSummary", ...]
    candidate_summaries: tuple["PlaceBacktestCandidateSummary", ...]
    uncertainty_summaries: tuple["PlaceBacktestUncertaintySummary", ...]
    yearly_summaries: tuple["PlaceBacktestYearlySummary", ...]
    monthly_summaries: tuple["PlaceBacktestMonthlySummary", ...]
    odds_band_summaries: tuple["PlaceBacktestOddsBandSummary", ...]
    place_basis_bucket_summaries: tuple["PlaceBacktestPlaceBasisBucketSummary", ...]


@dataclass(frozen=True)
class PlaceBacktestSelectionRollupSummary:
    selection_score_rule: str
    aggregate_selection_score_rule: str | None
    selection_mode: str
    min_bets_valid: int | None
    test_window_count: int
    test_window_labels: str
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


@dataclass(frozen=True)
class PlaceBacktestCandidateSummary:
    window_label: str
    selection_score_rule: str
    aggregate_selection_score_rule: str | None
    min_bets_valid: int | None
    selection_mode: str
    valid_window_labels: str | None
    test_window_label: str | None
    selection_metric: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    valid_aggregate_score: float | None
    valid_bet_count: int
    valid_hit_count: int
    valid_hit_rate: float
    valid_roi: float
    valid_total_profit: float
    valid_avg_payout: float
    valid_avg_edge: float
    valid_positive_window_count: int | None
    valid_mean_roi: float | None
    valid_min_roi: float | None
    valid_roi_std: float | None
    valid_window_rois: str | None
    test_bet_count: int
    test_hit_count: int
    test_hit_rate: float
    test_roi: float
    test_total_profit: float
    test_avg_payout: float
    test_avg_edge: float


@dataclass(frozen=True)
class PlaceBacktestUncertaintySummary:
    selection_score_rule: str
    aggregate_selection_score_rule: str | None
    selection_mode: str
    min_bets_valid: int | None
    test_window_count: int
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    roi_gt_1_ratio: float
    roi_ci_lower: float
    roi_ci_upper: float


@dataclass(frozen=True)
class PlaceBacktestYearlySummary:
    selection_score_rule: str
    aggregate_selection_score_rule: str | None
    selection_mode: str
    min_bets_valid: int | None
    year: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class PlaceBacktestMonthlySummary:
    window_label: str
    window_start_date: str | None
    window_end_date: str | None
    month: str
    split: str
    selection_metric: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class PlaceBacktestOddsBandSummary:
    window_label: str
    window_start_date: str | None
    window_end_date: str | None
    month: str
    split: str
    selection_metric: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    odds_metric: str
    bucket_label: str
    adopted_count: int


@dataclass(frozen=True)
class PlaceBacktestPlaceBasisBucketSummary:
    window_label: str
    window_start_date: str | None
    window_end_date: str | None
    month: str
    split: str
    selection_metric: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    bucket_label: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class PlaceBacktestScope:
    window_label: str
    window_start_date: str | None
    window_end_date: str | None
    split_expression: str
    split_name: str | None


def run_place_backtest(config: PlaceBacktestConfig) -> PlaceBacktestResult:
    validate_place_backtest_config(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(config.duckdb_path), read_only=True)
    try:
        prediction_has_window_label = prepare_prediction_rows_temp_table(connection, config)
        summaries = tuple(
            build_place_backtest_summaries(connection, config, prediction_has_window_label),
        )
        selected_summaries = tuple(build_place_backtest_selection_summaries(config, summaries))
        candidate_summaries = tuple(build_place_backtest_candidate_summaries(config, summaries))
        selected_test_rollups = tuple(
            build_place_backtest_selection_rollups(
                selected_summaries,
                config.stake_per_bet,
                config.bootstrap_iterations,
                config.random_seed,
            ),
        )
        uncertainty_summaries = tuple(build_place_backtest_uncertainty_summaries(selected_test_rollups))
        monthly_summaries = tuple(
            build_place_backtest_monthly_summaries(connection, config, prediction_has_window_label),
        )
        yearly_summaries = tuple(
            build_place_backtest_yearly_summaries(
                selected_summaries=selected_summaries,
                monthly_summaries=monthly_summaries,
                stake_per_bet=config.stake_per_bet,
            ),
        )
        odds_band_summaries = tuple(
            build_place_backtest_odds_band_summaries(
                connection,
                config,
                prediction_has_window_label,
            ),
        )
        place_basis_bucket_summaries = tuple(
            build_place_backtest_place_basis_bucket_summaries(
                connection,
                config,
                prediction_has_window_label,
            )
        )
    finally:
        connection.close()

    write_place_backtest_csv(config.output_dir / "summary.csv", summaries)
    write_place_backtest_json(config.output_dir / "summary.json", config, summaries)
    write_place_backtest_selection_csv(
        config.output_dir / "selected_summary.csv",
        selected_summaries,
    )
    write_place_backtest_selection_json(
        config.output_dir / "selected_summary.json",
        config,
        selected_summaries,
    )
    write_place_backtest_selection_rollup_csv(
        config.output_dir / "selected_test_rollup.csv",
        selected_test_rollups,
    )
    write_place_backtest_selection_rollup_json(
        config.output_dir / "selected_test_rollup.json",
        config,
        selected_test_rollups,
    )
    write_place_backtest_candidate_csv(
        config.output_dir / "candidate_summary.csv",
        candidate_summaries,
    )
    write_place_backtest_candidate_json(
        config.output_dir / "candidate_summary.json",
        config,
        candidate_summaries,
    )
    write_place_backtest_uncertainty_csv(
        config.output_dir / "uncertainty_summary.csv",
        uncertainty_summaries,
    )
    write_place_backtest_uncertainty_json(
        config.output_dir / "uncertainty_summary.json",
        config,
        uncertainty_summaries,
    )
    write_place_backtest_yearly_csv(
        config.output_dir / "yearly_summary.csv",
        yearly_summaries,
    )
    write_place_backtest_yearly_json(
        config.output_dir / "yearly_summary.json",
        config,
        yearly_summaries,
    )
    write_place_backtest_monthly_csv(
        config.output_dir / "monthly_summary.csv",
        monthly_summaries,
    )
    write_place_backtest_monthly_json(
        config.output_dir / "monthly_summary.json",
        config,
        monthly_summaries,
    )
    write_place_backtest_odds_band_csv(
        config.output_dir / "monthly_odds_bands.csv",
        odds_band_summaries,
    )
    write_place_backtest_odds_band_json(
        config.output_dir / "monthly_odds_bands.json",
        config,
        odds_band_summaries,
    )
    write_place_backtest_place_basis_bucket_csv(
        config.output_dir / "monthly_place_basis_buckets.csv",
        place_basis_bucket_summaries,
    )
    write_place_backtest_place_basis_bucket_json(
        config.output_dir / "monthly_place_basis_buckets.json",
        config,
        place_basis_bucket_summaries,
    )
    write_place_backtest_provenance_manifest(
        config.output_dir / "feature_provenance.json",
        config,
    )
    return PlaceBacktestResult(
        output_dir=config.output_dir,
        summaries=summaries,
        selected_summaries=selected_summaries,
        selected_test_rollups=selected_test_rollups,
        candidate_summaries=candidate_summaries,
        uncertainty_summaries=uncertainty_summaries,
        yearly_summaries=yearly_summaries,
        monthly_summaries=monthly_summaries,
        odds_band_summaries=odds_band_summaries,
        place_basis_bucket_summaries=place_basis_bucket_summaries,
    )


def validate_place_backtest_config(config: PlaceBacktestConfig) -> None:
    if config.payout_type not in {"place", "win"}:
        raise ValueError("payout_type must be 'place' or 'win'")
    if config.selection_metric not in {"probability", "edge"}:
        raise ValueError("selection_metric must be 'probability' or 'edge'")
    if config.market_prob_method not in {
        "inverse_win_odds",
        "normalized_inverse_win_odds",
        "oz_place_basis_inverse",
    }:
        raise ValueError(
            "market_prob_method must be 'inverse_win_odds', "
            "'normalized_inverse_win_odds', or 'oz_place_basis_inverse'",
        )
    if not config.thresholds:
        raise ValueError("thresholds must not be empty")
    for threshold in config.thresholds:
        if config.selection_metric == "probability" and not (0.0 <= threshold <= 1.0):
            raise ValueError("each probability threshold must be between 0.0 and 1.0")
    if (
        config.min_win_odds is not None
        and config.max_win_odds is not None
        and config.min_win_odds > config.max_win_odds
    ):
        raise ValueError("min_win_odds must be less than or equal to max_win_odds")
    if not config.win_odds_bands:
        raise ValueError("win_odds_bands must not be empty")
    for min_win_odds, max_win_odds in config.win_odds_bands:
        if min_win_odds is not None and max_win_odds is not None and min_win_odds > max_win_odds:
            raise ValueError("win_odds band min must be less than or equal to max")
    if (
        config.min_place_basis_odds is not None
        and config.max_place_basis_odds is not None
        and config.min_place_basis_odds > config.max_place_basis_odds
    ):
        raise ValueError(
            "min_place_basis_odds must be less than or equal to max_place_basis_odds",
        )
    if not config.popularity_bands:
        raise ValueError("popularity_bands must not be empty")
    for min_popularity, max_popularity in config.popularity_bands:
        if (
            min_popularity is not None
            and max_popularity is not None
            and min_popularity > max_popularity
        ):
            raise ValueError("min_popularity must be less than or equal to max_popularity")
    if not config.place_basis_bands:
        raise ValueError("place_basis_bands must not be empty")
    for min_place_basis_odds, max_place_basis_odds in config.place_basis_bands:
        if (
            min_place_basis_odds is not None
            and max_place_basis_odds is not None
            and min_place_basis_odds > max_place_basis_odds
        ):
            raise ValueError(
                "place_basis band min must be less than or equal to max",
            )
    if config.stake_per_bet <= 0.0:
        raise ValueError("stake_per_bet must be positive")
    valid_selection_score_rules = {
        "roi_max",
        "roi_then_bets",
        "roi_weighted_by_bets",
    }
    valid_aggregate_selection_score_rules = {
        "roi_max",
        "roi_then_bets",
        "mean_valid_roi",
        "mean_valid_roi_with_bets_weight",
        "positive_window_count_then_mean_roi",
        "mean_valid_roi_minus_std",
        "positive_window_count_then_mean_roi_then_min_roi",
        "min_valid_roi_then_mean_roi",
    }
    if config.selection_score_rule not in valid_selection_score_rules:
        raise ValueError(
            "selection_score_rule must be one of roi_max, roi_then_bets, roi_weighted_by_bets",
        )
    if not config.selection_score_rules:
        raise ValueError("selection_score_rules must not be empty")
    for selection_score_rule in config.selection_score_rules:
        if selection_score_rule not in valid_selection_score_rules:
            raise ValueError(
                "each selection_score_rules entry must be one of "
                "roi_max, roi_then_bets, roi_weighted_by_bets",
            )
    if config.aggregate_selection_score_rule not in valid_aggregate_selection_score_rules:
        raise ValueError(
            "aggregate_selection_score_rule must be one of "
            "roi_max, roi_then_bets, mean_valid_roi, mean_valid_roi_with_bets_weight, "
            "positive_window_count_then_mean_roi, mean_valid_roi_minus_std, "
            "positive_window_count_then_mean_roi_then_min_roi, "
            "min_valid_roi_then_mean_roi",
        )
    if not config.aggregate_selection_score_rules:
        raise ValueError("aggregate_selection_score_rules must not be empty")
    for aggregate_selection_score_rule in config.aggregate_selection_score_rules:
        if aggregate_selection_score_rule not in valid_aggregate_selection_score_rules:
            raise ValueError(
                "each aggregate_selection_score_rules entry must be one of "
                "roi_max, roi_then_bets, mean_valid_roi, mean_valid_roi_with_bets_weight, "
                "positive_window_count_then_mean_roi, mean_valid_roi_minus_std, "
                "positive_window_count_then_mean_roi_then_min_roi, "
                "min_valid_roi_then_mean_roi",
            )
    if config.min_bets_valid is not None and config.min_bets_valid < 1:
        raise ValueError("min_bets_valid must be positive")
    for min_bets_valid in config.min_bets_valid_values:
        if min_bets_valid < 1:
            raise ValueError("each min_bets_valid_values entry must be positive")
    if not config.evaluation_windows and not config.evaluation_window_pairs:
        raise ValueError("evaluation_windows must not be empty")
    for _, start_date, end_date in config.evaluation_windows:
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError("evaluation window start_date must be on or before end_date")
    for _, valid_start, valid_end, test_start, test_end in config.evaluation_window_pairs:
        if valid_start is not None and valid_end is not None and valid_start > valid_end:
            raise ValueError("valid window start_date must be on or before end_date")
        if test_start is not None and test_end is not None and test_start > test_end:
            raise ValueError("test window start_date must be on or before end_date")
    if config.selection_window_groups and not config.evaluation_window_pairs:
        raise ValueError("selection_window_groups requires evaluation_window_pairs")
    pair_labels = {label for label, *_ in config.evaluation_window_pairs}
    for label, valid_window_labels, test_window_label in config.selection_window_groups:
        if not valid_window_labels:
            raise ValueError(f"selection_window_group must include valid windows: {label}")
        missing_valid_labels = sorted(set(valid_window_labels) - pair_labels)
        if missing_valid_labels:
            raise ValueError(
                f"selection_window_group {label} has unknown valid windows: "
                f"{', '.join(missing_valid_labels)}",
            )
        if test_window_label not in pair_labels:
            raise ValueError(
                f"selection_window_group {label} has unknown test window: {test_window_label}",
            )
    if config.rolling_retrain_dataset_path is not None:
        if not config.evaluation_window_pairs:
            raise ValueError("rolling retrain requires evaluation_window_pairs")
        if config.rolling_model_name not in {
            "logistic_regression",
            "hist_gradient_boosting_small",
            "hist_gradient_boosting_small_ensemble",
        }:
            raise ValueError(
                "rolling_model_name must be logistic_regression "
                "or hist_gradient_boosting_small "
                "or hist_gradient_boosting_small_ensemble",
            )
        if not config.rolling_feature_columns:
            raise ValueError("rolling retrain requires rolling_feature_columns")
        if len(config.rolling_feature_columns) != len(config.rolling_feature_transforms):
            raise ValueError(
                "rolling_feature_columns and rolling_feature_transforms must have the same length",
            )
        for label, valid_start, valid_end, test_start, test_end in config.evaluation_window_pairs:
            if None in (valid_start, valid_end, test_start, test_end):
                raise ValueError(
                    f"rolling retrain requires concrete valid/test dates for window: {label}",
                )


def prepare_prediction_rows_temp_table(
    connection: duckdb.DuckDBPyConnection,
    config: PlaceBacktestConfig,
) -> bool:
    prediction_has_window_label = config.rolling_retrain_dataset_path is not None
    if prediction_has_window_label:
        rolling_prediction_path = config.output_dir / "rolling_predictions.csv"
        evaluation_window_pairs = tuple(
            RollingPairWindow(
                label=label,
                valid_start_date=valid_start,  # type: ignore[arg-type]
                valid_end_date=valid_end,  # type: ignore[arg-type]
                test_start_date=test_start,  # type: ignore[arg-type]
                test_end_date=test_end,  # type: ignore[arg-type]
            )
            for label, valid_start, valid_end, test_start, test_end
            in config.evaluation_window_pairs
        )
        generate_rolling_pair_predictions(
            dataset_path=config.rolling_retrain_dataset_path,  # type: ignore[arg-type]
            model_name=config.rolling_model_name,
            feature_columns=config.rolling_feature_columns,
            feature_transforms=config.rolling_feature_transforms,
            target_column=config.target_column,
            race_date_column=config.rolling_race_date_column,
            evaluation_window_pairs=evaluation_window_pairs,
            output_path=rolling_prediction_path,
            max_iter=config.rolling_max_iter,
            model_params=config.rolling_model_params,
        )
        connection.execute("DROP TABLE IF EXISTS prediction_rows_temp")
        connection.execute(
            """
            CREATE TEMP TABLE prediction_rows_temp AS
            SELECT * FROM read_csv_auto(?, header = true)
            """,
            [str(rolling_prediction_path)],
        )
        return True

    connection.execute("DROP TABLE IF EXISTS prediction_rows_temp")
    connection.execute(
        """
        CREATE TEMP TABLE prediction_rows_temp AS
        SELECT *, NULL::VARCHAR AS window_label
        FROM read_csv_auto(?, header = true)
        """,
        [str(config.predictions_path)],
    )
    return False


def build_prediction_row_filter(
    scope: PlaceBacktestScope,
    prediction_has_window_label: bool,
) -> tuple[str, list[object]]:
    if not prediction_has_window_label:
        return "", []
    return "\n                WHERE p.window_label = ?", [scope.window_label]


def iter_backtest_scopes(config: PlaceBacktestConfig) -> tuple[PlaceBacktestScope, ...]:
    if config.evaluation_window_pairs:
        scopes: list[PlaceBacktestScope] = []
        for (
            label,
            valid_start_date,
            valid_end_date,
            test_start_date,
            test_end_date,
        ) in config.evaluation_window_pairs:
            scopes.append(
                PlaceBacktestScope(
                    window_label=label,
                    window_start_date=(
                        valid_start_date.isoformat() if valid_start_date is not None else None
                    ),
                    window_end_date=(
                        valid_end_date.isoformat() if valid_end_date is not None else None
                    ),
                    split_expression="'valid' AS split",
                    split_name="valid",
                ),
            )
            scopes.append(
                PlaceBacktestScope(
                    window_label=label,
                    window_start_date=(
                        test_start_date.isoformat() if test_start_date is not None else None
                    ),
                    window_end_date=(
                        test_end_date.isoformat() if test_end_date is not None else None
                    ),
                    split_expression="'test' AS split",
                    split_name="test",
                ),
            )
        return tuple(scopes)
    return tuple(
        PlaceBacktestScope(
            window_label=label,
            window_start_date=start.isoformat() if start is not None else None,
            window_end_date=end.isoformat() if end is not None else None,
            split_expression=f"p.{config.split_column} AS split",
            split_name=None,
        )
        for label, start, end in config.evaluation_windows
    )


def iter_market_condition_bands(
    config: PlaceBacktestConfig,
) -> tuple[
    tuple[
        float | None,
        float | None,
        int | None,
        int | None,
        float | None,
        float | None,
    ],
    ...,
]:
    return tuple(
        (
            min_win_odds,
            max_win_odds,
            min_popularity,
            max_popularity,
            min_place_basis_odds,
            max_place_basis_odds,
        )
        for min_win_odds, max_win_odds in config.win_odds_bands
        for min_popularity, max_popularity in config.popularity_bands
        for min_place_basis_odds, max_place_basis_odds in config.place_basis_bands
    )


def expected_summary_splits(
    scope: PlaceBacktestScope,
    config: PlaceBacktestConfig,
) -> tuple[str, ...]:
    if config.evaluation_window_pairs:
        assert scope.split_name is not None
        return (scope.split_name,)
    return SPLIT_ORDER


def build_place_backtest_selection_summaries(
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSummary, ...],
) -> list[PlaceBacktestSelectionSummary]:
    if config.selection_window_groups:
        return build_aggregate_place_backtest_selection_summaries(config, summaries)

    selection_summaries: list[PlaceBacktestSelectionSummary] = []
    if config.min_bets_valid_values:
        min_bets_values: tuple[int | None, ...] = config.min_bets_valid_values
    elif config.min_bets_valid is not None:
        min_bets_values = (config.min_bets_valid,)
    else:
        min_bets_values = (None,)
    window_labels = sorted({summary.window_label for summary in summaries})
    for selection_score_rule in config.selection_score_rules:
        for min_bets_valid in min_bets_values:
            for window_label in window_labels:
                window_summaries = [
                    summary
                    for summary in summaries
                    if summary.window_label == window_label
                ]
                valid_summaries = [
                    summary for summary in window_summaries if summary.split == "valid"
                ]
                if min_bets_valid is not None:
                    valid_summaries = [
                        summary
                        for summary in valid_summaries
                        if summary.bet_count >= min_bets_valid
                    ]
                if not valid_summaries:
                    continue
                best_valid = max(
                    valid_summaries,
                    key=lambda summary: selection_sort_key(summary, selection_score_rule),
                )
                selection_summaries.append(
                    PlaceBacktestSelectionSummary(
                        window_label=window_label,
                        selection_score_rule=selection_score_rule,
                        min_bets_valid=min_bets_valid,
                        selected_on_split="valid",
                        applied_to_split="valid",
                        selection_metric=best_valid.selection_metric,
                        threshold=best_valid.threshold,
                        min_win_odds=best_valid.min_win_odds,
                        max_win_odds=best_valid.max_win_odds,
                        min_place_basis_odds=best_valid.min_place_basis_odds,
                        max_place_basis_odds=best_valid.max_place_basis_odds,
                        min_popularity=best_valid.min_popularity,
                        max_popularity=best_valid.max_popularity,
                        bet_count=best_valid.bet_count,
                        hit_count=best_valid.hit_count,
                        hit_rate=best_valid.hit_rate,
                        roi=best_valid.roi,
                        total_profit=best_valid.total_profit,
                        avg_payout=best_valid.avg_payout,
                        avg_edge=best_valid.avg_edge,
                    ),
                )
                test_match = next(
                    (
                        summary
                        for summary in window_summaries
                        if summary.split == "test"
                        and summary.selection_metric == best_valid.selection_metric
                        and summary.threshold == best_valid.threshold
                        and summary.min_win_odds == best_valid.min_win_odds
                        and summary.max_win_odds == best_valid.max_win_odds
                        and summary.min_place_basis_odds == best_valid.min_place_basis_odds
                        and summary.max_place_basis_odds == best_valid.max_place_basis_odds
                        and summary.min_popularity == best_valid.min_popularity
                        and summary.max_popularity == best_valid.max_popularity
                    ),
                    None,
                )
                if test_match is not None:
                    selection_summaries.append(
                        PlaceBacktestSelectionSummary(
                            window_label=window_label,
                            selection_score_rule=selection_score_rule,
                            min_bets_valid=min_bets_valid,
                            selected_on_split="valid",
                            applied_to_split="test",
                            selection_metric=test_match.selection_metric,
                            threshold=test_match.threshold,
                            min_win_odds=test_match.min_win_odds,
                            max_win_odds=test_match.max_win_odds,
                            min_place_basis_odds=test_match.min_place_basis_odds,
                            max_place_basis_odds=test_match.max_place_basis_odds,
                            min_popularity=test_match.min_popularity,
                            max_popularity=test_match.max_popularity,
                            bet_count=test_match.bet_count,
                            hit_count=test_match.hit_count,
                        hit_rate=test_match.hit_rate,
                        roi=test_match.roi,
                        total_profit=test_match.total_profit,
                        avg_payout=test_match.avg_payout,
                        avg_edge=test_match.avg_edge,
                        ),
                    )
    return selection_summaries


def build_aggregate_place_backtest_selection_summaries(
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSummary, ...],
) -> list[PlaceBacktestSelectionSummary]:
    selection_summaries: list[PlaceBacktestSelectionSummary] = []
    if config.min_bets_valid_values:
        min_bets_values: tuple[int | None, ...] = config.min_bets_valid_values
    elif config.min_bets_valid is not None:
        min_bets_values = (config.min_bets_valid,)
    else:
        min_bets_values = (None,)

    summaries_by_window_split: dict[tuple[str, str], list[PlaceBacktestSummary]] = {}
    for summary in summaries:
        summaries_by_window_split.setdefault(
            (summary.window_label, summary.split),
            [],
        ).append(summary)

    for aggregate_selection_score_rule in config.aggregate_selection_score_rules:
        for min_bets_valid in min_bets_values:
            for (
                group_label,
                valid_window_labels,
                test_window_label,
            ) in config.selection_window_groups:
                candidate_map: dict[
                    tuple[object, ...],
                    dict[str, PlaceBacktestSummary],
                ] = {}
                for valid_window_label in valid_window_labels:
                    for summary in summaries_by_window_split.get((valid_window_label, "valid"), []):
                        candidate_map.setdefault(candidate_identity(summary), {})[
                            valid_window_label
                        ] = summary

                aggregated_candidates: list[
                    tuple[PlaceBacktestSummary, AggregateValidSummary]
                ] = []
                for window_summary_map in candidate_map.values():
                    if any(label not in window_summary_map for label in valid_window_labels):
                        continue
                    valid_candidate_summaries = [
                        window_summary_map[label] for label in valid_window_labels
                    ]
                    if min_bets_valid is not None and any(
                        summary.bet_count < min_bets_valid
                        for summary in valid_candidate_summaries
                    ):
                        continue
                    aggregated_candidates.append(
                        (
                            valid_candidate_summaries[0],
                            aggregate_valid_summaries(
                                valid_candidate_summaries,
                                config.stake_per_bet,
                            ),
                        ),
                    )

                if not aggregated_candidates:
                    continue

                best_valid, best_aggregate = max(
                    aggregated_candidates,
                    key=lambda candidate: aggregate_selection_sort_key(
                        candidate[1],
                        best_candidate_threshold=candidate[0].threshold,
                        aggregate_selection_score_rule=aggregate_selection_score_rule,
                    ),
                )
                valid_window_labels_text = ",".join(valid_window_labels)
                selection_summaries.append(
                    PlaceBacktestSelectionSummary(
                        window_label=group_label,
                        selection_score_rule="aggregate_valid_windows",
                        aggregate_selection_score_rule=aggregate_selection_score_rule,
                        min_bets_valid=min_bets_valid,
                        selected_on_split="valid_aggregate",
                        applied_to_split="valid_aggregate",
                        selection_mode="aggregate_valid_windows",
                        valid_window_labels=valid_window_labels_text,
                        test_window_label=test_window_label,
                        valid_aggregate_score=aggregate_selection_score_value(
                            best_aggregate,
                            aggregate_selection_score_rule,
                        ),
                        valid_positive_window_count=best_aggregate.positive_window_count,
                        valid_mean_roi=best_aggregate.mean_valid_roi,
                        valid_min_roi=best_aggregate.min_valid_roi,
                        valid_roi_std=best_aggregate.valid_roi_std,
                        valid_window_rois=",".join(
                            f"{roi:.6f}" for roi in best_aggregate.valid_window_rois
                        ),
                        selection_metric=best_valid.selection_metric,
                        threshold=best_valid.threshold,
                        min_win_odds=best_valid.min_win_odds,
                        max_win_odds=best_valid.max_win_odds,
                        min_place_basis_odds=best_valid.min_place_basis_odds,
                        max_place_basis_odds=best_valid.max_place_basis_odds,
                        min_popularity=best_valid.min_popularity,
                        max_popularity=best_valid.max_popularity,
                        bet_count=best_aggregate.bet_count,
                        hit_count=best_aggregate.hit_count,
                        hit_rate=best_aggregate.hit_rate,
                        roi=best_aggregate.roi,
                        total_profit=best_aggregate.total_profit,
                        avg_payout=best_aggregate.avg_payout,
                        avg_edge=best_aggregate.avg_edge,
                    ),
                )

                test_match = next(
                    (
                        summary
                        for summary in summaries_by_window_split.get(
                            (test_window_label, "test"),
                            [],
                        )
                        if candidate_identity(summary) == candidate_identity(best_valid)
                    ),
                    None,
                )
                if test_match is None:
                    continue
                selection_summaries.append(
                    PlaceBacktestSelectionSummary(
                        window_label=group_label,
                        selection_score_rule="aggregate_valid_windows",
                        aggregate_selection_score_rule=aggregate_selection_score_rule,
                        min_bets_valid=min_bets_valid,
                        selected_on_split="valid_aggregate",
                        applied_to_split="test",
                        selection_mode="aggregate_valid_windows",
                        valid_window_labels=valid_window_labels_text,
                        test_window_label=test_window_label,
                        valid_aggregate_score=aggregate_selection_score_value(
                            best_aggregate,
                            aggregate_selection_score_rule,
                        ),
                        valid_positive_window_count=best_aggregate.positive_window_count,
                        valid_mean_roi=best_aggregate.mean_valid_roi,
                        valid_min_roi=best_aggregate.min_valid_roi,
                        valid_roi_std=best_aggregate.valid_roi_std,
                        valid_window_rois=",".join(
                            f"{roi:.6f}" for roi in best_aggregate.valid_window_rois
                        ),
                        selection_metric=test_match.selection_metric,
                        threshold=test_match.threshold,
                        min_win_odds=test_match.min_win_odds,
                        max_win_odds=test_match.max_win_odds,
                        min_place_basis_odds=test_match.min_place_basis_odds,
                        max_place_basis_odds=test_match.max_place_basis_odds,
                        min_popularity=test_match.min_popularity,
                        max_popularity=test_match.max_popularity,
                        bet_count=test_match.bet_count,
                        hit_count=test_match.hit_count,
                        hit_rate=test_match.hit_rate,
                        roi=test_match.roi,
                        total_profit=test_match.total_profit,
                        avg_payout=test_match.avg_payout,
                        avg_edge=test_match.avg_edge,
                    ),
                )
    return selection_summaries


def candidate_identity(summary: PlaceBacktestSummary) -> tuple[object, ...]:
    return (
        summary.selection_metric,
        summary.threshold,
        summary.min_win_odds,
        summary.max_win_odds,
        summary.min_place_basis_odds,
        summary.max_place_basis_odds,
        summary.min_popularity,
        summary.max_popularity,
    )


def build_place_backtest_selection_rollups(
    summaries: tuple[PlaceBacktestSelectionSummary, ...],
    stake_per_bet: float,
    bootstrap_iterations: int,
    random_seed: int,
) -> list[PlaceBacktestSelectionRollupSummary]:
    grouped: dict[
        tuple[str, str | None, str, int | None],
        list[PlaceBacktestSelectionSummary],
    ] = {}
    for summary in summaries:
        if summary.applied_to_split != "test":
            continue
        key = (
            summary.selection_score_rule,
            summary.aggregate_selection_score_rule,
            summary.selection_mode,
            summary.min_bets_valid,
        )
        grouped.setdefault(key, []).append(summary)

    rollups: list[PlaceBacktestSelectionRollupSummary] = []
    for key, group in grouped.items():
        ordered_group = list(group)
        bet_count = sum(summary.bet_count for summary in group)
        hit_count = sum(summary.hit_count for summary in group)
        total_profit = sum(summary.total_profit for summary in group)
        total_return = (bet_count * stake_per_bet) + total_profit
        total_payout = sum(summary.avg_payout * summary.hit_count for summary in group)
        weighted_edge_sum = sum(summary.avg_edge * summary.bet_count for summary in group)
        max_drawdown = compute_max_drawdown(
            [summary.total_profit for summary in ordered_group],
        )
        max_losing_streak = compute_max_losing_streak(
            [summary.total_profit for summary in ordered_group],
        )
        roi_gt_1_ratio = (
            sum(1 for summary in ordered_group if summary.roi > 1.0) / len(ordered_group)
            if ordered_group
            else 0.0
        )
        roi_ci_lower, roi_ci_upper = bootstrap_roi_interval(
            summaries=ordered_group,
            stake_per_bet=stake_per_bet,
            bootstrap_iterations=bootstrap_iterations,
            random_seed=random_seed,
        )
        rollups.append(
            PlaceBacktestSelectionRollupSummary(
                selection_score_rule=key[0],
                aggregate_selection_score_rule=key[1],
                selection_mode=key[2],
                min_bets_valid=key[3],
                test_window_count=len(group),
                test_window_labels=",".join(summary.window_label for summary in group),
                bet_count=bet_count,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
                total_profit=total_profit,
                avg_payout=(total_payout / hit_count) if hit_count > 0 else 0.0,
                avg_edge=(weighted_edge_sum / bet_count) if bet_count > 0 else 0.0,
                max_drawdown=max_drawdown,
                max_losing_streak=max_losing_streak,
                roi_gt_1_ratio=roi_gt_1_ratio,
                roi_ci_lower=roi_ci_lower,
                roi_ci_upper=roi_ci_upper,
            ),
        )
    return sorted(
        rollups,
        key=lambda summary: (
            summary.selection_mode,
            summary.aggregate_selection_score_rule or "",
            summary.selection_score_rule,
            summary.min_bets_valid or 0,
        ),
    )


def build_place_backtest_candidate_summaries(
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSummary, ...],
) -> list[PlaceBacktestCandidateSummary]:
    if config.selection_window_groups:
        return build_aggregate_place_backtest_candidate_summaries(config, summaries)
    return build_single_window_candidate_summaries(config, summaries)


def build_single_window_candidate_summaries(
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSummary, ...],
) -> list[PlaceBacktestCandidateSummary]:
    candidate_rows: list[PlaceBacktestCandidateSummary] = []
    if config.min_bets_valid_values:
        min_bets_values: tuple[int | None, ...] = config.min_bets_valid_values
    elif config.min_bets_valid is not None:
        min_bets_values = (config.min_bets_valid,)
    else:
        min_bets_values = (None,)
    window_labels = sorted({summary.window_label for summary in summaries})
    for selection_score_rule in config.selection_score_rules:
        for min_bets_valid in min_bets_values:
            for window_label in window_labels:
                window_summaries = [
                    summary
                    for summary in summaries
                    if summary.window_label == window_label
                ]
                valid_summaries = [
                    summary for summary in window_summaries if summary.split == "valid"
                ]
                if min_bets_valid is not None:
                    valid_summaries = [
                        summary for summary in valid_summaries if summary.bet_count >= min_bets_valid
                    ]
                for valid_summary in valid_summaries:
                    test_match = next(
                        (
                            summary
                            for summary in window_summaries
                            if summary.split == "test"
                            and candidate_identity(summary) == candidate_identity(valid_summary)
                        ),
                        None,
                    )
                    candidate_rows.append(
                        PlaceBacktestCandidateSummary(
                            window_label=window_label,
                            selection_score_rule=selection_score_rule,
                            aggregate_selection_score_rule=None,
                            min_bets_valid=min_bets_valid,
                            selection_mode="single_valid_window",
                            valid_window_labels=window_label,
                            test_window_label=window_label,
                            selection_metric=valid_summary.selection_metric,
                            threshold=valid_summary.threshold,
                            min_win_odds=valid_summary.min_win_odds,
                            max_win_odds=valid_summary.max_win_odds,
                            min_place_basis_odds=valid_summary.min_place_basis_odds,
                            max_place_basis_odds=valid_summary.max_place_basis_odds,
                            min_popularity=valid_summary.min_popularity,
                            max_popularity=valid_summary.max_popularity,
                            valid_aggregate_score=valid_summary.roi,
                            valid_bet_count=valid_summary.bet_count,
                            valid_hit_count=valid_summary.hit_count,
                            valid_hit_rate=valid_summary.hit_rate,
                            valid_roi=valid_summary.roi,
                            valid_total_profit=valid_summary.total_profit,
                            valid_avg_payout=valid_summary.avg_payout,
                            valid_avg_edge=valid_summary.avg_edge,
                            valid_positive_window_count=1 if valid_summary.roi > 1.0 else 0,
                            valid_mean_roi=valid_summary.roi,
                            valid_min_roi=valid_summary.roi,
                            valid_roi_std=0.0,
                            valid_window_rois=f"{valid_summary.roi:.6f}",
                            test_bet_count=test_match.bet_count if test_match is not None else 0,
                            test_hit_count=test_match.hit_count if test_match is not None else 0,
                            test_hit_rate=test_match.hit_rate if test_match is not None else 0.0,
                            test_roi=test_match.roi if test_match is not None else 0.0,
                            test_total_profit=(
                                test_match.total_profit if test_match is not None else 0.0
                            ),
                            test_avg_payout=test_match.avg_payout if test_match is not None else 0.0,
                            test_avg_edge=test_match.avg_edge if test_match is not None else 0.0,
                        ),
                    )
    return candidate_rows


def build_aggregate_place_backtest_candidate_summaries(
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSummary, ...],
) -> list[PlaceBacktestCandidateSummary]:
    candidate_rows: list[PlaceBacktestCandidateSummary] = []
    if config.min_bets_valid_values:
        min_bets_values: tuple[int | None, ...] = config.min_bets_valid_values
    elif config.min_bets_valid is not None:
        min_bets_values = (config.min_bets_valid,)
    else:
        min_bets_values = (None,)

    summaries_by_window_split: dict[tuple[str, str], list[PlaceBacktestSummary]] = {}
    for summary in summaries:
        summaries_by_window_split.setdefault((summary.window_label, summary.split), []).append(summary)

    for aggregate_selection_score_rule in config.aggregate_selection_score_rules:
        for min_bets_valid in min_bets_values:
            for group_label, valid_window_labels, test_window_label in config.selection_window_groups:
                candidate_map: dict[tuple[object, ...], dict[str, PlaceBacktestSummary]] = {}
                for valid_window_label in valid_window_labels:
                    for summary in summaries_by_window_split.get((valid_window_label, "valid"), []):
                        candidate_map.setdefault(candidate_identity(summary), {})[valid_window_label] = summary
                for window_summary_map in candidate_map.values():
                    if any(label not in window_summary_map for label in valid_window_labels):
                        continue
                    valid_candidate_summaries = [
                        window_summary_map[label] for label in valid_window_labels
                    ]
                    if min_bets_valid is not None and any(
                        summary.bet_count < min_bets_valid for summary in valid_candidate_summaries
                    ):
                        continue
                    base_summary = valid_candidate_summaries[0]
                    aggregate_summary = aggregate_valid_summaries(
                        valid_candidate_summaries,
                        config.stake_per_bet,
                    )
                    test_match = next(
                        (
                            summary
                            for summary in summaries_by_window_split.get((test_window_label, "test"), [])
                            if candidate_identity(summary) == candidate_identity(base_summary)
                        ),
                        None,
                    )
                    candidate_rows.append(
                        PlaceBacktestCandidateSummary(
                            window_label=group_label,
                            selection_score_rule="aggregate_valid_windows",
                            aggregate_selection_score_rule=aggregate_selection_score_rule,
                            min_bets_valid=min_bets_valid,
                            selection_mode="aggregate_valid_windows",
                            valid_window_labels=",".join(valid_window_labels),
                            test_window_label=test_window_label,
                            selection_metric=base_summary.selection_metric,
                            threshold=base_summary.threshold,
                            min_win_odds=base_summary.min_win_odds,
                            max_win_odds=base_summary.max_win_odds,
                            min_place_basis_odds=base_summary.min_place_basis_odds,
                            max_place_basis_odds=base_summary.max_place_basis_odds,
                            min_popularity=base_summary.min_popularity,
                            max_popularity=base_summary.max_popularity,
                            valid_aggregate_score=aggregate_selection_score_value(
                                aggregate_summary,
                                aggregate_selection_score_rule,
                            ),
                            valid_bet_count=aggregate_summary.bet_count,
                            valid_hit_count=aggregate_summary.hit_count,
                            valid_hit_rate=aggregate_summary.hit_rate,
                            valid_roi=aggregate_summary.roi,
                            valid_total_profit=aggregate_summary.total_profit,
                            valid_avg_payout=aggregate_summary.avg_payout,
                            valid_avg_edge=aggregate_summary.avg_edge,
                            valid_positive_window_count=aggregate_summary.positive_window_count,
                            valid_mean_roi=aggregate_summary.mean_valid_roi,
                            valid_min_roi=aggregate_summary.min_valid_roi,
                            valid_roi_std=aggregate_summary.valid_roi_std,
                            valid_window_rois=",".join(
                                f"{roi:.6f}" for roi in aggregate_summary.valid_window_rois
                            ),
                            test_bet_count=test_match.bet_count if test_match is not None else 0,
                            test_hit_count=test_match.hit_count if test_match is not None else 0,
                            test_hit_rate=test_match.hit_rate if test_match is not None else 0.0,
                            test_roi=test_match.roi if test_match is not None else 0.0,
                            test_total_profit=(
                                test_match.total_profit if test_match is not None else 0.0
                            ),
                            test_avg_payout=test_match.avg_payout if test_match is not None else 0.0,
                            test_avg_edge=test_match.avg_edge if test_match is not None else 0.0,
                        ),
                    )
    return sorted(
        candidate_rows,
        key=lambda row: (
            row.selection_mode,
            row.aggregate_selection_score_rule or "",
            row.window_label,
            row.min_bets_valid or 0,
            row.threshold,
        ),
    )


def build_place_backtest_uncertainty_summaries(
    rollups: tuple[PlaceBacktestSelectionRollupSummary, ...],
) -> list[PlaceBacktestUncertaintySummary]:
    return [
        PlaceBacktestUncertaintySummary(
            selection_score_rule=rollup.selection_score_rule,
            aggregate_selection_score_rule=rollup.aggregate_selection_score_rule,
            selection_mode=rollup.selection_mode,
            min_bets_valid=rollup.min_bets_valid,
            test_window_count=rollup.test_window_count,
            bet_count=rollup.bet_count,
            hit_count=rollup.hit_count,
            hit_rate=rollup.hit_rate,
            roi=rollup.roi,
            total_profit=rollup.total_profit,
            max_drawdown=rollup.max_drawdown,
            max_losing_streak=rollup.max_losing_streak,
            roi_gt_1_ratio=rollup.roi_gt_1_ratio,
            roi_ci_lower=rollup.roi_ci_lower,
            roi_ci_upper=rollup.roi_ci_upper,
        )
        for rollup in rollups
    ]


def build_place_backtest_yearly_summaries(
    *,
    selected_summaries: tuple[PlaceBacktestSelectionSummary, ...],
    monthly_summaries: tuple[PlaceBacktestMonthlySummary, ...],
    stake_per_bet: float,
) -> list[PlaceBacktestYearlySummary]:
    monthly_index: dict[tuple[object, ...], list[PlaceBacktestMonthlySummary]] = {}
    for monthly_summary in monthly_summaries:
        monthly_index.setdefault(
            (
                monthly_summary.window_label,
                monthly_summary.split,
                monthly_summary.selection_metric,
                monthly_summary.threshold,
                monthly_summary.min_win_odds,
                monthly_summary.max_win_odds,
                monthly_summary.min_place_basis_odds,
                monthly_summary.max_place_basis_odds,
                monthly_summary.min_popularity,
                monthly_summary.max_popularity,
            ),
            [],
        ).append(monthly_summary)

    grouped: dict[tuple[str, str | None, str, int | None, str], list[PlaceBacktestMonthlySummary]] = {}
    for selected_summary in selected_summaries:
        if selected_summary.applied_to_split != "test":
            continue
        matched_window_label = selected_summary.test_window_label or selected_summary.window_label
        matched_months = monthly_index.get(
            (
                matched_window_label,
                "test",
                selected_summary.selection_metric,
                selected_summary.threshold,
                selected_summary.min_win_odds,
                selected_summary.max_win_odds,
                selected_summary.min_place_basis_odds,
                selected_summary.max_place_basis_odds,
                selected_summary.min_popularity,
                selected_summary.max_popularity,
            ),
            [],
        )
        for month_row in matched_months:
            grouped.setdefault(
                (
                    selected_summary.selection_score_rule,
                    selected_summary.aggregate_selection_score_rule,
                    selected_summary.selection_mode,
                    selected_summary.min_bets_valid,
                    month_row.month[:4],
                ),
                [],
            ).append(month_row)

    yearly_rows: list[PlaceBacktestYearlySummary] = []
    for key, rows in grouped.items():
        bet_count = sum(row.bet_count for row in rows)
        hit_count = sum(row.hit_count for row in rows)
        total_profit = sum(row.total_profit for row in rows)
        total_return = (bet_count * stake_per_bet) + total_profit
        total_payout = sum(row.avg_payout * row.hit_count for row in rows)
        weighted_edge_sum = sum(row.avg_edge * row.bet_count for row in rows)
        yearly_rows.append(
            PlaceBacktestYearlySummary(
                selection_score_rule=key[0],
                aggregate_selection_score_rule=key[1],
                selection_mode=key[2],
                min_bets_valid=key[3],
                year=key[4],
                bet_count=bet_count,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
                total_profit=total_profit,
                avg_payout=(total_payout / hit_count) if hit_count > 0 else 0.0,
                avg_edge=(weighted_edge_sum / bet_count) if bet_count > 0 else 0.0,
            ),
        )
    return sorted(
        yearly_rows,
        key=lambda row: (
            row.selection_mode,
            row.aggregate_selection_score_rule or "",
            row.selection_score_rule,
            row.min_bets_valid or 0,
            row.year,
        ),
    )


def compute_max_drawdown(profit_sequence: list[float]) -> float:
    peak = 0.0
    equity = 0.0
    max_drawdown = 0.0
    for profit in profit_sequence:
        equity += profit
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def compute_max_losing_streak(profit_sequence: list[float]) -> int:
    losing_streak = 0
    max_losing_streak = 0
    for profit in profit_sequence:
        if profit < 0.0:
            losing_streak += 1
            max_losing_streak = max(max_losing_streak, losing_streak)
        else:
            losing_streak = 0
    return max_losing_streak


def bootstrap_roi_interval(
    *,
    summaries: list[PlaceBacktestSelectionSummary],
    stake_per_bet: float,
    bootstrap_iterations: int,
    random_seed: int,
) -> tuple[float, float]:
    if not summaries:
        return 0.0, 0.0
    rng = random.Random(random_seed)
    roi_samples: list[float] = []
    for _ in range(bootstrap_iterations):
        sampled = [rng.choice(summaries) for _ in range(len(summaries))]
        bet_count = sum(summary.bet_count for summary in sampled)
        total_profit = sum(summary.total_profit for summary in sampled)
        total_return = (bet_count * stake_per_bet) + total_profit
        roi_samples.append(
            (total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        )
    roi_samples.sort()
    lower_index = max(0, int(0.025 * (len(roi_samples) - 1)))
    upper_index = min(len(roi_samples) - 1, int(0.975 * (len(roi_samples) - 1)))
    return roi_samples[lower_index], roi_samples[upper_index]


def aggregate_valid_summaries(
    summaries: list[PlaceBacktestSummary],
    stake_per_bet: float,
) -> AggregateValidSummary:
    valid_window_rois = tuple(summary.roi for summary in summaries)
    bet_count = sum(summary.bet_count for summary in summaries)
    hit_count = sum(summary.hit_count for summary in summaries)
    total_profit = sum(summary.total_profit for summary in summaries)
    total_return = (bet_count * stake_per_bet) + total_profit
    total_payout = sum(summary.avg_payout * summary.hit_count for summary in summaries)
    mean_valid_roi = sum(valid_window_rois) / len(valid_window_rois)
    return AggregateValidSummary(
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(total_payout / hit_count) if hit_count > 0 else 0.0,
        avg_edge=(
            sum(summary.avg_edge * summary.bet_count for summary in summaries) / bet_count
            if bet_count > 0
            else 0.0
        ),
        mean_valid_roi=mean_valid_roi,
        mean_valid_roi_with_bets_weight=(
            sum(summary.roi * summary.bet_count for summary in summaries) / bet_count
            if bet_count > 0
            else 0.0
        ),
        positive_window_count=sum(1 for summary in summaries if summary.roi > 1.0),
        min_valid_roi=min(valid_window_rois),
        valid_roi_std=math.sqrt(
            sum((roi - mean_valid_roi) ** 2 for roi in valid_window_rois)
            / len(valid_window_rois)
        ),
        valid_window_rois=valid_window_rois,
    )


def selection_sort_key(
    summary: PlaceBacktestSummary,
    selection_score_rule: str,
) -> tuple[float, float, float, float]:
    if selection_score_rule == "roi_max":
        return (
            summary.roi,
            float(summary.bet_count),
            -summary.threshold,
            0.0,
        )
    elif selection_score_rule == "roi_then_bets":
        return (
            summary.roi,
            float(summary.bet_count),
            -summary.threshold,
            0.0,
        )
    elif selection_score_rule == "roi_weighted_by_bets":
        return (
            summary.roi * math.log1p(summary.bet_count),
            summary.roi,
            summary.bet_count,
            -summary.threshold,
        )
    else:
        raise ValueError(f"unsupported selection_score_rule: {selection_score_rule}")


def aggregate_selection_sort_key(
    aggregate_summary: AggregateValidSummary,
    best_candidate_threshold: float,
    aggregate_selection_score_rule: str,
) -> tuple[float, float, float, float]:
    if aggregate_selection_score_rule == "roi_max":
        return (
            aggregate_summary.roi,
            float(aggregate_summary.bet_count),
            -best_candidate_threshold,
            0.0,
        )
    if aggregate_selection_score_rule == "roi_then_bets":
        return (
            aggregate_summary.roi,
            float(aggregate_summary.bet_count),
            -best_candidate_threshold,
            0.0,
        )
    if aggregate_selection_score_rule == "mean_valid_roi":
        return (
            aggregate_summary.mean_valid_roi,
            aggregate_summary.mean_valid_roi_with_bets_weight,
            aggregate_summary.bet_count,
            -best_candidate_threshold,
        )
    if aggregate_selection_score_rule == "mean_valid_roi_with_bets_weight":
        return (
            aggregate_summary.mean_valid_roi_with_bets_weight,
            aggregate_summary.mean_valid_roi,
            aggregate_summary.bet_count,
            -best_candidate_threshold,
        )
    if aggregate_selection_score_rule == "positive_window_count_then_mean_roi":
        return (
            float(aggregate_summary.positive_window_count),
            aggregate_summary.mean_valid_roi,
            aggregate_summary.bet_count,
            -best_candidate_threshold,
        )
    if aggregate_selection_score_rule == "mean_valid_roi_minus_std":
        return (
            aggregate_summary.mean_valid_roi - aggregate_summary.valid_roi_std,
            aggregate_summary.min_valid_roi,
            aggregate_summary.bet_count,
            -best_candidate_threshold,
        )
    if aggregate_selection_score_rule == "positive_window_count_then_mean_roi_then_min_roi":
        return (
            float(aggregate_summary.positive_window_count),
            aggregate_summary.mean_valid_roi,
            aggregate_summary.min_valid_roi,
            -best_candidate_threshold,
        )
    if aggregate_selection_score_rule == "min_valid_roi_then_mean_roi":
        return (
            aggregate_summary.min_valid_roi,
            aggregate_summary.mean_valid_roi,
            aggregate_summary.bet_count,
            -best_candidate_threshold,
        )
    raise ValueError(
        "unsupported aggregate_selection_score_rule: "
        f"{aggregate_selection_score_rule}",
    )


def aggregate_selection_score_value(
    aggregate_summary: AggregateValidSummary,
    aggregate_selection_score_rule: str,
) -> float:
    if aggregate_selection_score_rule in {"roi_max", "roi_then_bets"}:
        return aggregate_summary.roi
    if aggregate_selection_score_rule == "mean_valid_roi":
        return aggregate_summary.mean_valid_roi
    if aggregate_selection_score_rule == "mean_valid_roi_with_bets_weight":
        return aggregate_summary.mean_valid_roi_with_bets_weight
    if aggregate_selection_score_rule == "positive_window_count_then_mean_roi":
        return float(aggregate_summary.positive_window_count)
    if aggregate_selection_score_rule == "mean_valid_roi_minus_std":
        return aggregate_summary.mean_valid_roi - aggregate_summary.valid_roi_std
    if aggregate_selection_score_rule == "positive_window_count_then_mean_roi_then_min_roi":
        return float(aggregate_summary.positive_window_count)
    if aggregate_selection_score_rule == "min_valid_roi_then_mean_roi":
        return aggregate_summary.min_valid_roi
    raise ValueError(
        "unsupported aggregate_selection_score_rule: "
        f"{aggregate_selection_score_rule}",
    )


def build_place_backtest_summaries(
    connection: duckdb.DuckDBPyConnection,
    config: PlaceBacktestConfig,
    prediction_has_window_label: bool,
) -> list[PlaceBacktestSummary]:
    payout_sql = build_payout_sql(config)
    summaries: list[PlaceBacktestSummary] = []
    selection_expression = build_selection_expression(config)
    market_prob_expression = build_market_prob_expression(config)
    for scope in iter_backtest_scopes(config):
        for (
            min_win_odds,
            max_win_odds,
            min_popularity,
            max_popularity,
            min_place_basis_odds,
            max_place_basis_odds,
        ) in iter_market_condition_bands(config):
            market_conditions = build_market_conditions(
                min_win_odds=min_win_odds,
                max_win_odds=max_win_odds,
                min_place_basis_odds=min_place_basis_odds,
                max_place_basis_odds=max_place_basis_odds,
                min_popularity=min_popularity,
                max_popularity=max_popularity,
                window_start_date=scope.window_start_date,
                window_end_date=scope.window_end_date,
            )
            prediction_row_filter, prediction_row_parameters = build_prediction_row_filter(
                scope,
                prediction_has_window_label,
            )
            for threshold in config.thresholds:
                parameters: list[object] = list(prediction_row_parameters)
                parameters.append(threshold)
                if min_win_odds is not None:
                    parameters.append(min_win_odds)
                if max_win_odds is not None:
                    parameters.append(max_win_odds)
                if min_place_basis_odds is not None:
                    parameters.append(min_place_basis_odds)
                if max_place_basis_odds is not None:
                    parameters.append(max_place_basis_odds)
                if min_popularity is not None:
                    parameters.append(min_popularity)
                if max_popularity is not None:
                    parameters.append(max_popularity)
                if scope.window_start_date is not None:
                    parameters.append(scope.window_start_date)
                if scope.window_end_date is not None:
                    parameters.append(scope.window_end_date)
                parameters.extend([config.stake_per_bet, config.stake_per_bet])
                rows = connection.execute(
                    f"""
            WITH base_rows AS (
                SELECT
                    p.race_key,
                    p.horse_number,
                    {scope.split_expression},
                    p.{config.target_column} AS target_value,
                    p.{config.probability_column} AS pred_probability,
                    s.result_date,
                    TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                    s.popularity AS popularity,
                    o.place_basis_odds,
                    CASE
                        WHEN TRY_CAST(s.win_odds AS DOUBLE) IS NOT NULL
                            AND TRY_CAST(s.win_odds AS DOUBLE) > 0.0
                        THEN 1.0 / TRY_CAST(s.win_odds AS DOUBLE)
                        ELSE NULL
                    END AS inverse_win_odds
                FROM prediction_rows_temp p
                INNER JOIN jrdb_sed_staging s
                    ON p.race_key = s.race_key
                    AND p.horse_number = s.horse_number
                LEFT JOIN jrdb_oz_staging o
                    ON p.race_key = o.race_key
                    AND p.horse_number = o.horse_number
                {prediction_row_filter}
                ),
            scored AS (
                SELECT
                    *,
                    {market_prob_expression} AS market_prob,
                    CASE
                        WHEN {market_prob_expression} IS NOT NULL
                        THEN pred_probability - {market_prob_expression}
                        ELSE NULL
                    END AS edge
                FROM base_rows
            ),
            adopted AS (
                SELECT *
                FROM scored
                WHERE {selection_expression}
                    {market_conditions}
            ),
            joined AS (
                SELECT
                    a.split,
                    a.target_value,
                    a.edge,
                    j.payout
                FROM adopted a
                LEFT JOIN {payout_sql} j
                    ON a.race_key = j.race_key
                    AND a.horse_number = j.horse_number
            )
            SELECT
                split,
                COUNT(*) AS bet_count,
                SUM(CASE WHEN payout IS NOT NULL THEN 1 ELSE 0 END) AS hit_count,
                AVG(CASE WHEN payout IS NOT NULL THEN 1.0 ELSE 0.0 END) AS hit_rate,
                SUM(COALESCE(payout, 0)) / (COUNT(*) * ?) AS roi,
                SUM(COALESCE(payout, 0)) - COUNT(*) * ? AS total_profit,
                AVG(CASE WHEN payout IS NOT NULL THEN payout END) AS avg_payout,
                AVG(edge) AS avg_edge
            FROM joined
            GROUP BY 1
            """,
                    parameters,
                ).fetchall()
                summary_by_split = {
                    str(row[0]): PlaceBacktestSummary(
                        window_label=scope.window_label,
                        window_start_date=scope.window_start_date,
                        window_end_date=scope.window_end_date,
                        split=str(row[0]),
                        selection_metric=config.selection_metric,
                        threshold=threshold,
                        min_win_odds=min_win_odds,
                        max_win_odds=max_win_odds,
                        min_place_basis_odds=min_place_basis_odds,
                        max_place_basis_odds=max_place_basis_odds,
                        min_popularity=min_popularity,
                        max_popularity=max_popularity,
                        bet_count=int(row[1]),
                        hit_count=int(row[2] or 0),
                        hit_rate=float(row[3]) if row[3] is not None else 0.0,
                        roi=float(row[4]) if row[4] is not None else 0.0,
                        total_profit=float(row[5]) if row[5] is not None else 0.0,
                        avg_payout=float(row[6]) if row[6] is not None else 0.0,
                        avg_edge=float(row[7]) if row[7] is not None else 0.0,
                    )
                    for row in rows
                }
                summaries.extend(
                    summary_by_split.get(
                        split_name,
                        PlaceBacktestSummary(
                            window_label=scope.window_label,
                            window_start_date=scope.window_start_date,
                            window_end_date=scope.window_end_date,
                            split=split_name,
                            selection_metric=config.selection_metric,
                            threshold=threshold,
                            min_win_odds=min_win_odds,
                            max_win_odds=max_win_odds,
                            min_place_basis_odds=min_place_basis_odds,
                            max_place_basis_odds=max_place_basis_odds,
                            min_popularity=min_popularity,
                            max_popularity=max_popularity,
                            bet_count=0,
                            hit_count=0,
                            hit_rate=0.0,
                            roi=0.0,
                            total_profit=0.0,
                            avg_payout=0.0,
                            avg_edge=0.0,
                        ),
                    )
                    for split_name in expected_summary_splits(scope, config)
                )
    return summaries


def build_place_backtest_monthly_summaries(
    connection: duckdb.DuckDBPyConnection,
    config: PlaceBacktestConfig,
    prediction_has_window_label: bool,
) -> list[PlaceBacktestMonthlySummary]:
    payout_sql = build_payout_sql(config)
    selection_expression = build_selection_expression(config)
    market_prob_expression = build_market_prob_expression(config)
    monthly_summaries: list[PlaceBacktestMonthlySummary] = []
    for scope in iter_backtest_scopes(config):
        for (
            min_win_odds,
            max_win_odds,
            min_popularity,
            max_popularity,
            min_place_basis_odds,
            max_place_basis_odds,
        ) in iter_market_condition_bands(config):
            market_conditions = build_market_conditions(
                min_win_odds=min_win_odds,
                max_win_odds=max_win_odds,
                min_place_basis_odds=min_place_basis_odds,
                max_place_basis_odds=max_place_basis_odds,
                min_popularity=min_popularity,
                max_popularity=max_popularity,
                window_start_date=scope.window_start_date,
                window_end_date=scope.window_end_date,
            )
            prediction_row_filter, prediction_row_parameters = build_prediction_row_filter(
                scope,
                prediction_has_window_label,
            )
            for threshold in config.thresholds:
                parameters: list[object] = list(prediction_row_parameters)
                parameters.append(threshold)
                if min_win_odds is not None:
                    parameters.append(min_win_odds)
                if max_win_odds is not None:
                    parameters.append(max_win_odds)
                if min_place_basis_odds is not None:
                    parameters.append(min_place_basis_odds)
                if max_place_basis_odds is not None:
                    parameters.append(max_place_basis_odds)
                if min_popularity is not None:
                    parameters.append(min_popularity)
                if max_popularity is not None:
                    parameters.append(max_popularity)
                if scope.window_start_date is not None:
                    parameters.append(scope.window_start_date)
                if scope.window_end_date is not None:
                    parameters.append(scope.window_end_date)
                parameters.extend([config.stake_per_bet, config.stake_per_bet])
                rows = connection.execute(
                    f"""
            WITH base_rows AS (
                SELECT
                    p.race_key,
                    p.horse_number,
                    {scope.split_expression},
                    p.{config.target_column} AS target_value,
                    p.{config.probability_column} AS pred_probability,
                    s.result_date,
                    TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                    s.popularity AS popularity,
                    o.place_basis_odds,
                    CASE
                        WHEN TRY_CAST(s.win_odds AS DOUBLE) IS NOT NULL
                            AND TRY_CAST(s.win_odds AS DOUBLE) > 0.0
                        THEN 1.0 / TRY_CAST(s.win_odds AS DOUBLE)
                        ELSE NULL
                    END AS inverse_win_odds
                FROM prediction_rows_temp p
                INNER JOIN jrdb_sed_staging s
                    ON p.race_key = s.race_key
                    AND p.horse_number = s.horse_number
                LEFT JOIN jrdb_oz_staging o
                    ON p.race_key = o.race_key
                    AND p.horse_number = o.horse_number
                {prediction_row_filter}
                ),
            scored AS (
                SELECT
                    *,
                    {market_prob_expression} AS market_prob,
                    CASE
                        WHEN {market_prob_expression} IS NOT NULL
                        THEN pred_probability - {market_prob_expression}
                        ELSE NULL
                    END AS edge
                FROM base_rows
            ),
            adopted AS (
                SELECT *
                FROM scored
                WHERE {selection_expression}
                    {market_conditions}
            ),
            joined AS (
                SELECT
                    STRFTIME(result_date, '%Y-%m') AS month,
                    a.split,
                    a.edge,
                    j.payout
                FROM adopted a
                LEFT JOIN {payout_sql} j
                    ON a.race_key = j.race_key
                    AND a.horse_number = j.horse_number
            )
            SELECT
                month,
                split,
                COUNT(*) AS bet_count,
                SUM(CASE WHEN payout IS NOT NULL THEN 1 ELSE 0 END) AS hit_count,
                AVG(CASE WHEN payout IS NOT NULL THEN 1.0 ELSE 0.0 END) AS hit_rate,
                SUM(COALESCE(payout, 0)) / (COUNT(*) * ?) AS roi,
                SUM(COALESCE(payout, 0)) - COUNT(*) * ? AS total_profit,
                AVG(CASE WHEN payout IS NOT NULL THEN payout END) AS avg_payout,
                AVG(edge) AS avg_edge
            FROM joined
            GROUP BY 1, 2
            ORDER BY 1, 2
            """,
                    parameters,
                ).fetchall()
                monthly_summaries.extend(
                    PlaceBacktestMonthlySummary(
                        window_label=scope.window_label,
                        window_start_date=scope.window_start_date,
                        window_end_date=scope.window_end_date,
                        month=str(row[0]),
                        split=str(row[1]),
                        selection_metric=config.selection_metric,
                        threshold=threshold,
                        min_win_odds=min_win_odds,
                        max_win_odds=max_win_odds,
                        min_place_basis_odds=min_place_basis_odds,
                        max_place_basis_odds=max_place_basis_odds,
                        min_popularity=min_popularity,
                        max_popularity=max_popularity,
                        bet_count=int(row[2]),
                        hit_count=int(row[3] or 0),
                        hit_rate=float(row[4]) if row[4] is not None else 0.0,
                        roi=float(row[5]) if row[5] is not None else 0.0,
                        total_profit=float(row[6]) if row[6] is not None else 0.0,
                        avg_payout=float(row[7]) if row[7] is not None else 0.0,
                        avg_edge=float(row[8]) if row[8] is not None else 0.0,
                    )
                    for row in rows
                )
    return monthly_summaries


def build_place_backtest_odds_band_summaries(
    connection: duckdb.DuckDBPyConnection,
    config: PlaceBacktestConfig,
    prediction_has_window_label: bool,
) -> list[PlaceBacktestOddsBandSummary]:
    selection_expression = build_selection_expression(config)
    market_prob_expression = build_market_prob_expression(config)
    odds_band_summaries: list[PlaceBacktestOddsBandSummary] = []
    for scope in iter_backtest_scopes(config):
        for (
            min_win_odds,
            max_win_odds,
            min_popularity,
            max_popularity,
            min_place_basis_odds,
            max_place_basis_odds,
        ) in iter_market_condition_bands(config):
            market_conditions = build_market_conditions(
                min_win_odds=min_win_odds,
                max_win_odds=max_win_odds,
                min_place_basis_odds=min_place_basis_odds,
                max_place_basis_odds=max_place_basis_odds,
                min_popularity=min_popularity,
                max_popularity=max_popularity,
                window_start_date=scope.window_start_date,
                window_end_date=scope.window_end_date,
            )
            prediction_row_filter, prediction_row_parameters = build_prediction_row_filter(
                scope,
                prediction_has_window_label,
            )
            for threshold in config.thresholds:
                parameters: list[object] = list(prediction_row_parameters)
                parameters.append(threshold)
                if min_win_odds is not None:
                    parameters.append(min_win_odds)
                if max_win_odds is not None:
                    parameters.append(max_win_odds)
                if min_place_basis_odds is not None:
                    parameters.append(min_place_basis_odds)
                if max_place_basis_odds is not None:
                    parameters.append(max_place_basis_odds)
                if min_popularity is not None:
                    parameters.append(min_popularity)
                if max_popularity is not None:
                    parameters.append(max_popularity)
                if scope.window_start_date is not None:
                    parameters.append(scope.window_start_date)
                if scope.window_end_date is not None:
                    parameters.append(scope.window_end_date)
                rows = connection.execute(
                    f"""
            WITH base_rows AS (
                SELECT
                    p.race_key,
                    p.horse_number,
                    {scope.split_expression},
                    p.{config.probability_column} AS pred_probability,
                    s.result_date,
                    TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                    s.popularity AS popularity,
                    o.place_basis_odds,
                    CASE
                        WHEN TRY_CAST(s.win_odds AS DOUBLE) IS NOT NULL
                            AND TRY_CAST(s.win_odds AS DOUBLE) > 0.0
                        THEN 1.0 / TRY_CAST(s.win_odds AS DOUBLE)
                        ELSE NULL
                    END AS inverse_win_odds
                FROM prediction_rows_temp p
                INNER JOIN jrdb_sed_staging s
                    ON p.race_key = s.race_key
                    AND p.horse_number = s.horse_number
                LEFT JOIN jrdb_oz_staging o
                    ON p.race_key = o.race_key
                    AND p.horse_number = o.horse_number
                {prediction_row_filter}
                ),
            scored AS (
                SELECT
                    *,
                    {market_prob_expression} AS market_prob,
                    CASE
                        WHEN {market_prob_expression} IS NOT NULL
                        THEN pred_probability - {market_prob_expression}
                        ELSE NULL
                    END AS edge
                FROM base_rows
            ),
            adopted AS (
                SELECT
                    STRFTIME(result_date, '%Y-%m') AS month,
                    split,
                    win_odds,
                    place_basis_odds
                FROM scored
                WHERE {selection_expression}
                    {market_conditions}
            ),
            bucketed AS (
                SELECT
                    month,
                    split,
                    'win_odds' AS odds_metric,
                    CASE
                        WHEN win_odds < 2.0 THEN 'lt_2'
                        WHEN win_odds < 5.0 THEN '2_to_5'
                        WHEN win_odds < 10.0 THEN '5_to_10'
                        WHEN win_odds < 20.0 THEN '10_to_20'
                        WHEN win_odds < 50.0 THEN '20_to_50'
                        ELSE '50_plus'
                    END AS bucket_label
                FROM adopted
                WHERE win_odds IS NOT NULL
                UNION ALL
                SELECT
                    month,
                    split,
                    'place_basis_odds' AS odds_metric,
                    CASE
                        WHEN place_basis_odds < 1.2 THEN 'lt_1_2'
                        WHEN place_basis_odds < 1.5 THEN '1_2_to_1_5'
                        WHEN place_basis_odds < 2.0 THEN '1_5_to_2_0'
                        WHEN place_basis_odds < 3.0 THEN '2_0_to_3_0'
                        WHEN place_basis_odds < 5.0 THEN '3_0_to_5_0'
                        ELSE '5_0_plus'
                    END AS bucket_label
                FROM adopted
                WHERE place_basis_odds IS NOT NULL
            )
            SELECT
                month,
                split,
                odds_metric,
                bucket_label,
                COUNT(*) AS adopted_count
            FROM bucketed
            GROUP BY 1, 2, 3, 4
            ORDER BY 1, 2, 3, 4
            """,
                    parameters,
                ).fetchall()
                odds_band_summaries.extend(
                    PlaceBacktestOddsBandSummary(
                        window_label=scope.window_label,
                        window_start_date=scope.window_start_date,
                        window_end_date=scope.window_end_date,
                        month=str(row[0]),
                        split=str(row[1]),
                        selection_metric=config.selection_metric,
                        threshold=threshold,
                        min_win_odds=min_win_odds,
                        max_win_odds=max_win_odds,
                        min_place_basis_odds=min_place_basis_odds,
                        max_place_basis_odds=max_place_basis_odds,
                        min_popularity=min_popularity,
                        max_popularity=max_popularity,
                        odds_metric=str(row[2]),
                        bucket_label=str(row[3]),
                        adopted_count=int(row[4]),
                    )
                    for row in rows
                )
    return odds_band_summaries


def build_place_backtest_place_basis_bucket_summaries(
    connection: duckdb.DuckDBPyConnection,
    config: PlaceBacktestConfig,
    prediction_has_window_label: bool,
) -> list[PlaceBacktestPlaceBasisBucketSummary]:
    payout_sql = build_payout_sql(config)
    selection_expression = build_selection_expression(config)
    market_prob_expression = build_market_prob_expression(config)
    bucket_summaries: list[PlaceBacktestPlaceBasisBucketSummary] = []
    for scope in iter_backtest_scopes(config):
        for (
            min_win_odds,
            max_win_odds,
            min_popularity,
            max_popularity,
            min_place_basis_odds,
            max_place_basis_odds,
        ) in iter_market_condition_bands(config):
            market_conditions = build_market_conditions(
                min_win_odds=min_win_odds,
                max_win_odds=max_win_odds,
                min_place_basis_odds=min_place_basis_odds,
                max_place_basis_odds=max_place_basis_odds,
                min_popularity=min_popularity,
                max_popularity=max_popularity,
                window_start_date=scope.window_start_date,
                window_end_date=scope.window_end_date,
            )
            prediction_row_filter, prediction_row_parameters = build_prediction_row_filter(
                scope,
                prediction_has_window_label,
            )
            for threshold in config.thresholds:
                parameters: list[object] = list(prediction_row_parameters)
                parameters.append(threshold)
                if min_win_odds is not None:
                    parameters.append(min_win_odds)
                if max_win_odds is not None:
                    parameters.append(max_win_odds)
                if min_place_basis_odds is not None:
                    parameters.append(min_place_basis_odds)
                if max_place_basis_odds is not None:
                    parameters.append(max_place_basis_odds)
                if min_popularity is not None:
                    parameters.append(min_popularity)
                if max_popularity is not None:
                    parameters.append(max_popularity)
                if scope.window_start_date is not None:
                    parameters.append(scope.window_start_date)
                if scope.window_end_date is not None:
                    parameters.append(scope.window_end_date)
                parameters.extend([config.stake_per_bet, config.stake_per_bet])
                rows = connection.execute(
                    f"""
            WITH base_rows AS (
                SELECT
                    p.race_key,
                    p.horse_number,
                    {scope.split_expression},
                    p.{config.probability_column} AS pred_probability,
                    s.result_date,
                    TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                    s.popularity AS popularity,
                    o.place_basis_odds,
                    CASE
                        WHEN TRY_CAST(s.win_odds AS DOUBLE) IS NOT NULL
                            AND TRY_CAST(s.win_odds AS DOUBLE) > 0.0
                        THEN 1.0 / TRY_CAST(s.win_odds AS DOUBLE)
                        ELSE NULL
                    END AS inverse_win_odds
                FROM prediction_rows_temp p
                INNER JOIN jrdb_sed_staging s
                    ON p.race_key = s.race_key
                    AND p.horse_number = s.horse_number
                LEFT JOIN jrdb_oz_staging o
                    ON p.race_key = o.race_key
                    AND p.horse_number = o.horse_number
                {prediction_row_filter}
                ),
            scored AS (
                SELECT
                    *,
                    {market_prob_expression} AS market_prob,
                    CASE
                        WHEN {market_prob_expression} IS NOT NULL
                        THEN pred_probability - {market_prob_expression}
                        ELSE NULL
                    END AS edge
                FROM base_rows
            ),
            adopted AS (
                SELECT
                    race_key,
                    horse_number,
                    STRFTIME(result_date, '%Y-%m') AS month,
                    split,
                    edge,
                    CASE
                        WHEN place_basis_odds < 1.2 THEN 'lt_1_2'
                        WHEN place_basis_odds < 1.5 THEN '1_2_to_1_5'
                        WHEN place_basis_odds < 2.0 THEN '1_5_to_2_0'
                        WHEN place_basis_odds < 3.0 THEN '2_0_to_3_0'
                        WHEN place_basis_odds < 5.0 THEN '3_0_to_5_0'
                        ELSE '5_0_plus'
                    END AS bucket_label
                FROM scored
                WHERE {selection_expression}
                    {market_conditions}
                    AND place_basis_odds IS NOT NULL
            ),
            joined AS (
                SELECT
                    a.month,
                    a.split,
                    a.bucket_label,
                    a.edge,
                    j.payout
                FROM adopted a
                LEFT JOIN {payout_sql} j
                    ON a.race_key = j.race_key
                    AND a.horse_number = j.horse_number
            )
            SELECT
                month,
                split,
                bucket_label,
                COUNT(*) AS bet_count,
                SUM(CASE WHEN payout IS NOT NULL THEN 1 ELSE 0 END) AS hit_count,
                AVG(CASE WHEN payout IS NOT NULL THEN 1.0 ELSE 0.0 END) AS hit_rate,
                SUM(COALESCE(payout, 0)) / (COUNT(*) * ?) AS roi,
                SUM(COALESCE(payout, 0)) - COUNT(*) * ? AS total_profit,
                AVG(CASE WHEN payout IS NOT NULL THEN payout END) AS avg_payout,
                AVG(edge) AS avg_edge
            FROM joined
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """,
                    parameters,
                ).fetchall()
                bucket_summaries.extend(
                    PlaceBacktestPlaceBasisBucketSummary(
                        window_label=scope.window_label,
                        window_start_date=scope.window_start_date,
                        window_end_date=scope.window_end_date,
                        month=str(row[0]),
                        split=str(row[1]),
                        selection_metric=config.selection_metric,
                        threshold=threshold,
                        min_win_odds=min_win_odds,
                        max_win_odds=max_win_odds,
                        min_place_basis_odds=min_place_basis_odds,
                        max_place_basis_odds=max_place_basis_odds,
                        min_popularity=min_popularity,
                        max_popularity=max_popularity,
                        bucket_label=str(row[2]),
                        bet_count=int(row[3]),
                        hit_count=int(row[4] or 0),
                        hit_rate=float(row[5]) if row[5] is not None else 0.0,
                        roi=float(row[6]) if row[6] is not None else 0.0,
                        total_profit=float(row[7]) if row[7] is not None else 0.0,
                        avg_payout=float(row[8]) if row[8] is not None else 0.0,
                        avg_edge=float(row[9]) if row[9] is not None else 0.0,
                    )
                    for row in rows
                )
    return bucket_summaries


def build_selection_expression(config: PlaceBacktestConfig) -> str:
    if config.selection_metric == "probability":
        return "pred_probability >= ?"
    return "edge >= ?"


def build_payout_sql(config: PlaceBacktestConfig) -> str:
    if config.payout_type == "win":
        return """
            (
                SELECT race_key, win_horse_number AS horse_number, win_payout AS payout
                FROM jrdb_hjc_staging
            )
        """
    return """
        (
            SELECT race_key, place_horse_number_1 AS horse_number, place_payout_1 AS payout
            FROM jrdb_hjc_staging
            UNION ALL
            SELECT race_key, place_horse_number_2 AS horse_number, place_payout_2 AS payout
            FROM jrdb_hjc_staging
            UNION ALL
            SELECT race_key, place_horse_number_3 AS horse_number, place_payout_3 AS payout
            FROM jrdb_hjc_staging
        )
    """


def build_market_prob_expression(config: PlaceBacktestConfig) -> str:
    if config.market_prob_method == "inverse_win_odds":
        return "inverse_win_odds"
    if config.market_prob_method == "oz_place_basis_inverse":
        return (
            "CASE "
            "WHEN place_basis_odds IS NOT NULL AND place_basis_odds > 0.0 "
            "THEN 1.0 / place_basis_odds "
            "ELSE NULL END"
        )
    return (
        "CASE "
        "WHEN inverse_win_odds IS NOT NULL "
        "AND SUM(inverse_win_odds) OVER (PARTITION BY race_key) > 0.0 "
        "THEN inverse_win_odds / SUM(inverse_win_odds) OVER (PARTITION BY race_key) "
        "ELSE NULL END"
    )


def build_market_conditions(
    *,
    min_win_odds: float | None,
    max_win_odds: float | None,
    min_place_basis_odds: float | None,
    max_place_basis_odds: float | None,
    min_popularity: int | None,
    max_popularity: int | None,
    window_start_date: str | None,
    window_end_date: str | None,
) -> str:
    conditions: list[str] = []
    if min_win_odds is not None:
        conditions.append("AND win_odds >= ?")
    if max_win_odds is not None:
        conditions.append("AND win_odds <= ?")
    if min_place_basis_odds is not None:
        conditions.append("AND place_basis_odds >= ?")
    if max_place_basis_odds is not None:
        conditions.append("AND place_basis_odds <= ?")
    if min_popularity is not None:
        conditions.append("AND popularity >= ?")
    if max_popularity is not None:
        conditions.append("AND popularity <= ?")
    if window_start_date is not None:
        conditions.append("AND result_date >= ?")
    if window_end_date is not None:
        conditions.append("AND result_date <= ?")
    if not conditions:
        return ""
    indent = "\n                    "
    return indent + indent.join(conditions)


def write_place_backtest_csv(path: Path, summaries: tuple[PlaceBacktestSummary, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSummary, ...],
) -> None:
    payload = {
        "provenance": build_place_backtest_provenance(
            config,
            artifact_kind="place_backtest_summary_json",
            artifact_path=path,
        ),
        "backtest": {
            "name": config.name,
            "predictions_path": str(config.predictions_path),
            "duckdb_path": str(config.duckdb_path),
            "payout_type": config.payout_type,
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "thresholds": list(config.thresholds),
            "min_win_odds": config.min_win_odds,
            "max_win_odds": config.max_win_odds,
            "win_odds_bands": [
                {"min": min_win_odds, "max": max_win_odds}
                for min_win_odds, max_win_odds in config.win_odds_bands
            ],
            "min_place_basis_odds": config.min_place_basis_odds,
            "max_place_basis_odds": config.max_place_basis_odds,
            "min_popularity": config.min_popularity,
            "max_popularity": config.max_popularity,
            "popularity_bands": [
                {"min": min_popularity, "max": max_popularity}
                for min_popularity, max_popularity in config.popularity_bands
            ],
            "place_basis_bands": [
                {"min": min_place_basis_odds, "max": max_place_basis_odds}
                for min_place_basis_odds, max_place_basis_odds in config.place_basis_bands
            ],
            "evaluation_windows": [
                {
                    "label": label,
                    "start_date": start.isoformat() if start is not None else None,
                    "end_date": end.isoformat() if end is not None else None,
                }
                for label, start, end in config.evaluation_windows
            ],
            "evaluation_window_pairs": [
                {
                    "label": label,
                    "valid_start_date": (
                        valid_start.isoformat() if valid_start is not None else None
                    ),
                    "valid_end_date": (
                        valid_end.isoformat() if valid_end is not None else None
                    ),
                    "test_start_date": (
                        test_start.isoformat() if test_start is not None else None
                    ),
                    "test_end_date": test_end.isoformat() if test_end is not None else None,
                }
                for label, valid_start, valid_end, test_start, test_end
                in config.evaluation_window_pairs
            ],
            "stake_per_bet": config.stake_per_bet,
            "bootstrap_iterations": config.bootstrap_iterations,
            "random_seed": config.random_seed,
            "min_bets_valid": config.min_bets_valid,
            "min_bets_valid_values": list(config.min_bets_valid_values),
            "rolling_retrain": (
                {
                    "dataset_path": str(config.rolling_retrain_dataset_path),
                    "model_name": config.rolling_model_name,
                    "feature_columns": list(config.rolling_feature_columns),
                    "feature_transforms": list(config.rolling_feature_transforms),
                    "race_date_column": config.rolling_race_date_column,
                    "max_iter": config.rolling_max_iter,
                }
                if config.rolling_retrain_dataset_path is not None
                else None
            ),
            "split_column": config.split_column,
            "target_column": config.target_column,
            "probability_column": config.probability_column,
        },
        "summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def build_place_backtest_provenance(
    config: PlaceBacktestConfig,
    *,
    artifact_kind: str,
    artifact_path: Path,
) -> dict[str, object]:
    model_feature_columns = (
        config.rolling_feature_columns
        if config.rolling_retrain_dataset_path is not None and config.rolling_feature_columns
        else None
    )
    return build_feature_provenance_payload(
        artifact_kind=artifact_kind,
        generated_by="horse_bet_lab.evaluation.place_backtest.run_place_backtest",
        config_identifier=config.name,
        model_feature_columns=model_feature_columns,
        artifact_path=str(artifact_path),
        extra={
            "predictions_path": str(config.predictions_path),
            "duckdb_path": str(config.duckdb_path),
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "rolling_retrain_dataset_path": (
                str(config.rolling_retrain_dataset_path)
                if config.rolling_retrain_dataset_path is not None
                else None
            ),
        },
    )


def write_place_backtest_provenance_manifest(path: Path, config: PlaceBacktestConfig) -> None:
    payload = build_place_backtest_provenance(
        config,
        artifact_kind="place_backtest_output_dir",
        artifact_path=path,
    )
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_selection_csv(
    path: Path,
    summaries: tuple[PlaceBacktestSelectionSummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_SELECTION_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_selection_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSelectionSummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "payout_type": config.payout_type,
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "thresholds": list(config.thresholds),
            "win_odds_bands": [
                {"min": min_win_odds, "max": max_win_odds}
                for min_win_odds, max_win_odds in config.win_odds_bands
            ],
            "place_basis_bands": [
                {"min": min_place_basis_odds, "max": max_place_basis_odds}
                for min_place_basis_odds, max_place_basis_odds in config.place_basis_bands
            ],
            "popularity_bands": [
                {"min": min_popularity, "max": max_popularity}
                for min_popularity, max_popularity in config.popularity_bands
            ],
            "selection_rule": {
                "selected_on_split": "valid",
                "selection_score_rule": config.selection_score_rule,
                "selection_score_rules": list(config.selection_score_rules),
                "aggregate_selection_score_rule": config.aggregate_selection_score_rule,
                "aggregate_selection_score_rules": list(config.aggregate_selection_score_rules),
                "min_bets_valid": config.min_bets_valid,
                "min_bets_valid_values": list(config.min_bets_valid_values),
                "sort_order": [
                    "rule_primary_desc",
                    "roi_desc",
                    "bet_count_desc",
                    "threshold_asc",
                ],
                "selection_window_groups": [
                    {
                        "label": label,
                        "valid_window_labels": list(valid_window_labels),
                        "test_window_label": test_window_label,
                    }
                    for label, valid_window_labels, test_window_label
                    in config.selection_window_groups
                ],
            },
            "rolling_retrain": (
                {
                    "dataset_path": str(config.rolling_retrain_dataset_path),
                    "model_name": config.rolling_model_name,
                    "feature_columns": list(config.rolling_feature_columns),
                    "feature_transforms": list(config.rolling_feature_transforms),
                    "race_date_column": config.rolling_race_date_column,
                    "max_iter": config.rolling_max_iter,
                }
                if config.rolling_retrain_dataset_path is not None
                else None
            ),
        },
        "selected_summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_selection_rollup_csv(
    path: Path,
    summaries: tuple[PlaceBacktestSelectionRollupSummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_SELECTION_ROLLUP_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_selection_rollup_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestSelectionRollupSummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "payout_type": config.payout_type,
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "thresholds": list(config.thresholds),
            "win_odds_bands": [
                {"min": min_win_odds, "max": max_win_odds}
                for min_win_odds, max_win_odds in config.win_odds_bands
            ],
        },
        "rollups": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_monthly_csv(
    path: Path,
    summaries: tuple[PlaceBacktestMonthlySummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_MONTHLY_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_monthly_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestMonthlySummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "predictions_path": str(config.predictions_path),
            "duckdb_path": str(config.duckdb_path),
            "payout_type": config.payout_type,
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "thresholds": list(config.thresholds),
            "win_odds_bands": [
                {"min": min_win_odds, "max": max_win_odds}
                for min_win_odds, max_win_odds in config.win_odds_bands
            ],
        },
        "monthly_summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_odds_band_csv(
    path: Path,
    summaries: tuple[PlaceBacktestOddsBandSummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_ODDS_BAND_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_odds_band_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestOddsBandSummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "predictions_path": str(config.predictions_path),
            "duckdb_path": str(config.duckdb_path),
            "payout_type": config.payout_type,
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "thresholds": list(config.thresholds),
            "win_odds_bands": [
                {"min": min_win_odds, "max": max_win_odds}
                for min_win_odds, max_win_odds in config.win_odds_bands
            ],
        },
        "odds_band_summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_place_basis_bucket_csv(
    path: Path,
    summaries: tuple[PlaceBacktestPlaceBasisBucketSummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_PLACE_BASIS_BUCKET_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_place_basis_bucket_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestPlaceBasisBucketSummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "predictions_path": str(config.predictions_path),
            "duckdb_path": str(config.duckdb_path),
            "payout_type": config.payout_type,
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "thresholds": list(config.thresholds),
            "win_odds_bands": [
                {"min": min_win_odds, "max": max_win_odds}
                for min_win_odds, max_win_odds in config.win_odds_bands
            ],
        },
        "place_basis_bucket_summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_candidate_csv(
    path: Path,
    summaries: tuple[PlaceBacktestCandidateSummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_CANDIDATE_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_candidate_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestCandidateSummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "payout_type": config.payout_type,
            "selection_metric": config.selection_metric,
            "market_prob_method": config.market_prob_method,
            "thresholds": list(config.thresholds),
            "win_odds_bands": [
                {"min": min_win_odds, "max": max_win_odds}
                for min_win_odds, max_win_odds in config.win_odds_bands
            ],
            "popularity_bands": [
                {"min": min_popularity, "max": max_popularity}
                for min_popularity, max_popularity in config.popularity_bands
            ],
            "place_basis_bands": [
                {"min": min_place_basis_odds, "max": max_place_basis_odds}
                for min_place_basis_odds, max_place_basis_odds in config.place_basis_bands
            ],
            "aggregate_selection_score_rules": list(config.aggregate_selection_score_rules),
            "min_bets_valid_values": list(config.min_bets_valid_values),
        },
        "candidates": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_uncertainty_csv(
    path: Path,
    summaries: tuple[PlaceBacktestUncertaintySummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_UNCERTAINTY_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_uncertainty_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestUncertaintySummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "bootstrap_iterations": config.bootstrap_iterations,
            "random_seed": config.random_seed,
        },
        "uncertainty_summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_place_backtest_yearly_csv(
    path: Path,
    summaries: tuple[PlaceBacktestYearlySummary, ...],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PLACE_BACKTEST_YEARLY_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_place_backtest_yearly_json(
    path: Path,
    config: PlaceBacktestConfig,
    summaries: tuple[PlaceBacktestYearlySummary, ...],
) -> None:
    payload = {
        "backtest": {
            "name": config.name,
            "payout_type": config.payout_type,
        },
        "yearly_summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
