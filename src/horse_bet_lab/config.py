from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class ExperimentConfig:
    name: str
    model: str
    feature_set: str
    strategy: str
    period: str


@dataclass(frozen=True)
class DatasetBuildConfig:
    name: str
    start_date: date
    end_date: date
    train_end_date: date | None
    valid_end_date: date | None
    feature_set: str
    include_popularity: bool
    target_name: str
    duckdb_path: Path
    output_path: Path


@dataclass(frozen=True)
class ModelTrainConfig:
    name: str
    dataset_path: Path
    model_name: str
    feature_columns: tuple[str, ...]
    feature_transforms: tuple[str, ...]
    target_column: str
    split_column: str
    output_dir: Path
    max_iter: int
    model_params: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketFeatureComparisonFeatureSetConfig:
    name: str
    dataset_feature_set: str
    include_popularity: bool
    model_name: str
    feature_columns: tuple[str, ...]
    feature_transforms: tuple[str, ...]
    max_iter: int
    model_params: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketFeatureComparisonConfig:
    name: str
    duckdb_path: Path
    output_dir: Path
    start_date: date
    end_date: date
    target_name: str
    backtest_template_config_path: Path
    aggregate_selection_score_rule: str
    feature_sets: tuple[MarketFeatureComparisonFeatureSetConfig, ...]


@dataclass(frozen=True)
class BetCandidateEvalConfig:
    name: str
    predictions_path: Path
    output_dir: Path
    thresholds: tuple[float, ...]
    split_column: str
    target_column: str
    probability_column: str


@dataclass(frozen=True)
class BetSetDiffComparisonConfig:
    baseline_feature_set: str
    variant_feature_set: str


@dataclass(frozen=True)
class BetSetDiffAnalysisConfig:
    name: str
    duckdb_path: Path
    comparison_root_dir: Path
    output_dir: Path
    selection_metric: str
    market_prob_method: str
    split_column: str
    target_column: str
    probability_column: str
    stake_per_bet: float
    comparisons: tuple[BetSetDiffComparisonConfig, ...]


@dataclass(frozen=True)
class RankingScoreDiffConfig:
    name: str
    duckdb_path: Path
    backtest_dir: Path
    output_dir: Path
    selection_metric: str
    aggregate_selection_score_rule: str
    min_bets_valid: int | None
    split_column: str
    target_column: str
    probability_column: str
    stake_per_bet: float
    ranking_scores: tuple[str, ...]


@dataclass(frozen=True)
class RankingRuleComparisonConfig:
    name: str
    duckdb_path: Path
    rolling_predictions_path: Path
    output_dir: Path
    selection_metric: str
    market_prob_method: str
    thresholds: tuple[float, ...]
    min_win_odds: float | None
    max_win_odds: float | None
    split_column: str
    target_column: str
    probability_column: str
    stake_per_bet: float
    aggregate_selection_score_rule: str
    min_bets_valid: int | None
    popularity_bands: tuple[tuple[int | None, int | None], ...]
    place_basis_bands: tuple[tuple[float | None, float | None], ...]
    evaluation_window_pairs: tuple[
        tuple[str, date | None, date | None, date | None, date | None],
        ...,
    ]
    selection_window_groups: tuple[tuple[str, tuple[str, ...], str], ...]
    ranking_score_rules: tuple[str, ...]


@dataclass(frozen=True)
class BetLogicOnlyConfig:
    config_path: Path
    name: str
    duckdb_path: Path
    rolling_predictions_path: Path
    reference_label_guard_compare_config_path: Path
    output_dir: Path
    selection_metric: str
    market_prob_method: str
    thresholds: tuple[float, ...]
    min_win_odds: float | None
    max_win_odds: float | None
    split_column: str
    target_column: str
    probability_column: str
    stake_per_bet: float
    aggregate_selection_score_rule: str
    min_bets_valid: int | None
    popularity_bands: tuple[tuple[int | None, int | None], ...]
    place_basis_bands: tuple[tuple[float | None, float | None], ...]
    evaluation_window_pairs: tuple[
        tuple[str, date | None, date | None, date | None, date | None],
        ...,
    ]
    selection_window_groups: tuple[tuple[str, tuple[str, ...], str], ...]
    bootstrap_iterations: int
    random_seed: int
    stronger_guard_edge_surcharge: float
    sizing_tilt_step: float
    sizing_tilt_min_multiplier: float
    sizing_tilt_max_multiplier: float
    no_bet_guard_sensitivity_levels: tuple[float, ...]
    sizing_tilt_max_multiplier_sensitivity_levels: tuple[float, ...]
    initial_bankroll: float
    provisional_proxy_domain_overlay_enabled: bool
    formal_domain_mapping_confirmed: bool
    active_run_mode: str


@dataclass(frozen=True)
class ReferenceStrategyDiagnosticsConfig:
    name: str
    ranking_rule_comparison_config_path: Path
    output_dir: Path


@dataclass(frozen=True)
class RegimeDiffAnalysisConfig:
    name: str
    reference_strategy_config_path: Path
    output_dir: Path
    regimes: tuple[tuple[str, date | None, date | None], ...]
    representative_examples_per_regime: int


@dataclass(frozen=True)
class WithinBandRegimeDiffConfig:
    name: str
    reference_strategy_config_path: Path
    output_dir: Path
    min_popularity: int
    max_popularity: int
    min_place_basis_odds: float
    max_place_basis_odds: float
    regimes: tuple[tuple[str, date | None, date | None], ...]
    representative_examples_per_regime: int


@dataclass(frozen=True)
class CalibrationDriftConfig:
    name: str
    reference_strategy_config_path: Path
    output_dir: Path
    min_popularity: int
    max_popularity: int
    min_place_basis_odds: float
    max_place_basis_odds: float
    regimes: tuple[tuple[str, date | None, date | None], ...]
    representative_examples_per_regime: int


@dataclass(frozen=True)
class ReferenceGuardCompareConfig:
    name: str
    reference_strategy_config_path: Path
    output_dir: Path
    problematic_min_popularity: int
    problematic_max_popularity: int
    problematic_min_place_basis_odds: float
    problematic_max_place_basis_odds: float
    edge_surcharges: tuple[float, ...]
    exclude_win_odds_below: float | None
    exclude_edge_below: float | None


@dataclass(frozen=True)
class ResidualLossAnalysisConfig:
    name: str
    reference_guard_compare_config_path: Path
    output_dir: Path
    baseline_variant: str
    guarded_variant: str
    representative_examples_per_regime: int


@dataclass(frozen=True)
class SecondGuardSelectionConfig:
    name: str
    reference_guard_compare_config_path: Path
    output_dir: Path
    first_guard_variant: str
    second_guard_variants: tuple[str, ...]


@dataclass(frozen=True)
class ReferenceUncertaintyConfig:
    name: str
    second_guard_selection_config_path: Path
    output_dir: Path
    bootstrap_iterations: int
    random_seed: int
    bootstrap_block_unit: str


@dataclass(frozen=True)
class ReferenceRegimeLabelDiffConfig:
    name: str
    second_guard_selection_config_path: Path
    output_dir: Path
    regimes: tuple[tuple[str, date | None, date | None], ...]
    representative_examples_per_group: int


@dataclass(frozen=True)
class ReferenceLabelGuardCompareConfig:
    name: str
    second_guard_selection_config_path: Path
    output_dir: Path
    extra_guard_variants: tuple[str, ...]


@dataclass(frozen=True)
class ReferenceLabelGuardNullTestConfig:
    name: str
    reference_label_guard_compare_config_path: Path
    output_dir: Path
    null_iterations: int
    random_seed: int
    null_modes: tuple[str, ...]


@dataclass(frozen=True)
class ReferenceLabelGuardUncertaintyConfig:
    name: str
    reference_label_guard_compare_config_path: Path
    output_dir: Path
    bootstrap_iterations: int
    random_seed: int
    bootstrap_block_unit: str


@dataclass(frozen=True)
class ReferenceStakeSizingCompareConfig:
    name: str
    reference_label_guard_compare_config_path: Path
    output_dir: Path
    stake_variants: tuple[str, ...]
    edge_small_base_stake: float
    edge_small_step_stake: float
    edge_small_step_edge: float
    edge_small_max_stake: float
    kelly_bankroll: float
    kelly_fraction: float
    kelly_cap_stake: float
    per_race_cap_stake: float
    per_day_cap_stake: float
    drawdown_reduction_threshold: float
    drawdown_reduction_factor: float


@dataclass(frozen=True)
class ReferenceStakeSizingUncertaintyConfig:
    name: str
    reference_stake_sizing_compare_config_path: Path
    output_dir: Path
    bootstrap_iterations: int
    random_seed: int
    bootstrap_block_unit: str


@dataclass(frozen=True)
class ReferenceBankrollSimulationConfig:
    name: str
    reference_label_guard_compare_config_path: Path
    output_dir: Path
    stake_variants: tuple[str, ...]
    initial_bankrolls: tuple[float, ...]
    kelly_fraction: float
    kelly_cap_stake: float
    per_race_cap_stake: float
    per_day_cap_stake: float
    drawdown_reduction_threshold: float
    drawdown_reduction_factor: float
    bootstrap_iterations: int
    random_seed: int


@dataclass(frozen=True)
class ReferenceBankrollSimulationUncertaintyConfig:
    name: str
    reference_bankroll_simulation_config_path: Path
    output_dir: Path
    stake_variants: tuple[str, ...]
    initial_bankrolls: tuple[float, ...]
    bootstrap_iterations: int
    random_seed: int
    bootstrap_block_unit: str


@dataclass(frozen=True)
class ReferencePerRaceCapSensitivityConfig:
    name: str
    reference_bankroll_simulation_config_path: Path
    output_dir: Path
    initial_bankrolls: tuple[float, ...]
    per_race_cap_values: tuple[float, ...]
    bootstrap_iterations: int
    random_seed: int


@dataclass(frozen=True)
class ReferencePerRaceCapDrawdownCompareConfig:
    name: str
    reference_bankroll_simulation_config_path: Path
    output_dir: Path
    initial_bankrolls: tuple[float, ...]
    per_race_cap_values: tuple[float, ...]
    bootstrap_iterations: int
    random_seed: int


@dataclass(frozen=True)
class ReferencePackConfig:
    name: str
    reference_label_guard_compare_config_path: Path
    reference_label_guard_uncertainty_config_path: Path
    reference_per_race_cap_sensitivity_config_path: Path
    output_dir: Path
    model_name: str
    first_guard_name: str
    extra_label_guard_name: str
    ranking_rule_name: str
    selection_rule_name: str
    stateful_stake_variant: str
    mainline_per_race_cap_stake: float
    standard_initial_bankroll: float
    reference_initial_bankrolls: tuple[float, ...]
    research_candidates: tuple[str, ...]


@dataclass(frozen=True)
class MainlineBlockSensitivityConfig:
    name: str
    reference_label_guard_uncertainty_config_path: Path
    reference_bankroll_simulation_uncertainty_config_path: Path
    output_dir: Path
    bootstrap_block_units: tuple[str, ...]
    stateful_stake_variant: str
    stateful_initial_bankrolls: tuple[float, ...]


@dataclass(frozen=True)
class PlaceBacktestConfig:
    name: str
    predictions_path: Path
    duckdb_path: Path
    output_dir: Path
    payout_type: str
    selection_metric: str
    market_prob_method: str
    thresholds: tuple[float, ...]
    min_win_odds: float | None
    max_win_odds: float | None
    win_odds_bands: tuple[tuple[float | None, float | None], ...]
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    split_column: str
    target_column: str
    probability_column: str
    stake_per_bet: float
    selection_score_rule: str
    selection_score_rules: tuple[str, ...]
    aggregate_selection_score_rule: str
    aggregate_selection_score_rules: tuple[str, ...]
    min_bets_valid: int | None
    min_bets_valid_values: tuple[int, ...]
    rolling_retrain_dataset_path: Path | None
    rolling_model_name: str
    rolling_model_params: dict[str, object]
    rolling_feature_columns: tuple[str, ...]
    rolling_feature_transforms: tuple[str, ...]
    rolling_race_date_column: str
    rolling_max_iter: int
    popularity_bands: tuple[tuple[int | None, int | None], ...]
    place_basis_bands: tuple[tuple[float | None, float | None], ...]
    evaluation_windows: tuple[tuple[str, date | None, date | None], ...]
    evaluation_window_pairs: tuple[
        tuple[str, date | None, date | None, date | None, date | None],
        ...,
    ]
    selection_window_groups: tuple[tuple[str, tuple[str, ...], str], ...]
    bootstrap_iterations: int
    random_seed: int


@dataclass(frozen=True)
class WideResearchBacktestConfig:
    config_section: str
    name: str
    predictions_path: Path
    partner_predictions_path: Path | None
    hjc_raw_dir: Path
    output_dir: Path
    score_methods: tuple[str, ...]
    pair_generation_methods: tuple[str, ...]
    candidate_top_k_values: tuple[int, ...]
    adopted_pair_count_values: tuple[int, ...]
    split_column: str
    probability_column: str
    partner_probability_column: str
    window_label_column: str
    partner_weight_values: tuple[float, ...]
    stake_per_pair: float
    bootstrap_iterations: int
    random_seed: int


@dataclass(frozen=True)
class WideResearchDiffConfig:
    name: str
    v2_selected_pairs_path: Path
    v3_selected_pairs_path: Path
    v2_label: str
    v3_label: str
    raw_dir: Path | None
    output_dir: Path
    split: str
    stake_per_pair: float
    representative_example_count: int
    v2_score_method: str
    v2_pair_generation_method: str
    v2_partner_weight: float | None
    v3_score_method: str
    v3_pair_generation_method: str
    v3_partner_weight: float | None


@dataclass(frozen=True)
class WideFamilySelectionConfig:
    name: str
    v3_summary_path: Path
    v3_selected_pairs_path: Path
    v6_summary_path: Path
    v6_selected_pairs_path: Path
    v3_label: str
    v6_label: str
    output_dir: Path
    split: str
    valid_split: str
    stake_per_pair: float
    selection_rules: tuple[str, ...]
    v3_score_method: str
    v3_pair_generation_method: str
    v3_partner_weight: float | None
    v6_score_method: str
    v6_pair_generation_method: str
    v6_partner_weight: float | None


def load_experiment_config(path: Path) -> ExperimentConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    experiment = raw_config["experiment"]
    return ExperimentConfig(
        name=experiment["name"],
        model=experiment["model"],
        feature_set=experiment["feature_set"],
        strategy=experiment["strategy"],
        period=experiment["period"],
    )


def load_dataset_build_config(path: Path) -> DatasetBuildConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    dataset = raw_config["dataset"]
    train_end_date = dataset.get("train_end_date")
    valid_end_date = dataset.get("valid_end_date")
    return DatasetBuildConfig(
        name=dataset["name"],
        start_date=date.fromisoformat(dataset["start_date"]),
        end_date=date.fromisoformat(dataset["end_date"]),
        train_end_date=date.fromisoformat(train_end_date) if train_end_date is not None else None,
        valid_end_date=date.fromisoformat(valid_end_date) if valid_end_date is not None else None,
        feature_set=dataset.get("feature_set", "minimal"),
        include_popularity=bool(dataset.get("include_popularity", False)),
        target_name=dataset["target_name"],
        duckdb_path=Path(dataset["duckdb_path"]),
        output_path=Path(dataset["output_path"]),
    )


def load_model_train_config(path: Path) -> ModelTrainConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    training = raw_config["training"]
    feature_columns = tuple(training["feature_columns"])
    feature_transforms = tuple(
        training.get("feature_transforms", ["identity"] * len(feature_columns)),
    )
    return ModelTrainConfig(
        name=training["name"],
        dataset_path=Path(training["dataset_path"]),
        model_name=training["model_name"],
        feature_columns=feature_columns,
        feature_transforms=feature_transforms,
        target_column=training.get("target_column", "target_value"),
        split_column=training.get("split_column", "split"),
        output_dir=Path(training["output_dir"]),
        max_iter=int(training.get("max_iter", 1000)),
        model_params=parse_model_params(training.get("model_params")),
    )


def load_market_feature_comparison_config(path: Path) -> MarketFeatureComparisonConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    comparison = raw_config["comparison"]
    feature_sets = tuple(
        MarketFeatureComparisonFeatureSetConfig(
            name=str(feature_set["name"]),
            dataset_feature_set=str(feature_set["dataset_feature_set"]),
            include_popularity=bool(feature_set.get("include_popularity", False)),
            model_name=str(feature_set.get("model_name", "logistic_regression")),
            feature_columns=tuple(str(value) for value in feature_set["feature_columns"]),
            feature_transforms=tuple(str(value) for value in feature_set["feature_transforms"]),
            max_iter=int(feature_set.get("max_iter", 1000)),
            model_params=parse_model_params(feature_set.get("model_params")),
        )
        for feature_set in comparison["feature_sets"]
    )
    return MarketFeatureComparisonConfig(
        name=str(comparison["name"]),
        duckdb_path=Path(comparison["duckdb_path"]),
        output_dir=Path(comparison["output_dir"]),
        start_date=date.fromisoformat(comparison["start_date"]),
        end_date=date.fromisoformat(comparison["end_date"]),
        target_name=str(comparison["target_name"]),
        backtest_template_config_path=Path(comparison["backtest_template_config_path"]),
        aggregate_selection_score_rule=str(comparison["aggregate_selection_score_rule"]),
        feature_sets=feature_sets,
    )


def load_bet_candidate_eval_config(path: Path) -> BetCandidateEvalConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    evaluation = raw_config["evaluation"]
    thresholds: tuple[float, ...]
    thresholds_raw = evaluation.get("thresholds")
    if thresholds_raw is None:
        thresholds = (float(evaluation["threshold"]),)
    else:
        thresholds = tuple(float(value) for value in thresholds_raw)
    return BetCandidateEvalConfig(
        name=evaluation["name"],
        predictions_path=Path(evaluation["predictions_path"]),
        output_dir=Path(evaluation["output_dir"]),
        thresholds=thresholds,
        split_column=evaluation.get("split_column", "split"),
        target_column=evaluation.get("target_column", "target_value"),
        probability_column=evaluation.get("probability_column", "pred_probability"),
    )


def load_bet_set_diff_analysis_config(path: Path) -> BetSetDiffAnalysisConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    comparisons = tuple(
        BetSetDiffComparisonConfig(
            baseline_feature_set=str(comparison["baseline_feature_set"]),
            variant_feature_set=str(comparison["variant_feature_set"]),
        )
        for comparison in analysis["comparisons"]
    )
    return BetSetDiffAnalysisConfig(
        name=str(analysis["name"]),
        duckdb_path=Path(analysis["duckdb_path"]),
        comparison_root_dir=Path(analysis["comparison_root_dir"]),
        output_dir=Path(analysis["output_dir"]),
        selection_metric=str(analysis.get("selection_metric", "edge")),
        market_prob_method=str(analysis.get("market_prob_method", "oz_place_basis_inverse")),
        split_column=str(analysis.get("split_column", "split")),
        target_column=str(analysis.get("target_column", "target_value")),
        probability_column=str(analysis.get("probability_column", "pred_probability")),
        stake_per_bet=float(analysis.get("stake_per_bet", 100.0)),
        comparisons=comparisons,
    )


def load_ranking_score_diff_config(path: Path) -> RankingScoreDiffConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    ranking_scores_raw = analysis.get("ranking_scores")
    ranking_scores: tuple[str, ...]
    if ranking_scores_raw is None:
        ranking_scores = (
            "edge",
            "edge_times_place_basis_odds",
            "pred_times_place_basis_odds",
            "edge_div_place_basis_odds",
        )
    else:
        ranking_scores = tuple(str(value) for value in ranking_scores_raw)
    return RankingScoreDiffConfig(
        name=str(analysis["name"]),
        duckdb_path=Path(analysis["duckdb_path"]),
        backtest_dir=Path(analysis["backtest_dir"]),
        output_dir=Path(analysis["output_dir"]),
        selection_metric=str(analysis.get("selection_metric", "edge")),
        aggregate_selection_score_rule=str(
            analysis.get(
                "aggregate_selection_score_rule",
                "positive_window_count_then_mean_roi_then_min_roi",
            ),
        ),
        min_bets_valid=(
            int(analysis["min_bets_valid"])
            if "min_bets_valid" in analysis
            else None
        ),
        split_column=str(analysis.get("split_column", "split")),
        target_column=str(analysis.get("target_column", "target_value")),
        probability_column=str(analysis.get("probability_column", "pred_probability")),
        stake_per_bet=float(analysis.get("stake_per_bet", 100.0)),
        ranking_scores=ranking_scores,
    )


def load_ranking_rule_comparison_config(path: Path) -> RankingRuleComparisonConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    base_backtest_config = load_place_backtest_config(Path(analysis["base_backtest_config_path"]))
    ranking_score_rules = tuple(str(value) for value in analysis["ranking_score_rules"])
    return RankingRuleComparisonConfig(
        name=str(analysis["name"]),
        duckdb_path=base_backtest_config.duckdb_path,
        rolling_predictions_path=Path(analysis["rolling_predictions_path"]),
        output_dir=Path(analysis["output_dir"]),
        selection_metric=base_backtest_config.selection_metric,
        market_prob_method=base_backtest_config.market_prob_method,
        thresholds=base_backtest_config.thresholds,
        min_win_odds=base_backtest_config.min_win_odds,
        max_win_odds=base_backtest_config.max_win_odds,
        split_column=base_backtest_config.split_column,
        target_column=base_backtest_config.target_column,
        probability_column=base_backtest_config.probability_column,
        stake_per_bet=base_backtest_config.stake_per_bet,
        aggregate_selection_score_rule=str(
            analysis.get(
                "aggregate_selection_score_rule",
                base_backtest_config.aggregate_selection_score_rule,
            ),
        ),
        min_bets_valid=(
            int(analysis["min_bets_valid"])
            if "min_bets_valid" in analysis
            else base_backtest_config.min_bets_valid
        ),
        popularity_bands=base_backtest_config.popularity_bands,
        place_basis_bands=base_backtest_config.place_basis_bands,
        evaluation_window_pairs=base_backtest_config.evaluation_window_pairs,
        selection_window_groups=base_backtest_config.selection_window_groups,
        ranking_score_rules=ranking_score_rules,
    )


def load_bet_logic_only_config(path: Path) -> BetLogicOnlyConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    base_backtest_config = load_place_backtest_config(Path(analysis["base_backtest_config_path"]))
    provisional_enabled = bool(analysis.get("provisional_proxy_domain_overlay_enabled", True))
    active_run_mode = "candidate_provisional" if provisional_enabled else "fallback_stable"
    return BetLogicOnlyConfig(
        config_path=path,
        name=str(analysis["name"]),
        duckdb_path=base_backtest_config.duckdb_path,
        rolling_predictions_path=Path(analysis["rolling_predictions_path"]),
        reference_label_guard_compare_config_path=Path(
            analysis["reference_label_guard_compare_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        selection_metric=base_backtest_config.selection_metric,
        market_prob_method=base_backtest_config.market_prob_method,
        thresholds=base_backtest_config.thresholds,
        min_win_odds=base_backtest_config.min_win_odds,
        max_win_odds=base_backtest_config.max_win_odds,
        split_column=base_backtest_config.split_column,
        target_column=base_backtest_config.target_column,
        probability_column=base_backtest_config.probability_column,
        stake_per_bet=base_backtest_config.stake_per_bet,
        aggregate_selection_score_rule=str(
            analysis.get(
                "aggregate_selection_score_rule",
                base_backtest_config.aggregate_selection_score_rule,
            ),
        ),
        min_bets_valid=(
            int(analysis["min_bets_valid"])
            if "min_bets_valid" in analysis
            else base_backtest_config.min_bets_valid
        ),
        popularity_bands=base_backtest_config.popularity_bands,
        place_basis_bands=base_backtest_config.place_basis_bands,
        evaluation_window_pairs=base_backtest_config.evaluation_window_pairs,
        selection_window_groups=base_backtest_config.selection_window_groups,
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
        stronger_guard_edge_surcharge=float(analysis.get("stronger_guard_edge_surcharge", 0.01)),
        sizing_tilt_step=float(analysis.get("sizing_tilt_step", 0.2)),
        sizing_tilt_min_multiplier=float(analysis.get("sizing_tilt_min_multiplier", 0.8)),
        sizing_tilt_max_multiplier=float(analysis.get("sizing_tilt_max_multiplier", 1.2)),
        no_bet_guard_sensitivity_levels=tuple(
            float(value)
            for value in analysis.get(
                "no_bet_guard_sensitivity_levels",
                [0.01, 0.02, 0.03],
            )
        ),
        sizing_tilt_max_multiplier_sensitivity_levels=tuple(
            float(value)
            for value in analysis.get(
                "sizing_tilt_max_multiplier_sensitivity_levels",
                [1.1, 1.2, 1.3],
            )
        ),
        initial_bankroll=float(analysis.get("initial_bankroll", 10000.0)),
        provisional_proxy_domain_overlay_enabled=provisional_enabled,
        formal_domain_mapping_confirmed=bool(
            analysis.get("formal_domain_mapping_confirmed", False),
        ),
        active_run_mode=active_run_mode,
    )


def load_reference_strategy_diagnostics_config(path: Path) -> ReferenceStrategyDiagnosticsConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceStrategyDiagnosticsConfig(
        name=str(analysis["name"]),
        ranking_rule_comparison_config_path=Path(analysis["ranking_rule_comparison_config_path"]),
        output_dir=Path(analysis["output_dir"]),
    )


def load_regime_diff_analysis_config(path: Path) -> RegimeDiffAnalysisConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    regimes_raw = analysis["regimes"]
    regimes = tuple(
        (
            str(regime["label"]),
            date.fromisoformat(regime["start_date"]) if "start_date" in regime else None,
            date.fromisoformat(regime["end_date"]) if "end_date" in regime else None,
        )
        for regime in regimes_raw
    )
    return RegimeDiffAnalysisConfig(
        name=str(analysis["name"]),
        reference_strategy_config_path=Path(analysis["reference_strategy_config_path"]),
        output_dir=Path(analysis["output_dir"]),
        regimes=regimes,
        representative_examples_per_regime=int(
            analysis.get("representative_examples_per_regime", 3),
        ),
    )


def load_within_band_regime_diff_config(path: Path) -> WithinBandRegimeDiffConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    regimes = tuple(
        (
            str(regime["label"]),
            date.fromisoformat(regime["start_date"]) if "start_date" in regime else None,
            date.fromisoformat(regime["end_date"]) if "end_date" in regime else None,
        )
        for regime in analysis["regimes"]
    )
    return WithinBandRegimeDiffConfig(
        name=str(analysis["name"]),
        reference_strategy_config_path=Path(analysis["reference_strategy_config_path"]),
        output_dir=Path(analysis["output_dir"]),
        min_popularity=int(analysis["min_popularity"]),
        max_popularity=int(analysis["max_popularity"]),
        min_place_basis_odds=float(analysis["min_place_basis_odds"]),
        max_place_basis_odds=float(analysis["max_place_basis_odds"]),
        regimes=regimes,
        representative_examples_per_regime=int(
            analysis.get("representative_examples_per_regime", 3),
        ),
    )


def load_calibration_drift_config(path: Path) -> CalibrationDriftConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    regimes = tuple(
        (
            str(regime["label"]),
            date.fromisoformat(regime["start_date"]) if "start_date" in regime else None,
            date.fromisoformat(regime["end_date"]) if "end_date" in regime else None,
        )
        for regime in analysis["regimes"]
    )
    return CalibrationDriftConfig(
        name=str(analysis["name"]),
        reference_strategy_config_path=Path(analysis["reference_strategy_config_path"]),
        output_dir=Path(analysis["output_dir"]),
        min_popularity=int(analysis["min_popularity"]),
        max_popularity=int(analysis["max_popularity"]),
        min_place_basis_odds=float(analysis["min_place_basis_odds"]),
        max_place_basis_odds=float(analysis["max_place_basis_odds"]),
        regimes=regimes,
        representative_examples_per_regime=int(
            analysis.get("representative_examples_per_regime", 3),
        ),
    )


def load_reference_guard_compare_config(path: Path) -> ReferenceGuardCompareConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceGuardCompareConfig(
        name=str(analysis["name"]),
        reference_strategy_config_path=Path(analysis["reference_strategy_config_path"]),
        output_dir=Path(analysis["output_dir"]),
        problematic_min_popularity=int(analysis["problematic_min_popularity"]),
        problematic_max_popularity=int(analysis["problematic_max_popularity"]),
        problematic_min_place_basis_odds=float(analysis["problematic_min_place_basis_odds"]),
        problematic_max_place_basis_odds=float(analysis["problematic_max_place_basis_odds"]),
        edge_surcharges=tuple(float(value) for value in analysis["edge_surcharges"]),
        exclude_win_odds_below=(
            float(analysis["exclude_win_odds_below"])
            if "exclude_win_odds_below" in analysis
            else None
        ),
        exclude_edge_below=(
            float(analysis["exclude_edge_below"])
            if "exclude_edge_below" in analysis
            else None
        ),
    )


def load_residual_loss_analysis_config(path: Path) -> ResidualLossAnalysisConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ResidualLossAnalysisConfig(
        name=str(analysis["name"]),
        reference_guard_compare_config_path=Path(analysis["reference_guard_compare_config_path"]),
        output_dir=Path(analysis["output_dir"]),
        baseline_variant=str(analysis.get("baseline_variant", "baseline")),
        guarded_variant=str(analysis.get("guarded_variant", "problematic_band_excluded")),
        representative_examples_per_regime=int(
            analysis.get("representative_examples_per_regime", 3),
        ),
    )


def load_second_guard_selection_config(path: Path) -> SecondGuardSelectionConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return SecondGuardSelectionConfig(
        name=str(analysis["name"]),
        reference_guard_compare_config_path=Path(analysis["reference_guard_compare_config_path"]),
        output_dir=Path(analysis["output_dir"]),
        first_guard_variant=str(
            analysis.get("first_guard_variant", "problematic_band_excluded"),
        ),
        second_guard_variants=tuple(
            str(value) for value in analysis["second_guard_variants"]
        ),
    )


def load_reference_uncertainty_config(path: Path) -> ReferenceUncertaintyConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceUncertaintyConfig(
        name=str(analysis["name"]),
        second_guard_selection_config_path=Path(
            analysis["second_guard_selection_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
        bootstrap_block_unit=str(analysis.get("bootstrap_block_unit", "race_date")),
    )


def load_reference_regime_label_diff_config(path: Path) -> ReferenceRegimeLabelDiffConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    regimes = tuple(
        (
            str(regime["label"]),
            date.fromisoformat(regime["start_date"]) if "start_date" in regime else None,
            date.fromisoformat(regime["end_date"]) if "end_date" in regime else None,
        )
        for regime in analysis["regimes"]
    )
    return ReferenceRegimeLabelDiffConfig(
        name=str(analysis["name"]),
        second_guard_selection_config_path=Path(
            analysis["second_guard_selection_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        regimes=regimes,
        representative_examples_per_group=int(
            analysis.get("representative_examples_per_group", 3),
        ),
    )


def load_reference_label_guard_compare_config(path: Path) -> ReferenceLabelGuardCompareConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceLabelGuardCompareConfig(
        name=str(analysis["name"]),
        second_guard_selection_config_path=Path(
            analysis["second_guard_selection_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        extra_guard_variants=tuple(str(value) for value in analysis["extra_guard_variants"]),
    )


def load_reference_label_guard_null_test_config(
    path: Path,
) -> ReferenceLabelGuardNullTestConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceLabelGuardNullTestConfig(
        name=str(analysis["name"]),
        reference_label_guard_compare_config_path=Path(
            analysis["reference_label_guard_compare_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        null_iterations=int(analysis.get("null_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
        null_modes=tuple(
            str(value)
            for value in analysis.get(
                "null_modes",
                ["current_shuffle", "race_internal_permutation"],
            )
        ),
    )


def load_reference_label_guard_uncertainty_config(
    path: Path,
) -> ReferenceLabelGuardUncertaintyConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceLabelGuardUncertaintyConfig(
        name=str(analysis["name"]),
        reference_label_guard_compare_config_path=Path(
            analysis["reference_label_guard_compare_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
        bootstrap_block_unit=str(analysis.get("bootstrap_block_unit", "race_date")),
    )


def load_reference_stake_sizing_compare_config(
    path: Path,
) -> ReferenceStakeSizingCompareConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceStakeSizingCompareConfig(
        name=str(analysis["name"]),
        reference_label_guard_compare_config_path=Path(
            analysis["reference_label_guard_compare_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        stake_variants=tuple(str(value) for value in analysis["stake_variants"]),
        edge_small_base_stake=float(analysis.get("edge_small_base_stake", 100.0)),
        edge_small_step_stake=float(analysis.get("edge_small_step_stake", 100.0)),
        edge_small_step_edge=float(analysis.get("edge_small_step_edge", 0.04)),
        edge_small_max_stake=float(analysis.get("edge_small_max_stake", 300.0)),
        kelly_bankroll=float(analysis.get("kelly_bankroll", 5000.0)),
        kelly_fraction=float(analysis.get("kelly_fraction", 0.25)),
        kelly_cap_stake=float(analysis.get("kelly_cap_stake", 500.0)),
        per_race_cap_stake=float(analysis.get("per_race_cap_stake", 300.0)),
        per_day_cap_stake=float(analysis.get("per_day_cap_stake", 800.0)),
        drawdown_reduction_threshold=float(
            analysis.get("drawdown_reduction_threshold", 500.0),
        ),
        drawdown_reduction_factor=float(
            analysis.get("drawdown_reduction_factor", 0.5),
        ),
    )


def load_reference_stake_sizing_uncertainty_config(
    path: Path,
) -> ReferenceStakeSizingUncertaintyConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceStakeSizingUncertaintyConfig(
        name=str(analysis["name"]),
        reference_stake_sizing_compare_config_path=Path(
            analysis["reference_stake_sizing_compare_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
        bootstrap_block_unit=str(analysis.get("bootstrap_block_unit", "race_date")),
    )


def load_reference_bankroll_simulation_config(
    path: Path,
) -> ReferenceBankrollSimulationConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceBankrollSimulationConfig(
        name=str(analysis["name"]),
        reference_label_guard_compare_config_path=Path(
            analysis["reference_label_guard_compare_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        stake_variants=tuple(str(value) for value in analysis["stake_variants"]),
        initial_bankrolls=tuple(float(value) for value in analysis["initial_bankrolls"]),
        kelly_fraction=float(analysis.get("kelly_fraction", 0.25)),
        kelly_cap_stake=float(analysis.get("kelly_cap_stake", 500.0)),
        per_race_cap_stake=float(analysis.get("per_race_cap_stake", 300.0)),
        per_day_cap_stake=float(analysis.get("per_day_cap_stake", 800.0)),
        drawdown_reduction_threshold=float(
            analysis.get("drawdown_reduction_threshold", 500.0),
        ),
        drawdown_reduction_factor=float(
            analysis.get("drawdown_reduction_factor", 0.5),
        ),
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
    )


def load_reference_bankroll_simulation_uncertainty_config(
    path: Path,
) -> ReferenceBankrollSimulationUncertaintyConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferenceBankrollSimulationUncertaintyConfig(
        name=str(analysis["name"]),
        reference_bankroll_simulation_config_path=Path(
            analysis["reference_bankroll_simulation_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        stake_variants=tuple(str(value) for value in analysis["stake_variants"]),
        initial_bankrolls=tuple(float(value) for value in analysis["initial_bankrolls"]),
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
        bootstrap_block_unit=str(analysis.get("bootstrap_block_unit", "race_date")),
    )


def load_reference_per_race_cap_sensitivity_config(
    path: Path,
) -> ReferencePerRaceCapSensitivityConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferencePerRaceCapSensitivityConfig(
        name=str(analysis["name"]),
        reference_bankroll_simulation_config_path=Path(
            analysis["reference_bankroll_simulation_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        initial_bankrolls=tuple(float(value) for value in analysis["initial_bankrolls"]),
        per_race_cap_values=tuple(float(value) for value in analysis["per_race_cap_values"]),
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
    )


def load_reference_per_race_cap_drawdown_compare_config(
    path: Path,
) -> ReferencePerRaceCapDrawdownCompareConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferencePerRaceCapDrawdownCompareConfig(
        name=str(analysis["name"]),
        reference_bankroll_simulation_config_path=Path(
            analysis["reference_bankroll_simulation_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        initial_bankrolls=tuple(float(value) for value in analysis["initial_bankrolls"]),
        per_race_cap_values=tuple(float(value) for value in analysis["per_race_cap_values"]),
        bootstrap_iterations=int(analysis.get("bootstrap_iterations", 1000)),
        random_seed=int(analysis.get("random_seed", 42)),
    )


def load_reference_pack_config(path: Path) -> ReferencePackConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return ReferencePackConfig(
        name=str(analysis["name"]),
        reference_label_guard_compare_config_path=Path(
            analysis["reference_label_guard_compare_config_path"],
        ),
        reference_label_guard_uncertainty_config_path=Path(
            analysis["reference_label_guard_uncertainty_config_path"],
        ),
        reference_per_race_cap_sensitivity_config_path=Path(
            analysis["reference_per_race_cap_sensitivity_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        model_name=str(analysis["model_name"]),
        first_guard_name=str(analysis["first_guard_name"]),
        extra_label_guard_name=str(analysis["extra_label_guard_name"]),
        ranking_rule_name=str(analysis["ranking_rule_name"]),
        selection_rule_name=str(analysis["selection_rule_name"]),
        stateful_stake_variant=str(analysis["stateful_stake_variant"]),
        mainline_per_race_cap_stake=float(analysis["mainline_per_race_cap_stake"]),
        standard_initial_bankroll=float(analysis["standard_initial_bankroll"]),
        reference_initial_bankrolls=tuple(
            float(value) for value in analysis["reference_initial_bankrolls"]
        ),
        research_candidates=tuple(str(value) for value in analysis["research_candidates"]),
    )


def load_mainline_block_sensitivity_config(
    path: Path,
) -> MainlineBlockSensitivityConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["analysis"]
    return MainlineBlockSensitivityConfig(
        name=str(analysis["name"]),
        reference_label_guard_uncertainty_config_path=Path(
            analysis["reference_label_guard_uncertainty_config_path"],
        ),
        reference_bankroll_simulation_uncertainty_config_path=Path(
            analysis["reference_bankroll_simulation_uncertainty_config_path"],
        ),
        output_dir=Path(analysis["output_dir"]),
        bootstrap_block_units=tuple(str(value) for value in analysis["bootstrap_block_units"]),
        stateful_stake_variant=str(analysis["stateful_stake_variant"]),
        stateful_initial_bankrolls=tuple(
            float(value) for value in analysis["stateful_initial_bankrolls"]
        ),
    )


def load_place_backtest_config(path: Path) -> PlaceBacktestConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    backtest = raw_config["backtest"]
    thresholds: tuple[float, ...]
    thresholds_raw = backtest.get("thresholds")
    if thresholds_raw is None:
        thresholds = (float(backtest["threshold"]),)
    else:
        thresholds = tuple(float(value) for value in thresholds_raw)
    selection_score_rule = str(backtest.get("selection_score_rule", "roi_then_bets"))
    selection_score_rules_raw = backtest.get("selection_score_rules")
    selection_score_rules: tuple[str, ...]
    if selection_score_rules_raw is None:
        selection_score_rules = (selection_score_rule,)
    else:
        selection_score_rules = tuple(str(value) for value in selection_score_rules_raw)
    aggregate_selection_score_rule = str(
        backtest.get("aggregate_selection_score_rule", "mean_valid_roi"),
    )
    aggregate_selection_score_rules_raw = backtest.get("aggregate_selection_score_rules")
    aggregate_selection_score_rules: tuple[str, ...]
    if aggregate_selection_score_rules_raw is None:
        aggregate_selection_score_rules = (aggregate_selection_score_rule,)
    else:
        aggregate_selection_score_rules = tuple(
            str(value) for value in aggregate_selection_score_rules_raw
        )
    popularity_bands_raw = backtest.get("popularity_bands")
    popularity_bands: tuple[tuple[int | None, int | None], ...]
    if popularity_bands_raw is None:
        popularity_bands = (
            (
                int(backtest["min_popularity"]) if "min_popularity" in backtest else None,
                int(backtest["max_popularity"]) if "max_popularity" in backtest else None,
            ),
        )
    else:
        popularity_bands = tuple(
            (
                int(band["min"]) if "min" in band else None,
                int(band["max"]) if "max" in band else None,
            )
            for band in popularity_bands_raw
        )
    win_odds_bands_raw = backtest.get("win_odds_bands")
    win_odds_bands: tuple[tuple[float | None, float | None], ...]
    if win_odds_bands_raw is None:
        win_odds_bands = (
            (
                float(backtest["min_win_odds"]) if "min_win_odds" in backtest else None,
                float(backtest["max_win_odds"]) if "max_win_odds" in backtest else None,
            ),
        )
    else:
        win_odds_bands = tuple(
            (
                float(band["min"]) if "min" in band else None,
                float(band["max"]) if "max" in band else None,
            )
            for band in win_odds_bands_raw
        )
    place_basis_bands_raw = backtest.get("place_basis_bands")
    place_basis_bands: tuple[tuple[float | None, float | None], ...]
    if place_basis_bands_raw is None:
        place_basis_bands = (
            (
                float(backtest["min_place_basis_odds"])
                if "min_place_basis_odds" in backtest
                else None,
                float(backtest["max_place_basis_odds"])
                if "max_place_basis_odds" in backtest
                else None,
            ),
        )
    else:
        place_basis_bands = tuple(
            (
                float(band["min"]) if "min" in band else None,
                float(band["max"]) if "max" in band else None,
            )
            for band in place_basis_bands_raw
        )
    evaluation_windows_raw = backtest.get("evaluation_windows")
    evaluation_windows: tuple[tuple[str, date | None, date | None], ...]
    if evaluation_windows_raw is None:
        evaluation_windows = (("all", None, None),)
    else:
        evaluation_windows = tuple(
            (
                str(window["label"]),
                date.fromisoformat(window["start_date"]) if "start_date" in window else None,
                date.fromisoformat(window["end_date"]) if "end_date" in window else None,
            )
            for window in evaluation_windows_raw
        )
    evaluation_window_pairs_raw = backtest.get("evaluation_window_pairs")
    evaluation_window_pairs: tuple[
        tuple[str, date | None, date | None, date | None, date | None],
        ...,
    ]
    if evaluation_window_pairs_raw is None:
        evaluation_window_pairs = ()
    else:
        evaluation_window_pairs = tuple(
            (
                str(window["label"]),
                (
                    date.fromisoformat(window["valid_start_date"])
                    if "valid_start_date" in window
                    else None
                ),
                (
                    date.fromisoformat(window["valid_end_date"])
                    if "valid_end_date" in window
                    else None
                ),
                (
                    date.fromisoformat(window["test_start_date"])
                    if "test_start_date" in window
                    else None
                ),
                (
                    date.fromisoformat(window["test_end_date"])
                    if "test_end_date" in window
                    else None
                ),
            )
            for window in evaluation_window_pairs_raw
        )
    selection_window_groups_raw = backtest.get("selection_window_groups")
    selection_window_groups: tuple[tuple[str, tuple[str, ...], str], ...]
    if selection_window_groups_raw is None:
        selection_window_groups = ()
    else:
        selection_window_groups = tuple(
            (
                str(group["label"]),
                tuple(str(value) for value in group["valid_window_labels"]),
                str(group["test_window_label"]),
            )
            for group in selection_window_groups_raw
        )
    min_bets_valid_values_raw = backtest.get("min_bets_valid_values")
    min_bets_valid: int | None
    min_bets_valid_values: tuple[int, ...]
    if min_bets_valid_values_raw is None:
        min_bets_valid = int(backtest["min_bets_valid"]) if "min_bets_valid" in backtest else None
        min_bets_valid_values = ()
    else:
        min_bets_valid = int(backtest["min_bets_valid"]) if "min_bets_valid" in backtest else None
        min_bets_valid_values = tuple(int(value) for value in min_bets_valid_values_raw)
    rolling_retrain = raw_config.get("rolling_retrain")
    rolling_retrain_dataset_path = (
        Path(rolling_retrain["dataset_path"]) if rolling_retrain is not None else None
    )
    rolling_feature_columns = (
        tuple(str(value) for value in rolling_retrain.get("feature_columns", ()))
        if rolling_retrain is not None
        else ()
    )
    rolling_feature_transforms = (
        tuple(str(value) for value in rolling_retrain.get("feature_transforms", ()))
        if rolling_retrain is not None
        else ()
    )
    return PlaceBacktestConfig(
        name=backtest["name"],
        predictions_path=Path(backtest["predictions_path"]),
        duckdb_path=Path(backtest["duckdb_path"]),
        output_dir=Path(backtest["output_dir"]),
        payout_type=str(backtest.get("payout_type", "place")),
        selection_metric=str(backtest.get("selection_metric", "probability")),
        market_prob_method=str(backtest.get("market_prob_method", "inverse_win_odds")),
        thresholds=thresholds,
        min_win_odds=float(backtest["min_win_odds"]) if "min_win_odds" in backtest else None,
        max_win_odds=float(backtest["max_win_odds"]) if "max_win_odds" in backtest else None,
        win_odds_bands=win_odds_bands,
        min_place_basis_odds=(
            float(backtest["min_place_basis_odds"])
            if "min_place_basis_odds" in backtest
            else None
        ),
        max_place_basis_odds=(
            float(backtest["max_place_basis_odds"])
            if "max_place_basis_odds" in backtest
            else None
        ),
        min_popularity=int(backtest["min_popularity"]) if "min_popularity" in backtest else None,
        max_popularity=int(backtest["max_popularity"]) if "max_popularity" in backtest else None,
        split_column=backtest.get("split_column", "split"),
        target_column=backtest.get("target_column", "target_value"),
        probability_column=backtest.get("probability_column", "pred_probability"),
        stake_per_bet=float(backtest.get("stake_per_bet", 100.0)),
        selection_score_rule=selection_score_rule,
        selection_score_rules=selection_score_rules,
        aggregate_selection_score_rule=aggregate_selection_score_rule,
        aggregate_selection_score_rules=aggregate_selection_score_rules,
        min_bets_valid=min_bets_valid,
        min_bets_valid_values=min_bets_valid_values,
        rolling_retrain_dataset_path=rolling_retrain_dataset_path,
        rolling_model_name=(
            str(rolling_retrain.get("model_name", "logistic_regression"))
            if rolling_retrain is not None
            else "logistic_regression"
        ),
        rolling_model_params=parse_model_params(
            rolling_retrain.get("model_params") if rolling_retrain is not None else None
        ),
        rolling_feature_columns=rolling_feature_columns,
        rolling_feature_transforms=rolling_feature_transforms,
        rolling_race_date_column=(
            str(rolling_retrain.get("race_date_column", "race_date"))
            if rolling_retrain is not None
            else "race_date"
        ),
        rolling_max_iter=(
            int(rolling_retrain.get("max_iter", 1000)) if rolling_retrain is not None else 1000
        ),
        popularity_bands=popularity_bands,
        place_basis_bands=place_basis_bands,
        evaluation_windows=evaluation_windows,
        evaluation_window_pairs=evaluation_window_pairs,
        selection_window_groups=selection_window_groups,
        bootstrap_iterations=int(backtest.get("bootstrap_iterations", 1000)),
        random_seed=int(backtest.get("random_seed", 42)),
    )


def load_wide_research_backtest_config(path: Path) -> WideResearchBacktestConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    if "wide_research_v6" in raw_config:
        config_section = "wide_research_v6"
    elif "wide_research_v5" in raw_config:
        config_section = "wide_research_v5"
    elif "wide_research_v4" in raw_config:
        config_section = "wide_research_v4"
    elif "wide_research_v3" in raw_config:
        config_section = "wide_research_v3"
    elif "wide_research_v2" in raw_config:
        config_section = "wide_research_v2"
    else:
        config_section = "wide_research"
    backtest = raw_config[config_section]
    score_methods_raw = backtest.get("score_methods")
    score_methods = (
        tuple(str(value) for value in score_methods_raw)
        if score_methods_raw is not None
        else ("product", "min_prob", "sum_logit")
    )
    pair_generation_methods_raw = backtest.get("pair_generation_methods")
    pair_generation_methods = (
        tuple(str(value) for value in pair_generation_methods_raw)
        if pair_generation_methods_raw is not None
        else ("symmetric_top_k_pairs",)
    )
    candidate_top_k_raw = backtest.get("candidate_top_k_values")
    candidate_top_k_values = (
        tuple(int(value) for value in candidate_top_k_raw)
        if candidate_top_k_raw is not None
        else (2, 3, 4, 5)
    )
    adopted_pair_count_raw = backtest.get("adopted_pair_count_values")
    adopted_pair_count_values = (
        tuple(int(value) for value in adopted_pair_count_raw)
        if adopted_pair_count_raw is not None
        else (1, 2)
    )
    partner_weight_raw = backtest.get("partner_weight_values")
    partner_weight_values = (
        tuple(float(value) for value in partner_weight_raw)
        if partner_weight_raw is not None
        else (0.5,)
    )
    return WideResearchBacktestConfig(
        config_section=config_section,
        name=str(backtest["name"]),
        predictions_path=Path(backtest["predictions_path"]),
        partner_predictions_path=(
            Path(backtest["partner_predictions_path"])
            if "partner_predictions_path" in backtest
            else None
        ),
        hjc_raw_dir=Path(backtest["hjc_raw_dir"]),
        output_dir=Path(backtest["output_dir"]),
        score_methods=score_methods,
        pair_generation_methods=pair_generation_methods,
        candidate_top_k_values=candidate_top_k_values,
        adopted_pair_count_values=adopted_pair_count_values,
        split_column=str(backtest.get("split_column", "split")),
        probability_column=str(backtest.get("probability_column", "pred_probability")),
        partner_probability_column=str(
            backtest.get(
                "partner_probability_column",
                backtest.get("probability_column", "pred_probability"),
            ),
        ),
        window_label_column=str(backtest.get("window_label_column", "window_label")),
        partner_weight_values=partner_weight_values,
        stake_per_pair=float(backtest.get("stake_per_pair", 100.0)),
        bootstrap_iterations=int(backtest.get("bootstrap_iterations", 1000)),
        random_seed=int(backtest.get("random_seed", 7)),
    )


def load_wide_research_diff_config(path: Path) -> WideResearchDiffConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["wide_research_diff"]
    raw_dir_value = analysis.get("raw_dir")
    return WideResearchDiffConfig(
        name=str(analysis["name"]),
        v2_selected_pairs_path=Path(analysis["v2_selected_pairs_path"]),
        v3_selected_pairs_path=Path(analysis["v3_selected_pairs_path"]),
        v2_label=str(analysis.get("v2_label", "v2")),
        v3_label=str(analysis.get("v3_label", "v3")),
        raw_dir=Path(raw_dir_value) if raw_dir_value is not None else None,
        output_dir=Path(analysis["output_dir"]),
        split=str(analysis.get("split", "test")),
        stake_per_pair=float(analysis.get("stake_per_pair", 100.0)),
        representative_example_count=int(analysis.get("representative_example_count", 5)),
        v2_score_method=str(analysis.get("v2_score_method", "pair_model_score")),
        v2_pair_generation_method=str(
            analysis.get("v2_pair_generation_method", "symmetric_top_k_pairs"),
        ),
        v2_partner_weight=(
            float(analysis["v2_partner_weight"]) if "v2_partner_weight" in analysis else None
        ),
        v3_score_method=str(analysis.get("v3_score_method", "pair_model_score")),
        v3_pair_generation_method=str(
            analysis["v3_pair_generation_method"],
        ),
        v3_partner_weight=(
            float(analysis["v3_partner_weight"]) if "v3_partner_weight" in analysis else None
        ),
    )


def load_wide_family_selection_config(path: Path) -> WideFamilySelectionConfig:
    with path.open("rb") as file:
        raw_config = tomllib.load(file)

    analysis = raw_config["wide_family_selection"]
    selection_rules_raw = analysis.get("selection_rules")
    selection_rules = (
        tuple(str(value) for value in selection_rules_raw)
        if selection_rules_raw is not None
        else (
            "total_valid_roi_max",
            "window_win_count_max",
            "mean_valid_roi_minus_std",
        )
    )
    return WideFamilySelectionConfig(
        name=str(analysis["name"]),
        v3_summary_path=Path(analysis["v3_summary_path"]),
        v3_selected_pairs_path=Path(analysis["v3_selected_pairs_path"]),
        v6_summary_path=Path(analysis["v6_summary_path"]),
        v6_selected_pairs_path=Path(analysis["v6_selected_pairs_path"]),
        v3_label=str(analysis.get("v3_label", "v3")),
        v6_label=str(analysis.get("v6_label", "v6")),
        output_dir=Path(analysis["output_dir"]),
        split=str(analysis.get("split", "test")),
        valid_split=str(analysis.get("valid_split", "valid")),
        stake_per_pair=float(analysis.get("stake_per_pair", 100.0)),
        selection_rules=selection_rules,
        v3_score_method=str(analysis.get("v3_score_method", "pair_model_score")),
        v3_pair_generation_method=str(
            analysis.get("v3_pair_generation_method", "anchor_top1_partner_pred_times_place"),
        ),
        v3_partner_weight=(
            float(analysis["v3_partner_weight"]) if "v3_partner_weight" in analysis else None
        ),
        v6_score_method=str(analysis["v6_score_method"]),
        v6_pair_generation_method=str(analysis["v6_pair_generation_method"]),
        v6_partner_weight=(
            float(analysis["v6_partner_weight"]) if "v6_partner_weight" in analysis else None
        ),
    )


def parse_model_params(raw_params: object) -> dict[str, object]:
    if raw_params is None:
        return {}
    if not isinstance(raw_params, dict):
        raise ValueError("model_params must be a table/dict")

    parsed: dict[str, object] = {}
    for key, value in raw_params.items():
        key_str = str(key)
        if value is None:
            parsed[key_str] = None
        elif isinstance(value, bool):
            parsed[key_str] = int(value)
        elif isinstance(value, list | tuple):
            parsed[key_str] = tuple(
                int(item) if isinstance(item, bool) else item
                for item in value
            )
        elif isinstance(value, int | float):
            parsed[key_str] = value
        else:
            raise ValueError(f"unsupported model_params value for {key_str}: {value!r}")
    return parsed
