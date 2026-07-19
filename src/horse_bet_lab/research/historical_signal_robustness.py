from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
from numpy.typing import NDArray
from sklearn.metrics import log_loss  # type: ignore[import-untyped]
from sklearn.model_selection import StratifiedGroupKFold  # type: ignore[import-untyped]

from horse_bet_lab.research.historical_ability_models import (
    HISTORY_NUMERIC_FEATURE_COLUMNS,
    ComparisonDataset,
    FloatArray,
    HistoryTransformer,
    IntArray,
    NumericTransformer,
    ObjectArray,
    constrain_probabilities_by_race,
    fit_logistic_probability_model,
    fit_offset_probability_model,
    metric_row,
    predict_positive_probability,
    race_bootstrap_log_loss_delta,
)

MARKET_C = 3.0
OFFSET_C = 1.0
CROSSFIT_FOLDS = 5

ROBUSTNESS_CONFIRMED = "HISTORY_SIGNAL_CROSSFIT_ROBUSTNESS_CONFIRMED"
ROBUSTNESS_DIAGNOSTIC_ONLY = "HISTORY_SIGNAL_CROSSFIT_DIAGNOSTIC_ONLY"
ROBUSTNESS_NOT_CONFIRMED = "HISTORY_SIGNAL_CROSSFIT_NOT_CONFIRMED"
RESEARCH_ONLY = "RETAIN_AS_PROBABILITY_DIAGNOSTIC_NOT_BETTING_RULE"

FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "current_context": ("venue_code", "current_distance_m", "card_field_size"),
    "history_depth_recency": ("prior_start_count", "days_since_last_start"),
    "recent_form": (
        "last_1_finish_percentile",
        "last_3_mean_finish_percentile",
        "last_5_mean_finish_percentile",
        "last_3_top3_rate",
        "last_5_top3_rate",
        "last_5_recency_weighted_form",
    ),
    "compatibility": (
        "last_5_distance_compatibility_rate",
        "last_5_venue_compatibility_rate",
    ),
}


@dataclass(frozen=True)
class FeatureSubset:
    name: str
    numeric_indices: tuple[int, ...]
    raw_feature_names: tuple[str, ...]
    use_venue: bool


@dataclass(frozen=True)
class MarketContext:
    train_oof_probabilities: FloatArray
    train_in_sample_probabilities: FloatArray
    evaluation_probabilities: FloatArray
    constrained_evaluation_probabilities: FloatArray


@dataclass(frozen=True)
class VariantPredictions:
    offset: FloatArray
    constrained_offset: FloatArray
    encoded_feature_count: int


@dataclass(frozen=True)
class RobustnessResult:
    summary: dict[str, Any]
    metrics: tuple[dict[str, Any], ...]
    ablation_rows: tuple[dict[str, Any], ...]
    bootstrap_rows: tuple[dict[str, Any], ...]
    subgroup_rows: tuple[dict[str, Any], ...]


def feature_subsets() -> tuple[FeatureSubset, ...]:
    all_raw_features = ("venue_code", *HISTORY_NUMERIC_FEATURE_COLUMNS)
    subsets = [
        _feature_subset("full_crossfit", excluded=()),
        _feature_subset("without_current_context", excluded=("current_context",)),
        _feature_subset(
            "current_context_only",
            excluded=("history_depth_recency", "recent_form", "compatibility"),
        ),
        _feature_subset("without_history_depth_recency", excluded=("history_depth_recency",)),
        _feature_subset("without_recent_form", excluded=("recent_form",)),
        _feature_subset("without_compatibility", excluded=("compatibility",)),
    ]
    if set(FEATURE_GROUPS).difference(
        {"current_context", "history_depth_recency", "recent_form", "compatibility"}
    ):
        raise AssertionError("unexpected feature group")
    grouped_features = {
        feature for group_features in FEATURE_GROUPS.values() for feature in group_features
    }
    if grouped_features != set(all_raw_features):
        raise ValueError(
            "feature groups must cover the Phase651 history contract exactly: "
            f"missing={set(all_raw_features) - grouped_features}, "
            f"extra={grouped_features - set(all_raw_features)}"
        )
    return tuple(subsets)


def cross_fitted_market_probabilities(
    dataset: ComparisonDataset,
    *,
    c_value: float = MARKET_C,
    folds: int = CROSSFIT_FOLDS,
    random_seed: int = 652,
) -> FloatArray:
    unique_races = len(set(str(value) for value in dataset.race_keys))
    if unique_races < folds:
        raise ValueError(f"cross-fit requires at least {folds} races, got {unique_races}")
    splitter = StratifiedGroupKFold(
        n_splits=folds,
        shuffle=True,
        random_state=random_seed,
    )
    predictions = np.full(len(dataset.targets), np.nan, dtype=np.float64)
    for train_indices, validation_indices in splitter.split(
        dataset.market_features,
        dataset.targets,
        groups=dataset.race_keys,
    ):
        transformer = NumericTransformer.fit(dataset.market_features[train_indices])
        train_values = transformer.transform(dataset.market_features[train_indices])
        validation_values = transformer.transform(dataset.market_features[validation_indices])
        model = fit_logistic_probability_model(
            train_values,
            dataset.targets[train_indices],
            c_value,
        )
        predictions[validation_indices] = predict_positive_probability(model, validation_values)
    if np.isnan(predictions).any():
        raise RuntimeError("cross-fit did not predict every training row")
    return predictions


def run_signal_robustness(
    dataset: ComparisonDataset,
    *,
    market_c: float = MARKET_C,
    offset_c: float = OFFSET_C,
    crossfit_folds: int = CROSSFIT_FOLDS,
    bootstrap_repetitions: int = 2_000,
    subgroup_bootstrap_repetitions: int = 500,
    minimum_subgroup_rows: int = 200,
    random_seed: int = 652,
) -> RobustnessResult:
    years = dataset.years
    train_2023 = dataset.subset(years == 2023)
    validation_2024 = dataset.subset(years == 2024)
    train_2023_2024 = dataset.subset(np.isin(years, [2023, 2024]))
    confirmation_2025 = dataset.subset(years == 2025)
    if (
        min(
            len(train_2023.targets),
            len(validation_2024.targets),
            len(confirmation_2025.targets),
        )
        == 0
    ):
        raise ValueError("2023, 2024, and 2025 rows are required")

    periods = (
        (
            "2024_validation",
            train_2023,
            validation_2024,
            build_market_context(
                train_2023,
                validation_2024,
                c_value=market_c,
                folds=crossfit_folds,
                random_seed=random_seed,
            ),
        ),
        (
            "2025_reused_confirmation",
            train_2023_2024,
            confirmation_2025,
            build_market_context(
                train_2023_2024,
                confirmation_2025,
                c_value=market_c,
                folds=crossfit_folds,
                random_seed=random_seed,
            ),
        ),
    )

    metrics: list[dict[str, Any]] = []
    ablation_rows: list[dict[str, Any]] = []
    bootstrap_rows: list[dict[str, Any]] = []
    subgroup_rows: list[dict[str, Any]] = []

    full_predictions_by_period: dict[
        str, tuple[ComparisonDataset, MarketContext, VariantPredictions]
    ] = {}
    for period_label, train, evaluation, market_context in periods:
        metrics.extend(
            (
                metric_row(
                    period_label,
                    "M1_market",
                    evaluation,
                    market_context.evaluation_probabilities,
                ),
                metric_row(
                    period_label,
                    "M1C_race_constrained_market",
                    evaluation,
                    market_context.constrained_evaluation_probabilities,
                ),
            )
        )
        full_subset = _feature_subset("full_crossfit", excluded=())
        full_crossfit = fit_history_variant(
            train,
            evaluation,
            market_context,
            full_subset,
            offset_c=offset_c,
            use_cross_fitted_offset=True,
        )
        full_in_sample = fit_history_variant(
            train,
            evaluation,
            market_context,
            full_subset,
            offset_c=offset_c,
            use_cross_fitted_offset=False,
        )
        full_predictions_by_period[period_label] = (evaluation, market_context, full_crossfit)
        metrics.extend(
            (
                metric_row(
                    period_label,
                    "M4_crossfit_full",
                    evaluation,
                    full_crossfit.offset,
                ),
                metric_row(
                    period_label,
                    "M5_crossfit_full",
                    evaluation,
                    full_crossfit.constrained_offset,
                ),
                metric_row(
                    period_label,
                    "M4_in_sample_offset_reference",
                    evaluation,
                    full_in_sample.offset,
                ),
            )
        )
        bootstrap_rows.extend(
            (
                race_bootstrap_log_loss_delta(
                    period_label=period_label,
                    baseline_name="M1_market",
                    candidate_name="M4_crossfit_full",
                    targets=evaluation.targets,
                    baseline_probabilities=market_context.evaluation_probabilities,
                    candidate_probabilities=full_crossfit.offset,
                    race_keys=evaluation.race_keys,
                    repetitions=bootstrap_repetitions,
                    random_seed=random_seed,
                ),
                race_bootstrap_log_loss_delta(
                    period_label=period_label,
                    baseline_name="M1C_race_constrained_market",
                    candidate_name="M5_crossfit_full",
                    targets=evaluation.targets,
                    baseline_probabilities=market_context.constrained_evaluation_probabilities,
                    candidate_probabilities=full_crossfit.constrained_offset,
                    race_keys=evaluation.race_keys,
                    repetitions=bootstrap_repetitions,
                    random_seed=random_seed,
                ),
                race_bootstrap_log_loss_delta(
                    period_label=period_label,
                    baseline_name="M4_in_sample_offset_reference",
                    candidate_name="M4_crossfit_full",
                    targets=evaluation.targets,
                    baseline_probabilities=full_in_sample.offset,
                    candidate_probabilities=full_crossfit.offset,
                    race_keys=evaluation.race_keys,
                    repetitions=bootstrap_repetitions,
                    random_seed=random_seed,
                ),
            )
        )

        full_offset_log_loss = _log_loss(evaluation.targets, full_crossfit.offset)
        full_constrained_log_loss = _log_loss(
            evaluation.targets,
            full_crossfit.constrained_offset,
        )
        for subset in feature_subsets():
            if subset.name == "full_crossfit":
                predictions = full_crossfit
            else:
                predictions = fit_history_variant(
                    train,
                    evaluation,
                    market_context,
                    subset,
                    offset_c=offset_c,
                    use_cross_fitted_offset=True,
                )
            offset_log_loss = _log_loss(evaluation.targets, predictions.offset)
            constrained_log_loss = _log_loss(
                evaluation.targets,
                predictions.constrained_offset,
            )
            ablation_rows.extend(
                (
                    _ablation_row(
                        period_label,
                        subset,
                        "offset",
                        predictions.encoded_feature_count,
                        offset_log_loss,
                        full_offset_log_loss,
                        _log_loss(
                            evaluation.targets,
                            market_context.evaluation_probabilities,
                        ),
                    ),
                    _ablation_row(
                        period_label,
                        subset,
                        "constrained_offset",
                        predictions.encoded_feature_count,
                        constrained_log_loss,
                        full_constrained_log_loss,
                        _log_loss(
                            evaluation.targets,
                            market_context.constrained_evaluation_probabilities,
                        ),
                    ),
                )
            )
            if subset.name in {"without_current_context", "current_context_only"}:
                signal_label = (
                    "history_only" if subset.name == "without_current_context" else "context_only"
                )
                bootstrap_rows.extend(
                    (
                        race_bootstrap_log_loss_delta(
                            period_label=period_label,
                            baseline_name="M1_market",
                            candidate_name=f"M4_crossfit_{signal_label}",
                            targets=evaluation.targets,
                            baseline_probabilities=market_context.evaluation_probabilities,
                            candidate_probabilities=predictions.offset,
                            race_keys=evaluation.race_keys,
                            repetitions=bootstrap_repetitions,
                            random_seed=random_seed,
                        ),
                        race_bootstrap_log_loss_delta(
                            period_label=period_label,
                            baseline_name="M1C_race_constrained_market",
                            candidate_name=f"M5_crossfit_{signal_label}",
                            targets=evaluation.targets,
                            baseline_probabilities=(
                                market_context.constrained_evaluation_probabilities
                            ),
                            candidate_probabilities=predictions.constrained_offset,
                            race_keys=evaluation.race_keys,
                            repetitions=bootstrap_repetitions,
                            random_seed=random_seed,
                        ),
                    )
                )

    for period_label, (
        evaluation,
        market_context,
        predictions,
    ) in full_predictions_by_period.items():
        for model_name, baseline_name, baseline, candidate in (
            (
                "M4_crossfit_full",
                "M1_market",
                market_context.evaluation_probabilities,
                predictions.offset,
            ),
            (
                "M5_crossfit_full",
                "M1C_race_constrained_market",
                market_context.constrained_evaluation_probabilities,
                predictions.constrained_offset,
            ),
        ):
            for dimension, subgroup, mask in _subgroup_masks(evaluation):
                if (
                    int(mask.sum()) < minimum_subgroup_rows
                    or len(np.unique(evaluation.targets[mask])) < 2
                ):
                    continue
                bootstrap = race_bootstrap_log_loss_delta(
                    period_label=f"{period_label}:{dimension}:{subgroup}",
                    baseline_name=baseline_name,
                    candidate_name=model_name,
                    targets=evaluation.targets[mask],
                    baseline_probabilities=baseline[mask],
                    candidate_probabilities=candidate[mask],
                    race_keys=evaluation.race_keys[mask],
                    repetitions=subgroup_bootstrap_repetitions,
                    random_seed=random_seed,
                )
                subgroup_rows.append(
                    {
                        "period_label": period_label,
                        "model_name": model_name,
                        "baseline_name": baseline_name,
                        "dimension": dimension,
                        "subgroup": subgroup,
                        "row_count": int(mask.sum()),
                        "race_count": bootstrap["race_count"],
                        "baseline_log_loss": _log_loss(evaluation.targets[mask], baseline[mask]),
                        "candidate_log_loss": _log_loss(evaluation.targets[mask], candidate[mask]),
                        "point_log_loss_delta": bootstrap["point_log_loss_delta"],
                        "ci95_low": bootstrap["ci95_low"],
                        "ci95_high": bootstrap["ci95_high"],
                        "bootstrap_probability_improved": bootstrap[
                            "bootstrap_probability_improved"
                        ],
                    }
                )

    final_verdict, evidence = _robustness_verdict(bootstrap_rows)
    supported_failure_pockets = [row for row in subgroup_rows if float(row["ci95_low"]) > 0.0]
    summary: dict[str, Any] = {
        "analysis_version": "phase652_historical_signal_robustness_v1",
        "final_verdict": final_verdict,
        "phase651_hyperparameters_frozen": {
            "market_c": market_c,
            "offset_c": offset_c,
        },
        "crossfit_contract": {
            "folds": crossfit_folds,
            "group": "race_key",
            "splitter": "StratifiedGroupKFold",
            "training_offset_predictions_are_out_of_fold": True,
        },
        "period_contract": {
            "2024": "validation under 2023 training",
            "2025": "reused confirmation only; not a fresh holdout",
        },
        "feature_groups": FEATURE_GROUPS,
        "feature_ablation_used_for_selection": False,
        "roi_or_betting_used": False,
        "operational_recommendation": RESEARCH_ONLY,
        "uniform_subgroup_improvement_supported": bool(subgroup_rows)
        and all(float(row["ci95_high"]) < 0.0 for row in subgroup_rows),
        "supported_failure_pockets": supported_failure_pockets,
        "bootstrap_repetitions": bootstrap_repetitions,
        "subgroup_bootstrap_repetitions": subgroup_bootstrap_repetitions,
        "minimum_subgroup_rows": minimum_subgroup_rows,
        "verdict_evidence": evidence,
        "historical_residual_hypothesis_requires": (
            "history-only residual improvement against matched market baselines"
        ),
    }
    return RobustnessResult(
        summary=summary,
        metrics=tuple(metrics),
        ablation_rows=tuple(ablation_rows),
        bootstrap_rows=tuple(bootstrap_rows),
        subgroup_rows=tuple(subgroup_rows),
    )


def write_robustness_result(result: RobustnessResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "phase652_summary.json", result.summary)
    _write_csv(output_dir / "phase652_metrics.csv", result.metrics)
    _write_csv(output_dir / "phase652_ablation.csv", result.ablation_rows)
    _write_csv(output_dir / "phase652_bootstrap.csv", result.bootstrap_rows)
    _write_csv(output_dir / "phase652_subgroup_stability.csv", result.subgroup_rows)
    (output_dir / "phase652_findings.md").write_text(_findings_markdown(result))


def build_market_context(
    train: ComparisonDataset,
    evaluation: ComparisonDataset,
    *,
    c_value: float,
    folds: int,
    random_seed: int,
) -> MarketContext:
    oof = cross_fitted_market_probabilities(
        train,
        c_value=c_value,
        folds=folds,
        random_seed=random_seed,
    )
    transformer = NumericTransformer.fit(train.market_features)
    train_values = transformer.transform(train.market_features)
    evaluation_values = transformer.transform(evaluation.market_features)
    model = fit_logistic_probability_model(train_values, train.targets, c_value)
    train_in_sample = predict_positive_probability(model, train_values)
    evaluation_probabilities = predict_positive_probability(model, evaluation_values)
    return MarketContext(
        train_oof_probabilities=oof,
        train_in_sample_probabilities=train_in_sample,
        evaluation_probabilities=evaluation_probabilities,
        constrained_evaluation_probabilities=constrain_probabilities_by_race(
            evaluation_probabilities,
            evaluation.race_keys,
            evaluation.place_slots,
        ),
    )


def fit_history_variant(
    train: ComparisonDataset,
    evaluation: ComparisonDataset,
    market_context: MarketContext,
    subset: FeatureSubset,
    *,
    offset_c: float,
    use_cross_fitted_offset: bool,
) -> VariantPredictions:
    train_numeric = train.history_numeric_features[:, subset.numeric_indices]
    evaluation_numeric = evaluation.history_numeric_features[:, subset.numeric_indices]
    train_venue = _venue_values(train.venue_codes, subset.use_venue)
    evaluation_venue = _venue_values(evaluation.venue_codes, subset.use_venue)
    transformer = HistoryTransformer.fit(train_numeric, train_venue)
    train_values = transformer.transform(train_numeric, train_venue)
    evaluation_values = transformer.transform(evaluation_numeric, evaluation_venue)
    offsets = (
        market_context.train_oof_probabilities
        if use_cross_fitted_offset
        else market_context.train_in_sample_probabilities
    )
    model = fit_offset_probability_model(
        train_values,
        train.targets,
        offsets,
        offset_c,
    )
    probabilities = model.predict(
        evaluation_values,
        market_context.evaluation_probabilities,
    )
    return VariantPredictions(
        offset=probabilities,
        constrained_offset=constrain_probabilities_by_race(
            probabilities,
            evaluation.race_keys,
            evaluation.place_slots,
        ),
        encoded_feature_count=train_values.shape[1],
    )


def _feature_subset(name: str, *, excluded: Sequence[str]) -> FeatureSubset:
    excluded_features = {
        feature for group_name in excluded for feature in FEATURE_GROUPS[group_name]
    }
    raw_features = tuple(
        feature
        for feature in ("venue_code", *HISTORY_NUMERIC_FEATURE_COLUMNS)
        if feature not in excluded_features
    )
    numeric_indices = tuple(
        index
        for index, feature in enumerate(HISTORY_NUMERIC_FEATURE_COLUMNS)
        if feature in raw_features
    )
    return FeatureSubset(
        name=name,
        numeric_indices=numeric_indices,
        raw_feature_names=raw_features,
        use_venue="venue_code" in raw_features,
    )


def _venue_values(values: ObjectArray, use_venue: bool) -> ObjectArray:
    if use_venue:
        return values
    return np.asarray(["__removed__"] * len(values), dtype=object)


def _ablation_row(
    period_label: str,
    subset: FeatureSubset,
    model_kind: str,
    encoded_feature_count: int,
    candidate_log_loss: float,
    full_log_loss: float,
    market_log_loss: float,
) -> dict[str, Any]:
    return {
        "period_label": period_label,
        "variant_name": subset.name,
        "model_kind": model_kind,
        "raw_feature_count": len(subset.raw_feature_names),
        "encoded_feature_count": encoded_feature_count,
        "raw_features": "|".join(subset.raw_feature_names),
        "log_loss": candidate_log_loss,
        "delta_vs_full": candidate_log_loss - full_log_loss,
        "delta_vs_matched_market": candidate_log_loss - market_log_loss,
    }


def _subgroup_masks(
    dataset: ComparisonDataset,
) -> tuple[tuple[str, str, NDArray[np.bool_]], ...]:
    prior_index = HISTORY_NUMERIC_FEATURE_COLUMNS.index("prior_start_count")
    prior_starts = dataset.history_numeric_features[:, prior_index]
    popularity = dataset.market_features[:, 2]
    months = np.asarray([value.month for value in dataset.race_dates], dtype=np.int64)
    return (
        ("prior_start_count", "0", prior_starts == 0),
        ("prior_start_count", "1-2", (prior_starts >= 1) & (prior_starts <= 2)),
        ("prior_start_count", "3-4", (prior_starts >= 3) & (prior_starts <= 4)),
        ("prior_start_count", "5+", prior_starts >= 5),
        ("active_field_size", "5-7", dataset.active_field_sizes <= 7),
        (
            "active_field_size",
            "8-12",
            (dataset.active_field_sizes >= 8) & (dataset.active_field_sizes <= 12),
        ),
        ("active_field_size", "13+", dataset.active_field_sizes >= 13),
        ("market_popularity", "1-3", popularity <= 3),
        ("market_popularity", "4-8", (popularity >= 4) & (popularity <= 8)),
        ("market_popularity", "9+", popularity >= 9),
        ("half_year", "H1", months <= 6),
        ("half_year", "H2", months >= 7),
    )


def _robustness_verdict(
    bootstrap_rows: Sequence[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    evidence: list[dict[str, Any]] = []
    evidence_by_candidate: dict[str, dict[str, Any]] = {}
    for baseline_name, candidate_name in (
        ("M1_market", "M4_crossfit_full"),
        ("M1C_race_constrained_market", "M5_crossfit_full"),
        ("M1_market", "M4_crossfit_history_only"),
        ("M1C_race_constrained_market", "M5_crossfit_history_only"),
        ("M1_market", "M4_crossfit_context_only"),
        ("M1C_race_constrained_market", "M5_crossfit_context_only"),
    ):
        rows = [
            row
            for row in bootstrap_rows
            if row["baseline_name"] == baseline_name and row["candidate_name"] == candidate_name
        ]
        point_both = len(rows) == 2 and all(
            float(row["point_log_loss_delta"]) < 0.0 for row in rows
        )
        robust_both = point_both and all(float(row["ci95_high"]) < 0.0 for row in rows)
        candidate_evidence = {
            "baseline_name": baseline_name,
            "candidate_name": candidate_name,
            "point_improvement_in_both_periods": point_both,
            "bootstrap_ci_below_zero_in_both_periods": robust_both,
        }
        evidence.append(candidate_evidence)
        evidence_by_candidate[candidate_name] = candidate_evidence
    full_point = all(
        evidence_by_candidate[name]["point_improvement_in_both_periods"]
        for name in ("M4_crossfit_full", "M5_crossfit_full")
    )
    history_point = all(
        evidence_by_candidate[name]["point_improvement_in_both_periods"]
        for name in ("M4_crossfit_history_only", "M5_crossfit_history_only")
    )
    full_robust = all(
        evidence_by_candidate[name]["bootstrap_ci_below_zero_in_both_periods"]
        for name in ("M4_crossfit_full", "M5_crossfit_full")
    )
    history_robust = all(
        evidence_by_candidate[name]["bootstrap_ci_below_zero_in_both_periods"]
        for name in ("M4_crossfit_history_only", "M5_crossfit_history_only")
    )
    if full_robust and history_robust:
        return ROBUSTNESS_CONFIRMED, evidence
    if full_point and history_point:
        return ROBUSTNESS_DIAGNOSTIC_ONLY, evidence
    return ROBUSTNESS_NOT_CONFIRMED, evidence


def _log_loss(targets: IntArray, probabilities: FloatArray) -> float:
    return float(log_loss(targets, probabilities, labels=[0, 1]))


def _findings_markdown(result: RobustnessResult) -> str:
    metrics = {(str(row["period_label"]), str(row["model_name"])): row for row in result.metrics}
    lines = [
        "# Phase652 Historical Signal Robustness",
        "",
        f"Final verdict: `{result.summary['final_verdict']}`",
        "",
        "## Fixed-model log loss",
        "",
        "| Model | 2024 validation | 2025 reused confirmation |",
        "|---|---:|---:|",
    ]
    for model_name in (
        "M1_market",
        "M1C_race_constrained_market",
        "M4_crossfit_full",
        "M5_crossfit_full",
        "M4_in_sample_offset_reference",
    ):
        lines.append(
            f"| `{model_name}` | "
            f"{metrics[('2024_validation', model_name)]['log_loss']:.6f} | "
            f"{metrics[('2025_reused_confirmation', model_name)]['log_loss']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Overall paired-bootstrap deltas",
            "",
            "| Candidate vs baseline | 2024 validation | 2025 reused confirmation |",
            "|---|---:|---:|",
        ]
    )
    bootstrap = {
        (
            str(row["period_label"]),
            str(row["baseline_name"]),
            str(row["candidate_name"]),
        ): row
        for row in result.bootstrap_rows
    }
    for baseline_name, candidate_name in (
        ("M1_market", "M4_crossfit_full"),
        ("M1C_race_constrained_market", "M5_crossfit_full"),
        ("M1_market", "M4_crossfit_history_only"),
        ("M1C_race_constrained_market", "M5_crossfit_history_only"),
        ("M1_market", "M4_crossfit_context_only"),
        ("M1C_race_constrained_market", "M5_crossfit_context_only"),
        ("M4_in_sample_offset_reference", "M4_crossfit_full"),
    ):
        row_2024 = bootstrap[("2024_validation", baseline_name, candidate_name)]
        row_2025 = bootstrap[("2025_reused_confirmation", baseline_name, candidate_name)]
        lines.append(
            f"| `{candidate_name}` vs `{baseline_name}` | "
            f"{float(row_2024['point_log_loss_delta']):+.6f} | "
            f"{float(row_2025['point_log_loss_delta']):+.6f} |"
        )
    lines.extend(
        [
            "",
            "## Supported failure pockets",
            "",
        ]
    )
    failure_pockets = result.summary["supported_failure_pockets"]
    if failure_pockets:
        for row in failure_pockets:
            lines.append(
                "- "
                f"`{row['period_label']}` `{row['model_name']}` "
                f"{row['dimension']}={row['subgroup']}: "
                f"delta {float(row['point_log_loss_delta']):+.6f}, "
                f"95% CI [{float(row['ci95_low']):+.6f}, "
                f"{float(row['ci95_high']):+.6f}]."
            )
    else:
        lines.append("- None detected under the configured subgroup threshold.")
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "- Phase651 hyperparameters are frozen.",
            "- 2025 has already been observed and is not presented as a fresh holdout.",
            "- Ablations and subgroups are diagnostic; none selects a betting rule.",
            "- ROI, payout, stake, and threshold logic remain out of scope.",
            f"- Operational recommendation: `{result.summary['operational_recommendation']}`.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    if not materialized:
        raise ValueError(f"cannot write empty CSV: {path}")
    with path.open("w", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=list(materialized[0]))
        writer.writeheader()
        writer.writerows(materialized)
