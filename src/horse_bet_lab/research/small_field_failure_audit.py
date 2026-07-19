from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
from numpy.typing import NDArray
from scipy.special import logit  # type: ignore[import-untyped]

from horse_bet_lab.research.historical_ability_models import (
    HISTORY_NUMERIC_FEATURE_COLUMNS,
    ComparisonDataset,
    FloatArray,
    IntArray,
    ObjectArray,
    race_bootstrap_log_loss_delta,
)
from horse_bet_lab.research.historical_signal_robustness import (
    CROSSFIT_FOLDS,
    MARKET_C,
    OFFSET_C,
    FeatureSubset,
    MarketContext,
    VariantPredictions,
    build_market_context,
    feature_subsets,
    fit_history_variant,
)

AUDIT_COMPLETE = "SMALL_FIELD_FAILURE_CAUSE_AUDITED_DIAGNOSTIC_ONLY"
RESEARCH_ONLY = "DO_NOT_CREATE_EXCLUSION_OR_BETTING_RULE"


@dataclass(frozen=True)
class SmallFieldAuditResult:
    summary: dict[str, Any]
    field_performance_rows: tuple[dict[str, Any], ...]
    decomposition_rows: tuple[dict[str, Any], ...]
    history_profile_rows: tuple[dict[str, Any], ...]
    slot2_training_rows: tuple[dict[str, Any], ...]


def run_small_field_failure_audit(
    dataset: ComparisonDataset,
    *,
    market_c: float = MARKET_C,
    offset_c: float = OFFSET_C,
    crossfit_folds: int = CROSSFIT_FOLDS,
    bootstrap_repetitions: int = 1_000,
    minimum_bootstrap_races: int = 30,
    crossfit_random_seed: int = 652,
    bootstrap_random_seed: int = 653,
) -> SmallFieldAuditResult:
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

    subsets = {subset.name: subset for subset in feature_subsets()}
    required_subsets = {"full_crossfit", "without_current_context", "current_context_only"}
    if not required_subsets.issubset(subsets):
        raise ValueError(f"missing frozen Phase652 subsets: {required_subsets - set(subsets)}")

    periods = (
        ("2024_validation", train_2023, validation_2024),
        ("2025_reused_confirmation", train_2023_2024, confirmation_2025),
    )
    field_performance_rows: list[dict[str, Any]] = []
    decomposition_rows: list[dict[str, Any]] = []
    history_profile_rows: list[dict[str, Any]] = []
    slot2_training_rows: list[dict[str, Any]] = []

    for period_label, train, evaluation in periods:
        market_context = build_market_context(
            train,
            evaluation,
            c_value=market_c,
            folds=crossfit_folds,
            random_seed=crossfit_random_seed,
        )
        variants = {
            "full": fit_history_variant(
                train,
                evaluation,
                market_context,
                subsets["full_crossfit"],
                offset_c=offset_c,
                use_cross_fitted_offset=True,
            ),
            "history_only": fit_history_variant(
                train,
                evaluation,
                market_context,
                subsets["without_current_context"],
                offset_c=offset_c,
                use_cross_fitted_offset=True,
            ),
            "context_only": fit_history_variant(
                train,
                evaluation,
                market_context,
                subsets["current_context_only"],
                offset_c=offset_c,
                use_cross_fitted_offset=True,
            ),
        }
        probability_pairs = _probability_pairs(market_context, variants)
        masks = field_group_masks(evaluation)
        for group_dimension, group_name, mask in masks:
            history_profile_rows.append(
                _history_profile_row(period_label, group_dimension, group_name, evaluation, mask)
            )
            for (
                comparison_name,
                baseline_name,
                candidate_name,
                baseline,
                candidate,
            ) in _bootstrap_comparisons(probability_pairs):
                field_performance_rows.append(
                    _field_performance_row(
                        period_label,
                        group_dimension,
                        group_name,
                        comparison_name,
                        baseline_name,
                        candidate_name,
                        evaluation,
                        mask,
                        baseline,
                        candidate,
                        bootstrap_repetitions=bootstrap_repetitions,
                        minimum_bootstrap_races=minimum_bootstrap_races,
                        random_seed=bootstrap_random_seed,
                    )
                )
            if group_dimension in {"exact_field_size", "field_size_band", "place_slots"}:
                for (
                    comparison_name,
                    baseline_name,
                    candidate_name,
                    baseline,
                    candidate,
                ) in _decomposition_comparisons(probability_pairs):
                    decomposition_rows.append(
                        _decomposition_row(
                            period_label,
                            group_dimension,
                            group_name,
                            comparison_name,
                            baseline_name,
                            candidate_name,
                            evaluation,
                            mask,
                            baseline,
                            candidate,
                        )
                    )

        slot2_training_rows.extend(
            _slot2_training_diagnostic(
                period_label,
                train,
                evaluation,
                market_context,
                variants,
                subsets,
                offset_c=offset_c,
                bootstrap_repetitions=bootstrap_repetitions,
                minimum_bootstrap_races=minimum_bootstrap_races,
                random_seed=bootstrap_random_seed,
            )
        )

    summary = _summary(
        field_performance_rows,
        decomposition_rows,
        slot2_training_rows,
        market_c=market_c,
        offset_c=offset_c,
        crossfit_folds=crossfit_folds,
        crossfit_random_seed=crossfit_random_seed,
        bootstrap_random_seed=bootstrap_random_seed,
        bootstrap_repetitions=bootstrap_repetitions,
        minimum_bootstrap_races=minimum_bootstrap_races,
    )
    return SmallFieldAuditResult(
        summary=summary,
        field_performance_rows=tuple(field_performance_rows),
        decomposition_rows=tuple(decomposition_rows),
        history_profile_rows=tuple(history_profile_rows),
        slot2_training_rows=tuple(slot2_training_rows),
    )


def field_group_masks(
    dataset: ComparisonDataset,
) -> tuple[tuple[str, str, NDArray[np.bool_]], ...]:
    sizes = dataset.active_field_sizes
    slots = dataset.place_slots
    prior_index = HISTORY_NUMERIC_FEATURE_COLUMNS.index("prior_start_count")
    prior_starts = dataset.history_numeric_features[:, prior_index]
    small_fields = (sizes >= 5) & (sizes <= 7)
    return (
        ("exact_field_size", "5", sizes == 5),
        ("exact_field_size", "6", sizes == 6),
        ("exact_field_size", "7", sizes == 7),
        ("field_size_band", "5-7", (sizes >= 5) & (sizes <= 7)),
        ("field_size_band", "8-12", (sizes >= 8) & (sizes <= 12)),
        ("field_size_band", "13+", sizes >= 13),
        ("place_slots", "2", slots == 2),
        ("place_slots", "3", slots == 3),
        ("small_field_prior_starts", "0", small_fields & (prior_starts == 0)),
        (
            "small_field_prior_starts",
            "1-2",
            small_fields & (prior_starts >= 1) & (prior_starts <= 2),
        ),
        (
            "small_field_prior_starts",
            "3-4",
            small_fields & (prior_starts >= 3) & (prior_starts <= 4),
        ),
        ("small_field_prior_starts", "5+", small_fields & (prior_starts >= 5)),
    )


def write_small_field_audit(result: SmallFieldAuditResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "phase653_summary.json", result.summary)
    _write_csv(output_dir / "phase653_field_performance.csv", result.field_performance_rows)
    _write_csv(output_dir / "phase653_error_decomposition.csv", result.decomposition_rows)
    _write_csv(output_dir / "phase653_history_profile.csv", result.history_profile_rows)
    _write_csv(output_dir / "phase653_slot2_training_diagnostic.csv", result.slot2_training_rows)
    (output_dir / "phase653_findings.md").write_text(
        _findings_markdown(result),
        encoding="utf-8",
    )


def _probability_pairs(
    market_context: MarketContext,
    variants: Mapping[str, VariantPredictions],
) -> dict[str, FloatArray]:
    return {
        "M1_market": market_context.evaluation_probabilities,
        "M1C_market": market_context.constrained_evaluation_probabilities,
        "M4_full": variants["full"].offset,
        "M5_full": variants["full"].constrained_offset,
        "M4_history_only": variants["history_only"].offset,
        "M5_history_only": variants["history_only"].constrained_offset,
        "M4_context_only": variants["context_only"].offset,
        "M5_context_only": variants["context_only"].constrained_offset,
    }


def _bootstrap_comparisons(
    probabilities: Mapping[str, FloatArray],
) -> tuple[tuple[str, str, str, FloatArray, FloatArray], ...]:
    return tuple(
        (
            candidate_name.removeprefix("M4_").removeprefix("M5_"),
            baseline_name,
            candidate_name,
            probabilities[baseline_name],
            probabilities[candidate_name],
        )
        for baseline_name, candidate_name in (
            ("M1_market", "M4_full"),
            ("M1C_market", "M5_full"),
            ("M1_market", "M4_history_only"),
            ("M1C_market", "M5_history_only"),
        )
    )


def _decomposition_comparisons(
    probabilities: Mapping[str, FloatArray],
) -> tuple[tuple[str, str, str, FloatArray, FloatArray], ...]:
    return tuple(
        (
            candidate_name.removeprefix("M4_").removeprefix("M5_"),
            baseline_name,
            candidate_name,
            probabilities[baseline_name],
            probabilities[candidate_name],
        )
        for baseline_name, candidate_name in (
            ("M1_market", "M4_full"),
            ("M1C_market", "M5_full"),
            ("M1_market", "M4_history_only"),
            ("M1C_market", "M5_history_only"),
            ("M1_market", "M4_context_only"),
            ("M1C_market", "M5_context_only"),
        )
    )


def _field_performance_row(
    period_label: str,
    group_dimension: str,
    group_name: str,
    comparison_name: str,
    baseline_name: str,
    candidate_name: str,
    dataset: ComparisonDataset,
    mask: NDArray[np.bool_],
    baseline: FloatArray,
    candidate: FloatArray,
    *,
    bootstrap_repetitions: int,
    minimum_bootstrap_races: int,
    random_seed: int,
) -> dict[str, Any]:
    row_count = int(mask.sum())
    race_count = len(set(str(value) for value in dataset.race_keys[mask]))
    if row_count == 0:
        return _empty_performance_row(
            period_label,
            group_dimension,
            group_name,
            comparison_name,
            baseline_name,
            candidate_name,
        )
    targets = dataset.targets[mask]
    baseline_values = baseline[mask]
    candidate_values = candidate[mask]
    row = {
        "period_label": period_label,
        "group_dimension": group_dimension,
        "group_name": group_name,
        "comparison_name": comparison_name,
        "baseline_name": baseline_name,
        "candidate_name": candidate_name,
        "row_count": row_count,
        "race_count": race_count,
        "baseline_log_loss": _mean_binary_log_loss(targets, baseline_values),
        "candidate_log_loss": _mean_binary_log_loss(targets, candidate_values),
        "point_log_loss_delta": _mean_binary_log_loss(targets, candidate_values)
        - _mean_binary_log_loss(targets, baseline_values),
        "ci95_low": "",
        "ci95_high": "",
        "bootstrap_probability_improved": "",
        "bootstrap_repetitions": 0,
    }
    if race_count >= minimum_bootstrap_races:
        bootstrap = race_bootstrap_log_loss_delta(
            period_label=f"{period_label}:{group_dimension}:{group_name}",
            baseline_name=baseline_name,
            candidate_name=candidate_name,
            targets=targets,
            baseline_probabilities=baseline_values,
            candidate_probabilities=candidate_values,
            race_keys=dataset.race_keys[mask],
            repetitions=bootstrap_repetitions,
            random_seed=random_seed,
        )
        row.update(
            {
                "point_log_loss_delta": bootstrap["point_log_loss_delta"],
                "ci95_low": bootstrap["ci95_low"],
                "ci95_high": bootstrap["ci95_high"],
                "bootstrap_probability_improved": bootstrap["bootstrap_probability_improved"],
                "bootstrap_repetitions": bootstrap_repetitions,
            }
        )
    return row


def _empty_performance_row(
    period_label: str,
    group_dimension: str,
    group_name: str,
    comparison_name: str,
    baseline_name: str,
    candidate_name: str,
) -> dict[str, Any]:
    return {
        "period_label": period_label,
        "group_dimension": group_dimension,
        "group_name": group_name,
        "comparison_name": comparison_name,
        "baseline_name": baseline_name,
        "candidate_name": candidate_name,
        "row_count": 0,
        "race_count": 0,
        "baseline_log_loss": "",
        "candidate_log_loss": "",
        "point_log_loss_delta": "",
        "ci95_low": "",
        "ci95_high": "",
        "bootstrap_probability_improved": "",
        "bootstrap_repetitions": 0,
    }


def _decomposition_row(
    period_label: str,
    group_dimension: str,
    group_name: str,
    comparison_name: str,
    baseline_name: str,
    candidate_name: str,
    dataset: ComparisonDataset,
    mask: NDArray[np.bool_],
    baseline: FloatArray,
    candidate: FloatArray,
) -> dict[str, Any]:
    row_count = int(mask.sum())
    if row_count == 0:
        return {
            "period_label": period_label,
            "group_dimension": group_dimension,
            "group_name": group_name,
            "comparison_name": comparison_name,
            "baseline_name": baseline_name,
            "candidate_name": candidate_name,
            "row_count": 0,
        }
    targets = dataset.targets[mask]
    baseline_values = np.clip(baseline[mask], 1e-8, 1.0 - 1e-8)
    candidate_values = np.clip(candidate[mask], 1e-8, 1.0 - 1e-8)
    race_keys = dataset.race_keys[mask]
    place_slots = dataset.place_slots[mask]
    positive_mask = targets == 1
    negative_mask = targets == 0
    logit_adjustments = logit(candidate_values) - logit(baseline_values)
    return {
        "period_label": period_label,
        "group_dimension": group_dimension,
        "group_name": group_name,
        "comparison_name": comparison_name,
        "baseline_name": baseline_name,
        "candidate_name": candidate_name,
        "row_count": row_count,
        "race_count": len(set(str(value) for value in race_keys)),
        "positive_rate": float(targets.mean()),
        "positive_class_loss_delta": _class_loss_delta(
            targets,
            baseline_values,
            candidate_values,
            positive_mask,
        ),
        "negative_class_loss_delta": _class_loss_delta(
            targets,
            baseline_values,
            candidate_values,
            negative_mask,
        ),
        "baseline_positive_mean_probability": float(baseline_values[positive_mask].mean()),
        "candidate_positive_mean_probability": float(candidate_values[positive_mask].mean()),
        "baseline_negative_mean_probability": float(baseline_values[negative_mask].mean()),
        "candidate_negative_mean_probability": float(candidate_values[negative_mask].mean()),
        "mean_probability_shift": float((candidate_values - baseline_values).mean()),
        "mean_absolute_probability_shift": float(np.abs(candidate_values - baseline_values).mean()),
        "mean_race_logit_adjustment": _mean_race_adjustment(logit_adjustments, race_keys),
        "mean_absolute_within_race_logit_adjustment": _mean_absolute_centered_adjustment(
            logit_adjustments,
            race_keys,
        ),
        "baseline_race_probability_sum_mae": _race_sum_mae(
            baseline_values,
            race_keys,
            place_slots,
        ),
        "candidate_race_probability_sum_mae": _race_sum_mae(
            candidate_values,
            race_keys,
            place_slots,
        ),
        "baseline_top_place_slots_capture": _top_k_capture(
            targets,
            baseline_values,
            race_keys,
            place_slots,
        ),
        "candidate_top_place_slots_capture": _top_k_capture(
            targets,
            candidate_values,
            race_keys,
            place_slots,
        ),
    }


def _history_profile_row(
    period_label: str,
    group_dimension: str,
    group_name: str,
    dataset: ComparisonDataset,
    mask: NDArray[np.bool_],
) -> dict[str, Any]:
    row_count = int(mask.sum())
    if row_count == 0:
        return {
            "period_label": period_label,
            "group_dimension": group_dimension,
            "group_name": group_name,
            "row_count": 0,
        }
    prior_index = HISTORY_NUMERIC_FEATURE_COLUMNS.index("prior_start_count")
    days_index = HISTORY_NUMERIC_FEATURE_COLUMNS.index("days_since_last_start")
    last1_index = HISTORY_NUMERIC_FEATURE_COLUMNS.index("last_1_finish_percentile")
    prior_starts = dataset.history_numeric_features[mask, prior_index]
    days_since = dataset.history_numeric_features[mask, days_index]
    last1 = dataset.history_numeric_features[mask, last1_index]
    finite_days = days_since[np.isfinite(days_since)]
    return {
        "period_label": period_label,
        "group_dimension": group_dimension,
        "group_name": group_name,
        "row_count": row_count,
        "race_count": len(set(str(value) for value in dataset.race_keys[mask])),
        "positive_rate": float(dataset.targets[mask].mean()),
        "mean_prior_start_count": float(np.mean(prior_starts)),
        "median_prior_start_count": float(np.median(prior_starts)),
        "zero_prior_start_rate": float(np.mean(prior_starts == 0)),
        "missing_last1_rate": float(np.mean(~np.isfinite(last1))),
        "mean_days_since_last_start": (float(finite_days.mean()) if len(finite_days) else ""),
        "mean_market_popularity": float(dataset.market_features[mask, 2].mean()),
    }


def _slot2_training_diagnostic(
    period_label: str,
    train: ComparisonDataset,
    evaluation: ComparisonDataset,
    market_context: MarketContext,
    pooled_variants: Mapping[str, VariantPredictions],
    subsets: Mapping[str, FeatureSubset],
    *,
    offset_c: float,
    bootstrap_repetitions: int,
    minimum_bootstrap_races: int,
    random_seed: int,
) -> list[dict[str, Any]]:
    train_mask = train.place_slots == 2
    evaluation_mask = evaluation.place_slots == 2
    train_slot2 = train.subset(train_mask)
    evaluation_slot2 = evaluation.subset(evaluation_mask)
    if len(train_slot2.targets) == 0 or len(evaluation_slot2.targets) == 0:
        return []
    slot2_context = MarketContext(
        train_oof_probabilities=market_context.train_oof_probabilities[train_mask],
        train_in_sample_probabilities=market_context.train_in_sample_probabilities[train_mask],
        evaluation_probabilities=market_context.evaluation_probabilities[evaluation_mask],
        constrained_evaluation_probabilities=(
            market_context.constrained_evaluation_probabilities[evaluation_mask]
        ),
    )
    slot2_variants = {
        "full": fit_history_variant(
            train_slot2,
            evaluation_slot2,
            slot2_context,
            subsets["full_crossfit"],
            offset_c=offset_c,
            use_cross_fitted_offset=True,
        ),
        "history_only": fit_history_variant(
            train_slot2,
            evaluation_slot2,
            slot2_context,
            subsets["without_current_context"],
            offset_c=offset_c,
            use_cross_fitted_offset=True,
        ),
    }
    rows: list[dict[str, Any]] = []
    race_count = len(set(str(value) for value in evaluation_slot2.race_keys))
    for feature_scope in ("full", "history_only"):
        pooled = pooled_variants[feature_scope]
        specific = slot2_variants[feature_scope]
        for model_kind, baseline_name, baseline, pooled_values, specific_values in (
            (
                "offset",
                "M1_market",
                slot2_context.evaluation_probabilities,
                pooled.offset[evaluation_mask],
                specific.offset,
            ),
            (
                "constrained_offset",
                "M1C_market",
                slot2_context.constrained_evaluation_probabilities,
                pooled.constrained_offset[evaluation_mask],
                specific.constrained_offset,
            ),
        ):
            scope_bootstrap = None
            if race_count >= minimum_bootstrap_races:
                scope_bootstrap = race_bootstrap_log_loss_delta(
                    period_label=f"{period_label}:place_slots:2:training_scope",
                    baseline_name=f"{feature_scope}_{model_kind}_pooled_all_place_slots",
                    candidate_name=f"{feature_scope}_{model_kind}_slot2_only",
                    targets=evaluation_slot2.targets,
                    baseline_probabilities=pooled_values,
                    candidate_probabilities=specific_values,
                    race_keys=evaluation_slot2.race_keys,
                    repetitions=bootstrap_repetitions,
                    random_seed=random_seed,
                )
            for training_scope, candidate_values in (
                ("pooled_all_place_slots", pooled_values),
                ("slot2_only", specific_values),
            ):
                candidate_name = f"{feature_scope}_{model_kind}_{training_scope}"
                bootstrap = None
                if race_count >= minimum_bootstrap_races:
                    bootstrap = race_bootstrap_log_loss_delta(
                        period_label=f"{period_label}:place_slots:2",
                        baseline_name=baseline_name,
                        candidate_name=candidate_name,
                        targets=evaluation_slot2.targets,
                        baseline_probabilities=baseline,
                        candidate_probabilities=candidate_values,
                        race_keys=evaluation_slot2.race_keys,
                        repetitions=bootstrap_repetitions,
                        random_seed=random_seed,
                    )
                rows.append(
                    {
                        "period_label": period_label,
                        "feature_scope": feature_scope,
                        "model_kind": model_kind,
                        "training_scope": training_scope,
                        "train_row_count": len(train_slot2.targets),
                        "train_race_count": len(set(str(value) for value in train_slot2.race_keys)),
                        "evaluation_row_count": len(evaluation_slot2.targets),
                        "evaluation_race_count": race_count,
                        "baseline_name": baseline_name,
                        "candidate_name": candidate_name,
                        "baseline_log_loss": _mean_binary_log_loss(
                            evaluation_slot2.targets,
                            baseline,
                        ),
                        "candidate_log_loss": _mean_binary_log_loss(
                            evaluation_slot2.targets,
                            candidate_values,
                        ),
                        "point_log_loss_delta": (
                            bootstrap["point_log_loss_delta"]
                            if bootstrap is not None
                            else _mean_binary_log_loss(
                                evaluation_slot2.targets,
                                candidate_values,
                            )
                            - _mean_binary_log_loss(evaluation_slot2.targets, baseline)
                        ),
                        "ci95_low": bootstrap["ci95_low"] if bootstrap is not None else "",
                        "ci95_high": bootstrap["ci95_high"] if bootstrap is not None else "",
                        "bootstrap_probability_improved": (
                            bootstrap["bootstrap_probability_improved"]
                            if bootstrap is not None
                            else ""
                        ),
                        "slot2_vs_pooled_log_loss_delta": (
                            scope_bootstrap["point_log_loss_delta"]
                            if training_scope == "slot2_only" and scope_bootstrap is not None
                            else 0.0
                            if training_scope == "pooled_all_place_slots"
                            else ""
                        ),
                        "slot2_vs_pooled_ci95_low": (
                            scope_bootstrap["ci95_low"]
                            if training_scope == "slot2_only" and scope_bootstrap is not None
                            else ""
                        ),
                        "slot2_vs_pooled_ci95_high": (
                            scope_bootstrap["ci95_high"]
                            if training_scope == "slot2_only" and scope_bootstrap is not None
                            else ""
                        ),
                    }
                )
    return rows


def _summary(
    field_rows: Sequence[dict[str, Any]],
    decomposition_rows: Sequence[dict[str, Any]],
    slot2_rows: Sequence[dict[str, Any]],
    *,
    market_c: float,
    offset_c: float,
    crossfit_folds: int,
    crossfit_random_seed: int,
    bootstrap_random_seed: int,
    bootstrap_repetitions: int,
    minimum_bootstrap_races: int,
) -> dict[str, Any]:
    small_field_rows = [
        row
        for row in field_rows
        if row["group_dimension"] == "field_size_band" and row["group_name"] == "5-7"
    ]
    supported_harm = [
        row for row in small_field_rows if row["ci95_low"] != "" and float(row["ci95_low"]) > 0.0
    ]
    supported_nested_history_harm = [
        row
        for row in field_rows
        if row["group_dimension"] == "small_field_prior_starts"
        and row["ci95_low"] != ""
        and float(row["ci95_low"]) > 0.0
    ]
    small_decomposition = [
        row
        for row in decomposition_rows
        if row["group_dimension"] == "field_size_band"
        and row["group_name"] == "5-7"
        and row["candidate_name"] in {"M5_full", "M5_history_only"}
    ]
    ranking_worse = [
        row
        for row in small_decomposition
        if float(row["candidate_top_place_slots_capture"])
        < float(row["baseline_top_place_slots_capture"])
    ]
    slot2_specific_better = [
        row
        for row in slot2_rows
        if row["training_scope"] == "slot2_only"
        and any(
            peer["period_label"] == row["period_label"]
            and peer["feature_scope"] == row["feature_scope"]
            and peer["model_kind"] == row["model_kind"]
            and peer["training_scope"] == "pooled_all_place_slots"
            and float(row["candidate_log_loss"]) < float(peer["candidate_log_loss"])
            for peer in slot2_rows
        )
    ]
    slot2_supported_improvement = [
        row
        for row in slot2_rows
        if row["training_scope"] == "slot2_only"
        and row["ci95_high"] != ""
        and float(row["ci95_high"]) < 0.0
    ]
    slot2_supported_better_than_pooled = [
        row
        for row in slot2_rows
        if row["training_scope"] == "slot2_only"
        and row["slot2_vs_pooled_ci95_high"] != ""
        and float(row["slot2_vs_pooled_ci95_high"]) < 0.0
    ]
    stable_slot2_pairs = []
    for feature_scope in ("full", "history_only"):
        for model_kind in ("offset", "constrained_offset"):
            rows = [
                row
                for row in slot2_supported_better_than_pooled
                if row["feature_scope"] == feature_scope and row["model_kind"] == model_kind
            ]
            if {row["period_label"] for row in rows} == {
                "2024_validation",
                "2025_reused_confirmation",
            }:
                stable_slot2_pairs.append(
                    {"feature_scope": feature_scope, "model_kind": model_kind}
                )
    return {
        "analysis_version": "phase653_small_field_failure_audit_v1",
        "final_verdict": AUDIT_COMPLETE,
        "operational_recommendation": RESEARCH_ONLY,
        "phase652_hyperparameters_frozen": {
            "market_c": market_c,
            "offset_c": offset_c,
            "crossfit_folds": crossfit_folds,
            "crossfit_random_seed": crossfit_random_seed,
        },
        "period_contract": {
            "2024": "validation under 2023 training",
            "2025": "reused confirmation only; not a fresh holdout",
        },
        "bootstrap_repetitions": bootstrap_repetitions,
        "bootstrap_random_seed": bootstrap_random_seed,
        "minimum_bootstrap_races": minimum_bootstrap_races,
        "supported_small_field_harm": supported_harm,
        "supported_nested_history_harm": supported_nested_history_harm,
        "within_race_ranking_worse_rows": ranking_worse,
        "slot2_specific_training_better_rows": slot2_specific_better,
        "slot2_supported_improvement_rows": slot2_supported_improvement,
        "slot2_supported_better_than_pooled_rows": slot2_supported_better_than_pooled,
        "stable_slot2_specific_recovery_pairs": stable_slot2_pairs,
        "exclusion_rule_created": False,
        "model_or_hyperparameter_selected": False,
        "roi_or_betting_used": False,
    }


def _class_loss_delta(
    targets: IntArray,
    baseline: FloatArray,
    candidate: FloatArray,
    mask: NDArray[np.bool_],
) -> float:
    if not mask.any():
        return float("nan")
    return _mean_binary_log_loss(targets[mask], candidate[mask]) - _mean_binary_log_loss(
        targets[mask],
        baseline[mask],
    )


def _mean_binary_log_loss(targets: IntArray, probabilities: FloatArray) -> float:
    clipped = np.clip(probabilities, 1e-8, 1.0 - 1e-8)
    return float(np.mean(-(targets * np.log(clipped) + (1 - targets) * np.log1p(-clipped))))


def _race_indices(race_keys: ObjectArray) -> tuple[NDArray[np.int64], ...]:
    groups: dict[str, list[int]] = {}
    for index, race_key in enumerate(race_keys):
        groups.setdefault(str(race_key), []).append(index)
    return tuple(np.asarray(indices, dtype=np.int64) for indices in groups.values())


def _mean_race_adjustment(adjustments: FloatArray, race_keys: ObjectArray) -> float:
    return float(np.mean([adjustments[indices].mean() for indices in _race_indices(race_keys)]))


def _mean_absolute_centered_adjustment(
    adjustments: FloatArray,
    race_keys: ObjectArray,
) -> float:
    centered = []
    for indices in _race_indices(race_keys):
        values = adjustments[indices]
        centered.extend(np.abs(values - values.mean()).tolist())
    return float(np.mean(centered))


def _race_sum_mae(
    probabilities: FloatArray,
    race_keys: ObjectArray,
    place_slots: IntArray,
) -> float:
    errors = []
    for indices in _race_indices(race_keys):
        errors.append(abs(float(probabilities[indices].sum()) - float(place_slots[indices[0]])))
    return float(np.mean(errors))


def _top_k_capture(
    targets: IntArray,
    probabilities: FloatArray,
    race_keys: ObjectArray,
    place_slots: IntArray,
) -> float:
    captures = []
    for indices in _race_indices(race_keys):
        slots = int(place_slots[indices[0]])
        order = indices[np.argsort(-probabilities[indices], kind="stable")]
        captures.append(float(targets[order[:slots]].sum()) / slots)
    return float(np.mean(captures))


def _findings_markdown(result: SmallFieldAuditResult) -> str:
    lines = [
        "# Phase653 Small-field Failure Audit",
        "",
        f"Final verdict: `{result.summary['final_verdict']}`",
        "",
        "## Boundary",
        "",
        "- Phase652 models and hyperparameters are frozen.",
        "- 2025 is reused confirmation, not a fresh holdout.",
        "- No exclusion, selection, ROI, threshold, or betting rule is created.",
        "",
        "## Supported small-field harm",
        "",
    ]
    harmful = result.summary["supported_small_field_harm"]
    if harmful:
        for row in harmful:
            lines.append(
                f"- `{row['period_label']}` `{row['candidate_name']}`: "
                f"delta {float(row['point_log_loss_delta']):+.6f}, "
                f"95% CI [{float(row['ci95_low']):+.6f}, "
                f"{float(row['ci95_high']):+.6f}]."
            )
    else:
        lines.append("- No supported harmful row under the frozen contract.")
    lines.extend(
        [
            "",
            "## Diagnostic flags",
            "",
            "- Within-race ranking-worse rows: "
            f"{len(result.summary['within_race_ranking_worse_rows'])}.",
            "- Slot-2-specific training-better rows: "
            f"{len(result.summary['slot2_specific_training_better_rows'])}.",
            "- Supported nested prior-start harm rows: "
            f"{len(result.summary['supported_nested_history_harm'])}.",
            "- Slot-2-specific recovery pairs supported in both periods: "
            f"{len(result.summary['stable_slot2_specific_recovery_pairs'])}.",
            "- Interpretation requires the emitted CSVs; these counts are not adoption rules.",
            "",
        ]
    )
    return "\n".join(lines)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    if not materialized:
        raise ValueError(f"cannot write empty CSV: {path}")
    fieldnames: list[str] = []
    for row in materialized:
        for fieldname in row:
            if fieldname not in fieldnames:
                fieldnames.append(fieldname)
    with path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(materialized)
