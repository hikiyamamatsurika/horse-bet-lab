from __future__ import annotations

import csv
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Sequence

import numpy as np
from numpy.typing import NDArray
from sklearn.metrics import log_loss  # type: ignore[import-untyped]

from horse_bet_lab.research.historical_ability_models import (
    DEFAULT_C_GRID,
    ComparisonDataset,
    InputAudit,
    NumericTransformer,
    constrain_probabilities_by_race,
    fit_logistic_probability_model,
    metric_row,
    predict_positive_probability,
    race_bootstrap_log_loss_delta,
)

LEGACY_RESULT_POPULARITY = "legacy_result_popularity"
NO_POPULARITY = "no_popularity"
DECISION_TIME_WIN_RANK = "decision_time_win_rank"
SAFE_VARIANTS = (NO_POPULARITY, DECISION_TIME_WIN_RANK)
ALL_VARIANTS = (LEGACY_RESULT_POPULARITY, *SAFE_VARIANTS)

FINAL_VERDICT = "DECISION_TIME_MARKET_FEATURE_CANDIDATE_SELECTED"
SEMANTIC_VERDICT = "RESULT_SIDE_POPULARITY_NOT_PROSPECTIVE_SAFE"

FloatArray = NDArray[np.float64]


@dataclass(frozen=True)
class PopularityCarrierComparisonResult:
    summary: dict[str, Any]
    metrics: tuple[dict[str, Any], ...]
    bootstrap_rows: tuple[dict[str, Any], ...]
    rank_agreement_rows: tuple[dict[str, Any], ...]
    hyperparameter_rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class _VariantFit:
    selected_c: float
    validation_raw: FloatArray
    validation_constrained: FloatArray
    holdout_raw: FloatArray
    holdout_constrained: FloatArray
    metrics: tuple[dict[str, Any], ...]
    hyperparameters: tuple[dict[str, Any], ...]


def market_variant_dataset(dataset: ComparisonDataset, variant: str) -> ComparisonDataset:
    if variant not in ALL_VARIANTS:
        raise ValueError(f"unsupported market variant: {variant}")
    if dataset.market_features.ndim != 2 or dataset.market_features.shape[1] != 3:
        raise ValueError(
            "Phase656 expects Phase651 market features in "
            "(log win odds, log place-basis odds, legacy popularity) order"
        )
    base_market = np.asarray(dataset.market_features[:, :2], dtype=np.float64)
    if variant == LEGACY_RESULT_POPULARITY:
        features = np.asarray(dataset.market_features, dtype=np.float64).copy()
    elif variant == NO_POPULARITY:
        features = base_market.copy()
    else:
        decision_time_rank = decision_time_win_midranks(
            dataset.market_features[:, 0],
            dataset.race_keys,
        )
        features = np.concatenate([base_market, decision_time_rank.reshape(-1, 1)], axis=1)
    return replace(dataset, market_features=features)


def decision_time_win_midranks(
    log_win_odds: FloatArray,
    race_keys: NDArray[np.object_],
) -> FloatArray:
    if len(log_win_odds) != len(race_keys):
        raise ValueError("win odds and race keys must have the same row count")
    ranks = np.empty(len(log_win_odds), dtype=np.float64)
    groups: dict[str, list[int]] = {}
    for index, race_key in enumerate(race_keys):
        groups.setdefault(str(race_key), []).append(index)
    for indices in groups.values():
        index_array = np.asarray(indices, dtype=np.int64)
        values = log_win_odds[index_array]
        order = np.argsort(values, kind="stable")
        ordered_values = values[order]
        ordered_ranks = np.empty(len(order), dtype=np.float64)
        start = 0
        while start < len(order):
            stop = start + 1
            while stop < len(order) and ordered_values[stop] == ordered_values[start]:
                stop += 1
            midrank = ((start + 1) + stop) / 2.0
            ordered_ranks[start:stop] = midrank
            start = stop
        inverse_order = np.empty(len(order), dtype=np.int64)
        inverse_order[order] = np.arange(len(order), dtype=np.int64)
        ranks[index_array] = ordered_ranks[inverse_order]
    return ranks


def run_popularity_carrier_comparison(
    dataset: ComparisonDataset,
    input_audit: InputAudit,
    *,
    c_grid: Sequence[float] = DEFAULT_C_GRID,
    bootstrap_repetitions: int = 2_000,
    random_seed: int = 656,
) -> PopularityCarrierComparisonResult:
    years = dataset.years
    train_mask = years == 2023
    validation_mask = years == 2024
    final_train_mask = np.isin(years, [2023, 2024])
    holdout_mask = years == 2025
    if (
        min(
            int(train_mask.sum()),
            int(validation_mask.sum()),
            int(holdout_mask.sum()),
        )
        == 0
    ):
        raise ValueError("2023 train, 2024 selection, and 2025 historical evaluation are required")

    variant_fits: dict[str, _VariantFit] = {}
    metrics: list[dict[str, Any]] = []
    hyperparameter_rows: list[dict[str, Any]] = []
    for variant in ALL_VARIANTS:
        variant_dataset = market_variant_dataset(dataset, variant)
        fit = _fit_market_variant(
            variant_dataset,
            variant=variant,
            c_grid=c_grid,
            train_mask=train_mask,
            validation_mask=validation_mask,
            final_train_mask=final_train_mask,
            holdout_mask=holdout_mask,
        )
        variant_fits[variant] = fit
        metrics.extend(fit.metrics)
        hyperparameter_rows.extend(fit.hyperparameters)

    selected_safe_variant = min(
        SAFE_VARIANTS,
        key=lambda name: (
            _metric_value(metrics, "2024_selection", f"{name}_M1C", "log_loss"),
            name,
        ),
    )

    validation = dataset.subset(validation_mask)
    holdout = dataset.subset(holdout_mask)
    bootstrap_rows: list[dict[str, Any]] = []
    for period_label, evaluation, probability_field in (
        ("2024_selection", validation, "validation_constrained"),
        ("2025_historical_evaluation", holdout, "holdout_constrained"),
    ):
        legacy_probabilities = getattr(
            variant_fits[LEGACY_RESULT_POPULARITY],
            probability_field,
        )
        for safe_variant in SAFE_VARIANTS:
            bootstrap_rows.append(
                race_bootstrap_log_loss_delta(
                    period_label=period_label,
                    baseline_name=f"{LEGACY_RESULT_POPULARITY}_M1C",
                    candidate_name=f"{safe_variant}_M1C",
                    targets=evaluation.targets,
                    baseline_probabilities=legacy_probabilities,
                    candidate_probabilities=getattr(variant_fits[safe_variant], probability_field),
                    race_keys=evaluation.race_keys,
                    repetitions=bootstrap_repetitions,
                    random_seed=random_seed,
                )
            )
        bootstrap_rows.append(
            race_bootstrap_log_loss_delta(
                period_label=period_label,
                baseline_name=f"{NO_POPULARITY}_M1C",
                candidate_name=f"{DECISION_TIME_WIN_RANK}_M1C",
                targets=evaluation.targets,
                baseline_probabilities=getattr(variant_fits[NO_POPULARITY], probability_field),
                candidate_probabilities=getattr(
                    variant_fits[DECISION_TIME_WIN_RANK],
                    probability_field,
                ),
                race_keys=evaluation.race_keys,
                repetitions=bootstrap_repetitions,
                random_seed=random_seed,
            )
        )

    rank_agreement_rows = _rank_agreement_rows(dataset)
    selected_holdout_delta = _bootstrap_delta(
        bootstrap_rows,
        period_label="2025_historical_evaluation",
        candidate_name=f"{selected_safe_variant}_M1C",
        baseline_name=f"{LEGACY_RESULT_POPULARITY}_M1C",
    )
    selected_validation_delta = _bootstrap_delta(
        bootstrap_rows,
        period_label="2024_selection",
        candidate_name=f"{selected_safe_variant}_M1C",
        baseline_name=f"{LEGACY_RESULT_POPULARITY}_M1C",
    )
    holdout_ci_low = float(selected_holdout_delta["ci95_low"])
    holdout_ci_high = float(selected_holdout_delta["ci95_high"])
    if holdout_ci_high < 0.0:
        empirical_verdict = "SELECTED_SAFE_CANDIDATE_IMPROVED_ON_2025_PAIRED_BOOTSTRAP"
    elif holdout_ci_low > 0.0:
        empirical_verdict = "SELECTED_SAFE_CANDIDATE_TRAILED_ON_2025_PAIRED_BOOTSTRAP"
    else:
        empirical_verdict = "SELECTED_SAFE_CANDIDATE_INDISTINGUISHABLE_ON_2025_PAIRED_BOOTSTRAP"
    recommendation = (
        "PREREGISTER_DECISION_TIME_WIN_RANK_AND_RERUN_PHASE651_TO_PHASE653"
        if selected_safe_variant == DECISION_TIME_WIN_RANK
        else "PREREGISTER_NO_POPULARITY_AND_RERUN_PHASE651_TO_PHASE653"
    )
    summary: dict[str, Any] = {
        "analysis_version": "phase656_popularity_carrier_comparison_v1",
        "final_verdict": FINAL_VERDICT,
        "semantic_verdict": SEMANTIC_VERDICT,
        "empirical_verdict": empirical_verdict,
        "research_question": (
            "which decision-time-safe replacement for result-side popularity should define "
            "the prospective market baseline"
        ),
        "variants": {
            LEGACY_RESULT_POPULARITY: (
                "log win odds, log place-basis odds, and result-side SED popularity; "
                "diagnostic comparator only"
            ),
            NO_POPULARITY: "log win odds and log place-basis odds",
            DECISION_TIME_WIN_RANK: (
                "log win odds, log place-basis odds, and per-race midrank derived only "
                "from observed win odds"
            ),
        },
        "selection_contract": {
            "train": 2023,
            "safe_candidate_selection": 2024,
            "historical_evaluation_after_selection": 2025,
            "selection_metric": "M1C constrained mean binary log loss",
            "safe_candidates": list(SAFE_VARIANTS),
            "legacy_result_popularity_eligible_for_selection": False,
            "2025_claimed_fresh": False,
        },
        "selected_safe_variant": selected_safe_variant,
        "selected_c_by_variant": {
            variant: variant_fits[variant].selected_c for variant in ALL_VARIANTS
        },
        "selected_vs_legacy": {
            "2024_point_log_loss_delta": selected_validation_delta["point_log_loss_delta"],
            "2024_ci95_low": selected_validation_delta["ci95_low"],
            "2024_ci95_high": selected_validation_delta["ci95_high"],
            "2025_point_log_loss_delta": selected_holdout_delta["point_log_loss_delta"],
            "2025_ci95_low": selected_holdout_delta["ci95_low"],
            "2025_ci95_high": selected_holdout_delta["ci95_high"],
        },
        "input_audit": {
            "joined_row_count": input_audit.joined_row_count,
            "joined_rows_by_year": input_audit.joined_rows_by_year,
            "target_mismatch_count_corrected_by_phase650h": (
                input_audit.existing_market_target_mismatch_count
            ),
        },
        "recommendation": recommendation,
        "phase654_contract_changed": False,
        "2026_data_used": False,
        "roi_or_betting_used": False,
    }
    return PopularityCarrierComparisonResult(
        summary=summary,
        metrics=tuple(metrics),
        bootstrap_rows=tuple(bootstrap_rows),
        rank_agreement_rows=rank_agreement_rows,
        hyperparameter_rows=tuple(hyperparameter_rows),
    )


def write_popularity_carrier_comparison(
    result: PopularityCarrierComparisonResult,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "phase656_summary.json", result.summary)
    _write_csv(output_dir / "phase656_market_metrics.csv", result.metrics)
    _write_csv(output_dir / "phase656_bootstrap.csv", result.bootstrap_rows)
    _write_csv(output_dir / "phase656_rank_agreement.csv", result.rank_agreement_rows)
    _write_csv(output_dir / "phase656_hyperparameters.csv", result.hyperparameter_rows)
    (output_dir / "phase656_findings.md").write_text(
        _findings_markdown(result),
        encoding="utf-8",
    )


def _fit_market_variant(
    dataset: ComparisonDataset,
    *,
    variant: str,
    c_grid: Sequence[float],
    train_mask: NDArray[np.bool_],
    validation_mask: NDArray[np.bool_],
    final_train_mask: NDArray[np.bool_],
    holdout_mask: NDArray[np.bool_],
) -> _VariantFit:
    train = dataset.subset(train_mask)
    validation = dataset.subset(validation_mask)
    final_train = dataset.subset(final_train_mask)
    holdout = dataset.subset(holdout_mask)
    if not c_grid:
        raise ValueError("at least one logistic C value is required")

    selection_transformer = NumericTransformer.fit(train.market_features)
    train_values = selection_transformer.transform(train.market_features)
    validation_values = selection_transformer.transform(validation.market_features)
    hyperparameters: list[dict[str, Any]] = []
    validation_predictions_by_c: dict[float, FloatArray] = {}
    for c_value in c_grid:
        model = fit_logistic_probability_model(train_values, train.targets, float(c_value))
        probabilities = predict_positive_probability(model, validation_values)
        validation_predictions_by_c[float(c_value)] = probabilities
        hyperparameters.append(
            {
                "c": float(c_value),
                "validation_log_loss": float(
                    log_loss(validation.targets, probabilities, labels=[0, 1])
                ),
                "selected": False,
            }
        )
    selected = min(
        hyperparameters,
        key=lambda row: (float(row["validation_log_loss"]), float(row["c"])),
    )
    selected["selected"] = True
    selected_c = float(selected["c"])
    validation_raw = validation_predictions_by_c[selected_c]
    validation_constrained = constrain_probabilities_by_race(
        validation_raw,
        validation.race_keys,
        validation.place_slots,
    )

    final_transformer = NumericTransformer.fit(final_train.market_features)
    final_train_values = final_transformer.transform(final_train.market_features)
    holdout_values = final_transformer.transform(holdout.market_features)
    final_model = fit_logistic_probability_model(
        final_train_values,
        final_train.targets,
        selected_c,
    )
    holdout_raw = predict_positive_probability(final_model, holdout_values)
    holdout_constrained = constrain_probabilities_by_race(
        holdout_raw,
        holdout.race_keys,
        holdout.place_slots,
    )

    for row in hyperparameters:
        row["variant"] = variant
    metrics = (
        metric_row("2024_selection", f"{variant}_M1", validation, validation_raw),
        metric_row(
            "2024_selection",
            f"{variant}_M1C",
            validation,
            validation_constrained,
        ),
        metric_row("2025_historical_evaluation", f"{variant}_M1", holdout, holdout_raw),
        metric_row(
            "2025_historical_evaluation",
            f"{variant}_M1C",
            holdout,
            holdout_constrained,
        ),
    )
    return _VariantFit(
        selected_c=selected_c,
        validation_raw=validation_raw,
        validation_constrained=validation_constrained,
        holdout_raw=holdout_raw,
        holdout_constrained=holdout_constrained,
        metrics=metrics,
        hyperparameters=tuple(hyperparameters),
    )


def _rank_agreement_rows(dataset: ComparisonDataset) -> tuple[dict[str, Any], ...]:
    derived = decision_time_win_midranks(dataset.market_features[:, 0], dataset.race_keys)
    legacy = np.asarray(dataset.market_features[:, 2], dtype=np.float64)
    rows: list[dict[str, Any]] = []
    for label, mask in (
        ("all", np.ones(len(legacy), dtype=np.bool_)),
        *((str(year), dataset.years == year) for year in (2023, 2024, 2025)),
    ):
        difference = np.abs(legacy[mask] - derived[mask])
        rows.append(
            {
                "period": label,
                "row_count": int(mask.sum()),
                "exact_match_rate": float(np.mean(difference == 0.0)),
                "mean_absolute_rank_difference": float(np.mean(difference)),
                "rank_correlation": _safe_correlation(legacy[mask], derived[mask]),
            }
        )
    return tuple(rows)


def _safe_correlation(left: FloatArray, right: FloatArray) -> float | None:
    if len(left) < 2 or float(np.std(left)) == 0.0 or float(np.std(right)) == 0.0:
        return None
    return float(np.corrcoef(left, right)[0, 1])


def _metric_value(
    rows: Sequence[dict[str, Any]],
    period_label: str,
    model_name: str,
    metric_name: str,
) -> float:
    matches = [
        row
        for row in rows
        if row["period_label"] == period_label and row["model_name"] == model_name
    ]
    if len(matches) != 1:
        raise ValueError(f"expected one metric row for {period_label}/{model_name}")
    return float(matches[0][metric_name])


def _bootstrap_delta(
    rows: Sequence[dict[str, Any]],
    *,
    period_label: str,
    candidate_name: str,
    baseline_name: str,
) -> dict[str, Any]:
    matches = [
        row
        for row in rows
        if row["period_label"] == period_label
        and row["candidate_name"] == candidate_name
        and row["baseline_name"] == baseline_name
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected one bootstrap row for {period_label}/{baseline_name}/{candidate_name}"
        )
    return matches[0]


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = list(rows[0]) if rows else []
    with path.open("w", encoding="utf-8", newline="") as file:
        if not fieldnames:
            return
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _findings_markdown(result: PopularityCarrierComparisonResult) -> str:
    selected = result.summary["selected_safe_variant"]
    delta = result.summary["selected_vs_legacy"]
    return "\n".join(
        [
            "# Phase656 Popularity Carrier Comparison",
            "",
            "## Verdict",
            "",
            f"`{result.summary['final_verdict']}`",
            "",
            f"Selected safe candidate: `{selected}`.",
            "",
            "Result-side SED popularity remains ineligible for prospective use regardless of "
            "its historical score.",
            "",
            "## Selected candidate versus legacy diagnostic",
            "",
            f"- 2024 constrained log-loss delta: {delta['2024_point_log_loss_delta']:.9f}",
            f"- 2025 constrained log-loss delta: {delta['2025_point_log_loss_delta']:.9f}",
            "",
            "The safe candidate was selected on 2024 only. The 2025 result is historical "
            "evaluation, not a fresh holdout claim.",
            "",
            "## Next step",
            "",
            f"`{result.summary['recommendation']}`",
            "",
            "No 2026 data, ROI, payout, threshold, stake, or betting rule was used.",
            "",
        ]
    )
