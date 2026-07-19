from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb
import numpy as np
from numpy.typing import NDArray
from scipy.optimize import minimize  # type: ignore[import-untyped]
from scipy.special import expit, logit  # type: ignore[import-untyped]
from sklearn.linear_model import LogisticRegression  # type: ignore[import-untyped]
from sklearn.metrics import (  # type: ignore[import-untyped]
    average_precision_score,
    log_loss,
    roc_auc_score,
)

from horse_bet_lab.research.historical_ability_source import READY_VERDICT

MARKET_FEATURE_COLUMNS = ("win_odds", "place_basis_odds", "popularity")
HISTORY_NUMERIC_FEATURE_COLUMNS = (
    "current_distance_m",
    "card_field_size",
    "prior_start_count",
    "days_since_last_start",
    "last_1_finish_percentile",
    "last_3_mean_finish_percentile",
    "last_5_mean_finish_percentile",
    "last_3_top3_rate",
    "last_5_top3_rate",
    "last_5_recency_weighted_form",
    "last_5_distance_compatibility_rate",
    "last_5_venue_compatibility_rate",
)
HISTORY_CATEGORICAL_FEATURE_COLUMNS = ("venue_code",)
MODEL_NAMES = (
    "M0_race_prior",
    "M1_market",
    "M1C_race_constrained_market",
    "M2_history",
    "M3_market_history",
    "M4_market_offset_history",
    "M5_race_constrained_offset",
)
DEFAULT_C_GRID = (0.03, 0.1, 0.3, 1.0, 3.0)

SIGNAL_SUPPORTED = "HISTORICAL_ABILITY_INCREMENTAL_SIGNAL_SUPPORTED"
SIGNAL_DIAGNOSTIC_ONLY = "HISTORICAL_ABILITY_SIGNAL_DIAGNOSTIC_ONLY"
SIGNAL_NOT_SUPPORTED = "HISTORICAL_ABILITY_SIGNAL_NOT_SUPPORTED"

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
ObjectArray = NDArray[np.object_]


@dataclass(frozen=True)
class ComparisonDataset:
    race_dates: tuple[date, ...]
    race_keys: ObjectArray
    horse_numbers: IntArray
    targets: IntArray
    market_targets: IntArray
    market_features: FloatArray
    history_numeric_features: FloatArray
    venue_codes: ObjectArray
    active_field_sizes: IntArray
    place_slots: IntArray

    @property
    def years(self) -> IntArray:
        return np.asarray([value.year for value in self.race_dates], dtype=np.int64)

    def subset(self, mask: NDArray[np.bool_]) -> ComparisonDataset:
        indices = np.flatnonzero(mask)
        return ComparisonDataset(
            race_dates=tuple(self.race_dates[index] for index in indices),
            race_keys=self.race_keys[mask],
            horse_numbers=self.horse_numbers[mask],
            targets=self.targets[mask],
            market_targets=self.market_targets[mask],
            market_features=self.market_features[mask],
            history_numeric_features=self.history_numeric_features[mask],
            venue_codes=self.venue_codes[mask],
            active_field_sizes=self.active_field_sizes[mask],
            place_slots=self.place_slots[mask],
        )


@dataclass(frozen=True)
class InputAudit:
    source_verdict: str
    history_row_count: int
    market_row_count: int
    joined_row_count: int
    missing_history_row_count: int
    missing_history_race_count: int
    partially_joined_race_count: int
    fully_missing_unsupported_race_count: int
    duplicate_history_identity_count: int
    duplicate_market_identity_count: int
    existing_market_target_mismatch_count: int
    mismatch_direction_counts: dict[str, int]
    mismatch_by_active_field_size: dict[str, int]
    joined_rows_by_year: dict[str, int]


@dataclass(frozen=True)
class NumericTransformer:
    medians: FloatArray
    means: FloatArray
    scales: FloatArray

    @classmethod
    def fit(cls, values: FloatArray) -> NumericTransformer:
        medians = np.nanmedian(values, axis=0)
        medians = np.where(np.isnan(medians), 0.0, medians)
        filled = np.where(np.isnan(values), medians, values)
        missing = np.isnan(values).astype(np.float64)
        augmented = np.concatenate([filled, missing], axis=1)
        means = augmented.mean(axis=0)
        scales = augmented.std(axis=0)
        scales = np.where(scales == 0.0, 1.0, scales)
        return cls(medians=medians, means=means, scales=scales)

    def transform(self, values: FloatArray) -> FloatArray:
        filled = np.where(np.isnan(values), self.medians, values)
        missing = np.isnan(values).astype(np.float64)
        augmented = np.concatenate([filled, missing], axis=1)
        return np.asarray((augmented - self.means) / self.scales, dtype=np.float64)


@dataclass(frozen=True)
class HistoryTransformer:
    numeric: NumericTransformer
    venue_categories: tuple[str, ...]

    @classmethod
    def fit(cls, numeric_values: FloatArray, venue_codes: ObjectArray) -> HistoryTransformer:
        return cls(
            numeric=NumericTransformer.fit(numeric_values),
            venue_categories=tuple(sorted({str(value) for value in venue_codes})),
        )

    def transform(self, numeric_values: FloatArray, venue_codes: ObjectArray) -> FloatArray:
        numeric = self.numeric.transform(numeric_values)
        venue = np.zeros((len(venue_codes), len(self.venue_categories)), dtype=np.float64)
        category_index = {value: index for index, value in enumerate(self.venue_categories)}
        for row_index, value in enumerate(venue_codes):
            column_index = category_index.get(str(value))
            if column_index is not None:
                venue[row_index, column_index] = 1.0
        return np.concatenate([numeric, venue], axis=1)


@dataclass(frozen=True)
class OffsetLogisticModel:
    intercept: float
    coefficients: FloatArray

    def predict(self, values: FloatArray, offset_probabilities: FloatArray) -> FloatArray:
        offsets = logit(np.clip(offset_probabilities, 1e-8, 1.0 - 1e-8))
        return np.asarray(
            expit(offsets + self.intercept + values @ self.coefficients),
            dtype=np.float64,
        )


@dataclass(frozen=True)
class ComparisonResult:
    summary: dict[str, Any]
    input_audit: dict[str, Any]
    validation_metrics: tuple[dict[str, Any], ...]
    holdout_metrics: tuple[dict[str, Any], ...]
    hyperparameters: tuple[dict[str, Any], ...]
    bootstrap_rows: tuple[dict[str, Any], ...]


def place_slots_for_field_size(field_size: int) -> int:
    if field_size >= 8:
        return 3
    if field_size >= 5:
        return 2
    raise ValueError(f"unsupported active field size: {field_size}")


def load_comparison_dataset(
    *,
    history_surface_path: Path,
    source_summary_path: Path,
    market_dataset_path: Path,
) -> tuple[ComparisonDataset, InputAudit]:
    source_summary = json.loads(source_summary_path.read_text())
    source_verdict = str(source_summary.get("final_verdict", ""))
    if source_verdict != READY_VERDICT:
        raise ValueError(f"history source is not ready: {source_verdict}")

    history_features = tuple(str(value) for value in source_summary["model_feature_columns"])
    expected_features = (*HISTORY_CATEGORICAL_FEATURE_COLUMNS, *HISTORY_NUMERIC_FEATURE_COLUMNS)
    if history_features != expected_features:
        raise ValueError(
            "history feature contract differs from Phase651 expectation: "
            f"expected={expected_features}, actual={history_features}"
        )

    connection = duckdb.connect()
    try:
        history_count = _required_scalar_int(
            connection.execute(
                "SELECT count(*) FROM read_csv_auto(?)",
                [str(history_surface_path)],
            ).fetchone()
        )
        market_count = _required_scalar_int(
            connection.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [str(market_dataset_path)],
            ).fetchone()
        )
        duplicate_history = _duplicate_identity_count(
            connection,
            "read_csv_auto(?)",
            history_surface_path,
        )
        duplicate_market = _duplicate_identity_count(
            connection,
            "read_parquet(?)",
            market_dataset_path,
        )
        if duplicate_history or duplicate_market:
            raise ValueError(
                "duplicate identities block comparison: "
                f"history={duplicate_history}, market={duplicate_market}"
            )

        join_coverage_rows = connection.execute(
            """
            WITH history AS (
                SELECT race_key, horse_number FROM read_csv_auto(?)
            ), market AS (
                SELECT race_key, horse_number FROM read_parquet(?)
            )
            SELECT
                market.race_key,
                count(*) AS market_rows,
                count(history.race_key) AS joined_rows
            FROM market
            LEFT JOIN history USING (race_key, horse_number)
            GROUP BY market.race_key
            HAVING count(history.race_key) < count(*)
            ORDER BY market.race_key
            """,
            [str(history_surface_path), str(market_dataset_path)],
        ).fetchall()
        partially_joined_races = [
            row for row in join_coverage_rows if 0 < int(row[2]) < int(row[1])
        ]
        missing_supported_races = [
            row for row in join_coverage_rows if int(row[2]) == 0 and int(row[1]) >= 5
        ]
        if partially_joined_races or missing_supported_races:
            raise ValueError(
                "incomplete supported-race join blocks comparison: "
                f"partial={partially_joined_races[:5]}, "
                f"missing_supported={missing_supported_races[:5]}"
            )

        selected_columns = [
            "h.race_date",
            "h.race_key",
            "h.horse_number",
            "CAST(h.is_place AS INTEGER) AS target_value",
            "CAST(m.target_value AS INTEGER) AS market_target_value",
            "m.win_odds",
            "m.place_basis_odds",
            "m.popularity",
            "h.venue_code",
            *(f"h.{column}" for column in HISTORY_NUMERIC_FEATURE_COLUMNS),
        ]
        rows = connection.execute(
            f"""
            SELECT {", ".join(selected_columns)}
            FROM read_csv_auto(?) AS h
            INNER JOIN read_parquet(?) AS m
              USING (race_key, horse_number)
            ORDER BY h.race_date, h.race_key, h.horse_number
            """,
            [str(history_surface_path), str(market_dataset_path)],
        ).fetchall()
    finally:
        connection.close()

    if not rows:
        raise ValueError("comparison join produced no rows")

    race_dates = tuple(_required_date(row[0]) for row in rows)
    race_keys = np.asarray([str(row[1]) for row in rows], dtype=object)
    horse_numbers = np.asarray([int(row[2]) for row in rows], dtype=np.int64)
    targets = np.asarray([int(row[3]) for row in rows], dtype=np.int64)
    market_targets = np.asarray([int(row[4]) for row in rows], dtype=np.int64)
    market_features = np.asarray(
        [[math.log1p(float(row[5])), math.log1p(float(row[6])), float(row[7])] for row in rows],
        dtype=np.float64,
    )
    venue_codes = np.asarray([str(row[8]) for row in rows], dtype=object)
    history_numeric = np.asarray(
        [[_optional_float(value) for value in row[9:]] for row in rows],
        dtype=np.float64,
    )

    active_sizes_by_race: dict[str, int] = {}
    for race_key in race_keys:
        key = str(race_key)
        active_sizes_by_race[key] = active_sizes_by_race.get(key, 0) + 1
    active_field_sizes = np.asarray(
        [active_sizes_by_race[str(race_key)] for race_key in race_keys],
        dtype=np.int64,
    )
    place_slots = np.asarray(
        [place_slots_for_field_size(int(value)) for value in active_field_sizes],
        dtype=np.int64,
    )

    mismatch_mask = targets != market_targets
    mismatch_direction_counts: dict[str, int] = {}
    mismatch_by_field_size: dict[str, int] = {}
    for target, market_target, field_size in zip(
        targets[mismatch_mask],
        market_targets[mismatch_mask],
        active_field_sizes[mismatch_mask],
        strict=True,
    ):
        direction = f"history_{target}_market_{market_target}"
        mismatch_direction_counts[direction] = mismatch_direction_counts.get(direction, 0) + 1
        key = str(int(field_size))
        mismatch_by_field_size[key] = mismatch_by_field_size.get(key, 0) + 1

    joined_rows_by_year: dict[str, int] = {}
    for race_date in race_dates:
        key = str(race_date.year)
        joined_rows_by_year[key] = joined_rows_by_year.get(key, 0) + 1

    dataset = ComparisonDataset(
        race_dates=race_dates,
        race_keys=race_keys,
        horse_numbers=horse_numbers,
        targets=targets,
        market_targets=market_targets,
        market_features=market_features,
        history_numeric_features=history_numeric,
        venue_codes=venue_codes,
        active_field_sizes=active_field_sizes,
        place_slots=place_slots,
    )
    audit = InputAudit(
        source_verdict=source_verdict,
        history_row_count=history_count,
        market_row_count=market_count,
        joined_row_count=len(rows),
        missing_history_row_count=market_count - len(rows),
        missing_history_race_count=len(join_coverage_rows),
        partially_joined_race_count=len(partially_joined_races),
        fully_missing_unsupported_race_count=sum(
            int(row[2]) == 0 and int(row[1]) < 5 for row in join_coverage_rows
        ),
        duplicate_history_identity_count=duplicate_history,
        duplicate_market_identity_count=duplicate_market,
        existing_market_target_mismatch_count=int(mismatch_mask.sum()),
        mismatch_direction_counts=mismatch_direction_counts,
        mismatch_by_active_field_size=mismatch_by_field_size,
        joined_rows_by_year=joined_rows_by_year,
    )
    return dataset, audit


def run_model_comparison(
    dataset: ComparisonDataset,
    input_audit: InputAudit,
    *,
    c_grid: Sequence[float] = DEFAULT_C_GRID,
    bootstrap_repetitions: int = 2_000,
    random_seed: int = 651,
) -> ComparisonResult:
    years = dataset.years
    train = dataset.subset(years == 2023)
    validation = dataset.subset(years == 2024)
    final_train = dataset.subset(np.isin(years, [2023, 2024]))
    holdout = dataset.subset(years == 2025)
    if min(len(train.targets), len(validation.targets), len(holdout.targets)) == 0:
        raise ValueError("2023 train, 2024 validation, and 2025 holdout are all required")

    selected_c: dict[str, float] = {}
    hyperparameters: list[dict[str, Any]] = []

    market_train_transformer = NumericTransformer.fit(train.market_features)
    market_train_values = market_train_transformer.transform(train.market_features)
    market_validation_values = market_train_transformer.transform(validation.market_features)
    selected_c["M1_market"] = _select_logistic_c(
        model_name="M1_market",
        train_values=market_train_values,
        train_targets=train.targets,
        validation_values=market_validation_values,
        validation_targets=validation.targets,
        c_grid=c_grid,
        rows=hyperparameters,
    )

    history_train_transformer = HistoryTransformer.fit(
        train.history_numeric_features,
        train.venue_codes,
    )
    history_train_values = history_train_transformer.transform(
        train.history_numeric_features,
        train.venue_codes,
    )
    history_validation_values = history_train_transformer.transform(
        validation.history_numeric_features,
        validation.venue_codes,
    )
    selected_c["M2_history"] = _select_logistic_c(
        model_name="M2_history",
        train_values=history_train_values,
        train_targets=train.targets,
        validation_values=history_validation_values,
        validation_targets=validation.targets,
        c_grid=c_grid,
        rows=hyperparameters,
    )

    combined_train_values = np.concatenate([market_train_values, history_train_values], axis=1)
    combined_validation_values = np.concatenate(
        [market_validation_values, history_validation_values],
        axis=1,
    )
    selected_c["M3_market_history"] = _select_logistic_c(
        model_name="M3_market_history",
        train_values=combined_train_values,
        train_targets=train.targets,
        validation_values=combined_validation_values,
        validation_targets=validation.targets,
        c_grid=c_grid,
        rows=hyperparameters,
    )

    validation_predictions = _fit_predict_models(
        train=train,
        evaluation=validation,
        selected_c=selected_c,
        c_grid=c_grid,
        hyperparameter_rows=hyperparameters,
        select_offset_c=True,
    )
    selected_c["M4_market_offset_history"] = float(
        next(
            row["c"]
            for row in hyperparameters
            if row["model_name"] == "M4_market_offset_history" and row["selected"]
        )
    )
    selected_c["M5_race_constrained_offset"] = float(
        next(
            row["c"]
            for row in hyperparameters
            if row["model_name"] == "M5_race_constrained_offset" and row["selected"]
        )
    )

    validation_metrics = tuple(
        metric_row("2024_validation", model_name, validation, predictions)
        for model_name, predictions in validation_predictions.items()
    )

    holdout_predictions = _fit_predict_models(
        train=final_train,
        evaluation=holdout,
        selected_c=selected_c,
        c_grid=(),
        hyperparameter_rows=None,
        select_offset_c=False,
    )
    holdout_metrics = tuple(
        metric_row("2025_holdout", model_name, holdout, predictions)
        for model_name, predictions in holdout_predictions.items()
    )

    bootstrap_rows: list[dict[str, Any]] = []
    for period_label, evaluation, predictions_by_model in (
        ("2024_validation", validation, validation_predictions),
        ("2025_holdout", holdout, holdout_predictions),
    ):
        comparison_pairs = (
            ("M1_market", "M1C_race_constrained_market"),
            ("M1_market", "M2_history"),
            ("M1_market", "M3_market_history"),
            ("M1_market", "M4_market_offset_history"),
            ("M1C_race_constrained_market", "M5_race_constrained_offset"),
        )
        for baseline_name, model_name in comparison_pairs:
            bootstrap_rows.append(
                race_bootstrap_log_loss_delta(
                    period_label=period_label,
                    baseline_name=baseline_name,
                    candidate_name=model_name,
                    targets=evaluation.targets,
                    baseline_probabilities=predictions_by_model[baseline_name],
                    candidate_probabilities=predictions_by_model[model_name],
                    race_keys=evaluation.race_keys,
                    repetitions=bootstrap_repetitions,
                    random_seed=random_seed,
                )
            )

    final_verdict, verdict_evidence = _model_verdict(bootstrap_rows)
    summary: dict[str, Any] = {
        "analysis_version": "phase651_historical_ability_model_comparison_v1",
        "final_verdict": final_verdict,
        "primary_hypothesis": (
            "strictly-prior horse history improves calibrated place probability "
            "beyond the decision-time market baseline"
        ),
        "target_contract": {
            "source": "Phase650H is_place",
            "definition": (
                "finish position within actual place slots (2 for 5-7 active runners; 3 for 8+)"
            ),
            "existing_market_target_used_as_predictor": False,
            "existing_market_target_mismatch_count": (
                input_audit.existing_market_target_mismatch_count
            ),
        },
        "split_contract": {
            "train": 2023,
            "selection": 2024,
            "final_refit": "2023-2024",
            "one_time_holdout": 2025,
        },
        "model_contract": {
            "M0_race_prior": "place slots / active field size",
            "M1_market": "logistic regression on win odds, place basis odds, popularity",
            "M1C_race_constrained_market": (
                "M1 with per-race probability sum constrained to place slots"
            ),
            "M2_history": "logistic regression on strictly-prior history and pre-race context",
            "M3_market_history": "direct logistic combination of market and history",
            "M4_market_offset_history": "fixed M1 logit plus historical residual correction",
            "M5_race_constrained_offset": (
                "M4 with per-race probability sum constrained to place slots"
            ),
        },
        "selected_c": selected_c,
        "bootstrap_repetitions": bootstrap_repetitions,
        "verdict_evidence": verdict_evidence,
        "roi_or_betting_used_for_selection": False,
    }
    return ComparisonResult(
        summary=summary,
        input_audit=_dataclass_dict(input_audit),
        validation_metrics=validation_metrics,
        holdout_metrics=holdout_metrics,
        hyperparameters=tuple(hyperparameters),
        bootstrap_rows=tuple(bootstrap_rows),
    )


def write_comparison_result(result: ComparisonResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_json(output_dir / "phase651_summary.json", result.summary)
    _write_json(output_dir / "phase651_input_audit.json", result.input_audit)
    _write_csv(output_dir / "phase651_validation_metrics.csv", result.validation_metrics)
    _write_csv(output_dir / "phase651_holdout_metrics.csv", result.holdout_metrics)
    _write_csv(output_dir / "phase651_hyperparameters.csv", result.hyperparameters)
    _write_csv(output_dir / "phase651_bootstrap.csv", result.bootstrap_rows)
    (output_dir / "phase651_findings.md").write_text(_findings_markdown(result))


def constrain_probabilities_by_race(
    probabilities: FloatArray,
    race_keys: ObjectArray,
    place_slots: IntArray,
) -> FloatArray:
    constrained = np.empty_like(probabilities)
    row_indices: dict[str, list[int]] = {}
    for index, race_key in enumerate(race_keys):
        row_indices.setdefault(str(race_key), []).append(index)
    for indices in row_indices.values():
        index_array = np.asarray(indices, dtype=np.int64)
        logits = logit(np.clip(probabilities[index_array], 1e-8, 1.0 - 1e-8))
        target_sum = float(place_slots[index_array[0]])
        low = -30.0
        high = 30.0
        for _ in range(80):
            middle = (low + high) / 2.0
            if float(expit(logits + middle).sum()) < target_sum:
                low = middle
            else:
                high = middle
        constrained[index_array] = expit(logits + (low + high) / 2.0)
    return constrained


def metric_row(
    period_label: str,
    model_name: str,
    dataset: ComparisonDataset,
    probabilities: FloatArray,
) -> dict[str, Any]:
    clipped = np.clip(probabilities, 1e-8, 1.0 - 1e-8)
    intercept, slope = _calibration_intercept_slope(dataset.targets, clipped)
    race_sum_error = _race_probability_sum_error(
        clipped,
        dataset.race_keys,
        dataset.place_slots,
    )
    return {
        "period_label": period_label,
        "model_name": model_name,
        "row_count": len(dataset.targets),
        "race_count": len(set(str(value) for value in dataset.race_keys)),
        "positive_rate": float(dataset.targets.mean()),
        "mean_probability": float(clipped.mean()),
        "log_loss": float(log_loss(dataset.targets, clipped, labels=[0, 1])),
        "brier_score": float(np.mean((clipped - dataset.targets) ** 2)),
        "roc_auc": float(roc_auc_score(dataset.targets, clipped)),
        "average_precision": float(average_precision_score(dataset.targets, clipped)),
        "calibration_gap": float(clipped.mean() - dataset.targets.mean()),
        "calibration_intercept": intercept,
        "calibration_slope": slope,
        "ece_10": _expected_calibration_error(dataset.targets, clipped, 10),
        "race_probability_sum_mae": race_sum_error,
    }


def race_bootstrap_log_loss_delta(
    *,
    period_label: str,
    baseline_name: str,
    candidate_name: str,
    targets: IntArray,
    baseline_probabilities: FloatArray,
    candidate_probabilities: FloatArray,
    race_keys: ObjectArray,
    repetitions: int,
    random_seed: int,
) -> dict[str, Any]:
    baseline_losses = _binary_log_losses(targets, baseline_probabilities)
    candidate_losses = _binary_log_losses(targets, candidate_probabilities)
    groups: dict[str, list[int]] = {}
    for index, race_key in enumerate(race_keys):
        groups.setdefault(str(race_key), []).append(index)
    race_names = tuple(groups)
    loss_deltas = np.asarray(
        [
            float((candidate_losses[groups[key]] - baseline_losses[groups[key]]).sum())
            for key in race_names
        ],
        dtype=np.float64,
    )
    row_counts = np.asarray([len(groups[key]) for key in race_names], dtype=np.float64)
    rng = np.random.default_rng(
        random_seed + sum(ord(value) for value in candidate_name + period_label)
    )
    samples = np.empty(repetitions, dtype=np.float64)
    for repetition in range(repetitions):
        sampled = rng.integers(0, len(race_names), size=len(race_names))
        samples[repetition] = loss_deltas[sampled].sum() / row_counts[sampled].sum()
    point_delta = float(loss_deltas.sum() / row_counts.sum())
    return {
        "period_label": period_label,
        "baseline_name": baseline_name,
        "candidate_name": candidate_name,
        "race_count": len(race_names),
        "point_log_loss_delta": point_delta,
        "ci95_low": float(np.quantile(samples, 0.025)),
        "ci95_high": float(np.quantile(samples, 0.975)),
        "bootstrap_probability_improved": float(np.mean(samples < 0.0)),
        "repetitions": repetitions,
    }


def _fit_predict_models(
    *,
    train: ComparisonDataset,
    evaluation: ComparisonDataset,
    selected_c: dict[str, float],
    c_grid: Sequence[float],
    hyperparameter_rows: list[dict[str, Any]] | None,
    select_offset_c: bool,
) -> dict[str, FloatArray]:
    market_transformer = NumericTransformer.fit(train.market_features)
    market_train = market_transformer.transform(train.market_features)
    market_evaluation = market_transformer.transform(evaluation.market_features)
    market_model = _fit_logistic(market_train, train.targets, selected_c["M1_market"])
    market_train_probabilities = _positive_probability(market_model, market_train)
    market_evaluation_probabilities = _positive_probability(market_model, market_evaluation)

    history_transformer = HistoryTransformer.fit(
        train.history_numeric_features,
        train.venue_codes,
    )
    history_train = history_transformer.transform(
        train.history_numeric_features,
        train.venue_codes,
    )
    history_evaluation = history_transformer.transform(
        evaluation.history_numeric_features,
        evaluation.venue_codes,
    )

    history_model = _fit_logistic(history_train, train.targets, selected_c["M2_history"])
    combined_train = np.concatenate([market_train, history_train], axis=1)
    combined_evaluation = np.concatenate([market_evaluation, history_evaluation], axis=1)
    combined_model = _fit_logistic(
        combined_train,
        train.targets,
        selected_c["M3_market_history"],
    )

    if select_offset_c:
        if hyperparameter_rows is None:
            raise ValueError("hyperparameter rows are required during offset selection")
        offset_predictions: dict[float, FloatArray] = {}
        constrained_predictions: dict[float, FloatArray] = {}
        for c_value in c_grid:
            offset_model = _fit_offset_logistic(
                history_train,
                train.targets,
                market_train_probabilities,
                c_value,
            )
            predicted = offset_model.predict(history_evaluation, market_evaluation_probabilities)
            constrained = constrain_probabilities_by_race(
                predicted,
                evaluation.race_keys,
                evaluation.place_slots,
            )
            offset_predictions[float(c_value)] = predicted
            constrained_predictions[float(c_value)] = constrained
            hyperparameter_rows.append(
                _hyperparameter_row(
                    "M4_market_offset_history",
                    float(c_value),
                    evaluation.targets,
                    predicted,
                )
            )
            hyperparameter_rows.append(
                _hyperparameter_row(
                    "M5_race_constrained_offset",
                    float(c_value),
                    evaluation.targets,
                    constrained,
                )
            )
        selected_offset_c = _mark_selected_hyperparameter(
            hyperparameter_rows,
            "M4_market_offset_history",
        )
        selected_constrained_c = _mark_selected_hyperparameter(
            hyperparameter_rows,
            "M5_race_constrained_offset",
        )
        offset_prediction = offset_predictions[selected_offset_c]
        constrained_prediction = constrained_predictions[selected_constrained_c]
    else:
        offset_model = _fit_offset_logistic(
            history_train,
            train.targets,
            market_train_probabilities,
            selected_c["M4_market_offset_history"],
        )
        offset_prediction = offset_model.predict(
            history_evaluation,
            market_evaluation_probabilities,
        )
        if selected_c["M5_race_constrained_offset"] == selected_c["M4_market_offset_history"]:
            constrained_base = offset_prediction
        else:
            constrained_model = _fit_offset_logistic(
                history_train,
                train.targets,
                market_train_probabilities,
                selected_c["M5_race_constrained_offset"],
            )
            constrained_base = constrained_model.predict(
                history_evaluation,
                market_evaluation_probabilities,
            )
        constrained_prediction = constrain_probabilities_by_race(
            constrained_base,
            evaluation.race_keys,
            evaluation.place_slots,
        )

    race_prior = evaluation.place_slots.astype(np.float64) / evaluation.active_field_sizes
    constrained_market = constrain_probabilities_by_race(
        market_evaluation_probabilities,
        evaluation.race_keys,
        evaluation.place_slots,
    )
    return {
        "M0_race_prior": race_prior,
        "M1_market": market_evaluation_probabilities,
        "M1C_race_constrained_market": constrained_market,
        "M2_history": _positive_probability(history_model, history_evaluation),
        "M3_market_history": _positive_probability(combined_model, combined_evaluation),
        "M4_market_offset_history": offset_prediction,
        "M5_race_constrained_offset": constrained_prediction,
    }


def _select_logistic_c(
    *,
    model_name: str,
    train_values: FloatArray,
    train_targets: IntArray,
    validation_values: FloatArray,
    validation_targets: IntArray,
    c_grid: Sequence[float],
    rows: list[dict[str, Any]],
) -> float:
    for c_value in c_grid:
        model = _fit_logistic(train_values, train_targets, float(c_value))
        probabilities = _positive_probability(model, validation_values)
        rows.append(
            _hyperparameter_row(model_name, float(c_value), validation_targets, probabilities)
        )
    return _mark_selected_hyperparameter(rows, model_name)


def _fit_logistic(values: FloatArray, targets: IntArray, c_value: float) -> LogisticRegression:
    model = LogisticRegression(
        C=c_value,
        max_iter=2_000,
        solver="lbfgs",
        random_state=651,
    )
    model.fit(values, targets)
    return model


def _positive_probability(model: LogisticRegression, values: FloatArray) -> FloatArray:
    return np.asarray(model.predict_proba(values)[:, 1], dtype=np.float64)


def _fit_offset_logistic(
    values: FloatArray,
    targets: IntArray,
    offset_probabilities: FloatArray,
    c_value: float,
) -> OffsetLogisticModel:
    offsets = logit(np.clip(offset_probabilities, 1e-8, 1.0 - 1e-8))
    target_values = targets.astype(np.float64)
    feature_count = values.shape[1]

    def objective(parameters: FloatArray) -> tuple[float, FloatArray]:
        intercept = parameters[0]
        coefficients = parameters[1:]
        linear = offsets + intercept + values @ coefficients
        probabilities = expit(linear)
        negative_log_likelihood = float(np.mean(np.logaddexp(0.0, linear) - target_values * linear))
        penalty = 0.5 * float(np.mean(coefficients**2)) / c_value
        residual = probabilities - target_values
        gradient = np.empty_like(parameters)
        gradient[0] = residual.mean()
        gradient[1:] = values.T @ residual / len(target_values) + coefficients / (
            c_value * feature_count
        )
        return negative_log_likelihood + penalty, gradient

    initial = np.zeros(feature_count + 1, dtype=np.float64)
    result = minimize(
        objective,
        initial,
        method="L-BFGS-B",
        jac=True,
        options={"maxiter": 500, "ftol": 1e-11},
    )
    if not result.success:
        raise RuntimeError(f"offset logistic fit failed: {result.message}")
    parameters = np.asarray(result.x, dtype=np.float64)
    return OffsetLogisticModel(
        intercept=float(parameters[0]),
        coefficients=parameters[1:],
    )


def _hyperparameter_row(
    model_name: str,
    c_value: float,
    targets: IntArray,
    probabilities: FloatArray,
) -> dict[str, Any]:
    return {
        "model_name": model_name,
        "c": c_value,
        "validation_log_loss": float(log_loss(targets, probabilities, labels=[0, 1])),
        "selected": False,
    }


def _mark_selected_hyperparameter(rows: list[dict[str, Any]], model_name: str) -> float:
    candidates = [row for row in rows if row["model_name"] == model_name]
    selected = min(candidates, key=lambda row: (float(row["validation_log_loss"]), float(row["c"])))
    selected["selected"] = True
    return float(selected["c"])


def _calibration_intercept_slope(
    targets: IntArray, probabilities: FloatArray
) -> tuple[float, float]:
    logits = logit(np.clip(probabilities, 1e-8, 1.0 - 1e-8)).reshape(-1, 1)
    model = LogisticRegression(C=1e6, max_iter=1_000, solver="lbfgs")
    model.fit(logits, targets)
    return float(model.intercept_[0]), float(model.coef_[0, 0])


def _expected_calibration_error(
    targets: IntArray,
    probabilities: FloatArray,
    bin_count: int,
) -> float:
    bins = np.linspace(0.0, 1.0, bin_count + 1)
    total = 0.0
    for lower, upper in zip(bins[:-1], bins[1:], strict=True):
        if upper == 1.0:
            mask = (probabilities >= lower) & (probabilities <= upper)
        else:
            mask = (probabilities >= lower) & (probabilities < upper)
        if not mask.any():
            continue
        total += float(mask.mean()) * abs(float(probabilities[mask].mean() - targets[mask].mean()))
    return total


def _race_probability_sum_error(
    probabilities: FloatArray,
    race_keys: ObjectArray,
    place_slots: IntArray,
) -> float:
    sums: dict[str, float] = {}
    slots: dict[str, int] = {}
    for probability, race_key, slot_count in zip(
        probabilities,
        race_keys,
        place_slots,
        strict=True,
    ):
        key = str(race_key)
        sums[key] = sums.get(key, 0.0) + float(probability)
        slots[key] = int(slot_count)
    return float(np.mean([abs(sums[key] - slots[key]) for key in sums]))


def _binary_log_losses(targets: IntArray, probabilities: FloatArray) -> FloatArray:
    clipped = np.clip(probabilities, 1e-8, 1.0 - 1e-8)
    return np.asarray(
        -(targets * np.log(clipped) + (1 - targets) * np.log(1.0 - clipped)),
        dtype=np.float64,
    )


def _model_verdict(bootstrap_rows: Sequence[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    evidence: list[dict[str, Any]] = []
    point_supported = False
    robust_supported = False
    for baseline_name, model_name in (
        ("M1_market", "M3_market_history"),
        ("M1_market", "M4_market_offset_history"),
        ("M1C_race_constrained_market", "M5_race_constrained_offset"),
    ):
        rows = [
            row
            for row in bootstrap_rows
            if row["candidate_name"] == model_name and row["baseline_name"] == baseline_name
        ]
        point_both = len(rows) == 2 and all(
            float(row["point_log_loss_delta"]) < 0.0 for row in rows
        )
        robust_both = point_both and all(float(row["ci95_high"]) < 0.0 for row in rows)
        evidence.append(
            {
                "baseline_name": baseline_name,
                "model_name": model_name,
                "point_improvement_in_validation_and_holdout": point_both,
                "bootstrap_ci_below_zero_in_validation_and_holdout": robust_both,
            }
        )
        point_supported = point_supported or point_both
        robust_supported = robust_supported or robust_both
    if robust_supported:
        return SIGNAL_SUPPORTED, evidence
    if point_supported:
        return SIGNAL_DIAGNOSTIC_ONLY, evidence
    return SIGNAL_NOT_SUPPORTED, evidence


def _findings_markdown(result: ComparisonResult) -> str:
    validation = {row["model_name"]: row for row in result.validation_metrics}
    holdout = {row["model_name"]: row for row in result.holdout_metrics}
    lines = [
        "# Phase651 Historical Ability Model Comparison",
        "",
        f"Final verdict: `{result.summary['final_verdict']}`",
        "",
        "## Target audit",
        "",
        f"- Joined rows: `{result.input_audit['joined_row_count']}`",
        "- Existing market-target mismatches against actual place slots: "
        f"`{result.input_audit['existing_market_target_mismatch_count']}`",
        "- The comparison uses the Phase650H place-slot-aware target for every model.",
        "",
        "## Log loss",
        "",
        "| Model | 2024 validation | 2025 holdout |",
        "|---|---:|---:|",
    ]
    for model_name in MODEL_NAMES:
        lines.append(
            f"| `{model_name}` | {validation[model_name]['log_loss']:.6f} | "
            f"{holdout[model_name]['log_loss']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Decision boundary",
            "",
            "- No ROI, payout, stake, threshold, or bet-selection result is used.",
            "- Incremental history is supported only when a market+history candidate beats M1 in "
            "both periods and both race-bootstrap 95% intervals remain below zero.",
            "- 2025 is evaluated once after 2024 hyperparameter selection.",
            "",
        ]
    )
    return "\n".join(lines)


def _duplicate_identity_count(
    connection: duckdb.DuckDBPyConnection,
    reader: str,
    path: Path,
) -> int:
    return _required_scalar_int(
        connection.execute(
            f"""
            SELECT coalesce(sum(row_count - 1), 0)
            FROM (
                SELECT race_key, horse_number, count(*) AS row_count
                FROM {reader}
                GROUP BY race_key, horse_number
                HAVING count(*) > 1
            )
            """,
            [str(path)],
        ).fetchone()
    )


def _required_date(value: object) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _optional_float(value: object) -> float:
    if value is None or value == "":
        return math.nan
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"cannot convert value to float: {value!r}")


def _required_scalar_int(row: tuple[Any, ...] | None) -> int:
    if row is None or not row:
        raise ValueError("query returned no scalar row")
    return int(row[0])


def _dataclass_dict(value: InputAudit) -> dict[str, Any]:
    return {
        "source_verdict": value.source_verdict,
        "history_row_count": value.history_row_count,
        "market_row_count": value.market_row_count,
        "joined_row_count": value.joined_row_count,
        "missing_history_row_count": value.missing_history_row_count,
        "missing_history_race_count": value.missing_history_race_count,
        "partially_joined_race_count": value.partially_joined_race_count,
        "fully_missing_unsupported_race_count": value.fully_missing_unsupported_race_count,
        "duplicate_history_identity_count": value.duplicate_history_identity_count,
        "duplicate_market_identity_count": value.duplicate_market_identity_count,
        "existing_market_target_mismatch_count": value.existing_market_target_mismatch_count,
        "mismatch_direction_counts": value.mismatch_direction_counts,
        "mismatch_by_active_field_size": value.mismatch_by_active_field_size,
        "joined_rows_by_year": value.joined_rows_by_year,
    }


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
