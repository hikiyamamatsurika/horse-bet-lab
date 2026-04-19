from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import cast

import duckdb
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier  # type: ignore[import-untyped]
from sklearn.linear_model import LogisticRegression  # type: ignore[import-untyped]
from sklearn.metrics import (  # type: ignore[import-untyped]
    brier_score_loss,
    log_loss,
    roc_auc_score,
)

from horse_bet_lab.config import ModelTrainConfig

SPLIT_ORDER = ("train", "valid", "test")
PREDICTION_COLUMNS = (
    "race_key",
    "horse_number",
    "split",
    "target_value",
    "pred_probability",
    "window_label",
)


@dataclass(frozen=True)
class SplitMetrics:
    auc: float
    logloss: float
    brier_score: float
    positive_rate: float
    prediction_mean: float
    prediction_std: float
    prediction_min: float
    prediction_p10: float
    prediction_p50: float
    prediction_p90: float
    prediction_max: float
    row_count: int


@dataclass(frozen=True)
class ModelTrainSummary:
    output_dir: Path
    metrics_by_split: dict[str, SplitMetrics]


@dataclass(frozen=True)
class RollingPairWindow:
    label: str
    valid_start_date: date
    valid_end_date: date
    test_start_date: date
    test_end_date: date


@dataclass(frozen=True)
class HistGradientBoostingEnsembleModel:
    models: tuple[HistGradientBoostingClassifier, ...]


def train_logistic_regression_baseline(config: ModelTrainConfig) -> ModelTrainSummary:
    validate_model_config(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    dataset = load_dataset(config)
    model = fit_model(
        model_name=config.model_name,
        X=dataset["train"]["X"],
        y=dataset["train"]["y"],
        max_iter=config.max_iter,
        model_params=config.model_params,
    )

    metrics_by_split: dict[str, SplitMetrics] = {}
    prediction_rows: list[dict[str, object]] = []
    calibration_rows: list[dict[str, object]] = []

    for split_name in SPLIT_ORDER:
        split_data = dataset[split_name]
        probabilities = predict_probabilities(model, split_data["X"])
        metrics_by_split[split_name] = compute_split_metrics(split_data["y"], probabilities)
        prediction_rows.extend(
            build_prediction_rows(
                split_name=split_name,
                race_keys=split_data["race_key"],
                horse_numbers=split_data["horse_number"],
                targets=split_data["y"],
                probabilities=probabilities,
            ),
        )
        calibration_rows.extend(
            build_calibration_rows(
                split_name=split_name,
                targets=split_data["y"],
                probabilities=probabilities,
            ),
        )

    write_metrics_artifact(config, model, metrics_by_split)
    write_predictions_artifact(config.output_dir / "predictions.csv", prediction_rows)
    write_calibration_artifact(config.output_dir / "calibration.csv", calibration_rows)

    return ModelTrainSummary(output_dir=config.output_dir, metrics_by_split=metrics_by_split)


def generate_rolling_pair_predictions(
    *,
    dataset_path: Path,
    model_name: str,
    feature_columns: tuple[str, ...],
    feature_transforms: tuple[str, ...],
    target_column: str,
    race_date_column: str,
    evaluation_window_pairs: tuple[RollingPairWindow, ...],
    output_path: Path,
    max_iter: int,
    model_params: dict[str, object],
) -> Path:
    validate_model_feature_spec(feature_columns, feature_transforms)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    try:
        schema_columns = tuple(
            row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(dataset_path)],
            ).fetchall()
        )
        required_columns = (
            "race_key",
            "horse_number",
            race_date_column,
            target_column,
        ) + feature_columns
        missing_columns = sorted(set(required_columns) - set(schema_columns))
        if missing_columns:
            raise ValueError(
                "dataset missing required columns for rolling retrain: "
                f"{missing_columns}",
            )

        rows = connection.execute(
            f"""
            SELECT
                race_key,
                horse_number,
                {race_date_column},
                {", ".join(feature_columns)},
                {target_column}
            FROM read_parquet(?)
            ORDER BY {race_date_column}, race_key, horse_number
            """,
            [str(dataset_path)],
        ).fetchall()
    finally:
        connection.close()

    race_keys = np.array([str(row[0]) for row in rows], dtype=str)
    horse_numbers = np.array([int(row[1]) for row in rows], dtype=np.int32)
    race_dates = np.array([row[2] for row in rows], dtype=object)
    feature_start = 3
    feature_end = feature_start + len(feature_columns)
    X_raw = np.array(
        [[float(value) for value in row[feature_start:feature_end]] for row in rows],
        dtype=np.float64,
    )
    X = apply_feature_transforms(X_raw, feature_transforms)
    y = np.array([int(row[feature_end]) for row in rows], dtype=np.int32)

    prediction_rows: list[dict[str, object]] = []
    for window in evaluation_window_pairs:
        train_mask = np.array(
            [race_date < window.valid_start_date for race_date in race_dates],
            dtype=bool,
        )
        valid_mask = np.array(
            [
                window.valid_start_date <= race_date <= window.valid_end_date
                for race_date in race_dates
            ],
            dtype=bool,
        )
        test_mask = np.array(
            [
                window.test_start_date <= race_date <= window.test_end_date
                for race_date in race_dates
            ],
            dtype=bool,
        )
        if int(np.sum(train_mask)) == 0:
            raise ValueError(f"rolling retrain train rows are empty for window: {window.label}")
        if int(np.sum(valid_mask)) == 0:
            raise ValueError(f"rolling retrain valid rows are empty for window: {window.label}")
        if int(np.sum(test_mask)) == 0:
            raise ValueError(f"rolling retrain test rows are empty for window: {window.label}")

        model = fit_model(
            model_name=model_name,
            X=X[train_mask],
            y=y[train_mask],
            max_iter=max_iter,
            model_params=model_params,
        )
        for split_name, mask in (("valid", valid_mask), ("test", test_mask)):
            probabilities = predict_probabilities(model, X[mask])
            prediction_rows.extend(
                build_prediction_rows(
                    split_name=split_name,
                    race_keys=race_keys[mask],
                    horse_numbers=horse_numbers[mask],
                    targets=y[mask],
                    probabilities=probabilities,
                    window_label=window.label,
                ),
            )

    write_predictions_artifact(output_path, prediction_rows)
    return output_path


def validate_model_config(config: ModelTrainConfig) -> None:
    if config.model_name not in {"logistic_regression", "hist_gradient_boosting_small"}:
        if config.model_name != "hist_gradient_boosting_small_ensemble":
            raise ValueError(f"unsupported model_name: {config.model_name}")
    if len(config.feature_columns) != len(config.feature_transforms):
        raise ValueError("feature_columns and feature_transforms must have the same length")
    validate_model_feature_spec(config.feature_columns, config.feature_transforms)


def validate_model_feature_spec(
    feature_columns: tuple[str, ...],
    feature_transforms: tuple[str, ...],
) -> None:
    supported_feature_sets = {
        ("win_odds",),
        ("win_odds", "popularity"),
        ("place_basis_odds",),
        ("place_basis_odds", "popularity"),
        ("win_odds", "place_basis_odds", "popularity"),
        ("win_odds", "place_basis_odds", "popularity", "headcount"),
        ("win_odds", "place_basis_odds", "popularity", "headcount", "place_slot_count"),
        (
            "win_odds",
            "place_basis_odds",
            "popularity",
            "headcount",
            "place_slot_count",
            "distance_m",
        ),
        ("win_odds", "place_basis_odds", "popularity", "log_place_minus_log_win"),
        ("win_odds", "place_basis_odds", "popularity", "implied_place_prob", "implied_win_prob"),
        (
            "win_odds",
            "place_basis_odds",
            "popularity",
            "implied_place_prob_minus_implied_win_prob",
        ),
        ("win_odds", "place_basis_odds", "popularity", "place_to_win_ratio"),
        ("win_odds", "popularity", "workout_gap_days", "workout_weekday_code"),
    }
    if feature_columns not in supported_feature_sets:
        raise ValueError(
            "baseline model currently supports feature_columns=['win_odds'] "
            "or ['win_odds', 'popularity'] "
            "or ['place_basis_odds'] "
            "or ['place_basis_odds', 'popularity'] "
            "or ['win_odds', 'place_basis_odds', 'popularity'] "
            "or ['win_odds', 'place_basis_odds', 'popularity', 'headcount'] "
            "or ['win_odds', 'place_basis_odds', 'popularity', 'headcount', 'place_slot_count'] "
            "or ['win_odds', 'place_basis_odds', 'popularity', 'headcount', "
            "'place_slot_count', 'distance_m'] "
            "or ['win_odds', 'place_basis_odds', 'popularity', 'log_place_minus_log_win'] "
            "or ['win_odds', 'place_basis_odds', 'popularity', "
            "'implied_place_prob', 'implied_win_prob'] "
            "or ['win_odds', 'place_basis_odds', 'popularity', "
            "'implied_place_prob_minus_implied_win_prob'] "
            "or ['win_odds', 'place_basis_odds', 'popularity', 'place_to_win_ratio'] "
            "or ['win_odds', 'popularity', 'workout_gap_days', 'workout_weekday_code'] only",
        )
    for column_name, transform_name in zip(
        feature_columns,
        feature_transforms,
        strict=True,
    ):
        if column_name == "win_odds" and transform_name not in {"identity", "log1p"}:
            raise ValueError("win_odds supports feature_transforms=['identity'|'log1p'] only")
        if column_name == "place_basis_odds" and transform_name not in {"identity", "log1p"}:
            raise ValueError(
                "place_basis_odds supports feature_transforms=['identity'|'log1p'] only",
            )
        if column_name == "popularity" and transform_name != "identity":
            raise ValueError("popularity currently supports feature_transforms=['identity'] only")
        if column_name == "headcount" and transform_name != "identity":
            raise ValueError("headcount currently supports feature_transforms=['identity'] only")
        if column_name == "place_slot_count" and transform_name != "identity":
            raise ValueError(
                "place_slot_count currently supports feature_transforms=['identity'] only",
            )
        if column_name == "distance_m" and transform_name != "identity":
            raise ValueError("distance_m currently supports feature_transforms=['identity'] only")
        if column_name in {
            "log_place_minus_log_win",
            "implied_place_prob",
            "implied_win_prob",
            "implied_place_prob_minus_implied_win_prob",
            "place_to_win_ratio",
        } and transform_name != "identity":
            raise ValueError(
                f"{column_name} currently supports feature_transforms=['identity'] only",
            )
        if column_name == "workout_gap_days" and transform_name != "identity":
            raise ValueError(
                "workout_gap_days currently supports feature_transforms=['identity'] only",
            )
        if column_name == "workout_weekday_code" and transform_name != "identity":
            raise ValueError(
                "workout_weekday_code currently supports feature_transforms=['identity'] only",
            )


def fit_model(
    *,
    model_name: str,
    X: np.ndarray,
    y: np.ndarray,
    max_iter: int,
    model_params: dict[str, object],
) -> LogisticRegression | HistGradientBoostingClassifier | HistGradientBoostingEnsembleModel:
    if model_name == "logistic_regression":
        model = LogisticRegression(max_iter=max_iter)
        model.fit(X, y)
        return model
    if model_name == "hist_gradient_boosting_small_ensemble":
        seed_values_raw = model_params.get("seeds", (42, 7, 99))
        if not isinstance(seed_values_raw, tuple):
            raise ValueError("hist_gradient_boosting_small_ensemble requires tuple seeds")
        models = tuple(
            fit_model(
                model_name="hist_gradient_boosting_small",
                X=X,
                y=y,
                max_iter=max_iter,
                model_params={**model_params, "random_state": int(seed)},
            )
            for seed in seed_values_raw
        )
        return HistGradientBoostingEnsembleModel(
            models=tuple(
                cast(HistGradientBoostingClassifier, model)
                for model in models
            ),
        )
    if model_name == "hist_gradient_boosting_small":
        max_depth = model_params.get("max_depth")
        max_leaf_nodes = model_params.get("max_leaf_nodes")
        min_samples_leaf = model_params.get("min_samples_leaf", 50)
        learning_rate = model_params.get("learning_rate", 0.05)
        random_state = model_params.get("random_state", 42)
        if max_depth is not None and not isinstance(max_depth, int | float):
            raise ValueError("max_depth must be numeric")
        if max_leaf_nodes is not None and not isinstance(max_leaf_nodes, int | float):
            raise ValueError("max_leaf_nodes must be numeric")
        if min_samples_leaf is not None and not isinstance(min_samples_leaf, int | float):
            raise ValueError("min_samples_leaf must be numeric")
        if learning_rate is not None and not isinstance(learning_rate, int | float):
            raise ValueError("learning_rate must be numeric")
        if random_state is not None and not isinstance(random_state, int | float):
            raise ValueError("random_state must be numeric")
        learning_rate_value = 0.05 if learning_rate is None else float(learning_rate)
        min_samples_leaf_value = 50 if min_samples_leaf is None else int(min_samples_leaf)
        model = HistGradientBoostingClassifier(
            max_iter=max_iter,
            learning_rate=learning_rate_value,
            max_depth=int(max_depth) if max_depth is not None else 3,
            max_leaf_nodes=int(max_leaf_nodes) if max_leaf_nodes is not None else None,
            min_samples_leaf=min_samples_leaf_value,
            random_state=int(random_state) if random_state is not None else None,
        )
        model.fit(X, y)
        return model
    raise ValueError(f"unsupported model_name: {model_name}")


def predict_probabilities(
    model: LogisticRegression | HistGradientBoostingClassifier | HistGradientBoostingEnsembleModel,
    X: np.ndarray,
) -> np.ndarray:
    if isinstance(model, HistGradientBoostingEnsembleModel):
        probabilities = np.vstack(
            [predict_probabilities(single_model, X) for single_model in model.models],
        )
        return cast(np.ndarray, np.mean(probabilities, axis=0))
    return cast(np.ndarray, model.predict_proba(X)[:, 1])


def load_dataset(config: ModelTrainConfig) -> dict[str, dict[str, np.ndarray]]:
    connection = duckdb.connect()
    try:
        schema_columns = tuple(
            row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(config.dataset_path)],
            ).fetchall()
        )
        required_columns = (
            "race_key",
            "horse_number",
            config.split_column,
            config.target_column,
        ) + config.feature_columns
        missing_columns = sorted(set(required_columns) - set(schema_columns))
        if missing_columns:
            raise ValueError(f"dataset missing required columns: {missing_columns}")

        dataset: dict[str, dict[str, np.ndarray]] = {}
        for split_name in SPLIT_ORDER:
            rows = connection.execute(
                f"""
                SELECT
                    race_key,
                    horse_number,
                    {", ".join(config.feature_columns)},
                    {config.target_column}
                FROM read_parquet(?)
                WHERE {config.split_column} = ?
                ORDER BY race_key, horse_number
                """,
                [str(config.dataset_path), split_name],
            ).fetchall()
            if not rows:
                raise ValueError(f"dataset split is empty: {split_name}")

            race_keys = np.array([str(row[0]) for row in rows], dtype=str)
            horse_numbers = np.array([int(row[1]) for row in rows], dtype=np.int32)
            feature_start = 2
            feature_end = feature_start + len(config.feature_columns)
            X_raw = np.array(
                [
                    [float(value) for value in row[feature_start:feature_end]]
                    for row in rows
                ],
                dtype=np.float64,
            )
            X = apply_feature_transforms(X_raw, config.feature_transforms)
            y = np.array([int(row[feature_end]) for row in rows], dtype=np.int32)
            dataset[split_name] = {
                "race_key": race_keys,
                "horse_number": horse_numbers,
                "X": X,
                "y": y,
            }
        return dataset
    finally:
        connection.close()


def compute_split_metrics(targets: np.ndarray, probabilities: np.ndarray) -> SplitMetrics:
    return SplitMetrics(
        auc=float(roc_auc_score(targets, probabilities)),
        logloss=float(log_loss(targets, probabilities, labels=[0, 1])),
        brier_score=float(brier_score_loss(targets, probabilities)),
        positive_rate=float(np.mean(targets)),
        prediction_mean=float(np.mean(probabilities)),
        prediction_std=float(np.std(probabilities)),
        prediction_min=float(np.min(probabilities)),
        prediction_p10=float(np.quantile(probabilities, 0.1)),
        prediction_p50=float(np.quantile(probabilities, 0.5)),
        prediction_p90=float(np.quantile(probabilities, 0.9)),
        prediction_max=float(np.max(probabilities)),
        row_count=int(len(targets)),
    )


def apply_feature_transforms(
    features: np.ndarray,
    feature_transforms: tuple[str, ...],
) -> np.ndarray:
    transformed = features.copy()
    for index, transform_name in enumerate(feature_transforms):
        if transform_name == "identity":
            continue
        if transform_name == "log1p":
            transformed[:, index] = np.log1p(transformed[:, index])
            continue
        raise ValueError(f"unsupported feature transform: {transform_name}")
    return transformed


def build_prediction_rows(
    *,
    split_name: str,
    race_keys: np.ndarray,
    horse_numbers: np.ndarray,
    targets: np.ndarray,
    probabilities: np.ndarray,
    window_label: str | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for race_key, horse_number, target_value, probability in zip(
        race_keys,
        horse_numbers,
        targets,
        probabilities,
        strict=True,
    ):
        rows.append(
            {
                "race_key": str(race_key),
                "horse_number": int(horse_number),
                "split": split_name,
                "target_value": int(target_value),
                "pred_probability": float(probability),
                "window_label": window_label,
            },
        )
    return rows


def build_calibration_rows(
    *,
    split_name: str,
    targets: np.ndarray,
    probabilities: np.ndarray,
) -> list[dict[str, object]]:
    edges = np.linspace(0.0, 1.0, 11)
    rows: list[dict[str, object]] = []
    bin_ids = np.clip(np.digitize(probabilities, edges[1:-1], right=False), 0, 9)
    for bin_id in range(10):
        mask = bin_ids == bin_id
        count = int(np.sum(mask))
        if count == 0:
            continue
        rows.append(
            {
                "split": split_name,
                "bin_id": bin_id,
                "bin_start": float(edges[bin_id]),
                "bin_end": float(edges[bin_id + 1]),
                "row_count": count,
                "avg_prediction": float(np.mean(probabilities[mask])),
                "positive_rate": float(np.mean(targets[mask])),
            },
        )
    return rows


def write_metrics_artifact(
    config: ModelTrainConfig,
    model: LogisticRegression | HistGradientBoostingClassifier | HistGradientBoostingEnsembleModel,
    metrics_by_split: dict[str, SplitMetrics],
) -> None:
    model_payload: dict[str, object] = {
        "classes": [0, 1],
    }
    if isinstance(model, LogisticRegression):
        model_payload["intercept"] = [float(value) for value in model.intercept_.tolist()]
        model_payload["coefficients"] = [float(value) for value in model.coef_[0].tolist()]
        model_payload["classes"] = [int(value) for value in model.classes_.tolist()]
    elif isinstance(model, HistGradientBoostingEnsembleModel):
        model_payload["ensemble_size"] = len(model.models)
        model_payload["members"] = [
            {
                "learning_rate": float(member.learning_rate),
                "max_iter": int(member.max_iter),
                "max_depth": member.max_depth,
                "max_leaf_nodes": member.max_leaf_nodes,
                "min_samples_leaf": int(member.min_samples_leaf),
            }
            for member in model.models
        ]
    else:
        model_payload["learning_rate"] = float(model.learning_rate)
        model_payload["max_iter"] = int(model.max_iter)
        model_payload["max_depth"] = model.max_depth
        model_payload["max_leaf_nodes"] = model.max_leaf_nodes
        model_payload["min_samples_leaf"] = int(model.min_samples_leaf)

    payload = {
        "training": {
            "name": config.name,
            "dataset_path": str(config.dataset_path),
            "model_name": config.model_name,
            "feature_columns": list(config.feature_columns),
            "feature_transforms": list(config.feature_transforms),
            "target_column": config.target_column,
            "split_column": config.split_column,
            "max_iter": config.max_iter,
            "model_params": config.model_params,
        },
        "model": model_payload,
        "metrics": {
            split_name: asdict(metrics)
            for split_name, metrics in metrics_by_split.items()
        },
    }
    (config.output_dir / "metrics.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def write_predictions_artifact(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=PREDICTION_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_calibration_artifact(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = (
        "split",
        "bin_id",
        "bin_start",
        "bin_end",
        "row_count",
        "avg_prediction",
        "positive_rate",
    )
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_prediction_metrics(
    predictions_path: Path,
    *,
    split_name: str,
    window_label: str | None = None,
) -> SplitMetrics:
    connection = duckdb.connect()
    try:
        where_clauses = ["split = ?"]
        parameters: list[object] = [split_name]
        if window_label is not None:
            where_clauses.append("window_label = ?")
            parameters.append(window_label)
        row = connection.execute(
            f"""
            SELECT
                list(target_value ORDER BY race_key, horse_number),
                list(pred_probability ORDER BY race_key, horse_number)
            FROM read_csv_auto(?, header = true)
            WHERE {" AND ".join(where_clauses)}
            """,
            [str(predictions_path), *parameters],
        ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            raise ValueError(f"no prediction rows found for split={split_name!r}")
        targets = np.array([int(value) for value in row[0]], dtype=np.int32)
        probabilities = np.array([float(value) for value in row[1]], dtype=np.float64)
        return compute_split_metrics(targets, probabilities)
    finally:
        connection.close()
