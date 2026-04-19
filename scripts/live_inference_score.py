from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np

from horse_bet_lab.model.service import (
    apply_feature_transforms,
    fit_model,
    predict_probabilities,
    validate_model_feature_spec,
)


@dataclass(frozen=True)
class ReferenceModelConfig:
    dataset_path: Path
    historical_duckdb_path: Path | None
    model_name: str
    feature_columns: tuple[str, ...]
    feature_transforms: tuple[str, ...]
    target_column: str
    split_column: str
    training_splits: tuple[str, ...]
    max_iter: int
    model_params: dict[str, object]
    live_feature_aliases: dict[str, str]


@dataclass(frozen=True)
class LiveInferenceConfig:
    name: str
    race_name: str
    race_date: str
    input_path: Path
    output_path: Path
    reference_model: ReferenceModelConfig
    input_source: str
    source_url: str
    source_timestamp: str
    proxy_rule: str
    caveat: tuple[str, ...]


REQUIRED_LIVE_COLUMNS = (
    "race_key",
    "horse_number",
    "horse_name",
    "place_odds_min",
    "place_odds_max",
)

FORBIDDEN_LIVE_COLUMNS = {
    "target_value",
    "target_name",
    "split",
    "pred_probability",
    "proxy_score",
    "score",
    "rank",
    "finish_position",
    "result_date",
    "payout",
    "place_basis_odds",
    "win_basis_odds",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train the reference model from a historical dataset and score live inputs.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a live inference TOML config.",
    )
    return parser


def load_config(path: Path) -> LiveInferenceConfig:
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    run = payload["live_inference"]
    reference = run["reference_model"]
    return LiveInferenceConfig(
        name=str(run["name"]),
        race_name=str(run["race_name"]),
        race_date=str(run["race_date"]),
        input_path=Path(str(run["input_path"])),
        output_path=Path(str(run["output_path"])),
        input_source=str(run["input_source"]),
        source_url=str(run["source_url"]),
        source_timestamp=str(run["source_timestamp"]),
        proxy_rule=str(run["proxy_rule"]),
        caveat=tuple(str(value) for value in run.get("caveat", [])),
        reference_model=ReferenceModelConfig(
            dataset_path=Path(str(reference["dataset_path"])),
            historical_duckdb_path=(
                Path(str(reference["historical_duckdb_path"]))
                if reference.get("historical_duckdb_path") is not None
                else None
            ),
            model_name=str(reference["model_name"]),
            feature_columns=tuple(str(value) for value in reference["feature_columns"]),
            feature_transforms=tuple(str(value) for value in reference["feature_transforms"]),
            target_column=str(reference.get("target_column", "target_value")),
            split_column=str(reference.get("split_column", "split")),
            training_splits=tuple(
                str(value) for value in reference.get("training_splits", ("train", "valid", "test"))
            ),
            max_iter=int(reference.get("max_iter", 1000)),
            model_params=dict(reference.get("model_params", {})),
            live_feature_aliases={
                str(key): str(value)
                for key, value in dict(reference.get("live_feature_aliases", {})).items()
            },
        ),
    )


def load_training_matrix(config: ReferenceModelConfig) -> tuple[np.ndarray, np.ndarray]:
    validate_model_feature_spec(config.feature_columns, config.feature_transforms)
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
            config.split_column,
            config.target_column,
        ) + config.feature_columns
        missing_columns = sorted(set(required_columns) - set(schema_columns))
        if missing_columns:
            raise ValueError(f"dataset missing required columns: {missing_columns}")

        split_placeholders = ", ".join(["?"] * len(config.training_splits))
        rows = connection.execute(
            f"""
            SELECT
                {", ".join(config.feature_columns)},
                {config.target_column}
            FROM read_parquet(?)
            WHERE {config.split_column} IN ({split_placeholders})
            ORDER BY race_key, horse_number
            """,
            [str(config.dataset_path), *config.training_splits],
        ).fetchall()
    finally:
        connection.close()

    if not rows:
        raise ValueError("no training rows found for configured training_splits")

    feature_count = len(config.feature_columns)
    X_raw = np.array(
        [
            [float(value) for value in row[:feature_count]]
            for row in rows
        ],
        dtype=np.float64,
    )
    y = np.array([int(row[feature_count]) for row in rows], dtype=np.int32)
    return apply_feature_transforms(X_raw, config.feature_transforms), y


def load_live_rows(config: LiveInferenceConfig) -> tuple[list[dict[str, str]], np.ndarray]:
    if not config.input_path.exists():
        raise FileNotFoundError(f"live input file does not exist: {config.input_path}")

    rows: list[dict[str, str]] = []
    with config.input_path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError("live input CSV is missing a header row")
        forbidden_columns = sorted(set(reader.fieldnames) & FORBIDDEN_LIVE_COLUMNS)
        if forbidden_columns:
            raise ValueError(f"live input CSV contains forbidden leakage columns: {forbidden_columns}")
        required_columns = list(REQUIRED_LIVE_COLUMNS)
        for column_name in config.reference_model.feature_columns:
            required_columns.append(
                config.reference_model.live_feature_aliases.get(column_name, column_name),
            )
        missing_columns = sorted(set(required_columns) - set(reader.fieldnames))
        if missing_columns:
            raise ValueError(f"live input CSV missing required columns: {missing_columns}")
        for row in reader:
            if not row:
                continue
            rows.append({key: (value or "").strip() for key, value in row.items() if key is not None})

    if not rows:
        raise ValueError("live input CSV has no data rows")

    for row in rows:
        validate_live_row_schema(row)
        place_odds_min = parse_required_float(row, "place_odds_min")
        place_odds_max = parse_required_float(row, "place_odds_max")
        if place_odds_min > place_odds_max:
            raise ValueError(
                "live input row has place_odds_min > place_odds_max for "
                f"race_key={row.get('race_key')} horse_number={row.get('horse_number')}",
            )
        raw_proxy_value = row.get("place_basis_odds_proxy", "").strip()
        computed_proxy = (place_odds_min + place_odds_max) / 2.0
        if raw_proxy_value != "":
            provided_proxy = float(raw_proxy_value)
            if not math.isclose(provided_proxy, computed_proxy, rel_tol=0.0, abs_tol=1e-9):
                raise ValueError(
                    "live input row has inconsistent place_basis_odds_proxy for "
                    f"race_key={row.get('race_key')} horse_number={row.get('horse_number')}",
                )
        row["place_basis_odds_proxy"] = str(computed_proxy)
        assert_no_leakage_columns(row)

    X_raw = np.array(
        [
            [
                parse_required_float(
                    row,
                    config.reference_model.live_feature_aliases.get(column_name, column_name),
                )
                for column_name in config.reference_model.feature_columns
            ]
            for row in rows
        ],
        dtype=np.float64,
    )
    return rows, apply_feature_transforms(X_raw, config.reference_model.feature_transforms)


def validate_live_row_schema(row: dict[str, str]) -> None:
    for column_name in REQUIRED_LIVE_COLUMNS:
        if row.get(column_name, "") == "":
            raise ValueError(f"live input CSV has empty {column_name!r}")
    race_key = row["race_key"]
    if len(race_key) != 8 or not race_key.isdigit():
        raise ValueError(f"live input row has invalid race_key: {race_key!r}")
    horse_number = parse_required_int(row, "horse_number")
    if horse_number <= 0:
        raise ValueError(f"live input row has non-positive horse_number: {horse_number}")
    horse_name = row["horse_name"].strip()
    if horse_name == "":
        raise ValueError("live input row has empty horse_name")
    win_odds = parse_required_float(row, "win_odds")
    popularity = parse_required_int(row, "popularity")
    if win_odds <= 0.0:
        raise ValueError(f"live input row has non-positive win_odds: {win_odds}")
    if popularity <= 0:
        raise ValueError(f"live input row has non-positive popularity: {popularity}")
    place_odds_min = parse_required_float(row, "place_odds_min")
    place_odds_max = parse_required_float(row, "place_odds_max")
    if place_odds_min <= 0.0 or place_odds_max <= 0.0:
        raise ValueError("live input row has non-positive place odds range")


def assert_no_leakage_columns(row: dict[str, str]) -> None:
    leakage_columns = sorted(set(row) & FORBIDDEN_LIVE_COLUMNS)
    if leakage_columns:
        raise ValueError(f"live input row contains forbidden leakage columns: {leakage_columns}")


def parse_required_float(row: dict[str, str], column_name: str) -> float:
    raw_value = row.get(column_name, "").strip()
    if raw_value == "":
        raise ValueError(
            f"live input row is missing {column_name!r} for "
            f"race_key={row.get('race_key')} horse_number={row.get('horse_number')}",
        )
    value = float(raw_value)
    if not math.isfinite(value):
        raise ValueError(
            f"live input row has non-finite {column_name!r} for "
            f"race_key={row.get('race_key')} horse_number={row.get('horse_number')}",
        )
    return value


def parse_required_int(row: dict[str, str], column_name: str) -> int:
    raw_value = row.get(column_name, "").strip()
    if raw_value == "":
        raise ValueError(
            f"live input row is missing {column_name!r} for "
            f"race_key={row.get('race_key')} horse_number={row.get('horse_number')}",
        )
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"live input row has non-integer {column_name!r} for "
            f"race_key={row.get('race_key')} horse_number={row.get('horse_number')}",
        ) from exc
    return value


def build_output_rows(
    live_rows: list[dict[str, str]],
    probabilities: np.ndarray,
) -> list[dict[str, object]]:
    grouped: dict[str, list[tuple[dict[str, str], float]]] = {}
    for row, probability in zip(live_rows, probabilities, strict=True):
        grouped.setdefault(row["race_key"], []).append((row, float(probability)))

    output_rows: list[dict[str, object]] = []
    for race_key in sorted(grouped):
        ranked = sorted(
            grouped[race_key],
            key=lambda item: (-item[1], int(item[0]["horse_number"])),
        )
        for rank, (row, probability) in enumerate(ranked, start=1):
            if "place_basis_odds_proxy" in row:
                place_basis_odds_proxy = float(row["place_basis_odds_proxy"])
            else:
                place_basis_odds_proxy = float(row["place_basis_odds"])
            output_rows.append(
                {
                    "race_key": row["race_key"],
                    "horse_number": int(row["horse_number"]),
                    "horse_name": row["horse_name"],
                    "win_odds": float(row["win_odds"]),
                    "place_basis_odds_proxy": place_basis_odds_proxy,
                    "popularity": int(float(row["popularity"])),
                    "place_odds_min": float(row["place_odds_min"]),
                    "place_odds_max": float(row["place_odds_max"]),
                    "proxy_score": probability,
                    "rank": rank,
                },
            )
    return output_rows


def write_output(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = (
        "race_key",
        "horse_number",
        "horse_name",
        "win_odds",
        "place_basis_odds_proxy",
        "popularity",
        "place_odds_min",
        "place_odds_max",
        "proxy_score",
        "rank",
    )
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_manifest(config: LiveInferenceConfig, path: Path) -> None:
    manifest = {
        "race_name": config.race_name,
        "race_date": config.race_date,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "model_lineage": {
            "line_name": config.name,
            "dataset_path": str(config.reference_model.dataset_path),
            "model_name": config.reference_model.model_name,
            "training_splits": list(config.reference_model.training_splits),
            "feature_transforms": list(config.reference_model.feature_transforms),
        },
        "input_source": config.input_source,
        "source_url": config.source_url,
        "source_timestamp": config.source_timestamp,
        "input_csv_hash": sha256_digest(config.input_path),
        "feature_list": list(config.reference_model.feature_columns),
        "proxy_rule": config.proxy_rule,
        "caveat": list(config.caveat),
    }
    path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")


def write_summary(config: LiveInferenceConfig, rows: list[dict[str, object]], path: Path) -> None:
    top_rows = rows[:5]
    lines = [
        f"proxy_live summary: {config.race_name} ({config.race_date})",
        "",
        "Top 5",
    ]
    for row in top_rows:
        lines.append(
            f"{row['rank']}. {row['horse_name']} "
            f"proxy_score={float(row['proxy_score']):.4f} "
            f"win_odds={float(row['win_odds']):.1f} "
            f"place_basis_odds_proxy={float(row['place_basis_odds_proxy']):.2f} "
            f"popularity={int(row['popularity'])}"
        )
    lines.extend(
        [
            "",
            "Notes",
            "- This is a proxy_live relative ranking score, not a calibrated probability.",
            "- JRDB OZ is not used here; place_basis_odds_proxy is the midpoint of the public place odds range.",
            "- Do not compare this output side-by-side with mainline backtest scores.",
            "- Input schema validation and leakage checks are enforced before scoring.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_proxy_gap_diagnostic(
    config: LiveInferenceConfig,
    live_rows: list[dict[str, str]],
    path: Path,
) -> None:
    duckdb_path = config.reference_model.historical_duckdb_path
    if duckdb_path is None or not duckdb_path.exists():
        payload = {
            "status": "skipped",
            "reason": "historical_duckdb_path_missing",
            "row_count": 0,
            "metrics": {},
        }
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return

    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        input_rows = [
            (
                row["race_key"],
                int(row["horse_number"]),
                float(row["place_basis_odds_proxy"]),
            )
            for row in live_rows
        ]
        if not input_rows:
            payload = {
                "status": "skipped",
                "reason": "no_input_rows",
                "row_count": 0,
                "metrics": {},
            }
        else:
            connection.execute(
                """
                CREATE TEMP TABLE live_proxy_input (
                    race_key VARCHAR,
                    horse_number INTEGER,
                    place_basis_odds_proxy DOUBLE
                )
                """,
            )
            connection.executemany(
                "INSERT INTO live_proxy_input VALUES (?, ?, ?)",
                input_rows,
            )
            rows = connection.execute(
                """
                SELECT
                    i.place_basis_odds_proxy,
                    o.place_basis_odds
                FROM live_proxy_input i
                INNER JOIN jrdb_oz_staging o
                    ON i.race_key = o.race_key
                    AND i.horse_number = o.horse_number
                """,
            ).fetchall()
            if not rows:
                payload = {
                    "status": "no_overlap",
                    "reason": "no_matching_historical_oz_rows",
                    "row_count": 0,
                    "metrics": {},
                }
            else:
                proxy_values = np.array([float(row[0]) for row in rows], dtype=np.float64)
                true_values = np.array([float(row[1]) for row in rows], dtype=np.float64)
                diff = proxy_values - true_values
                payload = {
                    "status": "ok",
                    "reason": None,
                    "row_count": int(len(diff)),
                    "metrics": {
                        "mean_diff": float(np.mean(diff)),
                        "median_diff": float(np.median(diff)),
                        "p10_diff": float(np.quantile(diff, 0.1)),
                        "p25_diff": float(np.quantile(diff, 0.25)),
                        "p75_diff": float(np.quantile(diff, 0.75)),
                        "p90_diff": float(np.quantile(diff, 0.9)),
                        "mean_abs_diff": float(np.mean(np.abs(diff))),
                    },
                }
    finally:
        connection.close()

    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def sha256_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)

    X_train, y_train = load_training_matrix(config.reference_model)
    model = fit_model(
        model_name=config.reference_model.model_name,
        X=X_train,
        y=y_train,
        max_iter=config.reference_model.max_iter,
        model_params=config.reference_model.model_params,
    )

    live_rows, X_live = load_live_rows(config)
    probabilities = predict_probabilities(model, X_live)
    output_rows = build_output_rows(live_rows, probabilities)
    write_output(config.output_path, output_rows)
    write_manifest(config, config.output_path.parent / "run_manifest.json")
    write_summary(config, output_rows, config.output_path.parent / "summary.txt")
    write_proxy_gap_diagnostic(
        config,
        live_rows,
        config.output_path.parent / "proxy_feature_gap_summary.json",
    )

    print(
        "Live inference completed: "
        f"name={config.name} rows={len(output_rows)} output_path={config.output_path}",
    )


if __name__ == "__main__":
    main()
