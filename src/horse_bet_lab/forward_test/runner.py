from __future__ import annotations

import argparse
import csv
import hashlib
import json
import tomllib
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

from horse_bet_lab.evaluation.bet_logic_only import (
    LogicBetRow,
    apply_overlay_variant_rows,
    build_final_bet_instruction_rows,
    build_final_race_instruction_rows,
    filter_guard_variant_rows,
)
from horse_bet_lab.features.provenance import (
    FEATURE_CONTRACT_VERSION,
    build_feature_provenance_payload,
    write_feature_provenance_sidecar,
)
from horse_bet_lab.features.registry import validate_model_feature_missing_values
from horse_bet_lab.forward_test.contracts import (
    PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    PLACE_FORWARD_TEST_NO_BET_REASONS,
    PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
    PlaceForwardBetDecisionRecord,
    PlaceForwardInputRecord,
    PlaceForwardPredictionOutputRecord,
    build_place_forward_artifact_provenance,
    validate_place_forward_bet_decision_record,
    validate_place_forward_input_records,
    validate_place_forward_prediction_output_record,
)
from horse_bet_lab.model.service import (
    apply_feature_transforms,
    fit_model,
    predict_probabilities,
    validate_model_feature_columns,
    validate_model_feature_spec,
)

SUPPORTED_FORWARD_TEST_FEATURE_COLUMNS = frozenset({"win_odds", "place_basis_odds", "popularity"})
FORWARD_TEST_WINDOW_LABEL = "forward_live"


@dataclass(frozen=True)
class ForwardReferenceModelConfig:
    dataset_path: Path
    model_name: str
    feature_columns: tuple[str, ...]
    feature_transforms: tuple[str, ...]
    target_column: str
    split_column: str
    training_splits: tuple[str, ...]
    max_iter: int
    model_params: dict[str, object]
    model_version: str


@dataclass(frozen=True)
class ForwardBetLogicConfig:
    selection_metric: str
    threshold: float
    stake_per_bet: float
    stronger_guard_edge_surcharge: float
    candidate_logic_id: str
    fallback_logic_id: str
    formal_domain_mapping_confirmed: bool


@dataclass(frozen=True)
class PlaceForwardTestConfig:
    name: str
    input_path: Path
    output_dir: Path
    reference_model: ForwardReferenceModelConfig
    bet_logic: ForwardBetLogicConfig
    input_schema_version: str


@dataclass(frozen=True)
class PlaceForwardRunResult:
    output_dir: Path
    input_record_count: int
    prediction_record_count: int
    decision_record_count: int
    candidate_bet_count: int
    no_bet_count: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the place-only forward-test Phase 1 runner.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a place forward-test TOML config.",
    )
    return parser


def load_config(path: Path) -> PlaceForwardTestConfig:
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    run = payload["place_forward_test"]
    reference = run["reference_model"]
    bet_logic = run["bet_logic"]
    feature_columns = tuple(str(value) for value in reference["feature_columns"])
    validate_model_feature_columns(
        feature_columns,
        context=f"place_forward_test.reference_model feature_columns in {path}",
    )
    unsupported_columns = sorted(set(feature_columns) - SUPPORTED_FORWARD_TEST_FEATURE_COLUMNS)
    if unsupported_columns:
        raise ValueError(
            "place forward-test Phase 1 supports only feature_columns drawn from "
            f"{sorted(SUPPORTED_FORWARD_TEST_FEATURE_COLUMNS)}, got unsupported columns: "
            f"{unsupported_columns}"
        )
    return PlaceForwardTestConfig(
        name=str(run["name"]),
        input_path=Path(str(run["input_path"])),
        output_dir=Path(str(run["output_dir"])),
        input_schema_version=str(
            run.get("input_schema_version", PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION)
        ),
        reference_model=ForwardReferenceModelConfig(
            dataset_path=Path(str(reference["dataset_path"])),
            model_name=str(reference["model_name"]),
            feature_columns=feature_columns,
            feature_transforms=tuple(str(value) for value in reference["feature_transforms"]),
            target_column=str(reference.get("target_column", "target_value")),
            split_column=str(reference.get("split_column", "split")),
            training_splits=tuple(
                str(value) for value in reference.get("training_splits", ("train", "valid", "test"))
            ),
            max_iter=int(reference.get("max_iter", 1000)),
            model_params=dict(reference.get("model_params", {})),
            model_version=str(reference["model_version"]),
        ),
        bet_logic=ForwardBetLogicConfig(
            selection_metric=str(bet_logic.get("selection_metric", "edge")),
            threshold=float(bet_logic["threshold"]),
            stake_per_bet=float(bet_logic.get("stake_per_bet", 100.0)),
            stronger_guard_edge_surcharge=float(
                bet_logic.get("stronger_guard_edge_surcharge", 0.01)
            ),
            candidate_logic_id=str(
                bet_logic.get(
                    "candidate_logic_id",
                    "guard_0_01_plus_proxy_domain_overlay",
                )
            ),
            fallback_logic_id=str(
                bet_logic.get("fallback_logic_id", "no_bet_guard_stronger")
            ),
            formal_domain_mapping_confirmed=bool(
                bet_logic.get("formal_domain_mapping_confirmed", True)
            ),
        ),
    )


def load_training_matrix(config: ForwardReferenceModelConfig) -> tuple[np.ndarray, np.ndarray]:
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
            raise ValueError(
                "dataset missing required columns for forward-test reference model: "
                f"{missing_columns}"
            )
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
        raise ValueError("no training rows found for place forward-test reference model")

    feature_count = len(config.feature_columns)
    validate_model_feature_missing_values(
        config.feature_columns,
        [row[:feature_count] for row in rows],
        context="place forward-test reference model dataset",
        row_labels=[
            f"training_row_index={index}"
            for index in range(1, len(rows) + 1)
        ],
    )
    X_raw = np.array(
        [[float(value) for value in row[:feature_count]] for row in rows],
        dtype=np.float64,
    )
    y = np.array([int(row[feature_count]) for row in rows], dtype=np.int32)
    return apply_feature_transforms(X_raw, config.feature_transforms), y


def load_input_records(path: Path) -> tuple[PlaceForwardInputRecord, ...]:
    if not path.exists():
        raise FileNotFoundError(f"place forward-test input CSV does not exist: {path}")
    with path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError("place forward-test input CSV is missing a header row")
        records = tuple(build_input_record(row, row_index=index) for index, row in enumerate(reader, start=1))
    if not records:
        raise ValueError("place forward-test input CSV has no data rows")
    return validate_place_forward_input_records(records)


def build_input_record(row: dict[str, str], *, row_index: int) -> PlaceForwardInputRecord:
    cleaned = {key: (value or "").strip() for key, value in row.items() if key is not None}
    def optional_text(field_name: str) -> str | None:
        value = cleaned.get(field_name, "")
        return value if value != "" else None

    def optional_float(field_name: str) -> float | None:
        value = optional_text(field_name)
        if value is None:
            return None
        try:
            return float(value)
        except ValueError as exc:
            raise ValueError(
                f"input row {row_index} has non-float {field_name!r}: {value!r}"
            ) from exc

    def optional_int(field_name: str) -> int | None:
        value = optional_text(field_name)
        if value is None:
            return None
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(
                f"input row {row_index} has non-integer {field_name!r}: {value!r}"
            ) from exc

    horse_number_raw = cleaned.get("horse_number", "")
    try:
        horse_number = int(horse_number_raw)
    except ValueError as exc:
        raise ValueError(
            f"input row {row_index} has non-integer 'horse_number': {horse_number_raw!r}"
        ) from exc

    retry_count_raw = cleaned.get("retry_count", "")
    try:
        retry_count = int(retry_count_raw)
    except ValueError as exc:
        raise ValueError(
            f"input row {row_index} has non-integer 'retry_count': {retry_count_raw!r}"
        ) from exc

    return PlaceForwardInputRecord(
        race_key=cleaned.get("race_key", ""),
        horse_number=horse_number,
        win_odds=optional_float("win_odds"),
        place_basis_odds=optional_float("place_basis_odds"),
        popularity=optional_int("popularity"),
        odds_observation_timestamp=cleaned.get("odds_observation_timestamp", ""),
        input_source_name=cleaned.get("input_source_name", ""),
        input_source_url=optional_text("input_source_url"),
        input_source_timestamp=optional_text("input_source_timestamp"),
        carrier_identity=cleaned.get("carrier_identity", ""),
        snapshot_status=cleaned.get("snapshot_status", ""),
        retry_count=retry_count,
        timeout_seconds=optional_float("timeout_seconds"),
        snapshot_failure_reason=optional_text("snapshot_failure_reason"),
        popularity_input_source=optional_text("popularity_input_source"),
        popularity_contract_status=optional_text("popularity_contract_status"),
        input_schema_version=cleaned.get(
            "input_schema_version",
            PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
        ),
    )


def build_live_feature_matrix(
    records: tuple[PlaceForwardInputRecord, ...],
    *,
    feature_columns: tuple[str, ...],
) -> np.ndarray:
    rows: list[tuple[float, ...]] = []
    for record in records:
        feature_row: list[float] = []
        for column_name in feature_columns:
            if column_name == "win_odds":
                if record.win_odds is None:
                    raise ValueError(
                        "forward-test record requires win_odds for model input: "
                        f"race_key={record.race_key} horse_number={record.horse_number}"
                    )
                feature_row.append(record.win_odds)
            elif column_name == "place_basis_odds":
                if record.place_basis_odds is None:
                    raise ValueError(
                        "forward-test record requires place_basis_odds for model input: "
                        f"race_key={record.race_key} horse_number={record.horse_number}"
                    )
                feature_row.append(record.place_basis_odds)
            elif column_name == "popularity":
                if record.popularity is None:
                    raise ValueError(
                        "forward-test model_feature_columns include popularity but the live "
                        "record does not provide it: "
                        f"race_key={record.race_key} horse_number={record.horse_number}"
                    )
                feature_row.append(float(record.popularity))
            else:
                raise ValueError(
                    "unsupported forward-test feature column: "
                    f"{column_name!r}; supported columns are "
                    f"{sorted(SUPPORTED_FORWARD_TEST_FEATURE_COLUMNS)}"
                )
        rows.append(tuple(feature_row))
    validate_model_feature_missing_values(
        feature_columns,
        rows,
        context="place forward-test live feature matrix",
        row_labels=[
            f"race_key={record.race_key} horse_number={record.horse_number}"
            for record in records
        ],
    )
    return np.array(rows, dtype=np.float64)


def run_place_forward_test(config: PlaceForwardTestConfig) -> PlaceForwardRunResult:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    input_records = load_input_records(config.input_path)
    write_csv_records(config.output_dir / "input_snapshot_records.csv", input_records)
    write_json_records(config.output_dir / "input_snapshot_records.json", input_records)

    X_train, y_train = load_training_matrix(config.reference_model)
    model = fit_model(
        model_name=config.reference_model.model_name,
        X=X_train,
        y=y_train,
        max_iter=config.reference_model.max_iter,
        model_params=config.reference_model.model_params,
    )

    ok_records = tuple(record for record in input_records if record.snapshot_status == "ok")
    failed_records = tuple(record for record in input_records if record.snapshot_status != "ok")

    prediction_records: list[PlaceForwardPredictionOutputRecord] = []
    decision_records: list[PlaceForwardBetDecisionRecord] = []

    if ok_records:
        X_live_raw = build_live_feature_matrix(
            ok_records,
            feature_columns=config.reference_model.feature_columns,
        )
        X_live = apply_feature_transforms(
            X_live_raw,
            config.reference_model.feature_transforms,
        )
        probabilities = predict_probabilities(model, X_live)
        ok_prediction_records = build_prediction_output_records(
            ok_records,
            probabilities,
            config=config,
        )
        prediction_records.extend(ok_prediction_records)
        decision_records.extend(
            build_bet_decision_records(
                ok_records=ok_records,
                prediction_records=tuple(ok_prediction_records),
                config=config,
            )
        )

    decision_records.extend(build_snapshot_failure_decision_records(failed_records, config=config))

    write_csv_records(config.output_dir / "prediction_output_records.csv", prediction_records)
    write_json_records(config.output_dir / "prediction_output_records.json", prediction_records)
    write_csv_records(config.output_dir / "bet_decision_records.csv", decision_records)
    write_json_records(config.output_dir / "bet_decision_records.json", decision_records)

    run_manifest_path = config.output_dir / "run_manifest.json"
    write_run_manifest(
        config,
        input_records=tuple(input_records),
        prediction_records=tuple(prediction_records),
        decision_records=tuple(decision_records),
        path=run_manifest_path,
    )
    write_summary(
        config,
        decision_records=tuple(decision_records),
        path=config.output_dir / "summary.txt",
    )

    return PlaceForwardRunResult(
        output_dir=config.output_dir,
        input_record_count=len(input_records),
        prediction_record_count=len(prediction_records),
        decision_record_count=len(decision_records),
        candidate_bet_count=sum(1 for record in decision_records if record.bet_action == "bet"),
        no_bet_count=sum(1 for record in decision_records if record.bet_action == "no_bet"),
    )


def build_prediction_output_records(
    records: tuple[PlaceForwardInputRecord, ...],
    probabilities: np.ndarray,
    *,
    config: PlaceForwardTestConfig,
) -> list[PlaceForwardPredictionOutputRecord]:
    output: list[PlaceForwardPredictionOutputRecord] = []
    for record, probability in zip(records, probabilities, strict=True):
        output.append(
            validate_place_forward_prediction_output_record(
                PlaceForwardPredictionOutputRecord(
                    race_key=record.race_key,
                    horse_number=record.horse_number,
                    prediction_probability=float(probability),
                    model_version=config.reference_model.model_version,
                    feature_contract_version=FEATURE_CONTRACT_VERSION,
                    carrier_identity=record.carrier_identity,
                    odds_observation_timestamp=record.odds_observation_timestamp,
                    input_schema_version=config.input_schema_version,
                    output_schema_version=PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
                )
            )
        )
    return output


def build_bet_decision_records(
    *,
    ok_records: tuple[PlaceForwardInputRecord, ...],
    prediction_records: tuple[PlaceForwardPredictionOutputRecord, ...],
    config: PlaceForwardTestConfig,
) -> tuple[PlaceForwardBetDecisionRecord, ...]:
    prediction_map = {
        (record.race_key, record.horse_number): record.prediction_probability
        for record in prediction_records
    }
    baseline_rows = build_forward_live_logic_rows(
        ok_records=ok_records,
        prediction_map=prediction_map,
        config=config,
    )
    edge_threshold_by_window = {FORWARD_TEST_WINDOW_LABEL: config.bet_logic.threshold}
    fallback_rows = filter_guard_variant_rows(
        baseline_rows=baseline_rows,
        surcharge=config.bet_logic.stronger_guard_edge_surcharge,
        source_logic="mainline_reference_selected_rows+stronger_guard_0.01",
        stake_per_bet=config.bet_logic.stake_per_bet,
        edge_threshold_by_window=edge_threshold_by_window,
        logic_variant=config.bet_logic.fallback_logic_id,
    )
    candidate_rows = apply_overlay_variant_rows(
        guard_rows=fallback_rows,
        logic_variant=config.bet_logic.candidate_logic_id,
        source_logic="guard_0.01+venue_code_based_domain_surcharge",
        edge_threshold_by_window=edge_threshold_by_window,
        stake_per_bet=config.bet_logic.stake_per_bet,
        chaos_by_race={},
        overlay_name="proxy_domain",
    )
    horse_metadata_by_key = {
        (record.race_key, record.horse_number): (
            record.odds_observation_timestamp[:10],
            "",
            "",
        )
        for record in ok_records
    }
    candidate_bet_rows = build_final_bet_instruction_rows(
        baseline_rows=baseline_rows,
        baseline_keys={identity_key(row) for row in baseline_rows},
        fallback_keys={identity_key(row) for row in fallback_rows},
        candidate_keys={identity_key(row) for row in candidate_rows},
        horse_metadata_by_key=horse_metadata_by_key,
        logic_name=config.bet_logic.candidate_logic_id,
        run_mode="candidate_provisional",
    )
    # Build race-level rows as a side effect to ensure race grouping remains consistent.
    build_final_race_instruction_rows(candidate_bet_rows)

    decision_rows: list[PlaceForwardBetDecisionRecord] = []
    input_record_map = {(record.race_key, record.horse_number): record for record in ok_records}
    for row in candidate_bet_rows:
        input_record = input_record_map[(row.race_key, row.horse_number)]
        no_bet_reason = None if row.decision == "BET" else "logic_filtered"
        decision_rows.append(
            validate_place_forward_bet_decision_record(
                PlaceForwardBetDecisionRecord(
                    race_key=row.race_key,
                    horse_number=row.horse_number,
                    bet_action="bet" if row.decision == "BET" else "no_bet",
                    decision_reason=row.final_reason,
                    no_bet_reason=no_bet_reason,
                    feature_contract_version=FEATURE_CONTRACT_VERSION,
                    model_version=config.reference_model.model_version,
                    carrier_identity=input_record.carrier_identity,
                    odds_observation_timestamp=input_record.odds_observation_timestamp,
                    baseline_logic_id=config.bet_logic.candidate_logic_id,
                    fallback_logic_id=config.bet_logic.fallback_logic_id,
                    input_schema_version=config.input_schema_version,
                )
            )
        )
    return tuple(decision_rows)


def build_forward_live_logic_rows(
    *,
    ok_records: tuple[PlaceForwardInputRecord, ...],
    prediction_map: dict[tuple[str, int], float],
    config: PlaceForwardTestConfig,
) -> tuple[LogicBetRow, ...]:
    rows: list[LogicBetRow] = []
    for record in ok_records:
        probability = prediction_map[(record.race_key, record.horse_number)]
        assert record.place_basis_odds is not None
        market_prob = 1.0 / record.place_basis_odds
        edge = (
            probability - market_prob
            if config.bet_logic.selection_metric == "edge"
            else probability
        )
        rows.append(
            LogicBetRow(
                logic_variant="forward_live_baseline_candidates",
                source_logic="place_forward_test.ok_snapshot_candidates",
                window_label=FORWARD_TEST_WINDOW_LABEL,
                result_date=record.odds_observation_timestamp[:10],
                race_key=record.race_key,
                horse_number=record.horse_number,
                stake=config.bet_logic.stake_per_bet,
                scaled_return=0.0,
                bet_profit=-config.bet_logic.stake_per_bet,
                target_value=0,
                pred_probability=probability,
                market_prob=market_prob,
                edge=edge,
                win_odds=record.win_odds,
                popularity=record.popularity,
                place_basis_odds=record.place_basis_odds,
                place_payout=None,
            )
        )
    return tuple(sorted(rows, key=lambda row: (row.result_date, row.race_key, row.horse_number)))


def build_snapshot_failure_decision_records(
    records: tuple[PlaceForwardInputRecord, ...],
    *,
    config: PlaceForwardTestConfig,
) -> tuple[PlaceForwardBetDecisionRecord, ...]:
    output: list[PlaceForwardBetDecisionRecord] = []
    for record in records:
        if record.snapshot_status not in PLACE_FORWARD_TEST_NO_BET_REASONS:
            raise ValueError(
                "snapshot failure record must map to a no_bet_reason, got "
                f"snapshot_status={record.snapshot_status!r}"
            )
        output.append(
            validate_place_forward_bet_decision_record(
                PlaceForwardBetDecisionRecord(
                    race_key=record.race_key,
                    horse_number=record.horse_number,
                    bet_action="no_bet",
                    decision_reason=record.snapshot_failure_reason
                    or f"snapshot status {record.snapshot_status}",
                    no_bet_reason=record.snapshot_status,
                    feature_contract_version=FEATURE_CONTRACT_VERSION,
                    model_version=config.reference_model.model_version,
                    carrier_identity=record.carrier_identity,
                    odds_observation_timestamp=record.odds_observation_timestamp,
                    baseline_logic_id=config.bet_logic.candidate_logic_id,
                    fallback_logic_id=config.bet_logic.fallback_logic_id,
                    input_schema_version=config.input_schema_version,
                )
            )
        )
    return tuple(output)


def identity_key(row: LogicBetRow) -> tuple[str, int]:
    return (row.race_key, row.horse_number)


def write_csv_records(path: Path, records: list[object] | tuple[object, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = tuple(field.name for field in fields(records[0]))
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def write_json_records(path: Path, records: list[object] | tuple[object, ...]) -> None:
    path.write_text(
        json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def write_run_manifest(
    config: PlaceForwardTestConfig,
    *,
    input_records: tuple[PlaceForwardInputRecord, ...],
    prediction_records: tuple[PlaceForwardPredictionOutputRecord, ...],
    decision_records: tuple[PlaceForwardBetDecisionRecord, ...],
    path: Path,
) -> None:
    input_csv_hash = hashlib.sha256(config.input_path.read_bytes()).hexdigest()
    snapshot_status_counts: dict[str, int] = {}
    for record in input_records:
        snapshot_status_counts[record.snapshot_status] = snapshot_status_counts.get(record.snapshot_status, 0) + 1
    payload = {
        "run_name": config.name,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "input_path": str(config.input_path),
        "input_csv_hash": input_csv_hash,
        "input_schema_version": config.input_schema_version,
        "prediction_output_schema_version": PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
        "record_counts": {
            "input_records": len(input_records),
            "prediction_records": len(prediction_records),
            "decision_records": len(decision_records),
        },
        "snapshot_status_counts": snapshot_status_counts,
        "bet_action_counts": {
            "bet": sum(1 for record in decision_records if record.bet_action == "bet"),
            "no_bet": sum(1 for record in decision_records if record.bet_action == "no_bet"),
        },
        "model_lineage": {
            "dataset_path": str(config.reference_model.dataset_path),
            "model_name": config.reference_model.model_name,
            "model_version": config.reference_model.model_version,
            "feature_columns": list(config.reference_model.feature_columns),
            "feature_transforms": list(config.reference_model.feature_transforms),
            "training_splits": list(config.reference_model.training_splits),
        },
        "bet_logic": {
            "selection_metric": config.bet_logic.selection_metric,
            "threshold": config.bet_logic.threshold,
            "stake_per_bet": config.bet_logic.stake_per_bet,
            "stronger_guard_edge_surcharge": config.bet_logic.stronger_guard_edge_surcharge,
            "candidate_logic_id": config.bet_logic.candidate_logic_id,
            "fallback_logic_id": config.bet_logic.fallback_logic_id,
            "formal_domain_mapping_confirmed": config.bet_logic.formal_domain_mapping_confirmed,
        },
        "provenance": build_feature_provenance_payload(
            artifact_kind="place_forward_test_run_manifest_json",
            generated_by="horse_bet_lab.forward_test.runner.run_place_forward_test",
            config_identifier=config.name,
            model_feature_columns=config.reference_model.feature_columns,
            artifact_path=str(path),
            extra={
                "input_schema_version": config.input_schema_version,
                "output_schema_version": PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
                "candidate_logic_id": config.bet_logic.candidate_logic_id,
                "fallback_logic_id": config.bet_logic.fallback_logic_id,
                "model_version": config.reference_model.model_version,
            },
        ),
        "artifact_contract": build_place_forward_artifact_provenance(
            model_version=config.reference_model.model_version,
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp=max(
                record.odds_observation_timestamp for record in input_records
            ),
            decision_reason="place forward-test phase-1 run manifest",
            baseline_logic_id=config.bet_logic.candidate_logic_id,
            fallback_logic_id=config.bet_logic.fallback_logic_id,
            input_schema_version=config.input_schema_version,
        ),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    write_feature_provenance_sidecar(
        path,
        build_feature_provenance_payload(
            artifact_kind="place_forward_test_run_manifest_sidecar",
            generated_by="horse_bet_lab.forward_test.runner.write_run_manifest",
            config_identifier=config.name,
            model_feature_columns=config.reference_model.feature_columns,
            artifact_path=str(path),
            extra={"input_csv_hash": input_csv_hash},
        ),
    )


def write_summary(
    config: PlaceForwardTestConfig,
    *,
    decision_records: tuple[PlaceForwardBetDecisionRecord, ...],
    path: Path,
) -> None:
    bet_rows = [record for record in decision_records if record.bet_action == "bet"]
    no_bet_rows = [record for record in decision_records if record.bet_action == "no_bet"]
    lines = [
        f"place forward-test summary: {config.name}",
        "",
        f"bets: {len(bet_rows)}",
        f"no_bet: {len(no_bet_rows)}",
        "",
        "Notes",
        "- snapshot failures are written as explicit no_bet decisions",
        "- popularity remains unresolved auxiliary input in Phase 1",
        "- current hard-adopt baseline logic id is retained in decision provenance",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config)
    result = run_place_forward_test(config)
    print(
        "Place forward-test completed: "
        f"name={config.name} inputs={result.input_record_count} "
        f"predictions={result.prediction_record_count} decisions={result.decision_record_count} "
        f"output_dir={result.output_dir}"
    )


if __name__ == "__main__":
    main()
