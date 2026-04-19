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

from horse_bet_lab.features.provenance import build_feature_provenance_payload, write_feature_provenance_sidecar
from horse_bet_lab.forward_test.contracts import (
    PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    PlaceForwardBetDecisionRecord,
    PlaceForwardInputRecord,
    PlaceForwardPredictionOutputRecord,
    build_place_forward_artifact_provenance,
)

RECONCILIATION_STATUS_SETTLED_HIT = "settled_hit"
RECONCILIATION_STATUS_SETTLED_MISS = "settled_miss"
RECONCILIATION_STATUS_SETTLED_NO_BET = "settled_no_bet"
RECONCILIATION_STATUS_UNSETTLED_RESULT_PENDING = "unsettled_result_pending"
RECONCILIATION_STATUS_UNSETTLED_RESULT_INCOMPLETE = "unsettled_result_incomplete"


@dataclass(frozen=True)
class PlaceForwardReconciliationConfig:
    name: str
    forward_output_dir: Path
    duckdb_path: Path
    output_dir: Path
    settled_as_of: str | None = None


@dataclass(frozen=True)
class PlaceForwardResultRecord:
    race_key: str
    horse_number: int
    result_date: str | None
    finish_position: int | None
    official_place_payout: float | None


@dataclass(frozen=True)
class PlaceForwardReconciledRecord:
    race_key: str
    horse_number: int
    snapshot_status: str
    bet_action: str
    decision_reason: str
    no_bet_reason: str | None
    prediction_probability: float | None
    result_known: bool
    result_date: str | None
    finish_position: int | None
    place_hit: bool | None
    official_place_payout: float | None
    simulated_payout: float | None
    simulated_profit_loss: float | None
    reconciliation_status: str
    feature_contract_version: str
    model_version: str
    carrier_identity: str
    odds_observation_timestamp: str
    baseline_logic_id: str | None
    fallback_logic_id: str | None
    input_schema_version: str


@dataclass(frozen=True)
class PlaceForwardReconciliationResult:
    output_dir: Path
    reconciled_record_count: int
    settled_bet_count: int
    unsettled_bet_count: int
    hit_count: int
    total_simulated_payout: float
    total_simulated_profit_loss: float


def build_reconciliation_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reconcile place-only forward-test artifacts against settled race results.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a place forward-test reconciliation TOML config.",
    )
    return parser


def load_reconciliation_config(path: Path) -> PlaceForwardReconciliationConfig:
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    section = payload["place_forward_reconciliation"]
    settled_as_of = section.get("settled_as_of")
    if settled_as_of is not None and str(settled_as_of).strip() == "":
        raise ValueError("settled_as_of must be omitted or non-empty")
    return PlaceForwardReconciliationConfig(
        name=str(section["name"]),
        forward_output_dir=Path(str(section["forward_output_dir"])),
        duckdb_path=Path(str(section["duckdb_path"])),
        output_dir=Path(str(section["output_dir"])),
        settled_as_of=str(settled_as_of) if settled_as_of is not None else None,
    )


@dataclass(frozen=True)
class ForwardArtifactBundle:
    input_records: tuple[PlaceForwardInputRecord, ...]
    prediction_records: tuple[PlaceForwardPredictionOutputRecord, ...]
    decision_records: tuple[PlaceForwardBetDecisionRecord, ...]
    run_manifest: dict[str, Any]


def run_place_forward_reconciliation(
    config: PlaceForwardReconciliationConfig,
) -> PlaceForwardReconciliationResult:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    bundle = load_forward_artifact_bundle(config.forward_output_dir)
    result_rows = load_result_rows(
        duckdb_path=config.duckdb_path,
        race_keys=tuple(sorted({record.race_key for record in bundle.input_records})),
    )
    reconciled_records = reconcile_forward_records(bundle=bundle, result_rows=result_rows)

    write_csv_records(config.output_dir / "reconciled_records.csv", reconciled_records)
    write_json_records(config.output_dir / "reconciled_records.json", reconciled_records)
    summary_payload = build_reconciliation_summary_payload(
        config=config,
        bundle=bundle,
        reconciled_records=reconciled_records,
    )
    summary_json_path = config.output_dir / "reconciliation_summary.json"
    summary_json_path.write_text(
        json.dumps(summary_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_reconciliation_summary_text(
        path=config.output_dir / "reconciliation_summary.txt",
        summary_payload=summary_payload,
    )
    write_reconciliation_manifest(
        config=config,
        bundle=bundle,
        reconciled_records=reconciled_records,
        path=config.output_dir / "reconciliation_manifest.json",
    )
    return PlaceForwardReconciliationResult(
        output_dir=config.output_dir,
        reconciled_record_count=len(reconciled_records),
        settled_bet_count=int(summary_payload["settled_bets"]),
        unsettled_bet_count=int(summary_payload["unsettled_bets"]),
        hit_count=int(summary_payload["hit_count"]),
        total_simulated_payout=float(summary_payload["total_simulated_payout"]),
        total_simulated_profit_loss=float(summary_payload["total_simulated_profit_loss"]),
    )


def load_forward_artifact_bundle(forward_output_dir: Path) -> ForwardArtifactBundle:
    if not forward_output_dir.exists():
        raise FileNotFoundError(f"forward output dir does not exist: {forward_output_dir}")
    input_records = tuple(
        load_dataclass_records(forward_output_dir / "input_snapshot_records.json", PlaceForwardInputRecord)
    )
    prediction_records = tuple(
        load_dataclass_records(
            forward_output_dir / "prediction_output_records.json",
            PlaceForwardPredictionOutputRecord,
        )
    )
    decision_records = tuple(
        load_dataclass_records(
            forward_output_dir / "bet_decision_records.json",
            PlaceForwardBetDecisionRecord,
        )
    )
    run_manifest_path = forward_output_dir / "run_manifest.json"
    if not run_manifest_path.exists():
        raise FileNotFoundError(f"forward run_manifest.json does not exist: {run_manifest_path}")
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    if not input_records:
        raise ValueError("forward output dir contains no input_snapshot_records")
    if not decision_records:
        raise ValueError("forward output dir contains no bet_decision_records")
    return ForwardArtifactBundle(
        input_records=input_records,
        prediction_records=prediction_records,
        decision_records=decision_records,
        run_manifest=run_manifest,
    )


def load_dataclass_records(path: Path, cls: type[object]) -> list[object]:
    if not path.exists():
        raise FileNotFoundError(f"required forward artifact does not exist: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"artifact must be a JSON list: {path}")
    return [cls(**row) for row in payload]


def load_result_rows(
    *,
    duckdb_path: Path,
    race_keys: tuple[str, ...],
) -> dict[tuple[str, int], PlaceForwardResultRecord]:
    if not race_keys:
        return {}
    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        placeholders = ", ".join(["?"] * len(race_keys))
        rows = connection.execute(
            f"""
            WITH place_payout_rows AS (
                SELECT race_key, place_horse_number_1 AS horse_number, CAST(place_payout_1 AS DOUBLE) AS payout
                FROM jrdb_hjc_staging
                WHERE place_horse_number_1 IS NOT NULL
                UNION ALL
                SELECT race_key, place_horse_number_2 AS horse_number, CAST(place_payout_2 AS DOUBLE) AS payout
                FROM jrdb_hjc_staging
                WHERE place_horse_number_2 IS NOT NULL
                UNION ALL
                SELECT race_key, place_horse_number_3 AS horse_number, CAST(place_payout_3 AS DOUBLE) AS payout
                FROM jrdb_hjc_staging
                WHERE place_horse_number_3 IS NOT NULL
            )
            SELECT
                s.race_key,
                s.horse_number,
                CAST(s.result_date AS VARCHAR) AS result_date,
                s.finish_position,
                p.payout
            FROM jrdb_sed_staging s
            LEFT JOIN place_payout_rows p
                ON s.race_key = p.race_key
                AND s.horse_number = p.horse_number
            WHERE s.race_key IN ({placeholders})
            ORDER BY s.race_key, s.horse_number
            """,
            [*race_keys],
        ).fetchall()
    finally:
        connection.close()

    output: dict[tuple[str, int], PlaceForwardResultRecord] = {}
    for race_key, horse_number, result_date, finish_position, payout in rows:
        key = (str(race_key), int(horse_number))
        if key in output:
            raise ValueError(
                "duplicate result row for forward-test reconciliation join: "
                f"race_key={key[0]} horse_number={key[1]}"
            )
        output[key] = PlaceForwardResultRecord(
            race_key=str(race_key),
            horse_number=int(horse_number),
            result_date=str(result_date) if result_date is not None else None,
            finish_position=int(finish_position) if finish_position is not None else None,
            official_place_payout=float(payout) if payout is not None else None,
        )
    return output


def reconcile_forward_records(
    *,
    bundle: ForwardArtifactBundle,
    result_rows: dict[tuple[str, int], PlaceForwardResultRecord],
) -> tuple[PlaceForwardReconciledRecord, ...]:
    input_by_key = {(record.race_key, record.horse_number): record for record in bundle.input_records}
    prediction_by_key = {
        (record.race_key, record.horse_number): record
        for record in bundle.prediction_records
    }
    stake_per_bet = float(bundle.run_manifest["bet_logic"]["stake_per_bet"])

    reconciled_rows: list[PlaceForwardReconciledRecord] = []
    for decision in bundle.decision_records:
        key = (decision.race_key, decision.horse_number)
        input_record = input_by_key.get(key)
        if input_record is None:
            raise ValueError(
                "missing input snapshot record for forward-test reconciliation: "
                f"race_key={decision.race_key} horse_number={decision.horse_number}"
            )
        prediction_record = prediction_by_key.get(key)
        result_record = result_rows.get(key)
        result_known = (
            result_record is not None
            and result_record.result_date is not None
            and result_record.finish_position is not None
        )
        if result_record is None:
            reconciliation_status = RECONCILIATION_STATUS_UNSETTLED_RESULT_PENDING
            place_hit = None
            simulated_payout = None
            simulated_profit_loss = None
            result_date = None
            finish_position = None
            official_place_payout = None
        elif not result_known:
            reconciliation_status = RECONCILIATION_STATUS_UNSETTLED_RESULT_INCOMPLETE
            place_hit = None
            simulated_payout = None
            simulated_profit_loss = None
            result_date = result_record.result_date
            finish_position = result_record.finish_position
            official_place_payout = result_record.official_place_payout
        else:
            result_date = result_record.result_date
            finish_position = result_record.finish_position
            official_place_payout = result_record.official_place_payout
            place_hit = official_place_payout is not None
            if decision.bet_action == "bet":
                if official_place_payout is not None:
                    reconciliation_status = RECONCILIATION_STATUS_SETTLED_HIT
                    simulated_payout = float(official_place_payout)
                    simulated_profit_loss = simulated_payout - stake_per_bet
                else:
                    reconciliation_status = RECONCILIATION_STATUS_SETTLED_MISS
                    simulated_payout = 0.0
                    simulated_profit_loss = -stake_per_bet
            else:
                reconciliation_status = RECONCILIATION_STATUS_SETTLED_NO_BET
                simulated_payout = 0.0
                simulated_profit_loss = 0.0
        reconciled_rows.append(
            PlaceForwardReconciledRecord(
                race_key=decision.race_key,
                horse_number=decision.horse_number,
                snapshot_status=input_record.snapshot_status,
                bet_action=decision.bet_action,
                decision_reason=decision.decision_reason,
                no_bet_reason=decision.no_bet_reason,
                prediction_probability=(
                    prediction_record.prediction_probability if prediction_record is not None else None
                ),
                result_known=result_known,
                result_date=result_date,
                finish_position=finish_position,
                place_hit=place_hit,
                official_place_payout=official_place_payout,
                simulated_payout=simulated_payout,
                simulated_profit_loss=simulated_profit_loss,
                reconciliation_status=reconciliation_status,
                feature_contract_version=decision.feature_contract_version,
                model_version=decision.model_version,
                carrier_identity=decision.carrier_identity,
                odds_observation_timestamp=decision.odds_observation_timestamp,
                baseline_logic_id=decision.baseline_logic_id,
                fallback_logic_id=decision.fallback_logic_id,
                input_schema_version=decision.input_schema_version,
            )
        )
    return tuple(reconciled_rows)


def build_reconciliation_summary_payload(
    *,
    config: PlaceForwardReconciliationConfig,
    bundle: ForwardArtifactBundle,
    reconciled_records: tuple[PlaceForwardReconciledRecord, ...],
) -> dict[str, Any]:
    no_bet_reason_counts: dict[str, int] = {}
    reconciliation_status_counts: dict[str, int] = {}
    for record in reconciled_records:
        reconciliation_status_counts[record.reconciliation_status] = (
            reconciliation_status_counts.get(record.reconciliation_status, 0) + 1
        )
        if record.no_bet_reason is not None:
            no_bet_reason_counts[record.no_bet_reason] = no_bet_reason_counts.get(record.no_bet_reason, 0) + 1

    settled_bets = sum(
        1
        for record in reconciled_records
        if record.bet_action == "bet"
        and record.reconciliation_status in {
            RECONCILIATION_STATUS_SETTLED_HIT,
            RECONCILIATION_STATUS_SETTLED_MISS,
        }
    )
    unsettled_bets = sum(
        1
        for record in reconciled_records
        if record.bet_action == "bet"
        and record.reconciliation_status
        not in {
            RECONCILIATION_STATUS_SETTLED_HIT,
            RECONCILIATION_STATUS_SETTLED_MISS,
        }
    )
    return {
        "run_name": config.name,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "forward_output_dir": str(config.forward_output_dir),
        "duckdb_path": str(config.duckdb_path),
        "settled_as_of": config.settled_as_of,
        "total_inputs": len(bundle.input_records),
        "total_predictions": len(bundle.prediction_records),
        "total_decisions": len(bundle.decision_records),
        "total_bets": sum(1 for record in reconciled_records if record.bet_action == "bet"),
        "settled_bets": settled_bets,
        "unsettled_bets": unsettled_bets,
        "hit_count": sum(
            1 for record in reconciled_records if record.reconciliation_status == RECONCILIATION_STATUS_SETTLED_HIT
        ),
        "total_simulated_payout": sum(
            float(record.simulated_payout or 0.0)
            for record in reconciled_records
            if record.bet_action == "bet"
        ),
        "total_simulated_profit_loss": sum(
            float(record.simulated_profit_loss or 0.0)
            for record in reconciled_records
            if record.bet_action == "bet"
        ),
        "no_bet_reason_counts": no_bet_reason_counts,
        "reconciliation_status_counts": reconciliation_status_counts,
    }


def write_reconciliation_summary_text(*, path: Path, summary_payload: dict[str, Any]) -> None:
    lines = [
        f"place forward-test reconciliation summary: {summary_payload['run_name']}",
        "",
        f"total_inputs: {summary_payload['total_inputs']}",
        f"total_predictions: {summary_payload['total_predictions']}",
        f"total_decisions: {summary_payload['total_decisions']}",
        f"total_bets: {summary_payload['total_bets']}",
        f"settled_bets: {summary_payload['settled_bets']}",
        f"unsettled_bets: {summary_payload['unsettled_bets']}",
        f"hit_count: {summary_payload['hit_count']}",
        f"total_simulated_payout: {summary_payload['total_simulated_payout']}",
        f"total_simulated_profit_loss: {summary_payload['total_simulated_profit_loss']}",
        "",
        "No-bet reason counts",
    ]
    no_bet_reason_counts = summary_payload["no_bet_reason_counts"]
    if no_bet_reason_counts:
        for reason in sorted(no_bet_reason_counts):
            lines.append(f"- {reason}: {no_bet_reason_counts[reason]}")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "Reconciliation status counts",
        ]
    )
    for status in sorted(summary_payload["reconciliation_status_counts"]):
        lines.append(f"- {status}: {summary_payload['reconciliation_status_counts'][status]}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_reconciliation_manifest(
    *,
    config: PlaceForwardReconciliationConfig,
    bundle: ForwardArtifactBundle,
    reconciled_records: tuple[PlaceForwardReconciledRecord, ...],
    path: Path,
) -> None:
    forward_manifest_path = config.forward_output_dir / "run_manifest.json"
    forward_manifest_hash = hashlib.sha256(forward_manifest_path.read_bytes()).hexdigest()
    model_feature_columns = tuple(
        bundle.run_manifest.get("provenance", {}).get("model_feature_columns") or ()
    )
    latest_observation_timestamp = max(
        record.odds_observation_timestamp for record in bundle.input_records
    )
    payload = {
        "run_name": config.name,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "forward_output_dir": str(config.forward_output_dir),
        "forward_run_manifest_path": str(forward_manifest_path),
        "forward_run_manifest_hash": forward_manifest_hash,
        "duckdb_path": str(config.duckdb_path),
        "settled_as_of": config.settled_as_of,
        "record_counts": {
            "input_records": len(bundle.input_records),
            "prediction_records": len(bundle.prediction_records),
            "decision_records": len(bundle.decision_records),
            "reconciled_records": len(reconciled_records),
        },
        "bet_logic": bundle.run_manifest.get("bet_logic"),
        "forward_run_name": bundle.run_manifest.get("run_name"),
        "forward_run_timestamp": bundle.run_manifest.get("run_timestamp"),
        "provenance": build_feature_provenance_payload(
            artifact_kind="place_forward_test_reconciliation_manifest_json",
            generated_by="horse_bet_lab.forward_test.reconciliation.run_place_forward_reconciliation",
            config_identifier=config.name,
            model_feature_columns=model_feature_columns,
            artifact_path=str(path),
            extra={
                "forward_run_manifest_hash": forward_manifest_hash,
                "forward_output_dir": str(config.forward_output_dir),
            },
        ),
        "artifact_contract": build_place_forward_artifact_provenance(
            model_version=str(bundle.run_manifest["model_lineage"]["model_version"]),
            carrier_identity=str(bundle.input_records[0].carrier_identity),
            odds_observation_timestamp=latest_observation_timestamp,
            decision_reason="place forward-test reconciliation manifest",
            baseline_logic_id=bundle.run_manifest["bet_logic"]["candidate_logic_id"],
            fallback_logic_id=bundle.run_manifest["bet_logic"]["fallback_logic_id"],
            input_schema_version=str(
                bundle.run_manifest.get("input_schema_version", PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION)
            ),
            run_manifest_hash=forward_manifest_hash,
        ),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    write_feature_provenance_sidecar(
        path,
        build_feature_provenance_payload(
            artifact_kind="place_forward_test_reconciliation_manifest_sidecar",
            generated_by="horse_bet_lab.forward_test.reconciliation.write_reconciliation_manifest",
            config_identifier=config.name,
            model_feature_columns=model_feature_columns,
            artifact_path=str(path),
            extra={"forward_run_manifest_hash": forward_manifest_hash},
        ),
    )


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


def main() -> None:
    args = build_reconciliation_parser().parse_args()
    config = load_reconciliation_config(args.config)
    result = run_place_forward_reconciliation(config)
    print(
        "Place forward-test reconciliation completed: "
        f"name={config.name} reconciled={result.reconciled_record_count} "
        f"settled_bets={result.settled_bet_count} unsettled_bets={result.unsettled_bet_count} "
        f"output_dir={result.output_dir}"
    )


if __name__ == "__main__":
    main()
