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
RESULT_DB_AVAILABILITY_SHOULD_SETTLE = "should_settle"
RESULT_DB_AVAILABILITY_EXPECTED_PENDING_OR_STALE_DB = "expected_pending_or_stale_db"
RESULT_DB_AVAILABILITY_PARTIAL_RESULTS = "result_db_partial_results"
RESULT_DB_AVAILABILITY_INCOMPLETE_PAYOUT_SIDE = "result_db_incomplete_payout_side"


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


@dataclass(frozen=True)
class PlaceForwardResultAvailabilityCheckResult:
    output_dir: Path
    run_name: str
    recommendation: str
    total_races: int
    total_requested_records: int
    total_requested_bets: int
    races_with_sed_rows: int
    races_with_hjc_rows: int
    requested_records_with_known_results: int


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


def build_result_db_availability_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check whether result DB state looks ready for place forward-test reconciliation.",
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


def run_place_forward_result_db_availability_check(
    config: PlaceForwardReconciliationConfig,
) -> PlaceForwardResultAvailabilityCheckResult:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    bundle = load_forward_artifact_bundle(config.forward_output_dir)
    race_keys = tuple(sorted({record.race_key for record in bundle.input_records}))
    result_rows = load_result_rows(duckdb_path=config.duckdb_path, race_keys=race_keys)
    sed_race_summary, hjc_race_summary = load_result_availability_race_summary(
        duckdb_path=config.duckdb_path,
        race_keys=race_keys,
    )
    payload = build_result_availability_summary_payload(
        config=config,
        bundle=bundle,
        result_rows=result_rows,
        sed_race_summary=sed_race_summary,
        hjc_race_summary=hjc_race_summary,
    )
    summary_json_path = config.output_dir / "result_availability_check.json"
    summary_json_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_result_availability_summary_text(
        path=config.output_dir / "result_availability_check.txt",
        summary_payload=payload,
    )
    aggregate = payload["aggregate"]
    return PlaceForwardResultAvailabilityCheckResult(
        output_dir=config.output_dir,
        run_name=str(payload["run_name"]),
        recommendation=str(payload["recommendation"]),
        total_races=int(aggregate["total_races"]),
        total_requested_records=int(aggregate["total_requested_records"]),
        total_requested_bets=int(aggregate["total_requested_bets"]),
        races_with_sed_rows=int(aggregate["races_with_sed_rows"]),
        races_with_hjc_rows=int(aggregate["races_with_hjc_rows"]),
        requested_records_with_known_results=int(aggregate["requested_records_with_known_results"]),
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


def load_result_availability_race_summary(
    *,
    duckdb_path: Path,
    race_keys: tuple[str, ...],
) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, int]]]:
    if not race_keys:
        return {}, {}
    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        placeholders = ", ".join(["?"] * len(race_keys))
        sed_rows = connection.execute(
            f"""
            SELECT
                race_key,
                COUNT(*) AS sed_rows_found,
                SUM(
                    CASE
                        WHEN result_date IS NOT NULL AND finish_position IS NOT NULL THEN 1
                        ELSE 0
                    END
                ) AS known_result_rows
            FROM jrdb_sed_staging
            WHERE race_key IN ({placeholders})
            GROUP BY race_key
            ORDER BY race_key
            """,
            [*race_keys],
        ).fetchall()
        hjc_rows = connection.execute(
            f"""
            WITH place_payout_rows AS (
                SELECT race_key, place_horse_number_1 AS horse_number
                FROM jrdb_hjc_staging
                WHERE place_horse_number_1 IS NOT NULL
                UNION ALL
                SELECT race_key, place_horse_number_2 AS horse_number
                FROM jrdb_hjc_staging
                WHERE place_horse_number_2 IS NOT NULL
                UNION ALL
                SELECT race_key, place_horse_number_3 AS horse_number
                FROM jrdb_hjc_staging
                WHERE place_horse_number_3 IS NOT NULL
            )
            SELECT
                h.race_key,
                COUNT(*) AS hjc_rows_found,
                COUNT(p.horse_number) AS payout_rows_found
            FROM jrdb_hjc_staging h
            LEFT JOIN place_payout_rows p
                ON h.race_key = p.race_key
            WHERE h.race_key IN ({placeholders})
            GROUP BY h.race_key
            ORDER BY h.race_key
            """,
            [*race_keys],
        ).fetchall()
    finally:
        connection.close()
    sed_output = {
        str(race_key): {
            "sed_rows_found": int(sed_rows_found),
            "known_result_rows": int(known_result_rows or 0),
        }
        for race_key, sed_rows_found, known_result_rows in sed_rows
    }
    hjc_output = {
        str(race_key): {
            "hjc_rows_found": int(hjc_rows_found),
            "payout_rows_found": int(payout_rows_found or 0),
        }
        for race_key, hjc_rows_found, payout_rows_found in hjc_rows
    }
    return sed_output, hjc_output


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


def build_result_availability_summary_payload(
    *,
    config: PlaceForwardReconciliationConfig,
    bundle: ForwardArtifactBundle,
    result_rows: dict[tuple[str, int], PlaceForwardResultRecord],
    sed_race_summary: dict[str, dict[str, int]],
    hjc_race_summary: dict[str, dict[str, int]],
) -> dict[str, Any]:
    requested_keys = {(record.race_key, record.horse_number) for record in bundle.input_records}
    bet_keys = {
        (record.race_key, record.horse_number)
        for record in bundle.decision_records
        if record.bet_action == "bet"
    }
    race_key_order = tuple(sorted({record.race_key for record in bundle.input_records}))
    requested_keys_by_race: dict[str, set[tuple[str, int]]] = {}
    bet_keys_by_race: dict[str, set[tuple[str, int]]] = {}
    for key in requested_keys:
        requested_keys_by_race.setdefault(key[0], set()).add(key)
    for key in bet_keys:
        bet_keys_by_race.setdefault(key[0], set()).add(key)

    race_summaries: list[dict[str, Any]] = []
    for race_key in race_key_order:
        requested_race_keys = requested_keys_by_race.get(race_key, set())
        bet_race_keys = bet_keys_by_race.get(race_key, set())
        matched_rows = {
            key: row
            for key, row in result_rows.items()
            if key in requested_race_keys
        }
        known_result_count = sum(
            1
            for row in matched_rows.values()
            if row.result_date is not None and row.finish_position is not None
        )
        bet_known_result_count = sum(
            1
            for key, row in matched_rows.items()
            if key in bet_race_keys and row.result_date is not None and row.finish_position is not None
        )
        payout_match_count = sum(
            1
            for key, row in matched_rows.items()
            if key in bet_race_keys and row.official_place_payout is not None
        )
        sed_summary = sed_race_summary.get(race_key, {})
        hjc_summary = hjc_race_summary.get(race_key, {})
        hjc_row_present = int(hjc_summary.get("hjc_rows_found", 0)) > 0
        if known_result_count == len(requested_race_keys) and hjc_row_present:
            race_recommendation = RESULT_DB_AVAILABILITY_SHOULD_SETTLE
        elif known_result_count == 0 and int(sed_summary.get("sed_rows_found", 0)) == 0:
            race_recommendation = RESULT_DB_AVAILABILITY_EXPECTED_PENDING_OR_STALE_DB
        elif known_result_count == len(requested_race_keys) and not hjc_row_present:
            race_recommendation = RESULT_DB_AVAILABILITY_INCOMPLETE_PAYOUT_SIDE
        else:
            race_recommendation = RESULT_DB_AVAILABILITY_PARTIAL_RESULTS
        race_summaries.append(
            {
                "race_key": race_key,
                "requested_records": len(requested_race_keys),
                "requested_bets": len(bet_race_keys),
                "sed_rows_found": int(sed_summary.get("sed_rows_found", 0)),
                "known_result_rows": known_result_count,
                "hjc_row_present": hjc_row_present,
                "payout_rows_found": int(hjc_summary.get("payout_rows_found", 0)),
                "bet_records_with_known_results": bet_known_result_count,
                "bet_records_with_payout_rows": payout_match_count,
                "recommendation": race_recommendation,
            }
        )

    requested_records_with_known_results = sum(
        1
        for key in requested_keys
        if (row := result_rows.get(key)) is not None
        and row.result_date is not None
        and row.finish_position is not None
    )
    requested_bets_with_known_results = sum(
        1
        for key in bet_keys
        if (row := result_rows.get(key)) is not None
        and row.result_date is not None
        and row.finish_position is not None
    )
    races_with_sed_rows = sum(1 for race_key in race_key_order if race_key in sed_race_summary)
    races_with_hjc_rows = sum(
        1 for race_key in race_key_order if int(hjc_race_summary.get(race_key, {}).get("hjc_rows_found", 0)) > 0
    )
    all_results_known = requested_records_with_known_results == len(requested_keys)
    all_hjc_present = races_with_hjc_rows == len(race_key_order)
    if all_results_known and all_hjc_present:
        recommendation = RESULT_DB_AVAILABILITY_SHOULD_SETTLE
        recommendation_reason = (
            "all requested records have known finish results and payout-side rows exist for each race"
        )
    elif requested_records_with_known_results == 0 and races_with_sed_rows == 0:
        recommendation = RESULT_DB_AVAILABILITY_EXPECTED_PENDING_OR_STALE_DB
        recommendation_reason = (
            "none of the unit races are present in jrdb_sed_staging yet, so pending is still expected or the DB is stale"
        )
    elif all_results_known and not all_hjc_present:
        recommendation = RESULT_DB_AVAILABILITY_INCOMPLETE_PAYOUT_SIDE
        recommendation_reason = (
            "finish results are present for all requested records, but payout-side rows are still missing for some races"
        )
    else:
        recommendation = RESULT_DB_AVAILABILITY_PARTIAL_RESULTS
        recommendation_reason = (
            "some requested records are present in the result DB, but the unit does not look fully settled yet"
        )

    settled_as_of_assessment = "not_provided"
    latest_known_result_date: str | None = None
    if config.settled_as_of is not None:
        known_result_dates = sorted(
            {
                row.result_date
                for row in result_rows.values()
                if row.result_date is not None
            }
        )
        latest_known_result_date = known_result_dates[-1] if known_result_dates else None
        if latest_known_result_date is None:
            settled_as_of_assessment = "no_known_result_dates_in_db"
        else:
            settled_as_of_date = datetime.fromisoformat(config.settled_as_of).date()
            latest_result_date = datetime.fromisoformat(f"{latest_known_result_date}T00:00:00+00:00").date()
            if settled_as_of_date >= latest_result_date:
                settled_as_of_assessment = "compatible_with_known_result_dates"
            else:
                settled_as_of_assessment = "earlier_than_latest_known_result_date"

    return {
        "run_name": config.name,
        "checked_timestamp": datetime.now(timezone.utc).isoformat(),
        "forward_output_dir": str(config.forward_output_dir),
        "duckdb_path": str(config.duckdb_path),
        "settled_as_of": config.settled_as_of,
        "settled_as_of_assessment": settled_as_of_assessment,
        "latest_known_result_date": latest_known_result_date,
        "aggregate": {
            "total_races": len(race_key_order),
            "total_requested_records": len(requested_keys),
            "total_requested_bets": len(bet_keys),
            "races_with_sed_rows": races_with_sed_rows,
            "races_with_hjc_rows": races_with_hjc_rows,
            "requested_records_with_known_results": requested_records_with_known_results,
            "requested_bets_with_known_results": requested_bets_with_known_results,
        },
        "recommendation": recommendation,
        "recommendation_reason": recommendation_reason,
        "race_summaries": race_summaries,
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


def write_result_availability_summary_text(*, path: Path, summary_payload: dict[str, Any]) -> None:
    aggregate = summary_payload["aggregate"]
    lines = [
        f"place forward-test result availability check: {summary_payload['run_name']}",
        "",
        f"recommendation: {summary_payload['recommendation']}",
        f"recommendation_reason: {summary_payload['recommendation_reason']}",
        f"settled_as_of: {summary_payload['settled_as_of']}",
        f"settled_as_of_assessment: {summary_payload['settled_as_of_assessment']}",
        f"latest_known_result_date: {summary_payload['latest_known_result_date']}",
        "",
        f"total_races: {aggregate['total_races']}",
        f"total_requested_records: {aggregate['total_requested_records']}",
        f"total_requested_bets: {aggregate['total_requested_bets']}",
        f"races_with_sed_rows: {aggregate['races_with_sed_rows']}",
        f"races_with_hjc_rows: {aggregate['races_with_hjc_rows']}",
        f"requested_records_with_known_results: {aggregate['requested_records_with_known_results']}",
        f"requested_bets_with_known_results: {aggregate['requested_bets_with_known_results']}",
        "",
        "Race summaries",
    ]
    for race_summary in summary_payload["race_summaries"]:
        lines.append(
            "- "
            f"{race_summary['race_key']}: requested_records={race_summary['requested_records']} "
            f"requested_bets={race_summary['requested_bets']} "
            f"sed_rows_found={race_summary['sed_rows_found']} "
            f"known_result_rows={race_summary['known_result_rows']} "
            f"hjc_row_present={race_summary['hjc_row_present']} "
            f"bet_records_with_payout_rows={race_summary['bet_records_with_payout_rows']} "
            f"recommendation={race_summary['recommendation']}"
        )
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
