from __future__ import annotations

import json
from pathlib import Path

import duckdb

from horse_bet_lab.features.provenance import FEATURE_CONTRACT_VERSION
from horse_bet_lab.forward_test.contracts import (
    PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
    PlaceForwardBetDecisionRecord,
    PlaceForwardInputRecord,
    PlaceForwardPredictionOutputRecord,
)
from horse_bet_lab.forward_test.reconciliation import (
    RESULT_DB_AVAILABILITY_EXPECTED_PENDING_OR_STALE_DB,
    RESULT_DB_AVAILABILITY_INCOMPLETE_PAYOUT_SIDE,
    RESULT_DB_AVAILABILITY_PARTIAL_RESULTS,
    RESULT_DB_AVAILABILITY_SHOULD_SETTLE,
    load_reconciliation_config,
    run_place_forward_result_db_availability_check,
    write_csv_records,
    write_json_records,
)


def create_forward_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    input_records = (
        PlaceForwardInputRecord(
            race_key="11111111",
            horse_number=1,
            win_odds=2.0,
            place_basis_odds=1.7,
            popularity=None,
            odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            input_source_name="operator_csv",
            input_source_url=None,
            input_source_timestamp="2026-04-19T09:59:00+09:00",
            carrier_identity="place_forward_live_snapshot_v1",
            snapshot_status="ok",
            retry_count=1,
            timeout_seconds=15.0,
            snapshot_failure_reason=None,
        ),
        PlaceForwardInputRecord(
            race_key="11111111",
            horse_number=2,
            win_odds=8.0,
            place_basis_odds=2.5,
            popularity=None,
            odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            input_source_name="operator_csv",
            input_source_url=None,
            input_source_timestamp="2026-04-19T09:59:00+09:00",
            carrier_identity="place_forward_live_snapshot_v1",
            snapshot_status="ok",
            retry_count=1,
            timeout_seconds=15.0,
            snapshot_failure_reason=None,
        ),
        PlaceForwardInputRecord(
            race_key="22222222",
            horse_number=1,
            win_odds=3.0,
            place_basis_odds=1.9,
            popularity=None,
            odds_observation_timestamp="2026-04-19T11:00:00+09:00",
            input_source_name="operator_csv",
            input_source_url=None,
            input_source_timestamp="2026-04-19T10:59:00+09:00",
            carrier_identity="place_forward_live_snapshot_v1",
            snapshot_status="ok",
            retry_count=1,
            timeout_seconds=15.0,
            snapshot_failure_reason=None,
        ),
    )
    prediction_records = (
        PlaceForwardPredictionOutputRecord(
            race_key="11111111",
            horse_number=1,
            prediction_probability=0.61,
            model_version="odds_only_logreg@phase1-test",
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            input_schema_version=PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
            output_schema_version=PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
        ),
        PlaceForwardPredictionOutputRecord(
            race_key="11111111",
            horse_number=2,
            prediction_probability=0.31,
            model_version="odds_only_logreg@phase1-test",
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            input_schema_version=PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
            output_schema_version=PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
        ),
        PlaceForwardPredictionOutputRecord(
            race_key="22222222",
            horse_number=1,
            prediction_probability=0.44,
            model_version="odds_only_logreg@phase1-test",
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp="2026-04-19T11:00:00+09:00",
            input_schema_version=PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
            output_schema_version=PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
        ),
    )
    decision_records = (
        PlaceForwardBetDecisionRecord(
            race_key="11111111",
            horse_number=1,
            bet_action="bet",
            decision_reason="selected by current baseline logic",
            no_bet_reason=None,
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            model_version="odds_only_logreg@phase1-test",
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            baseline_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
        ),
        PlaceForwardBetDecisionRecord(
            race_key="11111111",
            horse_number=2,
            bet_action="bet",
            decision_reason="selected by current baseline logic",
            no_bet_reason=None,
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            model_version="odds_only_logreg@phase1-test",
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            baseline_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
        ),
        PlaceForwardBetDecisionRecord(
            race_key="22222222",
            horse_number=1,
            bet_action="no_bet",
            decision_reason="dropped by guard_0_01 edge surcharge before overlay",
            no_bet_reason="logic_filtered",
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            model_version="odds_only_logreg@phase1-test",
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp="2026-04-19T11:00:00+09:00",
            baseline_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
        ),
    )
    write_csv_records(path / "input_snapshot_records.csv", input_records)
    write_json_records(path / "input_snapshot_records.json", input_records)
    write_csv_records(path / "prediction_output_records.csv", prediction_records)
    write_json_records(path / "prediction_output_records.json", prediction_records)
    write_csv_records(path / "bet_decision_records.csv", decision_records)
    write_json_records(path / "bet_decision_records.json", decision_records)
    (path / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_name": "phase1_forward_test",
                "run_timestamp": "2026-04-19T03:00:00+00:00",
                "input_schema_version": "place_forward_test_input_v1",
                "record_counts": {
                    "input_records": 3,
                    "prediction_records": 3,
                    "decision_records": 3,
                },
                "bet_logic": {
                    "stake_per_bet": 100.0,
                    "candidate_logic_id": "guard_0_01_plus_proxy_domain_overlay",
                    "fallback_logic_id": "no_bet_guard_stronger",
                },
                "model_lineage": {
                    "model_version": "odds_only_logreg@phase1-test",
                },
                "provenance": {
                    "model_feature_columns": ["win_odds"],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def create_result_duckdb_ready(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                result_date DATE,
                finish_position INTEGER
            )
            """
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('11111111', 1, DATE '2026-04-20', 1),
                ('11111111', 2, DATE '2026-04-20', 5),
                ('22222222', 1, DATE '2026-04-21', 2)
            """
        )
        connection.execute(
            """
            CREATE TABLE jrdb_hjc_staging (
                race_key VARCHAR,
                place_horse_number_1 INTEGER,
                place_payout_1 DOUBLE,
                place_horse_number_2 INTEGER,
                place_payout_2 DOUBLE,
                place_horse_number_3 INTEGER,
                place_payout_3 DOUBLE
            )
            """
        )
        connection.execute(
            """
            INSERT INTO jrdb_hjc_staging VALUES
                ('11111111', 1, 180.0, 3, 220.0, 4, 150.0),
                ('22222222', 1, 210.0, 4, 170.0, 6, 160.0)
            """
        )
    finally:
        connection.close()


def create_result_duckdb_partial(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                result_date DATE,
                finish_position INTEGER
            )
            """
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('11111111', 1, DATE '2026-04-20', 1),
                ('11111111', 2, DATE '2026-04-20', 5)
            """
        )
        connection.execute(
            """
            CREATE TABLE jrdb_hjc_staging (
                race_key VARCHAR,
                place_horse_number_1 INTEGER,
                place_payout_1 DOUBLE,
                place_horse_number_2 INTEGER,
                place_payout_2 DOUBLE,
                place_horse_number_3 INTEGER,
                place_payout_3 DOUBLE
            )
            """
        )
        connection.execute(
            """
            INSERT INTO jrdb_hjc_staging VALUES
                ('11111111', 1, 180.0, 3, 220.0, 4, 150.0)
            """
        )
    finally:
        connection.close()


def create_result_duckdb_missing_hjc(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                result_date DATE,
                finish_position INTEGER
            )
            """
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('11111111', 1, DATE '2026-04-20', 1),
                ('11111111', 2, DATE '2026-04-20', 5),
                ('22222222', 1, DATE '2026-04-21', 2)
            """
        )
        connection.execute(
            """
            CREATE TABLE jrdb_hjc_staging (
                race_key VARCHAR,
                place_horse_number_1 INTEGER,
                place_payout_1 DOUBLE,
                place_horse_number_2 INTEGER,
                place_payout_2 DOUBLE,
                place_horse_number_3 INTEGER,
                place_payout_3 DOUBLE
            )
            """
        )
        connection.execute(
            """
            INSERT INTO jrdb_hjc_staging VALUES
                ('11111111', 1, 180.0, 3, 220.0, 4, 150.0)
            """
        )
    finally:
        connection.close()


def write_reconciliation_config(path: Path, forward_output_dir: Path, duckdb_path: Path, output_dir: Path) -> None:
    path.write_text(
        (
            "[place_forward_reconciliation]\n"
            "name = 'phase1_forward_reconciliation'\n"
            f"forward_output_dir = '{forward_output_dir}'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_dir = '{output_dir}'\n"
            "settled_as_of = '2026-04-21T09:00:00+09:00'\n"
        ),
        encoding="utf-8",
    )


def test_result_db_availability_check_reports_should_settle(tmp_path: Path) -> None:
    forward_output_dir = tmp_path / "forward_output"
    duckdb_path = tmp_path / "results_ready.duckdb"
    output_dir = tmp_path / "reconciliation_output"
    config_path = tmp_path / "reconciliation.toml"

    create_forward_output_dir(forward_output_dir)
    create_result_duckdb_ready(duckdb_path)
    write_reconciliation_config(config_path, forward_output_dir, duckdb_path, output_dir)

    result = run_place_forward_result_db_availability_check(load_reconciliation_config(config_path))

    assert result.recommendation == RESULT_DB_AVAILABILITY_SHOULD_SETTLE
    payload = json.loads((output_dir / "result_availability_check.json").read_text(encoding="utf-8"))
    assert payload["aggregate"]["total_races"] == 2
    assert payload["aggregate"]["requested_records_with_known_results"] == 3
    assert payload["aggregate"]["races_with_hjc_rows"] == 2
    assert payload["settled_as_of_assessment"] == "compatible_with_known_result_dates"
    assert any(
        race_summary["recommendation"] == RESULT_DB_AVAILABILITY_SHOULD_SETTLE
        for race_summary in payload["race_summaries"]
    )
    assert (output_dir / "result_availability_check.txt").exists()


def test_result_db_availability_check_reports_partial_results(tmp_path: Path) -> None:
    forward_output_dir = tmp_path / "forward_output"
    duckdb_path = tmp_path / "results_partial.duckdb"
    output_dir = tmp_path / "reconciliation_output"
    config_path = tmp_path / "reconciliation.toml"

    create_forward_output_dir(forward_output_dir)
    create_result_duckdb_partial(duckdb_path)
    write_reconciliation_config(config_path, forward_output_dir, duckdb_path, output_dir)

    result = run_place_forward_result_db_availability_check(load_reconciliation_config(config_path))

    assert result.recommendation == RESULT_DB_AVAILABILITY_PARTIAL_RESULTS
    payload = json.loads((output_dir / "result_availability_check.json").read_text(encoding="utf-8"))
    assert payload["aggregate"]["races_with_sed_rows"] == 1
    assert payload["aggregate"]["requested_records_with_known_results"] == 2
    race_222 = next(row for row in payload["race_summaries"] if row["race_key"] == "22222222")
    assert race_222["recommendation"] == RESULT_DB_AVAILABILITY_EXPECTED_PENDING_OR_STALE_DB


def test_result_db_availability_check_reports_missing_payout_side(tmp_path: Path) -> None:
    forward_output_dir = tmp_path / "forward_output"
    duckdb_path = tmp_path / "results_missing_hjc.duckdb"
    output_dir = tmp_path / "reconciliation_output"
    config_path = tmp_path / "reconciliation.toml"

    create_forward_output_dir(forward_output_dir)
    create_result_duckdb_missing_hjc(duckdb_path)
    write_reconciliation_config(config_path, forward_output_dir, duckdb_path, output_dir)

    result = run_place_forward_result_db_availability_check(load_reconciliation_config(config_path))

    assert result.recommendation == RESULT_DB_AVAILABILITY_INCOMPLETE_PAYOUT_SIDE
    payload = json.loads((output_dir / "result_availability_check.json").read_text(encoding="utf-8"))
    assert payload["aggregate"]["races_with_hjc_rows"] == 1
    race_222 = next(row for row in payload["race_summaries"] if row["race_key"] == "22222222")
    assert race_222["recommendation"] == RESULT_DB_AVAILABILITY_INCOMPLETE_PAYOUT_SIDE
