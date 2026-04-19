from __future__ import annotations

import pytest

from horse_bet_lab.features.provenance import FEATURE_CONTRACT_VERSION
from horse_bet_lab.forward_test.contracts import (
    PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
    PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS,
    PlaceForwardBetDecisionRecord,
    PlaceForwardInputRecord,
    PlaceForwardPredictionOutputRecord,
    build_place_forward_artifact_provenance,
    validate_place_forward_bet_decision_record,
    validate_place_forward_input_record,
    validate_place_forward_input_records,
    validate_place_forward_prediction_output_record,
)


def build_valid_input_record(**overrides: object) -> PlaceForwardInputRecord:
    payload: dict[str, object] = {
        "race_key": "12345678",
        "horse_number": 3,
        "win_odds": 4.2,
        "place_basis_odds": 1.8,
        "popularity": None,
        "odds_observation_timestamp": "2026-04-19T10:00:00+09:00",
        "input_source_name": "operator_csv",
        "input_source_url": "https://example.com/place",
        "input_source_timestamp": "2026-04-19T09:59:00+09:00",
        "carrier_identity": "place_forward_live_snapshot_v1",
        "snapshot_status": "ok",
        "retry_count": 1,
        "timeout_seconds": 15.0,
        "snapshot_failure_reason": None,
        "popularity_input_source": None,
        "popularity_contract_status": None,
        "input_schema_version": PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    }
    payload.update(overrides)
    return PlaceForwardInputRecord(**payload)


def test_validate_place_forward_input_record_accepts_valid_ok_snapshot() -> None:
    record = validate_place_forward_input_record(build_valid_input_record())

    assert record.race_key == "12345678"
    assert record.snapshot_status == "ok"
    assert record.win_odds == 4.2
    assert record.place_basis_odds == 1.8


def test_validate_place_forward_input_record_accepts_popularity_as_unresolved_auxiliary() -> None:
    record = validate_place_forward_input_record(
        build_valid_input_record(
            popularity=2,
            popularity_input_source="operator_csv",
            popularity_contract_status=PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS,
        )
    )

    assert record.popularity == 2
    assert record.popularity_contract_status == "unresolved_auxiliary"


def test_validate_place_forward_input_record_allows_snapshot_failure_without_hidden_fallback() -> None:
    record = validate_place_forward_input_record(
        build_valid_input_record(
            win_odds=None,
            place_basis_odds=None,
            snapshot_status="retry_exhausted",
            snapshot_failure_reason="operator feed never returned a valid odds snapshot",
        )
    )

    assert record.snapshot_status == "retry_exhausted"
    assert record.snapshot_failure_reason is not None


def test_validate_place_forward_input_record_rejects_missing_odds_for_ok_snapshot() -> None:
    with pytest.raises(ValueError, match="place_basis_odds must be provided"):
        validate_place_forward_input_record(
            build_valid_input_record(place_basis_odds=None)
        )


def test_validate_place_forward_input_record_rejects_wrong_popularity_contract_status() -> None:
    with pytest.raises(ValueError, match="popularity_contract_status must remain"):
        validate_place_forward_input_record(
            build_valid_input_record(
                popularity=1,
                popularity_input_source="operator_csv",
                popularity_contract_status="confirmed_same_carrier",
            )
        )


def test_validate_place_forward_input_records_rejects_duplicate_keys() -> None:
    record = build_valid_input_record()
    with pytest.raises(ValueError, match="duplicate forward-test input record"):
        validate_place_forward_input_records([record, record])


def test_validate_place_forward_prediction_output_record_accepts_valid_probability() -> None:
    record = validate_place_forward_prediction_output_record(
        PlaceForwardPredictionOutputRecord(
            race_key="12345678",
            horse_number=3,
            prediction_probability=0.617,
            model_version="dual_market_logreg@2026-04-19",
            feature_contract_version=FEATURE_CONTRACT_VERSION,
            carrier_identity="place_forward_live_snapshot_v1",
            odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            input_schema_version=PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
            output_schema_version=PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION,
        )
    )

    assert record.prediction_probability == pytest.approx(0.617)


def test_validate_place_forward_bet_decision_record_requires_no_bet_reason() -> None:
    with pytest.raises(ValueError, match="no_bet_reason"):
        validate_place_forward_bet_decision_record(
            PlaceForwardBetDecisionRecord(
                race_key="12345678",
                horse_number=3,
                bet_action="no_bet",
                decision_reason="snapshot did not meet minimum contract",
                no_bet_reason=None,
                feature_contract_version=FEATURE_CONTRACT_VERSION,
                model_version="dual_market_logreg@2026-04-19",
                carrier_identity="place_forward_live_snapshot_v1",
                odds_observation_timestamp="2026-04-19T10:00:00+09:00",
            )
        )


def test_build_place_forward_artifact_provenance_contains_required_fields() -> None:
    payload = build_place_forward_artifact_provenance(
        model_version="dual_market_logreg@2026-04-19",
        carrier_identity="place_forward_live_snapshot_v1",
        odds_observation_timestamp="2026-04-19T10:00:00+09:00",
        decision_reason="mainline baseline decision contract",
        baseline_logic_id="guard_0_01_plus_proxy_domain_overlay",
        fallback_logic_id="no_bet_guard_stronger surcharge=0.01",
        run_manifest_hash="abc123",
    )

    assert payload["feature_contract_version"] == "v1"
    assert payload["model_version"] == "dual_market_logreg@2026-04-19"
    assert payload["carrier_identity"] == "place_forward_live_snapshot_v1"
    assert payload["odds_observation_timestamp"] == "2026-04-19T10:00:00+09:00"
    assert payload["decision_reason"] == "mainline baseline decision contract"
    assert payload["baseline_logic_id"] == "guard_0_01_plus_proxy_domain_overlay"
    assert payload["fallback_logic_id"] == "no_bet_guard_stronger surcharge=0.01"
    assert payload["input_schema_version"] == PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION
