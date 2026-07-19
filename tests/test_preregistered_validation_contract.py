from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from horse_bet_lab.research.preregistered_validation_contract import (
    EXPECTED_MODEL_IDS,
    EXPECTED_PRIMARY_COMPARISONS,
    VALID_VERDICT,
    load_registered_contract,
    validate_contract,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs/phase654_2026_h1_preregistered_validation.json"
CHECKSUM_PATH = ROOT / "configs/phase654_2026_h1_preregistered_validation.sha256"


def test_repository_preregistration_is_locked_and_valid() -> None:
    registration = load_registered_contract(CONTRACT_PATH, CHECKSUM_PATH)

    assert registration.summary["verdict"] == VALID_VERDICT
    assert registration.summary["fixed_model_ids"] == sorted(EXPECTED_MODEL_IDS)
    assert registration.summary["primary_comparison_ids"] == sorted(EXPECTED_PRIMARY_COMPARISONS)
    assert registration.summary["evaluation_window"] == ["2026-01-01", "2026-06-30"]
    assert registration.summary["source_audit_allowed_only_after_contract_merge"] is True
    assert registration.summary["roi_or_betting_used"] is False


def test_checksum_detects_contract_mutation(tmp_path: Path) -> None:
    mutated = CONTRACT_PATH.read_text(encoding="utf-8").replace(
        '"bootstrap_repetitions": 2000',
        '"bootstrap_repetitions": 1999',
    )
    contract_path = tmp_path / CONTRACT_PATH.name
    contract_path.write_text(mutated, encoding="utf-8")
    checksum_path = tmp_path / CHECKSUM_PATH.name
    checksum_path.write_text(CHECKSUM_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    with pytest.raises(ValueError, match="contract checksum mismatch"):
        load_registered_contract(contract_path, checksum_path)


def test_validator_rejects_changed_evaluation_window() -> None:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    payload["periods"]["evaluation_end"] = "2026-12-31"

    with pytest.raises(ValueError, match="preregistered windows"):
        validate_contract(payload)


def test_validator_rejects_primary_comparison_removal() -> None:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    payload["comparisons"] = [
        row for row in payload["comparisons"] if row["id"] != "slot2_full_specific_vs_market"
    ]

    with pytest.raises(ValueError, match="primary comparison set changed"):
        validate_contract(payload)


def test_validator_rejects_model_definition_change() -> None:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    payload["fixed_models"]["M5_full_slot2_only"]["training_scope"] = "all_supported_place_slots"

    with pytest.raises(ValueError, match="fixed model definitions changed"):
        validate_contract(payload)


def test_validator_rejects_comparison_definition_change() -> None:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    payload["comparisons"][0]["candidate"] = "M5_full_pooled"

    with pytest.raises(ValueError, match="comparison definition changed"):
        validate_contract(payload)


def test_validator_rejects_roi_or_betting_activation() -> None:
    payload = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    payload["phase_boundary"]["roi_or_betting_used"] = True

    with pytest.raises(ValueError, match="ROI or betting"):
        validate_contract(payload)


def test_checksum_file_matches_exact_repository_bytes() -> None:
    expected_digest, expected_name = CHECKSUM_PATH.read_text(encoding="utf-8").split()

    assert expected_name == CONTRACT_PATH.name
    assert expected_digest == hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest()
