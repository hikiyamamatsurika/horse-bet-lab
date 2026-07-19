from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from horse_bet_lab.research.preregistered_validation_amendment import (
    EXPECTED_IMPLEMENTATION_FILES,
    VALID_AMENDED_VERDICT,
    load_amended_registered_contract,
    validate_amended_contract,
    verify_implementation_snapshot,
)
from horse_bet_lab.research.preregistered_validation_contract import (
    load_registered_contract,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs/phase658_2026_forward_preregistered_validation.json"
CHECKSUM_PATH = ROOT / "configs/phase658_2026_forward_preregistered_validation.sha256"
SUPERSEDED_CONTRACT_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.json"
SUPERSEDED_CHECKSUM_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.sha256"


def _load_payloads() -> tuple[dict[str, object], dict[str, object], str]:
    amended = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    superseded = load_registered_contract(SUPERSEDED_CONTRACT_PATH, SUPERSEDED_CHECKSUM_PATH)
    return amended, dict(superseded.payload), superseded.sha256


def test_repository_amendment_is_locked_and_valid() -> None:
    registration = load_amended_registered_contract(
        CONTRACT_PATH,
        CHECKSUM_PATH,
        SUPERSEDED_CONTRACT_PATH,
        SUPERSEDED_CHECKSUM_PATH,
    )

    assert registration.summary["verdict"] == VALID_AMENDED_VERDICT
    assert registration.summary["market_feature_order"] == [
        "win_odds",
        "place_basis_odds",
    ]
    assert registration.summary["excluded_market_features"] == ["popularity"]
    assert registration.summary["evaluation_window"] == ["2026-07-20", "2026-12-31"]
    assert registration.summary["2026_data_used_for_amendment"] is False
    assert registration.summary["roi_or_betting_used"] is False


def test_phase654_lineage_remains_valid_and_unchanged() -> None:
    superseded = load_registered_contract(SUPERSEDED_CONTRACT_PATH, SUPERSEDED_CHECKSUM_PATH)
    amended = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))

    assert superseded.sha256 == "90070eca4bf576a2752dfdc4ba18d91367236664bfa17b782e88586653089297"
    assert amended["amendment"]["superseded_contract_sha256"] == superseded.sha256
    assert superseded.payload["analysis"]["market_logistic_c"] == 3.0
    assert amended["analysis"]["market_logistic_c"] == 1.0


def test_checksum_detects_amendment_mutation(tmp_path: Path) -> None:
    mutated = CONTRACT_PATH.read_text(encoding="utf-8").replace(
        '"market_logistic_c": 1.0',
        '"market_logistic_c": 3.0',
    )
    contract_path = tmp_path / CONTRACT_PATH.name
    contract_path.write_text(mutated, encoding="utf-8")
    checksum_path = tmp_path / CHECKSUM_PATH.name
    checksum_path.write_text(CHECKSUM_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    with pytest.raises(ValueError, match="amended contract checksum mismatch"):
        load_amended_registered_contract(
            contract_path,
            checksum_path,
            SUPERSEDED_CONTRACT_PATH,
            SUPERSEDED_CHECKSUM_PATH,
        )


def test_validator_rejects_popularity_reintroduction() -> None:
    amended, superseded, superseded_sha256 = _load_payloads()
    amended["market_contract"]["feature_order"].append("popularity")  # type: ignore[index,union-attr]

    with pytest.raises(ValueError, match="market contract changed"):
        validate_amended_contract(amended, superseded, superseded_sha256)


def test_validator_rejects_market_hyperparameter_change() -> None:
    amended, superseded, superseded_sha256 = _load_payloads()
    amended["analysis"]["market_logistic_c"] = 3.0  # type: ignore[index]

    with pytest.raises(ValueError, match="analysis contract changed"):
        validate_amended_contract(amended, superseded, superseded_sha256)


def test_validator_rejects_superseded_checksum_change() -> None:
    amended, superseded, superseded_sha256 = _load_payloads()
    amended["amendment"]["superseded_contract_sha256"] = "0" * 64  # type: ignore[index]

    with pytest.raises(ValueError, match="superseded contract checksum changed"):
        validate_amended_contract(amended, superseded, superseded_sha256)


def test_implementation_snapshot_matches_phase657_merge_sources() -> None:
    registration = load_amended_registered_contract(
        CONTRACT_PATH,
        CHECKSUM_PATH,
        SUPERSEDED_CONTRACT_PATH,
        SUPERSEDED_CHECKSUM_PATH,
    )

    verify_implementation_snapshot(registration, ROOT)


def test_implementation_snapshot_detects_source_mutation(tmp_path: Path) -> None:
    registration = load_amended_registered_contract(
        CONTRACT_PATH,
        CHECKSUM_PATH,
        SUPERSEDED_CONTRACT_PATH,
        SUPERSEDED_CHECKSUM_PATH,
    )
    for relative_path in EXPECTED_IMPLEMENTATION_FILES:
        source = ROOT / relative_path
        target = tmp_path / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(source.read_bytes())
    changed_path = tmp_path / next(iter(EXPECTED_IMPLEMENTATION_FILES))
    changed_path.write_bytes(changed_path.read_bytes() + b"\n# changed\n")

    with pytest.raises(ValueError, match="registered implementation source changed"):
        verify_implementation_snapshot(registration, tmp_path)


def test_checksum_file_matches_exact_repository_bytes() -> None:
    expected_digest, expected_name = CHECKSUM_PATH.read_text(encoding="utf-8").split()

    assert expected_name == CONTRACT_PATH.name
    assert expected_digest == hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest()
