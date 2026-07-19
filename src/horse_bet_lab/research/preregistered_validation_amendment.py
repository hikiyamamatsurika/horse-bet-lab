from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from horse_bet_lab.research.preregistered_validation_contract import (
    EXPECTED_COMPARISONS,
    EXPECTED_MODELS,
    load_registered_contract,
)

VALID_AMENDED_VERDICT = "PHASE658_NO_POPULARITY_FORWARD_PREREGISTRATION_VALID"

EXPECTED_TOP_LEVEL_KEYS = {
    "contract_version",
    "phase",
    "research_question",
    "amendment",
    "freshness",
    "periods",
    "data_contract",
    "market_contract",
    "fixed_models",
    "comparisons",
    "analysis",
    "verdicts",
    "implementation_snapshot",
    "phase_boundary",
}
EXPECTED_MARKET_CONTRACT = {
    "contract_id": "phase658_no_popularity_decision_time_market_v1",
    "feature_order": ["win_odds", "place_basis_odds"],
    "feature_transforms": {
        "win_odds": "log1p",
        "place_basis_odds": "log1p",
    },
    "numeric_preprocessing": (
        "training-fit median imputation, missing indicators, mean centering, "
        "and population-standard-deviation scaling"
    ),
    "excluded_features": ["popularity"],
    "excluded_feature_reason": (
        "the historical popularity field is carried by result-side SED and is not a "
        "confirmed decision-time input"
    ),
    "applies_to": [
        "M1C_race_constrained_market",
        "M5_full_pooled",
        "M5_history_only_pooled",
        "M5_full_slot2_only",
        "M5_history_only_slot2_only",
    ],
}
EXPECTED_IMPLEMENTATION_FILES = {
    "src/horse_bet_lab/research/historical_ability_source.py": (
        "d4014ccd5d50d27e506a84e0965cc9c6289f56c36b96ad8808603711e8de97b7"
    ),
    "src/horse_bet_lab/research/historical_ability_models.py": (
        "6059a1ab4111d195d8508c7b85ab833a3f81e8dbf32e4fadef0c2bf6547a33a2"
    ),
    "src/horse_bet_lab/research/historical_signal_robustness.py": (
        "940cfa8811e28ed689d6478c748ab2c8caf2d1cae23ae2da7d96362133458f90"
    ),
    "src/horse_bet_lab/research/small_field_failure_audit.py": (
        "792125534cf4faa0e7183f21dc09a7cfe72da0bf6849ac3c37cc109378ee15da"
    ),
    "src/horse_bet_lab/research/no_popularity_rebase.py": (
        "eeb09ee8f9d554b1534209e1f172211abe082207942708cb59fbf1ed301eccb2"
    ),
}


@dataclass(frozen=True)
class AmendedRegisteredValidationContract:
    payload: Mapping[str, Any]
    sha256: str
    superseded_sha256: str

    @property
    def summary(self) -> dict[str, Any]:
        periods = _as_mapping(self.payload["periods"], "periods")
        market = _as_mapping(self.payload["market_contract"], "market_contract")
        return {
            "verdict": VALID_AMENDED_VERDICT,
            "phase": self.payload["phase"],
            "contract_sha256": self.sha256,
            "superseded_contract_sha256": self.superseded_sha256,
            "market_contract_id": market["contract_id"],
            "market_feature_order": market["feature_order"],
            "excluded_market_features": market["excluded_features"],
            "training_window": [periods["training_start"], periods["training_end"]],
            "evaluation_window": [periods["evaluation_start"], periods["evaluation_end"]],
            "evaluation_unlock_date": periods["evaluation_unlock_date"],
            "2026_data_used_for_amendment": self.payload["amendment"]["evidence"]["2026_data_used"],
            "roi_or_betting_used": self.payload["phase_boundary"]["roi_or_betting_used"],
        }


def load_amended_registered_contract(
    contract_path: Path,
    checksum_path: Path,
    superseded_contract_path: Path,
    superseded_checksum_path: Path,
) -> AmendedRegisteredValidationContract:
    superseded = load_registered_contract(
        superseded_contract_path,
        superseded_checksum_path,
    )
    raw = contract_path.read_bytes()
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    expected_sha256 = _read_checksum(checksum_path, contract_path.name)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"amended contract checksum mismatch: expected {expected_sha256}, got {actual_sha256}"
        )
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("amended contract root must be an object")
    validate_amended_contract(payload, superseded.payload, superseded.sha256)
    return AmendedRegisteredValidationContract(
        payload=payload,
        sha256=actual_sha256,
        superseded_sha256=superseded.sha256,
    )


def validate_amended_contract(
    payload: Mapping[str, Any],
    superseded_payload: Mapping[str, Any],
    superseded_sha256: str,
) -> None:
    _expect_exact_keys(payload, EXPECTED_TOP_LEVEL_KEYS, "amended contract")
    _expect(payload["contract_version"] == 2, "contract_version must be 2")
    _expect(payload["phase"] == "Phase658", "phase must be Phase658")
    _expect(
        payload["research_question"] == superseded_payload["research_question"],
        "research question changed",
    )

    amendment = _as_mapping(payload["amendment"], "amendment")
    _expect_exact_keys(
        amendment,
        {
            "supersedes_phase",
            "superseded_contract_path",
            "superseded_contract_sha256",
            "reason",
            "evidence",
        },
        "amendment",
    )
    _expect(
        amendment.get("supersedes_phase") == "Phase654",
        "amendment must supersede Phase654",
    )
    _expect(
        amendment.get("superseded_contract_path")
        == "configs/phase654_2026_forward_preregistered_validation.json",
        "superseded contract path changed",
    )
    _expect(
        amendment.get("superseded_contract_sha256") == superseded_sha256,
        "superseded contract checksum changed",
    )
    _expect(
        amendment.get("reason")
        == (
            "Phase655 found that historical popularity came from result-side SED and was not "
            "prospectively reproducible. Phase656 selected a no-popularity decision-time market "
            "contract, and Phase657 reproduced the historical Phase651-653 conclusions under "
            "that contract before the forward window began."
        ),
        "amendment reason changed",
    )
    _validate_evidence(_as_mapping(amendment.get("evidence"), "amendment.evidence"))

    _expect(payload["freshness"] == superseded_payload["freshness"], "freshness rules changed")
    _expect(payload["periods"] == superseded_payload["periods"], "periods changed")

    expected_data_contract = dict(_as_mapping(superseded_payload["data_contract"], "data_contract"))
    expected_data_contract["market_surface"] = "Phase657 no-popularity decision-time market surface"
    _expect(payload["data_contract"] == expected_data_contract, "data contract changed")
    _expect(payload["market_contract"] == EXPECTED_MARKET_CONTRACT, "market contract changed")
    _expect(payload["fixed_models"] == EXPECTED_MODELS, "fixed model definitions changed")

    comparisons = _as_sequence(payload["comparisons"], "comparisons")
    _expect(
        comparisons == list(EXPECTED_COMPARISONS.values()),
        "comparison definitions changed",
    )

    expected_analysis = dict(_as_mapping(superseded_payload["analysis"], "analysis"))
    expected_analysis["market_logistic_c"] = 1.0
    _expect(payload["analysis"] == expected_analysis, "analysis contract changed")
    _expect(payload["verdicts"] == superseded_payload["verdicts"], "verdicts changed")

    implementation = _as_mapping(payload["implementation_snapshot"], "implementation_snapshot")
    _expect_exact_keys(
        implementation,
        {"base_commit", "source_sha256"},
        "implementation_snapshot",
    )
    _expect(
        implementation.get("base_commit") == "e82525a8af0440be459982b0d46bba8378fe32c0",
        "implementation base commit changed",
    )
    _expect(
        implementation.get("source_sha256") == EXPECTED_IMPLEMENTATION_FILES,
        "implementation source checksums changed",
    )

    boundary = _as_mapping(payload["phase_boundary"], "phase_boundary")
    _expect(
        boundary.get("current_phase_scope") == "preregistration_amendment_only",
        "phase scope changed",
    )
    _expect(boundary.get("roi_or_betting_used") is False, "ROI or betting must remain disabled")
    _expect(
        boundary.get("next_phase_after_merge")
        == "no_popularity_prospective_collection_readiness_and_schema_audit",
        "next phase changed",
    )
    for key in ("forbidden_before_contract_merge", "allowed_after_merge_before_evaluation_unlock"):
        _expect(
            boundary.get(key) == superseded_payload["phase_boundary"][key],
            f"phase_boundary.{key} changed",
        )


def verify_implementation_snapshot(
    registration: AmendedRegisteredValidationContract,
    repository_root: Path,
) -> None:
    source_sha256 = _as_mapping(
        registration.payload["implementation_snapshot"]["source_sha256"],
        "implementation_snapshot.source_sha256",
    )
    for relative_path, expected_sha256 in source_sha256.items():
        path = repository_root / str(relative_path)
        if not path.is_file():
            raise ValueError(f"registered implementation source is missing: {relative_path}")
        actual_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_sha256 != expected_sha256:
            raise ValueError(
                f"registered implementation source changed: {relative_path}; "
                f"expected {expected_sha256}, got {actual_sha256}"
            )


def _validate_evidence(evidence: Mapping[str, Any]) -> None:
    expected = {
        "phase655_verdict": "FORWARD_COLLECTION_READINESS_BLOCKED",
        "phase655_merge_commit": "4b05bb8c2c883baedf7953c893cab44e25e57be2",
        "phase656_verdict": "DECISION_TIME_MARKET_FEATURE_CANDIDATE_SELECTED",
        "phase656_selected_safe_variant": "no_popularity",
        "phase656_merge_commit": "9ed80d519abf049e750d3b58c5e6ae098043e2e3",
        "phase657_verdict": "NO_POPULARITY_REBASE_HISTORICAL_CONCLUSIONS_REPRODUCED",
        "phase657_recommendation": (
            "AMEND_PREREGISTRATION_TO_NO_POPULARITY_BEFORE_FORWARD_MODEL_EVALUATION"
        ),
        "phase657_merge_commit": "e82525a8af0440be459982b0d46bba8378fe32c0",
        "2026_data_used": False,
        "2025_claimed_fresh": False,
        "roi_or_betting_used": False,
    }
    _expect(evidence == expected, "amendment evidence changed")


def _read_checksum(checksum_path: Path, expected_name: str) -> str:
    parts = checksum_path.read_text(encoding="utf-8").strip().split()
    if len(parts) != 2 or parts[1] != expected_name:
        raise ValueError("checksum file must contain '<sha256> <contract filename>'")
    digest = parts[0]
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("checksum must be a lowercase SHA-256 digest")
    return digest


def _expect_exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ValueError(
            f"{label} keys changed: missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}"
        )


def _as_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _as_sequence(value: Any, label: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be an array")
    return value


def _expect(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)
