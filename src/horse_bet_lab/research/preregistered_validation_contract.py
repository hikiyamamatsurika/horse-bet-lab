from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

VALID_VERDICT = "PHASE654_2026_FORWARD_PREREGISTRATION_VALID"

EXPECTED_TOP_LEVEL_KEYS = {
    "contract_version",
    "phase",
    "research_question",
    "freshness",
    "periods",
    "data_contract",
    "fixed_models",
    "comparisons",
    "analysis",
    "verdicts",
    "phase_boundary",
}
EXPECTED_MODELS = {
    "M1C_race_constrained_market": {
        "feature_scope": "market_only",
        "training_scope": "all_supported_place_slots",
        "race_probability_sum_constraint": True,
    },
    "M5_full_pooled": {
        "feature_scope": "full_phase652_history_plus_current_context",
        "training_scope": "all_supported_place_slots",
        "race_probability_sum_constraint": True,
    },
    "M5_history_only_pooled": {
        "feature_scope": "phase652_without_current_context",
        "training_scope": "all_supported_place_slots",
        "race_probability_sum_constraint": True,
    },
    "M5_full_slot2_only": {
        "feature_scope": "full_phase652_history_plus_current_context",
        "training_scope": "place_slots_2_only",
        "race_probability_sum_constraint": True,
    },
    "M5_history_only_slot2_only": {
        "feature_scope": "phase652_without_current_context",
        "training_scope": "place_slots_2_only",
        "race_probability_sum_constraint": True,
    },
}
EXPECTED_MODEL_IDS = set(EXPECTED_MODELS)
EXPECTED_PRIMARY_COMPARISONS = {
    "overall_history_only_vs_market",
    "slot2_full_specific_vs_pooled",
    "slot2_full_specific_vs_market",
}
EXPECTED_SUPPORTING_COMPARISONS = {
    "overall_full_vs_market",
    "slot2_history_only_specific_vs_pooled",
    "slot2_history_only_specific_vs_market",
}
EXPECTED_COMPARISONS = {
    "overall_history_only_vs_market": {
        "id": "overall_history_only_vs_market",
        "role": "primary_overall_signal",
        "subset": "all_complete_supported_races",
        "baseline": "M1C_race_constrained_market",
        "candidate": "M5_history_only_pooled",
    },
    "slot2_full_specific_vs_pooled": {
        "id": "slot2_full_specific_vs_pooled",
        "role": "primary_slot2_recovery",
        "subset": "place_slots_2_only",
        "baseline": "M5_full_pooled",
        "candidate": "M5_full_slot2_only",
    },
    "slot2_full_specific_vs_market": {
        "id": "slot2_full_specific_vs_market",
        "role": "primary_slot2_recovery",
        "subset": "place_slots_2_only",
        "baseline": "M1C_race_constrained_market",
        "candidate": "M5_full_slot2_only",
    },
    "overall_full_vs_market": {
        "id": "overall_full_vs_market",
        "role": "supporting",
        "subset": "all_complete_supported_races",
        "baseline": "M1C_race_constrained_market",
        "candidate": "M5_full_pooled",
    },
    "slot2_history_only_specific_vs_pooled": {
        "id": "slot2_history_only_specific_vs_pooled",
        "role": "supporting",
        "subset": "place_slots_2_only",
        "baseline": "M5_history_only_pooled",
        "candidate": "M5_history_only_slot2_only",
    },
    "slot2_history_only_specific_vs_market": {
        "id": "slot2_history_only_specific_vs_market",
        "role": "supporting",
        "subset": "place_slots_2_only",
        "baseline": "M1C_race_constrained_market",
        "candidate": "M5_history_only_slot2_only",
    },
}


@dataclass(frozen=True)
class RegisteredValidationContract:
    payload: Mapping[str, Any]
    sha256: str

    @property
    def summary(self) -> dict[str, Any]:
        comparisons = _as_sequence(self.payload["comparisons"], "comparisons")
        return {
            "verdict": VALID_VERDICT,
            "phase": self.payload["phase"],
            "contract_sha256": self.sha256,
            "training_window": [
                self.payload["periods"]["training_start"],
                self.payload["periods"]["training_end"],
            ],
            "evaluation_window": [
                self.payload["periods"]["evaluation_start"],
                self.payload["periods"]["evaluation_end"],
            ],
            "evaluation_unlock_date": self.payload["periods"]["evaluation_unlock_date"],
            "fixed_model_ids": sorted(self.payload["fixed_models"]),
            "primary_comparison_ids": sorted(
                row["id"] for row in comparisons if str(row["role"]).startswith("primary_")
            ),
            "source_audit_allowed_only_after_contract_merge": self.payload["freshness"][
                "source_audit_allowed_only_after_contract_merge"
            ],
            "roi_or_betting_used": self.payload["phase_boundary"]["roi_or_betting_used"],
        }


def load_registered_contract(
    contract_path: Path,
    checksum_path: Path,
) -> RegisteredValidationContract:
    raw = contract_path.read_bytes()
    actual_sha256 = hashlib.sha256(raw).hexdigest()
    expected_sha256 = _read_checksum(checksum_path, contract_path.name)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"contract checksum mismatch: expected {expected_sha256}, got {actual_sha256}"
        )
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("contract root must be an object")
    validate_contract(payload)
    return RegisteredValidationContract(payload=payload, sha256=actual_sha256)


def validate_contract(payload: Mapping[str, Any]) -> None:
    _expect_exact_keys(payload, EXPECTED_TOP_LEVEL_KEYS, "contract")
    _expect(payload["contract_version"] == 1, "contract_version must be 1")
    _expect(payload["phase"] == "Phase654", "phase must be Phase654")

    freshness = _as_mapping(payload["freshness"], "freshness")
    _expect(
        freshness.get("source_audit_allowed_only_after_contract_merge") is True,
        "2026 source audit must be gated on contract merge",
    )
    _expect(
        freshness.get("model_or_threshold_change_after_source_audit") == "forbidden",
        "model and threshold changes after the source audit must be forbidden",
    )

    periods = _as_mapping(payload["periods"], "periods")
    _expect(
        periods
        == {
            "training_start": "2023-01-01",
            "training_end": "2025-12-31",
            "evaluation_start": "2026-07-20",
            "evaluation_end": "2026-12-31",
            "evaluation_unlock_date": "2027-01-01",
            "evaluation_label": "2026_forward_fresh_confirmation",
        },
        "training and evaluation periods differ from the preregistered windows",
    )

    data_contract = _as_mapping(payload["data_contract"], "data_contract")
    for key in (
        "complete_races_only",
        "reject_duplicate_identity",
        "reject_partial_history_join",
        "reject_target_or_place_slot_mismatch",
    ):
        _expect(data_contract.get(key) is True, f"data_contract.{key} must be true")
    _expect(
        data_contract.get("identity") == ["race_key", "horse_number"],
        "identity must be race_key plus horse_number",
    )
    _expect(
        data_contract.get("place_slot_rule")
        == {
            "field_size_5_to_7": 2,
            "field_size_8_or_more": 3,
            "field_size_4_or_less": "unsupported",
        },
        "place-slot rule changed",
    )

    models = _as_mapping(payload["fixed_models"], "fixed_models")
    _expect(models == EXPECTED_MODELS, "fixed model definitions changed")

    comparisons = _as_sequence(payload["comparisons"], "comparisons")
    comparison_ids: set[str] = set()
    primary_ids: set[str] = set()
    supporting_ids: set[str] = set()
    for index, raw_row in enumerate(comparisons):
        row = _as_mapping(raw_row, f"comparisons[{index}]")
        comparison_id = str(row.get("id", ""))
        _expect(comparison_id not in comparison_ids, f"duplicate comparison id: {comparison_id}")
        comparison_ids.add(comparison_id)
        _expect(
            row == EXPECTED_COMPARISONS.get(comparison_id),
            f"comparison definition changed: {comparison_id}",
        )
        baseline = str(row.get("baseline", ""))
        candidate = str(row.get("candidate", ""))
        _expect(baseline in models, f"unknown comparison baseline: {baseline}")
        _expect(candidate in models, f"unknown comparison candidate: {candidate}")
        role = str(row.get("role", ""))
        if role.startswith("primary_"):
            primary_ids.add(comparison_id)
        elif role == "supporting":
            supporting_ids.add(comparison_id)
        else:
            raise ValueError(f"unsupported comparison role: {role}")
    _expect(primary_ids == EXPECTED_PRIMARY_COMPARISONS, "primary comparison set changed")
    _expect(supporting_ids == EXPECTED_SUPPORTING_COMPARISONS, "supporting set changed")

    analysis = _as_mapping(payload["analysis"], "analysis")
    expected_analysis_values = {
        "primary_metric": "mean_binary_log_loss",
        "paired_unit": "race_key",
        "bootstrap_repetitions": 2000,
        "bootstrap_seed": 654,
        "confidence_level": 0.95,
        "market_logistic_c": 3.0,
        "offset_logistic_c": 1.0,
        "crossfit_folds": 5,
        "crossfit_group": "race_key",
        "crossfit_stratified": True,
        "crossfit_seed": 652,
        "minimum_complete_races_overall": 500,
        "minimum_complete_races_place_slots_2": 50,
        "minimum_rows_place_slots_2": 300,
        "comparison_success_rule": (
            "candidate_minus_baseline_point_delta_below_zero_and_ci95_high_below_zero"
        ),
    }
    for key, expected in expected_analysis_values.items():
        _expect(analysis.get(key) == expected, f"analysis.{key} changed")
    _expect(
        set(_as_sequence(analysis["overall_signal_confirmation"], "overall confirmation"))
        == {"overall_history_only_vs_market"},
        "overall signal confirmation changed",
    )
    _expect(
        set(_as_sequence(analysis["slot2_recovery_confirmation"], "slot2 confirmation"))
        == {
            "slot2_full_specific_vs_pooled",
            "slot2_full_specific_vs_market",
        },
        "slot-2 recovery confirmation changed",
    )
    _expect(
        analysis.get("selection_metrics")
        == ["mean_binary_log_loss", "paired_race_bootstrap_log_loss_delta"],
        "selection metrics changed",
    )
    _expect(
        analysis.get("supporting_metrics")
        == [
            "brier_score",
            "calibration_intercept",
            "calibration_slope",
            "expected_calibration_error",
            "race_probability_sum_error",
            "top_place_slots_capture",
        ],
        "supporting metrics changed",
    )

    verdicts = _as_mapping(payload["verdicts"], "verdicts")
    _expect(
        verdicts
        == {
            "source_blocked": "FRESH_2026_FORWARD_SOURCE_BLOCKED",
            "insufficient_sample": "FRESH_2026_FORWARD_INSUFFICIENT_SAMPLE",
            "slot2_recovery_confirmed": "FRESH_2026_FORWARD_SLOT2_RECOVERY_CONFIRMED",
            "slot2_recovery_not_confirmed": ("FRESH_2026_FORWARD_SLOT2_RECOVERY_NOT_CONFIRMED"),
        },
        "verdict set changed",
    )

    boundary = _as_mapping(payload["phase_boundary"], "phase_boundary")
    _expect(boundary.get("current_phase_scope") == "registration_only", "scope changed")
    _expect(boundary.get("roi_or_betting_used") is False, "ROI or betting must remain disabled")
    _expect(
        boundary.get("next_phase_after_merge")
        == "prospective_collection_readiness_and_schema_audit",
        "next phase must remain a prospective collection readiness audit",
    )
    _expect(
        boundary.get("forbidden_before_contract_merge")
        == [
            "inspect_forward_window_source_rows_or_coverage",
            "compute_forward_window_model_metrics",
            "change_features_models_hyperparameters_or_success_thresholds_after_forward_collection_starts",
            "optimize_roi_payout_bet_selection_or_stakes",
        ],
        "pre-merge prohibitions changed",
    )
    _expect(
        boundary.get("allowed_after_merge_before_evaluation_unlock")
        == [
            "read_only_schema_audit",
            "date_and_identity_coverage_monitoring_without_model_predictions_or_metrics",
            "missingness_and_join_diagnostics_without_outcome_conditioning",
        ],
        "pre-unlock monitoring boundary changed",
    )


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
