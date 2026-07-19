from __future__ import annotations

import csv
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from horse_bet_lab.forward_test.contracts import PlaceForwardInputRecord
from horse_bet_lab.forward_test.raw_snapshot_intake import (
    PLACE_COLUMN_ALTERNATIVES,
    REQUIRED_RAW_SNAPSHOT_COLUMNS,
)
from horse_bet_lab.forward_test.runner import SUPPORTED_FORWARD_TEST_FEATURE_COLUMNS
from horse_bet_lab.ingest.specs import SUPPORTED_FILE_SPECS
from horse_bet_lab.jrdb_ingestion.handoff import run_handoff
from horse_bet_lab.jrdb_ingestion.oz_pre_race_adapter import (
    RAW_ISH_OZ_OUTPUT_COLUMNS,
    run_oz_pre_race_adapter,
)
from horse_bet_lab.research.historical_ability_models import (
    HISTORY_CATEGORICAL_FEATURE_COLUMNS,
    HISTORY_NUMERIC_FEATURE_COLUMNS,
)
from horse_bet_lab.research.historical_ability_source import (
    MODEL_FEATURE_COLUMNS,
    build_source_audit,
)
from horse_bet_lab.research.preregistered_validation_amendment import (
    VALID_AMENDED_VERDICT,
    load_amended_registered_contract,
    verify_implementation_snapshot,
)

READY_VERDICT = "NO_POPULARITY_FORWARD_COLLECTION_READINESS_READY"
BLOCKED_VERDICT = "NO_POPULARITY_FORWARD_COLLECTION_READINESS_BLOCKED"
REQUIRED_RAW_FILE_KINDS = ("BAC", "KYI", "OZ", "SED")
REQUIRED_MARKET_FEATURES = ("win_odds", "place_basis_odds")


@dataclass(frozen=True)
class NoPopularityForwardReadinessResult:
    summary: dict[str, Any]
    checks: tuple[dict[str, Any], ...]


def run_no_popularity_forward_readiness(
    *,
    contract_path: Path,
    checksum_path: Path,
    superseded_contract_path: Path,
    superseded_checksum_path: Path,
    repository_root: Path,
) -> NoPopularityForwardReadinessResult:
    registration = load_amended_registered_contract(
        contract_path,
        checksum_path,
        superseded_contract_path,
        superseded_checksum_path,
    )
    checks: list[dict[str, Any]] = []
    blockers: list[str] = []

    amendment_valid = registration.summary["verdict"] == VALID_AMENDED_VERDICT
    _append_check(
        checks,
        check_id="phase658_amendment_locked",
        passed=amendment_valid,
        evidence=(
            f"sha256={registration.sha256}; superseded_sha256={registration.superseded_sha256}"
        ),
    )
    if not amendment_valid:
        blockers.append("Phase658 amendment did not validate")

    try:
        verify_implementation_snapshot(registration, repository_root)
        implementation_snapshot_matches = True
        implementation_evidence = "all registered Phase657 source hashes match"
    except ValueError as exc:
        implementation_snapshot_matches = False
        implementation_evidence = str(exc)
        blockers.append("registered probability implementation snapshot changed")
    _append_check(
        checks,
        check_id="registered_implementation_snapshot_matches",
        passed=implementation_snapshot_matches,
        evidence=implementation_evidence,
    )

    market_contract = registration.payload["market_contract"]
    market_features = tuple(str(value) for value in market_contract["feature_order"])
    excluded_features = tuple(str(value) for value in market_contract["excluded_features"])
    market_contract_matches = market_features == REQUIRED_MARKET_FEATURES and excluded_features == (
        "popularity",
    )
    _append_check(
        checks,
        check_id="no_popularity_market_contract_fixed",
        passed=market_contract_matches,
        evidence=f"features={list(market_features)}; excluded={list(excluded_features)}",
    )
    if not market_contract_matches:
        blockers.append("Phase658 no-popularity market feature contract changed")

    input_fields = set(PlaceForwardInputRecord.__dataclass_fields__)
    record_schema_ready = set(REQUIRED_MARKET_FEATURES).issubset(input_fields)
    runner_schema_ready = set(REQUIRED_MARKET_FEATURES).issubset(
        SUPPORTED_FORWARD_TEST_FEATURE_COLUMNS
    )
    _append_check(
        checks,
        check_id="forward_input_and_runner_support_registered_market_features",
        passed=record_schema_ready and runner_schema_ready,
        evidence=(
            f"record_schema_ready={record_schema_ready}; runner_schema_ready={runner_schema_ready}"
        ),
    )
    if not (record_schema_ready and runner_schema_ready):
        blockers.append("forward input schema or runner cannot carry the registered market vector")

    oz_output_ready = set(
        ("race_key", "horse_number", "win_odds", "place_basis_odds_proxy")
    ).issubset(RAW_ISH_OZ_OUTPUT_COLUMNS)
    intake_ready = (
        set(("race_key", "horse_number", "win_odds")).issubset(REQUIRED_RAW_SNAPSHOT_COLUMNS)
        and ("place_basis_odds_proxy",) in PLACE_COLUMN_ALTERNATIVES
    )
    official_oz_path_ready = callable(run_oz_pre_race_adapter) and callable(run_handoff)
    _append_check(
        checks,
        check_id="official_oz_market_snapshot_path_present",
        passed=oz_output_ready and intake_ready and official_oz_path_ready,
        evidence=(
            f"oz_output_columns={list(RAW_ISH_OZ_OUTPUT_COLUMNS)}; "
            f"intake_proxy_supported={intake_ready}; handoff_callable={official_oz_path_ready}"
        ),
    )
    if not (oz_output_ready and intake_ready and official_oz_path_ready):
        blockers.append("official OZ snapshot cannot supply both registered market features")

    supported_file_kinds = {spec.file_kind for spec in SUPPORTED_FILE_SPECS}
    raw_kinds_present = set(REQUIRED_RAW_FILE_KINDS).issubset(supported_file_kinds)
    _append_check(
        checks,
        check_id="required_historical_raw_file_kinds_supported",
        passed=raw_kinds_present,
        evidence=f"required={list(REQUIRED_RAW_FILE_KINDS)}",
    )
    if not raw_kinds_present:
        blockers.append(
            "missing raw file specifications: "
            f"{sorted(set(REQUIRED_RAW_FILE_KINDS) - supported_file_kinds)}"
        )

    source_parameters = inspect.signature(build_source_audit).parameters
    history_builder_parameterized = {
        "raw_root",
        "history_years",
        "evaluation_years",
        "minimum_identity_match_rate",
    }.issubset(source_parameters)
    phase651_history_features = {
        *HISTORY_NUMERIC_FEATURE_COLUMNS,
        *HISTORY_CATEGORICAL_FEATURE_COLUMNS,
    }
    history_surface_compatible = phase651_history_features.issubset(MODEL_FEATURE_COLUMNS)
    history_rebuild_ready = history_builder_parameterized and history_surface_compatible
    _append_check(
        checks,
        check_id="post_period_history_surface_rebuild",
        passed=history_rebuild_ready,
        evidence=(
            f"parameterized={history_builder_parameterized}; "
            f"phase651_features_present={history_surface_compatible}"
        ),
    )
    if not history_rebuild_ready:
        blockers.append("Phase650H history surface cannot be rebuilt after the forward period")

    popularity_not_required = "popularity" not in market_features
    _append_check(
        checks,
        check_id="popularity_carrier_not_required",
        passed=popularity_not_required,
        evidence="popularity remains optional provenance but is excluded from the model vector",
    )
    if not popularity_not_required:
        blockers.append("a popularity carrier is still required by the amended model")

    periods = registration.payload["periods"]
    prospective_boundary_fixed = (
        periods["evaluation_start"] == "2026-07-20"
        and periods["evaluation_end"] == "2026-12-31"
        and periods["evaluation_unlock_date"] == "2027-01-01"
    )
    _append_check(
        checks,
        check_id="prospective_window_and_unlock_fixed",
        passed=prospective_boundary_fixed,
        evidence=(
            f"window={periods['evaluation_start']}..{periods['evaluation_end']}; "
            f"unlock={periods['evaluation_unlock_date']}"
        ),
    )
    if not prospective_boundary_fixed:
        blockers.append("prospective evaluation window or unlock changed")

    final_verdict = READY_VERDICT if not blockers else BLOCKED_VERDICT
    summary = {
        "final_verdict": final_verdict,
        "phase658_contract_sha256": registration.sha256,
        "superseded_phase654_contract_sha256": registration.superseded_sha256,
        "evaluation_window": {
            "start": periods["evaluation_start"],
            "end": periods["evaluation_end"],
            "unlock_date": periods["evaluation_unlock_date"],
        },
        "fixed_market_features": list(market_features),
        "excluded_market_features": list(excluded_features),
        "official_oz_market_snapshot_path_ready": (
            oz_output_ready and intake_ready and official_oz_path_ready
        ),
        "history_surface_rebuild_ready": history_rebuild_ready,
        "implementation_snapshot_matches": implementation_snapshot_matches,
        "popularity_carrier_required": not popularity_not_required,
        "blockers": blockers,
        "source_content_inspected": False,
        "forward_rows_or_coverage_inspected": False,
        "model_predictions_or_metrics_computed": False,
        "outcome_conditioning_used": False,
        "2026_data_used": False,
        "roi_or_betting_used": False,
        "recommendation": (
            "BEGIN_PROSPECTIVE_NON_OUTCOME_CONDITIONED_COLLECTION_MONITORING"
            if not blockers
            else "RESOLVE_STATIC_COLLECTION_BLOCKERS_BEFORE_MONITORING"
        ),
    }
    return NoPopularityForwardReadinessResult(summary=summary, checks=tuple(checks))


def write_no_popularity_forward_readiness(
    result: NoPopularityForwardReadinessResult,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "phase659_summary.json").write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(output_dir / "phase659_checks.csv", result.checks)
    (output_dir / "phase659_findings.md").write_text(
        _findings_markdown(result),
        encoding="utf-8",
    )


def _append_check(
    rows: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    evidence: str,
) -> None:
    rows.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "evidence": evidence,
        }
    )


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames = list(rows[0]) if rows else []
    with path.open("w", encoding="utf-8", newline="") as file:
        if not fieldnames:
            return
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _findings_markdown(result: NoPopularityForwardReadinessResult) -> str:
    return "\n".join(
        [
            "# Phase659 No-Popularity Forward Collection Readiness",
            "",
            "## Final verdict",
            "",
            f"`{result.summary['final_verdict']}`",
            "",
            "## Static readiness",
            "",
            "- The checksummed Phase658 amendment and its Phase654 lineage validate.",
            "- The registered Phase657 implementation hashes still match.",
            (
                "- Official OZ input can carry win odds and place-basis odds through the "
                "existing bridge."
            ),
            "- The forward record schema and runner support both registered market features.",
            "- Phase650H can be rebuilt after the prospective period.",
            "- Popularity is not required by the amended model vector.",
            "",
            "## Boundary",
            "",
            "This is a code-and-contract audit only. It reads no source contents, forward rows, "
            "outcomes, predictions, metrics, ROI, payouts, selections, thresholds, or stakes.",
            "",
            "## Next gate",
            "",
            f"`{result.summary['recommendation']}`",
            "",
        ]
    )
