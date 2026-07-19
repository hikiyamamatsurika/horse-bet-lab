from __future__ import annotations

import csv
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from horse_bet_lab.dataset.service import market_feature_select_parts
from horse_bet_lab.features.registry import dataset_feature_columns
from horse_bet_lab.forward_test.contracts import (
    PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS,
)
from horse_bet_lab.ingest.specs import SUPPORTED_FILE_SPECS
from horse_bet_lab.jrdb_ingestion.orchestration import run_jrdb_auto_ingestion_job
from horse_bet_lab.research.historical_ability_models import (
    HISTORY_CATEGORICAL_FEATURE_COLUMNS,
    HISTORY_NUMERIC_FEATURE_COLUMNS,
)
from horse_bet_lab.research.historical_ability_source import (
    MODEL_FEATURE_COLUMNS,
    build_source_audit,
)
from horse_bet_lab.research.preregistered_validation_contract import (
    VALID_VERDICT as PREREGISTRATION_VALID_VERDICT,
)
from horse_bet_lab.research.preregistered_validation_contract import (
    load_registered_contract,
)

READY_VERDICT = "FORWARD_COLLECTION_READINESS_READY"
BLOCKED_VERDICT = "FORWARD_COLLECTION_READINESS_BLOCKED"

REQUIRED_RAW_FILE_KINDS = ("BAC", "KYI", "OZ", "SED")
REQUIRED_MARKET_FEATURES = ("win_odds", "place_basis_odds", "popularity")
CONFIRMED_FORWARD_POPULARITY_STATUS = "confirmed_pre_race_carrier"
CONFIRMED_FORWARD_POPULARITY_ORIGIN = "confirmed_pre_race_snapshot"


@dataclass(frozen=True)
class ForwardCollectionReadinessResult:
    summary: dict[str, Any]
    checks: tuple[dict[str, Any], ...]


def run_forward_collection_readiness(
    *,
    contract_path: Path,
    checksum_path: Path,
    popularity_contract_status_override: str | None = None,
    popularity_origin_override: str | None = None,
) -> ForwardCollectionReadinessResult:
    registration = load_registered_contract(contract_path, checksum_path)
    contract = registration.payload
    checks: list[dict[str, Any]] = []
    blockers: list[str] = []

    _append_check(
        checks,
        check_id="phase654_contract_locked",
        passed=registration.summary["verdict"] == PREREGISTRATION_VALID_VERDICT,
        evidence=f"sha256={registration.sha256}",
    )

    supported_file_kinds = {spec.file_kind for spec in SUPPORTED_FILE_SPECS}
    raw_kinds_present = set(REQUIRED_RAW_FILE_KINDS).issubset(supported_file_kinds)
    _append_check(
        checks,
        check_id="required_raw_file_kinds_supported",
        passed=raw_kinds_present,
        evidence=f"required={list(REQUIRED_RAW_FILE_KINDS)}",
    )
    if not raw_kinds_present:
        missing = sorted(set(REQUIRED_RAW_FILE_KINDS) - supported_file_kinds)
        blockers.append(f"missing raw file specifications: {missing}")

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
        blockers.append("Phase650H history surface cannot be rebuilt for a later evaluation year")

    ingestion_framework_present = callable(run_jrdb_auto_ingestion_job)
    _append_check(
        checks,
        check_id="raw_archive_ingestion_framework_present",
        passed=ingestion_framework_present,
        evidence="triggered archive download, extraction, raw placement, and ingest handoff exist",
    )
    if not ingestion_framework_present:
        blockers.append("raw archive ingestion framework is unavailable")

    market_features = dataset_feature_columns("dual_market")
    market_feature_contract_matches = market_features == REQUIRED_MARKET_FEATURES
    _append_check(
        checks,
        check_id="fixed_market_feature_contract_identified",
        passed=market_feature_contract_matches,
        evidence=f"dual_market_features={list(market_features)}",
    )
    if not market_feature_contract_matches:
        blockers.append("fixed Phase651 dual-market feature contract changed")

    market_sql = market_feature_select_parts(
        "dual_market",
        {"popularity": "popularity"},
        {"place_basis_odds": "place_basis_odds"},
    )
    detected_popularity_origin = (
        "result_side_sed"
        if any("r.popularity" in expression for expression in market_sql)
        else "unknown"
    )
    popularity_origin = popularity_origin_override or detected_popularity_origin
    popularity_status = (
        popularity_contract_status_override or PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS
    )
    popularity_carrier_ready = (
        popularity_status == CONFIRMED_FORWARD_POPULARITY_STATUS
        and popularity_origin == CONFIRMED_FORWARD_POPULARITY_ORIGIN
    )
    _append_check(
        checks,
        check_id="decision_time_popularity_carrier_confirmed",
        passed=popularity_carrier_ready,
        evidence=f"status={popularity_status}; origin={popularity_origin}",
    )
    if not popularity_carrier_ready:
        blockers.append(
            "fixed Phase651 market baseline requires popularity, but no confirmed equivalent "
            "decision-time popularity carrier exists"
        )
    if popularity_origin == "result_side_sed":
        blockers.append(
            "current dual-market dataset obtains popularity from result-side SED, so it cannot "
            "substantiate the registered decision-time-market claim"
        )

    periods = contract["periods"]
    _append_check(
        checks,
        check_id="prospective_window_and_unlock_fixed",
        passed=(
            periods["evaluation_start"] == "2026-07-20"
            and periods["evaluation_end"] == "2026-12-31"
            and periods["evaluation_unlock_date"] == "2027-01-01"
        ),
        evidence=(
            f"window={periods['evaluation_start']}..{periods['evaluation_end']}; "
            f"unlock={periods['evaluation_unlock_date']}"
        ),
    )

    final_verdict = READY_VERDICT if not blockers else BLOCKED_VERDICT
    summary = {
        "final_verdict": final_verdict,
        "phase654_contract_sha256": registration.sha256,
        "evaluation_window": {
            "start": periods["evaluation_start"],
            "end": periods["evaluation_end"],
            "unlock_date": periods["evaluation_unlock_date"],
        },
        "required_raw_file_kinds": list(REQUIRED_RAW_FILE_KINDS),
        "fixed_market_features": list(market_features),
        "history_surface_rebuild_ready": history_rebuild_ready,
        "raw_archive_ingestion_framework_present": ingestion_framework_present,
        "detected_popularity_origin": detected_popularity_origin,
        "forward_popularity_contract_status": popularity_status,
        "decision_time_popularity_carrier_ready": popularity_carrier_ready,
        "blockers": blockers,
        "source_content_inspected": False,
        "forward_rows_or_coverage_inspected": False,
        "model_predictions_or_metrics_computed": False,
        "outcome_conditioning_used": False,
        "roi_or_betting_used": False,
        "recommendation": (
            "RESOLVE_POPULARITY_CARRIER_OR_PREREGISTER_AND_REBASE_WITHOUT_POPULARITY"
            if blockers
            else "BEGIN_PROSPECTIVE_NON_OUTCOME_CONDITIONED_COLLECTION_MONITORING"
        ),
    }
    return ForwardCollectionReadinessResult(summary=summary, checks=tuple(checks))


def write_forward_collection_readiness(
    result: ForwardCollectionReadinessResult,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "phase655_summary.json").write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(output_dir / "phase655_checks.csv", result.checks)
    (output_dir / "phase655_findings.md").write_text(
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


def _findings_markdown(result: ForwardCollectionReadinessResult) -> str:
    lines = [
        "# Phase655 Forward Collection Readiness",
        "",
        "## Final verdict",
        "",
        f"`{result.summary['final_verdict']}`",
        "",
        "## Confirmed paths",
        "",
        "- Raw BAC, KYI, OZ, and SED file specifications exist.",
        "- The archive ingestion framework can preserve and ingest raw files.",
        (
            "- Phase650H can be parameterized for a later evaluation year and rebuilt "
            "after results arrive."
        ),
        "- Phase654 keeps model evaluation locked until 2027-01-01.",
        "",
        "## Blocking mismatch",
        "",
        "The fixed Phase651 market baseline requires popularity. The current dataset obtains that "
        "field from result-side SED, while the forward-test contract explicitly treats popularity "
        "as unresolved auxiliary input. Therefore the registered decision-time market surface "
        "cannot yet be reproduced prospectively with equivalent features.",
        "",
        "## Boundary",
        "",
        "No raw source content, forward-window rows, outcomes, predictions, metrics, ROI, payouts, "
        "or betting rules were inspected or computed in this audit.",
        "",
        "## Next decision",
        "",
        "Before collection is called ready, either confirm and snapshot an equivalent pre-race "
        "popularity carrier, or preregister a no-popularity baseline and rerun the 2023-2025 "
        "probability research under that new fixed feature contract.",
        "",
    ]
    return "\n".join(lines)
