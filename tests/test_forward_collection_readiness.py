from __future__ import annotations

from pathlib import Path

from horse_bet_lab.research.forward_collection_readiness import (
    BLOCKED_VERDICT,
    CONFIRMED_FORWARD_POPULARITY_ORIGIN,
    CONFIRMED_FORWARD_POPULARITY_STATUS,
    READY_VERDICT,
    run_forward_collection_readiness,
    write_forward_collection_readiness,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.json"
CHECKSUM_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.sha256"


def test_current_contract_is_blocked_by_unconfirmed_popularity_carrier() -> None:
    result = run_forward_collection_readiness(
        contract_path=CONTRACT_PATH,
        checksum_path=CHECKSUM_PATH,
    )

    assert result.summary["final_verdict"] == BLOCKED_VERDICT
    assert result.summary["history_surface_rebuild_ready"] is True
    assert result.summary["raw_archive_ingestion_framework_present"] is True
    assert result.summary["fixed_market_features"] == [
        "win_odds",
        "place_basis_odds",
        "popularity",
    ]
    assert result.summary["detected_popularity_origin"] == "result_side_sed"
    assert result.summary["decision_time_popularity_carrier_ready"] is False
    assert any("popularity" in blocker for blocker in result.summary["blockers"])
    assert result.summary["source_content_inspected"] is False
    assert result.summary["model_predictions_or_metrics_computed"] is False
    assert result.summary["roi_or_betting_used"] is False


def test_confirmed_equivalent_popularity_carrier_would_clear_static_gate() -> None:
    result = run_forward_collection_readiness(
        contract_path=CONTRACT_PATH,
        checksum_path=CHECKSUM_PATH,
        popularity_contract_status_override=CONFIRMED_FORWARD_POPULARITY_STATUS,
        popularity_origin_override=CONFIRMED_FORWARD_POPULARITY_ORIGIN,
    )

    assert result.summary["final_verdict"] == READY_VERDICT
    assert result.summary["decision_time_popularity_carrier_ready"] is True
    assert result.summary["blockers"] == []


def test_writer_emits_only_static_diagnostic_artifacts(tmp_path: Path) -> None:
    result = run_forward_collection_readiness(
        contract_path=CONTRACT_PATH,
        checksum_path=CHECKSUM_PATH,
    )
    output_dir = tmp_path / "reports"

    write_forward_collection_readiness(result, output_dir)

    assert {path.name for path in output_dir.iterdir()} == {
        "phase655_summary.json",
        "phase655_checks.csv",
        "phase655_findings.md",
    }
    assert not any(
        forbidden in path.name
        for path in output_dir.iterdir()
        for forbidden in ("prediction", "metric", "roi", "payout", "bet")
    )
