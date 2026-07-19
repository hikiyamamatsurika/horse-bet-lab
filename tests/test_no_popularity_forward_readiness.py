from __future__ import annotations

from pathlib import Path

from horse_bet_lab.research.no_popularity_forward_readiness import (
    READY_VERDICT,
    run_no_popularity_forward_readiness,
    write_no_popularity_forward_readiness,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs/phase658_2026_forward_preregistered_validation.json"
CHECKSUM_PATH = ROOT / "configs/phase658_2026_forward_preregistered_validation.sha256"
SUPERSEDED_CONTRACT_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.json"
SUPERSEDED_CHECKSUM_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.sha256"


def _run():  # type: ignore[no-untyped-def]
    return run_no_popularity_forward_readiness(
        contract_path=CONTRACT_PATH,
        checksum_path=CHECKSUM_PATH,
        superseded_contract_path=SUPERSEDED_CONTRACT_PATH,
        superseded_checksum_path=SUPERSEDED_CHECKSUM_PATH,
        repository_root=ROOT,
    )


def test_amended_contract_clears_static_popularity_blocker() -> None:
    result = _run()

    assert result.summary["final_verdict"] == READY_VERDICT
    assert result.summary["fixed_market_features"] == ["win_odds", "place_basis_odds"]
    assert result.summary["excluded_market_features"] == ["popularity"]
    assert result.summary["popularity_carrier_required"] is False
    assert result.summary["official_oz_market_snapshot_path_ready"] is True
    assert result.summary["history_surface_rebuild_ready"] is True
    assert result.summary["implementation_snapshot_matches"] is True
    assert result.summary["blockers"] == []


def test_readiness_audit_never_inspects_forward_content_or_metrics() -> None:
    result = _run()

    assert result.summary["source_content_inspected"] is False
    assert result.summary["forward_rows_or_coverage_inspected"] is False
    assert result.summary["model_predictions_or_metrics_computed"] is False
    assert result.summary["outcome_conditioning_used"] is False
    assert result.summary["2026_data_used"] is False
    assert result.summary["roi_or_betting_used"] is False


def test_writer_emits_only_static_diagnostic_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "reports"
    write_no_popularity_forward_readiness(_run(), output_dir)

    assert {path.name for path in output_dir.iterdir()} == {
        "phase659_summary.json",
        "phase659_checks.csv",
        "phase659_findings.md",
    }
    assert not any(
        forbidden in path.name
        for path in output_dir.iterdir()
        for forbidden in ("prediction", "metric", "roi", "payout", "bet")
    )
