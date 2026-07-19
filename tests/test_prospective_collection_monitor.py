from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from horse_bet_lab.research.prospective_collection_monitor import (
    MONITOR_BLOCKED,
    MONITOR_OK,
    WAITING_FOR_SNAPSHOTS,
    WAITING_FOR_WINDOW,
    ProspectiveCollectionMonitorResult,
    run_prospective_collection_monitor,
    write_prospective_collection_monitor,
)

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "configs/phase658_2026_forward_preregistered_validation.json"
CHECKSUM_PATH = ROOT / "configs/phase658_2026_forward_preregistered_validation.sha256"
SUPERSEDED_CONTRACT_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.json"
SUPERSEDED_CHECKSUM_PATH = ROOT / "configs/phase654_2026_forward_preregistered_validation.sha256"


def _run(
    snapshot_paths: tuple[Path, ...],
    as_of_date: date,
) -> ProspectiveCollectionMonitorResult:
    return run_prospective_collection_monitor(
        contract_path=CONTRACT_PATH,
        checksum_path=CHECKSUM_PATH,
        superseded_contract_path=SUPERSEDED_CONTRACT_PATH,
        superseded_checksum_path=SUPERSEDED_CHECKSUM_PATH,
        repository_root=ROOT,
        snapshot_paths=snapshot_paths,
        as_of_date=as_of_date,
    )


def _write_snapshot(path: Path, *, include_forbidden_column: bool = False) -> None:
    fieldnames = [
        "race_key",
        "horse_number",
        "win_odds",
        "place_basis_odds",
        "odds_observation_timestamp",
        "carrier_identity",
        "snapshot_status",
    ]
    if include_forbidden_column:
        fieldnames.append("finish_position")
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for horse_number in range(1, 9):
            row = {
                "race_key": "26010101",
                "horse_number": horse_number,
                "win_odds": 2.0 + horse_number,
                "place_basis_odds": 1.1 + horse_number / 10,
                "odds_observation_timestamp": "2026-07-20T15:20:00+09:00",
                "carrier_identity": "jrdb_oz_pre_race_v1",
                "snapshot_status": "ok",
            }
            if include_forbidden_column:
                row["finish_position"] = horse_number
            writer.writerow(row)


def test_empty_monitor_waits_before_window() -> None:
    result = _run((), date(2026, 7, 19))

    assert result.summary["final_verdict"] == WAITING_FOR_WINDOW
    assert result.summary["row_count"] == 0
    assert result.summary["source_outcomes_inspected"] is False


def test_empty_monitor_waits_for_first_snapshot_inside_window() -> None:
    result = _run((), date(2026, 7, 20))

    assert result.summary["final_verdict"] == WAITING_FOR_SNAPSHOTS
    assert result.summary["blockers"] == []


def test_valid_contract_snapshot_reports_coverage_only(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "input_snapshot.csv"
    _write_snapshot(snapshot_path)

    result = _run((snapshot_path,), date(2026, 7, 20))

    assert result.summary["final_verdict"] == MONITOR_OK
    assert result.summary["row_count"] == 8
    assert result.summary["unique_identity_count"] == 8
    assert result.summary["race_count"] == 1
    assert result.summary["ok_snapshot_row_count"] == 8
    assert result.summary["model_predictions_computed_or_inspected"] is False
    assert result.summary["model_metrics_computed_or_inspected"] is False
    assert result.summary["roi_or_betting_used"] is False
    assert len(result.file_rows[0]["sha256"]) == 64


def test_monitor_rejects_outcome_column_before_reading_values(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "contaminated_snapshot.csv"
    _write_snapshot(snapshot_path, include_forbidden_column=True)

    result = _run((snapshot_path,), date(2026, 7, 20))

    assert result.summary["final_verdict"] == MONITOR_BLOCKED
    assert any("forbidden outcome/model columns" in blocker for blocker in result.blockers)
    assert result.summary["row_count"] == 0
    assert result.file_rows[0]["row_count"] == 0
    assert result.file_rows[0]["header_status"] == "FAIL"
    assert result.summary["source_outcomes_inspected"] is False


def test_monitor_rejects_duplicate_identity_across_files(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    _write_snapshot(first)
    _write_snapshot(second)

    result = _run((first, second), date(2026, 7, 20))

    assert result.summary["final_verdict"] == MONITOR_BLOCKED
    assert any("duplicate prospective identity" in blocker for blocker in result.blockers)


def test_writer_emits_no_prediction_or_performance_artifacts(tmp_path: Path) -> None:
    result = _run((), date(2026, 7, 19))
    output_dir = tmp_path / "monitor"
    write_prospective_collection_monitor(result, output_dir)

    assert {path.name for path in output_dir.iterdir()} == {
        "phase660_summary.json",
        "phase660_files.csv",
        "phase660_dates.csv",
        "phase660_findings.md",
    }
    assert not any(
        forbidden in path.name
        for path in output_dir.iterdir()
        for forbidden in ("prediction", "metric", "roi", "payout", "bet")
    )
    assert (
        (output_dir / "phase660_files.csv")
        .read_text(encoding="utf-8")
        .startswith("snapshot_path,sha256,row_count,header_status")
    )
    assert (
        (output_dir / "phase660_dates.csv")
        .read_text(encoding="utf-8")
        .startswith("observation_date,row_count,race_count")
    )
