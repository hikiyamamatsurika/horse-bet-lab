from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

from horse_bet_lab.forward_test.contracts import PLACE_FORWARD_TEST_SNAPSHOT_STATUSES
from horse_bet_lab.research.preregistered_validation_amendment import (
    load_amended_registered_contract,
    verify_implementation_snapshot,
)

WAITING_FOR_WINDOW = "FORWARD_COLLECTION_MONITOR_WAITING_FOR_WINDOW"
WAITING_FOR_SNAPSHOTS = "FORWARD_COLLECTION_MONITOR_WAITING_FOR_SNAPSHOTS"
MONITOR_OK = "FORWARD_COLLECTION_MONITOR_OK"
MONITOR_BLOCKED = "FORWARD_COLLECTION_MONITOR_BLOCKED"

REQUIRED_COLUMNS = (
    "race_key",
    "horse_number",
    "win_odds",
    "place_basis_odds",
    "odds_observation_timestamp",
    "carrier_identity",
    "snapshot_status",
)
FILE_REPORT_COLUMNS = (
    "snapshot_path",
    "sha256",
    "row_count",
    "header_status",
)
DATE_REPORT_COLUMNS = (
    "observation_date",
    "row_count",
    "race_count",
)
FORBIDDEN_EXACT_COLUMNS = frozenset(
    {
        "target",
        "target_value",
        "is_place",
        "finish_position",
        "finish_order",
        "result",
        "result_code",
        "payout",
        "return",
        "profit",
        "hit",
        "prediction",
        "prediction_probability",
        "probability",
        "log_loss",
        "brier_score",
        "roi",
        "stake",
        "bet_action",
        "decision_reason",
        "no_bet_reason",
    }
)
FORBIDDEN_PREFIXES = (
    "target_",
    "result_",
    "finish_",
    "payout_",
    "return_",
    "profit_",
    "prediction_",
    "metric_",
    "roi_",
    "stake_",
    "bet_",
)


@dataclass(frozen=True)
class ProspectiveCollectionMonitorResult:
    summary: dict[str, Any]
    file_rows: tuple[dict[str, Any], ...]
    date_rows: tuple[dict[str, Any], ...]
    blockers: tuple[str, ...]


def run_prospective_collection_monitor(
    *,
    contract_path: Path,
    checksum_path: Path,
    superseded_contract_path: Path,
    superseded_checksum_path: Path,
    repository_root: Path,
    snapshot_paths: Sequence[Path],
    as_of_date: date,
) -> ProspectiveCollectionMonitorResult:
    registration = load_amended_registered_contract(
        contract_path,
        checksum_path,
        superseded_contract_path,
        superseded_checksum_path,
    )
    verify_implementation_snapshot(registration, repository_root)
    periods = registration.payload["periods"]
    evaluation_start = date.fromisoformat(str(periods["evaluation_start"]))
    evaluation_end = date.fromisoformat(str(periods["evaluation_end"]))
    unlock_date = date.fromisoformat(str(periods["evaluation_unlock_date"]))

    if not snapshot_paths:
        verdict = WAITING_FOR_WINDOW if as_of_date < evaluation_start else WAITING_FOR_SNAPSHOTS
        if as_of_date >= unlock_date:
            verdict = MONITOR_BLOCKED
        waiting_blockers = (
            ("Phase660 pre-unlock monitor is no longer applicable on or after unlock",)
            if verdict == MONITOR_BLOCKED
            else ()
        )
        return _empty_result(
            verdict=verdict,
            blockers=waiting_blockers,
            registration_sha256=registration.sha256,
            evaluation_start=evaluation_start,
            evaluation_end=evaluation_end,
            unlock_date=unlock_date,
            as_of_date=as_of_date,
        )

    blockers: list[str] = []
    if as_of_date < evaluation_start:
        blockers.append("snapshot inspection is forbidden before the prospective window starts")
    if as_of_date >= unlock_date:
        blockers.append("Phase660 pre-unlock monitor is no longer applicable on or after unlock")

    seen_identities: dict[tuple[str, int], Path] = {}
    date_counts: Counter[str] = Counter()
    date_races: dict[str, set[str]] = {}
    file_rows: list[dict[str, Any]] = []
    total_rows = 0
    total_ok_rows = 0
    total_failure_rows = 0
    total_missing_win_odds = 0
    total_missing_place_basis_odds = 0
    all_races: set[str] = set()

    for snapshot_path in snapshot_paths:
        file_summary, rows = _read_snapshot_file(snapshot_path, blockers)
        file_rows.append(file_summary)
        for row_index, row in rows:
            total_rows += 1
            parsed = _validate_collection_row(
                row,
                snapshot_path=snapshot_path,
                row_index=row_index,
                evaluation_start=evaluation_start,
                evaluation_end=evaluation_end,
                blockers=blockers,
            )
            if parsed is None:
                continue
            (
                race_key,
                horse_number,
                observation_date,
                snapshot_status,
                win_missing,
                place_missing,
            ) = parsed
            identity = (race_key, horse_number)
            previous_path = seen_identities.get(identity)
            if previous_path is not None:
                blockers.append(
                    "duplicate prospective identity across snapshots: "
                    f"{race_key}:{horse_number} in {previous_path} and {snapshot_path}"
                )
            else:
                seen_identities[identity] = snapshot_path
            observation_label = observation_date.isoformat()
            date_counts[observation_label] += 1
            date_races.setdefault(observation_label, set()).add(race_key)
            all_races.add(race_key)
            total_ok_rows += int(snapshot_status == "ok")
            total_failure_rows += int(snapshot_status != "ok")
            total_missing_win_odds += int(win_missing)
            total_missing_place_basis_odds += int(place_missing)

    if total_rows == 0:
        blockers.append("prospective snapshot inputs contain no data rows")

    date_rows = tuple(
        {
            "observation_date": observation_date,
            "row_count": date_counts[observation_date],
            "race_count": len(date_races[observation_date]),
        }
        for observation_date in sorted(date_counts)
    )
    deduplicated_blockers = tuple(dict.fromkeys(blockers))
    verdict = MONITOR_OK if not deduplicated_blockers else MONITOR_BLOCKED
    summary = {
        "final_verdict": verdict,
        "phase658_contract_sha256": registration.sha256,
        "as_of_date": as_of_date.isoformat(),
        "evaluation_window": {
            "start": evaluation_start.isoformat(),
            "end": evaluation_end.isoformat(),
            "unlock_date": unlock_date.isoformat(),
        },
        "snapshot_file_count": len(snapshot_paths),
        "row_count": total_rows,
        "unique_identity_count": len(seen_identities),
        "race_count": len(all_races),
        "observation_date_count": len(date_counts),
        "ok_snapshot_row_count": total_ok_rows,
        "failed_snapshot_row_count": total_failure_rows,
        "missing_win_odds_count": total_missing_win_odds,
        "missing_place_basis_odds_count": total_missing_place_basis_odds,
        "blocker_count": len(deduplicated_blockers),
        "blockers": list(deduplicated_blockers),
        "coverage_monitoring_only": True,
        "source_outcomes_inspected": False,
        "model_predictions_computed_or_inspected": False,
        "model_metrics_computed_or_inspected": False,
        "roi_or_betting_used": False,
        "recommendation": (
            "CONTINUE_APPEND_ONLY_PROSPECTIVE_SNAPSHOT_COLLECTION"
            if verdict == MONITOR_OK
            else "RESOLVE_COLLECTION_CONTRACT_VIOLATIONS_BEFORE_CONTINUING"
        ),
    }
    return ProspectiveCollectionMonitorResult(
        summary=summary,
        file_rows=tuple(file_rows),
        date_rows=date_rows,
        blockers=deduplicated_blockers,
    )


def write_prospective_collection_monitor(
    result: ProspectiveCollectionMonitorResult,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "phase660_summary.json").write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(
        output_dir / "phase660_files.csv",
        result.file_rows,
        fieldnames=FILE_REPORT_COLUMNS,
    )
    _write_csv(
        output_dir / "phase660_dates.csv",
        result.date_rows,
        fieldnames=DATE_REPORT_COLUMNS,
    )
    (output_dir / "phase660_findings.md").write_text(
        _findings_markdown(result),
        encoding="utf-8",
    )


def _read_snapshot_file(
    snapshot_path: Path,
    blockers: list[str],
) -> tuple[dict[str, Any], tuple[tuple[int, dict[str, str]], ...]]:
    if not snapshot_path.is_file():
        blockers.append(f"prospective snapshot file is missing: {snapshot_path}")
        return (
            {
                "snapshot_path": str(snapshot_path),
                "sha256": "",
                "row_count": 0,
                "header_status": "MISSING",
            },
            (),
        )
    raw = snapshot_path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    with snapshot_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        fieldnames = tuple(reader.fieldnames or ())
        normalized = tuple(value.strip().lower() for value in fieldnames)
        duplicate_headers = len(set(normalized)) != len(normalized)
        if duplicate_headers:
            blockers.append(f"duplicate header columns in prospective snapshot: {snapshot_path}")
        missing_required = sorted(set(REQUIRED_COLUMNS) - set(normalized))
        forbidden = sorted(value for value in normalized if _is_forbidden_column(value))
        if missing_required:
            blockers.append(
                "prospective snapshot missing required columns in "
                f"{snapshot_path}: {missing_required}"
            )
        if forbidden:
            blockers.append(
                f"prospective snapshot contains forbidden outcome/model columns in "
                f"{snapshot_path}: {forbidden}"
            )
        header_valid = not duplicate_headers and not missing_required and not forbidden
        if not header_valid:
            return (
                {
                    "snapshot_path": str(snapshot_path),
                    "sha256": digest,
                    "row_count": 0,
                    "header_status": "FAIL",
                },
                (),
            )
        rows = tuple(
            (
                row_index,
                {str(key).strip().lower(): (value or "").strip() for key, value in row.items()},
            )
            for row_index, row in enumerate(reader, start=2)
        )
    return (
        {
            "snapshot_path": str(snapshot_path),
            "sha256": digest,
            "row_count": len(rows),
            "header_status": "PASS",
        },
        rows,
    )


def _validate_collection_row(
    row: dict[str, str],
    *,
    snapshot_path: Path,
    row_index: int,
    evaluation_start: date,
    evaluation_end: date,
    blockers: list[str],
) -> tuple[str, int, date, str, bool, bool] | None:
    prefix = f"{snapshot_path}:{row_index}"
    race_key = row.get("race_key", "")
    if len(race_key) != 8 or not race_key.isdigit():
        blockers.append(f"invalid race_key at {prefix}: {race_key!r}")
        return None
    try:
        horse_number = int(row.get("horse_number", ""))
    except ValueError:
        blockers.append(f"invalid horse_number at {prefix}: {row.get('horse_number', '')!r}")
        return None
    if horse_number <= 0:
        blockers.append(f"horse_number must be positive at {prefix}: {horse_number}")
        return None

    timestamp_text = row.get("odds_observation_timestamp", "")
    try:
        timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
    except ValueError:
        blockers.append(f"invalid odds_observation_timestamp at {prefix}: {timestamp_text!r}")
        return None
    if timestamp.tzinfo is None:
        blockers.append(f"odds_observation_timestamp lacks timezone at {prefix}")
        return None
    observation_date = timestamp.date()
    if not evaluation_start <= observation_date <= evaluation_end:
        blockers.append(
            "observation date outside registered window at "
            f"{prefix}: {observation_date.isoformat()}"
        )

    snapshot_status = row.get("snapshot_status", "")
    if snapshot_status not in PLACE_FORWARD_TEST_SNAPSHOT_STATUSES:
        blockers.append(f"invalid snapshot_status at {prefix}: {snapshot_status!r}")
        return None
    if row.get("carrier_identity", "") == "":
        blockers.append(f"missing carrier_identity at {prefix}")

    win_missing = row.get("win_odds", "") == ""
    place_missing = row.get("place_basis_odds", "") == ""
    if snapshot_status == "ok":
        _validate_positive_odds(row.get("win_odds", ""), "win_odds", prefix, blockers)
        _validate_positive_odds(
            row.get("place_basis_odds", ""),
            "place_basis_odds",
            prefix,
            blockers,
        )
    return (
        race_key,
        horse_number,
        observation_date,
        snapshot_status,
        win_missing,
        place_missing,
    )


def _validate_positive_odds(
    value: str,
    field_name: str,
    prefix: str,
    blockers: list[str],
) -> None:
    try:
        parsed = float(value)
    except ValueError:
        blockers.append(f"invalid {field_name} at {prefix}: {value!r}")
        return
    if parsed <= 0.0:
        blockers.append(f"{field_name} must be positive at {prefix}: {parsed}")


def _is_forbidden_column(column_name: str) -> bool:
    return column_name in FORBIDDEN_EXACT_COLUMNS or column_name.startswith(FORBIDDEN_PREFIXES)


def _empty_result(
    *,
    verdict: str,
    blockers: Sequence[str],
    registration_sha256: str,
    evaluation_start: date,
    evaluation_end: date,
    unlock_date: date,
    as_of_date: date,
) -> ProspectiveCollectionMonitorResult:
    summary = {
        "final_verdict": verdict,
        "phase658_contract_sha256": registration_sha256,
        "as_of_date": as_of_date.isoformat(),
        "evaluation_window": {
            "start": evaluation_start.isoformat(),
            "end": evaluation_end.isoformat(),
            "unlock_date": unlock_date.isoformat(),
        },
        "snapshot_file_count": 0,
        "row_count": 0,
        "unique_identity_count": 0,
        "race_count": 0,
        "observation_date_count": 0,
        "ok_snapshot_row_count": 0,
        "failed_snapshot_row_count": 0,
        "missing_win_odds_count": 0,
        "missing_place_basis_odds_count": 0,
        "blocker_count": len(blockers),
        "blockers": list(blockers),
        "coverage_monitoring_only": True,
        "source_outcomes_inspected": False,
        "model_predictions_computed_or_inspected": False,
        "model_metrics_computed_or_inspected": False,
        "roi_or_betting_used": False,
        "recommendation": {
            WAITING_FOR_WINDOW: "WAIT_UNTIL_REGISTERED_EVALUATION_WINDOW",
            WAITING_FOR_SNAPSHOTS: "WAIT_FOR_FIRST_CONTRACT_SNAPSHOT",
            MONITOR_BLOCKED: "STOP_PHASE660_AFTER_UNLOCK",
        }[verdict],
    }
    return ProspectiveCollectionMonitorResult(
        summary=summary,
        file_rows=(),
        date_rows=(),
        blockers=tuple(blockers),
    )


def _write_csv(
    path: Path,
    rows: Iterable[dict[str, Any]],
    *,
    fieldnames: Sequence[str],
) -> None:
    rows = tuple(rows)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _findings_markdown(result: ProspectiveCollectionMonitorResult) -> str:
    return "\n".join(
        [
            "# Phase660 Prospective Collection Monitor",
            "",
            "## Verdict",
            "",
            f"`{result.summary['final_verdict']}`",
            "",
            "## Coverage-only summary",
            "",
            f"- Snapshot files: `{result.summary['snapshot_file_count']}`",
            f"- Rows: `{result.summary['row_count']}`",
            f"- Races: `{result.summary['race_count']}`",
            f"- Blockers: `{result.summary['blocker_count']}`",
            "",
            "## Boundary",
            "",
            "The monitor rejects outcome, prediction, model-metric, ROI, stake, and betting "
            "columns. It reports only schema, file hashes, dates, identities, status, and "
            "required-odds missingness.",
            "",
        ]
    )
