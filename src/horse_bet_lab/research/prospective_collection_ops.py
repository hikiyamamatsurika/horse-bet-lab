from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from horse_bet_lab.forward_test.raw_snapshot_intake import (
    build_default_raw_snapshot_intake_manifest,
    write_raw_snapshot_intake_manifest,
)
from horse_bet_lab.forward_test.snapshot_bridge import (
    PlaceForwardSnapshotBridgeConfig,
    SnapshotBridgeColumns,
    SnapshotBridgeSourceConfig,
    run_snapshot_bridge,
)
from horse_bet_lab.jrdb_ingestion.oz_pre_race_adapter import discover_oz_source_paths
from horse_bet_lab.jrdb_ingestion.tyb_oz_pre_race_adapter import (
    discover_tyb_source_paths,
    run_tyb_oz_pre_race_adapter,
)
from horse_bet_lab.research.prospective_collection_monitor import (
    MONITOR_OK,
    WAITING_FOR_SNAPSHOTS,
    run_prospective_collection_monitor,
    write_prospective_collection_monitor,
)

UNIT_ID_PATTERN = re.compile(r"^[0-9]{8}_[a-z0-9][a-z0-9_-]*$")
DEFAULT_CARRIER_IDENTITY = "place_forward_live_snapshot_v1"
DEFAULT_INPUT_SOURCE_NAME = "jrdb_tyb_oz_official"


@dataclass(frozen=True)
class ProspectiveCollectionOpsConfig:
    unit_id: str
    source_dir: Path
    output_root: Path
    input_source_name: str
    input_source_url: str
    input_source_timestamp: str
    odds_observation_timestamp: str
    carrier_identity: str
    as_of_date: date
    repository_root: Path
    contract_path: Path
    checksum_path: Path
    superseded_contract_path: Path
    superseded_checksum_path: Path


@dataclass(frozen=True)
class ProspectiveCollectionOpsResult:
    unit_id: str
    run_dir: Path
    raw_snapshot_path: Path
    contract_snapshot_path: Path
    monitor_output_dir: Path
    collection_summary_path: Path
    row_count: int
    race_count: int
    monitor_verdict: str


def run_prospective_collection_ops(
    config: ProspectiveCollectionOpsConfig,
) -> ProspectiveCollectionOpsResult:
    observation_timestamp = _require_timestamp(
        config.odds_observation_timestamp,
        "odds_observation_timestamp",
    )
    source_timestamp = _require_timestamp(
        config.input_source_timestamp,
        "input_source_timestamp",
    )
    _validate_config(
        config,
        observation_timestamp=observation_timestamp,
        source_timestamp=source_timestamp,
    )
    readiness = run_prospective_collection_monitor(
        contract_path=config.contract_path,
        checksum_path=config.checksum_path,
        superseded_contract_path=config.superseded_contract_path,
        superseded_checksum_path=config.superseded_checksum_path,
        repository_root=config.repository_root,
        snapshot_paths=(),
        as_of_date=config.as_of_date,
    )
    if readiness.summary["final_verdict"] != WAITING_FOR_SNAPSHOTS:
        raise ValueError(
            "prospective collection is not open for snapshot intake: "
            f"{readiness.summary['final_verdict']}"
        )

    tyb_source_paths = discover_tyb_source_paths(config.source_dir)
    oz_source_paths = discover_oz_source_paths(config.source_dir)
    run_dir = config.output_root / config.unit_id
    if run_dir.exists():
        raise FileExistsError(
            "prospective collection is append-only and refuses an existing unit directory: "
            f"{run_dir}"
        )
    raw_dir = run_dir / "raw"
    contract_dir = run_dir / "contract"
    notes_dir = run_dir / "notes"
    monitor_output_dir = run_dir / "monitor"
    for path in (raw_dir, contract_dir, notes_dir, monitor_output_dir):
        path.mkdir(parents=True, exist_ok=False)

    raw_snapshot_path = raw_dir / "input_snapshot_raw.csv"
    adapter_result = run_tyb_oz_pre_race_adapter(
        tyb_source_paths=tyb_source_paths,
        oz_source_paths=oz_source_paths,
        output_path=raw_snapshot_path,
        force=False,
    )

    intake_manifest = build_default_raw_snapshot_intake_manifest(
        unit_id=config.unit_id,
        raw_snapshot_path=raw_snapshot_path,
        source_family="jrdb_tyb_oz_pre_race_v1",
        input_source_name=config.input_source_name,
        input_source_url=config.input_source_url,
        input_source_timestamp=config.input_source_timestamp,
        odds_observation_timestamp=config.odds_observation_timestamp,
        carrier_identity=config.carrier_identity,
    )
    write_raw_snapshot_intake_manifest(
        raw_dir / "raw_snapshot_intake_manifest.json",
        intake_manifest,
    )

    contract_snapshot_path = contract_dir / f"input_snapshot_{config.unit_id}.csv"
    bridge_result = run_snapshot_bridge(
        PlaceForwardSnapshotBridgeConfig(
            name=f"phase661_prospective_collection_{config.unit_id}",
            output_path=contract_snapshot_path,
            columns=SnapshotBridgeColumns(),
            strict_race_key=True,
            infer_snapshot_status=True,
            write_json_copy=False,
            sources=(
                SnapshotBridgeSourceConfig(
                    path=raw_snapshot_path,
                    odds_observation_timestamp=config.odds_observation_timestamp,
                    input_source_name=config.input_source_name,
                    input_source_url=config.input_source_url,
                    input_source_timestamp=config.input_source_timestamp,
                    carrier_identity=config.carrier_identity,
                    default_retry_count=1,
                    default_timeout_seconds=15.0,
                    default_popularity_input_source=None,
                ),
            ),
        )
    )

    monitor_result = run_prospective_collection_monitor(
        contract_path=config.contract_path,
        checksum_path=config.checksum_path,
        superseded_contract_path=config.superseded_contract_path,
        superseded_checksum_path=config.superseded_checksum_path,
        repository_root=config.repository_root,
        snapshot_paths=(contract_snapshot_path,),
        as_of_date=config.as_of_date,
    )
    write_prospective_collection_monitor(monitor_result, monitor_output_dir)
    if monitor_result.summary["final_verdict"] != MONITOR_OK:
        raise ValueError(
            "prospective contract snapshot failed Phase660 monitoring: "
            f"{monitor_result.summary['blockers']}"
        )

    collection_summary_path = notes_dir / "phase661_collection_summary.json"
    summary = {
        "unit_id": config.unit_id,
        "source_dir": str(config.source_dir),
        "source_file_count": len(tyb_source_paths) + len(oz_source_paths),
        "source_file_sha256": {
            str(path): _sha256(path) for path in (*tyb_source_paths, *oz_source_paths)
        },
        "raw_snapshot_path": str(raw_snapshot_path),
        "raw_snapshot_sha256": _sha256(raw_snapshot_path),
        "contract_snapshot_path": str(contract_snapshot_path),
        "contract_snapshot_sha256": _sha256(contract_snapshot_path),
        "adapter_row_count": adapter_result.row_count,
        "adapter_race_count": adapter_result.race_count,
        "bridge_record_count": bridge_result.record_count,
        "monitor_verdict": monitor_result.summary["final_verdict"],
        "coverage_monitoring_only": True,
        "model_runner_invoked": False,
        "predictions_or_decisions_generated": False,
        "outcomes_or_model_metrics_inspected": False,
        "roi_or_betting_used": False,
    }
    collection_summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ProspectiveCollectionOpsResult(
        unit_id=config.unit_id,
        run_dir=run_dir,
        raw_snapshot_path=raw_snapshot_path,
        contract_snapshot_path=contract_snapshot_path,
        monitor_output_dir=monitor_output_dir,
        collection_summary_path=collection_summary_path,
        row_count=adapter_result.row_count,
        race_count=adapter_result.race_count,
        monitor_verdict=str(monitor_result.summary["final_verdict"]),
    )


def _validate_config(
    config: ProspectiveCollectionOpsConfig,
    *,
    observation_timestamp: datetime,
    source_timestamp: datetime,
) -> None:
    if UNIT_ID_PATTERN.fullmatch(config.unit_id) is None:
        raise ValueError(
            "unit_id must match YYYYMMDD_<lowercase-label> using letters, digits, underscores, "
            f"or hyphens: {config.unit_id!r}"
        )
    observation_date = observation_timestamp.date()
    if config.as_of_date != observation_date:
        raise ValueError(
            "as_of_date must equal the odds observation date: "
            f"{config.as_of_date.isoformat()} != {observation_date.isoformat()}"
        )
    if not config.unit_id.startswith(observation_date.strftime("%Y%m%d_")):
        raise ValueError("unit_id date prefix must equal the odds observation date")
    if source_timestamp > observation_timestamp:
        raise ValueError("input_source_timestamp must not be later than odds observation timestamp")
    for field_name, value in (
        ("input_source_name", config.input_source_name),
        ("input_source_url", config.input_source_url),
        ("carrier_identity", config.carrier_identity),
    ):
        if value.strip() == "":
            raise ValueError(f"{field_name} must be non-empty")


def _require_timestamp(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be ISO-8601: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone")
    return parsed


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()
