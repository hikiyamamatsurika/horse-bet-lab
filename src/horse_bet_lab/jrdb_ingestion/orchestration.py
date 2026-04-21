from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from horse_bet_lab.jrdb_ingestion.downloader import (
    DownloadedArchive,
    JRDBDownloadAuth,
    download_archive,
)
from horse_bet_lab.jrdb_ingestion.extractor import ExtractedArchive, extract_archive
from horse_bet_lab.jrdb_ingestion.handoff import JRDBHandoffResult, run_handoff
from horse_bet_lab.jrdb_ingestion.trigger import JRDBAutoIngestionTrigger

DEFAULT_WORKSPACE_ROOT = Path("data/local/jrdb_auto_ingestion")
DEFAULT_RAW_DIR = Path("data/raw/jrdb")


@dataclass(frozen=True)
class JRDBIngestionJobResult:
    job_id: str
    status: str
    dedupe_key: str
    ready_dir: Path | None
    raw_target_dir: Path | None
    report_path: Path


def run_jrdb_auto_ingestion_job(
    trigger: JRDBAutoIngestionTrigger,
    *,
    workspace_root: Path = DEFAULT_WORKSPACE_ROOT,
    raw_dir: Path = DEFAULT_RAW_DIR,
    auth: JRDBDownloadAuth | None = None,
) -> JRDBIngestionJobResult:
    incoming_root = workspace_root / "incoming"
    staging_root = workspace_root / "staging"
    ready_root = workspace_root / "ready"
    state_root = workspace_root / "state"
    runs_root = state_root / "runs"
    processed_root = state_root / "processed"
    for path in (incoming_root, staging_root, ready_root, runs_root, processed_root, raw_dir):
        path.mkdir(parents=True, exist_ok=True)

    dedupe_key = build_dedupe_key(trigger)
    processed_report_path = processed_root / f"{dedupe_key}.json"
    if processed_report_path.exists():
        payload = json.loads(processed_report_path.read_text(encoding="utf-8"))
        return JRDBIngestionJobResult(
            job_id=str(payload["job_id"]),
            status="duplicate_skipped",
            dedupe_key=dedupe_key,
            ready_dir=Path(str(payload["ready_dir"])) if payload.get("ready_dir") else None,
            raw_target_dir=(
                Path(str(payload["raw_target_dir"]))
                if payload.get("raw_target_dir")
                else None
            ),
            report_path=processed_report_path,
        )

    job_id = build_job_id(trigger=trigger, dedupe_key=dedupe_key)
    run_report_path = runs_root / f"{job_id}.json"
    events: list[dict[str, Any]] = []

    try:
        incoming_dir = incoming_root / job_id
        staging_dir = staging_root / job_id
        staging_extract_dir = staging_dir / "extract"
        downloaded_archives = tuple(
            _record_download(events, archive=archive)
            for archive in (
                download_archive(archive, destination_dir=incoming_dir, auth=auth)
                for archive in trigger.archives
            )
        )
        extracted_archives = tuple(
            _record_extract(events, extracted=extract_archive(
                archive.archive_path,
                destination_dir=staging_extract_dir / archive.archive_name,
                archive_kind=_lookup_archive_kind(trigger, archive.archive_name),
            ))
            for archive in downloaded_archives
        )

        ready_dir = ready_root / job_id
        if ready_dir.exists():
            raise FileExistsError(f"ready dir already exists: {ready_dir}")
        staging_dir.rename(ready_dir)

        extracted_root = ready_dir / "extract"
        raw_target_dir = raw_dir / job_id
        _materialize_raw_target(extracted_root=extracted_root, raw_target_dir=raw_target_dir)
        events.append({"event": "raw_target_ready", "path": str(raw_target_dir)})

        handoff_result = run_handoff(trigger, raw_target_dir=raw_target_dir)
        events.append(
            {
                "event": "handoff_completed",
                "unit_id": handoff_result.unit_id,
                "pre_race_output_dir": (
                    str(handoff_result.pre_race_output_dir)
                    if handoff_result.pre_race_output_dir is not None
                    else None
                ),
            }
        )

        report_payload = build_report_payload(
            job_id=job_id,
            status="completed",
            dedupe_key=dedupe_key,
            trigger=trigger,
            downloaded_archives=downloaded_archives,
            extracted_archives=extracted_archives,
            ready_dir=ready_dir,
            raw_target_dir=raw_target_dir,
            handoff_result=handoff_result,
            failure_reason=None,
            events=events,
        )
        ready_manifest_path = ready_dir / "ready_manifest.json"
        ready_manifest_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        run_report_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        processed_report_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return JRDBIngestionJobResult(
            job_id=job_id,
            status="completed",
            dedupe_key=dedupe_key,
            ready_dir=ready_dir,
            raw_target_dir=raw_target_dir,
            report_path=processed_report_path,
        )
    except Exception as exc:
        report_payload = build_report_payload(
            job_id=job_id,
            status="failed",
            dedupe_key=dedupe_key,
            trigger=trigger,
            downloaded_archives=(),
            extracted_archives=(),
            ready_dir=None,
            raw_target_dir=None,
            handoff_result=None,
            failure_reason=str(exc),
            events=events,
        )
        run_report_path.write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        raise


def build_dedupe_key(trigger: JRDBAutoIngestionTrigger) -> str:
    if trigger.message_id is not None:
        return hashlib.sha256(trigger.message_id.encode("utf-8")).hexdigest()
    material = {
        "trigger_kind": trigger.trigger_kind,
        "detected_at": trigger.detected_at,
        "archives": [
            {
                "name": archive.name,
                "source_uri": archive.source_uri,
                "expected_sha256": archive.expected_sha256,
            }
            for archive in trigger.archives
        ],
    }
    return hashlib.sha256(json.dumps(material, sort_keys=True).encode("utf-8")).hexdigest()


def build_job_id(*, trigger: JRDBAutoIngestionTrigger, dedupe_key: str) -> str:
    detected_at = datetime.fromisoformat(trigger.detected_at.replace("Z", "+00:00"))
    return f"{detected_at.astimezone(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{dedupe_key[:8]}"


def build_report_payload(
    *,
    job_id: str,
    status: str,
    dedupe_key: str,
    trigger: JRDBAutoIngestionTrigger,
    downloaded_archives: tuple[DownloadedArchive, ...],
    extracted_archives: tuple[ExtractedArchive, ...],
    ready_dir: Path | None,
    raw_target_dir: Path | None,
    handoff_result: JRDBHandoffResult | None,
    failure_reason: str | None,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "status": status,
        "dedupe_key": dedupe_key,
        "trigger_kind": trigger.trigger_kind,
        "message_id": trigger.message_id,
        "detected_at": trigger.detected_at,
        "ready_dir": str(ready_dir) if ready_dir is not None else None,
        "raw_target_dir": str(raw_target_dir) if raw_target_dir is not None else None,
        "downloaded_archives": [
            {
                "archive_name": archive.archive_name,
                "archive_path": str(archive.archive_path),
                "sha256": archive.sha256,
                "byte_count": archive.byte_count,
            }
            for archive in downloaded_archives
        ],
        "extracted_archives": [
            {
                "archive_path": str(extracted.archive_path),
                "archive_kind": extracted.archive_kind,
                "extracted_files": [str(path) for path in extracted.extracted_files],
            }
            for extracted in extracted_archives
        ],
        "handoff_summary": (
            {
                "unit_id": handoff_result.unit_id,
                "pre_race_output_dir": (
                    str(handoff_result.pre_race_output_dir)
                    if handoff_result.pre_race_output_dir is not None
                    else None
                ),
                "ingested_file_count": (
                    len(handoff_result.ingest_summary.ingested_files)
                    if handoff_result.ingest_summary is not None
                    else 0
                ),
            }
            if handoff_result is not None
            else None
        ),
        "failure_reason": failure_reason,
        "events": events,
    }


def _lookup_archive_kind(trigger: JRDBAutoIngestionTrigger, archive_name: str) -> str | None:
    for archive in trigger.archives:
        if archive.name == archive_name:
            return archive.archive_kind
    return None


def _record_download(
    events: list[dict[str, Any]],
    *,
    archive: DownloadedArchive,
) -> DownloadedArchive:
    events.append(
        {
            "event": "download_completed",
            "archive_name": archive.archive_name,
            "archive_path": str(archive.archive_path),
            "sha256": archive.sha256,
        }
    )
    return archive


def _record_extract(
    events: list[dict[str, Any]],
    *,
    extracted: ExtractedArchive,
) -> ExtractedArchive:
    events.append(
        {
            "event": "extract_completed",
            "archive_path": str(extracted.archive_path),
            "archive_kind": extracted.archive_kind,
            "file_count": len(extracted.extracted_files),
        }
    )
    return extracted


def _materialize_raw_target(*, extracted_root: Path, raw_target_dir: Path) -> None:
    if raw_target_dir.exists():
        raise FileExistsError(f"raw target dir already exists: {raw_target_dir}")
    temp_dir = raw_target_dir.parent / f".{raw_target_dir.name}.tmp"
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    shutil.copytree(extracted_root, temp_dir)
    temp_dir.rename(raw_target_dir)
