from __future__ import annotations

import csv
import hashlib
import json
import zipfile
from pathlib import Path

import duckdb

from horse_bet_lab.jrdb_ingestion.orchestration import run_jrdb_auto_ingestion_job
from horse_bet_lab.jrdb_ingestion.trigger import load_trigger_manifest


def test_jrdb_auto_ingestion_skips_duplicate_trigger(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    archive_path = tmp_path / "fixture.zip"
    _write_zip(archive_path, {"BAC250223.txt": b"fixture-data\n"})
    trigger_manifest_path = tmp_path / "trigger.json"
    trigger_manifest_path.write_text(
        json.dumps(
            {
                "trigger_kind": "manual_fixture",
                "message_id": "fixture-message-duplicate",
                "detected_at": "2026-04-21T11:00:00+09:00",
                "archives": [
                    {
                        "name": "fixture.zip",
                        "source_uri": str(archive_path),
                    }
                ],
                "handoff": {
                    "mode": "none",
                    "ingest_ready_files": False,
                },
            }
        ),
        encoding="utf-8",
    )

    trigger = load_trigger_manifest(trigger_manifest_path)
    first = run_jrdb_auto_ingestion_job(
        trigger,
        workspace_root=tmp_path / "workspace",
        raw_dir=tmp_path / "raw",
    )
    second = run_jrdb_auto_ingestion_job(
        trigger,
        workspace_root=tmp_path / "workspace",
        raw_dir=tmp_path / "raw",
    )

    assert first.status == "completed"
    assert second.status == "duplicate_skipped"
    assert second.report_path.exists()


def test_jrdb_auto_ingestion_rejects_hash_mismatch_before_ready(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    archive_path = tmp_path / "fixture.zip"
    _write_zip(archive_path, {"BAC250223.txt": b"fixture-data\n"})
    trigger_manifest_path = tmp_path / "trigger.json"
    trigger_manifest_path.write_text(
        json.dumps(
            {
                "trigger_kind": "manual_fixture",
                "detected_at": "2026-04-21T11:00:00+09:00",
                "archives": [
                    {
                        "name": "fixture.zip",
                        "source_uri": str(archive_path),
                        "expected_sha256": "deadbeef",
                    }
                ],
                "handoff": {
                    "mode": "none",
                    "ingest_ready_files": False,
                },
            }
        ),
        encoding="utf-8",
    )

    trigger = load_trigger_manifest(trigger_manifest_path)
    try:
        run_jrdb_auto_ingestion_job(
            trigger,
            workspace_root=tmp_path / "workspace",
            raw_dir=tmp_path / "raw",
        )
    except ValueError as exc:
        assert "hash_mismatch" in str(exc)
    else:
        raise AssertionError("expected hash mismatch to raise ValueError")

    ready_root = tmp_path / "workspace" / "ready"
    assert not ready_root.exists() or not any(ready_root.iterdir())
    run_reports = tuple((tmp_path / "workspace" / "state" / "runs").glob("*.json"))
    assert len(run_reports) == 1
    payload = json.loads(run_reports[0].read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert "hash_mismatch" in payload["failure_reason"]


def test_jrdb_auto_ingestion_fixture_can_handoff_to_forward_pre_race(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    archive_path = tmp_path / "fixture.zip"
    _write_zip(archive_path, {"BAC250223.txt": b"fixture-data\n"})
    dataset_path = tmp_path / "dataset.parquet"
    duckdb_path = tmp_path / "jrdb.duckdb"
    source_csv_path = tmp_path / "contract_like.csv"
    _write_dataset_parquet(dataset_path)
    _write_contract_like_source_csv(source_csv_path)
    trigger_manifest_path = tmp_path / "trigger.json"
    trigger_manifest_path.write_text(
        json.dumps(
            {
                "trigger_kind": "manual_fixture",
                "message_id": "fixture-pre-race",
                "detected_at": "2026-04-21T11:00:00+09:00",
                "archives": [
                    {
                        "name": "fixture.zip",
                        "source_uri": str(archive_path),
                        "expected_sha256": _sha256_digest(archive_path),
                    }
                ],
                "handoff": {
                    "mode": "forward_pre_race_contract_like_csv_v1",
                    "ingest_ready_files": False,
                    "unit_id": "20260426_example_meeting",
                    "source_path": str(source_csv_path),
                    "dataset_path": str(dataset_path),
                    "duckdb_path": str(duckdb_path),
                    "model_version": "odds_only_logreg_is_place@fixture",
                    "settled_as_of": "2026-04-26T18:00:00+09:00",
                    "input_source_name": "keibalab_public_pre_race_odds",
                    "input_source_url": "https://example.invalid/source",
                    "input_source_timestamp": "2026-04-26T15:38:00+09:00",
                    "odds_observation_timestamp": "2026-04-26T15:38:00+09:00",
                    "popularity_input_source": "keibalab_public_pre_race_odds",
                },
            }
        ),
        encoding="utf-8",
    )

    trigger = load_trigger_manifest(trigger_manifest_path)
    result = run_jrdb_auto_ingestion_job(
        trigger,
        workspace_root=tmp_path / "workspace",
        raw_dir=tmp_path / "raw",
    )

    assert result.status == "completed"
    run_manifest_path = (
        tmp_path
        / "data"
        / "artifacts"
        / "place_forward_test"
        / "20260426_example_meeting"
        / "pre_race"
        / "run_manifest.json"
    )
    assert run_manifest_path.exists()
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    assert run_manifest["record_counts"]["decision_records"] == 2
    assert run_manifest["bet_logic"]["candidate_logic_id"] == "guard_0_01_plus_proxy_domain_overlay"
    contract_csv_path = (
        tmp_path
        / "data"
        / "forward_test"
        / "runs"
        / "20260426_example_meeting"
        / "contract"
        / "input_snapshot_20260426_example_meeting.csv"
    )
    assert contract_csv_path.exists()


def test_jrdb_auto_ingestion_fixture_can_handoff_from_oz_archive_to_forward_pre_race(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    archive_path = tmp_path / "fixture_oz.zip"
    _write_zip(
        archive_path,
        {
            "nested/OZ250105.txt": (
                _make_oz_line(
                    race_key="06251101",
                    headcount=3,
                    win_basis_odds=(2.4, 8.7, 15.2),
                    place_basis_odds=(1.3, 2.8, 4.1),
                )
                + b"\r\n"
            ),
        },
    )
    dataset_path = tmp_path / "dataset.parquet"
    duckdb_path = tmp_path / "jrdb.duckdb"
    _write_dataset_parquet(dataset_path)
    trigger_manifest_path = tmp_path / "trigger_oz.json"
    trigger_manifest_path.write_text(
        json.dumps(
            {
                "trigger_kind": "manual_fixture",
                "message_id": "fixture-pre-race-oz",
                "detected_at": "2026-04-21T11:00:00+09:00",
                "archives": [
                    {
                        "name": "fixture_oz.zip",
                        "source_uri": str(archive_path),
                        "expected_sha256": _sha256_digest(archive_path),
                        "archive_kind": "zip",
                    }
                ],
                "handoff": {
                    "mode": "forward_pre_race_oz_v1",
                    "ingest_ready_files": False,
                    "unit_id": "20260426_oz_meeting",
                    "dataset_path": str(dataset_path),
                    "duckdb_path": str(duckdb_path),
                    "model_version": "odds_only_logreg_is_place@fixture",
                    "settled_as_of": "2026-04-26T18:00:00+09:00",
                    "input_source_name": "jrdb_oz_official",
                    "input_source_url": "https://example.invalid/jrdb/oz",
                    "input_source_timestamp": "2026-04-26T15:38:00+09:00",
                    "odds_observation_timestamp": "2026-04-26T15:38:00+09:00",
                    "popularity_input_source": "jrdb_oz_official",
                },
            }
        ),
        encoding="utf-8",
    )

    trigger = load_trigger_manifest(trigger_manifest_path)
    result = run_jrdb_auto_ingestion_job(
        trigger,
        workspace_root=tmp_path / "workspace",
        raw_dir=tmp_path / "raw",
    )

    assert result.status == "completed"
    run_manifest_path = (
        tmp_path
        / "data"
        / "artifacts"
        / "place_forward_test"
        / "20260426_oz_meeting"
        / "pre_race"
        / "run_manifest.json"
    )
    assert run_manifest_path.exists()
    run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
    assert run_manifest["record_counts"]["decision_records"] == 3
    raw_snapshot_path = (
        tmp_path
        / "data"
        / "forward_test"
        / "runs"
        / "20260426_oz_meeting"
        / "raw"
        / "input_snapshot_raw.csv"
    )
    rows = list(csv.DictReader(raw_snapshot_path.open(encoding="utf-8")))
    assert len(rows) == 3
    assert rows[0]["place_basis_odds_proxy"] == "1.3"


def _write_zip(path: Path, files: dict[str, bytes]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)


def _write_dataset_parquet(path: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            f"""
            COPY (
                SELECT * FROM (
                    VALUES
                        ('06253811', 1, 'train', 1, 2.0),
                        ('06253811', 2, 'train', 0, 12.0),
                        ('06253812', 1, 'valid', 1, 3.0),
                        ('06253812', 2, 'test', 0, 16.0)
                ) AS t(race_key, horse_number, split, target_value, win_odds)
            ) TO '{path}' (FORMAT PARQUET)
            """
        )
    finally:
        connection.close()


def _write_contract_like_source_csv(path: Path) -> None:
    fieldnames = [
        "race_key",
        "horse_number",
        "win_odds",
        "place_basis_odds",
        "popularity",
        "odds_observation_timestamp",
        "input_source_name",
        "input_source_url",
        "input_source_timestamp",
        "carrier_identity",
        "snapshot_status",
        "retry_count",
        "timeout_seconds",
        "snapshot_failure_reason",
        "popularity_input_source",
        "popularity_contract_status",
        "input_schema_version",
    ]
    rows = [
        {
            "race_key": "06253811",
            "horse_number": "1",
            "win_odds": "2.0",
            "place_basis_odds": "1.4",
            "popularity": "1",
            "odds_observation_timestamp": "2026-04-26T15:38:00+09:00",
            "input_source_name": "keibalab_public_pre_race_odds",
            "input_source_url": "https://example.invalid/source",
            "input_source_timestamp": "2026-04-26T15:38:00+09:00",
            "carrier_identity": "place_forward_live_snapshot_v1",
            "snapshot_status": "ok",
            "retry_count": "1",
            "timeout_seconds": "15",
            "snapshot_failure_reason": "",
            "popularity_input_source": "keibalab_public_pre_race_odds",
            "popularity_contract_status": "unresolved_auxiliary",
            "input_schema_version": "place_forward_test_input_v1",
        },
        {
            "race_key": "06253811",
            "horse_number": "2",
            "win_odds": "12.0",
            "place_basis_odds": "3.8",
            "popularity": "6",
            "odds_observation_timestamp": "2026-04-26T15:38:00+09:00",
            "input_source_name": "keibalab_public_pre_race_odds",
            "input_source_url": "https://example.invalid/source",
            "input_source_timestamp": "2026-04-26T15:38:00+09:00",
            "carrier_identity": "place_forward_live_snapshot_v1",
            "snapshot_status": "ok",
            "retry_count": "1",
            "timeout_seconds": "15",
            "snapshot_failure_reason": "",
            "popularity_input_source": "keibalab_public_pre_race_odds",
            "popularity_contract_status": "unresolved_auxiliary",
            "input_schema_version": "place_forward_test_input_v1",
        },
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _sha256_digest(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _make_oz_line(
    *,
    race_key: str,
    headcount: int,
    win_basis_odds: tuple[float, ...],
    place_basis_odds: tuple[float, ...],
) -> bytes:
    win_text = "".join(f"{value:>5.1f}" for value in win_basis_odds)
    place_text = "".join(f"{value:>5.1f}" for value in place_basis_odds)
    return f"{race_key[:8].ljust(8)}{headcount:02d}{win_text}{' ' * 12}{place_text}".encode("ascii")
