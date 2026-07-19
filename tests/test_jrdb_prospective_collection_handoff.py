from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from horse_bet_lab.jrdb_ingestion.orchestration import run_jrdb_auto_ingestion_job
from horse_bet_lab.jrdb_ingestion.trigger import (
    HANDOFF_MODE_FORWARD_COLLECTION_TYB_OZ,
    load_trigger_manifest,
)
from horse_bet_lab.research.prospective_collection_monitor import MONITOR_OK


def test_auto_ingestion_collection_mode_stops_after_contract_monitor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    archive_path = tmp_path / "fixture_tyb_oz.zip"
    race_key = "06261101"
    tyb_content = b"\r\n".join(
        _make_tyb_line(
            race_key=race_key,
            horse_number=horse_number,
            win_odds=2.0 + horse_number,
            place_odds_low=1.0 + horse_number / 10,
        )
        for horse_number in range(1, 9)
    )
    oz_content = _make_oz_line(
        race_key=race_key,
        headcount=8,
        win_basis_odds=tuple(2.2 + number for number in range(1, 9)),
        place_basis_odds=tuple(1.1 + number / 10 for number in range(1, 9)),
    )
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("nested/TYB260720.txt", tyb_content + b"\r\n")
        archive.writestr("nested/OZ260720.txt", oz_content + b"\r\n")

    manifest_path = tmp_path / "trigger.json"
    manifest_path.write_text(
        json.dumps(
            {
                "trigger_kind": "manual_fixture",
                "message_id": "phase662-synthetic-collection",
                "detected_at": "2026-07-20T15:20:00+09:00",
                "archives": [
                    {
                        "name": archive_path.name,
                        "source_uri": str(archive_path),
                        "expected_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
                        "archive_kind": "zip",
                    }
                ],
                "handoff": {
                    "mode": HANDOFF_MODE_FORWARD_COLLECTION_TYB_OZ,
                    "unit_id": "20260720_synthetic_auto_collection",
                    "input_source_name": "jrdb_tyb_oz_official",
                    "input_source_url": "https://example.invalid/jrdb/tyb-oz",
                    "input_source_timestamp": "2026-07-20T15:19:00+09:00",
                    "odds_observation_timestamp": "2026-07-20T15:20:00+09:00",
                },
            }
        ),
        encoding="utf-8",
    )

    trigger = load_trigger_manifest(manifest_path)
    assert trigger.handoff.collection is not None
    assert trigger.handoff.pre_race is None
    assert trigger.handoff.duckdb_path is None
    assert trigger.handoff.ingest_ready_files is False

    result = run_jrdb_auto_ingestion_job(
        trigger,
        workspace_root=tmp_path / "workspace",
        raw_dir=tmp_path / "raw",
    )

    assert result.status == "completed"
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    handoff_summary = report["handoff_summary"]
    assert handoff_summary["pre_race_output_dir"] is None
    contract_path = Path(handoff_summary["contract_snapshot_path"])
    monitor_dir = Path(handoff_summary["collection_monitor_output_dir"])
    assert contract_path.is_file()
    monitor_summary = json.loads((monitor_dir / "phase660_summary.json").read_text())
    assert monitor_summary["final_verdict"] == MONITOR_OK
    assert not (tmp_path / "data/artifacts/place_forward_test").exists()


def _make_tyb_line(
    *,
    race_key: str,
    horse_number: int,
    win_odds: float,
    place_odds_low: float,
) -> bytes:
    chunks = [
        race_key[:8].ljust(8),
        f"{horse_number:02d}",
        f"{0.0:>5.1f}" * 3,
        f"{horse_number / 10:>5.1f}",
        f"{0.0:>5.1f}" * 3,
        "000",
        "00000",
        "ﾃｽﾄｼﾞｮｷ".ljust(12),
        "540",
        "0",
        "10",
        "2",
        f"{win_odds:>6.1f}",
        f"{place_odds_low:>6.1f}",
        "1520",
        "484",
        "- 6",
        "   ",
        "2",
        "7",
        "1538",
        " " * 23,
    ]
    return "".join(chunks).encode("cp932")


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
