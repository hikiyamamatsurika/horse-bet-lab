from __future__ import annotations

import csv
from pathlib import Path

import pytest

from horse_bet_lab.forward_test.raw_snapshot_prepare import (
    KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE,
    run_raw_snapshot_prepare,
)


def _write_contract_like_source(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
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
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "race_key": "06253811",
                "horse_number": "2",
                "win_odds": "18.7",
                "place_basis_odds": "5.95",
                "popularity": "5",
                "odds_observation_timestamp": "2025-04-20T15:48:00+09:00",
                "input_source_name": "keibalab_public_pre_race_odds",
                "input_source_url": "https://www.keibalab.jp/db/race/202504200611/odds.html",
                "input_source_timestamp": "2025-04-20T15:48:00+09:00",
                "carrier_identity": "place_forward_live_snapshot_v1",
                "snapshot_status": "ok",
                "retry_count": "1",
                "timeout_seconds": "15",
                "snapshot_failure_reason": "",
                "popularity_input_source": "keibalab_public_pre_race_odds",
                "popularity_contract_status": "unresolved_auxiliary",
                "input_schema_version": "place_forward_test_input_v1",
            }
        )


def test_raw_snapshot_prepare_converts_contract_like_source_to_bridge_ready_csv(tmp_path: Path) -> None:
    input_path = tmp_path / "source.csv"
    output_path = tmp_path / "prepared.csv"
    _write_contract_like_source(input_path)

    result = run_raw_snapshot_prepare(
        preset_name=KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE,
        input_path=input_path,
        output_path=output_path,
        force=False,
    )

    assert result.row_count == 1
    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    assert rows[0]["place_basis_odds_proxy"] == "5.95"
    assert "place_basis_odds" not in rows[0]
    assert rows[0]["popularity_contract_status"] == "unresolved_auxiliary"


def test_raw_snapshot_prepare_rejects_missing_required_preset_columns(tmp_path: Path) -> None:
    input_path = tmp_path / "source.csv"
    input_path.write_text(
        "race_key,horse_number,win_odds\n06253811,2,18.7\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required preset columns"):
        run_raw_snapshot_prepare(
            preset_name=KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE,
            input_path=input_path,
            output_path=tmp_path / "prepared.csv",
            force=False,
        )


def test_raw_snapshot_prepare_rejects_existing_output_without_force(tmp_path: Path) -> None:
    input_path = tmp_path / "source.csv"
    output_path = tmp_path / "prepared.csv"
    _write_contract_like_source(input_path)
    output_path.write_text("already here\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="refuses to overwrite"):
        run_raw_snapshot_prepare(
            preset_name=KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE,
            input_path=input_path,
            output_path=output_path,
            force=False,
        )
