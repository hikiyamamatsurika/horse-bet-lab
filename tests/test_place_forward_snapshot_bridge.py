from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from horse_bet_lab.forward_test.snapshot_bridge import load_snapshot_bridge_config, run_snapshot_bridge


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_bridge_config(path: Path, source_paths: list[Path], output_path: Path) -> None:
    source_blocks = []
    for source_path in source_paths:
        source_blocks.append(
            "\n".join(
                [
                    "[[place_forward_snapshot_bridge.sources]]",
                    f"path = '{source_path}'",
                    "input_source_name = 'operator_csv'",
                    "input_source_url = 'https://example.com/odds'",
                    "input_source_timestamp = '2026-04-19T09:59:00+09:00'",
                    "odds_observation_timestamp = '2026-04-19T10:00:00+09:00'",
                    "carrier_identity = 'place_forward_live_snapshot_v1'",
                    "default_retry_count = 1",
                    "default_timeout_seconds = 15",
                    "default_popularity_input_source = 'operator_csv'",
                ]
            )
        )
    path.write_text(
        "\n".join(
            [
                "[place_forward_snapshot_bridge]",
                "name = 'snapshot_bridge_test'",
                f"output_path = '{output_path}'",
                "strict_race_key = true",
                "infer_snapshot_status = true",
                "write_json_copy = true",
                "",
                "[place_forward_snapshot_bridge.columns]",
                "race_key = 'race_key'",
                "horse_number = 'horse_number'",
                "win_odds = 'win_odds'",
                "place_basis_odds_proxy = 'place_basis_odds_proxy'",
                "popularity = 'popularity'",
                "snapshot_status = 'snapshot_status'",
                "retry_count = 'retry_count'",
                "timeout_seconds = 'timeout_seconds'",
                "snapshot_failure_reason = 'snapshot_failure_reason'",
                "",
                *source_blocks,
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_snapshot_bridge_converts_proxy_rows_to_contract_csv(tmp_path: Path) -> None:
    source_path = tmp_path / "raw.csv"
    output_path = tmp_path / "contract.csv"
    config_path = tmp_path / "bridge.toml"
    write_csv(
        source_path,
        [
            "race_key",
            "horse_number",
            "win_odds",
            "popularity",
            "place_basis_odds_proxy",
        ],
        [
            {
                "race_key": "01260101",
                "horse_number": "3",
                "win_odds": "5.5",
                "popularity": "4",
                "place_basis_odds_proxy": "2.8",
            }
        ],
    )
    write_bridge_config(config_path, [source_path], output_path)

    result = run_snapshot_bridge(load_snapshot_bridge_config(config_path))

    assert result.record_count == 1
    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    assert rows[0]["snapshot_status"] == "ok"
    assert rows[0]["place_basis_odds"] == "2.8"
    assert rows[0]["popularity_contract_status"] == "unresolved_auxiliary"
    manifest = json.loads(output_path.with_suffix(".manifest.json").read_text(encoding="utf-8"))
    assert manifest["snapshot_status_counts"] == {"ok": 1}


def test_snapshot_bridge_marks_missing_odds_as_required_missing(tmp_path: Path) -> None:
    source_path = tmp_path / "raw.csv"
    output_path = tmp_path / "contract.csv"
    config_path = tmp_path / "bridge.toml"
    write_csv(
        source_path,
        [
            "race_key",
            "horse_number",
            "win_odds",
            "snapshot_status",
        ],
        [
            {
                "race_key": "01260101",
                "horse_number": "7",
                "win_odds": "",
                "snapshot_status": "",
            }
        ],
    )
    write_bridge_config(config_path, [source_path], output_path)

    run_snapshot_bridge(load_snapshot_bridge_config(config_path))

    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    assert rows[0]["snapshot_status"] == "required_odds_missing"
    assert "required odds missing" in rows[0]["snapshot_failure_reason"]


def test_snapshot_bridge_rejects_non_digit_race_key(tmp_path: Path) -> None:
    source_path = tmp_path / "raw.csv"
    output_path = tmp_path / "contract.csv"
    config_path = tmp_path / "bridge.toml"
    write_csv(
        source_path,
        ["race_key", "horse_number", "win_odds", "place_basis_odds_proxy"],
        [
            {
                "race_key": "05252a11",
                "horse_number": "1",
                "win_odds": "3.1",
                "place_basis_odds_proxy": "1.8",
            }
        ],
    )
    write_bridge_config(config_path, [source_path], output_path)

    with pytest.raises(ValueError, match="explicit 8-digit race_key"):
        run_snapshot_bridge(load_snapshot_bridge_config(config_path))


def test_snapshot_bridge_combines_multiple_sources(tmp_path: Path) -> None:
    source_one = tmp_path / "raw_one.csv"
    source_two = tmp_path / "raw_two.csv"
    output_path = tmp_path / "contract.csv"
    config_path = tmp_path / "bridge.toml"
    write_csv(
        source_one,
        ["race_key", "horse_number", "win_odds", "place_basis_odds_proxy"],
        [
            {
                "race_key": "01260101",
                "horse_number": "1",
                "win_odds": "2.0",
                "place_basis_odds_proxy": "1.4",
            }
        ],
    )
    write_csv(
        source_two,
        [
            "race_key",
            "horse_number",
            "snapshot_status",
            "retry_count",
            "timeout_seconds",
            "snapshot_failure_reason",
        ],
        [
            {
                "race_key": "02260101",
                "horse_number": "2",
                "snapshot_status": "timeout",
                "retry_count": "3",
                "timeout_seconds": "15",
                "snapshot_failure_reason": "timed out while waiting for final odds snapshot",
            }
        ],
    )
    write_bridge_config(config_path, [source_one, source_two], output_path)

    result = run_snapshot_bridge(load_snapshot_bridge_config(config_path))

    assert result.snapshot_status_counts == {"ok": 1, "timeout": 1}
    rows = list(csv.DictReader(output_path.open(encoding="utf-8")))
    assert len(rows) == 2
