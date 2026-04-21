from __future__ import annotations

import csv
from pathlib import Path

from horse_bet_lab.forward_test.raw_snapshot_intake import run_raw_snapshot_intake_precheck
from horse_bet_lab.forward_test.scaffold import PlaceForwardScaffoldConfig, run_scaffold
from horse_bet_lab.jrdb_ingestion.oz_pre_race_adapter import (
    parse_oz_line,
    run_oz_pre_race_adapter,
)


def test_parse_oz_line_expands_headcount_rows() -> None:
    rows = parse_oz_line(
        make_oz_line(
            race_key="06251101",
            headcount=3,
            win_basis_odds=(2.4, 8.7, 15.2),
            place_basis_odds=(1.3, 2.8, 4.1),
        ),
        source_path=Path("OZ250105.txt"),
        line_number=1,
    )

    assert [row.horse_number for row in rows] == [1, 2, 3]
    assert [row.win_odds for row in rows] == [2.4, 8.7, 15.2]
    assert [row.place_basis_odds for row in rows] == [1.3, 2.8, 4.1]


def test_oz_pre_race_adapter_writes_bridge_ready_raw_csv_and_passes_intake(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    oz_path = tmp_path / "raw" / "nested" / "OZ250105.txt"
    oz_path.parent.mkdir(parents=True, exist_ok=True)
    oz_path.write_bytes(
        make_oz_line(
            race_key="06251101",
            headcount=3,
            win_basis_odds=(2.4, 8.7, 15.2),
            place_basis_odds=(1.3, 2.8, 4.1),
        )
        + b"\r\n"
    )

    scaffold_result = run_scaffold(
        PlaceForwardScaffoldConfig(
            unit_id="20260426_oz_fixture",
            raw_input_path=Path("data/forward_test/runs/20260426_oz_fixture/raw/input_snapshot_raw.csv"),
            contract_output_path=Path(
                "data/forward_test/runs/20260426_oz_fixture/contract/input_snapshot_20260426_oz_fixture.csv"
            ),
            pre_race_output_dir=Path("data/artifacts/place_forward_test/20260426_oz_fixture/pre_race"),
            reconciliation_output_dir=Path(
                "data/artifacts/place_forward_test/20260426_oz_fixture/reconciliation"
            ),
            dataset_path=tmp_path / "dataset.parquet",
            duckdb_path=tmp_path / "jrdb.duckdb",
            model_version="odds_only_logreg_is_place@fixture",
            candidate_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
            threshold=0.08,
            settled_as_of="2026-04-26T18:00:00+09:00",
            config_dir=Path("configs/recurring_rehearsal"),
            bridge_config_path=Path("configs/recurring_rehearsal/20260426_oz_fixture.bridge.toml"),
            pre_race_config_path=Path("configs/recurring_rehearsal/20260426_oz_fixture.pre_race.toml"),
            reconciliation_config_path=Path(
                "configs/recurring_rehearsal/20260426_oz_fixture.reconciliation.toml"
            ),
            notes_dir=Path("data/forward_test/runs/20260426_oz_fixture/notes"),
            input_source_name="jrdb_oz_official",
            input_source_url="https://example.invalid/jrdb/oz",
            input_source_timestamp="2026-04-26T15:38:00+09:00",
            odds_observation_timestamp="2026-04-26T15:38:00+09:00",
            carrier_identity="place_forward_live_snapshot_v1",
            retry_count=1,
            timeout_seconds=15.0,
            popularity_input_source="jrdb_oz_official",
            force=True,
        )
    )

    result = run_oz_pre_race_adapter(
        source_paths=(oz_path,),
        output_path=scaffold_result.raw_dir / "input_snapshot_raw.csv",
        force=True,
    )

    assert result.row_count == 3
    rows = list(
        csv.DictReader(
            (scaffold_result.raw_dir / "input_snapshot_raw.csv").open(encoding="utf-8")
        )
    )
    assert rows[0] == {
        "race_key": "06251101",
        "horse_number": "1",
        "win_odds": "2.4",
        "place_basis_odds_proxy": "1.3",
    }
    precheck = run_raw_snapshot_intake_precheck(
        bridge_config_path=scaffold_result.bridge_config_path
    )
    assert precheck.unit_id == "20260426_oz_fixture"


def make_oz_line(
    *,
    race_key: str,
    headcount: int,
    win_basis_odds: tuple[float, ...],
    place_basis_odds: tuple[float, ...],
) -> bytes:
    win_text = "".join(f"{value:>5.1f}" for value in win_basis_odds)
    place_text = "".join(f"{value:>5.1f}" for value in place_basis_odds)
    return f"{race_key[:8].ljust(8)}{headcount:02d}{win_text}{' ' * 12}{place_text}".encode("ascii")
