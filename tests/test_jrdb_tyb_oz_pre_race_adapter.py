from __future__ import annotations

import csv
from pathlib import Path

from horse_bet_lab.forward_test.raw_snapshot_intake import run_raw_snapshot_intake_precheck
from horse_bet_lab.forward_test.scaffold import PlaceForwardScaffoldConfig, run_scaffold
from horse_bet_lab.jrdb_ingestion.tyb_oz_pre_race_adapter import (
    parse_tyb_line,
    run_tyb_oz_pre_race_adapter,
)


def test_parse_tyb_line_extracts_live_market_fields() -> None:
    row = parse_tyb_line(
        make_tyb_line(
            race_key="06214201",
            horse_number=1,
            odds_index=0.0,
            win_odds=38.3,
            place_odds_low=7.0,
            odds_observation_time_hhmm="0954",
        ),
        source_path=Path("TYB210912.txt"),
        line_number=1,
    )

    assert row is not None
    assert row.race_key == "06214201"
    assert row.horse_number == 1
    assert row.win_odds == 38.3
    assert row.place_odds_low == 7.0
    assert row.odds_observation_time_hhmm == "0954"
    assert row.odds_index == 0.0


def test_tyb_oz_pre_race_adapter_writes_bridge_ready_raw_csv_and_passes_intake(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    tyb_path = tmp_path / "raw" / "nested" / "TYB250105.txt"
    oz_path = tmp_path / "raw" / "nested" / "OZ250105.txt"
    tyb_path.parent.mkdir(parents=True, exist_ok=True)
    tyb_path.write_bytes(
        make_tyb_line(
            race_key="06251101",
            horse_number=1,
            odds_index=0.6,
            win_odds=2.1,
            place_odds_low=1.2,
            odds_observation_time_hhmm="1538",
        )
        + b"\r\n"
        + make_tyb_line(
            race_key="06251101",
            horse_number=2,
            odds_index=-0.4,
            win_odds=8.4,
            place_odds_low=2.7,
            odds_observation_time_hhmm="1538",
        )
        + b"\r\n"
        + make_tyb_line(
            race_key="06251101",
            horse_number=3,
            odds_index=1.5,
            win_odds=14.9,
            place_odds_low=4.0,
            odds_observation_time_hhmm="1538",
        )
        + b"\r\n"
    )
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
            unit_id="20260426_tyb_oz_fixture",
            raw_input_path=Path("data/forward_test/runs/20260426_tyb_oz_fixture/raw/input_snapshot_raw.csv"),
            contract_output_path=Path(
                "data/forward_test/runs/20260426_tyb_oz_fixture/contract/input_snapshot_20260426_tyb_oz_fixture.csv"
            ),
            pre_race_output_dir=Path("data/artifacts/place_forward_test/20260426_tyb_oz_fixture/pre_race"),
            reconciliation_output_dir=Path(
                "data/artifacts/place_forward_test/20260426_tyb_oz_fixture/reconciliation"
            ),
            dataset_path=tmp_path / "dataset.parquet",
            duckdb_path=tmp_path / "jrdb.duckdb",
            model_version="odds_only_logreg_is_place@fixture",
            candidate_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
            threshold=0.08,
            settled_as_of="2026-04-26T18:00:00+09:00",
            config_dir=Path("configs/recurring_rehearsal"),
            bridge_config_path=Path("configs/recurring_rehearsal/20260426_tyb_oz_fixture.bridge.toml"),
            pre_race_config_path=Path("configs/recurring_rehearsal/20260426_tyb_oz_fixture.pre_race.toml"),
            reconciliation_config_path=Path(
                "configs/recurring_rehearsal/20260426_tyb_oz_fixture.reconciliation.toml"
            ),
            notes_dir=Path("data/forward_test/runs/20260426_tyb_oz_fixture/notes"),
            input_source_name="jrdb_tyb_oz_official",
            input_source_url="https://example.invalid/jrdb/tyb-oz",
            input_source_timestamp="2026-04-26T15:38:00+09:00",
            odds_observation_timestamp="2026-04-26T15:38:00+09:00",
            carrier_identity="place_forward_live_snapshot_v1",
            retry_count=1,
            timeout_seconds=15.0,
            popularity_input_source="jrdb_tyb_oz_official",
            force=True,
        )
    )

    result = run_tyb_oz_pre_race_adapter(
        tyb_source_paths=(tyb_path,),
        oz_source_paths=(oz_path,),
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
        "win_odds": "2.1",
        "place_basis_odds_proxy": "1.3",
        "place_odds_min": "1.2",
        "odds_index": "0.6",
    }
    precheck = run_raw_snapshot_intake_precheck(
        bridge_config_path=scaffold_result.bridge_config_path
    )
    assert precheck.unit_id == "20260426_tyb_oz_fixture"


def make_tyb_line(
    *,
    race_key: str,
    horse_number: int,
    odds_index: float,
    win_odds: float,
    place_odds_low: float,
    odds_observation_time_hhmm: str,
) -> bytes:
    chunks = [
        race_key[:8].ljust(8),
        f"{horse_number:02d}",
        f"{0.0:>5.1f}",
        f"{0.0:>5.1f}",
        f"{0.0:>5.1f}",
        f"{odds_index:>5.1f}",
        f"{0.0:>5.1f}",
        f"{0.0:>5.1f}",
        f"{0.0:>5.1f}",
        "0",
        "0",
        "0",
        "00000",
        "ﾃｽﾄｼﾞｮｷ".ljust(12),
        "540",
        "0",
        "10",
        "2",
        f"{win_odds:>6.1f}",
        f"{place_odds_low:>6.1f}",
        odds_observation_time_hhmm[:4].rjust(4),
        "484",
        "- 6",
        " ",
        " ",
        " ",
        "2",
        "7",
        "1538",
        " " * 23,
    ]
    payload = "".join(chunks).encode("cp932")
    return payload


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
