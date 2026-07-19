from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from horse_bet_lab.research.prospective_collection_monitor import MONITOR_OK
from horse_bet_lab.research.prospective_collection_ops import (
    ProspectiveCollectionOpsConfig,
    run_prospective_collection_ops,
)

ROOT = Path(__file__).resolve().parents[1]


def _config(
    tmp_path: Path, *, as_of_date: date = date(2026, 7, 20)
) -> ProspectiveCollectionOpsConfig:
    return ProspectiveCollectionOpsConfig(
        unit_id="20260720_synthetic_meeting",
        source_dir=tmp_path / "source",
        output_root=tmp_path / "collection",
        input_source_name="jrdb_tyb_oz_official",
        input_source_url="https://example.invalid/jrdb/tyb-oz",
        input_source_timestamp="2026-07-20T15:19:00+09:00",
        odds_observation_timestamp="2026-07-20T15:20:00+09:00",
        carrier_identity="place_forward_live_snapshot_v1",
        as_of_date=as_of_date,
        repository_root=ROOT,
        contract_path=ROOT / "configs/phase658_2026_forward_preregistered_validation.json",
        checksum_path=ROOT / "configs/phase658_2026_forward_preregistered_validation.sha256",
        superseded_contract_path=(
            ROOT / "configs/phase654_2026_forward_preregistered_validation.json"
        ),
        superseded_checksum_path=(
            ROOT / "configs/phase654_2026_forward_preregistered_validation.sha256"
        ),
    )


def _write_sources(source_dir: Path) -> None:
    source_dir.mkdir(parents=True)
    race_key = "06261101"
    tyb_lines = [
        _make_tyb_line(
            race_key=race_key,
            horse_number=horse_number,
            odds_index=horse_number / 10,
            win_odds=2.0 + horse_number,
            place_odds_low=1.0 + horse_number / 10,
            odds_observation_time_hhmm="1520",
        )
        for horse_number in range(1, 9)
    ]
    (source_dir / "TYB260720.txt").write_bytes(b"\r\n".join(tyb_lines) + b"\r\n")
    (source_dir / "OZ260720.txt").write_bytes(
        _make_oz_line(
            race_key=race_key,
            headcount=8,
            win_basis_odds=tuple(2.2 + number for number in range(1, 9)),
            place_basis_odds=tuple(1.1 + number / 10 for number in range(1, 9)),
        )
        + b"\r\n"
    )


def test_collection_ops_builds_contract_and_monitor_without_model_outputs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_sources(config.source_dir)

    result = run_prospective_collection_ops(config)

    assert result.row_count == 8
    assert result.race_count == 1
    assert result.monitor_verdict == MONITOR_OK
    assert result.contract_snapshot_path.is_file()
    monitor_summary = json.loads(
        (result.monitor_output_dir / "phase660_summary.json").read_text(encoding="utf-8")
    )
    assert monitor_summary["final_verdict"] == MONITOR_OK
    collection_summary = json.loads(result.collection_summary_path.read_text(encoding="utf-8"))
    assert collection_summary["model_runner_invoked"] is False
    assert collection_summary["predictions_or_decisions_generated"] is False
    assert collection_summary["outcomes_or_model_metrics_inspected"] is False
    names = {path.name for path in result.run_dir.rglob("*") if path.is_file()}
    assert not any(
        forbidden in name
        for name in names
        for forbidden in ("prediction", "decision", "metric", "roi", "payout", "result")
    )


def test_collection_ops_refuses_before_registered_window_without_writing(tmp_path: Path) -> None:
    config = replace(
        _config(tmp_path, as_of_date=date(2026, 7, 19)),
        unit_id="20260719_synthetic_meeting",
        input_source_timestamp="2026-07-19T15:19:00+09:00",
        odds_observation_timestamp="2026-07-19T15:20:00+09:00",
    )
    _write_sources(config.source_dir)

    with pytest.raises(ValueError, match="collection is not open"):
        run_prospective_collection_ops(config)

    assert not config.output_root.exists()


def test_collection_ops_refuses_existing_append_only_unit(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_sources(config.source_dir)
    (config.output_root / config.unit_id).mkdir(parents=True)

    with pytest.raises(FileExistsError, match="append-only"):
        run_prospective_collection_ops(config)


def test_collection_ops_rejects_source_timestamp_after_observation(tmp_path: Path) -> None:
    config = replace(
        _config(tmp_path),
        input_source_timestamp="2026-07-20T15:21:00+09:00",
    )

    with pytest.raises(ValueError, match="must not be later"):
        run_prospective_collection_ops(config)


def _make_tyb_line(
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
