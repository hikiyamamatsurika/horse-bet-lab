from __future__ import annotations

from pathlib import Path

from horse_bet_lab.research.historical_ability_source import (
    BLOCKED_VERDICT,
    READY_VERDICT,
    build_source_audit,
    write_source_audit,
)


def _write_field(payload: bytearray, start: int, end: int, value: str) -> None:
    encoded = value.encode("cp932")
    if len(encoded) > end - start:
        raise ValueError(value)
    payload[start:end] = encoded.ljust(end - start, b" ")


def _make_kyi_line(
    *,
    race_key: str,
    horse_number: int,
    registration_id: str,
    horse_name: str,
) -> bytes:
    payload = bytearray(b" " * 1022)
    _write_field(payload, 0, 8, race_key)
    _write_field(payload, 8, 10, f"{horse_number:02d}")
    _write_field(payload, 10, 18, registration_id)
    _write_field(payload, 18, 54, horse_name)
    return bytes(payload)


def _make_sed_line(
    *,
    race_key: str,
    horse_number: int,
    registration_id: str,
    result_date: str,
    horse_name: str,
    distance_m: int,
    finish_position: int,
) -> bytes:
    payload = bytearray(b" " * 374)
    _write_field(payload, 0, 8, race_key)
    _write_field(payload, 8, 10, f"{horse_number:02d}")
    _write_field(payload, 10, 18, registration_id)
    _write_field(payload, 18, 26, result_date)
    _write_field(payload, 26, 62, horse_name)
    _write_field(payload, 62, 66, f"{distance_m:04d}")
    _write_field(payload, 140, 142, f"{finish_position:02d}")
    return bytes(payload)


def _make_bac_line(*, race_key: str, race_date: str, distance_m: int) -> bytes:
    payload = bytearray(b" " * 182)
    _write_field(payload, 0, 8, race_key)
    _write_field(payload, 8, 16, race_date)
    _write_field(payload, 20, 24, f"{distance_m:04d}")
    return bytes(payload)


def _write_lines(path: Path, lines: list[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\r\n".join(lines) + b"\r\n")


def _build_fixture(
    raw_root: Path,
    *,
    mismatch_first_identity: bool = False,
    duplicate_current_sed: bool = False,
    conflicting_current_sed: bool = False,
) -> None:
    prior_registrations = [f"2200000{number}" for number in range(1, 6)]
    prior_names = [f"履歴馬{number}" for number in range(1, 6)]
    current_registrations = [prior_registrations[0]] + [
        f"2300000{number}" for number in range(2, 9)
    ]
    current_names = [prior_names[0]] + [f"現役馬{number}" for number in range(2, 9)]

    _write_lines(
        raw_root / "SED_2022" / "SED221218.txt",
        [
            _make_sed_line(
                race_key="06221101",
                horse_number=number,
                registration_id=prior_registrations[number - 1],
                result_date="20221218",
                horse_name=prior_names[number - 1],
                distance_m=1800,
                finish_position=number,
            )
            for number in range(1, 6)
        ],
    )
    _write_lines(
        raw_root / "BAC_2022" / "BAC221218.txt",
        [_make_bac_line(race_key="06221101", race_date="20221218", distance_m=1800)],
    )

    current_sed_lines = [
        _make_sed_line(
            race_key="06230102",
            horse_number=number,
            registration_id=current_registrations[number - 1],
            result_date="20230101",
            horse_name=current_names[number - 1],
            distance_m=2000,
            finish_position=number,
        )
        for number in range(1, 9)
    ]
    same_day_line = _make_sed_line(
        race_key="06230101",
        horse_number=1,
        registration_id=current_registrations[0],
        result_date="20230101",
        horse_name=current_names[0],
        distance_m=1800,
        finish_position=1,
    )
    extra_current_rows: list[bytes] = []
    if duplicate_current_sed:
        extra_current_rows.append(current_sed_lines[0])
    if conflicting_current_sed:
        extra_current_rows.append(
            _make_sed_line(
                race_key="06230102",
                horse_number=1,
                registration_id=current_registrations[0],
                result_date="20230101",
                horse_name=current_names[0],
                distance_m=2000,
                finish_position=8,
            )
        )
    _write_lines(
        raw_root / "SED_2023" / "SED230101.txt",
        [same_day_line, *current_sed_lines, *extra_current_rows],
    )
    _write_lines(
        raw_root / "BAC_2023" / "BAC230101.txt",
        [
            _make_bac_line(race_key="06230101", race_date="20230101", distance_m=1800),
            _make_bac_line(race_key="06230102", race_date="20230101", distance_m=2000),
        ],
    )

    kyi_registrations = list(current_registrations)
    if mismatch_first_identity:
        kyi_registrations[0] = "99999999"
    _write_lines(
        raw_root / "KYI_2023" / "KYI230101.txt",
        [
            _make_kyi_line(
                race_key="06230102",
                horse_number=number,
                registration_id=kyi_registrations[number - 1],
                horse_name=current_names[number - 1],
            )
            for number in range(1, 9)
        ],
    )


def test_source_audit_builds_strictly_prior_history_surface(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _build_fixture(raw_root)

    result = build_source_audit(
        raw_root=raw_root,
        history_years=(2022, 2023),
        evaluation_years=(2023,),
    )

    assert result.summary["final_verdict"] == READY_VERDICT
    assert result.summary["leakage"] == {
        "strict_prior_rule": "prior_result_date < current_race_date",
        "strict_prior_violations": 0,
        "same_day_results_excluded": True,
    }
    assert len(result.surface_rows) == 8
    first = result.surface_rows[0]
    assert first["registration_id"] == "22000001"
    assert first["prior_start_count"] == 1
    assert first["days_since_last_start"] == 14
    assert first["last_1_finish_percentile"] == 1.0
    assert first["last_5_distance_compatibility_rate"] == 1.0
    assert first["is_place"] == 1
    assert "finish_position" not in first
    assert "registration_id" not in result.summary["model_feature_columns"]
    assert "label_place_slots" not in result.summary["model_feature_columns"]


def test_source_audit_blocks_identity_mismatch(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _build_fixture(raw_root, mismatch_first_identity=True)

    result = build_source_audit(
        raw_root=raw_root,
        history_years=(2022, 2023),
        evaluation_years=(2023,),
    )

    assert result.summary["final_verdict"] == BLOCKED_VERDICT
    identity = result.summary["identity"]
    assert isinstance(identity, dict)
    assert identity["exact_identity_match_rate"] == 7 / 8
    assert any("identity match rate" in blocker for blocker in result.summary["blockers"])


def test_source_audit_deduplicates_exact_backup_rows(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _build_fixture(raw_root, duplicate_current_sed=True)

    result = build_source_audit(
        raw_root=raw_root,
        history_years=(2022, 2023),
        evaluation_years=(2023,),
    )

    assert result.summary["final_verdict"] == READY_VERDICT
    identity = result.summary["identity"]
    assert isinstance(identity, dict)
    assert identity["sed_exact_duplicate_rows_removed"] == 1
    assert len(result.surface_rows) == 8


def test_source_audit_blocks_conflicting_duplicate_rows(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    _build_fixture(raw_root, conflicting_current_sed=True)

    result = build_source_audit(
        raw_root=raw_root,
        history_years=(2022, 2023),
        evaluation_years=(2023,),
    )

    assert result.summary["final_verdict"] == BLOCKED_VERDICT
    identity = result.summary["identity"]
    assert isinstance(identity, dict)
    assert identity["sed_conflicting_duplicate_keys"] == 1
    assert any("conflicting duplicate" in blocker for blocker in result.summary["blockers"])


def test_source_audit_writes_generated_reports(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    output_dir = tmp_path / "reports"
    _build_fixture(raw_root)
    result = build_source_audit(
        raw_root=raw_root,
        history_years=(2022, 2023),
        evaluation_years=(2023,),
    )

    write_source_audit(result, output_dir)

    assert (output_dir / "phase650h_source_summary.json").is_file()
    assert (output_dir / "phase650h_year_coverage.csv").is_file()
    surface = (output_dir / "phase650h_history_surface.csv").read_text(encoding="utf-8")
    assert "prior_start_count" in surface
    assert "finish_position" not in surface.splitlines()[0]
    findings = (output_dir / "phase650h_findings.md").read_text(encoding="utf-8")
    assert READY_VERDICT in findings
