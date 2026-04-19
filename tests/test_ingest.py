from __future__ import annotations

from pathlib import Path

import duckdb

from horse_bet_lab.ingest.service import ingest_jrdb_directory

BAC_SAMPLE_LINE_1 = bytes.fromhex(
    "3035323531383031323032353032323331303035313430303232313132413331303233208140814081408140814081408140814081408140814081408140814081408140814081408140814081408140814081408140202020202020202031362031814081408140814082528dce96a28f9f9798814081408140814034303035363030303232303030313430303030383430303035363030343030303030303031313131313131312020202020202020202020202020",
)
BAC_SAMPLE_LINE_2 = bytes.fromhex(
    "3035323531383032323032353032323331303335323130303232313132413330303233208140814081408140814081408140814081408140814081408140814081408140814081408140814081408140814081408140202020202020202031362031814081408140814082528dce96a28f9f9798814081408140814034303035363030303232303030313430303030383430303035363030343030303030303031313131313131312020202020202020202020202020",
)
BAC_GRADE_SAMPLE_LINE = bytes.fromhex(
    "30383235313631313230323530323136313533303232303031313231344f5035303332328b9e93738b4c944f81408140814081408140814081408140814081408140814081408140814081408140814081408140814091e631313889f120313234328b9e93738b4c944f8b9e93738b4c944f8145826682518140814034303632303030323530303031363030303039333030303632303033313030303132353031313131313131312020202020202020342020202020",
)
CHA_SAMPLE_LINE_1 = bytes.fromhex(
    "3035323531383031303190853230323530323139303031333032313431343531333831323920323220313620313820353420202020202020202020202020",
)
CHA_SAMPLE_LINE_2 = bytes.fromhex(
    "3035323531383031303290853230323530323139313032333032313631363231333531323020203720323020323720363232332033413320202020202020",
)


def make_hjc_line(
    *,
    race_key: str,
    win_horse: int = 0,
    win_payout: int = 0,
    place_horses: tuple[int, int, int],
    place_payouts: tuple[int, int, int],
) -> bytes:
    chars = [" "] * 442
    chars[0:8] = list(race_key)
    chars[8:10] = list(f"{win_horse:02d}")
    chars[10:17] = list(f"{win_payout:>7d}")
    for horse, payout, horse_start, payout_start in (
        (place_horses[0], place_payouts[0], 35, 37),
        (place_horses[1], place_payouts[1], 44, 46),
        (place_horses[2], place_payouts[2], 53, 55),
    ):
        horse_text = f"{horse:02d}"
        payout_text = f"{payout:>7d}"
        chars[horse_start : horse_start + 2] = list(horse_text)
        chars[payout_start : payout_start + 7] = list(payout_text)
    return "".join(chars).encode("ascii")


def make_oz_line(
    *,
    race_key: str,
    headcount: int,
    win_basis_odds: tuple[float, ...],
    place_basis_odds: tuple[float, ...],
) -> bytes:
    win_text = "".join(f"{value:>5.1f}" for value in win_basis_odds)
    place_text = "".join(f"{value:>5.1f}" for value in place_basis_odds)
    return f"{race_key}{headcount:02d}{win_text}{' ' * 12}{place_text}".encode("ascii")


def test_ingest_jrdb_directory_creates_management_and_staging_tables(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(
        BAC_SAMPLE_LINE_1 + b"\r\n" + BAC_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "CHA_sample.txt").write_bytes(
        CHA_SAMPLE_LINE_1 + b"\r\n" + CHA_SAMPLE_LINE_2 + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    summary = ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    assert len(summary.ingested_files) == 2

    connection = duckdb.connect(str(duckdb_path))
    try:
        assert connection.execute("SELECT COUNT(*) FROM ingestion_runs").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM ingested_files").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM jrdb_bac_staging").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM jrdb_cha_staging").fetchone() == (2,)

        first_bac_row = connection.execute(
            """
            SELECT race_key, distance_m, race_name, CAST(race_date AS VARCHAR)
            FROM jrdb_bac_staging
            ORDER BY race_key
            LIMIT 1
            """,
        ).fetchone()
        assert first_bac_row == ("05251801", 1400, "３歳未勝利", "2025-02-23")

        first_cha_row = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                workout_weekday,
                CAST(workout_date AS VARCHAR),
                workout_code
            FROM jrdb_cha_staging
            ORDER BY horse_number
            LIMIT 1
            """,
        ).fetchone()
        assert first_cha_row == ("05251801", 1, "水", "2025-02-19", "0013")
    finally:
        connection.close()


def test_ingest_jrdb_directory_parses_hjc_place_payouts(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "HJC250105.txt").write_bytes(
        make_hjc_line(
            race_key="06251101",
            win_horse=11,
            win_payout=250,
            place_horses=(11, 3, 13),
            place_payouts=(120, 280, 1980),
        )
        + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    connection = duckdb.connect(str(duckdb_path))
    try:
        row = connection.execute(
            """
            SELECT
                race_key,
                win_horse_number,
                win_payout,
                place_horse_number_1,
                place_payout_1,
                place_horse_number_2,
                place_payout_2,
                place_horse_number_3,
                place_payout_3
            FROM jrdb_hjc_staging
            """,
        ).fetchone()
        assert row == ("06251101", 11, 250, 11, 120, 3, 280, 13, 1980)
    finally:
        connection.close()


def test_ingest_jrdb_directory_parses_oz_basis_odds(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "OZ250105.txt").write_bytes(
        make_oz_line(
            race_key="06251101",
            headcount=3,
            win_basis_odds=(2.4, 8.7, 15.2),
            place_basis_odds=(1.3, 2.8, 4.1),
        )
        + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    connection = duckdb.connect(str(duckdb_path))
    try:
        rows = connection.execute(
            """
            SELECT race_key, horse_number, headcount, win_basis_odds, place_basis_odds
            FROM jrdb_oz_staging
            ORDER BY horse_number
            """,
        ).fetchall()
        assert rows == [
            ("06251101", 1, 3, 2.4, 1.3),
            ("06251101", 2, 3, 8.7, 2.8),
            ("06251101", 3, 3, 15.2, 4.1),
        ]
    finally:
        connection.close()


def test_ingest_jrdb_directory_is_rerunnable_for_same_input(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(
        BAC_SAMPLE_LINE_1 + b"\r\n" + BAC_SAMPLE_LINE_2 + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)
    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    connection = duckdb.connect(str(duckdb_path))
    try:
        assert connection.execute("SELECT COUNT(*) FROM ingestion_runs").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM ingested_files").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM jrdb_bac_staging").fetchone() == (2,)
    finally:
        connection.close()


def test_ingest_jrdb_directory_recurses_into_subdirectories_and_skips_hidden_files(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    nested_dir = raw_dir / "BAC_2025"
    nested_dir.mkdir(parents=True)
    (raw_dir / ".DS_Store").write_bytes(b"ignored")
    (nested_dir / "BAC250223.txt").write_bytes(
        BAC_SAMPLE_LINE_1 + b"\r\n" + BAC_SAMPLE_LINE_2 + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    summary = ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    assert len(summary.ingested_files) == 1
    assert summary.ingested_files[0].file_path.endswith("BAC_2025/BAC250223.txt")

    connection = duckdb.connect(str(duckdb_path))
    try:
        assert connection.execute("SELECT COUNT(*) FROM jrdb_bac_staging").fetchone() == (2,)
        assert connection.execute("SELECT COUNT(*) FROM ingested_files").fetchone() == (1,)
    finally:
        connection.close()


def test_ingest_jrdb_directory_nulls_invalid_provisional_fields(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    malformed_line = bytearray(BAC_SAMPLE_LINE_1)
    malformed_line[92:96] = bytes.fromhex("f1203132")
    (raw_dir / "BAC_sample.txt").write_bytes(bytes(malformed_line) + b"\r\n")
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    connection = duckdb.connect(str(duckdb_path))
    try:
        row = connection.execute(
            """
            SELECT race_key, distance_m, entry_count
            FROM jrdb_bac_staging
            """,
        ).fetchone()
        assert row == ("05251801", 1400, 12)
    finally:
        connection.close()


def test_ingest_jrdb_directory_parses_bac_grade_race_name_and_entry_count(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC250216.txt").write_bytes(BAC_GRADE_SAMPLE_LINE + b"\r\n")
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    connection = duckdb.connect(str(duckdb_path))
    try:
        row = connection.execute(
            """
            SELECT race_key, entry_count, race_name
            FROM jrdb_bac_staging
            """,
        ).fetchone()
        assert row == ("08251611", 12, "京都記念・Ｇ２")
    finally:
        connection.close()
