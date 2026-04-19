from __future__ import annotations

from pathlib import Path

import duckdb

from horse_bet_lab.config import load_dataset_build_config
from horse_bet_lab.dataset.service import build_horse_dataset
from horse_bet_lab.ingest.service import ingest_jrdb_directory

BAC_SAMPLE_LINE_1 = bytes.fromhex(
    "3035323531383031323032353032323331303035313430303232313132413331303233208140814081408140814081408140814081408140814081408140814081408140814081408140814081408140814081408140202020202020202031362031814081408140814082528dce96a28f9f9798814081408140814034303035363030303232303030313430303030383430303035363030343030303030303031313131313131312020202020202020202020202020",
)
BAC_SAMPLE_LINE_2 = bytes.fromhex(
    "3035323531383032323032353032323331303335323130303232313132413330303233208140814081408140814081408140814081408140814081408140814081408140814081408140814081408140814081408140202020202020202031362031814081408140814082528dce96a28f9f9798814081408140814034303035363030303232303030313430303030383430303035363030343030303030303031313131313131312020202020202020202020202020",
)
CHA_SAMPLE_LINE_1 = bytes.fromhex(
    "3035323531383031303190853230323530323139303031333032313431343531333831323920323220313620313820353420202020202020202020202020",
)
CHA_SAMPLE_LINE_2 = bytes.fromhex(
    "3035323531383031303290853230323530323139313032333032313631363231333531323020203720323020323720363232332033413320202020202020",
)


def test_ingest_sed_result_file_creates_result_staging_rows(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "SED_sample.txt").write_bytes(
        build_sed_line(race_key="05251801", horse_number=1, finish_position=1)
        + b"\r\n"
        + build_sed_line(race_key="05251801", horse_number=2, finish_position=4)
        + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"

    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    connection = duckdb.connect(str(duckdb_path))
    try:
        rows = connection.execute(
            """
            SELECT race_key, horse_number, CAST(result_date AS VARCHAR), finish_position
            FROM jrdb_sed_staging
            ORDER BY horse_number
            """,
        ).fetchall()
        assert rows == [
            ("05251801", 1, "2025-02-23", 1),
            ("05251801", 2, "2025-02-23", 4),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_with_is_place_target_joins_results(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(
        BAC_SAMPLE_LINE_1 + b"\r\n" + BAC_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "CHA_sample.txt").write_bytes(
        CHA_SAMPLE_LINE_1 + b"\r\n" + CHA_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "SED_sample.txt").write_bytes(
        build_sed_line(race_key="05251801", horse_number=1, finish_position=1)
        + b"\r\n"
        + build_sed_line(race_key="05251801", horse_number=2, finish_position=4)
        + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"
    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    config_path = tmp_path / "dataset_is_place.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_is_place'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'minimal'\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{tmp_path / 'data' / 'processed' / 'dataset_is_place.parquet'}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT race_key, horse_number, split, target_name, target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(summary.output_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, "train", "is_place", 1),
            ("05251801", 2, "train", "is_place", 0),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_with_is_win_target_joins_results(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(
        BAC_SAMPLE_LINE_1 + b"\r\n" + BAC_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "CHA_sample.txt").write_bytes(
        CHA_SAMPLE_LINE_1 + b"\r\n" + CHA_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "SED_sample.txt").write_bytes(
        build_sed_line(race_key="05251801", horse_number=1, finish_position=1)
        + b"\r\n"
        + build_sed_line(race_key="05251801", horse_number=2, finish_position=2)
        + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"
    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    config_path = tmp_path / "dataset_is_win.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_is_win'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'minimal'\n"
            "target_name = 'is_win'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{tmp_path / 'data' / 'processed' / 'dataset_is_win.parquet'}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT race_key, horse_number, split, target_name, target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(summary.output_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, "train", "is_win", 1),
            ("05251801", 2, "train", "is_win", 0),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_with_is_place_excludes_nonstandard_finish_position(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(BAC_SAMPLE_LINE_1 + b"\r\n")
    (raw_dir / "CHA_sample.txt").write_bytes(CHA_SAMPLE_LINE_1 + b"\r\n")
    (raw_dir / "SED_sample.txt").write_bytes(
        build_sed_line(race_key="05251801", horse_number=1, finish_position=0) + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"
    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    config_path = tmp_path / "dataset_is_place.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_is_place'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'minimal'\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{tmp_path / 'data' / 'processed' / 'dataset_is_place.parquet'}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 0


def test_build_horse_dataset_with_odds_only_feature_set(tmp_path: Path) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(
        BAC_SAMPLE_LINE_1 + b"\r\n" + BAC_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "CHA_sample.txt").write_bytes(
        CHA_SAMPLE_LINE_1 + b"\r\n" + CHA_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "SED_sample.txt").write_bytes(
        build_sed_line(race_key="05251801", horse_number=1, finish_position=1)
        + b"\r\n"
        + build_sed_line(race_key="05251801", horse_number=2, finish_position=4)
        + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"
    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    config_path = tmp_path / "dataset_odds_only.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_odds_only_is_place'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'odds_only'\n"
            "include_popularity = true\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{tmp_path / 'data' / 'processed' / 'dataset_odds_only.parquet'}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        columns = [
            row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(summary.output_path)],
            ).fetchall()
        ]
        assert columns == [
            "race_key",
            "horse_number",
            "race_date",
            "split",
            "target_name",
            "win_odds",
            "popularity",
            "target_value",
        ]

        rows = connection.execute(
            """
            SELECT race_key, horse_number, split, target_name, win_odds, popularity, target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(summary.output_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, "train", "is_place", 12.3, 3, 1),
            ("05251801", 2, "train", "is_place", 12.3, 3, 0),
        ]
    finally:
        connection.close()


def build_sed_line(
    *,
    race_key: str,
    horse_number: int,
    finish_position: int,
    result_date: str = "20250223",
    registration_id: str = "12345678",
    horse_name: str = "テストホース",
) -> bytes:
    line = bytearray(b" " * 374)
    write_ascii(line, 0, 8, race_key)
    write_ascii(line, 8, 10, f"{horse_number:02d}")
    write_ascii(line, 10, 18, registration_id)
    write_ascii(line, 18, 26, result_date)
    write_cp932(line, 26, 62, horse_name)
    write_ascii(line, 62, 66, "1400")
    write_ascii(line, 140, 142, f"{finish_position:02d}")
    write_ascii(line, 174, 180, "  12.3")
    write_ascii(line, 180, 182, "03")
    return bytes(line)


def write_ascii(buffer: bytearray, start: int, end: int, value: str) -> None:
    encoded = value.encode("ascii")
    padded = encoded.ljust(end - start, b" ")
    buffer[start:end] = padded[: end - start]


def write_cp932(buffer: bytearray, start: int, end: int, value: str) -> None:
    encoded = value.encode("cp932")
    padded = encoded.ljust(end - start, b" ")
    buffer[start:end] = padded[: end - start]
