from __future__ import annotations

import json
from pathlib import Path

import duckdb

from horse_bet_lab.config import load_dataset_build_config
from horse_bet_lab.dataset.service import (
    METADATA_COLUMNS,
    TARGET_COLUMNS,
    TARGET_SOURCE_COLUMNS,
    build_horse_dataset,
    processed_columns,
)
from horse_bet_lab.ingest.service import (
    create_pre_race_market_tables,
    create_win_market_snapshot_view,
    ingest_jrdb_directory,
    refresh_pre_race_market_staging,
)

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


def test_build_horse_dataset_creates_expected_columns_and_grain(tmp_path: Path) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    config_path = tmp_path / "dataset.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_minimal'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'minimal'\n"
            "target_name = 'workout_gap_days'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{tmp_path / 'data' / 'processed' / 'dataset.parquet'}'\n"
        ),
        encoding="utf-8",
    )

    config = load_dataset_build_config(config_path)
    summary = build_horse_dataset(config)

    assert summary.row_count == 2
    provenance_path = summary.output_path.with_name(f"{summary.output_path.name}.provenance.json")
    assert provenance_path.exists()

    connection = duckdb.connect()
    try:
        columns = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [str(summary.output_path)],
        ).fetchall()
        assert tuple(column[0] for column in columns) == processed_columns(config)

        rows = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                CAST(race_date AS VARCHAR),
                split,
                target_name,
                target_value
            FROM read_parquet(?)
            ORDER BY race_key, horse_number
            """,
            [str(summary.output_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, "2025-02-23", "train", "workout_gap_days", 4),
            ("05251801", 2, "2025-02-23", "train", "workout_gap_days", 4),
        ]
    finally:
        connection.close()

    payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    assert payload["feature_contract_version"] == "v1"
    assert payload["dataset_feature_set"] == "minimal"
    assert payload["dataset_feature_columns"] == [
        "distance_m",
        "race_name",
        "workout_weekday",
        "workout_date",
    ]


def test_build_horse_dataset_applies_period_filter(tmp_path: Path) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    config_path = tmp_path / "dataset.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_minimal'\n"
            "start_date = '2025-03-01'\n"
            "end_date = '2025-03-31'\n"
            "feature_set = 'minimal'\n"
            "target_name = 'workout_gap_days'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{tmp_path / 'data' / 'processed' / 'dataset_filtered.parquet'}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 0


def test_build_horse_dataset_supports_market_plus_workout_minimal(tmp_path: Path) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    result_path = tmp_path / "data" / "processed" / "dataset_market_workout.parquet"
    prepare_result_staging_database(duckdb_path)
    prepare_oz_staging_database(duckdb_path)

    config_path = tmp_path / "dataset_market_workout.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_market_plus_workout_minimal'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "train_end_date = '2025-02-20'\n"
            "valid_end_date = '2025-02-25'\n"
            "feature_set = 'market_plus_workout_minimal'\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{result_path}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                split,
                win_odds,
                popularity,
                workout_gap_days,
                workout_weekday_code,
                target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(result_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, "valid", 11.2, 2, 4, 2, 1),
            ("05251801", 2, "valid", 40.0, 8, 4, 2, 0),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_uses_win_market_snapshot_for_win_odds_and_legacy_sed_for_popularity(
    tmp_path: Path,
) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    prepare_result_staging_database(duckdb_path)
    prepare_oz_staging_database(duckdb_path)
    result_path = tmp_path / "data" / "processed" / "dataset_win_market_contract.parquet"

    config_path = tmp_path / "dataset_win_market_contract.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_win_market_contract'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'win_market_only'\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{result_path}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                win_odds,
                popularity,
                target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(result_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, 11.2, 2, 1),
            ("05251801", 2, 40.0, 8, 0),
        ]
    finally:
        connection.close()

    payload = json.loads(
        result_path.with_name(f"{result_path.name}.provenance.json").read_text(encoding="utf-8"),
    )
    definitions = {row["canonical_name"]: row for row in payload["feature_definitions"]}
    assert definitions["win_odds"]["carrier_identity"] == "win_market_snapshot_v1"
    assert definitions["popularity"]["carrier_identity"] == "legacy_sed_only_non_mainline"
    assert payload["feature_source_summary"]["by_carrier_identity"] == {
        "legacy_sed_only_non_mainline": 1,
        "win_market_snapshot_v1": 1,
    }


def test_build_horse_dataset_supports_dual_market_features(tmp_path: Path) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    prepare_result_staging_database(duckdb_path)
    prepare_oz_staging_database(duckdb_path)
    result_path = tmp_path / "data" / "processed" / "dataset_dual_market.parquet"

    config_path = tmp_path / "dataset_dual_market.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_dual_market'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'dual_market'\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{result_path}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                win_odds,
                place_basis_odds,
                popularity,
                target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(result_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, 11.2, 2.4, 2, 1),
            ("05251801", 2, 40.0, 3.1, 8, 0),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_supports_single_win_market_feature_sets(tmp_path: Path) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    prepare_result_staging_database(duckdb_path)
    prepare_oz_staging_database(duckdb_path)
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            UPDATE jrdb_sed_staging
            SET finish_position = 1
            WHERE race_key = '05251801' AND horse_number = 1
            """,
        )
    finally:
        connection.close()
    win_market_path = tmp_path / "data" / "processed" / "dataset_win_market_only.parquet"
    dual_market_path = tmp_path / "data" / "processed" / "dataset_dual_market_for_win.parquet"

    win_market_config_path = tmp_path / "dataset_win_market_only.toml"
    win_market_config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_win_market_only_is_win'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'win_market_only'\n"
            "target_name = 'is_win'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{win_market_path}'\n"
        ),
        encoding="utf-8",
    )
    dual_market_config_path = tmp_path / "dataset_dual_market_for_win.toml"
    dual_market_config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_dual_market_for_win_is_win'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'dual_market_for_win'\n"
            "target_name = 'is_win'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{dual_market_path}'\n"
        ),
        encoding="utf-8",
    )

    build_horse_dataset(load_dataset_build_config(win_market_config_path))
    build_horse_dataset(load_dataset_build_config(dual_market_config_path))

    connection = duckdb.connect()
    try:
        win_market_rows = connection.execute(
            """
            SELECT race_key, horse_number, win_odds, popularity, target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(win_market_path)],
        ).fetchall()
        assert win_market_rows == [
            ("05251801", 1, 11.2, 2, 1),
            ("05251801", 2, 40.0, 8, 0),
        ]

        dual_market_rows = connection.execute(
            """
            SELECT race_key, horse_number, win_odds, place_basis_odds, popularity, target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(dual_market_path)],
        ).fetchall()
        assert dual_market_rows == [
            ("05251801", 1, 11.2, 2.4, 2, 1),
            ("05251801", 2, 40.0, 3.1, 8, 0),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_supports_dual_market_derived_features(tmp_path: Path) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    prepare_result_staging_database(duckdb_path)
    prepare_oz_staging_database(duckdb_path)
    result_path = tmp_path / "data" / "processed" / "dataset_dual_market_derived.parquet"

    config_path = tmp_path / "dataset_dual_market_derived.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_dual_market_plus_log_diff'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'dual_market_plus_log_diff'\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{result_path}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                win_odds,
                place_basis_odds,
                popularity,
                ROUND(log_place_minus_log_win, 6),
                target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(result_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, 11.2, 2.4, 2, -1.540445, 1),
            ("05251801", 2, 40.0, 3.1, 8, -2.557477, 0),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_supports_dual_market_headcount_features(tmp_path: Path) -> None:
    duckdb_path = prepare_staging_database(tmp_path)
    prepare_result_staging_database(duckdb_path)
    prepare_oz_staging_database(duckdb_path)
    result_path = tmp_path / "data" / "processed" / "dataset_dual_market_headcount.parquet"

    config_path = tmp_path / "dataset_dual_market_headcount.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_dual_market_plus_headcount_place_slots_distance'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-02-28'\n"
            "feature_set = 'dual_market_plus_headcount_place_slots_distance'\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{result_path}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 2

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                win_odds,
                place_basis_odds,
                popularity,
                headcount,
                place_slot_count,
                distance_m,
                target_value
            FROM read_parquet(?)
            ORDER BY horse_number
            """,
            [str(result_path)],
        ).fetchall()
        assert rows == [
            ("05251801", 1, 11.2, 2.4, 2, 2, 2, 1400, 1),
            ("05251801", 2, 40.0, 3.1, 8, 2, 2, 1400, 0),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_supports_odds_only_from_sed_without_bac_cha(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                registration_id VARCHAR,
                result_date DATE,
                horse_name VARCHAR,
                distance_m INTEGER,
                finish_position INTEGER,
                win_odds VARCHAR,
                popularity INTEGER,
                source_file_path VARCHAR,
                source_file_hash VARCHAR,
                ingestion_run_id BIGINT,
                ingested_at TIMESTAMP
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                (
                    '01240101', 1, 'r1', DATE '2024-01-06', 'horse1',
                    1200, 1, '8.5', 2, 'sample', 'hash', 1, NOW()
                ),
                (
                    '01240101', 2, 'r2', DATE '2024-01-06', 'horse2',
                    1200, 5, '15.2', 5, 'sample', 'hash', 1, NOW()
                ),
                (
                    '01240201', 1, 'r3', DATE '2024-02-10', 'horse3',
                    1400, 3, '6.1', 1, 'sample', 'hash', 1, NOW()
                )
            """,
        )
        create_pre_race_market_tables(connection)
        create_win_market_snapshot_view(connection)
        connection.execute(
            """
            INSERT INTO jrdb_pre_race_market_staging VALUES
                (
                    '01240101', 1, 8.5, 'oz_win_basis_odds', DATE '2024-01-06',
                    'jrdb_oz_staging', 'win_basis_odds', 'sample', 'hash', 1, NOW()
                ),
                (
                    '01240101', 2, 15.2, 'oz_win_basis_odds', DATE '2024-01-06',
                    'jrdb_oz_staging', 'win_basis_odds', 'sample', 'hash', 1, NOW()
                ),
                (
                    '01240201', 1, 6.1, 'oz_win_basis_odds', DATE '2024-02-10',
                    'jrdb_oz_staging', 'win_basis_odds', 'sample', 'hash', 1, NOW()
                )
            """,
        )
    finally:
        connection.close()

    result_path = tmp_path / "data" / "processed" / "dataset_odds_only.parquet"
    config_path = tmp_path / "dataset_odds_only.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_odds_only_2024'\n"
            "start_date = '2024-01-01'\n"
            "end_date = '2024-12-31'\n"
            "train_end_date = '2024-01-31'\n"
            "valid_end_date = '2024-06-30'\n"
            "feature_set = 'odds_only'\n"
            "include_popularity = true\n"
            "target_name = 'is_place'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{result_path}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 3

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                race_key,
                horse_number,
                CAST(race_date AS VARCHAR),
                split,
                win_odds,
                popularity,
                target_value
            FROM read_parquet(?)
            ORDER BY race_key, horse_number
            """,
            [str(result_path)],
        ).fetchall()
        assert rows == [
            ("01240101", 1, "2024-01-06", "train", 8.5, 2, 1),
            ("01240101", 2, "2024-01-06", "train", 15.2, 5, 0),
            ("01240201", 1, "2024-02-10", "valid", 6.1, 1, 1),
        ]
    finally:
        connection.close()


def test_build_horse_dataset_assigns_time_series_splits_and_validates_leakage(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(
        b"\r\n".join(
            [
                BAC_SAMPLE_LINE_1,
                mutate_ascii(BAC_SAMPLE_LINE_1, ((0, 8, "05251901"), (8, 16, "20250323"))),
                mutate_ascii(BAC_SAMPLE_LINE_1, ((0, 8, "05252001"), (8, 16, "20250423"))),
            ],
        )
        + b"\r\n",
    )
    (raw_dir / "CHA_sample.txt").write_bytes(
        b"\r\n".join(
            [
                CHA_SAMPLE_LINE_1,
                mutate_ascii(CHA_SAMPLE_LINE_1, ((0, 8, "05251901"), (12, 20, "20250319"))),
                mutate_ascii(CHA_SAMPLE_LINE_1, ((0, 8, "05252001"), (12, 20, "20250419"))),
            ],
        )
        + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"
    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)

    config_path = tmp_path / "dataset_split.toml"
    config_path.write_text(
        (
            "[dataset]\n"
            "name = 'horse_dataset_split'\n"
            "start_date = '2025-02-01'\n"
            "end_date = '2025-04-30'\n"
            "train_end_date = '2025-02-28'\n"
            "valid_end_date = '2025-03-31'\n"
            "feature_set = 'minimal'\n"
            "target_name = 'workout_gap_days'\n"
            f"duckdb_path = '{duckdb_path}'\n"
            f"output_path = '{tmp_path / 'data' / 'processed' / 'dataset_split.parquet'}'\n"
        ),
        encoding="utf-8",
    )

    summary = build_horse_dataset(load_dataset_build_config(config_path))

    assert summary.row_count == 3

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT race_key, CAST(race_date AS VARCHAR), split
            FROM read_parquet(?)
            ORDER BY race_key
            """,
            [str(summary.output_path)],
        ).fetchall()
        assert rows == [
            ("05251801", "2025-02-23", "train"),
            ("05251901", "2025-03-23", "valid"),
            ("05252001", "2025-04-23", "test"),
        ]

        assert connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT race_key
                FROM read_parquet(?)
                GROUP BY race_key
                HAVING COUNT(DISTINCT split) > 1
            )
            """,
            [str(summary.output_path)],
        ).fetchone() == (0,)

        assert connection.execute(
            """
            SELECT CAST(MAX(race_date) AS VARCHAR)
            FROM read_parquet(?)
            WHERE split = 'train'
            """,
            [str(summary.output_path)],
        ).fetchone() == ("2025-02-23",)
        assert connection.execute(
            """
            SELECT CAST(MIN(race_date) AS VARCHAR), CAST(MAX(race_date) AS VARCHAR)
            FROM read_parquet(?)
            WHERE split = 'valid'
            """,
            [str(summary.output_path)],
        ).fetchone() == ("2025-03-23", "2025-03-23")
        assert connection.execute(
            """
            SELECT CAST(MIN(race_date) AS VARCHAR)
            FROM read_parquet(?)
            WHERE split = 'test'
            """,
            [str(summary.output_path)],
        ).fetchone() == ("2025-04-23",)

        columns = {
            column[0]
            for column in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)",
                [str(summary.output_path)],
            ).fetchall()
        }
        assert set(METADATA_COLUMNS).issubset(columns)
        assert {"distance_m", "race_name", "workout_weekday", "workout_date"}.issubset(columns)
        assert set(TARGET_COLUMNS).issubset(columns)
        assert not (set(TARGET_SOURCE_COLUMNS) & columns)
    finally:
        connection.close()


def prepare_staging_database(tmp_path: Path) -> Path:
    raw_dir = tmp_path / "data" / "raw" / "jrdb"
    raw_dir.mkdir(parents=True)
    (raw_dir / "BAC_sample.txt").write_bytes(
        BAC_SAMPLE_LINE_1 + b"\r\n" + BAC_SAMPLE_LINE_2 + b"\r\n",
    )
    (raw_dir / "CHA_sample.txt").write_bytes(
        CHA_SAMPLE_LINE_1 + b"\r\n" + CHA_SAMPLE_LINE_2 + b"\r\n",
    )
    duckdb_path = tmp_path / "data" / "artifacts" / "jrdb.duckdb"
    ingest_jrdb_directory(raw_dir=raw_dir, duckdb_path=duckdb_path)
    return duckdb_path


def prepare_result_staging_database(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            DELETE FROM jrdb_sed_staging
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                (
                    '05251801', 1, 'r1', DATE '2025-02-23', 'horse1', 1400, 2, '12.3', 2,
                    'sample', 'hash', 1, NOW()
                ),
                (
                    '05251801', 2, 'r2', DATE '2025-02-23', 'horse2', 1400, 8, '45.6', 8,
                    'sample', 'hash', 1, NOW()
                )
            """,
        )
    finally:
        connection.close()


def prepare_oz_staging_database(duckdb_path: Path) -> None:
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS jrdb_oz_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                headcount INTEGER,
                win_basis_odds DOUBLE,
                place_basis_odds DOUBLE,
                source_file_path VARCHAR,
                source_file_hash VARCHAR,
                ingestion_run_id BIGINT,
                ingested_at TIMESTAMP
            )
            """,
        )
        connection.execute("DELETE FROM jrdb_oz_staging")
        connection.execute(
            """
            INSERT INTO jrdb_oz_staging VALUES
                ('05251801', 1, 2, 11.2, 2.4, 'sample', 'hash', 1, NOW()),
                ('05251801', 2, 2, 40.0, 3.1, 'sample', 'hash', 1, NOW())
            """,
        )
        refresh_pre_race_market_staging(connection)
    finally:
        connection.close()


def mutate_ascii(raw_line: bytes, replacements: tuple[tuple[int, int, str], ...]) -> bytes:
    buffer = bytearray(raw_line)
    for start, end, value in replacements:
        buffer[start:end] = value.encode("ascii")
    return bytes(buffer)
