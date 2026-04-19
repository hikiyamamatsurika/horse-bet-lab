from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from horse_bet_lab.ingest.specs import (
    CONFIRMED,
    SUPPORTED_FILE_SPECS,
    ColumnSpec,
    FileSpec,
    get_file_spec,
)
from horse_bet_lab.ingest.transforms import decode_text, decode_text_lossy

DEFAULT_RAW_DIR = Path("data/raw/jrdb")
DEFAULT_DUCKDB_PATH = Path("data/artifacts/jrdb.duckdb")


@dataclass(frozen=True)
class FileIngestionResult:
    file_path: str
    file_kind: str
    row_count: int


@dataclass(frozen=True)
class IngestionSummary:
    run_id: int
    ingested_files: tuple[FileIngestionResult, ...]


def ingest_jrdb_directory(
    raw_dir: Path = DEFAULT_RAW_DIR,
    duckdb_path: Path = DEFAULT_DUCKDB_PATH,
) -> IngestionSummary:
    raw_dir.mkdir(parents=True, exist_ok=True)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(duckdb_path))
    try:
        initialize_database(connection)
        run_id = start_ingestion_run(connection, raw_dir)
        results = tuple(process_all_files(connection, run_id, raw_dir))
        finish_ingestion_run(connection, run_id, "completed")
        return IngestionSummary(run_id=run_id, ingested_files=results)
    except Exception:
        if "run_id" in locals():
            finish_ingestion_run(connection, run_id, "failed")
        raise
    finally:
        connection.close()


def initialize_database(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS ingestion_id_seq START 1
        """,
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id BIGINT PRIMARY KEY DEFAULT nextval('ingestion_id_seq'),
            raw_dir VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            status VARCHAR NOT NULL
        )
        """,
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ingested_files (
            ingested_file_id BIGINT PRIMARY KEY DEFAULT nextval('ingestion_id_seq'),
            run_id BIGINT NOT NULL,
            file_path VARCHAR NOT NULL,
            file_kind VARCHAR NOT NULL,
            file_hash VARCHAR NOT NULL,
            row_count INTEGER NOT NULL,
            status VARCHAR NOT NULL,
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            FOREIGN KEY(run_id) REFERENCES ingestion_runs(run_id)
        )
        """,
    )

    for spec in SUPPORTED_FILE_SPECS:
        create_staging_table(connection, spec)


def create_staging_table(connection: duckdb.DuckDBPyConnection, spec: FileSpec) -> None:
    columns = ",\n".join(f"{column.name} {column.duckdb_type}" for column in spec.columns)
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {spec.table_name} (
            {columns},
            source_file_path VARCHAR NOT NULL,
            source_file_hash VARCHAR NOT NULL,
            ingestion_run_id BIGINT NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        )
        """,
    )
    ensure_staging_table_columns(connection, spec)


def ensure_staging_table_columns(
    connection: duckdb.DuckDBPyConnection,
    spec: FileSpec,
) -> None:
    existing_columns = {
        str(row[0])
        for row in connection.execute(f"DESCRIBE {spec.table_name}").fetchall()
    }
    for column in spec.columns:
        if column.name in existing_columns:
            continue
        connection.execute(
            f"ALTER TABLE {spec.table_name} ADD COLUMN {column.name} {column.duckdb_type}",
        )


def start_ingestion_run(connection: duckdb.DuckDBPyConnection, raw_dir: Path) -> int:
    row = connection.execute(
        """
        INSERT INTO ingestion_runs (raw_dir, started_at, status)
        VALUES (?, ?, ?)
        RETURNING run_id
        """,
        [str(raw_dir), utc_now(), "running"],
    ).fetchone()
    if row is None:
        raise RuntimeError("failed to create ingestion run")
    return int(row[0])


def finish_ingestion_run(
    connection: duckdb.DuckDBPyConnection,
    run_id: int,
    status: str,
) -> None:
    connection.execute(
        """
        UPDATE ingestion_runs
        SET completed_at = ?, status = ?
        WHERE run_id = ?
        """,
        [utc_now(), status, run_id],
    )


def process_all_files(
    connection: duckdb.DuckDBPyConnection,
    run_id: int,
    raw_dir: Path,
) -> list[FileIngestionResult]:
    results: list[FileIngestionResult] = []
    for path in sorted(raw_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.name.startswith("."):
            continue
        spec = get_file_spec(path)
        if spec is None:
            continue
        results.append(process_file(connection, run_id, path, spec))
    return results


def process_file(
    connection: duckdb.DuckDBPyConnection,
    run_id: int,
    path: Path,
    spec: FileSpec,
) -> FileIngestionResult:
    started_at = utc_now()
    file_hash = sha256_digest(path)
    ingested_at = utc_now()
    rows = parse_file_rows(path, spec)

    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(
            f"DELETE FROM {spec.table_name} WHERE source_file_path = ?",
            [str(path)],
        )
        insert_staging_rows(connection, spec, rows, path, file_hash, run_id, ingested_at)
        connection.execute(
            """
            INSERT INTO ingested_files (
                run_id, file_path, file_kind, file_hash, row_count, status, started_at, completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                str(path),
                spec.file_kind,
                file_hash,
                len(rows),
                "completed",
                started_at,
                utc_now(),
            ],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise

    return FileIngestionResult(file_path=str(path), file_kind=spec.file_kind, row_count=len(rows))


def insert_staging_rows(
    connection: duckdb.DuckDBPyConnection,
    spec: FileSpec,
    rows: list[tuple[object, ...]],
    path: Path,
    file_hash: str,
    run_id: int,
    ingested_at: datetime,
) -> None:
    if not rows:
        return

    metadata_values = (str(path), file_hash, run_id, ingested_at)
    payload = [row + metadata_values for row in rows]
    placeholders = ", ".join(["?"] * (len(spec.columns) + 4))
    column_names = ", ".join(column.name for column in spec.columns)
    connection.executemany(
        f"""
        INSERT INTO {spec.table_name} (
            {column_names}, source_file_path, source_file_hash, ingestion_run_id, ingested_at
        )
        VALUES ({placeholders})
        """,
        payload,
    )


def parse_file_rows(path: Path, spec: FileSpec) -> list[tuple[object, ...]]:
    if spec.file_kind == "OZ":
        return parse_oz_rows(path)

    rows: list[tuple[object, ...]] = []
    for line_number, raw_line in enumerate(path.read_bytes().splitlines(), start=1):
        if not raw_line.strip():
            continue
        if len(raw_line) != spec.record_bytes:
            raise ValueError(
                (
                    f"{path} line {line_number}: expected {spec.record_bytes} "
                    f"bytes, got {len(raw_line)}"
                ),
            )
        rows.append(
            tuple(
                parse_column_value(raw_line, column)
                for column in spec.columns
            ),
        )
    return rows


def parse_oz_rows(path: Path) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for line_number, raw_line in enumerate(path.read_bytes().splitlines(), start=1):
        if not raw_line.strip():
            continue
        text = decode_text(raw_line)
        race_key = text[0:8].strip()
        headcount = int(text[8:10].strip())
        odds_values = re.findall(r"\d+\.\d", text[10:])
        expected_count = headcount * 2
        if len(odds_values) < expected_count:
            raise ValueError(
                (
                    f"{path} line {line_number}: expected at least {expected_count} "
                    f"OZ odds values, got {len(odds_values)}"
                ),
            )
        win_basis_odds = odds_values[:headcount]
        place_basis_odds = odds_values[headcount:expected_count]
        rows.extend(
            (
                race_key,
                horse_number,
                headcount,
                float(win_basis_odds[horse_number - 1]),
                float(place_basis_odds[horse_number - 1]),
            )
            for horse_number in range(1, headcount + 1)
        )
    return rows


def parse_column_value(raw_line: bytes, column: ColumnSpec) -> object:
    raw_value = raw_line[column.byte_start : column.byte_end]
    try:
        return column.converter(decode_text(raw_value))
    except Exception:
        if column.contract_status == CONFIRMED:
            raise
        fallback_text = decode_text_lossy(raw_value)
        try:
            return column.converter(fallback_text)
        except Exception:
            return None


def sha256_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
