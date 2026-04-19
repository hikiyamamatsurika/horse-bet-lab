from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.ingest.service import DEFAULT_DUCKDB_PATH, DEFAULT_RAW_DIR, ingest_jrdb_directory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest JRDB raw files into DuckDB staging tables.",
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR, help="JRDB raw directory.")
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=DEFAULT_DUCKDB_PATH,
        help="DuckDB database file path.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = ingest_jrdb_directory(raw_dir=args.raw_dir, duckdb_path=args.duckdb_path)
    print(f"Ingestion run completed: run_id={summary.run_id}")
    for result in summary.ingested_files:
        print(f"  {result.file_kind} {result.file_path} rows={result.row_count}")


if __name__ == "__main__":
    main()
