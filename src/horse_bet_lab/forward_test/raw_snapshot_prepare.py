from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path


KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE = "place_forward_contract_like_csv_v1"


@dataclass(frozen=True)
class RawSnapshotPreparationPreset:
    name: str
    required_source_columns: tuple[str, ...]
    source_to_output_columns: tuple[tuple[str, str], ...]
    optional_passthrough_columns: tuple[str, ...]
    notes: str


@dataclass(frozen=True)
class RawSnapshotPreparationResult:
    preset_name: str
    input_path: Path
    output_path: Path
    row_count: int
    output_columns: tuple[str, ...]


PLACE_FORWARD_CONTRACT_LIKE_PRESET = RawSnapshotPreparationPreset(
    name=KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE,
    required_source_columns=(
        "race_key",
        "horse_number",
        "win_odds",
        "place_basis_odds",
        "odds_observation_timestamp",
        "input_source_name",
        "input_source_url",
        "input_source_timestamp",
        "carrier_identity",
        "snapshot_status",
        "retry_count",
    ),
    source_to_output_columns=(
        ("race_key", "race_key"),
        ("horse_number", "horse_number"),
        ("win_odds", "win_odds"),
        ("place_basis_odds", "place_basis_odds_proxy"),
        ("popularity", "popularity"),
        ("odds_observation_timestamp", "odds_observation_timestamp"),
        ("input_source_name", "input_source_name"),
        ("input_source_url", "input_source_url"),
        ("input_source_timestamp", "input_source_timestamp"),
        ("carrier_identity", "carrier_identity"),
        ("snapshot_status", "snapshot_status"),
        ("retry_count", "retry_count"),
        ("timeout_seconds", "timeout_seconds"),
        ("snapshot_failure_reason", "snapshot_failure_reason"),
        ("popularity_input_source", "popularity_input_source"),
    ),
    optional_passthrough_columns=(
        "popularity_contract_status",
        "input_schema_version",
    ),
    notes=(
        "Prepare bridge-ready raw-ish CSV from the known place-forward contract-like source "
        "family by explicitly renaming place_basis_odds to place_basis_odds_proxy."
    ),
)


PRESETS = {
    KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE: PLACE_FORWARD_CONTRACT_LIKE_PRESET,
}


def build_raw_snapshot_prepare_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare bridge-ready raw-ish snapshot CSV from a known local source-family preset.",
    )
    parser.add_argument(
        "--preset",
        required=True,
        choices=sorted(PRESETS),
        help="Source-family preset to use for explicit column mapping.",
    )
    parser.add_argument(
        "--input-path",
        type=Path,
        required=True,
        help="Input CSV path from the known local source family.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        required=True,
        help="Output raw-ish CSV path, usually data/forward_test/runs/<unit_id>/raw/input_snapshot_raw.csv.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow overwriting an existing output CSV.",
    )
    return parser


def run_raw_snapshot_prepare(
    *,
    preset_name: str,
    input_path: Path,
    output_path: Path,
    force: bool,
) -> RawSnapshotPreparationResult:
    preset = PRESETS[preset_name]
    if not input_path.exists():
        raise FileNotFoundError(f"raw-ish input preparation source CSV does not exist: {input_path}")
    if output_path.exists() and not force:
        raise FileExistsError(
            f"raw-ish input preparation refuses to overwrite existing output without --force: {output_path}"
        )

    with input_path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError(f"raw-ish input preparation source CSV is missing a header row: {input_path}")
        fieldnames = tuple(reader.fieldnames)
        missing_columns = [
            column for column in preset.required_source_columns if column not in fieldnames
        ]
        if missing_columns:
            raise ValueError(
                "raw-ish input preparation source CSV is missing required preset columns: "
                f"{missing_columns}"
            )

        output_columns = tuple(
            output_name
            for source_name, output_name in preset.source_to_output_columns
            if source_name in fieldnames
        ) + tuple(
            column for column in preset.optional_passthrough_columns if column in fieldnames
        )

        rows: list[dict[str, str]] = []
        for row_index, row in enumerate(reader, start=1):
            prepared_row: dict[str, str] = {}
            for source_name, output_name in preset.source_to_output_columns:
                if source_name not in fieldnames:
                    continue
                value = row.get(source_name, "")
                if source_name in preset.required_source_columns and (value is None or value == ""):
                    raise ValueError(
                        "raw-ish input preparation source CSV has an empty required value: "
                        f"row={row_index} column={source_name!r}"
                    )
                prepared_row[output_name] = value or ""
            for column in preset.optional_passthrough_columns:
                if column in fieldnames:
                    prepared_row[column] = row.get(column, "") or ""
            rows.append(prepared_row)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(output_columns))
        writer.writeheader()
        writer.writerows(rows)

    return RawSnapshotPreparationResult(
        preset_name=preset.name,
        input_path=input_path,
        output_path=output_path,
        row_count=len(rows),
        output_columns=output_columns,
    )


def main() -> int:
    parser = build_raw_snapshot_prepare_parser()
    args = parser.parse_args()
    run_raw_snapshot_prepare(
        preset_name=str(args.preset),
        input_path=Path(args.input_path),
        output_path=Path(args.output_path),
        force=bool(args.force),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
