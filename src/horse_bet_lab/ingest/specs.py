from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from horse_bet_lab.ingest.transforms import (
    to_bac_entry_count,
    to_bac_race_name,
    to_date,
    to_float,
    to_int,
    to_text,
)

Converter = Callable[[str], Any]
CONFIRMED = "confirmed"
PROVISIONAL = "provisional"
OPAQUE = "opaque"


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    duckdb_type: str
    byte_start: int
    byte_end: int
    converter: Converter
    contract_status: str
    dataset_allowed: bool
    notes: str


@dataclass(frozen=True)
class FileSpec:
    file_kind: str
    table_name: str
    record_bytes: int
    grain: str
    primary_key_candidates: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]

    def matches(self, path: Path) -> bool:
        return path.name.upper().startswith(self.file_kind)


SUPPORTED_FILE_SPECS: tuple[FileSpec, ...] = (
    FileSpec(
        file_kind="BAC",
        table_name="jrdb_bac_staging",
        record_bytes=182,
        grain="one row per race",
        primary_key_candidates=("race_key",),
        columns=(
            ColumnSpec("race_key", "VARCHAR", 0, 8, to_text, CONFIRMED, True, "Race-level key."),
            ColumnSpec(
                "race_date",
                "DATE",
                8,
                16,
                to_date,
                CONFIRMED,
                True,
                "Race date parsed from fixed-width bytes.",
            ),
            ColumnSpec(
                "post_time",
                "VARCHAR",
                16,
                20,
                to_text,
                PROVISIONAL,
                True,
                "Appears to be HHMM post time, not yet cross-checked with official spec.",
            ),
            ColumnSpec(
                "distance_m",
                "INTEGER",
                20,
                24,
                to_int,
                CONFIRMED,
                True,
                "Distance in meters in sample records.",
            ),
            ColumnSpec(
                "race_conditions_code",
                "VARCHAR",
                24,
                32,
                to_text,
                PROVISIONAL,
                False,
                "Condition-related code block, position confirmed but semantics incomplete.",
            ),
            ColumnSpec(
                "race_class_code",
                "VARCHAR",
                32,
                36,
                to_text,
                PROVISIONAL,
                False,
                "Class-related code block, position confirmed but semantics incomplete.",
            ),
            ColumnSpec(
                "entry_count",
                "INTEGER",
                92,
                96,
                to_bac_entry_count,
                PROVISIONAL,
                True,
                "Field size digits extracted from the compact BAC count block.",
            ),
            ColumnSpec(
                "race_name",
                "VARCHAR",
                106,
                124,
                to_bac_race_name,
                CONFIRMED,
                True,
                "Race name slice aligned to 2025 samples and stripped of trailing ASCII noise.",
            ),
            ColumnSpec(
                "odds_block",
                "VARCHAR",
                124,
                160,
                to_text,
                OPAQUE,
                False,
                "Opaque numeric block retained for traceability only.",
            ),
            ColumnSpec(
                "flags_block",
                "VARCHAR",
                160,
                182,
                to_text,
                OPAQUE,
                False,
                "Opaque trailing flags block retained for traceability only.",
            ),
        ),
    ),
    FileSpec(
        file_kind="CHA",
        table_name="jrdb_cha_staging",
        record_bytes=62,
        grain="one row per race_key and horse_number",
        primary_key_candidates=("race_key", "horse_number"),
        columns=(
            ColumnSpec(
                "race_key",
                "VARCHAR",
                0,
                8,
                to_text,
                CONFIRMED,
                True,
                "Race-level key shared with race tables.",
            ),
            ColumnSpec(
                "horse_number",
                "INTEGER",
                8,
                10,
                to_int,
                CONFIRMED,
                True,
                "Horse number within race.",
            ),
            ColumnSpec(
                "workout_weekday",
                "VARCHAR",
                10,
                12,
                to_text,
                CONFIRMED,
                True,
                "Japanese weekday character seen in sample data.",
            ),
            ColumnSpec(
                "workout_date",
                "DATE",
                12,
                20,
                to_date,
                CONFIRMED,
                True,
                "Workout date parsed from fixed-width bytes.",
            ),
            ColumnSpec(
                "workout_code",
                "VARCHAR",
                20,
                24,
                to_text,
                PROVISIONAL,
                True,
                "Stable positional code, semantics not fully decoded.",
            ),
            ColumnSpec(
                "workout_time_block",
                "VARCHAR",
                24,
                40,
                to_text,
                OPAQUE,
                False,
                "Opaque time-related block retained for traceability only.",
            ),
            ColumnSpec(
                "workout_metrics_block",
                "VARCHAR",
                40,
                52,
                to_text,
                OPAQUE,
                False,
                "Opaque metrics block retained for traceability only.",
            ),
            ColumnSpec(
                "workout_comment_code",
                "VARCHAR",
                52,
                62,
                to_text,
                PROVISIONAL,
                False,
                "Comment/status code block, not yet safe for dataset use.",
            ),
        ),
    ),
    FileSpec(
        file_kind="SED",
        table_name="jrdb_sed_staging",
        record_bytes=374,
        grain="one row per race_key and horse_number",
        primary_key_candidates=("race_key", "horse_number"),
        columns=(
            ColumnSpec(
                "race_key",
                "VARCHAR",
                0,
                8,
                to_text,
                CONFIRMED,
                False,
                "Race-level key for joining to race and horse staging.",
            ),
            ColumnSpec(
                "horse_number",
                "INTEGER",
                8,
                10,
                to_int,
                CONFIRMED,
                False,
                "Horse number within race for result join.",
            ),
            ColumnSpec(
                "registration_id",
                "VARCHAR",
                10,
                18,
                to_text,
                CONFIRMED,
                False,
                "Horse registration id from official SED spec.",
            ),
            ColumnSpec(
                "result_date",
                "DATE",
                18,
                26,
                to_date,
                CONFIRMED,
                False,
                "Race date in result file.",
            ),
            ColumnSpec(
                "horse_name",
                "VARCHAR",
                26,
                62,
                to_text,
                CONFIRMED,
                False,
                "Horse name from result file.",
            ),
            ColumnSpec(
                "distance_m",
                "INTEGER",
                62,
                66,
                to_int,
                PROVISIONAL,
                False,
                "Distance appears stable in spec but is redundant with BAC.",
            ),
            ColumnSpec(
                "finish_position",
                "INTEGER",
                140,
                142,
                to_int,
                CONFIRMED,
                False,
                "Official finish position for target generation.",
            ),
            ColumnSpec(
                "win_odds",
                "VARCHAR",
                174,
                180,
                to_text,
                PROVISIONAL,
                True,
                "Odds string available for odds-only baseline feature sets.",
            ),
            ColumnSpec(
                "popularity",
                "INTEGER",
                180,
                182,
                to_int,
                PROVISIONAL,
                True,
                "Popularity rank available as an optional odds-only baseline feature.",
            ),
        ),
    ),
    FileSpec(
        file_kind="OZ",
        table_name="jrdb_oz_staging",
        record_bytes=0,
        grain="one row per race_key and horse_number",
        primary_key_candidates=("race_key", "horse_number"),
        columns=(
            ColumnSpec(
                "race_key",
                "VARCHAR",
                0,
                0,
                to_text,
                CONFIRMED,
                False,
                "Race-level key expanded from OZ race-level odds arrays.",
            ),
            ColumnSpec(
                "horse_number",
                "INTEGER",
                0,
                0,
                to_int,
                CONFIRMED,
                False,
                (
                    "Horse number inferred from OZ array position; "
                    "validated sequentially on 2025 data."
                ),
            ),
            ColumnSpec(
                "headcount",
                "INTEGER",
                0,
                0,
                to_int,
                PROVISIONAL,
                False,
                "Declared runner count from OZ header bytes 8:10.",
            ),
            ColumnSpec(
                "win_basis_odds",
                "DOUBLE",
                0,
                0,
                to_float,
                PROVISIONAL,
                False,
                "First OZ odds block; aligns with pre-race win market proxy.",
            ),
            ColumnSpec(
                "place_basis_odds",
                "DOUBLE",
                0,
                0,
                to_float,
                PROVISIONAL,
                True,
                "Second OZ odds block; used as a minimal pre-race place-market proxy.",
            ),
        ),
    ),
    FileSpec(
        file_kind="HJC",
        table_name="jrdb_hjc_staging",
        record_bytes=442,
        grain="one row per race",
        primary_key_candidates=("race_key",),
        columns=(
            ColumnSpec(
                "race_key",
                "VARCHAR",
                0,
                8,
                to_text,
                CONFIRMED,
                False,
                "Race-level key for joining to predictions and result staging.",
            ),
            ColumnSpec(
                "win_horse_number",
                "INTEGER",
                8,
                10,
                to_int,
                PROVISIONAL,
                False,
                "Winning horse number from HJC payout block.",
            ),
            ColumnSpec(
                "win_payout",
                "INTEGER",
                10,
                17,
                to_int,
                PROVISIONAL,
                False,
                "Win payout in yen-for-100 stake units.",
            ),
            ColumnSpec(
                "place_horse_number_1",
                "INTEGER",
                35,
                37,
                to_int,
                CONFIRMED,
                False,
                "First place-paying horse number from HJC payout block.",
            ),
            ColumnSpec(
                "place_payout_1",
                "INTEGER",
                37,
                44,
                to_int,
                CONFIRMED,
                False,
                "First place payout in yen-for-100 stake units.",
            ),
            ColumnSpec(
                "place_horse_number_2",
                "INTEGER",
                44,
                46,
                to_int,
                CONFIRMED,
                False,
                "Second place-paying horse number from HJC payout block.",
            ),
            ColumnSpec(
                "place_payout_2",
                "INTEGER",
                46,
                53,
                to_int,
                CONFIRMED,
                False,
                "Second place payout in yen-for-100 stake units.",
            ),
            ColumnSpec(
                "place_horse_number_3",
                "INTEGER",
                53,
                55,
                to_int,
                CONFIRMED,
                False,
                "Third place-paying horse number from HJC payout block.",
            ),
            ColumnSpec(
                "place_payout_3",
                "INTEGER",
                55,
                62,
                to_int,
                CONFIRMED,
                False,
                "Third place payout in yen-for-100 stake units.",
            ),
        ),
    ),
)


def get_file_spec(path: Path) -> FileSpec | None:
    for spec in SUPPORTED_FILE_SPECS:
        if spec.matches(path):
            return spec
    return None


def dataset_allowlist(file_kind: str) -> tuple[str, ...]:
    for spec in SUPPORTED_FILE_SPECS:
        if spec.file_kind == file_kind:
            return tuple(column.name for column in spec.columns if column.dataset_allowed)
    raise KeyError(file_kind)
