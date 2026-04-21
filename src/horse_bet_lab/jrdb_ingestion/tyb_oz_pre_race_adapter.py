from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from horse_bet_lab.ingest.transforms import decode_text
from horse_bet_lab.jrdb_ingestion.oz_pre_race_adapter import OZPreRaceMarketRow, read_oz_source_rows

RAW_ISH_TYB_OZ_OUTPUT_COLUMNS = (
    "race_key",
    "horse_number",
    "win_odds",
    "place_basis_odds_proxy",
    "place_odds_min",
    "odds_index",
)


@dataclass(frozen=True)
class TYBLiveMarketRow:
    race_key: str
    horse_number: int
    win_odds: float
    place_odds_low: float
    odds_observation_time_hhmm: str
    odds_index: float | None


@dataclass(frozen=True)
class TYBOZPreRaceAdapterResult:
    tyb_source_paths: tuple[Path, ...]
    oz_source_paths: tuple[Path, ...]
    output_path: Path
    row_count: int
    race_count: int


def discover_tyb_source_paths(raw_root: Path) -> tuple[Path, ...]:
    if not raw_root.exists():
        raise FileNotFoundError(f"TYB+OZ pre-race adapter raw root does not exist: {raw_root}")
    source_paths = tuple(sorted(path for path in raw_root.rglob("TYB*.txt") if path.is_file()))
    if not source_paths:
        raise FileNotFoundError(
            f"TYB+OZ pre-race adapter found no TYB*.txt files under: {raw_root}"
        )
    return source_paths


def parse_tyb_line(
    raw_line: bytes,
    *,
    source_path: Path,
    line_number: int,
) -> TYBLiveMarketRow | None:
    if not raw_line.strip():
        return None
    if len(raw_line) < 88:
        raise ValueError(
            f"{source_path} line {line_number}: "
            f"TYB line shorter than required 88 bytes, got {len(raw_line)}"
        )

    race_key = decode_text(raw_line[0:8]).strip()
    if race_key == "":
        raise ValueError(f"{source_path} line {line_number}: missing race_key in bytes 0:8")

    horse_number_text = decode_text(raw_line[8:10]).strip()
    if horse_number_text == "":
        raise ValueError(f"{source_path} line {line_number}: missing horse_number in bytes 8:10")
    try:
        horse_number = int(horse_number_text)
    except ValueError as exc:
        raise ValueError(
            f"{source_path} line {line_number}: invalid TYB horse_number {horse_number_text!r}"
        ) from exc

    win_odds = _parse_required_float(
        decode_text(raw_line[72:78]),
        source_path=source_path,
        line_number=line_number,
        field_name="win_odds",
    )
    place_odds_low = _parse_required_float(
        decode_text(raw_line[78:84]),
        source_path=source_path,
        line_number=line_number,
        field_name="place_odds_low",
    )
    odds_observation_time_hhmm = decode_text(raw_line[84:88]).strip()
    if odds_observation_time_hhmm == "":
        raise ValueError(
            f"{source_path} line {line_number}: missing odds observation time in bytes 84:88"
        )

    odds_index_text = decode_text(raw_line[25:30]).strip()
    odds_index = float(odds_index_text) if odds_index_text != "" else None
    return TYBLiveMarketRow(
        race_key=race_key,
        horse_number=horse_number,
        win_odds=win_odds,
        place_odds_low=place_odds_low,
        odds_observation_time_hhmm=odds_observation_time_hhmm,
        odds_index=odds_index,
    )


def read_tyb_source_rows(source_paths: Iterable[Path]) -> tuple[TYBLiveMarketRow, ...]:
    rows: list[TYBLiveMarketRow] = []
    seen_keys: set[tuple[str, int]] = set()
    for source_path in source_paths:
        if not source_path.exists():
            raise FileNotFoundError(
                f"TYB+OZ pre-race adapter source file does not exist: {source_path}"
            )
        for line_number, raw_line in enumerate(source_path.read_bytes().splitlines(), start=1):
            row = parse_tyb_line(raw_line, source_path=source_path, line_number=line_number)
            if row is None:
                continue
            key = (row.race_key, row.horse_number)
            if key in seen_keys:
                raise ValueError(
                    "TYB+OZ pre-race adapter found duplicate TYB race_key/horse_number rows: "
                    f"{key} from {source_path}"
                )
            seen_keys.add(key)
            rows.append(row)
    if not rows:
        raise ValueError(
            "TYB+OZ pre-race adapter produced no TYB rows from the provided source files"
        )
    return tuple(rows)


def run_tyb_oz_pre_race_adapter(
    *,
    tyb_source_paths: Iterable[Path],
    oz_source_paths: Iterable[Path],
    output_path: Path,
    force: bool,
) -> TYBOZPreRaceAdapterResult:
    tyb_source_paths = tuple(tyb_source_paths)
    oz_source_paths = tuple(oz_source_paths)
    if not tyb_source_paths:
        raise ValueError("TYB+OZ pre-race adapter requires at least one TYB source path")
    if not oz_source_paths:
        raise ValueError("TYB+OZ pre-race adapter requires at least one OZ source path")
    if output_path.exists() and not force:
        raise FileExistsError(
            "TYB+OZ pre-race adapter refuses to overwrite existing output without --force: "
            f"{output_path}"
        )

    tyb_rows = read_tyb_source_rows(tyb_source_paths)
    oz_rows = read_oz_source_rows(oz_source_paths)
    oz_by_key = {(row.race_key, row.horse_number): row for row in oz_rows}

    joined_rows: list[tuple[TYBLiveMarketRow, OZPreRaceMarketRow]] = []
    missing_oz_keys: list[tuple[str, int]] = []
    for tyb_row in tyb_rows:
        key = (tyb_row.race_key, tyb_row.horse_number)
        oz_row = oz_by_key.get(key)
        if oz_row is None:
            missing_oz_keys.append(key)
            continue
        joined_rows.append((tyb_row, oz_row))

    if missing_oz_keys:
        preview = ", ".join(
            f"{race_key}:{horse_number}" for race_key, horse_number in missing_oz_keys[:5]
        )
        raise ValueError(
            "TYB+OZ pre-race adapter found TYB rows without matching OZ basis odds for keys: "
            f"{preview}"
        )
    if not joined_rows:
        raise ValueError("TYB+OZ pre-race adapter produced no joined TYB/OZ rows")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(RAW_ISH_TYB_OZ_OUTPUT_COLUMNS))
        writer.writeheader()
        for tyb_row, oz_row in joined_rows:
            writer.writerow(
                {
                    "race_key": tyb_row.race_key,
                    "horse_number": str(tyb_row.horse_number),
                    "win_odds": f"{tyb_row.win_odds:.1f}",
                    "place_basis_odds_proxy": f"{oz_row.place_basis_odds:.1f}",
                    "place_odds_min": f"{tyb_row.place_odds_low:.1f}",
                    "odds_index": (
                        f"{tyb_row.odds_index:.1f}" if tyb_row.odds_index is not None else ""
                    ),
                }
            )

    return TYBOZPreRaceAdapterResult(
        tyb_source_paths=tyb_source_paths,
        oz_source_paths=oz_source_paths,
        output_path=output_path,
        row_count=len(joined_rows),
        race_count=len({tyb_row.race_key for tyb_row, _ in joined_rows}),
    )


def _parse_required_float(
    raw_value: str,
    *,
    source_path: Path,
    line_number: int,
    field_name: str,
) -> float:
    text = raw_value.strip()
    if text == "":
        raise ValueError(f"{source_path} line {line_number}: missing {field_name}")
    try:
        return float(text)
    except ValueError as exc:
        raise ValueError(
            f"{source_path} line {line_number}: invalid {field_name} value {text!r}"
        ) from exc
