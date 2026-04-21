from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from horse_bet_lab.ingest.transforms import decode_text

RAW_ISH_OZ_OUTPUT_COLUMNS = (
    "race_key",
    "horse_number",
    "win_odds",
    "place_basis_odds_proxy",
)


@dataclass(frozen=True)
class OZPreRaceMarketRow:
    race_key: str
    horse_number: int
    headcount: int
    win_odds: float
    place_basis_odds: float


@dataclass(frozen=True)
class OZPreRaceAdapterResult:
    source_paths: tuple[Path, ...]
    output_path: Path
    row_count: int
    race_count: int


def discover_oz_source_paths(raw_root: Path) -> tuple[Path, ...]:
    if not raw_root.exists():
        raise FileNotFoundError(f"OZ pre-race adapter raw root does not exist: {raw_root}")
    source_paths = tuple(sorted(path for path in raw_root.rglob("OZ*.txt") if path.is_file()))
    if not source_paths:
        raise FileNotFoundError(f"OZ pre-race adapter found no OZ*.txt files under: {raw_root}")
    return source_paths


def parse_oz_line(
    raw_line: bytes,
    *,
    source_path: Path,
    line_number: int,
) -> tuple[OZPreRaceMarketRow, ...]:
    if not raw_line.strip():
        return ()
    text = decode_text(raw_line)
    race_key = text[0:8].strip()
    if race_key == "":
        raise ValueError(f"{source_path} line {line_number}: missing race_key in chars 0:8")
    headcount_text = text[8:10].strip()
    if headcount_text == "":
        raise ValueError(f"{source_path} line {line_number}: missing headcount in chars 8:10")
    try:
        headcount = int(headcount_text)
    except ValueError as exc:
        raise ValueError(
            f"{source_path} line {line_number}: invalid OZ headcount {headcount_text!r}"
        ) from exc
    if headcount <= 0:
        raise ValueError(
            f"{source_path} line {line_number}: headcount must be positive, got {headcount}"
        )

    odds_values = re.findall(r"\d+\.\d", text[10:])
    expected_count = headcount * 2
    if len(odds_values) < expected_count:
        raise ValueError(
            (
                f"{source_path} line {line_number}: expected at least {expected_count} "
                f"OZ odds values, got {len(odds_values)}"
            ),
        )

    win_odds = odds_values[:headcount]
    place_basis_odds = odds_values[headcount:expected_count]
    return tuple(
        OZPreRaceMarketRow(
            race_key=race_key,
            horse_number=horse_number,
            headcount=headcount,
            win_odds=float(win_odds[horse_number - 1]),
            place_basis_odds=float(place_basis_odds[horse_number - 1]),
        )
        for horse_number in range(1, headcount + 1)
    )


def read_oz_source_rows(source_paths: Iterable[Path]) -> tuple[OZPreRaceMarketRow, ...]:
    rows: list[OZPreRaceMarketRow] = []
    seen_keys: set[tuple[str, int]] = set()
    for source_path in source_paths:
        if not source_path.exists():
            raise FileNotFoundError(
                f"OZ pre-race adapter source file does not exist: {source_path}"
            )
        for line_number, raw_line in enumerate(source_path.read_bytes().splitlines(), start=1):
            for row in parse_oz_line(raw_line, source_path=source_path, line_number=line_number):
                key = (row.race_key, row.horse_number)
                if key in seen_keys:
                    raise ValueError(
                        "OZ pre-race adapter found duplicate race_key/horse_number rows: "
                        f"{key} from {source_path}"
                    )
                seen_keys.add(key)
                rows.append(row)
    if not rows:
        raise ValueError("OZ pre-race adapter produced no rows from the provided source files")
    return tuple(rows)


def run_oz_pre_race_adapter(
    *,
    source_paths: Iterable[Path],
    output_path: Path,
    force: bool,
) -> OZPreRaceAdapterResult:
    source_paths = tuple(source_paths)
    if not source_paths:
        raise ValueError("OZ pre-race adapter requires at least one source path")
    if output_path.exists() and not force:
        raise FileExistsError(
            "OZ pre-race adapter refuses to overwrite existing output without --force: "
            f"{output_path}"
        )

    rows = read_oz_source_rows(source_paths)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(RAW_ISH_OZ_OUTPUT_COLUMNS))
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "race_key": row.race_key,
                    "horse_number": str(row.horse_number),
                    "win_odds": f"{row.win_odds:.1f}",
                    "place_basis_odds_proxy": f"{row.place_basis_odds:.1f}",
                }
            )

    return OZPreRaceAdapterResult(
        source_paths=source_paths,
        output_path=output_path,
        row_count=len(rows),
        race_count=len({row.race_key for row in rows}),
    )
