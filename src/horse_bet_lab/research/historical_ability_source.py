from __future__ import annotations

import csv
import json
import math
import re
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import fmean
from typing import Callable, Iterable, Sequence, TypeVar

from horse_bet_lab.ingest.transforms import decode_text, to_date, to_int, to_text

READY_VERDICT = "HISTORICAL_ABILITY_SOURCE_BRIDGE_READY"
BLOCKED_VERDICT = "HISTORICAL_ABILITY_SOURCE_BRIDGE_BLOCKED"

KYI_RECORD_BYTES = 1022
SED_RECORD_BYTES = 374
BAC_RECORD_BYTES = 182

SURFACE_COLUMNS = (
    "race_date",
    "race_key",
    "horse_number",
    "registration_id",
    "horse_name",
    "evaluation_year",
    "venue_code",
    "current_distance_m",
    "card_field_size",
    "label_result_starter_count",
    "label_place_slots",
    "is_place",
    "prior_start_count",
    "days_since_last_start",
    "last_1_finish_percentile",
    "last_3_mean_finish_percentile",
    "last_5_mean_finish_percentile",
    "last_3_top3_rate",
    "last_5_top3_rate",
    "last_5_recency_weighted_form",
    "last_5_distance_compatibility_rate",
    "last_5_venue_compatibility_rate",
)

IDENTITY_COLUMNS = (
    "race_date",
    "race_key",
    "horse_number",
    "registration_id",
    "horse_name",
    "evaluation_year",
)

LABEL_COLUMNS = (
    "label_result_starter_count",
    "label_place_slots",
    "is_place",
)

MODEL_FEATURE_COLUMNS = tuple(
    column
    for column in SURFACE_COLUMNS
    if column not in {*IDENTITY_COLUMNS, *LABEL_COLUMNS}
)

RowT = TypeVar("RowT")


@dataclass(frozen=True)
class KyiCardRow:
    race_date: date
    race_key: str
    horse_number: int
    registration_id: str
    horse_name: str


@dataclass(frozen=True)
class SedResultRow:
    result_date: date
    race_key: str
    horse_number: int
    registration_id: str
    horse_name: str
    distance_m: int | None
    finish_position: int | None


@dataclass(frozen=True)
class BacRaceRow:
    race_date: date
    race_key: str
    distance_m: int | None


@dataclass(frozen=True)
class HistoricalStart:
    result_date: date
    venue_code: str
    distance_m: int | None
    finish_position: int
    field_size: int
    finish_percentile: float


@dataclass(frozen=True)
class SourceAuditResult:
    summary: dict[str, object]
    year_rows: tuple[dict[str, object], ...]
    surface_rows: tuple[dict[str, object], ...]


def discover_files(raw_root: Path, file_kind: str, years: Iterable[int]) -> tuple[Path, ...]:
    wanted_years = set(years)
    files = []
    for path in raw_root.rglob("*.txt"):
        if not path.name.upper().startswith(file_kind.upper()):
            continue
        if date_from_jrdb_filename(path).year not in wanted_years:
            continue
        files.append(path)
    return tuple(sorted(files))


def date_from_jrdb_filename(path: Path) -> date:
    matches = re.findall(r"(\d{6})", path.stem)
    if not matches:
        raise ValueError(f"cannot derive YYMMDD from JRDB filename: {path}")
    value = matches[-1]
    return date(2000 + int(value[0:2]), int(value[2:4]), int(value[4:6]))


def parse_kyi_file(path: Path) -> list[KyiCardRow]:
    race_date = date_from_jrdb_filename(path)
    rows: list[KyiCardRow] = []
    for line_number, raw_line in _record_lines(path, KYI_RECORD_BYTES):
        rows.append(
            KyiCardRow(
                race_date=race_date,
                race_key=_required_text(raw_line, 0, 8, path, line_number, "race_key"),
                horse_number=_required_int(
                    raw_line,
                    8,
                    10,
                    path,
                    line_number,
                    "horse_number",
                ),
                registration_id=_required_text(
                    raw_line,
                    10,
                    18,
                    path,
                    line_number,
                    "registration_id",
                ),
                horse_name=_required_text(
                    raw_line,
                    18,
                    54,
                    path,
                    line_number,
                    "horse_name",
                ),
            )
        )
    return rows


def parse_sed_file(path: Path) -> list[SedResultRow]:
    rows: list[SedResultRow] = []
    for line_number, raw_line in _record_lines(path, SED_RECORD_BYTES):
        result_date = to_date(decode_text(raw_line[18:26]))
        if result_date is None:
            raise ValueError(f"{path} line {line_number}: missing result_date")
        rows.append(
            SedResultRow(
                result_date=result_date,
                race_key=_required_text(raw_line, 0, 8, path, line_number, "race_key"),
                horse_number=_required_int(
                    raw_line,
                    8,
                    10,
                    path,
                    line_number,
                    "horse_number",
                ),
                registration_id=_required_text(
                    raw_line,
                    10,
                    18,
                    path,
                    line_number,
                    "registration_id",
                ),
                horse_name=_required_text(
                    raw_line,
                    26,
                    62,
                    path,
                    line_number,
                    "horse_name",
                ),
                distance_m=to_int(decode_text(raw_line[62:66])),
                finish_position=to_int(decode_text(raw_line[140:142])),
            )
        )
    return rows


def parse_bac_file(path: Path) -> list[BacRaceRow]:
    rows: list[BacRaceRow] = []
    for line_number, raw_line in _record_lines(path, BAC_RECORD_BYTES):
        race_date = to_date(decode_text(raw_line[8:16]))
        if race_date is None:
            raise ValueError(f"{path} line {line_number}: missing race_date")
        rows.append(
            BacRaceRow(
                race_date=race_date,
                race_key=_required_text(raw_line, 0, 8, path, line_number, "race_key"),
                distance_m=to_int(decode_text(raw_line[20:24])),
            )
        )
    return rows


def build_source_audit(
    *,
    raw_root: Path,
    history_years: Sequence[int] = tuple(range(2019, 2026)),
    evaluation_years: Sequence[int] = (2023, 2024, 2025),
    minimum_identity_match_rate: float = 0.999,
) -> SourceAuditResult:
    history_years_tuple = tuple(history_years)
    evaluation_years_tuple = tuple(evaluation_years)
    kyi_files = discover_files(raw_root, "KYI", evaluation_years_tuple)
    sed_files = discover_files(raw_root, "SED", history_years_tuple)
    bac_files = discover_files(raw_root, "BAC", history_years_tuple)

    raw_kyi_rows = [row for path in kyi_files for row in parse_kyi_file(path)]
    raw_sed_rows = [row for path in sed_files for row in parse_sed_file(path)]
    raw_bac_rows = [row for path in bac_files for row in parse_bac_file(path)]

    kyi_rows, kyi_exact_duplicates, kyi_conflicting_duplicates = _canonicalize_rows(
        raw_kyi_rows,
        key=lambda row: _row_key(row.race_date, row.race_key, row.horse_number),
    )
    sed_rows, sed_exact_duplicates, sed_conflicting_duplicates = _canonicalize_rows(
        raw_sed_rows,
        key=lambda row: _row_key(row.result_date, row.race_key, row.horse_number),
    )
    bac_rows, bac_exact_duplicates, bac_conflicting_duplicates = _canonicalize_rows(
        raw_bac_rows,
        key=lambda row: (row.race_date, row.race_key),
    )

    sed_by_key = {
        _row_key(row.result_date, row.race_key, row.horse_number): row for row in sed_rows
    }
    bac_by_key = {(row.race_date, row.race_key): row for row in bac_rows}

    card_field_size = Counter((row.race_date, row.race_key) for row in kyi_rows)
    result_starter_count = Counter(
        (row.result_date, row.race_key)
        for row in sed_rows
        if row.finish_position is not None and row.finish_position > 0
    )
    history_by_registration = _build_history_index(sed_rows, result_starter_count)
    history_dates = {
        registration_id: tuple(start.result_date for start in starts)
        for registration_id, starts in history_by_registration.items()
    }

    surface_rows: list[dict[str, object]] = []
    per_year: dict[int, Counter[str]] = defaultdict(Counter)
    registration_matches = 0
    horse_name_matches = 0
    exact_identity_matches = 0
    strict_prior_violations = 0

    for card in sorted(
        kyi_rows,
        key=lambda row: (row.race_date, row.race_key, row.horse_number),
    ):
        year_counter = per_year[card.race_date.year]
        year_counter["kyi_rows"] += 1
        result = sed_by_key.get(_row_key(card.race_date, card.race_key, card.horse_number))
        if result is None:
            year_counter["missing_current_sed"] += 1
            continue
        year_counter["current_sed_matches"] += 1
        registration_match = card.registration_id == result.registration_id
        horse_name_match = card.horse_name == result.horse_name
        registration_matches += int(registration_match)
        horse_name_matches += int(horse_name_match)
        exact_identity_matches += int(registration_match and horse_name_match)
        if not registration_match or not horse_name_match:
            year_counter["identity_mismatches"] += 1
            continue
        year_counter["exact_identity_matches"] += 1
        if result.finish_position is None or result.finish_position <= 0:
            year_counter["nonstarter_or_unranked_rows"] += 1
            continue

        starter_count = result_starter_count[(card.race_date, card.race_key)]
        place_slots = _place_slots(starter_count)
        if place_slots == 0:
            year_counter["unsupported_small_field_rows"] += 1
            continue

        starts = history_by_registration.get(card.registration_id, ())
        dates = history_dates.get(card.registration_id, ())
        prior_end = bisect_left(dates, card.race_date)
        prior_starts = starts[:prior_end]
        strict_prior_violations += sum(
            start.result_date >= card.race_date for start in prior_starts
        )
        current_bac = bac_by_key.get((card.race_date, card.race_key))
        current_distance = current_bac.distance_m if current_bac is not None else None
        features = _historical_features(
            prior_starts,
            current_date=card.race_date,
            current_distance_m=current_distance,
            current_venue_code=card.race_key[:2],
        )
        surface_rows.append(
            {
                "race_date": card.race_date.isoformat(),
                "race_key": card.race_key,
                "horse_number": card.horse_number,
                "registration_id": card.registration_id,
                "horse_name": card.horse_name,
                "evaluation_year": card.race_date.year,
                "venue_code": card.race_key[:2],
                "current_distance_m": current_distance,
                "card_field_size": card_field_size[(card.race_date, card.race_key)],
                "label_result_starter_count": starter_count,
                "label_place_slots": place_slots,
                "is_place": int(result.finish_position <= place_slots),
                **features,
            }
        )
        year_counter["surface_rows"] += 1
        prior_count = len(prior_starts)
        for threshold in (1, 3, 5, 10):
            if prior_count >= threshold:
                year_counter[f"prior_ge_{threshold}"] += 1

    kyi_count = len(kyi_rows)
    current_sed_matches = sum(counter["current_sed_matches"] for counter in per_year.values())
    registration_match_rate = _safe_ratio(registration_matches, kyi_count)
    horse_name_match_rate = _safe_ratio(horse_name_matches, kyi_count)
    exact_identity_match_rate = _safe_ratio(exact_identity_matches, kyi_count)
    blockers: list[str] = []
    if not kyi_files:
        blockers.append("no KYI files found for evaluation years")
    if not sed_files:
        blockers.append("no SED files found for history years")
    if not bac_files:
        blockers.append("no BAC files found for history years")
    if kyi_conflicting_duplicates:
        blockers.append(f"KYI conflicting duplicate keys: {kyi_conflicting_duplicates}")
    if sed_conflicting_duplicates:
        blockers.append(f"SED conflicting duplicate keys: {sed_conflicting_duplicates}")
    if bac_conflicting_duplicates:
        blockers.append(f"BAC conflicting duplicate keys: {bac_conflicting_duplicates}")
    if exact_identity_match_rate < minimum_identity_match_rate:
        blockers.append(
            "exact KYI-to-SED identity match rate below threshold: "
            f"{exact_identity_match_rate:.6f} < {minimum_identity_match_rate:.6f}"
        )
    if strict_prior_violations:
        blockers.append(f"strict-prior leakage violations: {strict_prior_violations}")

    year_rows = tuple(
        _year_summary_row(year, per_year[year]) for year in evaluation_years_tuple
    )
    summary: dict[str, object] = {
        "final_verdict": READY_VERDICT if not blockers else BLOCKED_VERDICT,
        "raw_root": str(raw_root),
        "history_years": list(history_years_tuple),
        "evaluation_years": list(evaluation_years_tuple),
        "files": {
            "kyi": len(kyi_files),
            "sed": len(sed_files),
            "bac": len(bac_files),
        },
        "rows": {
            "kyi_raw": len(raw_kyi_rows),
            "kyi_canonical": kyi_count,
            "sed_raw": len(raw_sed_rows),
            "sed_canonical": len(sed_rows),
            "bac_raw": len(raw_bac_rows),
            "bac_canonical": len(bac_rows),
            "current_sed_matches": current_sed_matches,
            "history_surface": len(surface_rows),
        },
        "identity": {
            "registration_match_rate": registration_match_rate,
            "horse_name_match_rate": horse_name_match_rate,
            "exact_identity_match_rate": exact_identity_match_rate,
            "minimum_required_rate": minimum_identity_match_rate,
            "kyi_exact_duplicate_rows_removed": kyi_exact_duplicates,
            "sed_exact_duplicate_rows_removed": sed_exact_duplicates,
            "bac_exact_duplicate_rows_removed": bac_exact_duplicates,
            "kyi_conflicting_duplicate_keys": kyi_conflicting_duplicates,
            "sed_conflicting_duplicate_keys": sed_conflicting_duplicates,
            "bac_conflicting_duplicate_keys": bac_conflicting_duplicates,
        },
        "leakage": {
            "strict_prior_rule": "prior_result_date < current_race_date",
            "strict_prior_violations": strict_prior_violations,
            "same_day_results_excluded": True,
        },
        "surface_missingness": _surface_missingness(surface_rows),
        "surface_columns": list(SURFACE_COLUMNS),
        "identity_columns": list(IDENTITY_COLUMNS),
        "label_columns": list(LABEL_COLUMNS),
        "model_feature_columns": list(MODEL_FEATURE_COLUMNS),
        "blockers": blockers,
        "year_coverage": list(year_rows),
    }
    return SourceAuditResult(
        summary=summary,
        year_rows=year_rows,
        surface_rows=tuple(surface_rows),
    )


def write_source_audit(result: SourceAuditResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "phase650h_source_summary.json").write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(output_dir / "phase650h_year_coverage.csv", result.year_rows)
    _write_csv(
        output_dir / "phase650h_history_surface.csv",
        result.surface_rows,
        fieldnames=SURFACE_COLUMNS,
    )
    (output_dir / "phase650h_findings.md").write_text(
        _build_findings_markdown(result),
        encoding="utf-8",
    )


def _record_lines(path: Path, expected_bytes: int) -> Iterable[tuple[int, bytes]]:
    for line_number, raw_line in enumerate(path.read_bytes().splitlines(), start=1):
        if not raw_line.strip():
            continue
        if len(raw_line) != expected_bytes:
            raise ValueError(
                f"{path} line {line_number}: expected {expected_bytes} bytes, got {len(raw_line)}"
            )
        yield line_number, raw_line


def _required_text(
    raw_line: bytes,
    start: int,
    end: int,
    path: Path,
    line_number: int,
    field_name: str,
) -> str:
    value = to_text(decode_text(raw_line[start:end]))
    if value is None:
        raise ValueError(f"{path} line {line_number}: missing {field_name}")
    return value


def _required_int(
    raw_line: bytes,
    start: int,
    end: int,
    path: Path,
    line_number: int,
    field_name: str,
) -> int:
    value = to_int(decode_text(raw_line[start:end]))
    if value is None:
        raise ValueError(f"{path} line {line_number}: missing {field_name}")
    return value


def _row_key(race_date: date, race_key: str, horse_number: int) -> tuple[date, str, int]:
    return race_date, race_key, horse_number


def _canonicalize_rows(
    rows: Sequence[RowT],
    *,
    key: Callable[[RowT], object],
) -> tuple[list[RowT], int, int]:
    grouped: dict[object, list[RowT]] = defaultdict(list)
    for row in rows:
        grouped[key(row)].append(row)
    canonical: list[RowT] = []
    exact_duplicate_rows = 0
    conflicting_duplicate_keys = 0
    for key_value in sorted(grouped, key=str):
        candidates = grouped[key_value]
        unique_candidates = list(dict.fromkeys(candidates))
        if len(unique_candidates) == 1:
            exact_duplicate_rows += len(candidates) - 1
        else:
            conflicting_duplicate_keys += 1
        canonical.append(unique_candidates[0])
    return canonical, exact_duplicate_rows, conflicting_duplicate_keys


def _build_history_index(
    sed_rows: Sequence[SedResultRow],
    result_starter_count: Counter[tuple[date, str]],
) -> dict[str, tuple[HistoricalStart, ...]]:
    grouped: dict[str, list[HistoricalStart]] = defaultdict(list)
    for row in sed_rows:
        if row.finish_position is None or row.finish_position <= 0:
            continue
        field_size = result_starter_count[(row.result_date, row.race_key)]
        if field_size <= 1:
            continue
        percentile = (field_size - row.finish_position) / (field_size - 1)
        grouped[row.registration_id].append(
            HistoricalStart(
                result_date=row.result_date,
                venue_code=row.race_key[:2],
                distance_m=row.distance_m,
                finish_position=row.finish_position,
                field_size=field_size,
                finish_percentile=percentile,
            )
        )
    return {
        registration_id: tuple(
            sorted(
                starts,
                key=lambda start: (
                    start.result_date,
                    start.venue_code,
                    start.distance_m or 0,
                    start.finish_position,
                ),
            )
        )
        for registration_id, starts in grouped.items()
    }


def _historical_features(
    prior_starts: Sequence[HistoricalStart],
    *,
    current_date: date,
    current_distance_m: int | None,
    current_venue_code: str,
) -> dict[str, object]:
    last_1 = prior_starts[-1:]
    last_3 = prior_starts[-3:]
    last_5 = prior_starts[-5:]
    return {
        "prior_start_count": len(prior_starts),
        "days_since_last_start": (
            (current_date - last_1[-1].result_date).days if last_1 else None
        ),
        "last_1_finish_percentile": _mean_percentile(last_1),
        "last_3_mean_finish_percentile": _mean_percentile(last_3),
        "last_5_mean_finish_percentile": _mean_percentile(last_5),
        "last_3_top3_rate": _top3_rate(last_3),
        "last_5_top3_rate": _top3_rate(last_5),
        "last_5_recency_weighted_form": _recency_weighted_form(last_5, current_date),
        "last_5_distance_compatibility_rate": _distance_compatibility_rate(
            last_5,
            current_distance_m,
        ),
        "last_5_venue_compatibility_rate": (
            fmean(start.venue_code == current_venue_code for start in last_5)
            if last_5
            else None
        ),
    }


def _mean_percentile(starts: Sequence[HistoricalStart]) -> float | None:
    return fmean(start.finish_percentile for start in starts) if starts else None


def _top3_rate(starts: Sequence[HistoricalStart]) -> float | None:
    return fmean(start.finish_position <= 3 for start in starts) if starts else None


def _recency_weighted_form(
    starts: Sequence[HistoricalStart],
    current_date: date,
) -> float | None:
    if not starts:
        return None
    weights = [math.pow(0.5, (current_date - start.result_date).days / 180.0) for start in starts]
    denominator = sum(weights)
    if denominator <= 0.0:
        return None
    weighted_sum = sum(
        start.finish_percentile * weight
        for start, weight in zip(starts, weights, strict=True)
    )
    return weighted_sum / denominator


def _distance_compatibility_rate(
    starts: Sequence[HistoricalStart],
    current_distance_m: int | None,
) -> float | None:
    if current_distance_m is None:
        return None
    comparable_distances = [
        start.distance_m for start in starts if start.distance_m is not None
    ]
    if not comparable_distances:
        return None
    return fmean(
        abs(distance_m - current_distance_m) <= 200
        for distance_m in comparable_distances
    )


def _place_slots(starter_count: int) -> int:
    if starter_count >= 8:
        return 3
    if starter_count >= 5:
        return 2
    return 0


def _year_summary_row(year: int, counts: Counter[str]) -> dict[str, object]:
    surface_rows = counts["surface_rows"]
    return {
        "evaluation_year": year,
        "kyi_rows": counts["kyi_rows"],
        "current_sed_matches": counts["current_sed_matches"],
        "exact_identity_matches": counts["exact_identity_matches"],
        "identity_mismatches": counts["identity_mismatches"],
        "missing_current_sed": counts["missing_current_sed"],
        "nonstarter_or_unranked_rows": counts["nonstarter_or_unranked_rows"],
        "unsupported_small_field_rows": counts["unsupported_small_field_rows"],
        "surface_rows": surface_rows,
        "prior_ge_1_rate": _safe_ratio(counts["prior_ge_1"], surface_rows),
        "prior_ge_3_rate": _safe_ratio(counts["prior_ge_3"], surface_rows),
        "prior_ge_5_rate": _safe_ratio(counts["prior_ge_5"], surface_rows),
        "prior_ge_10_rate": _safe_ratio(counts["prior_ge_10"], surface_rows),
    }


def _safe_ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _surface_missingness(rows: Sequence[dict[str, object]]) -> dict[str, float]:
    denominator = len(rows)
    if denominator == 0:
        return {column: 0.0 for column in SURFACE_COLUMNS}
    return {
        column: sum(row.get(column) is None for row in rows) / denominator
        for column in SURFACE_COLUMNS
    }


def _write_csv(
    path: Path,
    rows: Sequence[dict[str, object]],
    *,
    fieldnames: Sequence[str] | None = None,
) -> None:
    selected_fields = list(fieldnames or (list(rows[0].keys()) if rows else []))
    with path.open("w", encoding="utf-8", newline="") as file:
        if not selected_fields:
            return
        writer = csv.DictWriter(file, fieldnames=selected_fields)
        writer.writeheader()
        writer.writerows(rows)


def _build_findings_markdown(result: SourceAuditResult) -> str:
    identity = result.summary["identity"]
    rows = result.summary["rows"]
    assert isinstance(identity, dict)
    assert isinstance(rows, dict)
    lines = [
        "# Phase650H Historical Ability Source Audit",
        "",
        "## Final verdict",
        "",
        f"`{result.summary['final_verdict']}`",
        "",
        "## Source bridge",
        "",
        f"- KYI current-card rows: `{rows['kyi_canonical']}`",
        f"- SED history/result rows: `{rows['sed_canonical']}`",
        f"- Generated history-surface rows: `{rows['history_surface']}`",
        "- Exact KYI-to-SED identity match rate: "
        f"`{float(identity['exact_identity_match_rate']):.6f}`",
        "- Strict-prior rule: `prior_result_date < current_race_date`",
        "- Same-day results are excluded from history.",
        "",
        "## Scope boundary",
        "",
        "This phase audits source identity, coverage, and temporal safety only. "
        "It does not fit a model, search a betting threshold, size stakes, or create selections.",
        "",
    ]
    blockers = result.summary["blockers"]
    assert isinstance(blockers, list)
    if blockers:
        lines.extend(["## Blockers", ""])
        lines.extend(f"- {blocker}" for blocker in blockers)
        lines.append("")
    return "\n".join(lines)
