from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.config import WideResearchDiffConfig
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json


@dataclass(frozen=True)
class SelectedPairDiffRow:
    set_group: str
    window_label: str
    race_key: str
    horse_number_1: int
    horse_number_2: int
    pair_key: str
    is_hit: bool
    wide_payout: float | None
    pair_score: float
    geometric_mean_place_basis_odds: float
    ow_wide_odds: float | None
    geometric_mean_win_odds: float | None
    pair_edge_equivalent: float | None


@dataclass(frozen=True)
class WideResearchDiffSummaryRow:
    set_group: str
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_pair_score: float


@dataclass(frozen=True)
class WideResearchDiffDistributionRow:
    set_group: str
    bucket_type: str
    bucket_label: str
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_pair_score: float


@dataclass(frozen=True)
class WideResearchDiffWindowRow:
    set_group: str
    window_label: str
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_pair_score: float


@dataclass(frozen=True)
class WideResearchDiffPayoutBucketRow:
    set_group: str
    payout_bucket: str
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_pair_score: float


@dataclass(frozen=True)
class WideResearchRepresentativeExampleRow:
    set_group: str
    example_type: str
    window_label: str
    race_key: str
    horse_number_1: int
    horse_number_2: int
    pair_key: str
    is_hit: bool
    wide_payout: float | None
    pair_score: float
    geometric_mean_place_basis_odds: float
    geometric_mean_win_odds: float | None
    ow_wide_odds: float | None
    pair_edge_equivalent: float | None


@dataclass(frozen=True)
class WideResearchDiffResult:
    output_dir: Path
    summaries: tuple[WideResearchDiffSummaryRow, ...]
    distributions: tuple[WideResearchDiffDistributionRow, ...]
    window_rows: tuple[WideResearchDiffWindowRow, ...]
    payout_bucket_rows: tuple[WideResearchDiffPayoutBucketRow, ...]
    representative_examples: tuple[WideResearchRepresentativeExampleRow, ...]


def analyze_wide_research_diff(config: WideResearchDiffConfig) -> WideResearchDiffResult:
    v2_rows = load_selected_pair_rows(
        config.v2_selected_pairs_path,
        split=config.split,
        score_method=config.v2_score_method,
        pair_generation_method=config.v2_pair_generation_method,
        partner_weight=config.v2_partner_weight,
    )
    v3_rows = load_selected_pair_rows(
        config.v3_selected_pairs_path,
        split=config.split,
        score_method=config.v3_score_method,
        pair_generation_method=config.v3_pair_generation_method,
        partner_weight=config.v3_partner_weight,
    )
    win_odds = load_win_odds(config.raw_dir) if config.raw_dir is not None else {}
    v2_map = {pair_identity(row): enrich_row(row, config.v2_label, win_odds) for row in v2_rows}
    v3_map = {pair_identity(row): enrich_row(row, config.v3_label, win_odds) for row in v3_rows}

    common_keys = sorted(set(v2_map) & set(v3_map))
    v2_only_keys = sorted(set(v2_map) - set(v3_map))
    v3_only_keys = sorted(set(v3_map) - set(v2_map))

    grouped_rows = {
        "common": tuple(v3_map[key] for key in common_keys),
        f"{config.v2_label}_only": tuple(v2_map[key] for key in v2_only_keys),
        f"{config.v3_label}_only": tuple(v3_map[key] for key in v3_only_keys),
    }
    summaries = tuple(
        summarize_group(set_group, rows, stake_per_pair=config.stake_per_pair)
        for set_group, rows in grouped_rows.items()
    )
    distributions = build_distribution_rows(grouped_rows, stake_per_pair=config.stake_per_pair)
    window_rows = build_window_rows(grouped_rows, stake_per_pair=config.stake_per_pair)
    payout_bucket_rows = build_payout_bucket_rows(grouped_rows, stake_per_pair=config.stake_per_pair)
    representative_examples = build_representative_examples(
        grouped_rows,
        count=config.representative_example_count,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(config.output_dir / "summary.csv", summaries)
    write_json(
        config.output_dir / "summary.json",
        {
            "analysis": {
                "name": config.name,
                "split": config.split,
                "v2_label": config.v2_label,
                "v3_label": config.v3_label,
                "rows": summaries,
                "window_rows": window_rows,
                "payout_bucket_rows": payout_bucket_rows,
                "popularity_bucket_status": "unavailable_in_current_artifacts",
            },
        },
    )
    write_csv(config.output_dir / "distribution.csv", distributions)
    write_json(
        config.output_dir / "distribution.json",
        {"analysis": {"rows": distributions}},
    )
    write_csv(config.output_dir / "window_summary.csv", window_rows)
    write_json(
        config.output_dir / "window_summary.json",
        {"analysis": {"rows": window_rows}},
    )
    write_csv(config.output_dir / "payout_bucket_summary.csv", payout_bucket_rows)
    write_json(
        config.output_dir / "payout_bucket_summary.json",
        {"analysis": {"rows": payout_bucket_rows}},
    )
    write_csv(config.output_dir / "representative_examples.csv", representative_examples)
    write_json(
        config.output_dir / "representative_examples.json",
        {"analysis": {"rows": representative_examples}},
    )
    return WideResearchDiffResult(
        output_dir=config.output_dir,
        summaries=summaries,
        distributions=distributions,
        window_rows=window_rows,
        payout_bucket_rows=payout_bucket_rows,
        representative_examples=representative_examples,
    )


def load_selected_pair_rows(
    path: Path,
    *,
    split: str,
    score_method: str,
    pair_generation_method: str,
    partner_weight: float | None = None,
) -> tuple[dict[str, str], ...]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["split"] != split:
                continue
            if row["score_method"] != score_method:
                continue
            row_pair_generation_method = row.get("pair_generation_method") or "symmetric_top_k_pairs"
            if row_pair_generation_method != pair_generation_method:
                continue
            row_partner_weight_raw = row.get("partner_weight")
            row_partner_weight = (
                float(row_partner_weight_raw)
                if row_partner_weight_raw not in (None, "", "None")
                else None
            )
            if partner_weight is not None and row_partner_weight != partner_weight:
                continue
            row["pair_generation_method"] = row_pair_generation_method
            rows.append(row)
    return tuple(rows)


def load_win_odds(raw_dir: Path) -> dict[tuple[str, int], float]:
    win_odds: dict[tuple[str, int], float] = {}
    for path in sorted(raw_dir.rglob("OZ*.txt")):
        with path.open("rb") as file:
            for raw_line in file:
                line = raw_line.rstrip(b"\r\n")
                if not line.strip():
                    continue
                text = line.decode("cp932", errors="ignore")
                race_key = text[0:8].strip()
                headcount_raw = text[8:10].strip()
                if not race_key or not headcount_raw:
                    continue
                headcount = int(headcount_raw)
                odds_values = [float(value) for value in find_decimal_values(text[10:])]
                if len(odds_values) < headcount:
                    continue
                for horse_number in range(1, headcount + 1):
                    win_odds[(race_key, horse_number)] = odds_values[horse_number - 1]
    return win_odds


def find_decimal_values(body: str) -> list[str]:
    values: list[str] = []
    current = ""
    for char in body:
        if char.isdigit() or char == ".":
            current += char
        elif current:
            if current.count(".") == 1:
                values.append(current)
            current = ""
    if current and current.count(".") == 1:
        values.append(current)
    return values


def pair_identity(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        row["window_label"],
        row["race_key"],
        f"{int(row['horse_number_1']):02d}{int(row['horse_number_2']):02d}",
    )


def enrich_row(
    row: dict[str, str],
    set_group: str,
    win_odds: dict[tuple[str, int], float],
) -> SelectedPairDiffRow:
    race_key = row["race_key"]
    horse_number_1 = int(row["horse_number_1"])
    horse_number_2 = int(row["horse_number_2"])
    pair_score = float(row["pair_score"])
    ow_wide_odds = float(row["ow_wide_odds"]) if row["ow_wide_odds"] else None
    pair_edge_equivalent = (
        pair_score - (1.0 / ow_wide_odds)
        if ow_wide_odds is not None and ow_wide_odds > 0.0
        else None
    )
    win_odds_1 = win_odds.get((race_key, horse_number_1))
    win_odds_2 = win_odds.get((race_key, horse_number_2))
    geometric_mean_win_odds = (
        (win_odds_1 * win_odds_2) ** 0.5
        if win_odds_1 is not None and win_odds_2 is not None
        else None
    )
    return SelectedPairDiffRow(
        set_group=set_group,
        window_label=row["window_label"],
        race_key=race_key,
        horse_number_1=horse_number_1,
        horse_number_2=horse_number_2,
        pair_key=f"{horse_number_1:02d}{horse_number_2:02d}",
        is_hit=row["is_hit"] == "True",
        wide_payout=float(row["wide_payout"]) if row["wide_payout"] else None,
        pair_score=pair_score,
        geometric_mean_place_basis_odds=float(row["geometric_mean_place_basis_odds"]),
        ow_wide_odds=ow_wide_odds,
        geometric_mean_win_odds=geometric_mean_win_odds,
        pair_edge_equivalent=pair_edge_equivalent,
    )


def summarize_group(
    set_group: str,
    rows: tuple[SelectedPairDiffRow, ...],
    *,
    stake_per_pair: float,
) -> WideResearchDiffSummaryRow:
    pair_count = len(rows)
    hit_rows = tuple(row for row in rows if row.is_hit and row.wide_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.wide_payout or 0.0 for row in rows)
    total_profit = total_return - (pair_count * stake_per_pair)
    return WideResearchDiffSummaryRow(
        set_group=set_group,
        pair_count=pair_count,
        hit_count=hit_count,
        hit_rate=(hit_count / pair_count) if pair_count > 0 else 0.0,
        roi=(total_return / (pair_count * stake_per_pair)) if pair_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.wide_payout or 0.0 for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_pair_score=(sum(row.pair_score for row in rows) / pair_count) if pair_count > 0 else 0.0,
    )


def build_distribution_rows(
    grouped_rows: dict[str, tuple[SelectedPairDiffRow, ...]],
    *,
    stake_per_pair: float,
) -> tuple[WideResearchDiffDistributionRow, ...]:
    rows: list[WideResearchDiffDistributionRow] = []
    for set_group, pair_rows in grouped_rows.items():
        rows.extend(
            build_bucket_rows(
                set_group=set_group,
                bucket_type="place_basis_odds",
                pair_rows=pair_rows,
                value_fn=lambda row: row.geometric_mean_place_basis_odds,
                buckets=((1.4, "lt_1_4"), (1.8, "1_4_to_1_8"), (2.4, "1_8_to_2_4"), (9e9, "ge_2_4")),
                stake_per_pair=stake_per_pair,
            ),
        )
        rows.extend(
            build_bucket_rows(
                set_group=set_group,
                bucket_type="win_odds",
                pair_rows=pair_rows,
                value_fn=lambda row: row.geometric_mean_win_odds,
                buckets=((3.0, "lt_3"), (5.0, "3_to_5"), (8.0, "5_to_8"), (9e9, "ge_8")),
                stake_per_pair=stake_per_pair,
            ),
        )
        rows.extend(
            build_bucket_rows(
                set_group=set_group,
                bucket_type="pair_edge_equivalent",
                pair_rows=pair_rows,
                value_fn=lambda row: row.pair_edge_equivalent,
                buckets=((0.05, "lt_0_05"), (0.10, "0_05_to_0_10"), (0.20, "0_10_to_0_20"), (9e9, "ge_0_20")),
                stake_per_pair=stake_per_pair,
            ),
        )
    return tuple(rows)


def build_window_rows(
    grouped_rows: dict[str, tuple[SelectedPairDiffRow, ...]],
    *,
    stake_per_pair: float,
) -> tuple[WideResearchDiffWindowRow, ...]:
    rows: list[WideResearchDiffWindowRow] = []
    for set_group, pair_rows in grouped_rows.items():
        by_window: dict[str, list[SelectedPairDiffRow]] = {}
        for row in pair_rows:
            by_window.setdefault(row.window_label, []).append(row)
        for window_label in sorted(by_window):
            summary = summarize_group(set_group, tuple(by_window[window_label]), stake_per_pair=stake_per_pair)
            rows.append(
                WideResearchDiffWindowRow(
                    set_group=set_group,
                    window_label=window_label,
                    pair_count=summary.pair_count,
                    hit_count=summary.hit_count,
                    hit_rate=summary.hit_rate,
                    roi=summary.roi,
                    total_profit=summary.total_profit,
                    avg_payout=summary.avg_payout,
                    avg_pair_score=summary.avg_pair_score,
                ),
            )
    return tuple(rows)


def build_payout_bucket_rows(
    grouped_rows: dict[str, tuple[SelectedPairDiffRow, ...]],
    *,
    stake_per_pair: float,
) -> tuple[WideResearchDiffPayoutBucketRow, ...]:
    rows: list[WideResearchDiffPayoutBucketRow] = []
    for set_group, pair_rows in grouped_rows.items():
        by_bucket: dict[str, list[SelectedPairDiffRow]] = {
            "miss": [],
            "100_to_200": [],
            "200_to_400": [],
            "400_to_800": [],
            "800_plus": [],
        }
        for row in pair_rows:
            if row.wide_payout is None:
                by_bucket["miss"].append(row)
            elif row.wide_payout < 200.0:
                by_bucket["100_to_200"].append(row)
            elif row.wide_payout < 400.0:
                by_bucket["200_to_400"].append(row)
            elif row.wide_payout < 800.0:
                by_bucket["400_to_800"].append(row)
            else:
                by_bucket["800_plus"].append(row)
        for bucket_label, bucket_rows in by_bucket.items():
            summary = summarize_group(set_group, tuple(bucket_rows), stake_per_pair=stake_per_pair)
            rows.append(
                WideResearchDiffPayoutBucketRow(
                    set_group=set_group,
                    payout_bucket=bucket_label,
                    pair_count=summary.pair_count,
                    hit_count=summary.hit_count,
                    hit_rate=summary.hit_rate,
                    roi=summary.roi,
                    total_profit=summary.total_profit,
                    avg_payout=summary.avg_payout,
                    avg_pair_score=summary.avg_pair_score,
                ),
            )
    return tuple(rows)


def build_bucket_rows(
    *,
    set_group: str,
    bucket_type: str,
    pair_rows: tuple[SelectedPairDiffRow, ...],
    value_fn,
    buckets: tuple[tuple[float, str], ...],
    stake_per_pair: float,
) -> list[WideResearchDiffDistributionRow]:
    grouped: dict[str, list[SelectedPairDiffRow]] = {label: [] for _, label in buckets}
    grouped["unknown"] = []
    for row in pair_rows:
        value = value_fn(row)
        if value is None:
            grouped["unknown"].append(row)
            continue
        for boundary, label in buckets:
            if value < boundary:
                grouped[label].append(row)
                break
    distribution_rows: list[WideResearchDiffDistributionRow] = []
    for label, rows in grouped.items():
        summary = summarize_group(set_group, tuple(rows), stake_per_pair=stake_per_pair)
        distribution_rows.append(
            WideResearchDiffDistributionRow(
                set_group=set_group,
                bucket_type=bucket_type,
                bucket_label=label,
                pair_count=summary.pair_count,
                hit_count=summary.hit_count,
                hit_rate=summary.hit_rate,
                roi=summary.roi,
                total_profit=summary.total_profit,
                avg_payout=summary.avg_payout,
                avg_pair_score=summary.avg_pair_score,
            ),
        )
    return distribution_rows


def build_representative_examples(
    grouped_rows: dict[str, tuple[SelectedPairDiffRow, ...]],
    *,
    count: int,
) -> tuple[WideResearchRepresentativeExampleRow, ...]:
    rows: list[WideResearchRepresentativeExampleRow] = []
    for set_group, pair_rows in grouped_rows.items():
        example_type = "shared_signal" if set_group == "common" else set_group
        if set_group == "v2_only":
            selected = sorted(
                pair_rows,
                key=lambda row: (
                    row.is_hit,
                    -(row.wide_payout or 0.0),
                    -row.pair_score,
                    row.race_key,
                ),
            )[:count]
        elif set_group == "v3_only":
            selected = sorted(
                pair_rows,
                key=lambda row: (
                    not row.is_hit,
                    -(row.wide_payout or 0.0),
                    -row.pair_score,
                    row.race_key,
                ),
            )[:count]
        else:
            selected = sorted(
                pair_rows,
                key=lambda row: (
                    not row.is_hit,
                    -(row.wide_payout or 0.0),
                    row.race_key,
                ),
            )[:count]
        for row in selected:
            rows.append(
                WideResearchRepresentativeExampleRow(
                    set_group=set_group,
                    example_type=example_type,
                    window_label=row.window_label,
                    race_key=row.race_key,
                    horse_number_1=row.horse_number_1,
                    horse_number_2=row.horse_number_2,
                    pair_key=row.pair_key,
                    is_hit=row.is_hit,
                    wide_payout=row.wide_payout,
                    pair_score=row.pair_score,
                    geometric_mean_place_basis_odds=row.geometric_mean_place_basis_odds,
                    geometric_mean_win_odds=row.geometric_mean_win_odds,
                    ow_wide_odds=row.ow_wide_odds,
                    pair_edge_equivalent=row.pair_edge_equivalent,
                ),
            )
    return tuple(rows)
