from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, TypeAlias

import duckdb

from horse_bet_lab.config import (
    ReferenceRegimeLabelDiffConfig,
    load_ranking_rule_comparison_config,
    load_reference_guard_compare_config,
    load_reference_strategy_diagnostics_config,
    load_second_guard_selection_config,
)
from horse_bet_lab.evaluation.reference_strategy import (
    average_optional_float,
    write_csv,
    write_json,
)
from horse_bet_lab.evaluation.second_guard_selection import run_second_guard_selection


@dataclass(frozen=True)
class ReferenceRegimeLabelRow:
    regime_label: str
    window_label: str
    result_date: str
    race_key: str
    horse_number: int
    finish_position: int | None
    month_label: str
    headcount: int | None
    distance_m: int | None
    popularity: int | None
    win_odds: float | None
    place_basis_odds: float | None
    edge: float | None
    place_payout: float
    bet_profit: float


@dataclass(frozen=True)
class ReferenceRegimeLabelSummary:
    regime_label: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class ReferenceRegimeLabelDistributionRow:
    regime_label: str
    metric: str
    bucket_label: str
    bet_count: int
    share: float
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class ReferenceRegimeLabelDiffRow:
    comparison_type: str
    metric: str
    bucket_label: str
    left_regime_label: str
    left_bet_count: int
    left_hit_rate: float
    left_roi: float
    left_total_profit: float
    right_regime_label: str
    right_bet_count: int
    right_hit_rate: float
    right_roi: float
    right_total_profit: float
    roi_diff_right_minus_left: float
    profit_diff_right_minus_left: float


@dataclass(frozen=True)
class ReferenceRegimeLabelRepresentativeExampleRow:
    comparison_type: str
    metric: str
    bucket_label: str
    regime_label: str
    result_date: str
    window_label: str
    race_key: str
    horse_number: int
    finish_position: int | None
    month_label: str
    headcount: int | None
    distance_m: int | None
    popularity: int | None
    win_odds: float | None
    place_basis_odds: float | None
    edge: float | None
    place_payout: float
    bet_profit: float


@dataclass(frozen=True)
class ReferenceRegimeLabelDiffResult:
    output_dir: Path
    summaries: tuple[ReferenceRegimeLabelSummary, ...]
    distributions: tuple[ReferenceRegimeLabelDistributionRow, ...]
    stronger_rows: tuple[ReferenceRegimeLabelDiffRow, ...]
    weak_rows: tuple[ReferenceRegimeLabelDiffRow, ...]
    representative_examples: tuple[ReferenceRegimeLabelRepresentativeExampleRow, ...]


SerializableRow: TypeAlias = (
    ReferenceRegimeLabelSummary
    | ReferenceRegimeLabelDistributionRow
    | ReferenceRegimeLabelDiffRow
    | ReferenceRegimeLabelRepresentativeExampleRow
)


def run_reference_regime_label_diff(
    config: ReferenceRegimeLabelDiffConfig,
) -> ReferenceRegimeLabelDiffResult:
    selection_config = load_second_guard_selection_config(
        config.second_guard_selection_config_path,
    )
    selection_result = run_second_guard_selection(selection_config)
    duckdb_path = resolve_duckdb_path(selection_config.reference_guard_compare_config_path)
    rows = load_reference_rows(
        duckdb_path=duckdb_path,
        selected_rows=selection_result.selected_test_rows,
        regimes=config.regimes,
    )
    summaries = build_summary_rows(rows)
    distributions = build_distribution_rows(rows)
    stronger_rows = build_stronger_rows(distributions, config.regimes)
    weak_rows = build_weak_rows(distributions, config.regimes)
    representative_examples = build_representative_examples(
        rows=rows,
        stronger_rows=stronger_rows,
        weak_rows=weak_rows,
        examples_per_group=config.representative_examples_per_group,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceRegimeLabelDiffResult(
        output_dir=config.output_dir,
        summaries=summaries,
        distributions=distributions,
        stronger_rows=stronger_rows,
        weak_rows=weak_rows,
        representative_examples=representative_examples,
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "distribution.csv", result.distributions)
    write_json(
        config.output_dir / "distribution.json",
        {"analysis": {"rows": result.distributions}},
    )
    write_csv(config.output_dir / "stronger_regimes.csv", result.stronger_rows)
    write_json(
        config.output_dir / "stronger_regimes.json",
        {"analysis": {"rows": result.stronger_rows}},
    )
    write_csv(config.output_dir / "weak_in_both_regimes.csv", result.weak_rows)
    write_json(
        config.output_dir / "weak_in_both_regimes.json",
        {"analysis": {"rows": result.weak_rows}},
    )
    write_csv(config.output_dir / "representative_examples.csv", result.representative_examples)
    write_json(
        config.output_dir / "representative_examples.json",
        {"analysis": {"rows": result.representative_examples}},
    )
    return result


def resolve_duckdb_path(reference_guard_compare_config_path: Path) -> Path:
    guard_config = load_reference_guard_compare_config(reference_guard_compare_config_path)
    reference_config = load_reference_strategy_diagnostics_config(
        guard_config.reference_strategy_config_path,
    )
    ranking_config = load_ranking_rule_comparison_config(
        reference_config.ranking_rule_comparison_config_path,
    )
    return ranking_config.duckdb_path


def load_reference_rows(
    *,
    duckdb_path: Path,
    selected_rows: tuple[Any, ...],
    regimes: tuple[tuple[str, date | None, date | None], ...],
) -> tuple[ReferenceRegimeLabelRow, ...]:
    if not selected_rows:
        return ()
    connection = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        connection.execute("DROP TABLE IF EXISTS selected_reference_rows")
        connection.execute(
            """
            CREATE TEMP TABLE selected_reference_rows (
                window_label VARCHAR,
                result_date DATE,
                race_key VARCHAR,
                horse_number INTEGER,
                place_payout DOUBLE,
                edge DOUBLE,
                win_odds DOUBLE,
                place_basis_odds DOUBLE,
                popularity INTEGER
            )
            """,
        )
        connection.executemany(
            """
            INSERT INTO selected_reference_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.window_label,
                    row.row.result_date.isoformat(),
                    row.row.race_key,
                    row.row.horse_number,
                    row.row.place_payout or 0.0,
                    row.row.edge,
                    row.row.win_odds,
                    row.row.place_basis_odds,
                    row.row.popularity,
                )
                for row in selected_rows
            ],
        )
        fetched = connection.execute(
            """
            SELECT
                selected.window_label,
                selected.result_date,
                selected.race_key,
                selected.horse_number,
                sed.finish_position,
                oz.headcount,
                sed.distance_m,
                selected.popularity,
                selected.win_odds,
                selected.place_basis_odds,
                selected.edge,
                selected.place_payout
            FROM selected_reference_rows AS selected
            LEFT JOIN jrdb_sed_staging AS sed
                USING (race_key, horse_number)
            LEFT JOIN jrdb_oz_staging AS oz
                USING (race_key, horse_number)
            ORDER BY selected.result_date, selected.race_key, selected.horse_number
            """,
        ).fetchall()
    finally:
        connection.close()

    rows: list[ReferenceRegimeLabelRow] = []
    for (
        window_label,
        result_date_value,
        race_key,
        horse_number,
        finish_position,
        headcount,
        distance_m,
        popularity,
        win_odds,
        place_basis_odds,
        edge,
        place_payout,
    ) in fetched:
        result_date_obj = (
            result_date_value
            if isinstance(result_date_value, date)
            else date.fromisoformat(str(result_date_value))
        )
        regime_label = resolve_regime_label(result_date_obj, regimes)
        if regime_label is None:
            continue
        payout = float(place_payout or 0.0)
        rows.append(
            ReferenceRegimeLabelRow(
                regime_label=regime_label,
                window_label=str(window_label),
                result_date=result_date_obj.isoformat(),
                race_key=str(race_key),
                horse_number=int(horse_number),
                finish_position=int(finish_position) if finish_position is not None else None,
                month_label=f"{result_date_obj.month:02d}",
                headcount=int(headcount) if headcount is not None else None,
                distance_m=int(distance_m) if distance_m is not None else None,
                popularity=int(popularity) if popularity is not None else None,
                win_odds=float(win_odds) if win_odds is not None else None,
                place_basis_odds=(
                    float(place_basis_odds) if place_basis_odds is not None else None
                ),
                edge=float(edge) if edge is not None else None,
                place_payout=payout,
                bet_profit=payout - 100.0,
            ),
        )
    return tuple(rows)


def resolve_regime_label(
    row_date: date,
    regimes: tuple[tuple[str, date | None, date | None], ...],
) -> str | None:
    for label, start_date, end_date in regimes:
        if start_date is not None and row_date < start_date:
            continue
        if end_date is not None and row_date > end_date:
            continue
        return label
    return None


def build_summary_rows(
    rows: tuple[ReferenceRegimeLabelRow, ...],
) -> tuple[ReferenceRegimeLabelSummary, ...]:
    grouped: dict[str, list[ReferenceRegimeLabelRow]] = {}
    for row in rows:
        grouped.setdefault(row.regime_label, []).append(row)
    output: list[ReferenceRegimeLabelSummary] = []
    for regime_label in sorted(grouped):
        output.append(summarize_rows(regime_label=regime_label, rows=tuple(grouped[regime_label])))
    return tuple(output)


def summarize_rows(
    *,
    regime_label: str,
    rows: tuple[ReferenceRegimeLabelRow, ...],
) -> ReferenceRegimeLabelSummary:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout > 0.0)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout for row in rows)
    total_profit = sum(row.bet_profit for row in rows)
    return ReferenceRegimeLabelSummary(
        regime_label=regime_label,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * 100.0)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.edge for row in rows)) or 0.0,
    )


def build_distribution_rows(
    rows: tuple[ReferenceRegimeLabelRow, ...],
) -> tuple[ReferenceRegimeLabelDistributionRow, ...]:
    output: list[ReferenceRegimeLabelDistributionRow] = []
    grouped: dict[str, tuple[ReferenceRegimeLabelRow, ...]] = {}
    for row in rows:
        grouped.setdefault(row.regime_label, tuple())
    for regime_label in sorted({row.regime_label for row in rows}):
        regime_rows = tuple(row for row in rows if row.regime_label == regime_label)
        output.extend(
            build_bucket_rows(
                regime_label=regime_label,
                metric="month",
                rows=regime_rows,
                bucket_fn=lambda row: row.month_label,
            ),
        )
        output.extend(
            build_bucket_rows(
                regime_label=regime_label,
                metric="headcount",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_headcount(row.headcount),
            ),
        )
        output.extend(
            build_bucket_rows(
                regime_label=regime_label,
                metric="distance",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_distance(row.distance_m),
            ),
        )
        output.extend(
            build_bucket_rows(
                regime_label=regime_label,
                metric="popularity",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_popularity(row.popularity),
            ),
        )
        output.extend(
            build_bucket_rows(
                regime_label=regime_label,
                metric="place_basis_odds",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_place_basis_odds(row.place_basis_odds),
            ),
        )
        output.extend(
            build_bucket_rows(
                regime_label=regime_label,
                metric="win_odds",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_win_odds(row.win_odds),
            ),
        )
    return tuple(output)


def build_bucket_rows(
    *,
    regime_label: str,
    metric: str,
    rows: tuple[ReferenceRegimeLabelRow, ...],
    bucket_fn: Callable[[ReferenceRegimeLabelRow], str],
) -> tuple[ReferenceRegimeLabelDistributionRow, ...]:
    grouped: dict[str, list[ReferenceRegimeLabelRow]] = {}
    for row in rows:
        grouped.setdefault(bucket_fn(row), []).append(row)
    output: list[ReferenceRegimeLabelDistributionRow] = []
    total_count = len(rows)
    for bucket_label in sorted(grouped):
        bucket_rows = tuple(grouped[bucket_label])
        hit_rows = tuple(row for row in bucket_rows if row.place_payout > 0.0)
        bet_count = len(bucket_rows)
        hit_count = len(hit_rows)
        total_return = sum(row.place_payout for row in bucket_rows)
        total_profit = sum(row.bet_profit for row in bucket_rows)
        output.append(
            ReferenceRegimeLabelDistributionRow(
                regime_label=regime_label,
                metric=metric,
                bucket_label=bucket_label,
                bet_count=bet_count,
                share=(bet_count / total_count) if total_count > 0 else 0.0,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / (bet_count * 100.0)) if bet_count > 0 else 0.0,
                total_profit=total_profit,
                avg_payout=(
                    sum(row.place_payout for row in hit_rows) / hit_count
                    if hit_count > 0
                    else 0.0
                ),
                avg_edge=average_optional_float(tuple(row.edge for row in bucket_rows)) or 0.0,
            ),
        )
    return tuple(output)


def build_stronger_rows(
    distributions: tuple[ReferenceRegimeLabelDistributionRow, ...],
    regimes: tuple[tuple[str, date | None, date | None], ...],
) -> tuple[ReferenceRegimeLabelDiffRow, ...]:
    left_regime, right_regime = regimes[0][0], regimes[1][0]
    paired = build_distribution_pairs(distributions, left_regime, right_regime)
    stronger = [
        build_diff_row(
            comparison_type="stronger_in_2024_2025",
            metric=metric,
            bucket_label=bucket_label,
            left_row=left_row,
            right_row=right_row,
        )
        for (metric, bucket_label), (left_row, right_row) in paired.items()
        if left_row.roi < 1.0 and right_row.roi > left_row.roi
    ]
    stronger.sort(
        key=lambda row: (
            row.roi_diff_right_minus_left,
            row.profit_diff_right_minus_left,
            row.right_bet_count,
        ),
        reverse=True,
    )
    return tuple(stronger[:10])


def build_weak_rows(
    distributions: tuple[ReferenceRegimeLabelDistributionRow, ...],
    regimes: tuple[tuple[str, date | None, date | None], ...],
) -> tuple[ReferenceRegimeLabelDiffRow, ...]:
    left_regime, right_regime = regimes[0][0], regimes[1][0]
    paired = build_distribution_pairs(distributions, left_regime, right_regime)
    weak = [
        build_diff_row(
            comparison_type="weak_in_both",
            metric=metric,
            bucket_label=bucket_label,
            left_row=left_row,
            right_row=right_row,
        )
        for (metric, bucket_label), (left_row, right_row) in paired.items()
        if left_row.roi < 1.0 and right_row.roi < 1.0
    ]
    weak.sort(
        key=lambda row: (
            row.left_total_profit + row.right_total_profit,
            row.left_roi + row.right_roi,
            -(row.left_bet_count + row.right_bet_count),
        ),
    )
    return tuple(weak[:10])


def build_distribution_pairs(
    distributions: tuple[ReferenceRegimeLabelDistributionRow, ...],
    left_regime: str,
    right_regime: str,
) -> dict[
    tuple[str, str],
    tuple[ReferenceRegimeLabelDistributionRow, ReferenceRegimeLabelDistributionRow],
]:
    keyed: dict[tuple[str, str, str], ReferenceRegimeLabelDistributionRow] = {
        (row.regime_label, row.metric, row.bucket_label): row for row in distributions
    }
    output: dict[
        tuple[str, str],
        tuple[ReferenceRegimeLabelDistributionRow, ReferenceRegimeLabelDistributionRow],
    ] = {}
    metric_bucket_keys = {
        (row.metric, row.bucket_label)
        for row in distributions
        if row.regime_label in {left_regime, right_regime}
    }
    for metric, bucket_label in metric_bucket_keys:
        left_row = keyed.get((left_regime, metric, bucket_label))
        right_row = keyed.get((right_regime, metric, bucket_label))
        if left_row is None or right_row is None:
            continue
        output[(metric, bucket_label)] = (left_row, right_row)
    return output


def build_diff_row(
    *,
    comparison_type: str,
    metric: str,
    bucket_label: str,
    left_row: ReferenceRegimeLabelDistributionRow,
    right_row: ReferenceRegimeLabelDistributionRow,
) -> ReferenceRegimeLabelDiffRow:
    return ReferenceRegimeLabelDiffRow(
        comparison_type=comparison_type,
        metric=metric,
        bucket_label=bucket_label,
        left_regime_label=left_row.regime_label,
        left_bet_count=left_row.bet_count,
        left_hit_rate=left_row.hit_rate,
        left_roi=left_row.roi,
        left_total_profit=left_row.total_profit,
        right_regime_label=right_row.regime_label,
        right_bet_count=right_row.bet_count,
        right_hit_rate=right_row.hit_rate,
        right_roi=right_row.roi,
        right_total_profit=right_row.total_profit,
        roi_diff_right_minus_left=right_row.roi - left_row.roi,
        profit_diff_right_minus_left=right_row.total_profit - left_row.total_profit,
    )


def build_representative_examples(
    *,
    rows: tuple[ReferenceRegimeLabelRow, ...],
    stronger_rows: tuple[ReferenceRegimeLabelDiffRow, ...],
    weak_rows: tuple[ReferenceRegimeLabelDiffRow, ...],
    examples_per_group: int,
) -> tuple[ReferenceRegimeLabelRepresentativeExampleRow, ...]:
    output: list[ReferenceRegimeLabelRepresentativeExampleRow] = []
    for diff_row in stronger_rows[:examples_per_group]:
        output.extend(
            build_examples_for_diff(
                rows=rows,
                diff_row=diff_row,
                left_sort_key=lambda row: row.bet_profit,
                right_sort_key=lambda row: (-row.bet_profit, row.result_date),
            ),
        )
    for diff_row in weak_rows[:examples_per_group]:
        output.extend(
            build_examples_for_diff(
                rows=rows,
                diff_row=diff_row,
                left_sort_key=lambda row: row.bet_profit,
                right_sort_key=lambda row: row.bet_profit,
            ),
        )
    return tuple(output)


def build_examples_for_diff(
    *,
    rows: tuple[ReferenceRegimeLabelRow, ...],
    diff_row: ReferenceRegimeLabelDiffRow,
    left_sort_key: Callable[[ReferenceRegimeLabelRow], Any],
    right_sort_key: Callable[[ReferenceRegimeLabelRow], Any],
) -> tuple[ReferenceRegimeLabelRepresentativeExampleRow, ...]:
    output: list[ReferenceRegimeLabelRepresentativeExampleRow] = []
    left_rows = tuple(
        row
        for row in rows
        if row.regime_label == diff_row.left_regime_label
        and bucket_value_for_metric(diff_row.metric, row) == diff_row.bucket_label
    )
    right_rows = tuple(
        row
        for row in rows
        if row.regime_label == diff_row.right_regime_label
        and bucket_value_for_metric(diff_row.metric, row) == diff_row.bucket_label
    )
    if left_rows:
        left_row = sorted(left_rows, key=left_sort_key)[0]
        output.append(
            build_example_row(
                comparison_type=diff_row.comparison_type,
                metric=diff_row.metric,
                bucket_label=diff_row.bucket_label,
                row=left_row,
            ),
        )
    if right_rows:
        right_row = sorted(right_rows, key=right_sort_key)[0]
        output.append(
            build_example_row(
                comparison_type=diff_row.comparison_type,
                metric=diff_row.metric,
                bucket_label=diff_row.bucket_label,
                row=right_row,
            ),
        )
    return tuple(output)


def build_example_row(
    *,
    comparison_type: str,
    metric: str,
    bucket_label: str,
    row: ReferenceRegimeLabelRow,
) -> ReferenceRegimeLabelRepresentativeExampleRow:
    return ReferenceRegimeLabelRepresentativeExampleRow(
        comparison_type=comparison_type,
        metric=metric,
        bucket_label=bucket_label,
        regime_label=row.regime_label,
        result_date=row.result_date,
        window_label=row.window_label,
        race_key=row.race_key,
        horse_number=row.horse_number,
        finish_position=row.finish_position,
        month_label=row.month_label,
        headcount=row.headcount,
        distance_m=row.distance_m,
        popularity=row.popularity,
        win_odds=row.win_odds,
        place_basis_odds=row.place_basis_odds,
        edge=row.edge,
        place_payout=row.place_payout,
        bet_profit=row.bet_profit,
    )


def bucket_value_for_metric(metric: str, row: ReferenceRegimeLabelRow) -> str:
    if metric == "month":
        return row.month_label
    if metric == "headcount":
        return bucket_headcount(row.headcount)
    if metric == "distance":
        return bucket_distance(row.distance_m)
    if metric == "popularity":
        return bucket_popularity(row.popularity)
    if metric == "place_basis_odds":
        return bucket_place_basis_odds(row.place_basis_odds)
    if metric == "win_odds":
        return bucket_win_odds(row.win_odds)
    raise ValueError(f"unsupported metric: {metric}")


def bucket_headcount(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value < 8:
        return "lt_8"
    if value <= 11:
        return "8_11"
    if value <= 15:
        return "12_15"
    return "16_plus"


def bucket_distance(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value < 1400:
        return "lt_1400"
    if value < 1800:
        return "1400_1799"
    if value < 2200:
        return "1800_2199"
    return "2200_plus"


def bucket_popularity(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value <= 2:
        return "1_2"
    if value <= 4:
        return "3_4"
    if value <= 6:
        return "5_6"
    return "7_plus"


def bucket_place_basis_odds(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 2.0:
        return "lt_2_0"
    if value < 2.4:
        return "2_0_2_4"
    if value < 2.8:
        return "2_4_2_8"
    if value < 3.2:
        return "2_8_3_2"
    if value < 4.0:
        return "3_2_4_0"
    return "4_0_plus"


def bucket_win_odds(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 5.0:
        return "lt_5"
    if value < 10.0:
        return "5_10"
    if value < 20.0:
        return "10_20"
    return "20_plus"
