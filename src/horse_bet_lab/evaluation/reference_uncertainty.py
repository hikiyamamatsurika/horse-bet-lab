from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable, TypeVar

from horse_bet_lab.config import (
    ReferenceUncertaintyConfig,
    load_second_guard_selection_config,
)
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.second_guard_selection import run_second_guard_selection

T = TypeVar("T")


@dataclass(frozen=True)
class ReferenceUncertaintySummary:
    strategy_name: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    max_drawdown: float
    max_losing_streak: int
    bootstrap_iterations: int
    roi_p02_5: float
    roi_p50: float
    roi_p97_5: float
    roi_gt_1_ratio: float
    total_profit_p02_5: float
    total_profit_p50: float
    total_profit_p97_5: float
    max_drawdown_p02_5: float
    max_drawdown_p50: float
    max_drawdown_p97_5: float


@dataclass(frozen=True)
class ReferenceBootstrapRow:
    iteration: int
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class ReferenceYearContributionRow:
    year: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class ReferenceUncertaintyResult:
    output_dir: Path
    summary: ReferenceUncertaintySummary
    bootstrap_rows: tuple[ReferenceBootstrapRow, ...]
    year_contributions: tuple[ReferenceYearContributionRow, ...]


def run_reference_uncertainty(
    config: ReferenceUncertaintyConfig,
) -> ReferenceUncertaintyResult:
    selection_config = load_second_guard_selection_config(
        config.second_guard_selection_config_path,
    )
    selection_result = run_second_guard_selection(selection_config)
    rows = selection_result.selected_test_rows

    actual = summarize_rows(rows)
    bootstrap_rows = build_bootstrap_rows(
        rows=rows,
        iterations=config.bootstrap_iterations,
        random_seed=config.random_seed,
        block_unit=config.bootstrap_block_unit,
    )
    roi_values = [row.roi for row in bootstrap_rows]
    profit_values = [row.total_profit for row in bootstrap_rows]
    dd_values = [row.max_drawdown for row in bootstrap_rows]
    summary = ReferenceUncertaintySummary(
        strategy_name="reference_first_guard_plus_valid_selected_second_guard",
        bet_count=actual.bet_count,
        hit_count=actual.hit_count,
        hit_rate=actual.hit_rate,
        roi=actual.roi,
        total_profit=actual.total_profit,
        avg_payout=actual.avg_payout,
        avg_edge=actual.avg_edge,
        max_drawdown=actual.max_drawdown,
        max_losing_streak=actual.max_losing_streak,
        bootstrap_iterations=config.bootstrap_iterations,
        roi_p02_5=quantile(roi_values, 0.025),
        roi_p50=quantile(roi_values, 0.5),
        roi_p97_5=quantile(roi_values, 0.975),
        roi_gt_1_ratio=(
            sum(1 for value in roi_values if value > 1.0) / len(roi_values)
            if roi_values
            else 0.0
        ),
        total_profit_p02_5=quantile(profit_values, 0.025),
        total_profit_p50=quantile(profit_values, 0.5),
        total_profit_p97_5=quantile(profit_values, 0.975),
        max_drawdown_p02_5=quantile(dd_values, 0.025),
        max_drawdown_p50=quantile(dd_values, 0.5),
        max_drawdown_p97_5=quantile(dd_values, 0.975),
    )
    year_contributions = build_year_contributions(rows)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceUncertaintyResult(
        output_dir=config.output_dir,
        summary=summary,
        bootstrap_rows=tuple(bootstrap_rows),
        year_contributions=year_contributions,
    )
    write_csv(config.output_dir / "summary.csv", (result.summary,))
    write_json(config.output_dir / "summary.json", {"analysis": result.summary})
    write_csv(config.output_dir / "bootstrap_distribution.csv", result.bootstrap_rows)
    write_json(
        config.output_dir / "bootstrap_distribution.json",
        {"analysis": {"rows": result.bootstrap_rows}},
    )
    write_csv(config.output_dir / "yearly_contribution.csv", result.year_contributions)
    write_json(
        config.output_dir / "yearly_contribution.json",
        {"analysis": {"rows": result.year_contributions}},
    )
    return result


@dataclass(frozen=True)
class _SummaryStats:
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    max_drawdown: float
    max_losing_streak: int


def summarize_rows(rows: tuple[GuardVariantRow, ...]) -> _SummaryStats:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * 100.0)
    cumulative_profit = 0.0
    peak = 0.0
    max_drawdown = 0.0
    current_losing_streak = 0
    max_losing_streak = 0
    for item in rows:
        profit = (item.row.place_payout or 0.0) - 100.0
        cumulative_profit += profit
        peak = max(peak, cumulative_profit)
        max_drawdown = max(max_drawdown, peak - cumulative_profit)
        if profit < 0:
            current_losing_streak += 1
            max_losing_streak = max(max_losing_streak, current_losing_streak)
        else:
            current_losing_streak = 0
    return _SummaryStats(
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * 100.0)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(item.row.place_payout or 0.0 for item in hit_rows) / hit_count
            if hit_count > 0
            else 0.0
        ),
        avg_edge=(
            sum((item.row.edge or 0.0) for item in rows) / bet_count if bet_count > 0 else 0.0
        ),
        max_drawdown=max_drawdown,
        max_losing_streak=max_losing_streak,
    )


def build_bootstrap_rows(
    *,
    rows: tuple[GuardVariantRow, ...],
    iterations: int,
    random_seed: int,
    block_unit: str = "race_date",
) -> list[ReferenceBootstrapRow]:
    rng = random.Random(random_seed)
    output: list[ReferenceBootstrapRow] = []
    if not rows:
        return output
    block_key_fn = build_guard_variant_block_key_fn(block_unit)

    def sort_key_fn(item: GuardVariantRow) -> tuple[date, str, str, int]:
        return (
        item.row.result_date,
        item.window_label,
        item.row.race_key,
        item.row.horse_number,
        )

    for index in range(1, iterations + 1):
        sampled = sample_block_bootstrap(
            rows,
            rng=rng,
            block_key_fn=block_key_fn,
            sort_key_fn=sort_key_fn,
        )
        stats = summarize_rows(sampled)
        output.append(
            ReferenceBootstrapRow(
                iteration=index,
                bet_count=stats.bet_count,
                hit_count=stats.hit_count,
                hit_rate=stats.hit_rate,
                roi=stats.roi,
                total_profit=stats.total_profit,
                max_drawdown=stats.max_drawdown,
                max_losing_streak=stats.max_losing_streak,
            ),
        )
    return output


def build_date_block_key(block_unit: str, value: date) -> str:
    if block_unit == "race_date":
        return value.isoformat()
    if block_unit == "week":
        iso_year, iso_week, _ = value.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    if block_unit == "month":
        return f"{value.year:04d}-{value.month:02d}"
    raise ValueError(f"Unsupported bootstrap block unit: {block_unit}")


def build_guard_variant_block_key_fn(
    block_unit: str,
) -> Callable[[GuardVariantRow], str]:
    if block_unit in {"race_date", "week", "month"}:
        return lambda item: build_date_block_key(block_unit, item.row.result_date)
    if block_unit == "race_key":
        return lambda item: item.row.race_key
    raise ValueError(f"Unsupported bootstrap block unit: {block_unit}")


def sample_block_bootstrap(
    rows: tuple[T, ...],
    *,
    rng: random.Random,
    block_key_fn: Callable[[T], str],
    sort_key_fn: Callable[[T], tuple[object, ...]],
) -> tuple[T, ...]:
    blocks = build_ordered_blocks(rows, block_key_fn)
    if not blocks:
        return ()
    sampled_blocks = [rng.choice(blocks) for _ in range(len(blocks))]
    sampled_rows = [row for block in sampled_blocks for row in block]
    return tuple(sorted(sampled_rows, key=sort_key_fn))


def build_ordered_blocks(
    rows: tuple[T, ...],
    block_key_fn: Callable[[T], str],
) -> tuple[tuple[T, ...], ...]:
    grouped: dict[str, list[T]] = {}
    order: list[str] = []
    for row in rows:
        key = block_key_fn(row)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(row)
    return tuple(tuple(grouped[key]) for key in order)


def build_year_contributions(
    rows: tuple[GuardVariantRow, ...],
) -> tuple[ReferenceYearContributionRow, ...]:
    grouped: dict[str, list[GuardVariantRow]] = {}
    for row in rows:
        grouped.setdefault(row.row.result_date.isoformat()[:4], []).append(row)
    output: list[ReferenceYearContributionRow] = []
    for year in sorted(grouped):
        stats = summarize_rows(tuple(grouped[year]))
        output.append(
            ReferenceYearContributionRow(
                year=year,
                bet_count=stats.bet_count,
                hit_count=stats.hit_count,
                hit_rate=stats.hit_rate,
                roi=stats.roi,
                total_profit=stats.total_profit,
                avg_payout=stats.avg_payout,
                avg_edge=stats.avg_edge,
            ),
        )
    return tuple(output)


def quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight
