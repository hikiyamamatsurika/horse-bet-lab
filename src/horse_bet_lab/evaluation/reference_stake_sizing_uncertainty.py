from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.config import (
    ReferenceStakeSizingCompareConfig,
    ReferenceStakeSizingUncertaintyConfig,
    load_reference_stake_sizing_compare_config,
)
from horse_bet_lab.evaluation.reference_stake_sizing_compare import (
    _SizedBet,
    build_sized_rows,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.reference_uncertainty import sample_block_bootstrap


@dataclass(frozen=True)
class StakeSizingUncertaintySummary:
    stake_variant: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    avg_stake: float
    total_stake: float
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
class StakeSizingBootstrapRow:
    stake_variant: str
    iteration: int
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class StakeSizingYearContributionRow:
    stake_variant: str
    year: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    avg_stake: float
    total_stake: float


@dataclass(frozen=True)
class ReferenceStakeSizingUncertaintyResult:
    output_dir: Path
    summaries: tuple[StakeSizingUncertaintySummary, ...]
    bootstrap_rows: tuple[StakeSizingBootstrapRow, ...]
    yearly_contributions: tuple[StakeSizingYearContributionRow, ...]


@dataclass(frozen=True)
class _SummaryStats:
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    avg_stake: float
    total_stake: float
    max_drawdown: float
    max_losing_streak: int


def run_reference_stake_sizing_uncertainty(
    config: ReferenceStakeSizingUncertaintyConfig,
) -> ReferenceStakeSizingUncertaintyResult:
    compare_config = load_reference_stake_sizing_compare_config(
        config.reference_stake_sizing_compare_config_path,
    )

    summaries: list[StakeSizingUncertaintySummary] = []
    bootstrap_rows: list[StakeSizingBootstrapRow] = []
    yearly_contributions: list[StakeSizingYearContributionRow] = []

    sized_rows_by_variant = rebuild_rows_by_variant(compare_config)
    for stake_variant in compare_config.stake_variants:
        rows = sized_rows_by_variant[stake_variant]
        actual = summarize_rows(rows)
        variant_bootstrap_rows = build_bootstrap_rows(
            stake_variant=stake_variant,
            rows=rows,
            iterations=config.bootstrap_iterations,
            random_seed=config.random_seed,
            block_unit=config.bootstrap_block_unit,
        )
        roi_values = [row.roi for row in variant_bootstrap_rows]
        profit_values = [row.total_profit for row in variant_bootstrap_rows]
        dd_values = [row.max_drawdown for row in variant_bootstrap_rows]
        summaries.append(
            StakeSizingUncertaintySummary(
                stake_variant=stake_variant,
                bet_count=actual.bet_count,
                hit_count=actual.hit_count,
                hit_rate=actual.hit_rate,
                roi=actual.roi,
                total_profit=actual.total_profit,
                avg_payout=actual.avg_payout,
                avg_edge=actual.avg_edge,
                avg_stake=actual.avg_stake,
                total_stake=actual.total_stake,
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
            ),
        )
        bootstrap_rows.extend(variant_bootstrap_rows)
        yearly_contributions.extend(build_year_contributions(stake_variant, rows))

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceStakeSizingUncertaintyResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        bootstrap_rows=tuple(bootstrap_rows),
        yearly_contributions=tuple(yearly_contributions),
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "bootstrap_distribution.csv", result.bootstrap_rows)
    write_json(
        config.output_dir / "bootstrap_distribution.json",
        {"analysis": {"rows": result.bootstrap_rows}},
    )
    write_csv(config.output_dir / "yearly_contribution.csv", result.yearly_contributions)
    write_json(
        config.output_dir / "yearly_contribution.json",
        {"analysis": {"rows": result.yearly_contributions}},
    )
    return result


def rebuild_rows_by_variant(
    config: ReferenceStakeSizingCompareConfig,
) -> dict[str, tuple[_SizedBet, ...]]:
    from horse_bet_lab.config import load_reference_label_guard_compare_config
    from horse_bet_lab.evaluation.reference_label_guard_compare import (
        run_reference_label_guard_compare,
    )

    compare_config = load_reference_label_guard_compare_config(
        config.reference_label_guard_compare_config_path,
    )
    compare_result = run_reference_label_guard_compare(compare_config)
    base_rows = compare_result.selected_test_rows
    return {
        stake_variant: build_sized_rows(
            stake_variant=stake_variant,
            rows=base_rows,
            config=config,
        )
        for stake_variant in config.stake_variants
    }


def summarize_rows(rows: tuple[_SizedBet, ...]) -> _SummaryStats:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.row.row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.scaled_return for row in rows)
    total_stake = sum(row.stake for row in rows)
    total_profit = sum(row.bet_profit for row in rows)
    cumulative_profit = 0.0
    peak = 0.0
    max_drawdown = 0.0
    current_losing_streak = 0
    max_losing_streak = 0
    for item in rows:
        cumulative_profit += item.bet_profit
        peak = max(peak, cumulative_profit)
        max_drawdown = max(max_drawdown, peak - cumulative_profit)
        if item.bet_profit < 0:
            current_losing_streak += 1
            max_losing_streak = max(max_losing_streak, current_losing_streak)
        else:
            current_losing_streak = 0
    return _SummaryStats(
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / total_stake) if total_stake > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.scaled_return for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=(
            sum((item.row.row.edge or 0.0) for item in rows) / bet_count if bet_count > 0 else 0.0
        ),
        avg_stake=(total_stake / bet_count) if bet_count > 0 else 0.0,
        total_stake=total_stake,
        max_drawdown=max_drawdown,
        max_losing_streak=max_losing_streak,
    )


def build_bootstrap_rows(
    *,
    stake_variant: str,
    rows: tuple[_SizedBet, ...],
    iterations: int,
    random_seed: int,
    block_unit: str = "race_date",
) -> list[StakeSizingBootstrapRow]:
    rng = random.Random(random_seed)
    output: list[StakeSizingBootstrapRow] = []
    if not rows:
        return output
    if block_unit == "race_date":
        def block_key_fn(item: _SizedBet) -> str:
            return item.row.row.result_date.isoformat()
    elif block_unit == "race_key":
        def block_key_fn(item: _SizedBet) -> str:
            return item.row.row.race_key
    else:
        raise ValueError(f"Unsupported bootstrap block unit: {block_unit}")
    for index in range(1, iterations + 1):
        sampled = sample_block_bootstrap(
            rows,
            rng=rng,
            block_key_fn=block_key_fn,
            sort_key_fn=lambda item: (
                item.row.row.result_date,
                item.row.window_label,
                item.row.row.race_key,
                item.row.row.horse_number,
            ),
        )
        stats = summarize_rows(sampled)
        output.append(
            StakeSizingBootstrapRow(
                stake_variant=stake_variant,
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


def build_year_contributions(
    stake_variant: str,
    rows: tuple[_SizedBet, ...],
) -> tuple[StakeSizingYearContributionRow, ...]:
    grouped: dict[str, list[_SizedBet]] = {}
    for row in rows:
        grouped.setdefault(row.row.row.result_date.isoformat()[:4], []).append(row)
    output: list[StakeSizingYearContributionRow] = []
    for year in sorted(grouped):
        stats = summarize_rows(tuple(grouped[year]))
        output.append(
            StakeSizingYearContributionRow(
                stake_variant=stake_variant,
                year=year,
                bet_count=stats.bet_count,
                hit_count=stats.hit_count,
                hit_rate=stats.hit_rate,
                roi=stats.roi,
                total_profit=stats.total_profit,
                avg_payout=stats.avg_payout,
                avg_edge=stats.avg_edge,
                avg_stake=stats.avg_stake,
                total_stake=stats.total_stake,
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
