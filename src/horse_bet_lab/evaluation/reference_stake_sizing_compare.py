from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from horse_bet_lab.config import (
    ReferenceStakeSizingCompareConfig,
    load_reference_label_guard_compare_config,
)
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_label_guard_compare import (
    run_reference_label_guard_compare,
)
from horse_bet_lab.evaluation.reference_strategy import (
    average_optional_float,
    write_csv,
    write_json,
)


@dataclass(frozen=True)
class StakeSizingSummary:
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


@dataclass(frozen=True)
class StakeSizingGroupSummary:
    stake_variant: str
    group_key: str
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


@dataclass(frozen=True)
class StakeSizingEquityRow:
    stake_variant: str
    sequence: int
    result_date: str
    window_label: str
    race_key: str
    horse_number: int
    stake: float
    scaled_return: float
    bet_profit: float
    cumulative_profit: float
    drawdown: float
    edge: float | None
    pred_probability: float
    market_prob: float | None
    win_odds: float | None
    place_basis_odds: float | None
    popularity: int | None


@dataclass(frozen=True)
class ReferenceStakeSizingCompareResult:
    output_dir: Path
    summaries: tuple[StakeSizingSummary, ...]
    yearly_summaries: tuple[StakeSizingGroupSummary, ...]
    monthly_summaries: tuple[StakeSizingGroupSummary, ...]
    window_summaries: tuple[StakeSizingGroupSummary, ...]
    equity_rows: tuple[StakeSizingEquityRow, ...]


@dataclass(frozen=True)
class _SizedBet:
    stake_variant: str
    window_label: str
    row: GuardVariantRow
    stake: float
    scaled_return: float
    bet_profit: float


def run_reference_stake_sizing_compare(
    config: ReferenceStakeSizingCompareConfig,
) -> ReferenceStakeSizingCompareResult:
    compare_config = load_reference_label_guard_compare_config(
        config.reference_label_guard_compare_config_path,
    )
    compare_result = run_reference_label_guard_compare(compare_config)
    base_rows = compare_result.selected_test_rows

    summaries: list[StakeSizingSummary] = []
    yearly_summaries: list[StakeSizingGroupSummary] = []
    monthly_summaries: list[StakeSizingGroupSummary] = []
    window_summaries: list[StakeSizingGroupSummary] = []
    equity_rows: list[StakeSizingEquityRow] = []

    for stake_variant in config.stake_variants:
        sized_rows = build_sized_rows(stake_variant=stake_variant, rows=base_rows, config=config)
        summary, variant_equity_rows = summarize_variant(stake_variant, sized_rows)
        summaries.append(summary)
        equity_rows.extend(variant_equity_rows)
        yearly_summaries.extend(
            build_group_summaries(
                stake_variant=stake_variant,
                rows=sized_rows,
                key_fn=lambda item: item.row.row.result_date.isoformat()[:4],
            ),
        )
        monthly_summaries.extend(
            build_group_summaries(
                stake_variant=stake_variant,
                rows=sized_rows,
                key_fn=lambda item: item.row.row.result_date.isoformat()[:7],
            ),
        )
        window_summaries.extend(
            build_group_summaries(
                stake_variant=stake_variant,
                rows=sized_rows,
                key_fn=lambda item: item.window_label,
            ),
        )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceStakeSizingCompareResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        yearly_summaries=tuple(yearly_summaries),
        monthly_summaries=tuple(monthly_summaries),
        window_summaries=tuple(window_summaries),
        equity_rows=tuple(equity_rows),
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "yearly_summary.csv", result.yearly_summaries)
    write_json(
        config.output_dir / "yearly_summary.json",
        {"analysis": {"rows": result.yearly_summaries}},
    )
    write_csv(config.output_dir / "monthly_profit.csv", result.monthly_summaries)
    write_json(
        config.output_dir / "monthly_profit.json",
        {"analysis": {"rows": result.monthly_summaries}},
    )
    write_csv(config.output_dir / "window_profit.csv", result.window_summaries)
    write_json(
        config.output_dir / "window_profit.json",
        {"analysis": {"rows": result.window_summaries}},
    )
    write_csv(config.output_dir / "equity_curve.csv", result.equity_rows)
    write_json(
        config.output_dir / "equity_curve.json",
        {"analysis": {"rows": result.equity_rows}},
    )
    return result


def build_sized_rows(
    *,
    stake_variant: str,
    rows: tuple[GuardVariantRow, ...],
    config: ReferenceStakeSizingCompareConfig,
) -> tuple[_SizedBet, ...]:
    ordered_rows = tuple(
        sorted(
            rows,
            key=lambda item: (
                item.row.result_date,
                item.window_label,
                item.row.race_key,
                item.row.horse_number,
            ),
        ),
    )
    output: list[_SizedBet] = []
    cumulative_profit = 0.0
    running_peak = 0.0
    race_stake_totals: dict[tuple[str, str], float] = {}
    day_stake_totals: dict[str, float] = {}
    for item in ordered_rows:
        stake = compute_stake(stake_variant=stake_variant, row=item, config=config)
        result_date = item.row.result_date.isoformat()
        race_key = (result_date, item.row.race_key)
        race_stake_totals.setdefault(race_key, 0.0)
        day_stake_totals.setdefault(result_date, 0.0)

        if stake_variant == "capped_fractional_kelly_like_per_race_cap":
            available = config.per_race_cap_stake - race_stake_totals[race_key]
            stake = round_to_stake_unit_or_zero(min(stake, available))
        if stake_variant == "capped_fractional_kelly_like_per_day_cap":
            available = config.per_day_cap_stake - day_stake_totals[result_date]
            stake = round_to_stake_unit_or_zero(min(stake, available))
        if stake_variant == "capped_fractional_kelly_like_drawdown_reduction":
            drawdown = max(0.0, running_peak - cumulative_profit)
            if drawdown >= config.drawdown_reduction_threshold:
                stake = round_to_stake_unit(
                    max(100.0, stake * config.drawdown_reduction_factor),
                )

        if stake <= 0.0:
            continue
        payout = item.row.place_payout or 0.0
        scaled_return = payout * (stake / 100.0)
        bet_profit = scaled_return - stake
        cumulative_profit += bet_profit
        running_peak = max(running_peak, cumulative_profit)
        race_stake_totals[race_key] += stake
        day_stake_totals[result_date] += stake
        output.append(
            _SizedBet(
                stake_variant=stake_variant,
                window_label=item.window_label,
                row=item,
                stake=stake,
                scaled_return=scaled_return,
                bet_profit=bet_profit,
            ),
        )
    return tuple(output)


def compute_stake(
    *,
    stake_variant: str,
    row: GuardVariantRow,
    config: ReferenceStakeSizingCompareConfig,
) -> float:
    if stake_variant == "flat_100":
        return 100.0
    if stake_variant == "flat_200":
        return 200.0
    if stake_variant == "edge_proportional_small":
        edge = max(row.row.edge or 0.0, 0.0)
        steps = int(edge / config.edge_small_step_edge)
        stake = config.edge_small_base_stake + (steps * config.edge_small_step_stake)
        return min(config.edge_small_max_stake, max(100.0, stake))
    if stake_variant == "capped_fractional_kelly_like":
        place_basis_odds = row.row.place_basis_odds
        pred_probability = row.row.pred_probability
        if place_basis_odds is None or place_basis_odds <= 1.0:
            return 100.0
        b_value = max(place_basis_odds - 1.0, 0.01)
        q_value = 1.0 - pred_probability
        kelly_fraction = max(0.0, ((b_value * pred_probability) - q_value) / b_value)
        raw_stake = config.kelly_bankroll * config.kelly_fraction * kelly_fraction
        return round_to_stake_unit(max(100.0, min(config.kelly_cap_stake, raw_stake)))
    if stake_variant in {
        "capped_fractional_kelly_like_per_race_cap",
        "capped_fractional_kelly_like_per_day_cap",
        "capped_fractional_kelly_like_drawdown_reduction",
    }:
        return compute_stake(
            stake_variant="capped_fractional_kelly_like",
            row=row,
            config=config,
        )
    raise ValueError(f"Unsupported stake_variant: {stake_variant}")


def round_to_stake_unit(value: float) -> float:
    return float(max(100, int(math.floor(value / 100.0)) * 100))


def round_to_stake_unit_or_zero(value: float) -> float:
    if value < 100.0:
        return 0.0
    return float(int(math.floor(value / 100.0)) * 100)


def summarize_variant(
    stake_variant: str,
    rows: tuple[_SizedBet, ...],
) -> tuple[StakeSizingSummary, tuple[StakeSizingEquityRow, ...]]:
    equity_rows, max_drawdown, max_losing_streak = build_equity_curve(rows)
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.row.row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.scaled_return for row in rows)
    total_stake = sum(row.stake for row in rows)
    total_profit = sum(row.bet_profit for row in rows)
    summary = StakeSizingSummary(
        stake_variant=stake_variant,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / total_stake) if total_stake > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.scaled_return for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.row.row.edge for row in rows)) or 0.0,
        avg_stake=(total_stake / bet_count) if bet_count > 0 else 0.0,
        total_stake=total_stake,
        max_drawdown=max_drawdown,
        max_losing_streak=max_losing_streak,
    )
    return summary, equity_rows


def build_equity_curve(
    rows: tuple[_SizedBet, ...],
) -> tuple[tuple[StakeSizingEquityRow, ...], float, int]:
    cumulative_profit = 0.0
    running_peak = 0.0
    max_drawdown = 0.0
    current_losing_streak = 0
    max_losing_streak = 0
    equity_rows: list[StakeSizingEquityRow] = []
    for index, item in enumerate(rows, start=1):
        cumulative_profit += item.bet_profit
        running_peak = max(running_peak, cumulative_profit)
        drawdown = running_peak - cumulative_profit
        max_drawdown = max(max_drawdown, drawdown)
        if item.bet_profit < 0:
            current_losing_streak += 1
            max_losing_streak = max(max_losing_streak, current_losing_streak)
        else:
            current_losing_streak = 0
        equity_rows.append(
            StakeSizingEquityRow(
                stake_variant=item.stake_variant,
                sequence=index,
                result_date=item.row.row.result_date.isoformat(),
                window_label=item.window_label,
                race_key=item.row.row.race_key,
                horse_number=item.row.row.horse_number,
                stake=item.stake,
                scaled_return=item.scaled_return,
                bet_profit=item.bet_profit,
                cumulative_profit=cumulative_profit,
                drawdown=drawdown,
                edge=item.row.row.edge,
                pred_probability=item.row.row.pred_probability,
                market_prob=item.row.row.market_prob,
                win_odds=item.row.row.win_odds,
                place_basis_odds=item.row.row.place_basis_odds,
                popularity=item.row.row.popularity,
            ),
        )
    return tuple(equity_rows), max_drawdown, max_losing_streak


def build_group_summaries(
    *,
    stake_variant: str,
    rows: tuple[_SizedBet, ...],
    key_fn: Callable[[_SizedBet], str],
) -> tuple[StakeSizingGroupSummary, ...]:
    grouped: dict[str, list[_SizedBet]] = {}
    for item in rows:
        group_key = str(key_fn(item))
        grouped.setdefault(group_key, []).append(item)
    output: list[StakeSizingGroupSummary] = []
    for group_key in sorted(grouped):
        group_rows = tuple(
            sorted(
                grouped[group_key],
                key=lambda item: (
                    item.row.row.result_date,
                    item.window_label,
                    item.row.row.race_key,
                    item.row.row.horse_number,
                ),
            ),
        )
        _, max_drawdown, max_losing_streak = build_equity_curve(group_rows)
        bet_count = len(group_rows)
        hit_rows = tuple(row for row in group_rows if row.row.row.place_payout is not None)
        hit_count = len(hit_rows)
        total_return = sum(row.scaled_return for row in group_rows)
        total_stake = sum(row.stake for row in group_rows)
        total_profit = sum(row.bet_profit for row in group_rows)
        output.append(
            StakeSizingGroupSummary(
                stake_variant=stake_variant,
                group_key=group_key,
                bet_count=bet_count,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / total_stake) if total_stake > 0 else 0.0,
                total_profit=total_profit,
                avg_payout=(
                    sum(row.scaled_return for row in hit_rows) / hit_count if hit_count > 0 else 0.0
                ),
                avg_edge=average_optional_float(tuple(row.row.row.edge for row in group_rows))
                or 0.0,
                avg_stake=(total_stake / bet_count) if bet_count > 0 else 0.0,
                total_stake=total_stake,
                max_drawdown=max_drawdown,
                max_losing_streak=max_losing_streak,
            ),
        )
    return tuple(output)
