from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from horse_bet_lab.config import (
    ReferenceBankrollSimulationConfig,
    load_reference_label_guard_compare_config,
)
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_label_guard_compare import (
    run_reference_label_guard_compare,
)
from horse_bet_lab.evaluation.reference_stake_sizing_compare import (
    round_to_stake_unit_or_zero,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json


@dataclass(frozen=True)
class BankrollSimulationSummary:
    stake_variant: str
    initial_bankroll: float
    final_bankroll: float
    cumulative_profit: float
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
class BankrollPathRow:
    stake_variant: str
    initial_bankroll: float
    sequence: int
    result_date: str
    window_label: str
    race_key: str
    horse_number: int
    bankroll_before: float
    stake: float
    scaled_return: float
    bet_profit: float
    bankroll_after: float
    drawdown: float
    edge: float | None
    pred_probability: float
    market_prob: float | None
    win_odds: float | None
    place_basis_odds: float | None
    popularity: int | None


@dataclass(frozen=True)
class BankrollGroupSummary:
    stake_variant: str
    initial_bankroll: float
    group_key: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    bankroll_change: float
    avg_payout: float
    avg_edge: float
    avg_stake: float
    total_stake: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class BankrollBootstrapRow:
    stake_variant: str
    initial_bankroll: float
    iteration: int
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class ReferenceBankrollSimulationResult:
    output_dir: Path
    summaries: tuple[BankrollSimulationSummary, ...]
    yearly_summaries: tuple[BankrollGroupSummary, ...]
    monthly_summaries: tuple[BankrollGroupSummary, ...]
    equity_rows: tuple[BankrollPathRow, ...]
    bootstrap_rows: tuple[BankrollBootstrapRow, ...]


@dataclass(frozen=True)
class _PlacedBet:
    stake_variant: str
    initial_bankroll: float
    window_label: str
    row: GuardVariantRow
    bankroll_before: float
    stake: float
    scaled_return: float
    bet_profit: float
    bankroll_after: float
    drawdown: float


@dataclass(frozen=True)
class _Stats:
    final_bankroll: float
    cumulative_profit: float
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


def run_reference_bankroll_simulation(
    config: ReferenceBankrollSimulationConfig,
) -> ReferenceBankrollSimulationResult:
    base_rows = load_selected_reference_rows(config)

    summaries: list[BankrollSimulationSummary] = []
    yearly_summaries: list[BankrollGroupSummary] = []
    monthly_summaries: list[BankrollGroupSummary] = []
    equity_rows: list[BankrollPathRow] = []
    bootstrap_rows: list[BankrollBootstrapRow] = []

    for initial_bankroll in config.initial_bankrolls:
        for stake_variant in config.stake_variants:
            placed_rows = simulate_rows(
                rows=base_rows,
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                config=config,
            )
            stats = summarize_rows(initial_bankroll=initial_bankroll, rows=placed_rows)
            variant_bootstrap = build_bootstrap_rows(
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                rows=base_rows,
                config=config,
            )
            roi_values = [row.roi for row in variant_bootstrap]
            profit_values = [row.total_profit for row in variant_bootstrap]
            dd_values = [row.max_drawdown for row in variant_bootstrap]
            summaries.append(
                BankrollSimulationSummary(
                    stake_variant=stake_variant,
                    initial_bankroll=initial_bankroll,
                    final_bankroll=stats.final_bankroll,
                    cumulative_profit=stats.cumulative_profit,
                    bet_count=stats.bet_count,
                    hit_count=stats.hit_count,
                    hit_rate=stats.hit_rate,
                    roi=stats.roi,
                    total_profit=stats.total_profit,
                    avg_payout=stats.avg_payout,
                    avg_edge=stats.avg_edge,
                    avg_stake=stats.avg_stake,
                    total_stake=stats.total_stake,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
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
            yearly_summaries.extend(
                build_group_summaries(
                    stake_variant=stake_variant,
                    initial_bankroll=initial_bankroll,
                    rows=placed_rows,
                    key_fn=lambda item: item.row.row.result_date.isoformat()[:4],
                ),
            )
            monthly_summaries.extend(
                build_group_summaries(
                    stake_variant=stake_variant,
                    initial_bankroll=initial_bankroll,
                    rows=placed_rows,
                    key_fn=lambda item: item.row.row.result_date.isoformat()[:7],
                ),
            )
            equity_rows.extend(
                build_bankroll_path_rows(
                    stake_variant=stake_variant,
                    initial_bankroll=initial_bankroll,
                    rows=placed_rows,
                ),
            )
            bootstrap_rows.extend(variant_bootstrap)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceBankrollSimulationResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        yearly_summaries=tuple(yearly_summaries),
        monthly_summaries=tuple(monthly_summaries),
        equity_rows=tuple(equity_rows),
        bootstrap_rows=tuple(bootstrap_rows),
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "yearly_profit.csv", result.yearly_summaries)
    write_json(
        config.output_dir / "yearly_profit.json",
        {"analysis": {"rows": result.yearly_summaries}},
    )
    write_csv(config.output_dir / "monthly_profit.csv", result.monthly_summaries)
    write_json(
        config.output_dir / "monthly_profit.json",
        {"analysis": {"rows": result.monthly_summaries}},
    )
    write_csv(config.output_dir / "equity_curve.csv", result.equity_rows)
    write_json(
        config.output_dir / "equity_curve.json",
        {"analysis": {"rows": result.equity_rows}},
    )
    write_csv(config.output_dir / "bankroll_path.csv", result.equity_rows)
    write_json(
        config.output_dir / "bankroll_path.json",
        {"analysis": {"rows": result.equity_rows}},
    )
    write_csv(config.output_dir / "bootstrap_distribution.csv", result.bootstrap_rows)
    write_json(
        config.output_dir / "bootstrap_distribution.json",
        {"analysis": {"rows": result.bootstrap_rows}},
    )
    return result


def load_selected_reference_rows(
    config: ReferenceBankrollSimulationConfig,
) -> tuple[GuardVariantRow, ...]:
    compare_config = load_reference_label_guard_compare_config(
        config.reference_label_guard_compare_config_path,
    )
    compare_result = run_reference_label_guard_compare(compare_config)
    return tuple(
        sorted(
            compare_result.selected_test_rows,
            key=lambda item: (
                item.row.result_date,
                item.window_label,
                item.row.race_key,
                item.row.horse_number,
            ),
        ),
    )


def simulate_rows(
    *,
    rows: tuple[GuardVariantRow, ...],
    stake_variant: str,
    initial_bankroll: float,
    config: ReferenceBankrollSimulationConfig,
) -> tuple[_PlacedBet, ...]:
    bankroll = initial_bankroll
    running_peak = initial_bankroll
    race_stake_totals: dict[tuple[str, str], float] = {}
    day_stake_totals: dict[str, float] = {}
    output: list[_PlacedBet] = []
    for item in rows:
        result_date = item.row.result_date.isoformat()
        race_key = (result_date, item.row.race_key)
        race_stake_totals.setdefault(race_key, 0.0)
        day_stake_totals.setdefault(result_date, 0.0)
        drawdown = max(0.0, running_peak - bankroll)
        stake = compute_stateful_stake(
            stake_variant=stake_variant,
            row=item,
            bankroll=bankroll,
            current_drawdown=drawdown,
            race_spent=race_stake_totals[race_key],
            day_spent=day_stake_totals[result_date],
            config=config,
        )
        if stake <= 0.0 or bankroll < stake:
            continue
        payout = item.row.place_payout or 0.0
        scaled_return = payout * (stake / 100.0)
        bet_profit = scaled_return - stake
        bankroll_before = bankroll
        bankroll += bet_profit
        running_peak = max(running_peak, bankroll)
        drawdown_after = max(0.0, running_peak - bankroll)
        race_stake_totals[race_key] += stake
        day_stake_totals[result_date] += stake
        output.append(
            _PlacedBet(
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                window_label=item.window_label,
                row=item,
                bankroll_before=bankroll_before,
                stake=stake,
                scaled_return=scaled_return,
                bet_profit=bet_profit,
                bankroll_after=bankroll,
                drawdown=drawdown_after,
            ),
        )
    return tuple(output)


def compute_stateful_stake(
    *,
    stake_variant: str,
    row: GuardVariantRow,
    bankroll: float,
    current_drawdown: float,
    race_spent: float,
    day_spent: float,
    config: ReferenceBankrollSimulationConfig,
) -> float:
    if stake_variant == "flat_100":
        return 100.0 if bankroll >= 100.0 else 0.0
    place_basis_odds = row.row.place_basis_odds
    pred_probability = row.row.pred_probability
    if place_basis_odds is None or place_basis_odds <= 1.0:
        return 100.0 if bankroll >= 100.0 else 0.0
    b_value = max(place_basis_odds - 1.0, 0.01)
    q_value = 1.0 - pred_probability
    kelly_fraction = max(0.0, ((b_value * pred_probability) - q_value) / b_value)
    raw_stake = bankroll * config.kelly_fraction * kelly_fraction
    stake = round_to_stake_unit_or_zero(min(raw_stake, config.kelly_cap_stake, bankroll))

    if stake_variant == "capped_fractional_kelly_like_per_race_cap":
        available = config.per_race_cap_stake - race_spent
        return round_to_stake_unit_or_zero(min(stake, available, bankroll))
    if stake_variant == "capped_fractional_kelly_like_per_race_cap_drawdown_reduction":
        available = config.per_race_cap_stake - race_spent
        stake = round_to_stake_unit_or_zero(min(stake, available, bankroll))
        if current_drawdown >= config.drawdown_reduction_threshold:
            stake = round_to_stake_unit_or_zero(stake * config.drawdown_reduction_factor)
        return round_to_stake_unit_or_zero(min(stake, bankroll))
    if stake_variant == "capped_fractional_kelly_like_per_day_cap":
        available = config.per_day_cap_stake - day_spent
        return round_to_stake_unit_or_zero(min(stake, available, bankroll))
    if stake_variant == "capped_fractional_kelly_like_drawdown_reduction":
        if current_drawdown >= config.drawdown_reduction_threshold:
            stake = round_to_stake_unit_or_zero(stake * config.drawdown_reduction_factor)
        return round_to_stake_unit_or_zero(min(stake, bankroll))
    if stake_variant == "capped_fractional_kelly_like":
        return stake
    raise ValueError(f"Unsupported stake_variant: {stake_variant}")


def summarize_rows(
    *,
    initial_bankroll: float,
    rows: tuple[_PlacedBet, ...],
) -> _Stats:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.row.row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.scaled_return for row in rows)
    total_stake = sum(row.stake for row in rows)
    total_profit = sum(row.bet_profit for row in rows)
    max_drawdown = max((row.drawdown for row in rows), default=0.0)
    current_losing_streak = 0
    max_losing_streak = 0
    for item in rows:
        if item.bet_profit < 0:
            current_losing_streak += 1
            max_losing_streak = max(max_losing_streak, current_losing_streak)
        else:
            current_losing_streak = 0
    final_bankroll = rows[-1].bankroll_after if rows else initial_bankroll
    return _Stats(
        final_bankroll=final_bankroll,
        cumulative_profit=final_bankroll - initial_bankroll,
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


def build_group_summaries(
    *,
    stake_variant: str,
    initial_bankroll: float,
    rows: tuple[_PlacedBet, ...],
    key_fn: Callable[[_PlacedBet], str],
) -> tuple[BankrollGroupSummary, ...]:
    grouped: dict[str, list[_PlacedBet]] = {}
    for item in rows:
        grouped.setdefault(str(key_fn(item)), []).append(item)
    output: list[BankrollGroupSummary] = []
    for group_key in sorted(grouped):
        group_rows = tuple(grouped[group_key])
        hit_rows = tuple(row for row in group_rows if row.row.row.place_payout is not None)
        bet_count = len(group_rows)
        hit_count = len(hit_rows)
        total_return = sum(row.scaled_return for row in group_rows)
        total_stake = sum(row.stake for row in group_rows)
        total_profit = sum(row.bet_profit for row in group_rows)
        max_drawdown = max((row.drawdown for row in group_rows), default=0.0)
        current_losing_streak = 0
        max_losing_streak = 0
        for item in group_rows:
            if item.bet_profit < 0:
                current_losing_streak += 1
                max_losing_streak = max(max_losing_streak, current_losing_streak)
            else:
                current_losing_streak = 0
        output.append(
            BankrollGroupSummary(
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                group_key=group_key,
                bet_count=bet_count,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / total_stake) if total_stake > 0 else 0.0,
                total_profit=total_profit,
                bankroll_change=total_profit,
                avg_payout=(
                    sum(row.scaled_return for row in hit_rows) / hit_count if hit_count > 0 else 0.0
                ),
                avg_edge=(
                    sum((item.row.row.edge or 0.0) for item in group_rows) / bet_count
                    if bet_count > 0
                    else 0.0
                ),
                avg_stake=(total_stake / bet_count) if bet_count > 0 else 0.0,
                total_stake=total_stake,
                max_drawdown=max_drawdown,
                max_losing_streak=max_losing_streak,
            ),
        )
    return tuple(output)


def build_bankroll_path_rows(
    *,
    stake_variant: str,
    initial_bankroll: float,
    rows: tuple[_PlacedBet, ...],
) -> tuple[BankrollPathRow, ...]:
    return tuple(
        BankrollPathRow(
            stake_variant=stake_variant,
            initial_bankroll=initial_bankroll,
            sequence=index,
            result_date=item.row.row.result_date.isoformat(),
            window_label=item.window_label,
            race_key=item.row.row.race_key,
            horse_number=item.row.row.horse_number,
            bankroll_before=item.bankroll_before,
            stake=item.stake,
            scaled_return=item.scaled_return,
            bet_profit=item.bet_profit,
            bankroll_after=item.bankroll_after,
            drawdown=item.drawdown,
            edge=item.row.row.edge,
            pred_probability=item.row.row.pred_probability,
            market_prob=item.row.row.market_prob,
            win_odds=item.row.row.win_odds,
            place_basis_odds=item.row.row.place_basis_odds,
            popularity=item.row.row.popularity,
        )
        for index, item in enumerate(rows, start=1)
    )


def build_bootstrap_rows(
    *,
    stake_variant: str,
    initial_bankroll: float,
    rows: tuple[GuardVariantRow, ...],
    config: ReferenceBankrollSimulationConfig,
) -> list[BankrollBootstrapRow]:
    rng = random.Random(config.random_seed)
    output: list[BankrollBootstrapRow] = []
    if not rows:
        return output
    for iteration in range(1, config.bootstrap_iterations + 1):
        sampled = tuple(rng.choice(rows) for _ in range(len(rows)))
        simulated = simulate_rows(
            rows=sampled,
            stake_variant=stake_variant,
            initial_bankroll=initial_bankroll,
            config=config,
        )
        stats = summarize_rows(initial_bankroll=initial_bankroll, rows=simulated)
        output.append(
            BankrollBootstrapRow(
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                iteration=iteration,
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
