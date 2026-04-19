from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.config import (
    ReferenceBankrollSimulationConfig,
    ReferenceBankrollSimulationUncertaintyConfig,
    load_reference_bankroll_simulation_config,
)
from horse_bet_lab.evaluation.reference_bankroll_simulation import (
    _PlacedBet,
    load_selected_reference_rows,
    quantile,
    simulate_rows,
    summarize_rows,
)
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.reference_uncertainty import (
    build_guard_variant_block_key_fn,
    sample_block_bootstrap,
)


@dataclass(frozen=True)
class BankrollSimulationUncertaintySummary:
    stake_variant: str
    initial_bankroll: float
    final_bankroll: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    bootstrap_iterations: int
    final_bankroll_p02_5: float
    final_bankroll_p50: float
    final_bankroll_p97_5: float
    final_bankroll_below_initial_ratio: float
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
class BankrollSimulationUncertaintyBootstrapRow:
    stake_variant: str
    initial_bankroll: float
    iteration: int
    final_bankroll: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class BankrollSimulationYearContributionRow:
    stake_variant: str
    initial_bankroll: float
    year: str
    final_bankroll: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class ReferenceBankrollSimulationUncertaintyResult:
    output_dir: Path
    summaries: tuple[BankrollSimulationUncertaintySummary, ...]
    bootstrap_rows: tuple[BankrollSimulationUncertaintyBootstrapRow, ...]
    yearly_contributions: tuple[BankrollSimulationYearContributionRow, ...]


def run_reference_bankroll_simulation_uncertainty(
    config: ReferenceBankrollSimulationUncertaintyConfig,
) -> ReferenceBankrollSimulationUncertaintyResult:
    simulation_config = load_reference_bankroll_simulation_config(
        config.reference_bankroll_simulation_config_path,
    )
    runtime_config = ReferenceBankrollSimulationConfig(
        name=simulation_config.name,
        reference_label_guard_compare_config_path=simulation_config.reference_label_guard_compare_config_path,
        output_dir=simulation_config.output_dir,
        stake_variants=simulation_config.stake_variants,
        initial_bankrolls=simulation_config.initial_bankrolls,
        kelly_fraction=simulation_config.kelly_fraction,
        kelly_cap_stake=simulation_config.kelly_cap_stake,
        per_race_cap_stake=simulation_config.per_race_cap_stake,
        per_day_cap_stake=simulation_config.per_day_cap_stake,
        drawdown_reduction_threshold=simulation_config.drawdown_reduction_threshold,
        drawdown_reduction_factor=simulation_config.drawdown_reduction_factor,
        bootstrap_iterations=config.bootstrap_iterations,
        random_seed=config.random_seed,
    )
    base_rows = load_selected_reference_rows(runtime_config)

    summaries: list[BankrollSimulationUncertaintySummary] = []
    bootstrap_rows: list[BankrollSimulationUncertaintyBootstrapRow] = []
    yearly_contributions: list[BankrollSimulationYearContributionRow] = []

    for initial_bankroll in config.initial_bankrolls:
        for stake_variant in config.stake_variants:
            actual_rows = simulate_rows(
                rows=base_rows,
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                config=runtime_config,
            )
            actual_stats = summarize_rows(
                initial_bankroll=initial_bankroll,
                rows=actual_rows,
            )
            variant_bootstrap_rows = build_bootstrap_rows(
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                rows=base_rows,
                config=runtime_config,
                block_unit=config.bootstrap_block_unit,
            )
            final_bankroll_values = [row.final_bankroll for row in variant_bootstrap_rows]
            roi_values = [row.roi for row in variant_bootstrap_rows]
            profit_values = [row.total_profit for row in variant_bootstrap_rows]
            dd_values = [row.max_drawdown for row in variant_bootstrap_rows]
            summaries.append(
                BankrollSimulationUncertaintySummary(
                    stake_variant=stake_variant,
                    initial_bankroll=initial_bankroll,
                    final_bankroll=actual_stats.final_bankroll,
                    roi=actual_stats.roi,
                    total_profit=actual_stats.total_profit,
                    max_drawdown=actual_stats.max_drawdown,
                    max_losing_streak=actual_stats.max_losing_streak,
                    bootstrap_iterations=config.bootstrap_iterations,
                    final_bankroll_p02_5=quantile(final_bankroll_values, 0.025),
                    final_bankroll_p50=quantile(final_bankroll_values, 0.5),
                    final_bankroll_p97_5=quantile(final_bankroll_values, 0.975),
                    final_bankroll_below_initial_ratio=(
                        sum(1 for value in final_bankroll_values if value < initial_bankroll)
                        / len(final_bankroll_values)
                        if final_bankroll_values
                        else 0.0
                    ),
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
            yearly_contributions.extend(
                build_year_contributions(
                    stake_variant=stake_variant,
                    initial_bankroll=initial_bankroll,
                    rows=actual_rows,
                ),
            )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceBankrollSimulationUncertaintyResult(
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


def build_bootstrap_rows(
    *,
    stake_variant: str,
    initial_bankroll: float,
    rows: tuple[GuardVariantRow, ...],
    config: ReferenceBankrollSimulationConfig,
    block_unit: str = "race_date",
) -> list[BankrollSimulationUncertaintyBootstrapRow]:
    rng = random.Random(config.random_seed)
    output: list[BankrollSimulationUncertaintyBootstrapRow] = []
    if not rows:
        return output
    block_key_fn = build_guard_variant_block_key_fn(block_unit)
    for iteration in range(1, config.bootstrap_iterations + 1):
        sampled = sample_block_bootstrap(
            rows,
            rng=rng,
            block_key_fn=block_key_fn,
            sort_key_fn=lambda item: (
                item.row.result_date,
                item.window_label,
                item.row.race_key,
                item.row.horse_number,
            ),
        )
        placed_rows = simulate_rows(
            rows=sampled,
            stake_variant=stake_variant,
            initial_bankroll=initial_bankroll,
            config=config,
        )
        stats = summarize_rows(
            initial_bankroll=initial_bankroll,
            rows=placed_rows,
        )
        output.append(
            BankrollSimulationUncertaintyBootstrapRow(
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                iteration=iteration,
                final_bankroll=stats.final_bankroll,
                roi=stats.roi,
                total_profit=stats.total_profit,
                max_drawdown=stats.max_drawdown,
                max_losing_streak=stats.max_losing_streak,
            ),
        )
    return output


def build_year_contributions(
    *,
    stake_variant: str,
    initial_bankroll: float,
    rows: tuple[_PlacedBet, ...],
) -> tuple[BankrollSimulationYearContributionRow, ...]:
    grouped: dict[str, list[_PlacedBet]] = {}
    for item in rows:
        year = item.row.row.result_date.isoformat()[:4]
        grouped.setdefault(year, []).append(item)
    output: list[BankrollSimulationYearContributionRow] = []
    for year in sorted(grouped):
        group_rows = tuple(grouped[year])
        stats = summarize_rows(initial_bankroll=initial_bankroll, rows=group_rows)
        output.append(
            BankrollSimulationYearContributionRow(
                stake_variant=stake_variant,
                initial_bankroll=initial_bankroll,
                year=year,
                final_bankroll=stats.final_bankroll,
                roi=stats.roi,
                total_profit=stats.total_profit,
                max_drawdown=stats.max_drawdown,
                max_losing_streak=stats.max_losing_streak,
            ),
        )
    return tuple(output)
