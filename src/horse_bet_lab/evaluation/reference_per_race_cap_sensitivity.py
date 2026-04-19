from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.config import (
    ReferenceBankrollSimulationConfig,
    ReferencePerRaceCapSensitivityConfig,
    load_reference_bankroll_simulation_config,
)
from horse_bet_lab.evaluation.reference_bankroll_simulation import (
    BankrollGroupSummary,
    BankrollPathRow,
    build_bankroll_path_rows,
    build_group_summaries,
    load_selected_reference_rows,
    quantile,
    simulate_rows,
    summarize_rows,
)
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json


@dataclass(frozen=True)
class PerRaceCapSensitivitySummary:
    per_race_cap_stake: float
    initial_bankroll: float
    final_bankroll: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    avg_stake: float
    total_stake: float
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
    final_bankroll_p02_5: float
    final_bankroll_p50: float
    final_bankroll_p97_5: float
    final_bankroll_below_initial_ratio: float


@dataclass(frozen=True)
class PerRaceCapSensitivityGroupSummary:
    per_race_cap_stake: float
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
class PerRaceCapSensitivityPathRow:
    per_race_cap_stake: float
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
class PerRaceCapSensitivityBootstrapRow:
    per_race_cap_stake: float
    initial_bankroll: float
    iteration: int
    final_bankroll: float
    roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class ReferencePerRaceCapSensitivityResult:
    output_dir: Path
    summaries: tuple[PerRaceCapSensitivitySummary, ...]
    yearly_summaries: tuple[PerRaceCapSensitivityGroupSummary, ...]
    monthly_summaries: tuple[PerRaceCapSensitivityGroupSummary, ...]
    equity_rows: tuple[PerRaceCapSensitivityPathRow, ...]
    bootstrap_rows: tuple[PerRaceCapSensitivityBootstrapRow, ...]


def run_reference_per_race_cap_sensitivity(
    config: ReferencePerRaceCapSensitivityConfig,
) -> ReferencePerRaceCapSensitivityResult:
    base_config = load_reference_bankroll_simulation_config(
        config.reference_bankroll_simulation_config_path,
    )
    selected_rows = load_selected_reference_rows(base_config)

    summaries: list[PerRaceCapSensitivitySummary] = []
    yearly_summaries: list[PerRaceCapSensitivityGroupSummary] = []
    monthly_summaries: list[PerRaceCapSensitivityGroupSummary] = []
    equity_rows: list[PerRaceCapSensitivityPathRow] = []
    bootstrap_rows: list[PerRaceCapSensitivityBootstrapRow] = []

    for initial_bankroll in config.initial_bankrolls:
        for per_race_cap_stake in config.per_race_cap_values:
            runtime_config = build_runtime_config(
                base_config=base_config,
                initial_bankroll=initial_bankroll,
                per_race_cap_stake=per_race_cap_stake,
                bootstrap_iterations=config.bootstrap_iterations,
                random_seed=config.random_seed,
            )
            placed_rows = simulate_rows(
                rows=selected_rows,
                stake_variant="capped_fractional_kelly_like_per_race_cap",
                initial_bankroll=initial_bankroll,
                config=runtime_config,
            )
            stats = summarize_rows(initial_bankroll=initial_bankroll, rows=placed_rows)
            variant_bootstrap = build_bootstrap_rows(
                rows=selected_rows,
                initial_bankroll=initial_bankroll,
                config=runtime_config,
                per_race_cap_stake=per_race_cap_stake,
            )
            roi_values = [row.roi for row in variant_bootstrap]
            profit_values = [row.total_profit for row in variant_bootstrap]
            dd_values = [row.max_drawdown for row in variant_bootstrap]
            final_bankroll_values = [row.final_bankroll for row in variant_bootstrap]

            summaries.append(
                PerRaceCapSensitivitySummary(
                    per_race_cap_stake=per_race_cap_stake,
                    initial_bankroll=initial_bankroll,
                    final_bankroll=stats.final_bankroll,
                    roi=stats.roi,
                    total_profit=stats.total_profit,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
                    avg_stake=stats.avg_stake,
                    total_stake=stats.total_stake,
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
                    final_bankroll_p02_5=quantile(final_bankroll_values, 0.025),
                    final_bankroll_p50=quantile(final_bankroll_values, 0.5),
                    final_bankroll_p97_5=quantile(final_bankroll_values, 0.975),
                    final_bankroll_below_initial_ratio=(
                        sum(
                            1 for value in final_bankroll_values if value < initial_bankroll
                        )
                        / len(final_bankroll_values)
                        if final_bankroll_values
                        else 0.0
                    ),
                ),
            )
            yearly_summaries.extend(
                convert_group_summaries(
                    per_race_cap_stake=per_race_cap_stake,
                    initial_bankroll=initial_bankroll,
                    group_rows=build_group_summaries(
                        stake_variant="capped_fractional_kelly_like_per_race_cap",
                        initial_bankroll=initial_bankroll,
                        rows=placed_rows,
                        key_fn=lambda item: item.row.row.result_date.isoformat()[:4],
                    ),
                ),
            )
            monthly_summaries.extend(
                convert_group_summaries(
                    per_race_cap_stake=per_race_cap_stake,
                    initial_bankroll=initial_bankroll,
                    group_rows=build_group_summaries(
                        stake_variant="capped_fractional_kelly_like_per_race_cap",
                        initial_bankroll=initial_bankroll,
                        rows=placed_rows,
                        key_fn=lambda item: item.row.row.result_date.isoformat()[:7],
                    ),
                ),
            )
            equity_rows.extend(
                convert_path_rows(
                    per_race_cap_stake=per_race_cap_stake,
                    initial_bankroll=initial_bankroll,
                    rows=build_bankroll_path_rows(
                        stake_variant="capped_fractional_kelly_like_per_race_cap",
                        initial_bankroll=initial_bankroll,
                        rows=placed_rows,
                    ),
                ),
            )
            bootstrap_rows.extend(variant_bootstrap)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferencePerRaceCapSensitivityResult(
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
    write_csv(config.output_dir / "bootstrap_distribution.csv", result.bootstrap_rows)
    write_json(
        config.output_dir / "bootstrap_distribution.json",
        {"analysis": {"rows": result.bootstrap_rows}},
    )
    return result


def build_runtime_config(
    *,
    base_config: ReferenceBankrollSimulationConfig,
    initial_bankroll: float,
    per_race_cap_stake: float,
    bootstrap_iterations: int,
    random_seed: int,
) -> ReferenceBankrollSimulationConfig:
    return ReferenceBankrollSimulationConfig(
        name=base_config.name,
        reference_label_guard_compare_config_path=base_config.reference_label_guard_compare_config_path,
        output_dir=base_config.output_dir,
        stake_variants=("capped_fractional_kelly_like_per_race_cap",),
        initial_bankrolls=(initial_bankroll,),
        kelly_fraction=base_config.kelly_fraction,
        kelly_cap_stake=base_config.kelly_cap_stake,
        per_race_cap_stake=per_race_cap_stake,
        per_day_cap_stake=base_config.per_day_cap_stake,
        drawdown_reduction_threshold=base_config.drawdown_reduction_threshold,
        drawdown_reduction_factor=base_config.drawdown_reduction_factor,
        bootstrap_iterations=bootstrap_iterations,
        random_seed=random_seed,
    )


def build_bootstrap_rows(
    *,
    rows: tuple[GuardVariantRow, ...],
    initial_bankroll: float,
    config: ReferenceBankrollSimulationConfig,
    per_race_cap_stake: float,
) -> list[PerRaceCapSensitivityBootstrapRow]:
    from horse_bet_lab.evaluation.reference_bankroll_simulation_uncertainty import (
        build_bootstrap_rows as build_bankroll_bootstrap_rows,
    )

    return [
        PerRaceCapSensitivityBootstrapRow(
            per_race_cap_stake=per_race_cap_stake,
            initial_bankroll=initial_bankroll,
            iteration=row.iteration,
            final_bankroll=row.final_bankroll,
            roi=row.roi,
            total_profit=row.total_profit,
            max_drawdown=row.max_drawdown,
            max_losing_streak=row.max_losing_streak,
        )
        for row in build_bankroll_bootstrap_rows(
            stake_variant="capped_fractional_kelly_like_per_race_cap",
            initial_bankroll=initial_bankroll,
            rows=rows,
            config=config,
        )
    ]


def convert_group_summaries(
    *,
    per_race_cap_stake: float,
    initial_bankroll: float,
    group_rows: tuple[BankrollGroupSummary, ...],
) -> tuple[PerRaceCapSensitivityGroupSummary, ...]:
    return tuple(
        PerRaceCapSensitivityGroupSummary(
            per_race_cap_stake=per_race_cap_stake,
            initial_bankroll=initial_bankroll,
            group_key=row.group_key,
            bet_count=row.bet_count,
            hit_count=row.hit_count,
            hit_rate=row.hit_rate,
            roi=row.roi,
            total_profit=row.total_profit,
            bankroll_change=row.bankroll_change,
            avg_payout=row.avg_payout,
            avg_edge=row.avg_edge,
            avg_stake=row.avg_stake,
            total_stake=row.total_stake,
            max_drawdown=row.max_drawdown,
            max_losing_streak=row.max_losing_streak,
        )
        for row in group_rows
    )


def convert_path_rows(
    *,
    per_race_cap_stake: float,
    initial_bankroll: float,
    rows: tuple[BankrollPathRow, ...],
) -> tuple[PerRaceCapSensitivityPathRow, ...]:
    return tuple(
        PerRaceCapSensitivityPathRow(
            per_race_cap_stake=per_race_cap_stake,
            initial_bankroll=initial_bankroll,
            sequence=row.sequence,
            result_date=row.result_date,
            window_label=row.window_label,
            race_key=row.race_key,
            horse_number=row.horse_number,
            bankroll_before=row.bankroll_before,
            stake=row.stake,
            scaled_return=row.scaled_return,
            bet_profit=row.bet_profit,
            bankroll_after=row.bankroll_after,
            drawdown=row.drawdown,
            edge=row.edge,
            pred_probability=row.pred_probability,
            market_prob=row.market_prob,
            win_odds=row.win_odds,
            place_basis_odds=row.place_basis_odds,
            popularity=row.popularity,
        )
        for row in rows
    )
