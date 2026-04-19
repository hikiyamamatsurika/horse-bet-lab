from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from horse_bet_lab.config import (
    ReferenceGuardCompareConfig,
    load_ranking_rule_comparison_config,
    load_reference_strategy_diagnostics_config,
)
from horse_bet_lab.evaluation.ranking_rule_rollforward import (
    CandidateBetRow,
    build_selected_test_rows_by_window,
    run_ranking_rule_comparison,
)
from horse_bet_lab.evaluation.reference_strategy import (
    average_optional_float,
    build_equity_curve,
    write_csv,
    write_json,
)


@dataclass(frozen=True)
class GuardVariantRow:
    variant: str
    window_label: str
    row: CandidateBetRow


@dataclass(frozen=True)
class GuardVariantSummary:
    variant: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    refund_count: int
    refund_ratio: float
    payout_100_110_count: int
    payout_100_110_ratio: float
    payout_le_120_count: int
    payout_le_120_ratio: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class GuardVariantGroupSummary:
    variant: str
    group_key: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    refund_count: int
    refund_ratio: float
    payout_100_110_count: int
    payout_100_110_ratio: float
    payout_le_120_count: int
    payout_le_120_ratio: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class GuardVariantEquityRow:
    variant: str
    sequence: int
    result_date: str
    window_label: str
    race_key: str
    horse_number: int
    place_payout: float
    bet_profit: float
    cumulative_profit: float
    drawdown: float
    edge: float | None
    win_odds: float | None
    place_basis_odds: float | None
    popularity: int | None


@dataclass(frozen=True)
class ReferenceGuardCompareResult:
    output_dir: Path
    summaries: tuple[GuardVariantSummary, ...]
    yearly_summaries: tuple[GuardVariantGroupSummary, ...]
    window_summaries: tuple[GuardVariantGroupSummary, ...]
    equity_rows: tuple[GuardVariantEquityRow, ...]


def run_reference_guard_compare(
    config: ReferenceGuardCompareConfig,
) -> ReferenceGuardCompareResult:
    reference_config = load_reference_strategy_diagnostics_config(
        config.reference_strategy_config_path,
    )
    ranking_config = load_ranking_rule_comparison_config(
        reference_config.ranking_rule_comparison_config_path,
    )
    ranking_result = run_ranking_rule_comparison(ranking_config)
    selected_rows_by_window = build_selected_test_rows_by_window(
        selected_summaries=ranking_result.selected_summaries,
        selected_rows_by_candidate=ranking_result.selected_rows_by_candidate,
    )
    edge_threshold_by_window = build_edge_threshold_by_window(ranking_result.selected_summaries)
    variants = build_variant_rows(
        config=config,
        selected_rows_by_window=selected_rows_by_window,
        edge_threshold_by_window=edge_threshold_by_window,
    )

    summaries: list[GuardVariantSummary] = []
    yearly_summaries: list[GuardVariantGroupSummary] = []
    window_summaries: list[GuardVariantGroupSummary] = []
    equity_rows: list[GuardVariantEquityRow] = []

    for variant_name in sorted(variants):
        variant_rows = variants[variant_name]
        variant_equity_rows, max_drawdown, max_losing_streak = build_variant_equity_curve(
            variant_rows,
            stake_per_bet=ranking_config.stake_per_bet,
        )
        equity_rows.extend(variant_equity_rows)
        summaries.append(
            summarize_variant(
                variant=variant_name,
                rows=variant_rows,
                stake_per_bet=ranking_config.stake_per_bet,
                max_drawdown=max_drawdown,
                max_losing_streak=max_losing_streak,
            ),
        )
        yearly_summaries.extend(
            build_group_summaries(
                variant=variant_name,
                rows=variant_rows,
                stake_per_bet=ranking_config.stake_per_bet,
                key_fn=lambda item: item.row.result_date.isoformat()[:4],
            ),
        )
        window_summaries.extend(
            build_group_summaries(
                variant=variant_name,
                rows=variant_rows,
                stake_per_bet=ranking_config.stake_per_bet,
                key_fn=lambda item: item.window_label,
            ),
        )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceGuardCompareResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        yearly_summaries=tuple(yearly_summaries),
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
    write_csv(config.output_dir / "window_summary.csv", result.window_summaries)
    write_json(
        config.output_dir / "window_summary.json",
        {"analysis": {"rows": result.window_summaries}},
    )
    write_csv(config.output_dir / "equity_curve.csv", result.equity_rows)
    write_json(
        config.output_dir / "equity_curve.json",
        {"analysis": {"rows": result.equity_rows}},
    )
    return result


def build_edge_threshold_by_window(selected_summaries: tuple[Any, ...]) -> dict[str, float]:
    output: dict[str, float] = {}
    for summary in selected_summaries:
        if summary.applied_to_split != "test":
            continue
        if summary.ranking_score_rule != "edge":
            continue
        output[str(summary.test_window_label)] = float(summary.threshold)
    return output


def build_variant_rows(
    *,
    config: ReferenceGuardCompareConfig,
    selected_rows_by_window: dict[str, dict[str, tuple[CandidateBetRow, ...]]],
    edge_threshold_by_window: dict[str, float],
) -> dict[str, tuple[GuardVariantRow, ...]]:
    base_rows = build_consensus_reference_rows(selected_rows_by_window)
    output: dict[str, tuple[GuardVariantRow, ...]] = {
        "baseline": base_rows,
        "problematic_band_excluded": tuple(
            GuardVariantRow(
                variant="problematic_band_excluded",
                window_label=item.window_label,
                row=item.row,
            )
            for item in base_rows
            if not in_problematic_band(item.row, config)
        ),
    }
    excluded_rows = output["problematic_band_excluded"]
    if config.exclude_win_odds_below is not None:
        output["problematic_band_excluded_win_odds_lt_5_excluded"] = tuple(
            GuardVariantRow(
                variant="problematic_band_excluded_win_odds_lt_5_excluded",
                window_label=item.window_label,
                row=item.row,
            )
            for item in excluded_rows
            if keep_with_win_odds_guard(item.row, config.exclude_win_odds_below)
        )
    if config.exclude_edge_below is not None:
        output["problematic_band_excluded_edge_lt_0_06_excluded"] = tuple(
            GuardVariantRow(
                variant="problematic_band_excluded_edge_lt_0_06_excluded",
                window_label=item.window_label,
                row=item.row,
            )
            for item in excluded_rows
            if keep_with_edge_floor(item.row, config.exclude_edge_below)
        )
    for surcharge in config.edge_surcharges:
        label = f"problematic_band_edge_plus_{format_surcharge(surcharge)}"
        output[label] = tuple(
            GuardVariantRow(
                variant=label,
                window_label=item.window_label,
                row=item.row,
            )
            for item in base_rows
            if keep_with_surcharge(
                item,
                config=config,
                edge_threshold_by_window=edge_threshold_by_window,
                surcharge=surcharge,
            )
        )
    return output


def build_consensus_reference_rows(
    selected_rows_by_window: dict[str, dict[str, tuple[CandidateBetRow, ...]]],
) -> tuple[GuardVariantRow, ...]:
    rows: list[GuardVariantRow] = []
    for window_label in sorted(selected_rows_by_window):
        edge_rows = {
            (row.race_key, row.horse_number): row
            for row in selected_rows_by_window[window_label].get("edge", ())
        }
        pred_rows = {
            (row.race_key, row.horse_number): row
            for row in selected_rows_by_window[window_label].get(
                "pred_times_place_basis_odds",
                (),
            )
        }
        common_keys = sorted(set(edge_rows) & set(pred_rows))
        for race_key, horse_number in common_keys:
            rows.append(
                GuardVariantRow(
                    variant="baseline",
                    window_label=window_label,
                    row=edge_rows[(race_key, horse_number)],
                ),
            )
    rows.sort(
        key=lambda item: (
            item.row.result_date,
            item.window_label,
            item.row.race_key,
            item.row.horse_number,
        ),
    )
    return tuple(rows)


def in_problematic_band(row: CandidateBetRow, config: ReferenceGuardCompareConfig) -> bool:
    if row.popularity is None or row.place_basis_odds is None:
        return False
    return (
        config.problematic_min_popularity <= row.popularity <= config.problematic_max_popularity
        and config.problematic_min_place_basis_odds
        <= row.place_basis_odds
        <= config.problematic_max_place_basis_odds
    )


def keep_with_surcharge(
    item: GuardVariantRow,
    *,
    config: ReferenceGuardCompareConfig,
    edge_threshold_by_window: dict[str, float],
    surcharge: float,
) -> bool:
    row = item.row
    if not in_problematic_band(row, config):
        return True
    base_threshold = edge_threshold_by_window.get(item.window_label)
    if base_threshold is None or row.edge is None:
        return False
    return row.edge >= (base_threshold + surcharge)


def keep_with_win_odds_guard(row: CandidateBetRow, minimum_win_odds: float) -> bool:
    if row.win_odds is None:
        return False
    return row.win_odds >= minimum_win_odds


def keep_with_edge_floor(row: CandidateBetRow, minimum_edge: float) -> bool:
    if row.edge is None:
        return False
    return row.edge >= minimum_edge


def format_surcharge(value: float) -> str:
    return f"{value:.2f}".replace(".", "_")


def build_variant_equity_curve(
    rows: tuple[GuardVariantRow, ...],
    *,
    stake_per_bet: float,
) -> tuple[tuple[GuardVariantEquityRow, ...], float, int]:
    equity_rows, max_drawdown, max_losing_streak = build_equity_curve(
        tuple((item.window_label, item.row) for item in rows),
        stake_per_bet=stake_per_bet,
    )
    return (
        tuple(
            GuardVariantEquityRow(
                variant=rows[index].variant if index < len(rows) else "unknown",
                sequence=row.sequence,
                result_date=row.result_date,
                window_label=row.window_label,
                race_key=row.race_key,
                horse_number=row.horse_number,
                place_payout=row.place_payout,
                bet_profit=row.bet_profit,
                cumulative_profit=row.cumulative_profit,
                drawdown=row.drawdown,
                edge=row.edge,
                win_odds=row.win_odds,
                place_basis_odds=row.place_basis_odds,
                popularity=row.popularity,
            )
            for index, row in enumerate(equity_rows)
        ),
        max_drawdown,
        max_losing_streak,
    )


def summarize_variant(
    *,
    variant: str,
    rows: tuple[GuardVariantRow, ...],
    stake_per_bet: float,
    max_drawdown: float,
    max_losing_streak: int,
) -> GuardVariantSummary:
    bet_rows = tuple(item.row for item in rows)
    bet_count = len(bet_rows)
    hit_rows = tuple(row for row in bet_rows if row.place_payout is not None)
    refund_count = sum(1 for row in hit_rows if (row.place_payout or 0.0) == 100.0)
    payout_100_110_count = sum(
        1 for row in hit_rows if 100.0 <= (row.place_payout or 0.0) <= 110.0
    )
    payout_le_120_count = sum(1 for row in hit_rows if (row.place_payout or 0.0) <= 120.0)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in bet_rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return GuardVariantSummary(
        variant=variant,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout or 0.0 for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.edge for row in bet_rows)) or 0.0,
        refund_count=refund_count,
        refund_ratio=(refund_count / bet_count) if bet_count > 0 else 0.0,
        payout_100_110_count=payout_100_110_count,
        payout_100_110_ratio=(
            payout_100_110_count / bet_count if bet_count > 0 else 0.0
        ),
        payout_le_120_count=payout_le_120_count,
        payout_le_120_ratio=(payout_le_120_count / bet_count) if bet_count > 0 else 0.0,
        max_drawdown=max_drawdown,
        max_losing_streak=max_losing_streak,
    )


def build_group_summaries(
    *,
    variant: str,
    rows: tuple[GuardVariantRow, ...],
    stake_per_bet: float,
    key_fn: Any,
) -> tuple[GuardVariantGroupSummary, ...]:
    grouped: dict[str, list[GuardVariantRow]] = {}
    for item in rows:
        grouped.setdefault(str(key_fn(item)), []).append(item)
    summaries: list[GuardVariantGroupSummary] = []
    for group_key in sorted(grouped):
        group_rows = tuple(grouped[group_key])
        equity_rows, max_drawdown, max_losing_streak = build_variant_equity_curve(
            tuple(
                sorted(
                    group_rows,
                    key=lambda item: (
                        item.row.result_date,
                        item.window_label,
                        item.row.race_key,
                        item.row.horse_number,
                    ),
                ),
            ),
            stake_per_bet=stake_per_bet,
        )
        del equity_rows
        bet_rows = tuple(item.row for item in group_rows)
        hit_rows = tuple(row for row in bet_rows if row.place_payout is not None)
        refund_count = sum(1 for row in hit_rows if (row.place_payout or 0.0) == 100.0)
        payout_100_110_count = sum(
            1 for row in hit_rows if 100.0 <= (row.place_payout or 0.0) <= 110.0
        )
        payout_le_120_count = sum(
            1 for row in hit_rows if (row.place_payout or 0.0) <= 120.0
        )
        bet_count = len(bet_rows)
        hit_count = len(hit_rows)
        total_return = sum(row.place_payout or 0.0 for row in bet_rows)
        total_profit = total_return - (bet_count * stake_per_bet)
        summaries.append(
            GuardVariantGroupSummary(
                variant=variant,
                group_key=group_key,
                bet_count=bet_count,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
                total_profit=total_profit,
                avg_payout=(
                    sum(row.place_payout or 0.0 for row in hit_rows) / hit_count
                    if hit_count > 0
                    else 0.0
                ),
                avg_edge=average_optional_float(tuple(row.edge for row in bet_rows)) or 0.0,
                refund_count=refund_count,
                refund_ratio=(refund_count / bet_count) if bet_count > 0 else 0.0,
                payout_100_110_count=payout_100_110_count,
                payout_100_110_ratio=(
                    payout_100_110_count / bet_count if bet_count > 0 else 0.0
                ),
                payout_le_120_count=payout_le_120_count,
                payout_le_120_ratio=(
                    payout_le_120_count / bet_count if bet_count > 0 else 0.0
                ),
                max_drawdown=max_drawdown,
                max_losing_streak=max_losing_streak,
            ),
        )
    return tuple(summaries)
