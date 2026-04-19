from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from horse_bet_lab.config import (
    ReferenceStrategyDiagnosticsConfig,
    load_ranking_rule_comparison_config,
)
from horse_bet_lab.evaluation.ranking_rule_rollforward import (
    CandidateBetRow,
    build_consensus_rows,
    build_selected_test_rows_by_window,
    run_ranking_rule_comparison,
)


@dataclass(frozen=True)
class ReferenceStrategySummary:
    strategy_name: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    cumulative_profit: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class ReferenceStrategyEquityRow:
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
class ReferenceStrategyGroupSummary:
    group_key: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class ReferenceStrategyDiagnosticsResult:
    output_dir: Path
    summary: ReferenceStrategySummary
    equity_curve: tuple[ReferenceStrategyEquityRow, ...]
    monthly_profit: tuple[ReferenceStrategyGroupSummary, ...]
    window_profit: tuple[ReferenceStrategyGroupSummary, ...]


def run_reference_strategy_diagnostics(
    config: ReferenceStrategyDiagnosticsConfig,
) -> ReferenceStrategyDiagnosticsResult:
    ranking_config = load_ranking_rule_comparison_config(
        config.ranking_rule_comparison_config_path,
    )
    ranking_result = run_ranking_rule_comparison(ranking_config)
    selected_rows_by_window = build_selected_test_rows_by_window(
        selected_summaries=ranking_result.selected_summaries,
        selected_rows_by_candidate=ranking_result.selected_rows_by_candidate,
    )
    consensus_rows = build_reference_strategy_rows(selected_rows_by_window)
    equity_curve, max_drawdown, max_losing_streak = build_equity_curve(
        consensus_rows,
        stake_per_bet=ranking_config.stake_per_bet,
    )
    summary = build_reference_strategy_summary(
        consensus_rows,
        stake_per_bet=ranking_config.stake_per_bet,
        max_drawdown=max_drawdown,
        max_losing_streak=max_losing_streak,
    )
    monthly_profit = build_group_summaries(
        consensus_rows,
        stake_per_bet=ranking_config.stake_per_bet,
        key_fn=lambda item: item[1].result_date.isoformat()[:7],
    )
    window_profit = build_group_summaries(
        consensus_rows,
        stake_per_bet=ranking_config.stake_per_bet,
        key_fn=lambda row: row[0],
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceStrategyDiagnosticsResult(
        output_dir=config.output_dir,
        summary=summary,
        equity_curve=equity_curve,
        monthly_profit=monthly_profit,
        window_profit=window_profit,
    )
    write_csv(config.output_dir / "summary.csv", (result.summary,))
    write_json(config.output_dir / "summary.json", {"analysis": asdict(result.summary)})
    write_csv(config.output_dir / "equity_curve.csv", result.equity_curve)
    write_json(
        config.output_dir / "equity_curve.json",
        {"analysis": {"rows": result.equity_curve}},
    )
    write_csv(config.output_dir / "monthly_profit.csv", result.monthly_profit)
    write_json(
        config.output_dir / "monthly_profit.json",
        {"analysis": {"rows": result.monthly_profit}},
    )
    write_csv(config.output_dir / "window_profit.csv", result.window_profit)
    write_json(
        config.output_dir / "window_profit.json",
        {"analysis": {"rows": result.window_profit}},
    )
    return result


def build_reference_strategy_rows(
    selected_rows_by_window: dict[str, dict[str, tuple[CandidateBetRow, ...]]],
) -> tuple[tuple[str, CandidateBetRow], ...]:
    rows: list[tuple[str, CandidateBetRow]] = []
    for window_label in sorted(selected_rows_by_window):
        edge_rows = selected_rows_by_window[window_label].get("edge", ())
        pred_rows = selected_rows_by_window[window_label].get("pred_times_place_basis_odds", ())
        for row in build_consensus_rows(edge_rows, pred_rows):
            rows.append((window_label, row))
    rows.sort(
        key=lambda item: (
            item[1].result_date,
            item[1].race_key,
            item[1].horse_number,
        ),
    )
    return tuple(rows)


def build_equity_curve(
    rows: tuple[tuple[str, CandidateBetRow], ...],
    *,
    stake_per_bet: float,
) -> tuple[tuple[ReferenceStrategyEquityRow, ...], float, int]:
    cumulative_profit = 0.0
    running_peak = 0.0
    max_drawdown = 0.0
    current_losing_streak = 0
    max_losing_streak = 0
    equity_rows: list[ReferenceStrategyEquityRow] = []
    for index, (window_label, row) in enumerate(rows, start=1):
        payout = row.place_payout or 0.0
        bet_profit = payout - stake_per_bet
        cumulative_profit += bet_profit
        running_peak = max(running_peak, cumulative_profit)
        drawdown = running_peak - cumulative_profit
        max_drawdown = max(max_drawdown, drawdown)
        if bet_profit < 0:
            current_losing_streak += 1
            max_losing_streak = max(max_losing_streak, current_losing_streak)
        else:
            current_losing_streak = 0
        equity_rows.append(
            ReferenceStrategyEquityRow(
                sequence=index,
                result_date=row.result_date.isoformat(),
                window_label=window_label,
                race_key=row.race_key,
                horse_number=row.horse_number,
                place_payout=payout,
                bet_profit=bet_profit,
                cumulative_profit=cumulative_profit,
                drawdown=drawdown,
                edge=row.edge,
                win_odds=row.win_odds,
                place_basis_odds=row.place_basis_odds,
                popularity=row.popularity,
            ),
        )
    return tuple(equity_rows), max_drawdown, max_losing_streak


def build_reference_strategy_summary(
    rows: tuple[tuple[str, CandidateBetRow], ...],
    *,
    stake_per_bet: float,
    max_drawdown: float,
    max_losing_streak: int,
) -> ReferenceStrategySummary:
    bet_rows = tuple(row for _, row in rows)
    bet_count = len(bet_rows)
    hit_rows = tuple(row for row in bet_rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in bet_rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    avg_edge = average_optional_float(tuple(row.edge for row in bet_rows)) or 0.0
    return ReferenceStrategySummary(
        strategy_name="reference_consensus",
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout or 0.0 for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=avg_edge,
        cumulative_profit=total_profit,
        max_drawdown=max_drawdown,
        max_losing_streak=max_losing_streak,
    )


def build_group_summaries(
    rows: tuple[tuple[str, CandidateBetRow], ...],
    *,
    stake_per_bet: float,
    key_fn: Any,
) -> tuple[ReferenceStrategyGroupSummary, ...]:
    grouped: dict[str, list[CandidateBetRow]] = {}
    for item in rows:
        group_key = key_fn(item)
        row = item[1]
        grouped.setdefault(str(group_key), []).append(row)
    summaries: list[ReferenceStrategyGroupSummary] = []
    for group_key in sorted(grouped):
        group_rows = tuple(grouped[group_key])
        sorted_group_rows = tuple(
            sorted(
                group_rows,
                key=lambda row: (
                    row.result_date,
                    row.race_key,
                    row.horse_number,
                ),
            ),
        )
        hit_rows = tuple(row for row in group_rows if row.place_payout is not None)
        bet_count = len(group_rows)
        hit_count = len(hit_rows)
        total_return = sum(row.place_payout or 0.0 for row in group_rows)
        total_profit = total_return - (bet_count * stake_per_bet)
        _, max_drawdown, max_losing_streak = build_equity_curve(
            tuple((group_key, row) for row in sorted_group_rows),
            stake_per_bet=stake_per_bet,
        )
        summaries.append(
            ReferenceStrategyGroupSummary(
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
                avg_edge=average_optional_float(tuple(row.edge for row in group_rows)) or 0.0,
                max_drawdown=max_drawdown,
                max_losing_streak=max_losing_streak,
            ),
        )
    return tuple(summaries)


def average_optional_float(values: tuple[float | None, ...]) -> float | None:
    filtered = tuple(value for value in values if value is not None)
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def write_csv(path: Path, rows: tuple[Any, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        if rows:
            fieldnames = tuple(asdict(rows[0]).keys())
        else:
            fieldnames = ()
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=serialize_json),
        encoding="utf-8",
    )


def serialize_json(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "__dataclass_fields__"):
        return {key: serialize_json(val) for key, val in asdict(value).items()}
    if isinstance(value, tuple):
        return [serialize_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): serialize_json(val) for key, val in value.items()}
    return value
