from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.config import (
    SecondGuardSelectionConfig,
    load_ranking_rule_comparison_config,
    load_reference_guard_compare_config,
    load_reference_strategy_diagnostics_config,
)
from horse_bet_lab.evaluation.ranking_rule_rollforward import (
    build_selected_test_rows_by_window,
    run_ranking_rule_comparison,
)
from horse_bet_lab.evaluation.reference_guard_compare import (
    GuardVariantGroupSummary,
    GuardVariantRow,
    build_edge_threshold_by_window,
    build_group_summaries,
    build_variant_rows,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json


@dataclass(frozen=True)
class SecondGuardCandidateSummary:
    selection_window_label: str
    candidate_second_guard: str
    applied_to_split: str
    valid_window_labels: str
    test_window_label: str
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
class SecondGuardSelectedSummary:
    selection_window_label: str
    applied_to_split: str
    selected_second_guard: str
    valid_window_labels: str
    test_window_label: str
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
class SecondGuardSelectionResult:
    output_dir: Path
    candidate_summaries: tuple[SecondGuardCandidateSummary, ...]
    selected_summaries: tuple[SecondGuardSelectedSummary, ...]
    selected_test_rollup: tuple[GuardVariantGroupSummary, ...]
    selected_test_rows: tuple[GuardVariantRow, ...]
    candidate_rows: dict[str, tuple[GuardVariantRow, ...]]


def run_second_guard_selection(
    config: SecondGuardSelectionConfig,
) -> SecondGuardSelectionResult:
    guard_config = load_reference_guard_compare_config(
        config.reference_guard_compare_config_path,
    )
    reference_config = load_reference_strategy_diagnostics_config(
        guard_config.reference_strategy_config_path,
    )
    ranking_config = load_ranking_rule_comparison_config(
        reference_config.ranking_rule_comparison_config_path,
    )
    ranking_result = run_ranking_rule_comparison(ranking_config)
    selected_rows_by_window = build_selected_test_rows_by_window(
        selected_summaries=ranking_result.selected_summaries,
        selected_rows_by_candidate=ranking_result.selected_rows_by_candidate,
    )
    edge_threshold_by_window = build_edge_threshold_by_window(
        ranking_result.selected_summaries,
    )
    variant_rows = build_variant_rows(
        config=guard_config,
        selected_rows_by_window=selected_rows_by_window,
        edge_threshold_by_window=edge_threshold_by_window,
    )

    first_guard_rows = variant_rows[config.first_guard_variant]
    candidate_rows: dict[str, tuple[GuardVariantRow, ...]] = {}
    candidate_rows["no_second_guard"] = first_guard_rows
    for variant_name in config.second_guard_variants:
        if variant_name == "no_second_guard":
            continue
        candidate_rows[variant_name] = variant_rows[variant_name]

    candidate_summaries: list[SecondGuardCandidateSummary] = []
    selected_summaries: list[SecondGuardSelectedSummary] = []
    selected_test_rows: list[GuardVariantRow] = []
    candidate_order = {name: index for index, name in enumerate(config.second_guard_variants)}
    test_to_selection_label = {
        test_window_label: group_label
        for group_label, _, test_window_label in ranking_config.selection_window_groups
    }

    for (
        group_label,
        valid_window_labels,
        test_window_label,
    ) in ranking_config.selection_window_groups:
        valid_selection_labels = tuple(
            test_to_selection_label[label]
            for label in valid_window_labels
            if label in test_to_selection_label
        )
        valid_labels_text = ",".join(valid_window_labels)
        valid_candidates: list[tuple[str, GuardVariantGroupSummary]] = []
        for candidate_name in config.second_guard_variants:
            rows = candidate_rows[candidate_name]
            valid_rows = tuple(
                row for row in rows if row.window_label in valid_selection_labels
            )
            valid_summary = summarize_rows(
                variant=candidate_name,
                group_key="valid_aggregate",
                rows=valid_rows,
                stake_per_bet=ranking_config.stake_per_bet,
            )
            candidate_summaries.append(
                SecondGuardCandidateSummary(
                    selection_window_label=group_label,
                    candidate_second_guard=candidate_name,
                    applied_to_split="valid_aggregate",
                    valid_window_labels=valid_labels_text,
                    test_window_label=test_window_label,
                    bet_count=valid_summary.bet_count,
                    hit_count=valid_summary.hit_count,
                    hit_rate=valid_summary.hit_rate,
                    roi=valid_summary.roi,
                    total_profit=valid_summary.total_profit,
                    avg_payout=valid_summary.avg_payout,
                    avg_edge=valid_summary.avg_edge,
                    max_drawdown=valid_summary.max_drawdown,
                    max_losing_streak=valid_summary.max_losing_streak,
                ),
            )
            if (
                ranking_config.min_bets_valid is not None
                and valid_summary.bet_count < ranking_config.min_bets_valid
            ):
                continue
            valid_candidates.append((candidate_name, valid_summary))
        if not valid_candidates:
            continue

        selected_guard_name, selected_valid_summary = max(
            valid_candidates,
            key=lambda item: (
                item[1].roi,
                item[1].bet_count,
                -candidate_order[item[0]],
            ),
        )
        selected_summaries.append(
            SecondGuardSelectedSummary(
                selection_window_label=group_label,
                applied_to_split="valid_aggregate",
                selected_second_guard=selected_guard_name,
                valid_window_labels=valid_labels_text,
                test_window_label=test_window_label,
                bet_count=selected_valid_summary.bet_count,
                hit_count=selected_valid_summary.hit_count,
                hit_rate=selected_valid_summary.hit_rate,
                roi=selected_valid_summary.roi,
                total_profit=selected_valid_summary.total_profit,
                avg_payout=selected_valid_summary.avg_payout,
                avg_edge=selected_valid_summary.avg_edge,
                max_drawdown=selected_valid_summary.max_drawdown,
                max_losing_streak=selected_valid_summary.max_losing_streak,
            ),
        )

        selected_test = tuple(
            row
            for row in candidate_rows[selected_guard_name]
            if row.window_label == group_label
        )
        selected_test_rows.extend(selected_test)
        test_summary = summarize_rows(
            variant=selected_guard_name,
            group_key=test_window_label,
            rows=selected_test,
            stake_per_bet=ranking_config.stake_per_bet,
        )
        candidate_summaries.append(
            SecondGuardCandidateSummary(
                selection_window_label=group_label,
                candidate_second_guard=selected_guard_name,
                applied_to_split="test",
                valid_window_labels=valid_labels_text,
                test_window_label=test_window_label,
                bet_count=test_summary.bet_count,
                hit_count=test_summary.hit_count,
                hit_rate=test_summary.hit_rate,
                roi=test_summary.roi,
                total_profit=test_summary.total_profit,
                avg_payout=test_summary.avg_payout,
                avg_edge=test_summary.avg_edge,
                max_drawdown=test_summary.max_drawdown,
                max_losing_streak=test_summary.max_losing_streak,
            ),
        )
        selected_summaries.append(
            SecondGuardSelectedSummary(
                selection_window_label=group_label,
                applied_to_split="test",
                selected_second_guard=selected_guard_name,
                valid_window_labels=valid_labels_text,
                test_window_label=test_window_label,
                bet_count=test_summary.bet_count,
                hit_count=test_summary.hit_count,
                hit_rate=test_summary.hit_rate,
                roi=test_summary.roi,
                total_profit=test_summary.total_profit,
                avg_payout=test_summary.avg_payout,
                avg_edge=test_summary.avg_edge,
                max_drawdown=test_summary.max_drawdown,
                max_losing_streak=test_summary.max_losing_streak,
            ),
        )

    selected_test_rollup = build_selected_test_rollup(
        rows=tuple(selected_test_rows),
        stake_per_bet=ranking_config.stake_per_bet,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = SecondGuardSelectionResult(
        output_dir=config.output_dir,
        candidate_summaries=tuple(candidate_summaries),
        selected_summaries=tuple(selected_summaries),
        selected_test_rollup=selected_test_rollup,
        selected_test_rows=tuple(selected_test_rows),
        candidate_rows=candidate_rows,
    )
    write_csv(config.output_dir / "candidate_summary.csv", result.candidate_summaries)
    write_json(
        config.output_dir / "candidate_summary.json",
        {"analysis": {"rows": result.candidate_summaries}},
    )
    write_csv(config.output_dir / "selected_summary.csv", result.selected_summaries)
    write_json(
        config.output_dir / "selected_summary.json",
        {"analysis": {"rows": result.selected_summaries}},
    )
    write_csv(config.output_dir / "selected_test_rollup.csv", result.selected_test_rollup)
    write_json(
        config.output_dir / "selected_test_rollup.json",
        {"analysis": {"rows": result.selected_test_rollup}},
    )
    return result


def summarize_rows(
    *,
    variant: str,
    group_key: str,
    rows: tuple[GuardVariantRow, ...],
    stake_per_bet: float,
) -> GuardVariantGroupSummary:
    summaries = build_group_summaries(
        variant=variant,
        rows=rows,
        stake_per_bet=stake_per_bet,
        key_fn=lambda item: group_key,
    )
    if summaries:
        return summaries[0]
    return GuardVariantGroupSummary(
        variant=variant,
        group_key=group_key,
        bet_count=0,
        hit_count=0,
        hit_rate=0.0,
        roi=0.0,
        total_profit=0.0,
        avg_payout=0.0,
        avg_edge=0.0,
        refund_count=0,
        refund_ratio=0.0,
        payout_100_110_count=0,
        payout_100_110_ratio=0.0,
        payout_le_120_count=0,
        payout_le_120_ratio=0.0,
        max_drawdown=0.0,
        max_losing_streak=0,
    )


def build_selected_test_rollup(
    *,
    rows: tuple[GuardVariantRow, ...],
    stake_per_bet: float,
) -> tuple[GuardVariantGroupSummary, ...]:
    overall = summarize_rows(
        variant="selected_per_window",
        group_key="all_oos",
        rows=rows,
        stake_per_bet=stake_per_bet,
    )
    yearly = build_group_summaries(
        variant="selected_per_window",
        rows=rows,
        stake_per_bet=stake_per_bet,
        key_fn=lambda item: item.row.result_date.isoformat()[:4],
    )
    return (overall,) + yearly
