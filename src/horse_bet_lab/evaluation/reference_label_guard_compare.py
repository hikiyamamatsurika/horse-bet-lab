from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from random import Random
from typing import Any

from horse_bet_lab.config import (
    ReferenceLabelGuardCompareConfig,
    load_ranking_rule_comparison_config,
    load_reference_guard_compare_config,
    load_reference_strategy_diagnostics_config,
    load_second_guard_selection_config,
)
from horse_bet_lab.evaluation.reference_guard_compare import (
    GuardVariantGroupSummary,
    GuardVariantRow,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.second_guard_selection import (
    build_selected_test_rollup,
    run_second_guard_selection,
    summarize_rows,
)


@dataclass(frozen=True)
class ReferenceLabelGuardCandidateSummary:
    selection_window_label: str
    candidate_extra_guard: str
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
class ReferenceLabelGuardSelectedSummary:
    selection_window_label: str
    applied_to_split: str
    selected_second_guard: str
    selected_extra_guard: str
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
class ReferenceLabelGuardCompareResult:
    output_dir: Path
    candidate_summaries: tuple[ReferenceLabelGuardCandidateSummary, ...]
    candidate_test_rollups: tuple[GuardVariantGroupSummary, ...]
    selected_summaries: tuple[ReferenceLabelGuardSelectedSummary, ...]
    selected_test_rollup: tuple[GuardVariantGroupSummary, ...]
    selected_test_rows: tuple[GuardVariantRow, ...]


@dataclass(frozen=True)
class ReferenceLabelGuardSelectionWindow:
    selection_window_label: str
    selected_second_guard: str
    valid_window_labels: str
    raw_test_window_label: str
    valid_base_rows: tuple[GuardVariantRow, ...]
    test_base_rows: tuple[GuardVariantRow, ...]


def run_reference_label_guard_compare(
    config: ReferenceLabelGuardCompareConfig,
) -> ReferenceLabelGuardCompareResult:
    ranking_config, selection_windows = load_reference_label_guard_selection_windows(
        config,
    )
    candidate_order = {name: index for index, name in enumerate(config.extra_guard_variants)}

    candidate_summaries: list[ReferenceLabelGuardCandidateSummary] = []
    candidate_test_rows: dict[str, list[GuardVariantRow]] = {
        variant: [] for variant in config.extra_guard_variants
    }
    selected_summaries: list[ReferenceLabelGuardSelectedSummary] = []
    selected_test_rows: list[GuardVariantRow] = []

    for selection_window in selection_windows:
        (
            selected_extra_guard,
            selected_valid_summary,
            valid_candidate_details,
        ) = select_extra_guard_for_window(
            ranking_config=ranking_config,
            selection_window=selection_window,
            candidate_order=candidate_order,
            extra_guard_variants=config.extra_guard_variants,
        )
        for candidate_name, valid_summary in valid_candidate_details:
            candidate_summaries.append(
                ReferenceLabelGuardCandidateSummary(
                    selection_window_label=selection_window.selection_window_label,
                    candidate_extra_guard=candidate_name,
                    applied_to_split="valid_aggregate",
                    selected_second_guard=selection_window.selected_second_guard,
                    valid_window_labels=selection_window.valid_window_labels,
                    test_window_label=selection_window.raw_test_window_label,
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

        if selected_extra_guard is None or selected_valid_summary is None:
            continue

        selected_summaries.append(
            ReferenceLabelGuardSelectedSummary(
                selection_window_label=selection_window.selection_window_label,
                applied_to_split="valid_aggregate",
                selected_second_guard=selection_window.selected_second_guard,
                selected_extra_guard=selected_extra_guard,
                valid_window_labels=selection_window.valid_window_labels,
                test_window_label=selection_window.raw_test_window_label,
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

        selected_test = apply_extra_guard(selected_extra_guard, selection_window.test_base_rows)
        selected_test_rows.extend(selected_test)
        for candidate_name in config.extra_guard_variants:
            candidate_test_rows[candidate_name].extend(
                apply_extra_guard(candidate_name, selection_window.test_base_rows),
            )
        test_summary = summarize_rows(
            variant=selected_extra_guard,
            group_key=selection_window.raw_test_window_label,
            rows=selected_test,
            stake_per_bet=ranking_config.stake_per_bet,
        )
        candidate_summaries.append(
            ReferenceLabelGuardCandidateSummary(
                selection_window_label=selection_window.selection_window_label,
                candidate_extra_guard=selected_extra_guard,
                applied_to_split="test",
                selected_second_guard=selection_window.selected_second_guard,
                valid_window_labels=selection_window.valid_window_labels,
                test_window_label=selection_window.raw_test_window_label,
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
            ReferenceLabelGuardSelectedSummary(
                selection_window_label=selection_window.selection_window_label,
                applied_to_split="test",
                selected_second_guard=selection_window.selected_second_guard,
                selected_extra_guard=selected_extra_guard,
                valid_window_labels=selection_window.valid_window_labels,
                test_window_label=selection_window.raw_test_window_label,
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
    candidate_test_rollups = build_candidate_test_rollups(
        candidate_test_rows=candidate_test_rows,
        stake_per_bet=ranking_config.stake_per_bet,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceLabelGuardCompareResult(
        output_dir=config.output_dir,
        candidate_summaries=tuple(candidate_summaries),
        candidate_test_rollups=candidate_test_rollups,
        selected_summaries=tuple(selected_summaries),
        selected_test_rollup=selected_test_rollup,
        selected_test_rows=tuple(selected_test_rows),
    )
    write_csv(config.output_dir / "candidate_summary.csv", result.candidate_summaries)
    write_json(
        config.output_dir / "candidate_summary.json",
        {"analysis": {"rows": result.candidate_summaries}},
    )
    write_csv(config.output_dir / "candidate_test_rollup.csv", result.candidate_test_rollups)
    write_json(
        config.output_dir / "candidate_test_rollup.json",
        {"analysis": {"rows": result.candidate_test_rollups}},
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


def load_reference_label_guard_selection_windows(
    config: ReferenceLabelGuardCompareConfig,
) -> tuple[Any, tuple[ReferenceLabelGuardSelectionWindow, ...]]:
    selection_config = load_second_guard_selection_config(
        config.second_guard_selection_config_path,
    )
    selection_result = run_second_guard_selection(selection_config)
    guard_config = load_reference_guard_compare_config(
        selection_config.reference_guard_compare_config_path,
    )
    reference_config = load_reference_strategy_diagnostics_config(
        guard_config.reference_strategy_config_path,
    )
    ranking_config = load_ranking_rule_comparison_config(
        reference_config.ranking_rule_comparison_config_path,
    )
    raw_test_to_selection = {
        test_window_label: group_label
        for group_label, _, test_window_label in ranking_config.selection_window_groups
    }

    selection_windows: list[ReferenceLabelGuardSelectionWindow] = []
    for summary in selection_result.selected_summaries:
        if summary.applied_to_split != "valid_aggregate":
            continue
        valid_selection_labels = tuple(
            raw_test_to_selection[label]
            for label in summary.valid_window_labels.split(",")
            if label and label in raw_test_to_selection
        )
        base_rows = selection_result.candidate_rows[summary.selected_second_guard]
        selection_windows.append(
            ReferenceLabelGuardSelectionWindow(
                selection_window_label=summary.selection_window_label,
                selected_second_guard=summary.selected_second_guard,
                valid_window_labels=summary.valid_window_labels,
                raw_test_window_label=summary.test_window_label,
                valid_base_rows=tuple(
                    row for row in base_rows if row.window_label in valid_selection_labels
                ),
                test_base_rows=tuple(
                    row
                    for row in base_rows
                    if row.window_label == summary.selection_window_label
                ),
            ),
        )
    selection_windows.sort(key=lambda item: item.selection_window_label)
    return ranking_config, tuple(selection_windows)


def select_extra_guard_for_window(
    *,
    ranking_config: Any,
    selection_window: ReferenceLabelGuardSelectionWindow,
    candidate_order: dict[str, int],
    extra_guard_variants: tuple[str, ...],
) -> tuple[
    str | None,
    GuardVariantGroupSummary | None,
    tuple[tuple[str, GuardVariantGroupSummary], ...],
]:
    valid_candidates: list[tuple[str, GuardVariantGroupSummary]] = []
    valid_candidate_details: list[tuple[str, GuardVariantGroupSummary]] = []
    for candidate_name in extra_guard_variants:
        valid_candidate_rows = apply_extra_guard(
            candidate_name,
            selection_window.valid_base_rows,
        )
        valid_summary = summarize_rows(
            variant=candidate_name,
            group_key="valid_aggregate",
            rows=valid_candidate_rows,
            stake_per_bet=ranking_config.stake_per_bet,
        )
        valid_candidate_details.append((candidate_name, valid_summary))
        if (
            ranking_config.min_bets_valid is not None
            and valid_summary.bet_count < ranking_config.min_bets_valid
        ):
            continue
        valid_candidates.append((candidate_name, valid_summary))

    if not valid_candidates:
        return None, None, tuple(valid_candidate_details)

    selected_extra_guard, selected_valid_summary = max(
        valid_candidates,
        key=lambda item: (
            item[1].roi,
            item[1].bet_count,
            -candidate_order[item[0]],
        ),
    )
    return selected_extra_guard, selected_valid_summary, tuple(valid_candidate_details)


def shuffle_guard_row_labels(
    rows: tuple[GuardVariantRow, ...],
    rng: Random,
) -> tuple[GuardVariantRow, ...]:
    donor_rows = list(rows)
    rng.shuffle(donor_rows)
    return tuple(
        GuardVariantRow(
            variant=row.variant,
            window_label=row.window_label,
            row=replace(
                row.row,
                target_value=donor.row.target_value,
                place_payout=donor.row.place_payout,
            ),
        )
        for row, donor in zip(rows, donor_rows, strict=True)
    )


def permute_guard_row_labels_within_race(
    rows: tuple[GuardVariantRow, ...],
    rng: Random,
) -> tuple[GuardVariantRow, ...]:
    grouped: dict[str, list[GuardVariantRow]] = {}
    for row in rows:
        grouped.setdefault(row.row.race_key, []).append(row)

    donor_map: dict[tuple[str, int], GuardVariantRow] = {}
    for race_rows in grouped.values():
        donors = list(race_rows)
        rng.shuffle(donors)
        for row, donor in zip(race_rows, donors, strict=True):
            donor_map[(row.row.race_key, row.row.horse_number)] = donor

    return tuple(
        GuardVariantRow(
            variant=row.variant,
            window_label=row.window_label,
            row=replace(
                row.row,
                target_value=donor_map[(row.row.race_key, row.row.horse_number)].row.target_value,
                place_payout=donor_map[(row.row.race_key, row.row.horse_number)].row.place_payout,
            ),
        )
        for row in rows
    )


def build_candidate_test_rollups(
    *,
    candidate_test_rows: dict[str, list[GuardVariantRow]],
    stake_per_bet: float,
) -> tuple[GuardVariantGroupSummary, ...]:
    output: list[GuardVariantGroupSummary] = []
    for candidate_name in sorted(candidate_test_rows):
        rows = tuple(candidate_test_rows[candidate_name])
        overall = summarize_rows(
            variant=candidate_name,
            group_key="all_oos",
            rows=rows,
            stake_per_bet=stake_per_bet,
        )
        output.append(overall)
        years = sorted({row.row.result_date.isoformat()[:4] for row in rows})
        for year in years:
            year_rows = tuple(
                row for row in rows if row.row.result_date.isoformat()[:4] == year
            )
            output.append(
                summarize_rows(
                    variant=candidate_name,
                    group_key=year,
                    rows=year_rows,
                    stake_per_bet=stake_per_bet,
                ),
            )
    return tuple(output)


def apply_extra_guard(
    variant_name: str,
    rows: tuple[GuardVariantRow, ...],
) -> tuple[GuardVariantRow, ...]:
    if variant_name == "no_extra_label_guard":
        return rows
    if variant_name == "popularity_3_4_excluded":
        return tuple(
            row
            for row in rows
            if row.row.popularity is None or row.row.popularity not in {3, 4}
        )
    if variant_name == "month_07_08_excluded":
        return tuple(
            row
            for row in rows
            if row.row.result_date.month not in {7, 8}
        )
    if variant_name == "popularity_3_4_or_month_07_08_excluded":
        return tuple(
            row
            for row in rows
            if (
                row.row.result_date.month not in {7, 8}
                and (row.row.popularity is None or row.row.popularity not in {3, 4})
            )
        )
    raise ValueError(f"unsupported extra guard variant: {variant_name}")
