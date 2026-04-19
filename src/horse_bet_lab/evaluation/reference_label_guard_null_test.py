from __future__ import annotations

import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from horse_bet_lab.config import (
    ReferenceLabelGuardNullTestConfig,
    load_reference_label_guard_compare_config,
)
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow
from horse_bet_lab.evaluation.reference_label_guard_compare import (
    ReferenceLabelGuardSelectionWindow,
    apply_extra_guard,
    load_reference_label_guard_selection_windows,
    permute_guard_row_labels_within_race,
    run_reference_label_guard_compare,
    select_extra_guard_for_window,
    shuffle_guard_row_labels,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.reference_uncertainty import quantile
from horse_bet_lab.evaluation.second_guard_selection import build_selected_test_rollup


@dataclass(frozen=True)
class ReferenceLabelGuardNullSummary:
    null_mode: str
    strategy_name: str
    observed_test_roi: float
    observed_test_profit: float
    observed_test_bet_count: int
    null_iterations: int
    null_test_roi_p02_5: float
    null_test_roi_p50: float
    null_test_roi_p97_5: float
    null_test_profit_p02_5: float
    null_test_profit_p50: float
    null_test_profit_p97_5: float
    observed_test_roi_percentile: float
    observed_test_profit_percentile: float
    observed_minus_null_median_roi: float
    observed_minus_null_median_profit: float


@dataclass(frozen=True)
class ReferenceLabelGuardNullDistributionRow:
    null_mode: str
    iteration: int
    test_roi: float
    test_profit: float


@dataclass(frozen=True)
class ReferenceLabelGuardNullSelectionRow:
    null_mode: str
    iteration: int
    selection_window_label: str
    selected_extra_guard: str


@dataclass(frozen=True)
class ReferenceLabelGuardNullTestResult:
    output_dir: Path
    summaries: tuple[ReferenceLabelGuardNullSummary, ...]
    null_distribution_rows: tuple[ReferenceLabelGuardNullDistributionRow, ...]
    null_selection_rows: tuple[ReferenceLabelGuardNullSelectionRow, ...]


def run_reference_label_guard_null_test(
    config: ReferenceLabelGuardNullTestConfig,
) -> ReferenceLabelGuardNullTestResult:
    compare_config = load_reference_label_guard_compare_config(
        config.reference_label_guard_compare_config_path,
    )
    compare_result = run_reference_label_guard_compare(compare_config)
    observed_rollup = next(
        row for row in compare_result.selected_test_rollup if row.group_key == "all_oos"
    )
    ranking_config, selection_windows = load_reference_label_guard_selection_windows(
        compare_config,
    )
    candidate_order = {
        name: index for index, name in enumerate(compare_config.extra_guard_variants)
    }

    summaries: list[ReferenceLabelGuardNullSummary] = []
    null_distribution_rows: list[ReferenceLabelGuardNullDistributionRow] = []
    null_selection_rows: list[ReferenceLabelGuardNullSelectionRow] = []
    for mode_index, null_mode in enumerate(config.null_modes):
        rng = random.Random(config.random_seed + mode_index)
        for iteration in range(1, config.null_iterations + 1):
            selected_test_rows = build_null_selected_test_rows(
                ranking_config=ranking_config,
                selection_windows=selection_windows,
                candidate_order=candidate_order,
                extra_guard_variants=compare_config.extra_guard_variants,
                rng=rng,
                selection_output=null_selection_rows,
                iteration=iteration,
                null_mode=null_mode,
            )
            test_rollup = build_selected_test_rollup(
                rows=selected_test_rows,
                stake_per_bet=ranking_config.stake_per_bet,
            )
            overall = next(row for row in test_rollup if row.group_key == "all_oos")
            null_distribution_rows.append(
                ReferenceLabelGuardNullDistributionRow(
                    null_mode=null_mode,
                    iteration=iteration,
                    test_roi=overall.roi,
                    test_profit=overall.total_profit,
                ),
            )

        mode_rows = [row for row in null_distribution_rows if row.null_mode == null_mode]
        roi_values = [row.test_roi for row in mode_rows]
        profit_values = [row.test_profit for row in mode_rows]
        summaries.append(
            ReferenceLabelGuardNullSummary(
                null_mode=null_mode,
                strategy_name="reference_first_guard_plus_valid_selected_label_guard_null_test",
                observed_test_roi=observed_rollup.roi,
                observed_test_profit=observed_rollup.total_profit,
                observed_test_bet_count=observed_rollup.bet_count,
                null_iterations=config.null_iterations,
                null_test_roi_p02_5=quantile(roi_values, 0.025),
                null_test_roi_p50=quantile(roi_values, 0.5),
                null_test_roi_p97_5=quantile(roi_values, 0.975),
                null_test_profit_p02_5=quantile(profit_values, 0.025),
                null_test_profit_p50=quantile(profit_values, 0.5),
                null_test_profit_p97_5=quantile(profit_values, 0.975),
                observed_test_roi_percentile=percentile_rank(
                    roi_values,
                    observed_rollup.roi,
                ),
                observed_test_profit_percentile=percentile_rank(
                    profit_values,
                    observed_rollup.total_profit,
                ),
                observed_minus_null_median_roi=observed_rollup.roi - quantile(roi_values, 0.5),
                observed_minus_null_median_profit=(
                    observed_rollup.total_profit - quantile(profit_values, 0.5)
                ),
            ),
        )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceLabelGuardNullTestResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        null_distribution_rows=tuple(null_distribution_rows),
        null_selection_rows=tuple(null_selection_rows),
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(
        config.output_dir / "null_distribution.csv",
        result.null_distribution_rows,
    )
    write_json(
        config.output_dir / "null_distribution.json",
        {"analysis": {"rows": result.null_distribution_rows}},
    )
    write_csv(
        config.output_dir / "null_selected_guards.csv",
        result.null_selection_rows,
    )
    write_json(
        config.output_dir / "null_selected_guards.json",
        {"analysis": {"rows": result.null_selection_rows}},
    )
    return result


def build_null_selected_test_rows(
    *,
    ranking_config: Any,
    selection_windows: tuple[ReferenceLabelGuardSelectionWindow, ...],
    candidate_order: dict[str, int],
    extra_guard_variants: tuple[str, ...],
    rng: random.Random,
    selection_output: list[ReferenceLabelGuardNullSelectionRow],
    iteration: int,
    null_mode: str,
) -> tuple[GuardVariantRow, ...]:
    selected_test_rows: list[GuardVariantRow] = []
    for selection_window in selection_windows:
        shuffled_valid_rows = shuffle_valid_rows_for_null_mode(
            selection_window.valid_base_rows,
            rng,
            null_mode,
        )
        shuffled_window = ReferenceLabelGuardSelectionWindow(
            selection_window_label=selection_window.selection_window_label,
            selected_second_guard=selection_window.selected_second_guard,
            valid_window_labels=selection_window.valid_window_labels,
            raw_test_window_label=selection_window.raw_test_window_label,
            valid_base_rows=shuffled_valid_rows,
            test_base_rows=selection_window.test_base_rows,
        )
        selected_extra_guard, _, _ = select_extra_guard_for_window(
            ranking_config=ranking_config,
            selection_window=shuffled_window,
            candidate_order=candidate_order,
            extra_guard_variants=extra_guard_variants,
        )
        if selected_extra_guard is None:
            continue
        selection_output.append(
            ReferenceLabelGuardNullSelectionRow(
                null_mode=null_mode,
                iteration=iteration,
                selection_window_label=selection_window.selection_window_label,
                selected_extra_guard=selected_extra_guard,
            ),
        )
        selected_test_rows.extend(
            apply_extra_guard(selected_extra_guard, selection_window.test_base_rows),
        )
    return tuple(selected_test_rows)


def percentile_rank(values: list[float], observed: float) -> float:
    if not values:
        return 0.0
    return sum(1 for value in values if value <= observed) / len(values)


def shuffle_valid_rows_for_null_mode(
    rows: tuple[GuardVariantRow, ...],
    rng: random.Random,
    null_mode: str,
) -> tuple[GuardVariantRow, ...]:
    if null_mode == "current_shuffle":
        return shuffle_guard_row_labels(rows, rng)
    if null_mode == "race_internal_permutation":
        return permute_guard_row_labels_within_race(rows, rng)
    raise ValueError(f"Unsupported null mode: {null_mode}")
