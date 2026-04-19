from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.config import (
    ReferenceLabelGuardUncertaintyConfig,
    load_reference_label_guard_compare_config,
)
from horse_bet_lab.evaluation.reference_label_guard_compare import (
    run_reference_label_guard_compare,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.reference_uncertainty import (
    ReferenceBootstrapRow,
    ReferenceUncertaintySummary,
    ReferenceYearContributionRow,
    build_bootstrap_rows,
    build_year_contributions,
    quantile,
    summarize_rows,
)


@dataclass(frozen=True)
class ReferenceLabelGuardUncertaintyResult:
    output_dir: Path
    summary: ReferenceUncertaintySummary
    bootstrap_rows: tuple[ReferenceBootstrapRow, ...]
    year_contributions: tuple[ReferenceYearContributionRow, ...]


def run_reference_label_guard_uncertainty(
    config: ReferenceLabelGuardUncertaintyConfig,
) -> ReferenceLabelGuardUncertaintyResult:
    compare_config = load_reference_label_guard_compare_config(
        config.reference_label_guard_compare_config_path,
    )
    compare_result = run_reference_label_guard_compare(compare_config)
    rows = compare_result.selected_test_rows

    actual = summarize_rows(rows)
    bootstrap_rows = build_bootstrap_rows(
        rows=rows,
        iterations=config.bootstrap_iterations,
        random_seed=config.random_seed,
        block_unit=config.bootstrap_block_unit,
    )
    roi_values = [row.roi for row in bootstrap_rows]
    profit_values = [row.total_profit for row in bootstrap_rows]
    dd_values = [row.max_drawdown for row in bootstrap_rows]
    summary = ReferenceUncertaintySummary(
        strategy_name="reference_first_guard_plus_valid_selected_label_guard",
        bet_count=actual.bet_count,
        hit_count=actual.hit_count,
        hit_rate=actual.hit_rate,
        roi=actual.roi,
        total_profit=actual.total_profit,
        avg_payout=actual.avg_payout,
        avg_edge=actual.avg_edge,
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
    )
    year_contributions = build_year_contributions(rows)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferenceLabelGuardUncertaintyResult(
        output_dir=config.output_dir,
        summary=summary,
        bootstrap_rows=tuple(bootstrap_rows),
        year_contributions=year_contributions,
    )
    write_csv(config.output_dir / "summary.csv", (result.summary,))
    write_json(config.output_dir / "summary.json", {"analysis": result.summary})
    write_csv(config.output_dir / "bootstrap_distribution.csv", result.bootstrap_rows)
    write_json(
        config.output_dir / "bootstrap_distribution.json",
        {"analysis": {"rows": result.bootstrap_rows}},
    )
    write_csv(config.output_dir / "yearly_contribution.csv", result.year_contributions)
    write_json(
        config.output_dir / "yearly_contribution.json",
        {"analysis": {"rows": result.year_contributions}},
    )
    return result
