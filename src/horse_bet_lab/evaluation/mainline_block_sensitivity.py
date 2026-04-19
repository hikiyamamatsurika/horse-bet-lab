from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from horse_bet_lab.config import (
    MainlineBlockSensitivityConfig,
    load_reference_bankroll_simulation_uncertainty_config,
    load_reference_label_guard_uncertainty_config,
)
from horse_bet_lab.evaluation.reference_bankroll_simulation_uncertainty import (
    BankrollSimulationUncertaintySummary,
    run_reference_bankroll_simulation_uncertainty,
)
from horse_bet_lab.evaluation.reference_label_guard_uncertainty import (
    run_reference_label_guard_uncertainty,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.reference_uncertainty import ReferenceUncertaintySummary


@dataclass(frozen=True)
class MainlineBlockSensitivitySummaryRow:
    scope: str
    block_unit: str
    initial_bankroll: float | None
    stake_variant: str | None
    roi_p02_5: float
    roi_p97_5: float
    total_profit_p02_5: float
    total_profit_p97_5: float
    max_drawdown_p02_5: float
    max_drawdown_p97_5: float
    roi_gt_1_ratio: float


@dataclass(frozen=True)
class MainlineBlockSensitivityDiffRow:
    scope: str
    baseline_block_unit: str
    compared_block_unit: str
    initial_bankroll: float | None
    stake_variant: str | None
    delta_roi_p02_5: float
    delta_roi_p97_5: float
    delta_total_profit_p02_5: float
    delta_total_profit_p97_5: float
    delta_max_drawdown_p02_5: float
    delta_max_drawdown_p97_5: float
    delta_roi_gt_1_ratio: float


@dataclass(frozen=True)
class MainlineBlockSensitivityResult:
    output_dir: Path
    summary_rows: tuple[MainlineBlockSensitivitySummaryRow, ...]
    diff_rows: tuple[MainlineBlockSensitivityDiffRow, ...]


def run_mainline_block_sensitivity(
    config: MainlineBlockSensitivityConfig,
) -> MainlineBlockSensitivityResult:
    summary_rows: list[MainlineBlockSensitivitySummaryRow] = []

    strategy_config = load_reference_label_guard_uncertainty_config(
        config.reference_label_guard_uncertainty_config_path,
    )
    stateful_config = load_reference_bankroll_simulation_uncertainty_config(
        config.reference_bankroll_simulation_uncertainty_config_path,
    )

    for block_unit in config.bootstrap_block_units:
        strategy_result = run_reference_label_guard_uncertainty(
            replace(
                strategy_config,
                output_dir=config.output_dir / "strategy" / block_unit,
                bootstrap_block_unit=block_unit,
            ),
        )
        summary_rows.append(
            build_strategy_summary_row(
                block_unit=block_unit,
                summary=strategy_result.summary,
            ),
        )

        stateful_result = run_reference_bankroll_simulation_uncertainty(
            replace(
                stateful_config,
                output_dir=config.output_dir / "stateful" / block_unit,
                bootstrap_block_unit=block_unit,
            ),
        )
        for row in stateful_result.summaries:
            if row.stake_variant != config.stateful_stake_variant:
                continue
            if row.initial_bankroll not in config.stateful_initial_bankrolls:
                continue
            summary_rows.append(
                build_stateful_summary_row(
                    block_unit=block_unit,
                    summary=row,
                ),
            )

    diff_rows = build_diff_rows(summary_rows, baseline_block_unit="race_date")
    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = MainlineBlockSensitivityResult(
        output_dir=config.output_dir,
        summary_rows=tuple(summary_rows),
        diff_rows=tuple(diff_rows),
    )
    write_csv(config.output_dir / "summary.csv", result.summary_rows)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summary_rows}})
    write_csv(config.output_dir / "diff_summary.csv", result.diff_rows)
    write_json(
        config.output_dir / "diff_summary.json",
        {"analysis": {"rows": result.diff_rows}},
    )
    return result


def build_strategy_summary_row(
    *,
    block_unit: str,
    summary: ReferenceUncertaintySummary,
) -> MainlineBlockSensitivitySummaryRow:
    return MainlineBlockSensitivitySummaryRow(
        scope="strategy",
        block_unit=block_unit,
        initial_bankroll=None,
        stake_variant=None,
        roi_p02_5=summary.roi_p02_5,
        roi_p97_5=summary.roi_p97_5,
        total_profit_p02_5=summary.total_profit_p02_5,
        total_profit_p97_5=summary.total_profit_p97_5,
        max_drawdown_p02_5=summary.max_drawdown_p02_5,
        max_drawdown_p97_5=summary.max_drawdown_p97_5,
        roi_gt_1_ratio=summary.roi_gt_1_ratio,
    )


def build_stateful_summary_row(
    *,
    block_unit: str,
    summary: BankrollSimulationUncertaintySummary,
) -> MainlineBlockSensitivitySummaryRow:
    return MainlineBlockSensitivitySummaryRow(
        scope="stateful",
        block_unit=block_unit,
        initial_bankroll=summary.initial_bankroll,
        stake_variant=summary.stake_variant,
        roi_p02_5=summary.roi_p02_5,
        roi_p97_5=summary.roi_p97_5,
        total_profit_p02_5=summary.total_profit_p02_5,
        total_profit_p97_5=summary.total_profit_p97_5,
        max_drawdown_p02_5=summary.max_drawdown_p02_5,
        max_drawdown_p97_5=summary.max_drawdown_p97_5,
        roi_gt_1_ratio=summary.roi_gt_1_ratio,
    )


def build_diff_rows(
    rows: list[MainlineBlockSensitivitySummaryRow],
    *,
    baseline_block_unit: str,
) -> list[MainlineBlockSensitivityDiffRow]:
    baseline_map = {
        build_summary_key(row): row
        for row in rows
        if row.block_unit == baseline_block_unit
    }
    output: list[MainlineBlockSensitivityDiffRow] = []
    for row in rows:
        if row.block_unit == baseline_block_unit:
            continue
        baseline = baseline_map.get(build_summary_key(row))
        if baseline is None:
            continue
        output.append(
            MainlineBlockSensitivityDiffRow(
                scope=row.scope,
                baseline_block_unit=baseline_block_unit,
                compared_block_unit=row.block_unit,
                initial_bankroll=row.initial_bankroll,
                stake_variant=row.stake_variant,
                delta_roi_p02_5=row.roi_p02_5 - baseline.roi_p02_5,
                delta_roi_p97_5=row.roi_p97_5 - baseline.roi_p97_5,
                delta_total_profit_p02_5=(
                    row.total_profit_p02_5 - baseline.total_profit_p02_5
                ),
                delta_total_profit_p97_5=(
                    row.total_profit_p97_5 - baseline.total_profit_p97_5
                ),
                delta_max_drawdown_p02_5=(
                    row.max_drawdown_p02_5 - baseline.max_drawdown_p02_5
                ),
                delta_max_drawdown_p97_5=(
                    row.max_drawdown_p97_5 - baseline.max_drawdown_p97_5
                ),
                delta_roi_gt_1_ratio=row.roi_gt_1_ratio - baseline.roi_gt_1_ratio,
            ),
        )
    return output


def build_summary_key(
    row: MainlineBlockSensitivitySummaryRow,
) -> tuple[str, float | None, str | None]:
    return (row.scope, row.initial_bankroll, row.stake_variant)
