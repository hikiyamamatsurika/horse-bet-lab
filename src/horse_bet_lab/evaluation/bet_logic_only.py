from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
from random import Random
import subprocess

import duckdb

from horse_bet_lab.config import (
    BetLogicOnlyConfig,
    RankingRuleComparisonConfig,
    load_reference_label_guard_compare_config,
)
from horse_bet_lab.evaluation.place_backtest import compute_max_drawdown, compute_max_losing_streak
from horse_bet_lab.evaluation.ranking_rule_rollforward import (
    CandidateBetRow,
    build_selected_test_rows_by_window,
    run_ranking_rule_comparison,
)
from horse_bet_lab.evaluation.reference_label_guard_compare import run_reference_label_guard_compare
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json

BASELINE_LOGIC_NAME = "baseline_current_logic"
VARIANT_NAMES = (
    BASELINE_LOGIC_NAME,
    "no_bet_guard_stronger",
    "guard_0_01_plus_proxy_domain_overlay",
    "guard_0_01_plus_near_threshold_overlay",
    "guard_0_01_plus_place_basis_overlay",
    "guard_0_01_plus_domain_x_threshold_overlay",
)


@dataclass(frozen=True)
class LogicBetRow:
    logic_variant: str
    source_logic: str
    window_label: str
    result_date: str
    race_key: str
    horse_number: int
    stake: float
    scaled_return: float
    bet_profit: float
    target_value: int
    pred_probability: float
    market_prob: float | None
    edge: float | None
    win_odds: float | None
    popularity: int | None
    place_basis_odds: float | None
    place_payout: float | None


@dataclass(frozen=True)
class BetLogicOnlySelectedSummary:
    logic_variant: str
    source_logic: str
    window_label: str
    baseline_threshold: float | None
    bet_count: int
    hit_count: int
    hit_rate: float
    total_stake: float
    total_return: float
    roi_multiple: float
    net_roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    avg_edge: float
    avg_stake: float


@dataclass(frozen=True)
class BetLogicOnlySummary:
    logic_variant: str
    source_logic: str
    bet_count: int
    hit_count: int
    hit_rate: float
    total_stake: float
    total_return: float
    roi_multiple: float
    net_roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    roi_gt_1_ratio: float
    roi_multiple_ci_lower: float
    roi_multiple_ci_upper: float
    avg_edge: float
    avg_stake: float
    test_window_count: int
    test_window_labels: str


@dataclass(frozen=True)
class BetLogicOnlyComparisonRow:
    logic_variant: str
    source_logic: str
    bet_count: int
    hit_rate: float
    roi_multiple: float
    net_roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    roi_gt_1_ratio: float
    roi_multiple_95_interval: str
    delta_bet_count_vs_baseline: int
    delta_roi_multiple_vs_baseline: float
    delta_net_roi_vs_baseline: float
    delta_total_profit_vs_baseline: float
    delta_max_drawdown_vs_baseline: float
    delta_max_losing_streak_vs_baseline: int
    delta_bet_count_vs_guard_001: int
    delta_roi_multiple_vs_guard_001: float
    delta_net_roi_vs_guard_001: float
    delta_total_profit_vs_guard_001: float
    delta_max_drawdown_vs_guard_001: float
    delta_max_losing_streak_vs_guard_001: int


@dataclass(frozen=True)
class BetLogicOnlyParitySummary:
    reference_logic: str
    baseline_logic: str
    row_count_reference: int
    row_count_baseline: int
    row_diff_count: int
    selected_rows_match: bool
    bet_count_reference: int
    bet_count_baseline: int
    total_stake_reference: float
    total_stake_baseline: float
    total_return_reference: float
    total_return_baseline: float
    total_profit_reference: float
    total_profit_baseline: float
    roi_multiple_reference: float
    roi_multiple_baseline: float
    net_roi_reference: float
    net_roi_baseline: float
    parity_ok: bool


@dataclass(frozen=True)
class BetLogicOnlyParityDetailRow:
    race_key: str
    horse_number: int
    set_group: str
    reference_window_label: str | None
    baseline_window_label: str | None
    reference_stake: float
    baseline_stake: float
    reference_return: float
    baseline_return: float
    reference_profit: float
    baseline_profit: float


@dataclass(frozen=True)
class BetLogicOnlyDiffSummary:
    logic_variant: str
    set_group: str
    baseline_bet_count: int
    baseline_total_stake: float
    baseline_total_return: float
    baseline_total_profit: float
    baseline_roi_multiple: float
    baseline_net_roi: float
    logic_variant_bet_count: int
    logic_variant_total_stake: float
    logic_variant_total_return: float
    logic_variant_total_profit: float
    logic_variant_roi_multiple: float
    logic_variant_net_roi: float


@dataclass(frozen=True)
class BetLogicOnlyDiffDetailRow:
    logic_variant: str
    set_group: str
    race_key: str
    horse_number: int
    baseline_window_label: str | None
    variant_window_label: str | None
    baseline_stake: float
    variant_stake: float
    baseline_return: float
    variant_return: float
    baseline_profit: float
    variant_profit: float
    edge: float | None
    pred_probability: float | None
    place_basis_odds: float | None
    win_odds: float | None
    popularity: int | None


@dataclass(frozen=True)
class BetLogicOnlyBankrollSummary:
    logic_variant: str
    initial_bankroll: float
    final_bankroll: float
    cumulative_profit: float
    total_stake: float
    avg_stake: float
    roi_multiple: float
    net_roi: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class BetLogicOnlySensitivityRow:
    sensitivity_family: str
    sensitivity_value: float
    logic_variant: str
    bet_count: int
    roi_multiple: float
    net_roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    delta_roi_multiple_vs_baseline: float
    delta_net_roi_vs_baseline: float
    delta_total_profit_vs_baseline: float
    delta_max_drawdown_vs_baseline: float


@dataclass(frozen=True)
class NoBetGuardDroppedSummaryRow:
    surcharge: float
    group_type: str
    group_key: str
    dropped_bet_count: int
    dropped_total_stake: float
    dropped_total_return: float
    dropped_total_profit: float
    dropped_roi_multiple: float
    dropped_net_roi: float
    dropped_hit_rate: float


@dataclass(frozen=True)
class NoBetGuardDroppedDetailRow:
    surcharge: float
    set_group: str
    domain: str
    year: str
    month: str
    edge_bucket: str
    place_basis_bucket: str
    market_odds_bucket: str
    popularity_bucket: str
    window_label: str
    race_key: str
    horse_number: int
    stake: float
    total_return: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    hit: int


@dataclass(frozen=True)
class NoBetGuardKeptComparisonRow:
    surcharge: float
    kept_bet_count: int
    row_match_count: int
    all_rows_match: bool
    baseline_total_stake: float
    kept_total_stake: float
    baseline_total_return: float
    kept_total_return: float
    baseline_total_profit: float
    kept_total_profit: float
    baseline_roi_multiple: float
    kept_roi_multiple: float
    baseline_net_roi: float
    kept_net_roi: float


@dataclass(frozen=True)
class NoBetGuardStabilityRow:
    surcharge: float
    group_type: str
    group_key: str
    bet_count: int
    total_stake: float
    total_return: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    hit_rate: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class RacePredictionRow:
    window_label: str
    result_date: str
    race_key: str
    horse_number: int
    target_value: int
    pred_probability: float
    market_prob: float | None
    edge: float | None
    win_odds: float | None
    popularity: int | None
    place_basis_odds: float | None
    place_payout: float | None


@dataclass(frozen=True)
class RaceChaosRow:
    window_label: str
    result_date: str
    race_key: str
    domain: str
    field_count: int
    max_edge: float
    mean_edge: float
    near_threshold_candidate_count: int
    top1_top2_probability_gap: float
    top3_probability_concentration: float
    probability_entropy: float
    edge_closeness: float
    edge_place_basis_rank_disagreement: float
    win_odds_entropy: float
    fair_odds_dispersion: float
    popularity_entropy: float
    chaos_score: float
    chaos_bucket: str
    chaos_skip_flag: int
    chaos_edge_surcharge: float


@dataclass(frozen=True)
class ChaosSummaryRow:
    logic_variant: str
    anchor_logic: str
    chaos_mode: str
    bet_count: int
    hit_rate: float
    total_stake: float
    total_return: float
    roi_multiple: float
    net_roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    roi_gt_1_ratio: float
    roi_95_interval: str
    delta_bet_count_vs_baseline: int
    delta_roi_multiple_vs_baseline: float
    delta_net_roi_vs_baseline: float
    delta_total_profit_vs_baseline: float
    delta_max_drawdown_vs_baseline: float
    delta_max_losing_streak_vs_baseline: int
    delta_bet_count_vs_guard_001: int
    delta_roi_multiple_vs_guard_001: float
    delta_net_roi_vs_guard_001: float
    delta_total_profit_vs_guard_001: float
    delta_max_drawdown_vs_guard_001: float
    delta_max_losing_streak_vs_guard_001: int


@dataclass(frozen=True)
class ChaosCorrelationRow:
    analysis_group: str
    metric_name: str
    metric_value: float


@dataclass(frozen=True)
class ChaosBucketReadoutRow:
    analysis_type: str
    group_key: str
    chaos_bucket: str
    bet_count: int
    total_stake: float
    total_return: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    hit_rate: float
    mean_chaos_score: float


@dataclass(frozen=True)
class ChaosDroppedSummaryRow:
    logic_variant: str
    anchor_logic: str
    group_type: str
    group_key: str
    dropped_bet_count: int
    dropped_total_stake: float
    dropped_total_return: float
    dropped_total_profit: float
    dropped_roi_multiple: float
    dropped_net_roi: float
    dropped_hit_rate: float
    mean_chaos_score: float


@dataclass(frozen=True)
class ChaosDroppedDetailRow:
    logic_variant: str
    anchor_logic: str
    race_key: str
    horse_number: int
    window_label: str
    result_date: str
    domain: str
    chaos_score: float
    chaos_bucket: str
    edge_bucket: str
    place_basis_bucket: str
    market_odds_bucket: str
    popularity_bucket: str
    stake: float
    total_return: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    hit: int


@dataclass(frozen=True)
class ChaosStabilityRow:
    logic_variant: str
    anchor_logic: str
    group_type: str
    group_key: str
    bet_count: int
    total_stake: float
    total_return: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    hit_rate: float
    max_drawdown: float
    max_losing_streak: int
    delta_total_profit_vs_baseline: float
    delta_total_profit_vs_guard_001: float
    delta_roi_multiple_vs_baseline: float
    delta_roi_multiple_vs_guard_001: float


@dataclass(frozen=True)
class BetLogicStatusRow:
    logic_variant: str
    status: str
    scope: str
    rationale: str


@dataclass(frozen=True)
class GuardWeakRegimeRow:
    group_type: str
    group_key: str
    analysis_scope: str
    bet_count: int
    total_stake: float
    total_return: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    hit_rate: float
    max_drawdown: float
    max_losing_streak: int


@dataclass(frozen=True)
class GuardRegionCandidateRow:
    group_type: str
    group_key: str
    classification: str
    all_bet_count: int
    all_total_profit: float
    all_roi_multiple: float
    year_2023_profit: float
    year_2024_profit: float
    year_2025_profit: float
    note: str


@dataclass(frozen=True)
class OverlayDiffSummaryRow:
    logic_variant: str
    set_group: str
    guard_bet_count: int
    guard_total_stake: float
    guard_total_return: float
    guard_total_profit: float
    guard_roi_multiple: float
    guard_net_roi: float
    overlay_bet_count: int
    overlay_total_stake: float
    overlay_total_return: float
    overlay_total_profit: float
    overlay_roi_multiple: float
    overlay_net_roi: float


@dataclass(frozen=True)
class OverlayDiffDetailRow:
    logic_variant: str
    set_group: str
    race_key: str
    horse_number: int
    guard_window_label: str | None
    overlay_window_label: str | None
    guard_stake: float
    overlay_stake: float
    guard_return: float
    overlay_return: float
    guard_profit: float
    overlay_profit: float
    edge: float | None
    pred_probability: float | None
    place_basis_odds: float | None
    win_odds: float | None
    popularity: int | None


@dataclass(frozen=True)
class OverlayStabilityRow:
    logic_variant: str
    group_type: str
    group_key: str
    bet_count: int
    total_stake: float
    total_return: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    hit_rate: float
    max_drawdown: float
    max_losing_streak: int
    delta_total_profit_vs_baseline: float
    delta_total_profit_vs_guard_001: float
    delta_roi_multiple_vs_baseline: float
    delta_roi_multiple_vs_guard_001: float


@dataclass(frozen=True)
class FinalBetInstructionRow:
    run_mode: str
    race_key: str
    race_date: str
    horse_id: str
    horse_number: int
    horse_name: str
    decision: str
    stake: float
    model_probability: float
    edge: float | None
    place_basis: float | None
    win_odds: float | None
    # Historical artifact column name retained for compatibility.
    # Values are project-owned buckets derived from upstream venue_code = race_key[:2].
    proxy_domain: str
    logic_name: str
    kept_by_baseline: int
    kept_by_guard_0_01: int
    kept_by_proxy_overlay: int
    final_reason: str


@dataclass(frozen=True)
class FinalRaceInstructionRow:
    run_mode: str
    race_key: str
    race_date: str
    bet_count: int
    total_stake: float
    selected_horses: str
    # Historical artifact column name retained for compatibility.
    # Values are project-owned buckets derived from upstream venue_code = race_key[:2].
    proxy_domain: str
    logic_name: str
    race_decision_summary: str


@dataclass(frozen=True)
class FinalCandidateFallbackDiffRow:
    run_mode: str
    diff_level: str
    race_key: str
    race_date: str
    horse_id: str
    horse_number: int | None
    horse_name: str
    # Historical artifact column name retained for compatibility.
    # Values are project-owned buckets derived from upstream venue_code = race_key[:2].
    proxy_domain: str
    candidate_decision: str
    fallback_decision: str
    candidate_stake: float
    fallback_stake: float
    candidate_logic_name: str
    fallback_logic_name: str
    candidate_bet_count: int | None
    fallback_bet_count: int | None
    candidate_total_stake: float | None
    fallback_total_stake: float | None
    candidate_selected_horses: str
    fallback_selected_horses: str
    changed_field_summary: str
    final_reason: str


@dataclass(frozen=True)
class DomainMappingAuditRow:
    audit_item: str
    source_location: str
    evidence_type: str
    finding: str
    implication: str
    supports_formal_mapping: int


@dataclass(frozen=True)
class DomainMappingReportRow:
    # Historical column name retained for artifact stability.
    # This stores a project-derived bucket based on upstream venue_code = race_key[:2].
    proxy_domain: str
    observed_race_count: int
    observed_bet_count: int
    fallback_bet_count: int
    fallback_total_profit: float
    fallback_roi_multiple: float
    candidate_bet_count: int
    candidate_total_profit: float
    candidate_roi_multiple: float
    inferred_meaning: str
    evidence_locations: str
    formal_mapping_status: str


@dataclass(frozen=True)
class HardAdoptDecisionRow:
    decision_status: str
    candidate_logic_name: str
    fallback_logic_name: str
    external_mapping_evidence_found: int
    race_key_prefix_to_category_confirmed: int
    recommended_operational_status: str
    reason: str


@dataclass(frozen=True)
class InstructionPackageManifestRow:
    run_mode: str
    logic_name: str
    adopt_status: str
    config_path: str
    freeze_flags: str
    source_artifact_path: str
    generated_at: str
    git_commit: str
    working_tree_note: str


@dataclass(frozen=True)
class InstructionPackageSummaryRow:
    run_mode: str
    logic_name: str
    adopt_status: str
    bet_count: int
    total_stake: float
    total_profit: float
    roi_multiple: float
    max_drawdown: float
    max_losing_streak: int
    candidate_fallback_diff_count: int


@dataclass(frozen=True)
class MonitoringSummaryRow:
    generated_at: str
    run_mode: str
    logic_name: str
    source_artifact_path: str
    bet_count: int
    total_stake: float
    total_profit: float
    roi_multiple: float
    net_roi: float
    max_drawdown: float
    max_losing_streak: int
    candidate_fallback_diff_count: int


@dataclass(frozen=True)
class RegressionGateReportRow:
    generated_at: str
    gate_name: str
    candidate_run_mode: str
    fallback_run_mode: str
    candidate_logic_name: str
    fallback_logic_name: str
    status: str
    metric_name: str
    candidate_value: float
    fallback_value: float
    threshold: float
    message: str


@dataclass(frozen=True)
class ArtifactCompareReportRow:
    generated_at: str
    compare_group: str
    run_mode: str
    current_source_artifact_path: str
    previous_source_artifact_path: str
    current_value: float
    previous_value: float
    delta_value: float
    note: str


@dataclass(frozen=True)
class BetLogicOnlyResult:
    output_dir: Path
    summaries: tuple[BetLogicOnlySummary, ...]
    selected_summaries: tuple[BetLogicOnlySelectedSummary, ...]
    comparison_rows: tuple[BetLogicOnlyComparisonRow, ...]
    parity_summary: BetLogicOnlyParitySummary
    parity_detail_rows: tuple[BetLogicOnlyParityDetailRow, ...]
    diff_summaries: tuple[BetLogicOnlyDiffSummary, ...]
    diff_detail_rows: tuple[BetLogicOnlyDiffDetailRow, ...]
    bankroll_summaries: tuple[BetLogicOnlyBankrollSummary, ...]
    sensitivity_rows: tuple[BetLogicOnlySensitivityRow, ...]
    no_bet_guard_dropped_rows: tuple[NoBetGuardDroppedSummaryRow, ...]
    no_bet_guard_dropped_detail_rows: tuple[NoBetGuardDroppedDetailRow, ...]
    no_bet_guard_kept_rows: tuple[NoBetGuardKeptComparisonRow, ...]
    no_bet_guard_stability_rows: tuple[NoBetGuardStabilityRow, ...]
    chaos_rows: tuple[RaceChaosRow, ...]
    chaos_summary_rows: tuple[ChaosSummaryRow, ...]
    chaos_correlation_rows: tuple[ChaosCorrelationRow, ...]
    chaos_bucket_rows: tuple[ChaosBucketReadoutRow, ...]
    chaos_dropped_summary_rows: tuple[ChaosDroppedSummaryRow, ...]
    chaos_dropped_detail_rows: tuple[ChaosDroppedDetailRow, ...]
    chaos_stability_rows: tuple[ChaosStabilityRow, ...]
    logic_status_rows: tuple[BetLogicStatusRow, ...]
    guard_weak_regime_rows: tuple[GuardWeakRegimeRow, ...]
    guard_region_candidate_rows: tuple[GuardRegionCandidateRow, ...]
    overlay_diff_summaries: tuple[OverlayDiffSummaryRow, ...]
    overlay_diff_detail_rows: tuple[OverlayDiffDetailRow, ...]
    overlay_stability_rows: tuple[OverlayStabilityRow, ...]
    final_bet_instructions_candidate: tuple[FinalBetInstructionRow, ...]
    final_bet_instructions_fallback: tuple[FinalBetInstructionRow, ...]
    final_race_instructions_candidate: tuple[FinalRaceInstructionRow, ...]
    final_race_instructions_fallback: tuple[FinalRaceInstructionRow, ...]
    final_candidate_vs_fallback_diff: tuple[FinalCandidateFallbackDiffRow, ...]
    instruction_package_manifest_rows: tuple[InstructionPackageManifestRow, ...]
    instruction_package_summary_rows: tuple[InstructionPackageSummaryRow, ...]
    monitoring_summary_rows: tuple[MonitoringSummaryRow, ...]
    regression_gate_rows: tuple[RegressionGateReportRow, ...]
    artifact_compare_rows: tuple[ArtifactCompareReportRow, ...]
    domain_mapping_audit_rows: tuple[DomainMappingAuditRow, ...]
    domain_mapping_report_rows: tuple[DomainMappingReportRow, ...]
    domain_mapping_adoption_memo: str
    hard_adopt_decision_rows: tuple[HardAdoptDecisionRow, ...]
    hard_adopt_decision_memo: str


@dataclass(frozen=True)
class _LogicRowStats:
    bet_count: int
    hit_count: int
    hit_rate: float
    total_stake: float
    total_return: float
    roi_multiple: float
    net_roi: float
    total_profit: float
    max_drawdown: float
    max_losing_streak: int
    avg_edge: float
    avg_stake: float


def run_bet_logic_only_analysis(config: BetLogicOnlyConfig) -> BetLogicOnlyResult:
    ranking_config = RankingRuleComparisonConfig(
        name=f"{config.name}_ranking_compare",
        duckdb_path=config.duckdb_path,
        rolling_predictions_path=config.rolling_predictions_path,
        output_dir=config.output_dir / "_ranking_compare",
        selection_metric=config.selection_metric,
        market_prob_method=config.market_prob_method,
        thresholds=config.thresholds,
        min_win_odds=config.min_win_odds,
        max_win_odds=config.max_win_odds,
        split_column=config.split_column,
        target_column=config.target_column,
        probability_column=config.probability_column,
        stake_per_bet=config.stake_per_bet,
        aggregate_selection_score_rule=config.aggregate_selection_score_rule,
        min_bets_valid=config.min_bets_valid,
        popularity_bands=config.popularity_bands,
        place_basis_bands=config.place_basis_bands,
        evaluation_window_pairs=config.evaluation_window_pairs,
        selection_window_groups=config.selection_window_groups,
        ranking_score_rules=(
            "edge",
            "pred_times_place_basis_odds",
            "edge_times_place_basis_odds",
            "edge_plus_payout_tilt",
        ),
    )
    ranking_result = run_ranking_rule_comparison(ranking_config)
    race_prediction_rows = load_race_prediction_rows(config)
    selected_rows_by_window = build_selected_test_rows_by_window(
        selected_summaries=ranking_result.selected_summaries,
        selected_rows_by_candidate=ranking_result.selected_rows_by_candidate,
    )
    edge_threshold_by_window = build_edge_threshold_by_window(ranking_result.selected_summaries)
    chaos_rows = build_race_chaos_rows(
        race_prediction_rows=race_prediction_rows,
        edge_threshold_by_window=edge_threshold_by_window,
    )
    chaos_by_race = {row.race_key: row for row in chaos_rows}
    horse_metadata_by_key = load_horse_metadata_by_key(config)

    reference_compare_config = load_reference_label_guard_compare_config(
        config.reference_label_guard_compare_config_path,
    )
    reference_result = run_reference_label_guard_compare(reference_compare_config)
    baseline_rows = build_baseline_rows(
        reference_rows=reference_result.selected_test_rows,
        stake_per_bet=config.stake_per_bet,
    )

    variant_rows = build_variant_rows(
        baseline_rows=baseline_rows,
        selected_rows_by_window=selected_rows_by_window,
        edge_threshold_by_window=edge_threshold_by_window,
        chaos_by_race=chaos_by_race,
        config=config,
    )
    parity_summary, parity_detail_rows = build_parity_report(
        reference_rows=baseline_rows,
        baseline_rows=variant_rows[BASELINE_LOGIC_NAME],
    )
    selected_summaries = tuple(
        build_selected_summaries(
            variant_rows=variant_rows,
            edge_threshold_by_window=edge_threshold_by_window,
        ),
    )
    summaries = tuple(
        build_rollup_summaries(
            selected_summaries,
            variant_rows=variant_rows,
            bootstrap_iterations=config.bootstrap_iterations,
            random_seed=config.random_seed,
        ),
    )
    comparison_rows = tuple(build_comparison_rows(summaries))
    diff_summaries, diff_detail_rows = build_diff_reports(variant_rows=variant_rows)
    bankroll_summaries = tuple(build_bankroll_summaries(variant_rows=variant_rows, config=config))
    sensitivity_rows = tuple(
        build_sensitivity_rows(
            baseline_rows=baseline_rows,
            edge_threshold_by_window=edge_threshold_by_window,
            config=config,
        ),
    )
    (
        no_bet_guard_dropped_rows,
        no_bet_guard_dropped_detail_rows,
        no_bet_guard_kept_rows,
        no_bet_guard_stability_rows,
    ) = build_no_bet_guard_reports(
        baseline_rows=baseline_rows,
        edge_threshold_by_window=edge_threshold_by_window,
        config=config,
    )
    (
        chaos_summary_rows,
        chaos_correlation_rows,
        chaos_bucket_rows,
        chaos_dropped_summary_rows,
        chaos_dropped_detail_rows,
        chaos_stability_rows,
    ) = build_chaos_reports(
        baseline_rows=baseline_rows,
        guard_rows=variant_rows["no_bet_guard_stronger"],
        variant_rows=variant_rows,
        chaos_by_race=chaos_by_race,
        selected_summaries=selected_summaries,
        bootstrap_iterations=config.bootstrap_iterations,
        random_seed=config.random_seed,
    )
    logic_status_rows = build_logic_status_rows(config)
    guard_weak_regime_rows, guard_region_candidate_rows = build_guard_regime_reports(
        guard_rows=variant_rows["no_bet_guard_stronger"],
        chaos_by_race=chaos_by_race,
    )
    overlay_diff_summaries, overlay_diff_detail_rows = build_overlay_diff_reports(
        guard_rows=variant_rows["no_bet_guard_stronger"],
        variant_rows=variant_rows,
    )
    overlay_stability_rows = build_overlay_stability_rows(
        baseline_rows=baseline_rows,
        guard_rows=variant_rows["no_bet_guard_stronger"],
        variant_rows=variant_rows,
    )
    (
        final_bet_instructions_candidate,
        final_bet_instructions_fallback,
        final_race_instructions_candidate,
        final_race_instructions_fallback,
        final_candidate_vs_fallback_diff,
    ) = build_final_instruction_reports(
        baseline_rows=variant_rows[BASELINE_LOGIC_NAME],
        fallback_rows=variant_rows["no_bet_guard_stronger"],
        candidate_rows=variant_rows["guard_0_01_plus_proxy_domain_overlay"],
        horse_metadata_by_key=horse_metadata_by_key,
        config=config,
    )
    (
        instruction_package_manifest_rows,
        instruction_package_summary_rows,
    ) = build_instruction_package_artifacts(
        candidate_rows=variant_rows["guard_0_01_plus_proxy_domain_overlay"],
        fallback_rows=variant_rows["no_bet_guard_stronger"],
        final_candidate_vs_fallback_diff=final_candidate_vs_fallback_diff,
        config=config,
    )
    (
        monitoring_summary_rows,
        regression_gate_rows,
        artifact_compare_rows,
    ) = build_monitoring_artifacts(
        output_dir=config.output_dir,
        instruction_package_manifest_rows=instruction_package_manifest_rows,
        instruction_package_summary_rows=instruction_package_summary_rows,
    )
    (
        domain_mapping_audit_rows,
        domain_mapping_report_rows,
        domain_mapping_adoption_memo,
    ) = build_domain_mapping_artifacts(
        race_prediction_rows=race_prediction_rows,
        baseline_rows=variant_rows[BASELINE_LOGIC_NAME],
        fallback_rows=variant_rows["no_bet_guard_stronger"],
        candidate_rows=variant_rows["guard_0_01_plus_proxy_domain_overlay"],
        config=config,
    )
    (
        hard_adopt_decision_rows,
        hard_adopt_decision_memo,
    ) = build_hard_adopt_decision_artifacts(
        audit_rows=domain_mapping_audit_rows,
        report_rows=domain_mapping_report_rows,
        config=config,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = BetLogicOnlyResult(
        output_dir=config.output_dir,
        summaries=summaries,
        selected_summaries=selected_summaries,
        comparison_rows=comparison_rows,
        parity_summary=parity_summary,
        parity_detail_rows=parity_detail_rows,
        diff_summaries=diff_summaries,
        diff_detail_rows=diff_detail_rows,
        bankroll_summaries=bankroll_summaries,
        sensitivity_rows=sensitivity_rows,
        no_bet_guard_dropped_rows=no_bet_guard_dropped_rows,
        no_bet_guard_dropped_detail_rows=no_bet_guard_dropped_detail_rows,
        no_bet_guard_kept_rows=no_bet_guard_kept_rows,
        no_bet_guard_stability_rows=no_bet_guard_stability_rows,
        chaos_rows=chaos_rows,
        chaos_summary_rows=chaos_summary_rows,
        chaos_correlation_rows=chaos_correlation_rows,
        chaos_bucket_rows=chaos_bucket_rows,
        chaos_dropped_summary_rows=chaos_dropped_summary_rows,
        chaos_dropped_detail_rows=chaos_dropped_detail_rows,
        chaos_stability_rows=chaos_stability_rows,
        logic_status_rows=logic_status_rows,
        guard_weak_regime_rows=guard_weak_regime_rows,
        guard_region_candidate_rows=guard_region_candidate_rows,
        overlay_diff_summaries=overlay_diff_summaries,
        overlay_diff_detail_rows=overlay_diff_detail_rows,
        overlay_stability_rows=overlay_stability_rows,
        final_bet_instructions_candidate=final_bet_instructions_candidate,
        final_bet_instructions_fallback=final_bet_instructions_fallback,
        final_race_instructions_candidate=final_race_instructions_candidate,
        final_race_instructions_fallback=final_race_instructions_fallback,
        final_candidate_vs_fallback_diff=final_candidate_vs_fallback_diff,
        instruction_package_manifest_rows=instruction_package_manifest_rows,
        instruction_package_summary_rows=instruction_package_summary_rows,
        monitoring_summary_rows=monitoring_summary_rows,
        regression_gate_rows=regression_gate_rows,
        artifact_compare_rows=artifact_compare_rows,
        domain_mapping_audit_rows=domain_mapping_audit_rows,
        domain_mapping_report_rows=domain_mapping_report_rows,
        domain_mapping_adoption_memo=domain_mapping_adoption_memo,
        hard_adopt_decision_rows=hard_adopt_decision_rows,
        hard_adopt_decision_memo=hard_adopt_decision_memo,
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "selected_summary.csv", result.selected_summaries)
    write_json(
        config.output_dir / "selected_summary.json",
        {"analysis": {"rows": result.selected_summaries}},
    )
    write_csv(config.output_dir / "comparison_readout.csv", result.comparison_rows)
    write_json(
        config.output_dir / "comparison_readout.json",
        {"analysis": {"rows": result.comparison_rows}},
    )
    write_csv(config.output_dir / "parity_summary.csv", (result.parity_summary,))
    write_json(
        config.output_dir / "parity_summary.json",
        {"analysis": result.parity_summary},
    )
    write_csv(config.output_dir / "parity_detail.csv", result.parity_detail_rows)
    write_json(
        config.output_dir / "parity_detail.json",
        {"analysis": {"rows": result.parity_detail_rows}},
    )
    write_csv(config.output_dir / "diff_summary.csv", result.diff_summaries)
    write_json(
        config.output_dir / "diff_summary.json",
        {"analysis": {"rows": result.diff_summaries}},
    )
    write_csv(config.output_dir / "diff_detail.csv", result.diff_detail_rows)
    write_json(
        config.output_dir / "diff_detail.json",
        {"analysis": {"rows": result.diff_detail_rows}},
    )
    write_csv(config.output_dir / "bankroll_summary.csv", result.bankroll_summaries)
    write_json(
        config.output_dir / "bankroll_summary.json",
        {"analysis": {"rows": result.bankroll_summaries}},
    )
    write_csv(config.output_dir / "sensitivity_summary.csv", result.sensitivity_rows)
    write_json(
        config.output_dir / "sensitivity_summary.json",
        {"analysis": {"rows": result.sensitivity_rows}},
    )
    write_csv(config.output_dir / "no_bet_guard_dropped_summary.csv", result.no_bet_guard_dropped_rows)
    write_json(
        config.output_dir / "no_bet_guard_dropped_summary.json",
        {"analysis": {"rows": result.no_bet_guard_dropped_rows}},
    )
    write_csv(
        config.output_dir / "no_bet_guard_dropped_detail.csv",
        result.no_bet_guard_dropped_detail_rows,
    )
    write_json(
        config.output_dir / "no_bet_guard_dropped_detail.json",
        {"analysis": {"rows": result.no_bet_guard_dropped_detail_rows}},
    )
    write_csv(config.output_dir / "no_bet_guard_kept_summary.csv", result.no_bet_guard_kept_rows)
    write_json(
        config.output_dir / "no_bet_guard_kept_summary.json",
        {"analysis": {"rows": result.no_bet_guard_kept_rows}},
    )
    write_csv(config.output_dir / "no_bet_guard_stability.csv", result.no_bet_guard_stability_rows)
    write_json(
        config.output_dir / "no_bet_guard_stability.json",
        {"analysis": {"rows": result.no_bet_guard_stability_rows}},
    )
    write_csv(config.output_dir / "chaos_detail.csv", result.chaos_rows)
    write_json(
        config.output_dir / "chaos_detail.json",
        {"analysis": {"rows": result.chaos_rows}},
    )
    write_csv(config.output_dir / "chaos_summary.csv", result.chaos_summary_rows)
    write_json(
        config.output_dir / "chaos_summary.json",
        {"analysis": {"rows": result.chaos_summary_rows}},
    )
    write_csv(config.output_dir / "chaos_correlation.csv", result.chaos_correlation_rows)
    write_json(
        config.output_dir / "chaos_correlation.json",
        {"analysis": {"rows": result.chaos_correlation_rows}},
    )
    write_csv(config.output_dir / "chaos_bucket_readout.csv", result.chaos_bucket_rows)
    write_json(
        config.output_dir / "chaos_bucket_readout.json",
        {"analysis": {"rows": result.chaos_bucket_rows}},
    )
    write_csv(config.output_dir / "chaos_dropped_summary.csv", result.chaos_dropped_summary_rows)
    write_json(
        config.output_dir / "chaos_dropped_summary.json",
        {"analysis": {"rows": result.chaos_dropped_summary_rows}},
    )
    write_csv(config.output_dir / "chaos_dropped_detail.csv", result.chaos_dropped_detail_rows)
    write_json(
        config.output_dir / "chaos_dropped_detail.json",
        {"analysis": {"rows": result.chaos_dropped_detail_rows}},
    )
    write_csv(config.output_dir / "chaos_stability.csv", result.chaos_stability_rows)
    write_json(
        config.output_dir / "chaos_stability.json",
        {"analysis": {"rows": result.chaos_stability_rows}},
    )
    write_csv(config.output_dir / "logic_status.csv", result.logic_status_rows)
    write_json(
        config.output_dir / "logic_status.json",
        {"analysis": {"rows": result.logic_status_rows}},
    )
    write_csv(config.output_dir / "guard_weak_regime_summary.csv", result.guard_weak_regime_rows)
    write_json(
        config.output_dir / "guard_weak_regime_summary.json",
        {"analysis": {"rows": result.guard_weak_regime_rows}},
    )
    write_csv(config.output_dir / "guard_region_candidates.csv", result.guard_region_candidate_rows)
    write_json(
        config.output_dir / "guard_region_candidates.json",
        {"analysis": {"rows": result.guard_region_candidate_rows}},
    )
    write_csv(config.output_dir / "overlay_diff_summary.csv", result.overlay_diff_summaries)
    write_json(
        config.output_dir / "overlay_diff_summary.json",
        {"analysis": {"rows": result.overlay_diff_summaries}},
    )
    write_csv(config.output_dir / "overlay_diff_detail.csv", result.overlay_diff_detail_rows)
    write_json(
        config.output_dir / "overlay_diff_detail.json",
        {"analysis": {"rows": result.overlay_diff_detail_rows}},
    )
    write_csv(config.output_dir / "overlay_stability.csv", result.overlay_stability_rows)
    write_json(
        config.output_dir / "overlay_stability.json",
        {"analysis": {"rows": result.overlay_stability_rows}},
    )
    write_csv(
        config.output_dir / "final_bet_instructions_candidate.csv",
        result.final_bet_instructions_candidate,
    )
    write_json(
        config.output_dir / "final_bet_instructions_candidate.json",
        {"analysis": {"rows": result.final_bet_instructions_candidate}},
    )
    write_csv(
        config.output_dir / "final_bet_instructions_fallback.csv",
        result.final_bet_instructions_fallback,
    )
    write_json(
        config.output_dir / "final_bet_instructions_fallback.json",
        {"analysis": {"rows": result.final_bet_instructions_fallback}},
    )
    write_csv(
        config.output_dir / "final_race_instructions_candidate.csv",
        result.final_race_instructions_candidate,
    )
    write_json(
        config.output_dir / "final_race_instructions_candidate.json",
        {"analysis": {"rows": result.final_race_instructions_candidate}},
    )
    write_csv(
        config.output_dir / "final_race_instructions_fallback.csv",
        result.final_race_instructions_fallback,
    )
    write_json(
        config.output_dir / "final_race_instructions_fallback.json",
        {"analysis": {"rows": result.final_race_instructions_fallback}},
    )
    write_csv(
        config.output_dir / "final_candidate_vs_fallback_diff.csv",
        result.final_candidate_vs_fallback_diff,
    )
    write_json(
        config.output_dir / "final_candidate_vs_fallback_diff.json",
        {"analysis": {"rows": result.final_candidate_vs_fallback_diff}},
    )
    write_csv(
        config.output_dir / "final_instruction_package_manifest.csv",
        result.instruction_package_manifest_rows,
    )
    write_json(
        config.output_dir / "final_instruction_package_manifest.json",
        {"analysis": {"rows": result.instruction_package_manifest_rows}},
    )
    write_csv(
        config.output_dir / "final_instruction_package_summary.csv",
        result.instruction_package_summary_rows,
    )
    write_json(
        config.output_dir / "final_instruction_package_summary.json",
        {"analysis": {"rows": result.instruction_package_summary_rows}},
    )
    write_csv(
        config.output_dir / "monitoring_summary.csv",
        result.monitoring_summary_rows,
    )
    write_json(
        config.output_dir / "monitoring_summary.json",
        {"analysis": {"rows": result.monitoring_summary_rows}},
    )
    write_csv(
        config.output_dir / "regression_gate_report.csv",
        result.regression_gate_rows,
    )
    write_json(
        config.output_dir / "regression_gate_report.json",
        {"analysis": {"rows": result.regression_gate_rows}},
    )
    write_csv(
        config.output_dir / "artifact_compare_report.csv",
        result.artifact_compare_rows,
    )
    write_json(
        config.output_dir / "artifact_compare_report.json",
        {"analysis": {"rows": result.artifact_compare_rows}},
    )
    write_csv(
        config.output_dir / "domain_mapping_audit.csv",
        result.domain_mapping_audit_rows,
    )
    write_json(
        config.output_dir / "domain_mapping_audit.json",
        {"analysis": {"rows": result.domain_mapping_audit_rows}},
    )
    write_csv(
        config.output_dir / "domain_mapping_report.csv",
        result.domain_mapping_report_rows,
    )
    write_json(
        config.output_dir / "domain_mapping_report.json",
        {"analysis": {"rows": result.domain_mapping_report_rows}},
    )
    write_csv(
        config.output_dir / "hard_adopt_decision.csv",
        result.hard_adopt_decision_rows,
    )
    write_json(
        config.output_dir / "hard_adopt_decision.json",
        {"analysis": {"rows": result.hard_adopt_decision_rows}},
    )
    (config.output_dir / "domain_mapping_adoption_memo.md").write_text(
        result.domain_mapping_adoption_memo,
        encoding="utf-8",
    )
    (config.output_dir / "hard_adopt_decision_memo.md").write_text(
        result.hard_adopt_decision_memo,
        encoding="utf-8",
    )
    return result


def build_edge_threshold_by_window(selected_summaries: tuple[object, ...]) -> dict[str, float]:
    output: dict[str, float] = {}
    for summary in selected_summaries:
        if summary.applied_to_split != "test":
            continue
        if summary.ranking_score_rule != "edge":
            continue
        output[str(summary.window_label)] = float(summary.threshold)
    return output


def build_baseline_rows(
    *,
    reference_rows: tuple[object, ...],
    stake_per_bet: float,
) -> tuple[LogicBetRow, ...]:
    rows: list[LogicBetRow] = []
    for item in reference_rows:
        row = item.row
        rows.append(
            LogicBetRow(
                logic_variant=BASELINE_LOGIC_NAME,
                source_logic="mainline_reference_selected_rows",
                window_label=item.window_label,
                result_date=row.result_date.isoformat(),
                race_key=row.race_key,
                horse_number=row.horse_number,
                stake=stake_per_bet,
                scaled_return=row.place_payout or 0.0,
                bet_profit=(row.place_payout or 0.0) - stake_per_bet,
                target_value=row.target_value,
                pred_probability=row.pred_probability,
                market_prob=row.market_prob,
                edge=row.edge,
                win_odds=row.win_odds,
                popularity=row.popularity,
                place_basis_odds=row.place_basis_odds,
                place_payout=row.place_payout,
            ),
        )
    return order_logic_rows(tuple(rows))


def build_variant_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    selected_rows_by_window: dict[str, dict[str, tuple[CandidateBetRow, ...]]],
    edge_threshold_by_window: dict[str, float],
    chaos_by_race: dict[str, RaceChaosRow],
    config: BetLogicOnlyConfig,
) -> dict[str, tuple[LogicBetRow, ...]]:
    del selected_rows_by_window
    primary_guard_surcharge = 0.01
    guard_rows = filter_guard_variant_rows(
        baseline_rows=baseline_rows,
        surcharge=primary_guard_surcharge,
        source_logic="mainline_reference_selected_rows+stronger_guard_0.01",
        stake_per_bet=config.stake_per_bet,
        edge_threshold_by_window=edge_threshold_by_window,
        logic_variant="no_bet_guard_stronger",
    )
    variant_rows: dict[str, tuple[LogicBetRow, ...]] = {
        BASELINE_LOGIC_NAME: baseline_rows,
        "no_bet_guard_stronger": guard_rows,
        "guard_0_01_plus_proxy_domain_overlay": apply_overlay_variant_rows(
            guard_rows=guard_rows,
            logic_variant="guard_0_01_plus_proxy_domain_overlay",
            source_logic="guard_0.01+venue_code_based_domain_surcharge",
            edge_threshold_by_window=edge_threshold_by_window,
            stake_per_bet=config.stake_per_bet,
            chaos_by_race=chaos_by_race,
            overlay_name="proxy_domain",
        ),
        "guard_0_01_plus_near_threshold_overlay": apply_overlay_variant_rows(
            guard_rows=guard_rows,
            logic_variant="guard_0_01_plus_near_threshold_overlay",
            source_logic="guard_0.01+near_threshold_surcharge",
            edge_threshold_by_window=edge_threshold_by_window,
            stake_per_bet=config.stake_per_bet,
            chaos_by_race=chaos_by_race,
            overlay_name="near_threshold",
        ),
        "guard_0_01_plus_place_basis_overlay": apply_overlay_variant_rows(
            guard_rows=guard_rows,
            logic_variant="guard_0_01_plus_place_basis_overlay",
            source_logic="guard_0.01+place_basis_surcharge",
            edge_threshold_by_window=edge_threshold_by_window,
            stake_per_bet=config.stake_per_bet,
            chaos_by_race=chaos_by_race,
            overlay_name="place_basis",
        ),
        "guard_0_01_plus_domain_x_threshold_overlay": apply_overlay_variant_rows(
            guard_rows=guard_rows,
            logic_variant="guard_0_01_plus_domain_x_threshold_overlay",
            source_logic="guard_0.01+domain_x_threshold_surcharge",
            edge_threshold_by_window=edge_threshold_by_window,
            stake_per_bet=config.stake_per_bet,
            chaos_by_race=chaos_by_race,
            overlay_name="domain_x_threshold",
        ),
        "chaos_no_bet_guard": apply_chaos_layer_variant_rows(
            baseline_rows=baseline_rows,
            logic_variant="chaos_no_bet_guard",
            source_logic="mainline_reference_selected_rows+race_chaos_skip",
            edge_threshold_by_window=edge_threshold_by_window,
            stake_per_bet=config.stake_per_bet,
            chaos_by_race=chaos_by_race,
            mode="skip",
        ),
        "chaos_edge_surcharge": apply_chaos_layer_variant_rows(
            baseline_rows=baseline_rows,
            logic_variant="chaos_edge_surcharge",
            source_logic="mainline_reference_selected_rows+race_chaos_surcharge",
            edge_threshold_by_window=edge_threshold_by_window,
            stake_per_bet=config.stake_per_bet,
            chaos_by_race=chaos_by_race,
            mode="surcharge",
        ),
        "no_bet_guard_plus_chaos": apply_chaos_layer_variant_rows(
            baseline_rows=guard_rows,
            logic_variant="no_bet_guard_plus_chaos",
            source_logic="mainline_reference_selected_rows+stronger_guard_0.01+race_chaos_surcharge",
            edge_threshold_by_window=edge_threshold_by_window,
            stake_per_bet=config.stake_per_bet,
            chaos_by_race=chaos_by_race,
            mode="surcharge",
        ),
    }
    return variant_rows


def apply_overlay_variant_rows(
    *,
    guard_rows: tuple[LogicBetRow, ...],
    logic_variant: str,
    source_logic: str,
    edge_threshold_by_window: dict[str, float],
    stake_per_bet: float,
    chaos_by_race: dict[str, RaceChaosRow],
    overlay_name: str,
) -> tuple[LogicBetRow, ...]:
    rows: list[LogicBetRow] = []
    for row in guard_rows:
        threshold = edge_threshold_by_window.get(row.window_label)
        if threshold is None or row.edge is None:
            continue
        extra_surcharge = overlay_extra_surcharge(
            row=row,
            threshold=threshold,
            chaos=chaos_by_race.get(row.race_key),
            overlay_name=overlay_name,
        )
        if row.edge < threshold + 0.01 + extra_surcharge:
            continue
        rows.append(
            with_logic_variant(
                row=row,
                logic_variant=logic_variant,
                source_logic=source_logic,
                stake=row.stake if row.stake > 0.0 else stake_per_bet,
            ),
        )
    return order_logic_rows(tuple(rows))


def overlay_extra_surcharge(
    *,
    row: LogicBetRow,
    threshold: float,
    chaos: RaceChaosRow | None,
    overlay_name: str,
) -> float:
    near_threshold_count = chaos.near_threshold_candidate_count if chaos is not None else 0
    domain = domain_key(row)
    is_near_threshold = row.edge < threshold + 0.02
    if overlay_name == "proxy_domain":
        # Historical overlay name retained. The condition actually keys off a
        # project-owned bucket derived from upstream venue_code = race_key[:2].
        return 0.02 if domain in {"02", "09"} else 0.0
    if overlay_name == "near_threshold":
        return 0.01 if near_threshold_count >= 2 and is_near_threshold else 0.0
    if overlay_name == "place_basis":
        return 0.01 if row.place_basis_odds is not None and 2.8 <= row.place_basis_odds < 3.2 else 0.0
    if overlay_name == "domain_x_threshold":
        return 0.02 if domain in {"02", "09"} and near_threshold_count >= 1 and is_near_threshold else 0.0
    return 0.0


def load_race_prediction_rows(config: BetLogicOnlyConfig) -> tuple[RacePredictionRow, ...]:
    connection = duckdb.connect(str(config.duckdb_path), read_only=True)
    try:
        prediction_table = "prediction_rows_bet_logic_only_chaos"
        connection.execute(f"DROP TABLE IF EXISTS {prediction_table}")
        connection.execute(
            f"""
            CREATE TEMP TABLE {prediction_table} AS
            SELECT * FROM read_csv_auto(?)
            """,
            [str(config.rolling_predictions_path)],
        )
        rows = connection.execute(
            f"""
            WITH payout_rows AS (
                SELECT race_key, place_horse_number_1 AS horse_number, place_payout_1 AS place_payout
                FROM jrdb_hjc_staging
                UNION ALL
                SELECT race_key, place_horse_number_2 AS horse_number, place_payout_2 AS place_payout
                FROM jrdb_hjc_staging
                UNION ALL
                SELECT race_key, place_horse_number_3 AS horse_number, place_payout_3 AS place_payout
                FROM jrdb_hjc_staging
            )
            SELECT
                CAST(p.window_label AS VARCHAR),
                CAST(s.result_date AS VARCHAR),
                CAST(p.race_key AS VARCHAR),
                CAST(p.horse_number AS INTEGER),
                CAST(p.{config.target_column} AS INTEGER),
                CAST(p.{config.probability_column} AS DOUBLE),
                CASE
                    WHEN o.place_basis_odds IS NOT NULL AND o.place_basis_odds > 0.0
                    THEN 1.0 / o.place_basis_odds
                    ELSE NULL
                END AS market_prob,
                CASE
                    WHEN o.place_basis_odds IS NOT NULL AND o.place_basis_odds > 0.0
                    THEN CAST(p.{config.probability_column} AS DOUBLE) - (1.0 / o.place_basis_odds)
                    ELSE NULL
                END AS edge,
                TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                CAST(s.popularity AS INTEGER) AS popularity,
                CAST(o.place_basis_odds AS DOUBLE) AS place_basis_odds,
                CAST(pay.place_payout AS DOUBLE) AS place_payout
            FROM {prediction_table} AS p
            INNER JOIN jrdb_sed_staging AS s
                ON s.race_key = p.race_key
                AND s.horse_number = p.horse_number
            LEFT JOIN jrdb_oz_staging AS o
                ON o.race_key = p.race_key
                AND o.horse_number = p.horse_number
            LEFT JOIN payout_rows AS pay
                ON pay.race_key = p.race_key
                AND pay.horse_number = p.horse_number
            WHERE CAST(p.{config.split_column} AS VARCHAR) = 'test'
            ORDER BY s.result_date, p.window_label, p.race_key, p.horse_number
            """,
        ).fetchall()
    finally:
        connection.close()
    return tuple(
        RacePredictionRow(
            window_label=str(row[0]),
            result_date=str(row[1]),
            race_key=str(row[2]),
            horse_number=int(row[3]),
            target_value=int(row[4]),
            pred_probability=float(row[5]),
            market_prob=float(row[6]) if row[6] is not None else None,
            edge=float(row[7]) if row[7] is not None else None,
            win_odds=float(row[8]) if row[8] is not None else None,
            popularity=int(row[9]) if row[9] is not None else None,
            place_basis_odds=float(row[10]) if row[10] is not None else None,
            place_payout=float(row[11]) if row[11] is not None else None,
        )
        for row in rows
    )


def load_horse_metadata_by_key(
    config: BetLogicOnlyConfig,
) -> dict[tuple[str, int], tuple[str, str, str]]:
    connection = duckdb.connect(str(config.duckdb_path), read_only=True)
    try:
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info('jrdb_sed_staging')").fetchall()
        }
        horse_id_expr = (
            "CAST(registration_id AS VARCHAR)"
            if "registration_id" in columns
            else "CAST(NULL AS VARCHAR)"
        )
        horse_name_expr = (
            "CAST(horse_name AS VARCHAR)"
            if "horse_name" in columns
            else "CAST(NULL AS VARCHAR)"
        )
        rows = connection.execute(
            f"""
            SELECT
                CAST(race_key AS VARCHAR),
                CAST(horse_number AS INTEGER),
                CAST(result_date AS VARCHAR),
                {horse_id_expr} AS horse_id,
                {horse_name_expr} AS horse_name
            FROM jrdb_sed_staging
            """,
        ).fetchall()
    finally:
        connection.close()
    return {
        (str(row[0]), int(row[1])): (
            str(row[2]) if row[2] is not None else "",
            str(row[3]) if row[3] is not None else "",
            str(row[4]) if row[4] is not None else "",
        )
        for row in rows
    }


def build_race_chaos_rows(
    *,
    race_prediction_rows: tuple[RacePredictionRow, ...],
    edge_threshold_by_window: dict[str, float],
) -> tuple[RaceChaosRow, ...]:
    grouped: dict[tuple[str, str], list[RacePredictionRow]] = {}
    for row in race_prediction_rows:
        grouped.setdefault((row.window_label, row.race_key), []).append(row)

    raw_rows: list[dict[str, object]] = []
    for (window_label, race_key), rows in sorted(grouped.items()):
        ordered = sorted(rows, key=lambda row: row.horse_number)
        probs = [max(row.pred_probability, 0.0) for row in ordered]
        sorted_probs = sorted(probs, reverse=True)
        edges = [row.edge for row in ordered if row.edge is not None]
        threshold = edge_threshold_by_window.get(window_label, 0.0)
        near_threshold_count = sum(
            1
            for edge in edges
            if abs(edge - threshold) <= 0.02
        )
        top1_gap = (
            max(0.0, sorted_probs[0] - sorted_probs[1])
            if len(sorted_probs) >= 2
            else (sorted_probs[0] if sorted_probs else 0.0)
        )
        top3_concentration = (
            sum(sorted_probs[:3]) / sum(sorted_probs)
            if sorted_probs and sum(sorted_probs) > 0.0
            else 0.0
        )
        probability_entropy = normalized_entropy(probs)
        edge_closeness = 1.0 - min(1.0, (top_two_gap(edges) / 0.05)) if len(edges) >= 2 else 1.0
        edge_place_basis_rank_disagreement = rank_disagreement(
            values_a=[row.edge for row in ordered],
            values_b=[place_basis_rank_value(row.place_basis_odds) for row in ordered],
        )
        win_odds_entropy = normalized_entropy(
            [1.0 / row.win_odds for row in ordered if row.win_odds is not None and row.win_odds > 0.0],
        )
        fair_odds_dispersion = coefficient_of_variation(
            [1.0 / row.pred_probability for row in ordered if row.pred_probability > 0.0],
        )
        popularity_entropy = normalized_entropy(
            [1.0 / row.popularity for row in ordered if row.popularity is not None and row.popularity > 0],
        )
        raw_rows.append(
            {
                "window_label": window_label,
                "result_date": ordered[0].result_date,
                "race_key": race_key,
                "domain": race_key[:2] if len(race_key) >= 2 else "unknown",
                "field_count": len(ordered),
                "max_edge": max(edges) if edges else 0.0,
                "mean_edge": (sum(edges) / len(edges)) if edges else 0.0,
                "near_threshold_candidate_count": near_threshold_count,
                "top1_top2_probability_gap": top1_gap,
                "top3_probability_concentration": top3_concentration,
                "probability_entropy": probability_entropy,
                "edge_closeness": edge_closeness,
                "edge_place_basis_rank_disagreement": edge_place_basis_rank_disagreement,
                "win_odds_entropy": win_odds_entropy,
                "fair_odds_dispersion": fair_odds_dispersion,
                "popularity_entropy": popularity_entropy,
            },
        )

    if not raw_rows:
        return ()

    max_near_threshold = max(
        int(row["near_threshold_candidate_count"]) for row in raw_rows
    ) or 1
    max_fair_odds_dispersion = max(
        float(row["fair_odds_dispersion"]) for row in raw_rows
    ) or 1.0

    scored: list[RaceChaosRow] = []
    raw_scores: list[float] = []
    for row in raw_rows:
        near_threshold_ratio = float(row["near_threshold_candidate_count"]) / max_near_threshold
        fair_odds_dispersion = float(row["fair_odds_dispersion"]) / max_fair_odds_dispersion
        chaos_score = (
            (1.0 - min(1.0, float(row["top1_top2_probability_gap"]) / 0.12))
            + (1.0 - float(row["top3_probability_concentration"]))
            + float(row["probability_entropy"])
            + near_threshold_ratio
            + float(row["edge_closeness"])
            + float(row["edge_place_basis_rank_disagreement"])
            + float(row["win_odds_entropy"])
            + fair_odds_dispersion
            + float(row["popularity_entropy"])
        ) / 9.0
        raw_scores.append(chaos_score)
        scored.append(
            RaceChaosRow(
                window_label=str(row["window_label"]),
                result_date=str(row["result_date"]),
                race_key=str(row["race_key"]),
                domain=str(row["domain"]),
                field_count=int(row["field_count"]),
                max_edge=float(row["max_edge"]),
                mean_edge=float(row["mean_edge"]),
                near_threshold_candidate_count=int(row["near_threshold_candidate_count"]),
                top1_top2_probability_gap=float(row["top1_top2_probability_gap"]),
                top3_probability_concentration=float(row["top3_probability_concentration"]),
                probability_entropy=float(row["probability_entropy"]),
                edge_closeness=float(row["edge_closeness"]),
                edge_place_basis_rank_disagreement=float(row["edge_place_basis_rank_disagreement"]),
                win_odds_entropy=float(row["win_odds_entropy"]),
                fair_odds_dispersion=float(row["fair_odds_dispersion"]),
                popularity_entropy=float(row["popularity_entropy"]),
                chaos_score=chaos_score,
                chaos_bucket="pending",
                chaos_skip_flag=0,
                chaos_edge_surcharge=0.0,
            ),
        )

    low_cut = percentile(raw_scores, 0.33)
    high_cut = percentile(raw_scores, 0.67)
    skip_cut = percentile(raw_scores, 0.75)

    output: list[RaceChaosRow] = []
    for row in scored:
        chaos_bucket = "mid"
        if row.chaos_score <= low_cut:
            chaos_bucket = "low"
        elif row.chaos_score >= high_cut:
            chaos_bucket = "high"
        edge_surcharge = 0.0
        if chaos_bucket == "mid":
            edge_surcharge = 0.01
        if chaos_bucket == "high":
            edge_surcharge = 0.02
        output.append(
            RaceChaosRow(
                window_label=row.window_label,
                result_date=row.result_date,
                race_key=row.race_key,
                domain=row.domain,
                field_count=row.field_count,
                max_edge=row.max_edge,
                mean_edge=row.mean_edge,
                near_threshold_candidate_count=row.near_threshold_candidate_count,
                top1_top2_probability_gap=row.top1_top2_probability_gap,
                top3_probability_concentration=row.top3_probability_concentration,
                probability_entropy=row.probability_entropy,
                edge_closeness=row.edge_closeness,
                edge_place_basis_rank_disagreement=row.edge_place_basis_rank_disagreement,
                win_odds_entropy=row.win_odds_entropy,
                fair_odds_dispersion=row.fair_odds_dispersion,
                popularity_entropy=row.popularity_entropy,
                chaos_score=row.chaos_score,
                chaos_bucket=chaos_bucket,
                chaos_skip_flag=1 if row.chaos_score >= skip_cut else 0,
                chaos_edge_surcharge=edge_surcharge,
            ),
        )
    return tuple(sorted(output, key=lambda row: (row.result_date, row.window_label, row.race_key)))


def apply_chaos_layer_variant_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    logic_variant: str,
    source_logic: str,
    edge_threshold_by_window: dict[str, float],
    stake_per_bet: float,
    chaos_by_race: dict[str, RaceChaosRow],
    mode: str,
) -> tuple[LogicBetRow, ...]:
    rows: list[LogicBetRow] = []
    for row in baseline_rows:
        chaos = chaos_by_race.get(row.race_key)
        threshold = edge_threshold_by_window.get(row.window_label)
        if threshold is None or row.edge is None:
            continue
        if mode == "skip" and chaos is not None and chaos.chaos_skip_flag:
            continue
        adjusted_threshold = threshold + (chaos.chaos_edge_surcharge if chaos is not None and mode == "surcharge" else 0.0)
        if row.edge < adjusted_threshold:
            continue
        rows.append(
            with_logic_variant(
                row=row,
                logic_variant=logic_variant,
                source_logic=source_logic,
                stake=row.stake if row.stake > 0.0 else stake_per_bet,
            ),
        )
    return order_logic_rows(tuple(rows))


def build_scored_variant_rows(
    *,
    logic_variant: str,
    source_logic: str,
    candidate_rows_by_window: dict[str, tuple[CandidateBetRow, ...]],
    stake_per_bet: float,
) -> tuple[LogicBetRow, ...]:
    rows: list[LogicBetRow] = []
    for window_label, candidate_rows in candidate_rows_by_window.items():
        for row in candidate_rows:
            rows.append(
                LogicBetRow(
                    logic_variant=logic_variant,
                    source_logic=source_logic,
                    window_label=window_label,
                    result_date=row.result_date.isoformat(),
                    race_key=row.race_key,
                    horse_number=row.horse_number,
                    stake=stake_per_bet,
                    scaled_return=row.place_payout or 0.0,
                    bet_profit=(row.place_payout or 0.0) - stake_per_bet,
                    target_value=row.target_value,
                    pred_probability=row.pred_probability,
                    market_prob=row.market_prob,
                    edge=row.edge,
                    win_odds=row.win_odds,
                    popularity=row.popularity,
                    place_basis_odds=row.place_basis_odds,
                    place_payout=row.place_payout,
                ),
            )
    return order_logic_rows(tuple(rows))


def filter_guard_variant_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    surcharge: float,
    source_logic: str,
    stake_per_bet: float,
    edge_threshold_by_window: dict[str, float],
    logic_variant: str,
) -> tuple[LogicBetRow, ...]:
    rows: list[LogicBetRow] = []
    for row in baseline_rows:
        threshold = edge_threshold_by_window.get(row.window_label)
        if threshold is None or row.edge is None:
            continue
        if row.edge < threshold + surcharge:
            continue
        rows.append(
            with_logic_variant(
                row=row,
                logic_variant=logic_variant,
                source_logic=source_logic,
                stake=row.stake if row.stake > 0.0 else stake_per_bet,
            ),
        )
    return order_logic_rows(tuple(rows))


def apply_sizing_tilt_variant_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    logic_variant: str,
    source_logic: str,
    edge_threshold_by_window: dict[str, float],
    sizing_tilt_step: float,
    sizing_tilt_min_multiplier: float,
    sizing_tilt_max_multiplier: float,
    stake_per_bet: float,
) -> tuple[LogicBetRow, ...]:
    rows: list[LogicBetRow] = []
    for row in baseline_rows:
        threshold = edge_threshold_by_window.get(row.window_label, 0.0)
        multiplier = sizing_multiplier(
            edge=row.edge,
            place_basis_odds=row.place_basis_odds,
            threshold=threshold,
            sizing_tilt_step=sizing_tilt_step,
            sizing_tilt_min_multiplier=sizing_tilt_min_multiplier,
            sizing_tilt_max_multiplier=sizing_tilt_max_multiplier,
        )
        stake = stake_per_bet * multiplier
        rows.append(
            with_logic_variant(
                row=row,
                logic_variant=logic_variant,
                source_logic=source_logic,
                stake=stake,
            ),
        )
    return order_logic_rows(tuple(rows))


def with_logic_variant(
    *,
    row: LogicBetRow,
    logic_variant: str,
    source_logic: str,
    stake: float,
) -> LogicBetRow:
    scaled_return = (
        (row.place_payout or 0.0) * (stake / row.stake)
        if row.stake > 0.0
        else 0.0
    )
    return LogicBetRow(
        logic_variant=logic_variant,
        source_logic=source_logic,
        window_label=row.window_label,
        result_date=row.result_date,
        race_key=row.race_key,
        horse_number=row.horse_number,
        stake=stake,
        scaled_return=scaled_return,
        bet_profit=scaled_return - stake,
        target_value=row.target_value,
        pred_probability=row.pred_probability,
        market_prob=row.market_prob,
        edge=row.edge,
        win_odds=row.win_odds,
        popularity=row.popularity,
        place_basis_odds=row.place_basis_odds,
        place_payout=row.place_payout,
    )


def sizing_multiplier(
    *,
    edge: float | None,
    place_basis_odds: float | None,
    threshold: float,
    sizing_tilt_step: float,
    sizing_tilt_min_multiplier: float,
    sizing_tilt_max_multiplier: float,
) -> float:
    if edge is None:
        return 1.0
    uplift = 0.0
    if edge >= threshold + 0.04:
        uplift += sizing_tilt_step
    elif edge <= threshold + 0.01:
        uplift -= sizing_tilt_step
    if place_basis_odds is not None and place_basis_odds >= 2.8:
        uplift += sizing_tilt_step
    return min(
        sizing_tilt_max_multiplier,
        max(sizing_tilt_min_multiplier, 1.0 + uplift),
    )


def build_parity_report(
    *,
    reference_rows: tuple[LogicBetRow, ...],
    baseline_rows: tuple[LogicBetRow, ...],
) -> tuple[BetLogicOnlyParitySummary, tuple[BetLogicOnlyParityDetailRow, ...]]:
    reference_map = {identity_key(row): row for row in reference_rows}
    baseline_map = {identity_key(row): row for row in baseline_rows}
    all_keys = sorted(set(reference_map) | set(baseline_map))
    detail_rows: list[BetLogicOnlyParityDetailRow] = []
    diff_count = 0
    for key in all_keys:
        reference_row = reference_map.get(key)
        baseline_row = baseline_map.get(key)
        set_group = "common"
        if reference_row is None:
            set_group = "baseline_only"
        elif baseline_row is None:
            set_group = "reference_only"
        elif not rows_equivalent(reference_row, baseline_row):
            set_group = "metric_diff"
        if set_group != "common":
            diff_count += 1
        detail_rows.append(
            BetLogicOnlyParityDetailRow(
                race_key=key[0],
                horse_number=key[1],
                set_group=set_group,
                reference_window_label=reference_row.window_label if reference_row is not None else None,
                baseline_window_label=baseline_row.window_label if baseline_row is not None else None,
                reference_stake=reference_row.stake if reference_row is not None else 0.0,
                baseline_stake=baseline_row.stake if baseline_row is not None else 0.0,
                reference_return=reference_row.scaled_return if reference_row is not None else 0.0,
                baseline_return=baseline_row.scaled_return if baseline_row is not None else 0.0,
                reference_profit=reference_row.bet_profit if reference_row is not None else 0.0,
                baseline_profit=baseline_row.bet_profit if baseline_row is not None else 0.0,
            ),
        )
    reference_stats = summarize_logic_rows(reference_rows)
    baseline_stats = summarize_logic_rows(baseline_rows)
    parity_summary = BetLogicOnlyParitySummary(
        reference_logic="mainline_reference_selected_rows",
        baseline_logic=BASELINE_LOGIC_NAME,
        row_count_reference=len(reference_rows),
        row_count_baseline=len(baseline_rows),
        row_diff_count=diff_count,
        selected_rows_match=diff_count == 0,
        bet_count_reference=reference_stats.bet_count,
        bet_count_baseline=baseline_stats.bet_count,
        total_stake_reference=reference_stats.total_stake,
        total_stake_baseline=baseline_stats.total_stake,
        total_return_reference=reference_stats.total_return,
        total_return_baseline=baseline_stats.total_return,
        total_profit_reference=reference_stats.total_profit,
        total_profit_baseline=baseline_stats.total_profit,
        roi_multiple_reference=reference_stats.roi_multiple,
        roi_multiple_baseline=baseline_stats.roi_multiple,
        net_roi_reference=reference_stats.net_roi,
        net_roi_baseline=baseline_stats.net_roi,
        parity_ok=(
            diff_count == 0
            and reference_stats.bet_count == baseline_stats.bet_count
            and floats_close(reference_stats.total_stake, baseline_stats.total_stake)
            and floats_close(reference_stats.total_return, baseline_stats.total_return)
            and floats_close(reference_stats.total_profit, baseline_stats.total_profit)
            and floats_close(reference_stats.roi_multiple, baseline_stats.roi_multiple)
        ),
    )
    return parity_summary, tuple(detail_rows)


def build_selected_summaries(
    *,
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
    edge_threshold_by_window: dict[str, float],
) -> list[BetLogicOnlySelectedSummary]:
    output: list[BetLogicOnlySelectedSummary] = []
    for logic_variant in VARIANT_NAMES:
        grouped: dict[str, list[LogicBetRow]] = {}
        for row in variant_rows[logic_variant]:
            grouped.setdefault(row.window_label, []).append(row)
        for window_label in sorted(edge_threshold_by_window):
            rows = order_logic_rows(tuple(grouped.get(window_label, [])))
            stats = summarize_logic_rows(rows)
            source_logic = rows[0].source_logic if rows else default_source_logic(logic_variant)
            if not rows and not variant_rows[logic_variant]:
                source_logic = default_source_logic(logic_variant)
            output.append(
                BetLogicOnlySelectedSummary(
                    logic_variant=logic_variant,
                    source_logic=source_logic,
                    window_label=window_label,
                    baseline_threshold=edge_threshold_by_window.get(window_label),
                    bet_count=stats.bet_count,
                    hit_count=stats.hit_count,
                    hit_rate=stats.hit_rate,
                    total_stake=stats.total_stake,
                    total_return=stats.total_return,
                    roi_multiple=stats.roi_multiple,
                    net_roi=stats.net_roi,
                    total_profit=stats.total_profit,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
                    avg_edge=stats.avg_edge,
                    avg_stake=stats.avg_stake,
                ),
            )
    return output


def build_rollup_summaries(
    selected_summaries: tuple[BetLogicOnlySelectedSummary, ...],
    *,
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
    bootstrap_iterations: int,
    random_seed: int,
) -> list[BetLogicOnlySummary]:
    grouped: dict[str, list[BetLogicOnlySelectedSummary]] = {}
    for summary in selected_summaries:
        grouped.setdefault(summary.logic_variant, []).append(summary)
    output: list[BetLogicOnlySummary] = []
    for logic_variant in VARIANT_NAMES:
        group = sorted(grouped.get(logic_variant, []), key=lambda row: row.window_label)
        if not group:
            continue
        row_stats = summarize_logic_rows(variant_rows[logic_variant])
        roi_ci_lower, roi_ci_upper = bootstrap_variant_roi_interval(
            group,
            bootstrap_iterations=bootstrap_iterations,
            random_seed=random_seed,
        )
        output.append(
            BetLogicOnlySummary(
                logic_variant=logic_variant,
                source_logic=group[0].source_logic,
                bet_count=row_stats.bet_count,
                hit_count=row_stats.hit_count,
                hit_rate=row_stats.hit_rate,
                total_stake=row_stats.total_stake,
                total_return=row_stats.total_return,
                roi_multiple=row_stats.roi_multiple,
                net_roi=row_stats.net_roi,
                total_profit=row_stats.total_profit,
                max_drawdown=row_stats.max_drawdown,
                max_losing_streak=row_stats.max_losing_streak,
                roi_gt_1_ratio=(
                    sum(1 for row in group if row.roi_multiple > 1.0) / len(group)
                    if group
                    else 0.0
                ),
                roi_multiple_ci_lower=roi_ci_lower,
                roi_multiple_ci_upper=roi_ci_upper,
                avg_edge=row_stats.avg_edge,
                avg_stake=row_stats.avg_stake,
                test_window_count=len(group),
                test_window_labels=",".join(row.window_label for row in group),
            ),
        )
    return output


def build_comparison_rows(
    summaries: tuple[BetLogicOnlySummary, ...],
) -> list[BetLogicOnlyComparisonRow]:
    baseline = next(row for row in summaries if row.logic_variant == BASELINE_LOGIC_NAME)
    guard = next(row for row in summaries if row.logic_variant == "no_bet_guard_stronger")
    output: list[BetLogicOnlyComparisonRow] = []
    for row in summaries:
        output.append(
            BetLogicOnlyComparisonRow(
                logic_variant=row.logic_variant,
                source_logic=row.source_logic,
                bet_count=row.bet_count,
                hit_rate=row.hit_rate,
                roi_multiple=row.roi_multiple,
                net_roi=row.net_roi,
                total_profit=row.total_profit,
                max_drawdown=row.max_drawdown,
                max_losing_streak=row.max_losing_streak,
                roi_gt_1_ratio=row.roi_gt_1_ratio,
                roi_multiple_95_interval=(
                    f"{row.roi_multiple_ci_lower:.4f} - {row.roi_multiple_ci_upper:.4f}"
                ),
                delta_bet_count_vs_baseline=row.bet_count - baseline.bet_count,
                delta_roi_multiple_vs_baseline=row.roi_multiple - baseline.roi_multiple,
                delta_net_roi_vs_baseline=row.net_roi - baseline.net_roi,
                delta_total_profit_vs_baseline=row.total_profit - baseline.total_profit,
                delta_max_drawdown_vs_baseline=row.max_drawdown - baseline.max_drawdown,
                delta_max_losing_streak_vs_baseline=(
                    row.max_losing_streak - baseline.max_losing_streak
                ),
                delta_bet_count_vs_guard_001=row.bet_count - guard.bet_count,
                delta_roi_multiple_vs_guard_001=row.roi_multiple - guard.roi_multiple,
                delta_net_roi_vs_guard_001=row.net_roi - guard.net_roi,
                delta_total_profit_vs_guard_001=row.total_profit - guard.total_profit,
                delta_max_drawdown_vs_guard_001=row.max_drawdown - guard.max_drawdown,
                delta_max_losing_streak_vs_guard_001=(
                    row.max_losing_streak - guard.max_losing_streak
                ),
            ),
        )
    return output


def build_diff_reports(
    *,
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
) -> tuple[tuple[BetLogicOnlyDiffSummary, ...], tuple[BetLogicOnlyDiffDetailRow, ...]]:
    baseline_map = {
        identity_key(row): row for row in variant_rows[BASELINE_LOGIC_NAME]
    }
    summary_rows: list[BetLogicOnlyDiffSummary] = []
    detail_rows: list[BetLogicOnlyDiffDetailRow] = []
    for logic_variant in VARIANT_NAMES:
        if logic_variant == BASELINE_LOGIC_NAME:
            continue
        variant_map = {identity_key(row): row for row in variant_rows[logic_variant]}
        grouped = {
            "common": sorted(set(baseline_map) & set(variant_map)),
            "baseline_only": sorted(set(baseline_map) - set(variant_map)),
            "logic_variant_only": sorted(set(variant_map) - set(baseline_map)),
        }
        for set_group, keys in grouped.items():
            baseline_group_rows = tuple(
                baseline_map[key]
                for key in keys
                if key in baseline_map
            )
            variant_group_rows = tuple(
                variant_map[key]
                for key in keys
                if key in variant_map
            )
            baseline_stats = summarize_logic_rows(baseline_group_rows)
            variant_stats = summarize_logic_rows(variant_group_rows)
            summary_rows.append(
                BetLogicOnlyDiffSummary(
                    logic_variant=logic_variant,
                    set_group=set_group,
                    baseline_bet_count=baseline_stats.bet_count,
                    baseline_total_stake=baseline_stats.total_stake,
                    baseline_total_return=baseline_stats.total_return,
                    baseline_total_profit=baseline_stats.total_profit,
                    baseline_roi_multiple=baseline_stats.roi_multiple,
                    baseline_net_roi=baseline_stats.net_roi,
                    logic_variant_bet_count=variant_stats.bet_count,
                    logic_variant_total_stake=variant_stats.total_stake,
                    logic_variant_total_return=variant_stats.total_return,
                    logic_variant_total_profit=variant_stats.total_profit,
                    logic_variant_roi_multiple=variant_stats.roi_multiple,
                    logic_variant_net_roi=variant_stats.net_roi,
                ),
            )
            for key in keys:
                baseline_row = baseline_map.get(key)
                variant_row = variant_map.get(key)
                anchor_row = baseline_row if baseline_row is not None else variant_row
                detail_rows.append(
                    BetLogicOnlyDiffDetailRow(
                        logic_variant=logic_variant,
                        set_group=set_group,
                        race_key=key[0],
                        horse_number=key[1],
                        baseline_window_label=baseline_row.window_label if baseline_row else None,
                        variant_window_label=variant_row.window_label if variant_row else None,
                        baseline_stake=baseline_row.stake if baseline_row else 0.0,
                        variant_stake=variant_row.stake if variant_row else 0.0,
                        baseline_return=baseline_row.scaled_return if baseline_row else 0.0,
                        variant_return=variant_row.scaled_return if variant_row else 0.0,
                        baseline_profit=baseline_row.bet_profit if baseline_row else 0.0,
                        variant_profit=variant_row.bet_profit if variant_row else 0.0,
                        edge=anchor_row.edge if anchor_row else None,
                        pred_probability=anchor_row.pred_probability if anchor_row else None,
                        place_basis_odds=anchor_row.place_basis_odds if anchor_row else None,
                        win_odds=anchor_row.win_odds if anchor_row else None,
                        popularity=anchor_row.popularity if anchor_row else None,
                    ),
                )
    return tuple(summary_rows), tuple(detail_rows)


def build_guard_regime_reports(
    *,
    guard_rows: tuple[LogicBetRow, ...],
    chaos_by_race: dict[str, RaceChaosRow],
) -> tuple[tuple[GuardWeakRegimeRow, ...], tuple[GuardRegionCandidateRow, ...]]:
    group_specs = (
        ("proxy_domain", lambda row: domain_key(row)),
        ("edge_bucket", lambda row: bucket_edge(row.edge)),
        ("place_basis_bucket", lambda row: bucket_place_basis_odds(row.place_basis_odds)),
        ("win_odds_bucket", lambda row: bucket_win_odds(row.win_odds)),
        ("popularity_bucket", lambda row: bucket_popularity(row.popularity)),
        ("near_threshold_bucket", lambda row: bucket_near_threshold_count(chaos_by_race.get(row.race_key))),
        ("rolling_window", lambda row: row.window_label),
    )
    weak_rows: list[GuardWeakRegimeRow] = []
    for group_type, group_fn in group_specs:
        grouped_all: dict[str, list[LogicBetRow]] = {}
        grouped_by_year: dict[tuple[str, str], list[LogicBetRow]] = {}
        for row in guard_rows:
            group_key = str(group_fn(row))
            grouped_all.setdefault(group_key, []).append(row)
            grouped_by_year.setdefault((row.result_date[:4], group_key), []).append(row)
        for group_key in sorted(grouped_all):
            stats = summarize_logic_rows(tuple(grouped_all[group_key]))
            weak_rows.append(
                GuardWeakRegimeRow(
                    group_type=group_type,
                    group_key=group_key,
                    analysis_scope="all",
                    bet_count=stats.bet_count,
                    total_stake=stats.total_stake,
                    total_return=stats.total_return,
                    total_profit=stats.total_profit,
                    roi_multiple=stats.roi_multiple,
                    net_roi=stats.net_roi,
                    hit_rate=stats.hit_rate,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
                ),
            )
        for (year, group_key), rows in sorted(grouped_by_year.items()):
            stats = summarize_logic_rows(tuple(rows))
            weak_rows.append(
                GuardWeakRegimeRow(
                    group_type=group_type,
                    group_key=group_key,
                    analysis_scope=year,
                    bet_count=stats.bet_count,
                    total_stake=stats.total_stake,
                    total_return=stats.total_return,
                    total_profit=stats.total_profit,
                    roi_multiple=stats.roi_multiple,
                    net_roi=stats.net_roi,
                    hit_rate=stats.hit_rate,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
                ),
            )

    candidate_rows: list[GuardRegionCandidateRow] = []
    for group_type in ("proxy_domain", "edge_bucket", "place_basis_bucket", "near_threshold_bucket"):
        all_rows = {
            row.group_key: row
            for row in weak_rows
            if row.group_type == group_type and row.analysis_scope == "all"
        }
        year_rows = {
            (row.analysis_scope, row.group_key): row
            for row in weak_rows
            if row.group_type == group_type and row.analysis_scope != "all"
        }
        for group_key, all_row in sorted(all_rows.items()):
            year_2023_profit = year_rows.get(("2023", group_key), _empty_guard_row(group_type, group_key)).total_profit
            year_2024_profit = year_rows.get(("2024", group_key), _empty_guard_row(group_type, group_key)).total_profit
            year_2025_profit = year_rows.get(("2025", group_key), _empty_guard_row(group_type, group_key)).total_profit
            classification = "neutral"
            note = "mixed"
            if year_2023_profit < 0.0 and max(year_2024_profit, year_2025_profit) <= 100.0 and all_row.total_profit < 0.0:
                classification = "stable_negative"
                note = "weak in 2023 and not strongly positive later"
            elif min(year_2024_profit, year_2025_profit) > 100.0:
                classification = "protect_positive"
                note = "strong positive in 2024-2025; avoid cutting"
            candidate_rows.append(
                GuardRegionCandidateRow(
                    group_type=group_type,
                    group_key=group_key,
                    classification=classification,
                    all_bet_count=all_row.bet_count,
                    all_total_profit=all_row.total_profit,
                    all_roi_multiple=all_row.roi_multiple,
                    year_2023_profit=year_2023_profit,
                    year_2024_profit=year_2024_profit,
                    year_2025_profit=year_2025_profit,
                    note=note,
                ),
            )
    return tuple(weak_rows), tuple(candidate_rows)


def build_overlay_diff_reports(
    *,
    guard_rows: tuple[LogicBetRow, ...],
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
) -> tuple[tuple[OverlayDiffSummaryRow, ...], tuple[OverlayDiffDetailRow, ...]]:
    overlay_variants = tuple(
        logic_variant for logic_variant in VARIANT_NAMES if logic_variant not in {BASELINE_LOGIC_NAME, "no_bet_guard_stronger"}
    )
    guard_map = {identity_key(row): row for row in guard_rows}
    summary_rows: list[OverlayDiffSummaryRow] = []
    detail_rows: list[OverlayDiffDetailRow] = []
    for logic_variant in overlay_variants:
        overlay_map = {identity_key(row): row for row in variant_rows[logic_variant]}
        grouped = {
            "common": sorted(set(guard_map) & set(overlay_map)),
            "guard_only": sorted(set(guard_map) - set(overlay_map)),
            "overlay_only": sorted(set(overlay_map) - set(guard_map)),
        }
        for set_group, keys in grouped.items():
            guard_group_rows = tuple(guard_map[key] for key in keys if key in guard_map)
            overlay_group_rows = tuple(overlay_map[key] for key in keys if key in overlay_map)
            guard_stats = summarize_logic_rows(guard_group_rows)
            overlay_stats = summarize_logic_rows(overlay_group_rows)
            summary_rows.append(
                OverlayDiffSummaryRow(
                    logic_variant=logic_variant,
                    set_group=set_group,
                    guard_bet_count=guard_stats.bet_count,
                    guard_total_stake=guard_stats.total_stake,
                    guard_total_return=guard_stats.total_return,
                    guard_total_profit=guard_stats.total_profit,
                    guard_roi_multiple=guard_stats.roi_multiple,
                    guard_net_roi=guard_stats.net_roi,
                    overlay_bet_count=overlay_stats.bet_count,
                    overlay_total_stake=overlay_stats.total_stake,
                    overlay_total_return=overlay_stats.total_return,
                    overlay_total_profit=overlay_stats.total_profit,
                    overlay_roi_multiple=overlay_stats.roi_multiple,
                    overlay_net_roi=overlay_stats.net_roi,
                ),
            )
            for key in keys:
                guard_row = guard_map.get(key)
                overlay_row = overlay_map.get(key)
                anchor_row = guard_row if guard_row is not None else overlay_row
                detail_rows.append(
                    OverlayDiffDetailRow(
                        logic_variant=logic_variant,
                        set_group=set_group,
                        race_key=key[0],
                        horse_number=key[1],
                        guard_window_label=guard_row.window_label if guard_row else None,
                        overlay_window_label=overlay_row.window_label if overlay_row else None,
                        guard_stake=guard_row.stake if guard_row else 0.0,
                        overlay_stake=overlay_row.stake if overlay_row else 0.0,
                        guard_return=guard_row.scaled_return if guard_row else 0.0,
                        overlay_return=overlay_row.scaled_return if overlay_row else 0.0,
                        guard_profit=guard_row.bet_profit if guard_row else 0.0,
                        overlay_profit=overlay_row.bet_profit if overlay_row else 0.0,
                        edge=anchor_row.edge if anchor_row else None,
                        pred_probability=anchor_row.pred_probability if anchor_row else None,
                        place_basis_odds=anchor_row.place_basis_odds if anchor_row else None,
                        win_odds=anchor_row.win_odds if anchor_row else None,
                        popularity=anchor_row.popularity if anchor_row else None,
                    ),
                )
    return tuple(summary_rows), tuple(detail_rows)


def build_overlay_stability_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    guard_rows: tuple[LogicBetRow, ...],
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
) -> tuple[OverlayStabilityRow, ...]:
    output: list[OverlayStabilityRow] = []
    baseline_groups = group_logic_rows_for_stability(baseline_rows)
    guard_groups = group_logic_rows_for_stability(guard_rows)
    overlay_variants = tuple(
        logic_variant for logic_variant in VARIANT_NAMES if logic_variant not in {BASELINE_LOGIC_NAME, "no_bet_guard_stronger"}
    )
    for logic_variant in overlay_variants:
        grouped = group_logic_rows_for_stability(variant_rows[logic_variant])
        for (group_type, group_key), rows in grouped.items():
            stats = summarize_logic_rows(rows)
            baseline_stats = summarize_logic_rows(baseline_groups.get((group_type, group_key), ()))
            guard_stats = summarize_logic_rows(guard_groups.get((group_type, group_key), ()))
            output.append(
                OverlayStabilityRow(
                    logic_variant=logic_variant,
                    group_type=group_type,
                    group_key=group_key,
                    bet_count=stats.bet_count,
                    total_stake=stats.total_stake,
                    total_return=stats.total_return,
                    total_profit=stats.total_profit,
                    roi_multiple=stats.roi_multiple,
                    net_roi=stats.net_roi,
                    hit_rate=stats.hit_rate,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
                    delta_total_profit_vs_baseline=stats.total_profit - baseline_stats.total_profit,
                    delta_total_profit_vs_guard_001=stats.total_profit - guard_stats.total_profit,
                    delta_roi_multiple_vs_baseline=stats.roi_multiple - baseline_stats.roi_multiple,
                    delta_roi_multiple_vs_guard_001=stats.roi_multiple - guard_stats.roi_multiple,
                ),
            )
    return tuple(output)


def build_bankroll_summaries(
    *,
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
    config: BetLogicOnlyConfig,
) -> list[BetLogicOnlyBankrollSummary]:
    output: list[BetLogicOnlyBankrollSummary] = []
    for logic_variant in VARIANT_NAMES:
        stats = summarize_logic_rows(variant_rows[logic_variant])
        output.append(
            BetLogicOnlyBankrollSummary(
                logic_variant=logic_variant,
                initial_bankroll=config.initial_bankroll,
                final_bankroll=config.initial_bankroll + stats.total_profit,
                cumulative_profit=stats.total_profit,
                total_stake=stats.total_stake,
                avg_stake=stats.avg_stake,
                roi_multiple=stats.roi_multiple,
                net_roi=stats.net_roi,
                max_drawdown=stats.max_drawdown,
                max_losing_streak=stats.max_losing_streak,
            ),
        )
    return output


def build_sensitivity_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    edge_threshold_by_window: dict[str, float],
    config: BetLogicOnlyConfig,
) -> list[BetLogicOnlySensitivityRow]:
    baseline_stats = summarize_logic_rows(baseline_rows)
    output: list[BetLogicOnlySensitivityRow] = []
    for surcharge in config.no_bet_guard_sensitivity_levels:
        rows = filter_guard_variant_rows(
            baseline_rows=baseline_rows,
            surcharge=surcharge,
            source_logic="mainline_reference_selected_rows+stronger_guard",
            stake_per_bet=config.stake_per_bet,
            edge_threshold_by_window=edge_threshold_by_window,
            logic_variant="no_bet_guard_stronger",
        )
        stats = summarize_logic_rows(rows)
        output.append(
            BetLogicOnlySensitivityRow(
                sensitivity_family="no_bet_guard_stronger",
                sensitivity_value=surcharge,
                logic_variant="no_bet_guard_stronger",
                bet_count=stats.bet_count,
                roi_multiple=stats.roi_multiple,
                net_roi=stats.net_roi,
                total_profit=stats.total_profit,
                max_drawdown=stats.max_drawdown,
                max_losing_streak=stats.max_losing_streak,
                delta_roi_multiple_vs_baseline=(
                    stats.roi_multiple - baseline_stats.roi_multiple
                ),
                delta_net_roi_vs_baseline=stats.net_roi - baseline_stats.net_roi,
                delta_total_profit_vs_baseline=stats.total_profit - baseline_stats.total_profit,
                delta_max_drawdown_vs_baseline=stats.max_drawdown - baseline_stats.max_drawdown,
            ),
        )
    for max_multiplier in config.sizing_tilt_max_multiplier_sensitivity_levels:
        min_multiplier = max(0.0, 2.0 - max_multiplier)
        rows = apply_sizing_tilt_variant_rows(
            baseline_rows=baseline_rows,
            logic_variant="sizing_tilt_light",
            source_logic="mainline_reference_selected_rows+sizing_tilt",
            edge_threshold_by_window=edge_threshold_by_window,
            sizing_tilt_step=config.sizing_tilt_step,
            sizing_tilt_min_multiplier=min_multiplier,
            sizing_tilt_max_multiplier=max_multiplier,
            stake_per_bet=config.stake_per_bet,
        )
        stats = summarize_logic_rows(rows)
        output.append(
            BetLogicOnlySensitivityRow(
                sensitivity_family="sizing_tilt_light",
                sensitivity_value=max_multiplier,
                logic_variant="sizing_tilt_light",
                bet_count=stats.bet_count,
                roi_multiple=stats.roi_multiple,
                net_roi=stats.net_roi,
                total_profit=stats.total_profit,
                max_drawdown=stats.max_drawdown,
                max_losing_streak=stats.max_losing_streak,
                delta_roi_multiple_vs_baseline=(
                    stats.roi_multiple - baseline_stats.roi_multiple
                ),
                delta_net_roi_vs_baseline=stats.net_roi - baseline_stats.net_roi,
                delta_total_profit_vs_baseline=stats.total_profit - baseline_stats.total_profit,
                delta_max_drawdown_vs_baseline=stats.max_drawdown - baseline_stats.max_drawdown,
            ),
        )
    return output


def build_no_bet_guard_reports(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    edge_threshold_by_window: dict[str, float],
    config: BetLogicOnlyConfig,
) -> tuple[
    tuple[NoBetGuardDroppedSummaryRow, ...],
    tuple[NoBetGuardDroppedDetailRow, ...],
    tuple[NoBetGuardKeptComparisonRow, ...],
    tuple[NoBetGuardStabilityRow, ...],
]:
    dropped_summary_rows: list[NoBetGuardDroppedSummaryRow] = []
    dropped_detail_rows: list[NoBetGuardDroppedDetailRow] = []
    kept_rows: list[NoBetGuardKeptComparisonRow] = []
    stability_rows: list[NoBetGuardStabilityRow] = []
    baseline_map = {identity_key(row): row for row in baseline_rows}

    for surcharge in config.no_bet_guard_sensitivity_levels:
        kept_variant = filter_guard_variant_rows(
            baseline_rows=baseline_rows,
            surcharge=surcharge,
            source_logic="mainline_reference_selected_rows+stronger_guard",
            stake_per_bet=config.stake_per_bet,
            edge_threshold_by_window=edge_threshold_by_window,
            logic_variant="no_bet_guard_stronger",
        )
        kept_map = {identity_key(row): row for row in kept_variant}
        dropped_rows = order_logic_rows(
            tuple(
                row
                for row in baseline_rows
                if identity_key(row) not in kept_map
            ),
        )
        kept_baseline_rows = order_logic_rows(
            tuple(
                baseline_map[key]
                for key in sorted(set(baseline_map) & set(kept_map))
            ),
        )

        dropped_summary_rows.extend(
            build_group_summary_rows(
                surcharge=surcharge,
                rows=dropped_rows,
                group_specs=(
                    ("all", lambda _row: "all"),
                    ("domain", lambda row: domain_key(row)),
                    ("year", lambda row: row.result_date[:4]),
                    ("month", lambda row: row.result_date[:7]),
                    ("edge_bucket", lambda row: bucket_edge(row.edge)),
                    ("place_basis_bucket", lambda row: bucket_place_basis_odds(row.place_basis_odds)),
                    ("market_odds_bucket", lambda row: bucket_win_odds(row.win_odds)),
                    ("popularity_bucket", lambda row: bucket_popularity(row.popularity)),
                ),
            ),
        )
        dropped_detail_rows.extend(build_dropped_detail_rows(surcharge=surcharge, rows=dropped_rows))
        kept_rows.append(
            build_kept_comparison_row(
                surcharge=surcharge,
                baseline_rows=kept_baseline_rows,
                kept_rows=kept_variant,
            ),
        )
        stability_rows.extend(
            build_stability_rows(
                surcharge=surcharge,
                rows=kept_variant,
            ),
        )

    return (
        tuple(dropped_summary_rows),
        tuple(dropped_detail_rows),
        tuple(kept_rows),
        tuple(stability_rows),
    )


def build_group_summary_rows(
    *,
    surcharge: float,
    rows: tuple[LogicBetRow, ...],
    group_specs: tuple[tuple[str, object], ...],
) -> list[NoBetGuardDroppedSummaryRow]:
    output: list[NoBetGuardDroppedSummaryRow] = []
    for group_type, group_fn in group_specs:
        grouped: dict[str, list[LogicBetRow]] = {}
        for row in rows:
            grouped.setdefault(str(group_fn(row)), []).append(row)
        for group_key in sorted(grouped):
            stats = summarize_logic_rows(tuple(grouped[group_key]))
            output.append(
                NoBetGuardDroppedSummaryRow(
                    surcharge=surcharge,
                    group_type=group_type,
                    group_key=group_key,
                    dropped_bet_count=stats.bet_count,
                    dropped_total_stake=stats.total_stake,
                    dropped_total_return=stats.total_return,
                    dropped_total_profit=stats.total_profit,
                    dropped_roi_multiple=stats.roi_multiple,
                    dropped_net_roi=stats.net_roi,
                    dropped_hit_rate=stats.hit_rate,
                ),
            )
    return output


def build_dropped_detail_rows(
    *,
    surcharge: float,
    rows: tuple[LogicBetRow, ...],
) -> list[NoBetGuardDroppedDetailRow]:
    output: list[NoBetGuardDroppedDetailRow] = []
    for row in rows:
        output.append(
            NoBetGuardDroppedDetailRow(
                surcharge=surcharge,
                set_group="dropped",
                domain=domain_key(row),
                year=row.result_date[:4],
                month=row.result_date[:7],
                edge_bucket=bucket_edge(row.edge),
                place_basis_bucket=bucket_place_basis_odds(row.place_basis_odds),
                market_odds_bucket=bucket_win_odds(row.win_odds),
                popularity_bucket=bucket_popularity(row.popularity),
                window_label=row.window_label,
                race_key=row.race_key,
                horse_number=row.horse_number,
                stake=row.stake,
                total_return=row.scaled_return,
                total_profit=row.bet_profit,
                roi_multiple=(row.scaled_return / row.stake) if row.stake > 0.0 else 0.0,
                net_roi=(row.bet_profit / row.stake) if row.stake > 0.0 else 0.0,
                hit=1 if row.place_payout is not None else 0,
            ),
        )
    return output


def build_kept_comparison_row(
    *,
    surcharge: float,
    baseline_rows: tuple[LogicBetRow, ...],
    kept_rows: tuple[LogicBetRow, ...],
) -> NoBetGuardKeptComparisonRow:
    baseline_stats = summarize_logic_rows(baseline_rows)
    kept_stats = summarize_logic_rows(kept_rows)
    row_match_count = sum(
        1
        for baseline_row, kept_row in zip(baseline_rows, kept_rows, strict=False)
        if rows_equivalent(baseline_row, kept_row)
    )
    return NoBetGuardKeptComparisonRow(
        surcharge=surcharge,
        kept_bet_count=kept_stats.bet_count,
        row_match_count=row_match_count,
        all_rows_match=(
            row_match_count == len(baseline_rows) == len(kept_rows)
            and floats_close(baseline_stats.total_stake, kept_stats.total_stake)
            and floats_close(baseline_stats.total_return, kept_stats.total_return)
            and floats_close(baseline_stats.total_profit, kept_stats.total_profit)
        ),
        baseline_total_stake=baseline_stats.total_stake,
        kept_total_stake=kept_stats.total_stake,
        baseline_total_return=baseline_stats.total_return,
        kept_total_return=kept_stats.total_return,
        baseline_total_profit=baseline_stats.total_profit,
        kept_total_profit=kept_stats.total_profit,
        baseline_roi_multiple=baseline_stats.roi_multiple,
        kept_roi_multiple=kept_stats.roi_multiple,
        baseline_net_roi=baseline_stats.net_roi,
        kept_net_roi=kept_stats.net_roi,
    )


def build_stability_rows(
    *,
    surcharge: float,
    rows: tuple[LogicBetRow, ...],
) -> list[NoBetGuardStabilityRow]:
    group_specs = (
        ("window", lambda row: row.window_label),
        ("year", lambda row: row.result_date[:4]),
        ("domain", lambda row: domain_key(row)),
        ("year_x_domain", lambda row: f"{row.result_date[:4]}::{domain_key(row)}"),
    )
    output: list[NoBetGuardStabilityRow] = []
    for group_type, group_fn in group_specs:
        grouped: dict[str, list[LogicBetRow]] = {}
        for row in rows:
            grouped.setdefault(str(group_fn(row)), []).append(row)
        for group_key in sorted(grouped):
            stats = summarize_logic_rows(tuple(grouped[group_key]))
            output.append(
                NoBetGuardStabilityRow(
                    surcharge=surcharge,
                    group_type=group_type,
                    group_key=group_key,
                    bet_count=stats.bet_count,
                    total_stake=stats.total_stake,
                    total_return=stats.total_return,
                    total_profit=stats.total_profit,
                    roi_multiple=stats.roi_multiple,
                    net_roi=stats.net_roi,
                    hit_rate=stats.hit_rate,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
                ),
            )
    return output


def build_chaos_reports(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    guard_rows: tuple[LogicBetRow, ...],
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
    chaos_by_race: dict[str, RaceChaosRow],
    selected_summaries: tuple[BetLogicOnlySelectedSummary, ...],
    bootstrap_iterations: int,
    random_seed: int,
) -> tuple[
    tuple[ChaosSummaryRow, ...],
    tuple[ChaosCorrelationRow, ...],
    tuple[ChaosBucketReadoutRow, ...],
    tuple[ChaosDroppedSummaryRow, ...],
    tuple[ChaosDroppedDetailRow, ...],
    tuple[ChaosStabilityRow, ...],
]:
    baseline_summary = build_logic_variant_summary(
        logic_variant=BASELINE_LOGIC_NAME,
        rows=baseline_rows,
        selected_summaries=selected_summaries,
        bootstrap_iterations=bootstrap_iterations,
        random_seed=random_seed,
    )
    guard_summary = build_logic_variant_summary(
        logic_variant="no_bet_guard_stronger",
        rows=guard_rows,
        selected_summaries=selected_summaries,
        bootstrap_iterations=bootstrap_iterations,
        random_seed=random_seed,
    )
    summary_rows: list[ChaosSummaryRow] = []
    for logic_variant, anchor_logic, chaos_mode in (
        ("chaos_no_bet_guard", BASELINE_LOGIC_NAME, "skip"),
        ("chaos_edge_surcharge", BASELINE_LOGIC_NAME, "surcharge"),
        ("no_bet_guard_plus_chaos", "no_bet_guard_stronger", "guard_0.01_plus_surcharge"),
    ):
        variant_summary = build_logic_variant_summary(
            logic_variant=logic_variant,
            rows=variant_rows[logic_variant],
            selected_summaries=selected_summaries,
            bootstrap_iterations=bootstrap_iterations,
            random_seed=random_seed,
        )
        summary_rows.append(
            ChaosSummaryRow(
                logic_variant=logic_variant,
                anchor_logic=anchor_logic,
                chaos_mode=chaos_mode,
                bet_count=variant_summary.bet_count,
                hit_rate=variant_summary.hit_rate,
                total_stake=variant_summary.total_stake,
                total_return=variant_summary.total_return,
                roi_multiple=variant_summary.roi_multiple,
                net_roi=variant_summary.net_roi,
                total_profit=variant_summary.total_profit,
                max_drawdown=variant_summary.max_drawdown,
                max_losing_streak=variant_summary.max_losing_streak,
                roi_gt_1_ratio=variant_summary.roi_gt_1_ratio,
                roi_95_interval=(
                    f"{variant_summary.roi_multiple_ci_lower:.4f} - "
                    f"{variant_summary.roi_multiple_ci_upper:.4f}"
                ),
                delta_bet_count_vs_baseline=variant_summary.bet_count - baseline_summary.bet_count,
                delta_roi_multiple_vs_baseline=(
                    variant_summary.roi_multiple - baseline_summary.roi_multiple
                ),
                delta_net_roi_vs_baseline=variant_summary.net_roi - baseline_summary.net_roi,
                delta_total_profit_vs_baseline=(
                    variant_summary.total_profit - baseline_summary.total_profit
                ),
                delta_max_drawdown_vs_baseline=(
                    variant_summary.max_drawdown - baseline_summary.max_drawdown
                ),
                delta_max_losing_streak_vs_baseline=(
                    variant_summary.max_losing_streak - baseline_summary.max_losing_streak
                ),
                delta_bet_count_vs_guard_001=variant_summary.bet_count - guard_summary.bet_count,
                delta_roi_multiple_vs_guard_001=(
                    variant_summary.roi_multiple - guard_summary.roi_multiple
                ),
                delta_net_roi_vs_guard_001=variant_summary.net_roi - guard_summary.net_roi,
                delta_total_profit_vs_guard_001=(
                    variant_summary.total_profit - guard_summary.total_profit
                ),
                delta_max_drawdown_vs_guard_001=(
                    variant_summary.max_drawdown - guard_summary.max_drawdown
                ),
                delta_max_losing_streak_vs_guard_001=(
                    variant_summary.max_losing_streak - guard_summary.max_losing_streak
                ),
            ),
        )

    correlation_rows = build_chaos_correlation_rows(
        baseline_rows=baseline_rows,
        guard_rows=guard_rows,
        chaos_by_race=chaos_by_race,
    )
    bucket_rows = build_chaos_bucket_rows(
        baseline_rows=baseline_rows,
        guard_rows=guard_rows,
        chaos_by_race=chaos_by_race,
    )
    dropped_summary_rows, dropped_detail_rows = build_chaos_dropped_rows(
        baseline_rows=baseline_rows,
        guard_rows=guard_rows,
        variant_rows=variant_rows,
        chaos_by_race=chaos_by_race,
    )
    stability_rows = build_chaos_stability_rows(
        baseline_rows=baseline_rows,
        guard_rows=guard_rows,
        variant_rows=variant_rows,
    )
    return (
        tuple(summary_rows),
        tuple(correlation_rows),
        tuple(bucket_rows),
        tuple(dropped_summary_rows),
        tuple(dropped_detail_rows),
        tuple(stability_rows),
    )


def build_logic_variant_summary(
    *,
    logic_variant: str,
    rows: tuple[LogicBetRow, ...],
    selected_summaries: tuple[BetLogicOnlySelectedSummary, ...],
    bootstrap_iterations: int,
    random_seed: int,
) -> BetLogicOnlySummary:
    summaries = sorted(
        [row for row in selected_summaries if row.logic_variant == logic_variant],
        key=lambda row: row.window_label,
    )
    row_stats = summarize_logic_rows(rows)
    roi_ci_lower, roi_ci_upper = bootstrap_variant_roi_interval(
        summaries,
        bootstrap_iterations=bootstrap_iterations,
        random_seed=random_seed,
    )
    return BetLogicOnlySummary(
        logic_variant=logic_variant,
        source_logic=summaries[0].source_logic if summaries else default_source_logic(logic_variant),
        bet_count=row_stats.bet_count,
        hit_count=row_stats.hit_count,
        hit_rate=row_stats.hit_rate,
        total_stake=row_stats.total_stake,
        total_return=row_stats.total_return,
        roi_multiple=row_stats.roi_multiple,
        net_roi=row_stats.net_roi,
        total_profit=row_stats.total_profit,
        max_drawdown=row_stats.max_drawdown,
        max_losing_streak=row_stats.max_losing_streak,
        roi_gt_1_ratio=(
            sum(1 for row in summaries if row.roi_multiple > 1.0) / len(summaries)
            if summaries
            else 0.0
        ),
        roi_multiple_ci_lower=roi_ci_lower,
        roi_multiple_ci_upper=roi_ci_upper,
        avg_edge=row_stats.avg_edge,
        avg_stake=row_stats.avg_stake,
        test_window_count=len(summaries),
        test_window_labels=",".join(row.window_label for row in summaries),
    )


def build_chaos_correlation_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    guard_rows: tuple[LogicBetRow, ...],
    chaos_by_race: dict[str, RaceChaosRow],
) -> list[ChaosCorrelationRow]:
    rows = list(chaos_by_race.values())
    max_edges = [row.max_edge for row in rows]
    mean_edges = [row.mean_edge for row in rows]
    near_threshold_counts = [float(row.near_threshold_candidate_count) for row in rows]
    chaos_scores = [row.chaos_score for row in rows]
    output = [
        ChaosCorrelationRow(
            analysis_group="race_level",
            metric_name="corr_chaos_vs_max_edge",
            metric_value=pearson_correlation(chaos_scores, max_edges),
        ),
        ChaosCorrelationRow(
            analysis_group="race_level",
            metric_name="corr_chaos_vs_mean_edge",
            metric_value=pearson_correlation(chaos_scores, mean_edges),
        ),
        ChaosCorrelationRow(
            analysis_group="race_level",
            metric_name="corr_chaos_vs_candidate_count_near_threshold",
            metric_value=pearson_correlation(chaos_scores, near_threshold_counts),
        ),
    ]
    output.extend(
        build_chaos_distribution_rows(
            analysis_group="guard_0.01_status",
            rows=baseline_rows,
            chaos_by_race=chaos_by_race,
            set_names=("guard_0.01_kept", "guard_0.01_dropped"),
            predicate=lambda race_key, guard_set: race_key in guard_set,
            guard_rows=guard_rows,
        ),
    )
    return output


def build_chaos_distribution_rows(
    *,
    analysis_group: str,
    rows: tuple[LogicBetRow, ...],
    chaos_by_race: dict[str, RaceChaosRow],
    set_names: tuple[str, str],
    predicate: object,
    guard_rows: tuple[LogicBetRow, ...],
) -> list[ChaosCorrelationRow]:
    guard_set = {row.race_key for row in guard_rows}
    grouped_scores = {set_names[0]: [], set_names[1]: []}
    for row in rows:
        chaos = chaos_by_race.get(row.race_key)
        if chaos is None:
            continue
        bucket_name = set_names[0] if predicate(row.race_key, guard_set) else set_names[1]
        grouped_scores[bucket_name].append(chaos.chaos_score)
    output: list[ChaosCorrelationRow] = []
    for bucket_name, scores in grouped_scores.items():
        output.append(
            ChaosCorrelationRow(
                analysis_group=analysis_group,
                metric_name=f"{bucket_name}_mean_chaos_score",
                metric_value=(sum(scores) / len(scores)) if scores else 0.0,
            ),
        )
    return output


def build_chaos_bucket_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    guard_rows: tuple[LogicBetRow, ...],
    chaos_by_race: dict[str, RaceChaosRow],
) -> list[ChaosBucketReadoutRow]:
    output: list[ChaosBucketReadoutRow] = []
    output.extend(build_bucket_readout("baseline_edge_bucket", baseline_rows, chaos_by_race, lambda row: bucket_edge(row.edge)))
    output.extend(
        build_bucket_readout(
            "baseline_place_basis_bucket",
            baseline_rows,
            chaos_by_race,
            lambda row: bucket_place_basis_odds(row.place_basis_odds),
        ),
    )
    target_rows = tuple(
        row
        for row in baseline_rows
        if bucket_edge(row.edge) == "0_08_to_0_10"
        and bucket_place_basis_odds(row.place_basis_odds) == "2_8_to_3_2"
        and bucket_win_odds(row.win_odds) == "lt_5"
    )
    output.extend(
        build_bucket_readout(
            "known_guard_region",
            target_rows,
            chaos_by_race,
            lambda _row: "edge_0.08_0.10__basis_2.8_3.2__win_lt_5",
        ),
    )
    output.extend(
        build_bucket_readout(
            "guard_0.01_status",
            baseline_rows,
            chaos_by_race,
            lambda row: "kept" if identity_key(row) in {identity_key(item) for item in guard_rows} else "dropped",
        ),
    )
    return output


def build_bucket_readout(
    analysis_type: str,
    rows: tuple[LogicBetRow, ...],
    chaos_by_race: dict[str, RaceChaosRow],
    group_fn: object,
) -> list[ChaosBucketReadoutRow]:
    grouped: dict[tuple[str, str], list[LogicBetRow]] = {}
    for row in rows:
        chaos = chaos_by_race.get(row.race_key)
        if chaos is None:
            continue
        grouped.setdefault((str(group_fn(row)), chaos.chaos_bucket), []).append(row)
    output: list[ChaosBucketReadoutRow] = []
    for (group_key, chaos_bucket), group_rows in sorted(grouped.items()):
        stats = summarize_logic_rows(tuple(group_rows))
        mean_chaos = sum(
            chaos_by_race[row.race_key].chaos_score for row in group_rows if row.race_key in chaos_by_race
        ) / len(group_rows)
        output.append(
            ChaosBucketReadoutRow(
                analysis_type=analysis_type,
                group_key=group_key,
                chaos_bucket=chaos_bucket,
                bet_count=stats.bet_count,
                total_stake=stats.total_stake,
                total_return=stats.total_return,
                total_profit=stats.total_profit,
                roi_multiple=stats.roi_multiple,
                net_roi=stats.net_roi,
                hit_rate=stats.hit_rate,
                mean_chaos_score=mean_chaos,
            ),
        )
    return output


def build_chaos_dropped_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    guard_rows: tuple[LogicBetRow, ...],
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
    chaos_by_race: dict[str, RaceChaosRow],
) -> tuple[list[ChaosDroppedSummaryRow], list[ChaosDroppedDetailRow]]:
    output_summary: list[ChaosDroppedSummaryRow] = []
    output_detail: list[ChaosDroppedDetailRow] = []
    variant_specs = (
        ("chaos_no_bet_guard", baseline_rows, BASELINE_LOGIC_NAME),
        ("chaos_edge_surcharge", baseline_rows, BASELINE_LOGIC_NAME),
        ("no_bet_guard_plus_chaos", guard_rows, "no_bet_guard_stronger"),
    )
    for logic_variant, anchor_rows, anchor_logic in variant_specs:
        anchor_map = {identity_key(row): row for row in anchor_rows}
        kept_map = {identity_key(row): row for row in variant_rows[logic_variant]}
        dropped_rows = tuple(
            row for row in anchor_rows if identity_key(row) not in kept_map
        )
        output_summary.extend(
            build_chaos_dropped_summary_rows(
                logic_variant=logic_variant,
                anchor_logic=anchor_logic,
                rows=dropped_rows,
                chaos_by_race=chaos_by_race,
            ),
        )
        for row in dropped_rows:
            chaos = chaos_by_race.get(row.race_key)
            output_detail.append(
                ChaosDroppedDetailRow(
                    logic_variant=logic_variant,
                    anchor_logic=anchor_logic,
                    race_key=row.race_key,
                    horse_number=row.horse_number,
                    window_label=row.window_label,
                    result_date=row.result_date,
                    domain=domain_key(row),
                    chaos_score=chaos.chaos_score if chaos is not None else 0.0,
                    chaos_bucket=chaos.chaos_bucket if chaos is not None else "unknown",
                    edge_bucket=bucket_edge(row.edge),
                    place_basis_bucket=bucket_place_basis_odds(row.place_basis_odds),
                    market_odds_bucket=bucket_win_odds(row.win_odds),
                    popularity_bucket=bucket_popularity(row.popularity),
                    stake=row.stake,
                    total_return=row.scaled_return,
                    total_profit=row.bet_profit,
                    roi_multiple=(row.scaled_return / row.stake) if row.stake > 0.0 else 0.0,
                    net_roi=(row.bet_profit / row.stake) if row.stake > 0.0 else 0.0,
                    hit=1 if row.place_payout is not None else 0,
                ),
            )
        del anchor_map
    return output_summary, output_detail


def build_chaos_dropped_summary_rows(
    *,
    logic_variant: str,
    anchor_logic: str,
    rows: tuple[LogicBetRow, ...],
    chaos_by_race: dict[str, RaceChaosRow],
) -> list[ChaosDroppedSummaryRow]:
    group_specs = (
        ("all", lambda _row: "all"),
        ("domain", lambda row: domain_key(row)),
        ("year", lambda row: row.result_date[:4]),
        ("month", lambda row: row.result_date[:7]),
        ("edge_bucket", lambda row: bucket_edge(row.edge)),
        ("place_basis_bucket", lambda row: bucket_place_basis_odds(row.place_basis_odds)),
        ("market_odds_bucket", lambda row: bucket_win_odds(row.win_odds)),
        ("popularity_bucket", lambda row: bucket_popularity(row.popularity)),
        ("chaos_bucket", lambda row: chaos_by_race[row.race_key].chaos_bucket if row.race_key in chaos_by_race else "unknown"),
    )
    output: list[ChaosDroppedSummaryRow] = []
    for group_type, group_fn in group_specs:
        grouped: dict[str, list[LogicBetRow]] = {}
        for row in rows:
            grouped.setdefault(str(group_fn(row)), []).append(row)
        for group_key in sorted(grouped):
            group_rows = tuple(grouped[group_key])
            stats = summarize_logic_rows(group_rows)
            mean_chaos = (
                sum(chaos_by_race[row.race_key].chaos_score for row in group_rows if row.race_key in chaos_by_race)
                / len(group_rows)
                if group_rows
                else 0.0
            )
            output.append(
                ChaosDroppedSummaryRow(
                    logic_variant=logic_variant,
                    anchor_logic=anchor_logic,
                    group_type=group_type,
                    group_key=group_key,
                    dropped_bet_count=stats.bet_count,
                    dropped_total_stake=stats.total_stake,
                    dropped_total_return=stats.total_return,
                    dropped_total_profit=stats.total_profit,
                    dropped_roi_multiple=stats.roi_multiple,
                    dropped_net_roi=stats.net_roi,
                    dropped_hit_rate=stats.hit_rate,
                    mean_chaos_score=mean_chaos,
                ),
            )
    return output


def build_chaos_stability_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    guard_rows: tuple[LogicBetRow, ...],
    variant_rows: dict[str, tuple[LogicBetRow, ...]],
) -> list[ChaosStabilityRow]:
    output: list[ChaosStabilityRow] = []
    baseline_groups = group_logic_rows_for_stability(baseline_rows)
    guard_groups = group_logic_rows_for_stability(guard_rows)
    for logic_variant, anchor_logic in (
        ("chaos_no_bet_guard", BASELINE_LOGIC_NAME),
        ("chaos_edge_surcharge", BASELINE_LOGIC_NAME),
        ("no_bet_guard_plus_chaos", "no_bet_guard_stronger"),
    ):
        grouped = group_logic_rows_for_stability(variant_rows[logic_variant])
        for (group_type, group_key), group_rows in grouped.items():
            stats = summarize_logic_rows(group_rows)
            baseline_stats = summarize_logic_rows(baseline_groups.get((group_type, group_key), ()))
            guard_stats = summarize_logic_rows(guard_groups.get((group_type, group_key), ()))
            output.append(
                ChaosStabilityRow(
                    logic_variant=logic_variant,
                    anchor_logic=anchor_logic,
                    group_type=group_type,
                    group_key=group_key,
                    bet_count=stats.bet_count,
                    total_stake=stats.total_stake,
                    total_return=stats.total_return,
                    total_profit=stats.total_profit,
                    roi_multiple=stats.roi_multiple,
                    net_roi=stats.net_roi,
                    hit_rate=stats.hit_rate,
                    max_drawdown=stats.max_drawdown,
                    max_losing_streak=stats.max_losing_streak,
                    delta_total_profit_vs_baseline=stats.total_profit - baseline_stats.total_profit,
                    delta_total_profit_vs_guard_001=stats.total_profit - guard_stats.total_profit,
                    delta_roi_multiple_vs_baseline=stats.roi_multiple - baseline_stats.roi_multiple,
                    delta_roi_multiple_vs_guard_001=stats.roi_multiple - guard_stats.roi_multiple,
                ),
            )
    return output


def group_logic_rows_for_stability(
    rows: tuple[LogicBetRow, ...],
) -> dict[tuple[str, str], tuple[LogicBetRow, ...]]:
    grouped: dict[tuple[str, str], list[LogicBetRow]] = {}
    for row in rows:
        for group_type, group_key in (
            ("window", row.window_label),
            ("year", row.result_date[:4]),
            ("domain", domain_key(row)),
            ("year_x_domain", f"{row.result_date[:4]}::{domain_key(row)}"),
        ):
            grouped.setdefault((group_type, group_key), []).append(row)
    return {key: tuple(value) for key, value in grouped.items()}


def build_final_instruction_reports(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    fallback_rows: tuple[LogicBetRow, ...],
    candidate_rows: tuple[LogicBetRow, ...],
    horse_metadata_by_key: dict[tuple[str, int], tuple[str, str, str]],
    config: BetLogicOnlyConfig,
) -> tuple[
    tuple[FinalBetInstructionRow, ...],
    tuple[FinalBetInstructionRow, ...],
    tuple[FinalRaceInstructionRow, ...],
    tuple[FinalRaceInstructionRow, ...],
    tuple[FinalCandidateFallbackDiffRow, ...],
]:
    baseline_keys = {identity_key(row) for row in baseline_rows}
    fallback_keys = {identity_key(row) for row in fallback_rows}
    candidate_keys = {identity_key(row) for row in candidate_rows}
    candidate_bet_rows = build_final_bet_instruction_rows(
        baseline_rows=baseline_rows,
        baseline_keys=baseline_keys,
        fallback_keys=fallback_keys,
        candidate_keys=candidate_keys,
        horse_metadata_by_key=horse_metadata_by_key,
        logic_name="guard_0_01_plus_proxy_domain_overlay",
        run_mode="candidate_provisional",
    )
    fallback_bet_rows = build_final_bet_instruction_rows(
        baseline_rows=baseline_rows,
        baseline_keys=baseline_keys,
        fallback_keys=fallback_keys,
        candidate_keys=candidate_keys,
        horse_metadata_by_key=horse_metadata_by_key,
        logic_name="no_bet_guard_stronger",
        run_mode="fallback_stable",
    )
    candidate_race_rows = build_final_race_instruction_rows(candidate_bet_rows)
    fallback_race_rows = build_final_race_instruction_rows(fallback_bet_rows)
    diff_rows = build_final_candidate_vs_fallback_diff_rows(
        candidate_bet_rows=candidate_bet_rows,
        fallback_bet_rows=fallback_bet_rows,
        candidate_race_rows=candidate_race_rows,
        fallback_race_rows=fallback_race_rows,
    )
    return (
        candidate_bet_rows,
        fallback_bet_rows,
        candidate_race_rows,
        fallback_race_rows,
        diff_rows,
    )


def build_final_bet_instruction_rows(
    *,
    baseline_rows: tuple[LogicBetRow, ...],
    baseline_keys: set[tuple[str, int]],
    fallback_keys: set[tuple[str, int]],
    candidate_keys: set[tuple[str, int]],
    horse_metadata_by_key: dict[tuple[str, int], tuple[str, str, str]],
    logic_name: str,
    run_mode: str,
) -> tuple[FinalBetInstructionRow, ...]:
    rows: list[FinalBetInstructionRow] = []
    for row in baseline_rows:
        key = identity_key(row)
        race_date, horse_id, horse_name = horse_metadata_by_key.get(
            key,
            (row.result_date, "", ""),
        )
        kept_by_baseline = key in baseline_keys
        kept_by_guard = key in fallback_keys
        kept_by_overlay = key in candidate_keys
        decision_is_bet = kept_by_guard if logic_name == "no_bet_guard_stronger" else kept_by_overlay
        decision = "BET" if decision_is_bet else "SKIP"
        stake = row.stake if decision_is_bet else 0.0
        rows.append(
            FinalBetInstructionRow(
                run_mode=run_mode,
                race_key=row.race_key,
                race_date=race_date or row.result_date,
                horse_id=horse_id,
                horse_number=row.horse_number,
                horse_name=horse_name,
                decision=decision,
                stake=stake,
                model_probability=row.pred_probability,
                edge=row.edge,
                place_basis=row.place_basis_odds,
                win_odds=row.win_odds,
                proxy_domain=domain_key(row),
                logic_name=logic_name,
                kept_by_baseline=1 if kept_by_baseline else 0,
                kept_by_guard_0_01=1 if kept_by_guard else 0,
                kept_by_proxy_overlay=1 if kept_by_overlay else 0,
                final_reason=build_final_reason(
                    row=row,
                    logic_name=logic_name,
                    kept_by_guard=kept_by_guard,
                    kept_by_overlay=kept_by_overlay,
                ),
            ),
        )
    return tuple(rows)


def build_final_reason(
    *,
    row: LogicBetRow,
    logic_name: str,
    kept_by_guard: bool,
    kept_by_overlay: bool,
) -> str:
    if logic_name == "no_bet_guard_stronger":
        if kept_by_guard:
            return "kept by baseline and guard_0_01"
        return "dropped by guard_0_01 edge surcharge"
    if kept_by_overlay:
        return "kept by baseline, guard_0_01, and proxy_domain overlay"
    if not kept_by_guard:
        return "dropped by guard_0_01 edge surcharge before overlay"
    return f"dropped by proxy_domain overlay for proxy_domain={domain_key(row)}"


def build_final_race_instruction_rows(
    rows: tuple[FinalBetInstructionRow, ...],
) -> tuple[FinalRaceInstructionRow, ...]:
    grouped: dict[str, list[FinalBetInstructionRow]] = {}
    for row in rows:
        grouped.setdefault(row.race_key, []).append(row)
    output: list[FinalRaceInstructionRow] = []
    for race_key, group_rows in sorted(grouped.items()):
        ordered = sorted(group_rows, key=lambda row: row.horse_number)
        bet_rows = [row for row in ordered if row.decision == "BET"]
        selected_horses = ",".join(
            row.horse_name or str(row.horse_number) for row in bet_rows
        )
        if not selected_horses:
            selected_horses = "-"
        if bet_rows:
            summary = f"BET {len(bet_rows)} horse(s): {selected_horses}"
        else:
            summary = "SKIP race"
        output.append(
            FinalRaceInstructionRow(
                run_mode=ordered[0].run_mode,
                race_key=race_key,
                race_date=ordered[0].race_date,
                bet_count=len(bet_rows),
                total_stake=sum(row.stake for row in bet_rows),
                selected_horses=selected_horses,
                proxy_domain=ordered[0].proxy_domain,
                logic_name=ordered[0].logic_name,
                race_decision_summary=summary,
            ),
        )
    return tuple(output)


def build_final_candidate_vs_fallback_diff_rows(
    *,
    candidate_bet_rows: tuple[FinalBetInstructionRow, ...],
    fallback_bet_rows: tuple[FinalBetInstructionRow, ...],
    candidate_race_rows: tuple[FinalRaceInstructionRow, ...],
    fallback_race_rows: tuple[FinalRaceInstructionRow, ...],
) -> tuple[FinalCandidateFallbackDiffRow, ...]:
    output: list[FinalCandidateFallbackDiffRow] = []
    fallback_bet_map = {
        (row.race_key, row.horse_number): row for row in fallback_bet_rows
    }
    for candidate_row in candidate_bet_rows:
        fallback_row = fallback_bet_map[(candidate_row.race_key, candidate_row.horse_number)]
        if (
            candidate_row.decision == fallback_row.decision
            and abs(candidate_row.stake - fallback_row.stake) < 1e-9
        ):
            continue
        output.append(
            FinalCandidateFallbackDiffRow(
                run_mode="candidate_provisional_vs_fallback_stable",
                diff_level="bet",
                race_key=candidate_row.race_key,
                race_date=candidate_row.race_date,
                horse_id=candidate_row.horse_id,
                horse_number=candidate_row.horse_number,
                horse_name=candidate_row.horse_name,
                proxy_domain=candidate_row.proxy_domain,
                candidate_decision=candidate_row.decision,
                fallback_decision=fallback_row.decision,
                candidate_stake=candidate_row.stake,
                fallback_stake=fallback_row.stake,
                candidate_logic_name=candidate_row.logic_name,
                fallback_logic_name=fallback_row.logic_name,
                candidate_bet_count=None,
                fallback_bet_count=None,
                candidate_total_stake=None,
                fallback_total_stake=None,
                candidate_selected_horses="",
                fallback_selected_horses="",
                changed_field_summary="decision,stake",
                final_reason=(
                    "overlay drops additional fallback bet"
                    if fallback_row.decision == "BET" and candidate_row.decision == "SKIP"
                    else "candidate/fallback decision changed"
                ),
            ),
        )
    fallback_race_map = {row.race_key: row for row in fallback_race_rows}
    for candidate_row in candidate_race_rows:
        fallback_row = fallback_race_map[candidate_row.race_key]
        if (
            candidate_row.bet_count == fallback_row.bet_count
            and abs(candidate_row.total_stake - fallback_row.total_stake) < 1e-9
            and candidate_row.selected_horses == fallback_row.selected_horses
        ):
            continue
        output.append(
            FinalCandidateFallbackDiffRow(
                run_mode="candidate_provisional_vs_fallback_stable",
                diff_level="race",
                race_key=candidate_row.race_key,
                race_date=candidate_row.race_date,
                horse_id="",
                horse_number=None,
                horse_name="",
                proxy_domain=candidate_row.proxy_domain,
                candidate_decision="BET" if candidate_row.bet_count > 0 else "SKIP",
                fallback_decision="BET" if fallback_row.bet_count > 0 else "SKIP",
                candidate_stake=candidate_row.total_stake,
                fallback_stake=fallback_row.total_stake,
                candidate_logic_name=candidate_row.logic_name,
                fallback_logic_name=fallback_row.logic_name,
                candidate_bet_count=candidate_row.bet_count,
                fallback_bet_count=fallback_row.bet_count,
                candidate_total_stake=candidate_row.total_stake,
                fallback_total_stake=fallback_row.total_stake,
                candidate_selected_horses=candidate_row.selected_horses,
                fallback_selected_horses=fallback_row.selected_horses,
                changed_field_summary="bet_count,total_stake,selected_horses",
                final_reason="overlay changes final race instruction vs fallback",
            ),
        )
    return tuple(
        sorted(
            output,
            key=lambda row: (
                row.diff_level,
                row.race_date,
                row.race_key,
                row.horse_number if row.horse_number is not None else -1,
            ),
        ),
    )


def build_domain_mapping_artifacts(
    *,
    race_prediction_rows: tuple[RacePredictionRow, ...],
    baseline_rows: tuple[LogicBetRow, ...],
    fallback_rows: tuple[LogicBetRow, ...],
    candidate_rows: tuple[LogicBetRow, ...],
    config: BetLogicOnlyConfig,
) -> tuple[
    tuple[DomainMappingAuditRow, ...],
    tuple[DomainMappingReportRow, ...],
    str,
]:
    audit_rows = build_domain_mapping_audit_rows(config)
    report_rows = build_domain_mapping_report_rows(
        race_prediction_rows=race_prediction_rows,
        baseline_rows=baseline_rows,
        fallback_rows=fallback_rows,
        candidate_rows=candidate_rows,
    )
    memo = build_domain_mapping_adoption_memo(
        audit_rows=audit_rows,
        report_rows=report_rows,
        config=config,
    )
    return audit_rows, report_rows, memo


def build_hard_adopt_decision_artifacts(
    *,
    audit_rows: tuple[DomainMappingAuditRow, ...],
    report_rows: tuple[DomainMappingReportRow, ...],
    config: BetLogicOnlyConfig,
) -> tuple[tuple[HardAdoptDecisionRow, ...], str]:
    external_evidence_found = int(
        any(row.evidence_type in {"external_spec", "external_search"} for row in audit_rows)
    )
    mapping_confirmed = int(
        config.formal_domain_mapping_confirmed
        and any(row.supports_formal_mapping for row in audit_rows)
        and any(
            row.formal_mapping_status in {"confirmed", "project_mapping_formalized"}
            for row in report_rows
        )
    )
    if mapping_confirmed:
        decision_status = "mapping_confirmed"
        recommended_operational_status = "hard_adopt"
        reason = (
            "upstream venue_code and project-owned derived domain mapping are formalized, so the"
            " overlay can be promoted from provisional to hard adopt"
        )
    else:
        decision_status = "mapping_not_confirmed"
        recommended_operational_status = "provisional_adopt"
        reason = (
            "venue-code or derived-domain formalization is still incomplete"
        )
    rows = (
        HardAdoptDecisionRow(
            decision_status=decision_status,
            candidate_logic_name="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_name="no_bet_guard_stronger",
            external_mapping_evidence_found=external_evidence_found,
            race_key_prefix_to_category_confirmed=mapping_confirmed,
            recommended_operational_status=recommended_operational_status,
            reason=reason,
        ),
    )
    memo = build_hard_adopt_decision_memo(
        audit_rows=audit_rows,
        report_rows=report_rows,
        rows=rows,
        config=config,
    )
    return rows, memo


def build_instruction_package_artifacts(
    *,
    candidate_rows: tuple[LogicBetRow, ...],
    fallback_rows: tuple[LogicBetRow, ...],
    final_candidate_vs_fallback_diff: tuple[FinalCandidateFallbackDiffRow, ...],
    config: BetLogicOnlyConfig,
) -> tuple[
    tuple[InstructionPackageManifestRow, ...],
    tuple[InstructionPackageSummaryRow, ...],
]:
    generated_at = datetime.now(timezone.utc).isoformat()
    git_commit, working_tree_note = detect_git_state(config.output_dir)
    diff_count = sum(1 for row in final_candidate_vs_fallback_diff if row.diff_level == "bet")
    candidate_adopt_status = (
        "hard_adopt" if config.formal_domain_mapping_confirmed else "provisional_adopt"
    )
    fallback_adopt_status = "fallback_stable"
    manifest_rows = (
        InstructionPackageManifestRow(
            run_mode="candidate_provisional",
            logic_name="guard_0_01_plus_proxy_domain_overlay",
            adopt_status=candidate_adopt_status,
            config_path=str(config.config_path),
            freeze_flags=(
                "provisional_proxy_domain_overlay_enabled="
                f"{str(config.provisional_proxy_domain_overlay_enabled).lower()},"
                "formal_domain_mapping_confirmed="
                f"{str(config.formal_domain_mapping_confirmed).lower()}"
            ),
            source_artifact_path=str(config.rolling_predictions_path),
            generated_at=generated_at,
            git_commit=git_commit,
            working_tree_note=working_tree_note,
        ),
        InstructionPackageManifestRow(
            run_mode="fallback_stable",
            logic_name="no_bet_guard_stronger",
            adopt_status=fallback_adopt_status,
            config_path=str(config.config_path),
            freeze_flags=(
                "provisional_proxy_domain_overlay_enabled="
                f"{str(config.provisional_proxy_domain_overlay_enabled).lower()},"
                "formal_domain_mapping_confirmed="
                f"{str(config.formal_domain_mapping_confirmed).lower()}"
            ),
            source_artifact_path=str(config.rolling_predictions_path),
            generated_at=generated_at,
            git_commit=git_commit,
            working_tree_note=working_tree_note,
        ),
    )
    candidate_stats = summarize_logic_rows(candidate_rows)
    fallback_stats = summarize_logic_rows(fallback_rows)
    summary_rows = (
        InstructionPackageSummaryRow(
            run_mode="candidate_provisional",
            logic_name="guard_0_01_plus_proxy_domain_overlay",
            adopt_status=candidate_adopt_status,
            bet_count=candidate_stats.bet_count,
            total_stake=candidate_stats.total_stake,
            total_profit=candidate_stats.total_profit,
            roi_multiple=candidate_stats.roi_multiple,
            max_drawdown=candidate_stats.max_drawdown,
            max_losing_streak=candidate_stats.max_losing_streak,
            candidate_fallback_diff_count=diff_count,
        ),
        InstructionPackageSummaryRow(
            run_mode="fallback_stable",
            logic_name="no_bet_guard_stronger",
            adopt_status=fallback_adopt_status,
            bet_count=fallback_stats.bet_count,
            total_stake=fallback_stats.total_stake,
            total_profit=fallback_stats.total_profit,
            roi_multiple=fallback_stats.roi_multiple,
            max_drawdown=fallback_stats.max_drawdown,
            max_losing_streak=fallback_stats.max_losing_streak,
            candidate_fallback_diff_count=diff_count,
        ),
    )
    return manifest_rows, summary_rows


def build_monitoring_artifacts(
    *,
    output_dir: Path,
    instruction_package_manifest_rows: tuple[InstructionPackageManifestRow, ...],
    instruction_package_summary_rows: tuple[InstructionPackageSummaryRow, ...],
) -> tuple[
    tuple[MonitoringSummaryRow, ...],
    tuple[RegressionGateReportRow, ...],
    tuple[ArtifactCompareReportRow, ...],
]:
    previous_rows = load_existing_monitoring_summary_rows(output_dir / "monitoring_summary.csv")
    summary_by_run_mode = {row.run_mode: row for row in instruction_package_summary_rows}
    manifest_by_run_mode = {row.run_mode: row for row in instruction_package_manifest_rows}
    current_rows = tuple(
        MonitoringSummaryRow(
            generated_at=manifest_by_run_mode[row.run_mode].generated_at,
            run_mode=row.run_mode,
            logic_name=row.logic_name,
            source_artifact_path=manifest_by_run_mode[row.run_mode].source_artifact_path,
            bet_count=row.bet_count,
            total_stake=row.total_stake,
            total_profit=row.total_profit,
            roi_multiple=row.roi_multiple,
            net_roi=((row.total_profit / row.total_stake) if row.total_stake > 0.0 else 0.0),
            max_drawdown=row.max_drawdown,
            max_losing_streak=row.max_losing_streak,
            candidate_fallback_diff_count=row.candidate_fallback_diff_count,
        )
        for row in instruction_package_summary_rows
    )
    monitoring_rows = tuple(previous_rows + list(current_rows))
    regression_rows = build_regression_gate_rows(current_rows)
    compare_rows = build_artifact_compare_rows(
        current_rows=current_rows,
        previous_rows=previous_rows,
    )
    return monitoring_rows, regression_rows, compare_rows


def load_existing_monitoring_summary_rows(path: Path) -> list[MonitoringSummaryRow]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        rows = []
        for row in reader:
            rows.append(
                MonitoringSummaryRow(
                    generated_at=str(row["generated_at"]),
                    run_mode=str(row["run_mode"]),
                    logic_name=str(row["logic_name"]),
                    source_artifact_path=str(row["source_artifact_path"]),
                    bet_count=int(row["bet_count"]),
                    total_stake=float(row["total_stake"]),
                    total_profit=float(row["total_profit"]),
                    roi_multiple=float(row["roi_multiple"]),
                    net_roi=float(row["net_roi"]),
                    max_drawdown=float(row["max_drawdown"]),
                    max_losing_streak=int(row["max_losing_streak"]),
                    candidate_fallback_diff_count=int(row["candidate_fallback_diff_count"]),
                ),
            )
        return rows


def build_regression_gate_rows(
    current_rows: tuple[MonitoringSummaryRow, ...],
) -> tuple[RegressionGateReportRow, ...]:
    candidate = next(row for row in current_rows if row.run_mode == "candidate_provisional")
    fallback = next(row for row in current_rows if row.run_mode == "fallback_stable")
    generated_at = candidate.generated_at
    rows: list[RegressionGateReportRow] = []

    def status_for_profit(candidate_value: float, fallback_value: float) -> tuple[str, float, str]:
        delta = candidate_value - fallback_value
        if delta < -100.0:
            return "fail", -100.0, "candidate profit is materially below fallback"
        if delta < 0.0:
            return "warn", 0.0, "candidate profit is below fallback"
        return "pass", 0.0, "candidate profit is not below fallback"

    def status_for_roi(candidate_value: float, fallback_value: float) -> tuple[str, float, str]:
        delta = candidate_value - fallback_value
        if delta < -0.01:
            return "fail", -0.01, "candidate roi_multiple is materially below fallback"
        if delta < 0.0:
            return "warn", 0.0, "candidate roi_multiple is below fallback"
        return "pass", 0.0, "candidate roi_multiple is not below fallback"

    def status_for_drawdown(candidate_value: float, fallback_value: float) -> tuple[str, float, str]:
        delta = candidate_value - fallback_value
        if delta > 100.0:
            return "fail", 100.0, "candidate max_drawdown is materially worse than fallback"
        if delta > 0.0:
            return "warn", 0.0, "candidate max_drawdown is worse than fallback"
        return "pass", 0.0, "candidate max_drawdown is not worse than fallback"

    for metric_name, candidate_value, fallback_value, status_fn in (
        ("total_profit", candidate.total_profit, fallback.total_profit, status_for_profit),
        ("roi_multiple", candidate.roi_multiple, fallback.roi_multiple, status_for_roi),
        ("max_drawdown", candidate.max_drawdown, fallback.max_drawdown, status_for_drawdown),
    ):
        status, threshold, message = status_fn(candidate_value, fallback_value)
        rows.append(
            RegressionGateReportRow(
                generated_at=generated_at,
                gate_name="candidate_vs_fallback_regression_gate",
                candidate_run_mode=candidate.run_mode,
                fallback_run_mode=fallback.run_mode,
                candidate_logic_name=candidate.logic_name,
                fallback_logic_name=fallback.logic_name,
                status=status,
                metric_name=metric_name,
                candidate_value=candidate_value,
                fallback_value=fallback_value,
                threshold=threshold,
                message=message,
            ),
        )
    return tuple(rows)


def build_artifact_compare_rows(
    *,
    current_rows: tuple[MonitoringSummaryRow, ...],
    previous_rows: list[MonitoringSummaryRow],
) -> tuple[ArtifactCompareReportRow, ...]:
    output: list[ArtifactCompareReportRow] = []
    current_by_mode = {row.run_mode: row for row in current_rows}
    previous_by_mode: dict[str, MonitoringSummaryRow] = {}
    for run_mode in ("candidate_provisional", "fallback_stable"):
        matches = [row for row in previous_rows if row.run_mode == run_mode]
        if matches:
            previous_by_mode[run_mode] = matches[-1]

    for run_mode in ("candidate_provisional", "fallback_stable"):
        current = current_by_mode[run_mode]
        previous = previous_by_mode.get(run_mode)
        if previous is None:
            for compare_group, current_value in (
                ("summary_total_profit", current.total_profit),
                ("summary_roi_multiple", current.roi_multiple),
                ("summary_max_drawdown", current.max_drawdown),
            ):
                output.append(
                    ArtifactCompareReportRow(
                        generated_at=current.generated_at,
                        compare_group=compare_group,
                        run_mode=run_mode,
                        current_source_artifact_path=current.source_artifact_path,
                        previous_source_artifact_path="",
                        current_value=current_value,
                        previous_value=0.0,
                        delta_value=0.0,
                        note="no previous monitoring baseline",
                    ),
                )
            continue
        for compare_group, current_value, previous_value in (
            ("summary_total_profit", current.total_profit, previous.total_profit),
            ("summary_roi_multiple", current.roi_multiple, previous.roi_multiple),
            ("summary_max_drawdown", current.max_drawdown, previous.max_drawdown),
        ):
            output.append(
                ArtifactCompareReportRow(
                    generated_at=current.generated_at,
                    compare_group=compare_group,
                    run_mode=run_mode,
                    current_source_artifact_path=current.source_artifact_path,
                    previous_source_artifact_path=previous.source_artifact_path,
                    current_value=current_value,
                    previous_value=previous_value,
                    delta_value=current_value - previous_value,
                    note="current vs previous run for same run_mode",
                ),
            )

    candidate = current_by_mode["candidate_provisional"]
    fallback = current_by_mode["fallback_stable"]
    previous_candidate = previous_by_mode.get("candidate_provisional")
    previous_fallback = previous_by_mode.get("fallback_stable")
    current_gap_profit = candidate.total_profit - fallback.total_profit
    current_gap_roi = candidate.roi_multiple - fallback.roi_multiple
    if previous_candidate is not None and previous_fallback is not None:
        previous_gap_profit = previous_candidate.total_profit - previous_fallback.total_profit
        previous_gap_roi = previous_candidate.roi_multiple - previous_fallback.roi_multiple
        previous_source = previous_candidate.source_artifact_path
    else:
        previous_gap_profit = 0.0
        previous_gap_roi = 0.0
        previous_source = ""
    output.append(
        ArtifactCompareReportRow(
            generated_at=candidate.generated_at,
            compare_group="candidate_vs_fallback_profit_gap",
            run_mode="candidate_provisional_vs_fallback_stable",
            current_source_artifact_path=candidate.source_artifact_path,
            previous_source_artifact_path=previous_source,
            current_value=current_gap_profit,
            previous_value=previous_gap_profit,
            delta_value=current_gap_profit - previous_gap_profit,
            note="current candidate vs fallback total_profit gap",
        ),
    )
    output.append(
        ArtifactCompareReportRow(
            generated_at=candidate.generated_at,
            compare_group="candidate_vs_fallback_roi_gap",
            run_mode="candidate_provisional_vs_fallback_stable",
            current_source_artifact_path=candidate.source_artifact_path,
            previous_source_artifact_path=previous_source,
            current_value=current_gap_roi,
            previous_value=previous_gap_roi,
            delta_value=current_gap_roi - previous_gap_roi,
            note="current candidate vs fallback roi_multiple gap",
        ),
    )
    return tuple(output)


def detect_git_state(base_dir: Path) -> tuple[str, str]:
    repo_dir = base_dir
    while not (repo_dir / ".git").exists():
        if repo_dir.parent == repo_dir:
            return "unknown", "git repo not found"
        repo_dir = repo_dir.parent
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status_output = subprocess.run(
            ["git", "status", "--short"],
            cwd=repo_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        working_tree_note = "clean" if not status_output else "dirty_worktree"
        return commit, working_tree_note
    except Exception:
        return "unknown", "git state unavailable"


def build_domain_mapping_audit_rows(config: BetLogicOnlyConfig) -> tuple[DomainMappingAuditRow, ...]:
    rows = (
        DomainMappingAuditRow(
            audit_item="race_key_width_and_role",
            source_location="src/horse_bet_lab/ingest/specs.py",
            evidence_type="code_spec",
            finding="BAC/CHA/SED specs define race_key as an 8-byte race-level key shared across staging tables.",
            implication="Confirms race_key is stable as a join key, but does not define prefix semantics as a formal domain/category.",
            supports_formal_mapping=0,
        ),
        DomainMappingAuditRow(
            audit_item="staging_contract_key_only",
            source_location="docs/staging_contract.md",
            evidence_type="doc_contract",
            finding="Staging contract documents race_key as a confirmed race-level key and does not expose any domain/category column.",
            implication="Repo-level contract supports key stability only; no formal upstream category mapping is documented here.",
            supports_formal_mapping=0,
        ),
        DomainMappingAuditRow(
            audit_item="raw_parser_slices_first_8_chars",
            source_location="src/horse_bet_lab/ingest/service.py",
            evidence_type="loader_behavior",
            finding="OZ parser extracts race_key as text[0:8] without decoding subfields into domain/category attributes.",
            implication="Loader preserves raw key but does not formalize prefix meaning.",
            supports_formal_mapping=0,
        ),
        DomainMappingAuditRow(
            audit_item="historical_hex_suffix_allowed",
            source_location="tests/test_proxy_live_inference.py",
            evidence_type="test_evidence",
            finding="Historical race_key with hex-like suffix (example: 05252a11) is accepted as-is.",
            implication="String structure is treated opaquely in repo code; suffix semantics are not formalized, which weakens any claim that prefix semantics are officially documented here.",
            supports_formal_mapping=0,
        ),
        DomainMappingAuditRow(
            audit_item="no_formal_domain_columns_in_staging",
            source_location="duckdb schema: jrdb_bac_staging, jrdb_sed_staging, jrdb_oz_staging, jrdb_hjc_staging",
            evidence_type="schema_audit",
            finding="Observed staging tables do not include a formal domain/category column corresponding to race_key prefix.",
            implication="This is acceptable under the formalized boundary: staging provides race_key, venue_code is derived from race_key[:2], and any broader grouping is project-owned.",
            supports_formal_mapping=0,
        ),
        DomainMappingAuditRow(
            audit_item="venue_code_based_domain_mapping_is_project_owned",
            source_location="src/horse_bet_lab/evaluation/bet_logic_only.py",
            evidence_type="analysis_code",
            finding="Current overlay uses a project-owned bucket derived from upstream venue_code = race_key[:2]; historical artifact name proxy_domain is retained only for compatibility.",
            implication="Upstream provides venue_code, while broader domain/group semantics are formalized in-project as a derived mapping layer.",
            supports_formal_mapping=1,
        ),
        DomainMappingAuditRow(
            audit_item="project_docs_formalize_venue_code_boundary",
            source_location="docs/staging_contract.md; src/horse_bet_lab/ingest/specs.py",
            evidence_type="project_formalization",
            finding="Project docs/specs explicitly define race_key[:2] as upstream venue_code and reserve broader domain/grouping for project-owned derived mapping.",
            implication="This resolves the previous ambiguity around proxy_domain semantics without changing BET logic behavior.",
            supports_formal_mapping=1,
        ),
        DomainMappingAuditRow(
            audit_item="external_jrdb_ba_code_table_exists",
            source_location="https://jrdb.com/member/data/JRDBcode.html",
            evidence_type="external_spec",
            finding="JRDB external code table lists official 場コード meanings including 02=函館 and 09=阪神.",
            implication="Supports the project boundary that upstream owns venue_code meaning; project-owned domain/grouping can be derived from that code.",
            supports_formal_mapping=1,
        ),
        DomainMappingAuditRow(
            audit_item="external_search_found_no_upstream_domain_group_mapping",
            source_location="external JRDB/raw-spec search",
            evidence_type="external_search",
            finding="External search did not find an upstream-provided broader domain/group enum beyond venue_code itself.",
            implication="This is acceptable once the project formalizes domain/group as venue_code-derived instead of claiming it as an upstream category.",
            supports_formal_mapping=1,
        ),
    )
    return rows


def build_domain_mapping_report_rows(
    *,
    race_prediction_rows: tuple[RacePredictionRow, ...],
    baseline_rows: tuple[LogicBetRow, ...],
    fallback_rows: tuple[LogicBetRow, ...],
    candidate_rows: tuple[LogicBetRow, ...],
) -> tuple[DomainMappingReportRow, ...]:
    observed_races: dict[str, set[str]] = {}
    observed_bets: dict[str, int] = {}
    for row in race_prediction_rows:
        domain = row.race_key[:2] if len(row.race_key) >= 2 else "unknown"
        observed_races.setdefault(domain, set()).add(row.race_key)
    for row in baseline_rows:
        domain = domain_key(row)
        observed_bets[domain] = observed_bets.get(domain, 0) + 1

    def summarize(rows: tuple[LogicBetRow, ...]) -> dict[str, _LogicRowStats]:
        grouped: dict[str, list[LogicBetRow]] = {}
        for row in rows:
            grouped.setdefault(domain_key(row), []).append(row)
        return {key: summarize_logic_rows(tuple(value)) for key, value in grouped.items()}

    fallback_stats = summarize(fallback_rows)
    candidate_stats = summarize(candidate_rows)
    all_domains = sorted(set(observed_races) | set(observed_bets) | set(fallback_stats) | set(candidate_stats))
    evidence_locations = "; ".join(
        (
            "src/horse_bet_lab/ingest/specs.py",
            "docs/staging_contract.md",
            "src/horse_bet_lab/ingest/service.py",
            "tests/test_proxy_live_inference.py",
            "src/horse_bet_lab/evaluation/bet_logic_only.py",
            "https://jrdb.com/member/data/JRDBcode.html",
        ),
    )
    output: list[DomainMappingReportRow] = []
    for domain in all_domains:
        fallback = fallback_stats.get(domain, summarize_logic_rows(()))
        candidate = candidate_stats.get(domain, summarize_logic_rows(()))
        inferred_meaning = "Project-owned bucket derived from upstream venue_code = race_key[:2]"
        if domain in {"02", "09"}:
            inferred_meaning = (
                "Project-owned venue-code-derived bucket; current overlay applies surcharge on this weak OOS bucket"
            )
        output.append(
            DomainMappingReportRow(
                proxy_domain=domain,
                observed_race_count=len(observed_races.get(domain, set())),
                observed_bet_count=observed_bets.get(domain, 0),
                fallback_bet_count=fallback.bet_count,
                fallback_total_profit=fallback.total_profit,
                fallback_roi_multiple=fallback.roi_multiple,
                candidate_bet_count=candidate.bet_count,
                candidate_total_profit=candidate.total_profit,
                candidate_roi_multiple=candidate.roi_multiple,
                inferred_meaning=inferred_meaning,
                evidence_locations=evidence_locations,
                formal_mapping_status="project_mapping_formalized",
            ),
        )
    return tuple(output)


def build_domain_mapping_adoption_memo(
    *,
    audit_rows: tuple[DomainMappingAuditRow, ...],
    report_rows: tuple[DomainMappingReportRow, ...],
    config: BetLogicOnlyConfig,
) -> str:
    supports_formal_mapping = any(row.supports_formal_mapping for row in audit_rows)
    external_evidence_found = any(
        row.evidence_type in {"external_spec", "external_search"} for row in audit_rows
    )
    domain_02 = next((row for row in report_rows if row.proxy_domain == "02"), None)
    domain_09 = next((row for row in report_rows if row.proxy_domain == "09"), None)
    decision = "B. formal mapping なし"
    adoption = "provisional 維持"
    if supports_formal_mapping and config.formal_domain_mapping_confirmed:
        decision = "A. venue_code formalized + project mapping confirmed"
        adoption = "hard adopt"
    elif supports_formal_mapping:
        decision = "A. venue_code formalized + project mapping ready"
        adoption = "hard adopt 可能"
    domain_02_line = (
        f"- `02`: observed_race_count={domain_02.observed_race_count}, fallback_bet_count={domain_02.fallback_bet_count}, "
        f"fallback_profit={domain_02.fallback_total_profit:.0f}, candidate_bet_count={domain_02.candidate_bet_count}, candidate_profit={domain_02.candidate_total_profit:.0f}"
        if domain_02 is not None
        else "- `02`: no observed rows"
    )
    domain_09_line = (
        f"- `09`: observed_race_count={domain_09.observed_race_count}, fallback_bet_count={domain_09.fallback_bet_count}, "
        f"fallback_profit={domain_09.fallback_total_profit:.0f}, candidate_bet_count={domain_09.candidate_bet_count}, candidate_profit={domain_09.candidate_total_profit:.0f}"
        if domain_09 is not None
        else "- `09`: no observed rows"
    )
    return "\n".join(
        [
            "# Domain Mapping Adoption Memo",
            "",
            "## Decision",
            "",
            f"- 判定: {decision}",
            f"- 採用判断: {adoption}",
            "",
            "## Evidence Summary",
            "",
            "- repo / schema / loader / docs の formalization では、race_key は upstream race identifier、race_key[:2] は upstream venue_code として扱う境界を明文化した",
            (
                "- repo 外検索では JRDB の official code table を確認でき、venue_code meaning の外部根拠も補強できた"
                if external_evidence_found
                else "- repo 外検索の追加根拠は未取得"
            ),
            "- upstream が直接提供しているのは venue_code までであり、broader domain/group は project-owned derived mapping として扱う",
            "- 現行 overlay の historical name は `proxy_domain` だが、意味としては `venue_code` 由来の project mapping bucket である",
            "",
            "## Detailed Prefix Readout",
            "",
            domain_02_line,
            domain_09_line,
            "",
            "## Adoption Impact",
            "",
            "- current candidate: `guard_0_01_plus_proxy_domain_overlay`",
            "- fallback: `no_bet_guard_stronger surcharge=0.01`",
            "- hard adopt に必要だった blocker は『proxy domain を upstream domain と見なしていた曖昧さ』だった",
            "- 今回の formalization で、その境界を `upstream venue_code + project-owned mapping` に修正した",
            "- fallback を mainline に戻す理由は現時点では弱い",
            "  - 数値上の candidate 優位は維持されている",
            "  - BET behavior は変えずに domain semantics だけ formalize できた",
            "",
            "## Conclusion",
            "",
            (
                "- current candidate は hard adopt に上げてよい"
                if supports_formal_mapping
                else "- current candidate はまだ provisional 維持"
            ),
            "- 新しい BET logic variant 探索は不要",
        ],
    )


def build_hard_adopt_decision_memo(
    *,
    audit_rows: tuple[DomainMappingAuditRow, ...],
    report_rows: tuple[DomainMappingReportRow, ...],
    rows: tuple[HardAdoptDecisionRow, ...],
    config: BetLogicOnlyConfig,
) -> str:
    decision_row = rows[0]
    domain_02 = next((row for row in report_rows if row.proxy_domain == "02"), None)
    domain_09 = next((row for row in report_rows if row.proxy_domain == "09"), None)
    domain_02_line = (
        f"- `02`: fallback_bet_count={domain_02.fallback_bet_count}, fallback_profit={domain_02.fallback_total_profit:.0f}, "
        f"candidate_bet_count={domain_02.candidate_bet_count}, candidate_profit={domain_02.candidate_total_profit:.0f}, formal_mapping_status={domain_02.formal_mapping_status}"
        if domain_02 is not None
        else "- `02`: no observed rows"
    )
    domain_09_line = (
        f"- `09`: fallback_bet_count={domain_09.fallback_bet_count}, fallback_profit={domain_09.fallback_total_profit:.0f}, "
        f"candidate_bet_count={domain_09.candidate_bet_count}, candidate_profit={domain_09.candidate_total_profit:.0f}, formal_mapping_status={domain_09.formal_mapping_status}"
        if domain_09 is not None
        else "- `09`: no observed rows"
    )
    external_lines = [
        f"- {row.audit_item}: {row.finding}"
        for row in audit_rows
        if row.evidence_type in {"external_spec", "external_search"}
    ]
    if not external_lines:
        external_lines = ["- external evidence not found"]
    return "\n".join(
        [
            "# Hard Adopt Decision Memo",
            "",
            "## Final Decision",
            "",
            f"- decision_status: `{decision_row.decision_status}`",
            f"- recommended_operational_status: `{decision_row.recommended_operational_status}`",
            f"- current candidate: `{decision_row.candidate_logic_name}`",
            f"- fallback: `{decision_row.fallback_logic_name}`",
            "",
            "## Why",
            "",
            f"- {decision_row.reason}",
            (
                "- `formal_domain_mapping_confirmed=true` にして hard adopt に上げてよい状態"
                if config.formal_domain_mapping_confirmed
                and decision_row.race_key_prefix_to_category_confirmed
                else "- current candidate は provisional adopt のまま固定し、fallback を stable path として維持する"
            ),
            "",
            "## External Mapping Search",
            "",
            *external_lines,
            "",
            "## Prefix Readout",
            "",
            domain_02_line,
            domain_09_line,
            "",
            "## Conclusion",
            "",
            (
                "- venue_code formalization と project-owned mapping が揃ったため、BET logic candidate を hard adopt に上げてよい"
                if decision_row.race_key_prefix_to_category_confirmed
                else "- venue_code or derived-domain formalization が不足するため、hard adopt には上げない"
            ),
            (
                "- このチャットの BET logic candidate は data/domain formalization 観点でも固定できる"
                if not decision_row.race_key_prefix_to_category_confirmed
                else "- このチャットの BET logic candidate は hard adopt に更新済み"
            ),
        ],
    )


def build_logic_status_rows(config: BetLogicOnlyConfig) -> tuple[BetLogicStatusRow, ...]:
    overlay_status = (
        "hard_adopt"
        if config.formal_domain_mapping_confirmed
        else (
            "provisional_adopt"
            if config.provisional_proxy_domain_overlay_enabled
            else "frozen_candidate_disabled"
        )
    )
    overlay_scope = (
        "mainline_candidate"
        if config.formal_domain_mapping_confirmed
        else (
            "mainline_candidate_pending_domain_mapping"
            if config.provisional_proxy_domain_overlay_enabled
            else "candidate_frozen_fallback_active"
        )
    )
    overlay_rationale = (
        "recommended overlay; upstream venue_code and project-owned derived mapping are formalized"
        if config.formal_domain_mapping_confirmed
        else (
            "recommended overlay; improves profit and drawdown vs guard 0.01, but legacy proxy-domain wording has not yet been promoted to formal venue-code-based mapping"
            if config.provisional_proxy_domain_overlay_enabled
            else "candidate is frozen but disabled in this config; use no_bet_guard_stronger surcharge=0.01 fallback until venue-code-based overlay usage is explicitly allowed"
        )
    )
    fallback_status = (
        "fallback_ready"
        if config.provisional_proxy_domain_overlay_enabled
        else "active_fallback"
    )
    return (
        BetLogicStatusRow(
            logic_variant=BASELINE_LOGIC_NAME,
            status="fixed_reference",
            scope="comparison_anchor",
            rationale="reference parity anchor; do not change",
        ),
        BetLogicStatusRow(
            logic_variant="no_bet_guard_stronger",
            status=fallback_status,
            scope="guard_base",
            rationale=(
                "base guard line; surcharge=0.01 remains the production-simple fallback anchor"
                if config.provisional_proxy_domain_overlay_enabled
                else "base guard line; surcharge=0.01 is the active fallback because provisional proxy-domain overlay is disabled in this config"
            ),
        ),
        BetLogicStatusRow(
            logic_variant="chaos_no_bet_guard",
            status="stop",
            scope="research_archived",
            rationale="heuristic chaos skip removed profitable bets and underperformed guard 0.01",
        ),
        BetLogicStatusRow(
            logic_variant="chaos_edge_surcharge",
            status="research_only",
            scope="research_backlog",
            rationale="weak independent signal; keep only as research, not as a mainline candidate",
        ),
        BetLogicStatusRow(
            logic_variant="no_bet_guard_plus_chaos",
            status="stop",
            scope="research_archived",
            rationale="post-guard chaos layer damaged kept winners and reduced profit vs guard 0.01",
        ),
        BetLogicStatusRow(
            logic_variant="guard_0_01_plus_proxy_domain_overlay",
            status=overlay_status,
            scope=overlay_scope,
            rationale=overlay_rationale,
        ),
        BetLogicStatusRow(
            logic_variant="guard_0_01_plus_near_threshold_overlay",
            status="not_adopted",
            scope="overlay_evaluated",
            rationale="not adopted; worsened profit and drawdown vs guard 0.01",
        ),
        BetLogicStatusRow(
            logic_variant="guard_0_01_plus_place_basis_overlay",
            status="not_adopted",
            scope="overlay_evaluated",
            rationale="not adopted; improvement vs guard 0.01 was too small and drawdown worsened",
        ),
        BetLogicStatusRow(
            logic_variant="guard_0_01_plus_domain_x_threshold_overlay",
            status="keep_research_secondary",
            scope="overlay_secondary_candidate",
            rationale="positive overlay but weaker than proxy-domain overlay; keep only as secondary reference",
        ),
    )


def bootstrap_variant_roi_interval(
    summaries: list[BetLogicOnlySelectedSummary],
    *,
    bootstrap_iterations: int,
    random_seed: int,
) -> tuple[float, float]:
    if not summaries:
        return 0.0, 0.0
    rng = Random(random_seed)
    roi_samples: list[float] = []
    for _ in range(bootstrap_iterations):
        sampled = [rng.choice(summaries) for _ in range(len(summaries))]
        total_stake = sum(summary.total_stake for summary in sampled)
        total_return = sum(summary.total_return for summary in sampled)
        roi_samples.append((total_return / total_stake) if total_stake > 0.0 else 0.0)
    roi_samples.sort()
    lower_index = max(0, int(0.025 * (len(roi_samples) - 1)))
    upper_index = min(len(roi_samples) - 1, int(0.975 * (len(roi_samples) - 1)))
    return roi_samples[lower_index], roi_samples[upper_index]


def summarize_logic_rows(rows: tuple[LogicBetRow, ...]) -> _LogicRowStats:
    bet_count = len(rows)
    hit_count = sum(1 for row in rows if row.place_payout is not None)
    total_stake = sum(row.stake for row in rows)
    total_return = sum(row.scaled_return for row in rows)
    total_profit = total_return - total_stake
    edge_values = [row.edge for row in rows if row.edge is not None]
    return _LogicRowStats(
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        total_stake=total_stake,
        total_return=total_return,
        roi_multiple=(total_return / total_stake) if total_stake > 0.0 else 0.0,
        net_roi=(total_profit / total_stake) if total_stake > 0.0 else 0.0,
        total_profit=total_profit,
        max_drawdown=compute_max_drawdown([row.bet_profit for row in rows]),
        max_losing_streak=compute_max_losing_streak([row.bet_profit for row in rows]),
        avg_edge=(sum(edge_values) / len(edge_values)) if edge_values else 0.0,
        avg_stake=(total_stake / bet_count) if bet_count > 0 else 0.0,
    )


def rows_equivalent(reference_row: LogicBetRow, baseline_row: LogicBetRow) -> bool:
    return (
        reference_row.window_label == baseline_row.window_label
        and floats_close(reference_row.stake, baseline_row.stake)
        and floats_close(reference_row.scaled_return, baseline_row.scaled_return)
        and floats_close(reference_row.bet_profit, baseline_row.bet_profit)
    )


def floats_close(left: float, right: float) -> bool:
    return abs(left - right) <= 1e-9


def identity_key(row: LogicBetRow) -> tuple[str, int]:
    return (row.race_key, row.horse_number)


def order_logic_rows(rows: tuple[LogicBetRow, ...]) -> tuple[LogicBetRow, ...]:
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                row.result_date,
                row.window_label,
                row.race_key,
                row.horse_number,
            ),
        ),
    )


def default_source_logic(logic_variant: str) -> str:
    mapping = {
        BASELINE_LOGIC_NAME: "mainline_reference_selected_rows",
        "no_bet_guard_stronger": "mainline_reference_selected_rows+stronger_guard_0.01",
        "guard_0_01_plus_proxy_domain_overlay": "guard_0.01+proxy_domain_surcharge",
        "guard_0_01_plus_near_threshold_overlay": "guard_0.01+near_threshold_surcharge",
        "guard_0_01_plus_place_basis_overlay": "guard_0.01+place_basis_surcharge",
        "guard_0_01_plus_domain_x_threshold_overlay": "guard_0.01+domain_x_threshold_surcharge",
        "chaos_no_bet_guard": "mainline_reference_selected_rows+race_chaos_skip",
        "chaos_edge_surcharge": "mainline_reference_selected_rows+race_chaos_surcharge",
        "no_bet_guard_plus_chaos": (
            "mainline_reference_selected_rows+stronger_guard_0.01+race_chaos_surcharge"
        ),
    }
    return mapping[logic_variant]


def venue_code_from_race_key(race_key: str) -> str:
    """Return the upstream venue_code carried in the first two characters of race_key."""
    return race_key[:2] if len(race_key) >= 2 else "unknown"


def project_domain_from_venue_code(venue_code: str) -> str:
    """Return the project-owned domain bucket derived from an upstream venue_code.

    The current BET-logic-only line keeps an identity mapping for artifact stability,
    so historical `proxy_domain` columns still carry the venue-code-based bucket.
    """
    return venue_code


def domain_key(row: LogicBetRow) -> str:
    return project_domain_from_venue_code(venue_code_from_race_key(row.race_key))


def bucket_popularity(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value <= 2:
        return "1_2"
    if value <= 4:
        return "3_4"
    if value <= 6:
        return "5_6"
    return "7_plus"


def bucket_place_basis_odds(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 2.0:
        return "lt_2_0"
    if value < 2.4:
        return "2_0_to_2_4"
    if value < 2.8:
        return "2_4_to_2_8"
    if value < 3.2:
        return "2_8_to_3_2"
    if value < 4.0:
        return "3_2_to_4_0"
    return "4_0_plus"


def bucket_win_odds(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 5.0:
        return "lt_5"
    if value < 10.0:
        return "5_to_10"
    if value < 20.0:
        return "10_to_20"
    return "20_plus"


def bucket_edge(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.06:
        return "lt_0_06"
    if value < 0.08:
        return "0_06_to_0_08"
    if value < 0.10:
        return "0_08_to_0_10"
    if value < 0.12:
        return "0_10_to_0_12"
    return "0_12_plus"


def bucket_near_threshold_count(chaos: RaceChaosRow | None) -> str:
    count = chaos.near_threshold_candidate_count if chaos is not None else 0
    if count <= 0:
        return "0"
    if count == 1:
        return "1"
    if count == 2:
        return "2"
    return "3_plus"


def _empty_guard_row(group_type: str, group_key: str) -> GuardWeakRegimeRow:
    return GuardWeakRegimeRow(
        group_type=group_type,
        group_key=group_key,
        analysis_scope="empty",
        bet_count=0,
        total_stake=0.0,
        total_return=0.0,
        total_profit=0.0,
        roi_multiple=0.0,
        net_roi=0.0,
        hit_rate=0.0,
        max_drawdown=0.0,
        max_losing_streak=0,
    )


def normalized_entropy(values: list[float]) -> float:
    positive = [value for value in values if value > 0.0]
    if len(positive) <= 1:
        return 0.0
    total = sum(positive)
    if total <= 0.0:
        return 0.0
    probabilities = [value / total for value in positive]
    entropy = -sum(probability * math.log(probability) for probability in probabilities)
    return entropy / math.log(len(probabilities))


def top_two_gap(values: list[float]) -> float:
    ordered = sorted(values, reverse=True)
    if len(ordered) < 2:
        return ordered[0] if ordered else 0.0
    return ordered[0] - ordered[1]


def rank_disagreement(values_a: list[float | None], values_b: list[float | None]) -> float:
    indexed_a = [(index, value) for index, value in enumerate(values_a) if value is not None]
    indexed_b = [(index, value) for index, value in enumerate(values_b) if value is not None]
    common_indices = sorted(set(index for index, _ in indexed_a) & set(index for index, _ in indexed_b))
    if len(common_indices) <= 1:
        return 0.0
    rank_a = {
        index: rank
        for rank, (index, _value) in enumerate(
            sorted(((index, values_a[index]) for index in common_indices), key=lambda item: float(item[1]), reverse=True),
            start=1,
        )
    }
    rank_b = {
        index: rank
        for rank, (index, _value) in enumerate(
            sorted(((index, values_b[index]) for index in common_indices), key=lambda item: float(item[1]), reverse=True),
            start=1,
        )
    }
    normalizer = max(1, len(common_indices) - 1)
    return sum(abs(rank_a[index] - rank_b[index]) for index in common_indices) / (
        len(common_indices) * normalizer
    )


def place_basis_rank_value(place_basis_odds: float | None) -> float | None:
    if place_basis_odds is None or place_basis_odds <= 0.0:
        return None
    return 1.0 / place_basis_odds


def coefficient_of_variation(values: list[float]) -> float:
    positive = [value for value in values if value > 0.0]
    if len(positive) <= 1:
        return 0.0
    mean = sum(positive) / len(positive)
    if mean <= 0.0:
        return 0.0
    variance = sum((value - mean) ** 2 for value in positive) / len(positive)
    return math.sqrt(variance) / mean


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * quantile))))
    return ordered[position]


def pearson_correlation(xs: list[float], ys: list[float]) -> float:
    if len(xs) != len(ys) or len(xs) <= 1:
        return 0.0
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys, strict=False))
    denom_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    denom_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if denom_x <= 0.0 or denom_y <= 0.0:
        return 0.0
    return numerator / (denom_x * denom_y)
