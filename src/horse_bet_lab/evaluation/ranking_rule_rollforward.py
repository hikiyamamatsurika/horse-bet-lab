from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from horse_bet_lab.config import RankingRuleComparisonConfig


@dataclass(frozen=True)
class CandidateBetRow:
    race_key: str
    horse_number: int
    result_date: date
    target_value: int
    pred_probability: float
    market_prob: float | None
    edge: float | None
    win_odds: float | None
    popularity: int | None
    place_basis_odds: float | None
    place_payout: float | None


@dataclass(frozen=True)
class RankingRuleSummary:
    window_label: str
    split: str
    ranking_score_rule: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RankingRuleSelectionSummary:
    window_label: str
    ranking_score_rule: str
    aggregate_selection_score_rule: str
    min_bets_valid: int | None
    selected_on_split: str
    applied_to_split: str
    valid_window_labels: str
    test_window_label: str
    valid_aggregate_score: float
    valid_positive_window_count: int
    valid_mean_roi: float
    valid_min_roi: float
    valid_roi_std: float
    valid_window_rois: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RankingRuleRollupSummary:
    ranking_score_rule: str
    aggregate_selection_score_rule: str
    min_bets_valid: int | None
    test_window_count: int
    test_window_labels: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RankingStrategyVariantSummary:
    window_label: str
    aggregate_selection_score_rule: str
    min_bets_valid: int | None
    strategy_variant: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RankingStrategyVariantRollupSummary:
    strategy_variant: str
    aggregate_selection_score_rule: str
    min_bets_valid: int | None
    test_window_count: int
    test_window_labels: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RankingStrategyVariantDiffSummary:
    set_group: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float | None


@dataclass(frozen=True)
class AggregateValidSummary:
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float
    mean_valid_roi: float
    positive_window_count: int
    min_valid_roi: float
    valid_roi_std: float
    valid_window_rois: tuple[float, ...]


@dataclass(frozen=True)
class RankingRuleDiffSummary:
    ranking_score_rule: str
    set_group: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float | None


@dataclass(frozen=True)
class RankingRuleComparisonResult:
    output_dir: Path
    summaries: tuple[RankingRuleSummary, ...]
    selected_summaries: tuple[RankingRuleSelectionSummary, ...]
    rollups: tuple[RankingRuleRollupSummary, ...]
    diff_summaries: tuple[RankingRuleDiffSummary, ...]
    strategy_variant_summaries: tuple[RankingStrategyVariantSummary, ...]
    strategy_variant_rollups: tuple[RankingStrategyVariantRollupSummary, ...]
    strategy_variant_diff_summaries: tuple[RankingStrategyVariantDiffSummary, ...]
    selected_rows_by_candidate: dict[tuple[object, ...], tuple[CandidateBetRow, ...]]


def run_ranking_rule_comparison(config: RankingRuleComparisonConfig) -> RankingRuleComparisonResult:
    validate_config(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(config.duckdb_path), read_only=True)
    try:
        prediction_table = "prediction_rows_ranking_rule_compare"
        connection.execute(f"DROP TABLE IF EXISTS {prediction_table}")
        connection.execute(
            f"""
            CREATE TEMP TABLE {prediction_table} AS
            SELECT * FROM read_csv_auto(?)
            """,
            [str(config.rolling_predictions_path)],
        )
        summaries, selected_rows_by_candidate = build_summaries(
            connection,
            prediction_table,
            config,
        )
    finally:
        connection.close()

    selected_summaries = tuple(build_selection_summaries(config, summaries))
    rollups = tuple(build_rollups(selected_summaries, config.stake_per_bet))
    diff_summaries = tuple(
        build_diff_summaries(
            config,
            selected_summaries,
            selected_rows_by_candidate,
        ),
    )
    strategy_variant_summaries = tuple(
        build_strategy_variant_summaries(
            config,
            selected_summaries,
            selected_rows_by_candidate,
        ),
    )
    strategy_variant_rollups = tuple(
        build_strategy_variant_rollups(
            strategy_variant_summaries,
            config.stake_per_bet,
        ),
    )
    strategy_variant_diff_summaries = tuple(
        build_strategy_variant_diff_summaries(
            config,
            selected_summaries,
            selected_rows_by_candidate,
        ),
    )
    result = RankingRuleComparisonResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        selected_summaries=selected_summaries,
        rollups=rollups,
        diff_summaries=diff_summaries,
        strategy_variant_summaries=strategy_variant_summaries,
        strategy_variant_rollups=strategy_variant_rollups,
        strategy_variant_diff_summaries=strategy_variant_diff_summaries,
        selected_rows_by_candidate=selected_rows_by_candidate,
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "selected_summary.csv", result.selected_summaries)
    write_json(
        config.output_dir / "selected_summary.json",
        {"analysis": {"rows": result.selected_summaries}},
    )
    write_csv(config.output_dir / "selected_test_rollup.csv", result.rollups)
    write_json(
        config.output_dir / "selected_test_rollup.json",
        {"analysis": {"rows": result.rollups}},
    )
    write_csv(config.output_dir / "diff_summary.csv", result.diff_summaries)
    write_json(
        config.output_dir / "diff_summary.json",
        {"analysis": {"rows": result.diff_summaries}},
    )
    write_csv(
        config.output_dir / "strategy_variant_summary.csv",
        result.strategy_variant_summaries,
    )
    write_json(
        config.output_dir / "strategy_variant_summary.json",
        {"analysis": {"rows": result.strategy_variant_summaries}},
    )
    write_csv(
        config.output_dir / "strategy_variant_rollup.csv",
        result.strategy_variant_rollups,
    )
    write_json(
        config.output_dir / "strategy_variant_rollup.json",
        {"analysis": {"rows": result.strategy_variant_rollups}},
    )
    write_csv(
        config.output_dir / "strategy_variant_diff_summary.csv",
        result.strategy_variant_diff_summaries,
    )
    write_json(
        config.output_dir / "strategy_variant_diff_summary.json",
        {"analysis": {"rows": result.strategy_variant_diff_summaries}},
    )
    return result


def validate_config(config: RankingRuleComparisonConfig) -> None:
    valid_scores = {"edge", "pred_times_place_basis_odds"}
    if config.selection_metric not in {"edge", "probability"}:
        raise ValueError("selection_metric must be 'edge' or 'probability'")
    if config.market_prob_method != "oz_place_basis_inverse":
        raise ValueError("ranking rule comparison currently supports oz_place_basis_inverse only")
    if not config.thresholds:
        raise ValueError("thresholds must not be empty")
    if not config.popularity_bands:
        raise ValueError("popularity_bands must not be empty")
    if not config.place_basis_bands:
        raise ValueError("place_basis_bands must not be empty")
    if not config.evaluation_window_pairs:
        raise ValueError("evaluation_window_pairs must not be empty")
    if not config.selection_window_groups:
        raise ValueError("selection_window_groups must not be empty")
    if not config.ranking_score_rules:
        raise ValueError("ranking_score_rules must not be empty")
    for ranking_score_rule in config.ranking_score_rules:
        if ranking_score_rule not in valid_scores:
            raise ValueError(f"unsupported ranking_score_rule: {ranking_score_rule}")


def build_summaries(
    connection: duckdb.DuckDBPyConnection,
    prediction_table: str,
    config: RankingRuleComparisonConfig,
) -> tuple[list[RankingRuleSummary], dict[tuple[object, ...], tuple[CandidateBetRow, ...]]]:
    summaries: list[RankingRuleSummary] = []
    selected_rows_by_candidate: dict[tuple[object, ...], tuple[CandidateBetRow, ...]] = {}
    for (
        pair_label,
        valid_start_date,
        valid_end_date,
        test_start_date,
        test_end_date,
    ) in config.evaluation_window_pairs:
        for split_name, start_date, end_date in (
            ("valid", valid_start_date, valid_end_date),
            ("test", test_start_date, test_end_date),
        ):
            for min_popularity, max_popularity in config.popularity_bands:
                for min_place_basis_odds, max_place_basis_odds in config.place_basis_bands:
                    candidate_rows = load_candidate_rows(
                        connection=connection,
                        prediction_table=prediction_table,
                        config=config,
                        pair_label=pair_label,
                        split_name=split_name,
                        start_date=start_date,
                        end_date=end_date,
                        min_popularity=min_popularity,
                        max_popularity=max_popularity,
                        min_place_basis_odds=min_place_basis_odds,
                        max_place_basis_odds=max_place_basis_odds,
                    )
                    for threshold in config.thresholds:
                        target_bet_count = count_threshold_selected_rows(
                            candidate_rows=candidate_rows,
                            selection_metric=config.selection_metric,
                            threshold=threshold,
                        )
                        for ranking_score_rule in config.ranking_score_rules:
                            selected_rows = select_top_ranked_rows(
                                candidate_rows=candidate_rows,
                                ranking_score_rule=ranking_score_rule,
                                target_bet_count=target_bet_count,
                            )
                            key = candidate_key(
                                window_label=pair_label,
                                split=split_name,
                                ranking_score_rule=ranking_score_rule,
                                threshold=threshold,
                                min_win_odds=config.min_win_odds,
                                max_win_odds=config.max_win_odds,
                                min_place_basis_odds=min_place_basis_odds,
                                max_place_basis_odds=max_place_basis_odds,
                                min_popularity=min_popularity,
                                max_popularity=max_popularity,
                            )
                            selected_rows_by_candidate[key] = selected_rows
                            summaries.append(
                                build_summary(
                                    window_label=pair_label,
                                    split=split_name,
                                    ranking_score_rule=ranking_score_rule,
                                    threshold=threshold,
                                    min_win_odds=config.min_win_odds,
                                    max_win_odds=config.max_win_odds,
                                    min_place_basis_odds=min_place_basis_odds,
                                    max_place_basis_odds=max_place_basis_odds,
                                    min_popularity=min_popularity,
                                    max_popularity=max_popularity,
                                    rows=selected_rows,
                                    stake_per_bet=config.stake_per_bet,
                                ),
                            )
    return summaries, selected_rows_by_candidate


def load_candidate_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    prediction_table: str,
    config: RankingRuleComparisonConfig,
    pair_label: str,
    split_name: str,
    start_date: date | None,
    end_date: date | None,
    min_popularity: int | None,
    max_popularity: int | None,
    min_place_basis_odds: float | None,
    max_place_basis_odds: float | None,
) -> tuple[CandidateBetRow, ...]:
    payout_sql = """
        (
            SELECT race_key, place_horse_number_1 AS horse_number, place_payout_1 AS place_payout
            FROM jrdb_hjc_staging
            UNION ALL
            SELECT race_key, place_horse_number_2 AS horse_number, place_payout_2 AS place_payout
            FROM jrdb_hjc_staging
            UNION ALL
            SELECT race_key, place_horse_number_3 AS horse_number, place_payout_3 AS place_payout
            FROM jrdb_hjc_staging
        )
    """
    conditions: list[str] = []
    parameters: list[object] = [pair_label, split_name]
    if config.min_win_odds is not None:
        conditions.append("AND TRY_CAST(s.win_odds AS DOUBLE) >= ?")
        parameters.append(config.min_win_odds)
    if config.max_win_odds is not None:
        conditions.append("AND TRY_CAST(s.win_odds AS DOUBLE) <= ?")
        parameters.append(config.max_win_odds)
    if min_place_basis_odds is not None:
        conditions.append("AND o.place_basis_odds >= ?")
        parameters.append(min_place_basis_odds)
    if max_place_basis_odds is not None:
        conditions.append("AND o.place_basis_odds <= ?")
        parameters.append(max_place_basis_odds)
    if min_popularity is not None:
        conditions.append("AND s.popularity >= ?")
        parameters.append(min_popularity)
    if max_popularity is not None:
        conditions.append("AND s.popularity <= ?")
        parameters.append(max_popularity)
    if start_date is not None:
        conditions.append("AND s.result_date >= ?")
        parameters.append(start_date)
    if end_date is not None:
        conditions.append("AND s.result_date <= ?")
        parameters.append(end_date)
    where_clause = "\n                ".join(conditions)
    rows = connection.execute(
        f"""
        WITH base_rows AS (
            SELECT
                p.race_key,
                p.horse_number,
                s.result_date,
                p.{config.target_column} AS target_value,
                p.{config.probability_column} AS pred_probability,
                TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                s.popularity AS popularity,
                o.place_basis_odds
            FROM {prediction_table} p
            INNER JOIN jrdb_sed_staging s
                ON p.race_key = s.race_key
                AND p.horse_number = s.horse_number
            LEFT JOIN jrdb_oz_staging o
                ON p.race_key = o.race_key
                AND p.horse_number = o.horse_number
            WHERE p.window_label = ?
                AND p.{config.split_column} = ?
                {where_clause}
        ),
        scored AS (
            SELECT
                *,
                CASE
                    WHEN place_basis_odds IS NOT NULL AND place_basis_odds > 0.0
                    THEN 1.0 / place_basis_odds
                    ELSE NULL
                END AS market_prob,
                CASE
                    WHEN place_basis_odds IS NOT NULL AND place_basis_odds > 0.0
                    THEN pred_probability - (1.0 / place_basis_odds)
                    ELSE NULL
                END AS edge
            FROM base_rows
        )
        SELECT
            s.race_key,
            s.horse_number,
            s.result_date,
            s.target_value,
            s.pred_probability,
            s.market_prob,
            s.edge,
            s.win_odds,
            s.popularity,
            s.place_basis_odds,
            p.place_payout
        FROM scored s
        LEFT JOIN {payout_sql} p
            ON s.race_key = p.race_key
            AND s.horse_number = p.horse_number
        ORDER BY s.race_key, s.horse_number
        """,
        parameters,
    ).fetchall()
    return tuple(
        CandidateBetRow(
            race_key=str(row[0]),
            horse_number=int(row[1]),
            result_date=row[2],
            target_value=int(row[3]),
            pred_probability=float(row[4]),
            market_prob=float(row[5]) if row[5] is not None else None,
            edge=float(row[6]) if row[6] is not None else None,
            win_odds=float(row[7]) if row[7] is not None else None,
            popularity=int(row[8]) if row[8] is not None else None,
            place_basis_odds=float(row[9]) if row[9] is not None else None,
            place_payout=float(row[10]) if row[10] is not None else None,
        )
        for row in rows
    )


def count_threshold_selected_rows(
    *,
    candidate_rows: tuple[CandidateBetRow, ...],
    selection_metric: str,
    threshold: float,
) -> int:
    if selection_metric == "probability":
        return sum(1 for row in candidate_rows if row.pred_probability >= threshold)
    return sum(1 for row in candidate_rows if row.edge is not None and row.edge >= threshold)


def select_top_ranked_rows(
    *,
    candidate_rows: tuple[CandidateBetRow, ...],
    ranking_score_rule: str,
    target_bet_count: int,
) -> tuple[CandidateBetRow, ...]:
    if target_bet_count <= 0:
        return ()
    scored_rows = [
        (score_value(row, ranking_score_rule), row)
        for row in candidate_rows
    ]
    filtered = [(value, row) for value, row in scored_rows if value is not None]
    filtered.sort(
        key=lambda item: (
            float(item[0]),
            float(item[1].edge if item[1].edge is not None else -999.0),
            item[1].pred_probability,
            float(item[1].place_basis_odds if item[1].place_basis_odds is not None else -999.0),
            item[1].race_key,
            -item[1].horse_number,
        ),
        reverse=True,
    )
    return tuple(row for _, row in filtered[:target_bet_count])


def score_value(row: CandidateBetRow, ranking_score_rule: str) -> float | None:
    if ranking_score_rule == "edge":
        return row.edge
    if ranking_score_rule == "pred_times_place_basis_odds":
        if row.place_basis_odds is None or row.place_basis_odds <= 0.0:
            return None
        return row.pred_probability * row.place_basis_odds
    raise ValueError(f"unsupported ranking_score_rule: {ranking_score_rule}")


def build_summary(
    *,
    window_label: str,
    split: str,
    ranking_score_rule: str,
    threshold: float,
    min_win_odds: float | None,
    max_win_odds: float | None,
    min_place_basis_odds: float | None,
    max_place_basis_odds: float | None,
    min_popularity: int | None,
    max_popularity: int | None,
    rows: tuple[CandidateBetRow, ...],
    stake_per_bet: float,
) -> RankingRuleSummary:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return RankingRuleSummary(
        window_label=window_label,
        split=split,
        ranking_score_rule=ranking_score_rule,
        threshold=threshold,
        min_win_odds=min_win_odds,
        max_win_odds=max_win_odds,
        min_place_basis_odds=min_place_basis_odds,
        max_place_basis_odds=max_place_basis_odds,
        min_popularity=min_popularity,
        max_popularity=max_popularity,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout or 0.0 for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.edge for row in rows)) or 0.0,
    )


def build_selection_summaries(
    config: RankingRuleComparisonConfig,
    summaries: list[RankingRuleSummary],
) -> list[RankingRuleSelectionSummary]:
    summaries_by_window_split: dict[tuple[str, str, str], list[RankingRuleSummary]] = {}
    for summary in summaries:
        summaries_by_window_split.setdefault(
            (summary.ranking_score_rule, summary.window_label, summary.split),
            [],
        ).append(summary)

    selection_summaries: list[RankingRuleSelectionSummary] = []
    for ranking_score_rule in config.ranking_score_rules:
        for (
            group_label,
            valid_window_labels,
            test_window_label,
        ) in config.selection_window_groups:
            candidate_map: dict[tuple[object, ...], dict[str, RankingRuleSummary]] = {}
            for valid_window_label in valid_window_labels:
                for summary in summaries_by_window_split.get(
                    (ranking_score_rule, valid_window_label, "valid"),
                    [],
                ):
                    candidate_map.setdefault(candidate_identity(summary), {})[
                        valid_window_label
                    ] = summary

            aggregated_candidates: list[tuple[RankingRuleSummary, AggregateValidSummary]] = []
            for window_summary_map in candidate_map.values():
                if any(label not in window_summary_map for label in valid_window_labels):
                    continue
                valid_candidate_summaries = [
                    window_summary_map[label] for label in valid_window_labels
                ]
                if config.min_bets_valid is not None and any(
                    summary.bet_count < config.min_bets_valid
                    for summary in valid_candidate_summaries
                ):
                    continue
                aggregated_candidates.append(
                    (
                        valid_candidate_summaries[0],
                        aggregate_valid_summaries(
                            valid_candidate_summaries,
                            config.stake_per_bet,
                        ),
                    ),
                )
            if not aggregated_candidates:
                continue
            best_valid, best_aggregate = max(
                aggregated_candidates,
                key=lambda candidate: aggregate_selection_sort_key(
                    candidate[1],
                    candidate[0].threshold,
                    config.aggregate_selection_score_rule,
                ),
            )
            valid_labels_text = ",".join(valid_window_labels)
            selection_summaries.append(
                RankingRuleSelectionSummary(
                    window_label=group_label,
                    ranking_score_rule=ranking_score_rule,
                    aggregate_selection_score_rule=config.aggregate_selection_score_rule,
                    min_bets_valid=config.min_bets_valid,
                    selected_on_split="valid_aggregate",
                    applied_to_split="valid_aggregate",
                    valid_window_labels=valid_labels_text,
                    test_window_label=test_window_label,
                    valid_aggregate_score=aggregate_selection_score_value(
                        best_aggregate,
                        config.aggregate_selection_score_rule,
                    ),
                    valid_positive_window_count=best_aggregate.positive_window_count,
                    valid_mean_roi=best_aggregate.mean_valid_roi,
                    valid_min_roi=best_aggregate.min_valid_roi,
                    valid_roi_std=best_aggregate.valid_roi_std,
                    valid_window_rois=",".join(
                        f"{roi:.6f}" for roi in best_aggregate.valid_window_rois
                    ),
                    threshold=best_valid.threshold,
                    min_win_odds=best_valid.min_win_odds,
                    max_win_odds=best_valid.max_win_odds,
                    min_place_basis_odds=best_valid.min_place_basis_odds,
                    max_place_basis_odds=best_valid.max_place_basis_odds,
                    min_popularity=best_valid.min_popularity,
                    max_popularity=best_valid.max_popularity,
                    bet_count=best_aggregate.bet_count,
                    hit_count=best_aggregate.hit_count,
                    hit_rate=best_aggregate.hit_rate,
                    roi=best_aggregate.roi,
                    total_profit=best_aggregate.total_profit,
                    avg_payout=best_aggregate.avg_payout,
                    avg_edge=best_aggregate.avg_edge,
                ),
            )
            test_match = next(
                (
                    summary
                    for summary in summaries_by_window_split.get(
                        (ranking_score_rule, test_window_label, "test"),
                        [],
                    )
                    if candidate_identity(summary) == candidate_identity(best_valid)
                ),
                None,
            )
            if test_match is None:
                continue
            selection_summaries.append(
                RankingRuleSelectionSummary(
                    window_label=group_label,
                    ranking_score_rule=ranking_score_rule,
                    aggregate_selection_score_rule=config.aggregate_selection_score_rule,
                    min_bets_valid=config.min_bets_valid,
                    selected_on_split="valid_aggregate",
                    applied_to_split="test",
                    valid_window_labels=valid_labels_text,
                    test_window_label=test_window_label,
                    valid_aggregate_score=aggregate_selection_score_value(
                        best_aggregate,
                        config.aggregate_selection_score_rule,
                    ),
                    valid_positive_window_count=best_aggregate.positive_window_count,
                    valid_mean_roi=best_aggregate.mean_valid_roi,
                    valid_min_roi=best_aggregate.min_valid_roi,
                    valid_roi_std=best_aggregate.valid_roi_std,
                    valid_window_rois=",".join(
                        f"{roi:.6f}" for roi in best_aggregate.valid_window_rois
                    ),
                    threshold=test_match.threshold,
                    min_win_odds=test_match.min_win_odds,
                    max_win_odds=test_match.max_win_odds,
                    min_place_basis_odds=test_match.min_place_basis_odds,
                    max_place_basis_odds=test_match.max_place_basis_odds,
                    min_popularity=test_match.min_popularity,
                    max_popularity=test_match.max_popularity,
                    bet_count=test_match.bet_count,
                    hit_count=test_match.hit_count,
                    hit_rate=test_match.hit_rate,
                    roi=test_match.roi,
                    total_profit=test_match.total_profit,
                    avg_payout=test_match.avg_payout,
                    avg_edge=test_match.avg_edge,
                ),
            )
    return selection_summaries


def build_rollups(
    summaries: tuple[RankingRuleSelectionSummary, ...],
    stake_per_bet: float,
) -> list[RankingRuleRollupSummary]:
    grouped: dict[str, list[RankingRuleSelectionSummary]] = {}
    for summary in summaries:
        if summary.applied_to_split != "test":
            continue
        grouped.setdefault(summary.ranking_score_rule, []).append(summary)
    rollups: list[RankingRuleRollupSummary] = []
    for ranking_score_rule, group in grouped.items():
        bet_count = sum(summary.bet_count for summary in group)
        hit_count = sum(summary.hit_count for summary in group)
        total_profit = sum(summary.total_profit for summary in group)
        total_return = (bet_count * stake_per_bet) + total_profit
        total_payout = sum(summary.avg_payout * summary.hit_count for summary in group)
        weighted_edge_sum = sum(summary.avg_edge * summary.bet_count for summary in group)
        rollups.append(
            RankingRuleRollupSummary(
                ranking_score_rule=ranking_score_rule,
                aggregate_selection_score_rule=group[0].aggregate_selection_score_rule,
                min_bets_valid=group[0].min_bets_valid,
                test_window_count=len(group),
                test_window_labels=",".join(summary.window_label for summary in group),
                bet_count=bet_count,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
                total_profit=total_profit,
                avg_payout=(total_payout / hit_count) if hit_count > 0 else 0.0,
                avg_edge=(weighted_edge_sum / bet_count) if bet_count > 0 else 0.0,
            ),
        )
    return sorted(rollups, key=lambda summary: summary.ranking_score_rule)


def build_strategy_variant_summaries(
    config: RankingRuleComparisonConfig,
    selected_summaries: tuple[RankingRuleSelectionSummary, ...],
    selected_rows_by_candidate: dict[tuple[object, ...], tuple[CandidateBetRow, ...]],
) -> list[RankingStrategyVariantSummary]:
    grouped_rows = build_selected_test_rows_by_window(
        selected_summaries=selected_summaries,
        selected_rows_by_candidate=selected_rows_by_candidate,
    )
    summaries: list[RankingStrategyVariantSummary] = []
    for window_label in sorted(grouped_rows):
        edge_rows = grouped_rows[window_label].get("edge", ())
        pred_rows = grouped_rows[window_label].get("pred_times_place_basis_odds", ())
        consensus_rows = build_consensus_rows(edge_rows, pred_rows)
        summaries.extend(
            (
                build_strategy_variant_summary(
                    window_label=window_label,
                    aggregate_selection_score_rule=config.aggregate_selection_score_rule,
                    min_bets_valid=config.min_bets_valid,
                    strategy_variant="edge",
                    rows=edge_rows,
                    stake_per_bet=config.stake_per_bet,
                ),
                build_strategy_variant_summary(
                    window_label=window_label,
                    aggregate_selection_score_rule=config.aggregate_selection_score_rule,
                    min_bets_valid=config.min_bets_valid,
                    strategy_variant="pred_times_place_basis_odds",
                    rows=pred_rows,
                    stake_per_bet=config.stake_per_bet,
                ),
                build_strategy_variant_summary(
                    window_label=window_label,
                    aggregate_selection_score_rule=config.aggregate_selection_score_rule,
                    min_bets_valid=config.min_bets_valid,
                    strategy_variant="consensus",
                    rows=consensus_rows,
                    stake_per_bet=config.stake_per_bet,
                ),
            ),
        )
    return summaries


def build_strategy_variant_summary(
    *,
    window_label: str,
    aggregate_selection_score_rule: str,
    min_bets_valid: int | None,
    strategy_variant: str,
    rows: tuple[CandidateBetRow, ...],
    stake_per_bet: float,
) -> RankingStrategyVariantSummary:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return RankingStrategyVariantSummary(
        window_label=window_label,
        aggregate_selection_score_rule=aggregate_selection_score_rule,
        min_bets_valid=min_bets_valid,
        strategy_variant=strategy_variant,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout or 0.0 for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.edge for row in rows)) or 0.0,
    )


def build_strategy_variant_rollups(
    summaries: tuple[RankingStrategyVariantSummary, ...],
    stake_per_bet: float,
) -> list[RankingStrategyVariantRollupSummary]:
    grouped: dict[str, list[RankingStrategyVariantSummary]] = {}
    for summary in summaries:
        grouped.setdefault(summary.strategy_variant, []).append(summary)
    rollups: list[RankingStrategyVariantRollupSummary] = []
    for strategy_variant, group in grouped.items():
        bet_count = sum(summary.bet_count for summary in group)
        hit_count = sum(summary.hit_count for summary in group)
        total_profit = sum(summary.total_profit for summary in group)
        total_return = (bet_count * stake_per_bet) + total_profit
        total_payout = sum(summary.avg_payout * summary.hit_count for summary in group)
        weighted_edge_sum = sum(summary.avg_edge * summary.bet_count for summary in group)
        rollups.append(
            RankingStrategyVariantRollupSummary(
                strategy_variant=strategy_variant,
                aggregate_selection_score_rule=group[0].aggregate_selection_score_rule,
                min_bets_valid=group[0].min_bets_valid,
                test_window_count=len(group),
                test_window_labels=",".join(summary.window_label for summary in group),
                bet_count=bet_count,
                hit_count=hit_count,
                hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
                roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
                total_profit=total_profit,
                avg_payout=(total_payout / hit_count) if hit_count > 0 else 0.0,
                avg_edge=(weighted_edge_sum / bet_count) if bet_count > 0 else 0.0,
            ),
        )
    return sorted(rollups, key=lambda summary: summary.strategy_variant)


def build_strategy_variant_diff_summaries(
    config: RankingRuleComparisonConfig,
    selected_summaries: tuple[RankingRuleSelectionSummary, ...],
    selected_rows_by_candidate: dict[tuple[object, ...], tuple[CandidateBetRow, ...]],
) -> list[RankingStrategyVariantDiffSummary]:
    grouped_rows = build_selected_test_rows_by_window(
        selected_summaries=selected_summaries,
        selected_rows_by_candidate=selected_rows_by_candidate,
    )
    edge_rows: list[CandidateBetRow] = []
    pred_rows: list[CandidateBetRow] = []
    for window_label in sorted(grouped_rows):
        edge_rows.extend(grouped_rows[window_label].get("edge", ()))
        pred_rows.extend(grouped_rows[window_label].get("pred_times_place_basis_odds", ()))
    grouped = build_diff_groups(tuple(edge_rows), tuple(pred_rows))
    return list(
        build_strategy_variant_diff_summary_rows(
            grouped=grouped,
            stake_per_bet=config.stake_per_bet,
        ),
    )


def build_selected_test_rows_by_window(
    *,
    selected_summaries: tuple[RankingRuleSelectionSummary, ...],
    selected_rows_by_candidate: dict[tuple[object, ...], tuple[CandidateBetRow, ...]],
) -> dict[str, dict[str, tuple[CandidateBetRow, ...]]]:
    grouped_rows: dict[str, dict[str, tuple[CandidateBetRow, ...]]] = {}
    for summary in selected_summaries:
        if summary.applied_to_split != "test":
            continue
        grouped_rows.setdefault(summary.window_label, {})[summary.ranking_score_rule] = (
            selected_rows_by_candidate[
                candidate_key(
                    window_label=summary.test_window_label,
                    split="test",
                    ranking_score_rule=summary.ranking_score_rule,
                    threshold=summary.threshold,
                    min_win_odds=summary.min_win_odds,
                    max_win_odds=summary.max_win_odds,
                    min_place_basis_odds=summary.min_place_basis_odds,
                    max_place_basis_odds=summary.max_place_basis_odds,
                    min_popularity=summary.min_popularity,
                    max_popularity=summary.max_popularity,
                )
            ]
        )
    return grouped_rows


def build_consensus_rows(
    edge_rows: tuple[CandidateBetRow, ...],
    pred_rows: tuple[CandidateBetRow, ...],
) -> tuple[CandidateBetRow, ...]:
    pred_map = {identity_key(row): row for row in pred_rows}
    return tuple(row for row in edge_rows if identity_key(row) in pred_map)


def build_diff_summaries(
    config: RankingRuleComparisonConfig,
    selected_summaries: tuple[RankingRuleSelectionSummary, ...],
    selected_rows_by_candidate: dict[tuple[object, ...], tuple[CandidateBetRow, ...]],
) -> list[RankingRuleDiffSummary]:
    if "edge" not in config.ranking_score_rules:
        return []
    baseline_rows: list[CandidateBetRow] = []
    for summary in selected_summaries:
        if summary.applied_to_split != "test":
            continue
        if summary.ranking_score_rule != "edge":
            continue
        baseline_rows.extend(
            selected_rows_by_candidate[
                candidate_key(
                    window_label=summary.test_window_label,
                    split="test",
                    ranking_score_rule="edge",
                    threshold=summary.threshold,
                    min_win_odds=summary.min_win_odds,
                    max_win_odds=summary.max_win_odds,
                    min_place_basis_odds=summary.min_place_basis_odds,
                    max_place_basis_odds=summary.max_place_basis_odds,
                    min_popularity=summary.min_popularity,
                    max_popularity=summary.max_popularity,
                )
            ],
        )
    baseline_tuple = tuple(baseline_rows)
    diff_summaries: list[RankingRuleDiffSummary] = []
    for ranking_score_rule in config.ranking_score_rules:
        if ranking_score_rule == "edge":
            continue
        score_rows: list[CandidateBetRow] = []
        for summary in selected_summaries:
            if summary.applied_to_split != "test":
                continue
            if summary.ranking_score_rule != ranking_score_rule:
                continue
            score_rows.extend(
                selected_rows_by_candidate[
                    candidate_key(
                        window_label=summary.test_window_label,
                        split="test",
                        ranking_score_rule=ranking_score_rule,
                        threshold=summary.threshold,
                        min_win_odds=summary.min_win_odds,
                        max_win_odds=summary.max_win_odds,
                        min_place_basis_odds=summary.min_place_basis_odds,
                        max_place_basis_odds=summary.max_place_basis_odds,
                        min_popularity=summary.min_popularity,
                        max_popularity=summary.max_popularity,
                    )
                ],
            )
        grouped = build_diff_groups(baseline_tuple, tuple(score_rows))
        diff_summaries.extend(
            build_diff_summary_rows(
                ranking_score_rule=ranking_score_rule,
                grouped=grouped,
                stake_per_bet=config.stake_per_bet,
            ),
        )
    return diff_summaries


def build_strategy_variant_diff_summary_rows(
    *,
    grouped: dict[str, tuple[CandidateBetRow, ...]],
    stake_per_bet: float,
) -> tuple[RankingStrategyVariantDiffSummary, ...]:
    label_map = {
        "common": "common",
        "baseline_only": "edge_only",
        "score_variant_only": "pred_only",
    }
    return tuple(
        build_strategy_variant_diff_summary(
            set_group=label_map[set_group],
            rows=rows,
            stake_per_bet=stake_per_bet,
        )
        for set_group, rows in grouped.items()
    )


def build_strategy_variant_diff_summary(
    *,
    set_group: str,
    rows: tuple[CandidateBetRow, ...],
    stake_per_bet: float,
) -> RankingStrategyVariantDiffSummary:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return RankingStrategyVariantDiffSummary(
        set_group=set_group,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout or 0.0 for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.edge for row in rows)),
    )


def candidate_key(
    *,
    window_label: str,
    split: str,
    ranking_score_rule: str,
    threshold: float,
    min_win_odds: float | None,
    max_win_odds: float | None,
    min_place_basis_odds: float | None,
    max_place_basis_odds: float | None,
    min_popularity: int | None,
    max_popularity: int | None,
) -> tuple[object, ...]:
    return (
        window_label,
        split,
        ranking_score_rule,
        threshold,
        min_win_odds,
        max_win_odds,
        min_place_basis_odds,
        max_place_basis_odds,
        min_popularity,
        max_popularity,
    )


def candidate_identity(summary: RankingRuleSummary) -> tuple[object, ...]:
    return (
        summary.threshold,
        summary.min_win_odds,
        summary.max_win_odds,
        summary.min_place_basis_odds,
        summary.max_place_basis_odds,
        summary.min_popularity,
        summary.max_popularity,
    )


def aggregate_valid_summaries(
    summaries: list[RankingRuleSummary],
    stake_per_bet: float,
) -> AggregateValidSummary:
    valid_window_rois = tuple(summary.roi for summary in summaries)
    bet_count = sum(summary.bet_count for summary in summaries)
    hit_count = sum(summary.hit_count for summary in summaries)
    total_profit = sum(summary.total_profit for summary in summaries)
    total_return = (bet_count * stake_per_bet) + total_profit
    total_payout = sum(summary.avg_payout * summary.hit_count for summary in summaries)
    mean_valid_roi = sum(valid_window_rois) / len(valid_window_rois)
    return AggregateValidSummary(
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(total_payout / hit_count) if hit_count > 0 else 0.0,
        avg_edge=(
            sum(summary.avg_edge * summary.bet_count for summary in summaries) / bet_count
            if bet_count > 0
            else 0.0
        ),
        mean_valid_roi=mean_valid_roi,
        positive_window_count=sum(1 for summary in summaries if summary.roi > 1.0),
        min_valid_roi=min(valid_window_rois),
        valid_roi_std=math.sqrt(
            sum((roi - mean_valid_roi) ** 2 for roi in valid_window_rois)
            / len(valid_window_rois)
        ),
        valid_window_rois=valid_window_rois,
    )


def aggregate_selection_sort_key(
    aggregate_summary: AggregateValidSummary,
    threshold: float,
    aggregate_selection_score_rule: str,
) -> tuple[float, float, float, float]:
    if aggregate_selection_score_rule == "positive_window_count_then_mean_roi_then_min_roi":
        return (
            float(aggregate_summary.positive_window_count),
            aggregate_summary.mean_valid_roi,
            aggregate_summary.min_valid_roi,
            -threshold,
        )
    if aggregate_selection_score_rule == "mean_valid_roi_minus_std":
        return (
            aggregate_summary.mean_valid_roi - aggregate_summary.valid_roi_std,
            aggregate_summary.min_valid_roi,
            aggregate_summary.bet_count,
            -threshold,
        )
    if aggregate_selection_score_rule == "min_valid_roi_then_mean_roi":
        return (
            aggregate_summary.min_valid_roi,
            aggregate_summary.mean_valid_roi,
            aggregate_summary.bet_count,
            -threshold,
        )
    raise ValueError(
        f"unsupported aggregate_selection_score_rule: {aggregate_selection_score_rule}",
    )


def aggregate_selection_score_value(
    aggregate_summary: AggregateValidSummary,
    aggregate_selection_score_rule: str,
) -> float:
    if aggregate_selection_score_rule == "positive_window_count_then_mean_roi_then_min_roi":
        return float(aggregate_summary.positive_window_count)
    if aggregate_selection_score_rule == "mean_valid_roi_minus_std":
        return aggregate_summary.mean_valid_roi - aggregate_summary.valid_roi_std
    if aggregate_selection_score_rule == "min_valid_roi_then_mean_roi":
        return aggregate_summary.min_valid_roi
    raise ValueError(
        f"unsupported aggregate_selection_score_rule: {aggregate_selection_score_rule}",
    )


def build_diff_groups(
    baseline_rows: tuple[CandidateBetRow, ...],
    score_rows: tuple[CandidateBetRow, ...],
) -> dict[str, tuple[CandidateBetRow, ...]]:
    baseline_map = {identity_key(row): row for row in baseline_rows}
    score_map = {identity_key(row): row for row in score_rows}
    common_keys = tuple(sorted(set(baseline_map) & set(score_map)))
    baseline_only_keys = tuple(sorted(set(baseline_map) - set(score_map)))
    score_only_keys = tuple(sorted(set(score_map) - set(baseline_map)))
    return {
        "common": tuple(baseline_map[key] for key in common_keys),
        "baseline_only": tuple(baseline_map[key] for key in baseline_only_keys),
        "score_variant_only": tuple(score_map[key] for key in score_only_keys),
    }


def build_diff_summary_rows(
    *,
    ranking_score_rule: str,
    grouped: dict[str, tuple[CandidateBetRow, ...]],
    stake_per_bet: float,
) -> tuple[RankingRuleDiffSummary, ...]:
    return tuple(
        build_diff_summary(
            ranking_score_rule=ranking_score_rule,
            set_group=set_group,
            rows=rows,
            stake_per_bet=stake_per_bet,
        )
        for set_group, rows in grouped.items()
    )


def build_diff_summary(
    *,
    ranking_score_rule: str,
    set_group: str,
    rows: tuple[CandidateBetRow, ...],
    stake_per_bet: float,
) -> RankingRuleDiffSummary:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return RankingRuleDiffSummary(
        ranking_score_rule=ranking_score_rule,
        set_group=set_group,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout or 0.0 for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.edge for row in rows)),
    )


def identity_key(row: CandidateBetRow) -> tuple[str, int]:
    return (row.race_key, row.horse_number)


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
    def serialize(value: Any) -> Any:
        if isinstance(value, tuple):
            return [serialize(item) for item in value]
        if hasattr(value, "__dataclass_fields__"):
            return {key: serialize(val) for key, val in asdict(value).items()}
        if isinstance(value, dict):
            return {str(key): serialize(val) for key, val in value.items()}
        return value

    path.write_text(json.dumps(serialize(payload), indent=2, sort_keys=True), encoding="utf-8")
