from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb

from horse_bet_lab.config import RankingScoreDiffConfig


@dataclass(frozen=True)
class CandidateBetRow:
    test_window_label: str
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
class SelectedConditionRow:
    window_label: str
    test_window_label: str
    selection_metric: str
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None


@dataclass(frozen=True)
class RankingScoreSummaryRow:
    score_name: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float | None


@dataclass(frozen=True)
class RankingScoreDiffSummaryRow:
    score_name: str
    set_group: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float | None
    avg_win_odds: float | None
    avg_place_basis_odds: float | None
    avg_popularity: float | None


@dataclass(frozen=True)
class RankingScoreDistributionRow:
    score_name: str
    set_group: str
    metric: str
    bucket_label: str
    count: int
    share: float


@dataclass(frozen=True)
class RankingScoreRaceExampleRow:
    score_name: str
    race_key: str
    baseline_only_count: int
    score_variant_only_count: int
    baseline_only_horse_numbers: str
    score_variant_only_horse_numbers: str
    baseline_only_avg_edge: float | None
    score_variant_only_avg_edge: float | None
    baseline_only_avg_win_odds: float | None
    score_variant_only_avg_win_odds: float | None
    baseline_only_avg_place_basis_odds: float | None
    score_variant_only_avg_place_basis_odds: float | None


@dataclass(frozen=True)
class RankingScoreDiffResult:
    output_dir: Path
    summaries: tuple[RankingScoreSummaryRow, ...]
    diff_summaries: tuple[RankingScoreDiffSummaryRow, ...]
    distributions: tuple[RankingScoreDistributionRow, ...]
    race_examples: tuple[RankingScoreRaceExampleRow, ...]


def analyze_ranking_score_diff(config: RankingScoreDiffConfig) -> RankingScoreDiffResult:
    validate_ranking_score_diff_config(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(config.duckdb_path))
    try:
        prediction_table = "prediction_rows_ranking_score_diff"
        connection.execute(f"DROP TABLE IF EXISTS {prediction_table}")
        connection.execute(
            f"""
            CREATE TEMP TABLE {prediction_table} AS
            SELECT * FROM read_csv_auto(?)
            """,
            [str(config.backtest_dir / "rolling_predictions.csv")],
        )
        selection_rows = load_selected_test_rows(
            config.backtest_dir / "selected_summary.csv",
            config,
        )
        baseline_rows: list[CandidateBetRow] = []
        rows_by_score: dict[str, list[CandidateBetRow]] = {
            score_name: [] for score_name in config.ranking_scores
        }
        for selection in selection_rows:
            candidate_rows = load_candidate_rows(
                connection=connection,
                prediction_table=prediction_table,
                config=config,
                selection=selection,
            )
            selected_baseline_rows = apply_baseline_selection(
                candidate_rows=candidate_rows,
                selection=selection,
            )
            baseline_rows.extend(selected_baseline_rows)
            target_bet_count = len(selected_baseline_rows)
            for score_name in config.ranking_scores:
                ranked_rows = select_top_ranked_rows(
                    candidate_rows=candidate_rows,
                    score_name=score_name,
                    target_bet_count=target_bet_count,
                )
                rows_by_score[score_name].extend(ranked_rows)
    finally:
        connection.close()

    baseline_tuple = tuple(baseline_rows)
    summaries = [
        build_summary_row(
            score_name="baseline_edge_threshold",
            rows=baseline_tuple,
            stake_per_bet=config.stake_per_bet,
        ),
    ]
    diff_summaries: list[RankingScoreDiffSummaryRow] = []
    distributions: list[RankingScoreDistributionRow] = []
    race_examples: list[RankingScoreRaceExampleRow] = []
    for score_name in config.ranking_scores:
        score_rows = tuple(rows_by_score[score_name])
        summaries.append(
            build_summary_row(
                score_name=score_name,
                rows=score_rows,
                stake_per_bet=config.stake_per_bet,
            ),
        )
        grouped = build_diff_groups(baseline_tuple, score_rows)
        diff_summaries.extend(
            build_diff_summary_rows(
                score_name=score_name,
                grouped=grouped,
                stake_per_bet=config.stake_per_bet,
            ),
        )
        distributions.extend(
            build_distribution_rows(
                score_name=score_name,
                grouped=grouped,
            ),
        )
        race_examples.extend(
            build_race_example_rows(
                score_name=score_name,
                baseline_only=grouped["baseline_only"],
                score_variant_only=grouped["score_variant_only"],
            ),
        )

    result = RankingScoreDiffResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        diff_summaries=tuple(diff_summaries),
        distributions=tuple(distributions),
        race_examples=tuple(race_examples),
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "diff_summary.csv", result.diff_summaries)
    write_json(
        config.output_dir / "diff_summary.json",
        {"analysis": {"rows": result.diff_summaries}},
    )
    write_csv(config.output_dir / "distribution.csv", result.distributions)
    write_json(
        config.output_dir / "distribution.json",
        {"analysis": {"rows": result.distributions}},
    )
    write_csv(config.output_dir / "race_examples.csv", result.race_examples)
    write_json(
        config.output_dir / "race_examples.json",
        {"analysis": {"rows": result.race_examples}},
    )
    return result


def validate_ranking_score_diff_config(config: RankingScoreDiffConfig) -> None:
    valid_scores = {
        "edge",
        "edge_times_place_basis_odds",
        "pred_times_place_basis_odds",
        "edge_div_place_basis_odds",
    }
    if config.selection_metric not in {"edge", "probability"}:
        raise ValueError("selection_metric must be 'edge' or 'probability'")
    if not config.ranking_scores:
        raise ValueError("ranking_scores must not be empty")
    for score_name in config.ranking_scores:
        if score_name not in valid_scores:
            raise ValueError(f"unsupported ranking score: {score_name}")
    if config.stake_per_bet <= 0.0:
        raise ValueError("stake_per_bet must be positive")


def load_selected_test_rows(
    path: Path,
    config: RankingScoreDiffConfig,
) -> tuple[SelectedConditionRow, ...]:
    rows: list[SelectedConditionRow] = []
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["applied_to_split"] != "test":
                continue
            if row["selection_mode"] != "aggregate_valid_windows":
                continue
            if row["aggregate_selection_score_rule"] != config.aggregate_selection_score_rule:
                continue
            min_bets_valid_value = parse_optional_int(row["min_bets_valid"])
            if min_bets_valid_value != config.min_bets_valid:
                continue
            rows.append(
                SelectedConditionRow(
                    window_label=row["window_label"],
                    test_window_label=row["test_window_label"] or row["window_label"],
                    selection_metric=row["selection_metric"],
                    threshold=float(row["threshold"]),
                    min_win_odds=parse_optional_float(row["min_win_odds"]),
                    max_win_odds=parse_optional_float(row["max_win_odds"]),
                    min_place_basis_odds=parse_optional_float(row["min_place_basis_odds"]),
                    max_place_basis_odds=parse_optional_float(row["max_place_basis_odds"]),
                    min_popularity=parse_optional_int(row["min_popularity"]),
                    max_popularity=parse_optional_int(row["max_popularity"]),
                ),
            )
    return tuple(rows)


def load_candidate_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    prediction_table: str,
    config: RankingScoreDiffConfig,
    selection: SelectedConditionRow,
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
    parameters: list[object] = [selection.test_window_label, "test"]
    market_conditions = build_market_conditions(
        min_win_odds=selection.min_win_odds,
        max_win_odds=selection.max_win_odds,
        min_place_basis_odds=selection.min_place_basis_odds,
        max_place_basis_odds=selection.max_place_basis_odds,
        min_popularity=selection.min_popularity,
        max_popularity=selection.max_popularity,
    )
    if selection.min_win_odds is not None:
        parameters.append(selection.min_win_odds)
    if selection.max_win_odds is not None:
        parameters.append(selection.max_win_odds)
    if selection.min_place_basis_odds is not None:
        parameters.append(selection.min_place_basis_odds)
    if selection.max_place_basis_odds is not None:
        parameters.append(selection.max_place_basis_odds)
    if selection.min_popularity is not None:
        parameters.append(selection.min_popularity)
    if selection.max_popularity is not None:
        parameters.append(selection.max_popularity)

    rows = connection.execute(
        f"""
        WITH base_rows AS (
            SELECT
                p.race_key,
                p.horse_number,
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
                {market_conditions}
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
            ? AS test_window_label,
            s.race_key,
            s.horse_number,
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
        [*parameters, selection.test_window_label],
    ).fetchall()
    return tuple(
        CandidateBetRow(
            test_window_label=str(row[0]),
            race_key=str(row[1]),
            horse_number=int(row[2]),
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


def apply_baseline_selection(
    *,
    candidate_rows: tuple[CandidateBetRow, ...],
    selection: SelectedConditionRow,
) -> tuple[CandidateBetRow, ...]:
    if selection.selection_metric == "probability":
        return tuple(
            row for row in candidate_rows if row.pred_probability >= selection.threshold
        )
    return tuple(
        row
        for row in candidate_rows
        if row.edge is not None and row.edge >= selection.threshold
    )


def select_top_ranked_rows(
    *,
    candidate_rows: tuple[CandidateBetRow, ...],
    score_name: str,
    target_bet_count: int,
) -> tuple[CandidateBetRow, ...]:
    if target_bet_count <= 0:
        return ()
    scored_rows = [
        (score_value(row, score_name), row)
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


def score_value(row: CandidateBetRow, score_name: str) -> float | None:
    if score_name == "edge":
        return row.edge
    if row.place_basis_odds is None or row.place_basis_odds <= 0.0:
        return None
    if score_name == "edge_times_place_basis_odds":
        if row.edge is None:
            return None
        return row.edge * row.place_basis_odds
    if score_name == "pred_times_place_basis_odds":
        return row.pred_probability * row.place_basis_odds
    if score_name == "edge_div_place_basis_odds":
        if row.edge is None:
            return None
        return row.edge / row.place_basis_odds
    raise ValueError(f"unsupported ranking score: {score_name}")


def build_market_conditions(
    *,
    min_win_odds: float | None,
    max_win_odds: float | None,
    min_place_basis_odds: float | None,
    max_place_basis_odds: float | None,
    min_popularity: int | None,
    max_popularity: int | None,
) -> str:
    conditions: list[str] = []
    if min_win_odds is not None:
        conditions.append("AND TRY_CAST(s.win_odds AS DOUBLE) >= ?")
    if max_win_odds is not None:
        conditions.append("AND TRY_CAST(s.win_odds AS DOUBLE) <= ?")
    if min_place_basis_odds is not None:
        conditions.append("AND o.place_basis_odds >= ?")
    if max_place_basis_odds is not None:
        conditions.append("AND o.place_basis_odds <= ?")
    if min_popularity is not None:
        conditions.append("AND s.popularity >= ?")
    if max_popularity is not None:
        conditions.append("AND s.popularity <= ?")
    if not conditions:
        return ""
    return "\n                " + "\n                ".join(conditions)


def build_summary_row(
    *,
    score_name: str,
    rows: tuple[CandidateBetRow, ...],
    stake_per_bet: float,
) -> RankingScoreSummaryRow:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return RankingScoreSummaryRow(
        score_name=score_name,
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
        "common_baseline": tuple(baseline_map[key] for key in common_keys),
        "common_score": tuple(score_map[key] for key in common_keys),
        "baseline_only": tuple(baseline_map[key] for key in baseline_only_keys),
        "score_variant_only": tuple(score_map[key] for key in score_only_keys),
    }


def build_diff_summary_rows(
    *,
    score_name: str,
    grouped: dict[str, tuple[CandidateBetRow, ...]],
    stake_per_bet: float,
) -> tuple[RankingScoreDiffSummaryRow, ...]:
    return (
        summary_diff_row(
            score_name=score_name,
            set_group="common",
            rows=grouped["common_baseline"],
            stake_per_bet=stake_per_bet,
        ),
        summary_diff_row(
            score_name=score_name,
            set_group="baseline_only",
            rows=grouped["baseline_only"],
            stake_per_bet=stake_per_bet,
        ),
        summary_diff_row(
            score_name=score_name,
            set_group="score_variant_only",
            rows=grouped["score_variant_only"],
            stake_per_bet=stake_per_bet,
        ),
    )


def summary_diff_row(
    *,
    score_name: str,
    set_group: str,
    rows: tuple[CandidateBetRow, ...],
    stake_per_bet: float,
) -> RankingScoreDiffSummaryRow:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return RankingScoreDiffSummaryRow(
        score_name=score_name,
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
        avg_win_odds=average_optional_float(tuple(row.win_odds for row in rows)),
        avg_place_basis_odds=average_optional_float(tuple(row.place_basis_odds for row in rows)),
        avg_popularity=average_optional_float(
            tuple(float(row.popularity) if row.popularity is not None else None for row in rows)
        ),
    )


def build_distribution_rows(
    *,
    score_name: str,
    grouped: dict[str, tuple[CandidateBetRow, ...]],
) -> tuple[RankingScoreDistributionRow, ...]:
    rows: list[RankingScoreDistributionRow] = []
    named_groups = {
        "common": grouped["common_baseline"],
        "baseline_only": grouped["baseline_only"],
        "score_variant_only": grouped["score_variant_only"],
    }
    for set_group, group_rows in named_groups.items():
        rows.extend(
            bucket_distribution_rows(
                score_name=score_name,
                set_group=set_group,
                metric="win_odds",
                values=tuple(row.win_odds for row in group_rows),
                buckets=(
                    (2.0, "lt_2"),
                    (5.0, "2_to_5"),
                    (10.0, "5_to_10"),
                    (20.0, "10_to_20"),
                    (50.0, "20_to_50"),
                ),
            ),
        )
        rows.extend(
            bucket_distribution_rows(
                score_name=score_name,
                set_group=set_group,
                metric="place_basis_odds",
                values=tuple(row.place_basis_odds for row in group_rows),
                buckets=(
                    (1.5, "1_0_to_1_5"),
                    (2.0, "1_5_to_2_0"),
                    (3.0, "2_0_to_3_0"),
                    (5.0, "3_0_to_5_0"),
                ),
            ),
        )
        rows.extend(
            bucket_distribution_rows(
                score_name=score_name,
                set_group=set_group,
                metric="popularity",
                values=tuple(
                    float(row.popularity) if row.popularity is not None else None
                    for row in group_rows
                ),
                buckets=((1.0, "1"), (2.0, "2"), (3.0, "3"), (6.0, "4_to_6"), (9.0, "7_to_9")),
            ),
        )
        rows.extend(
            bucket_distribution_rows(
                score_name=score_name,
                set_group=set_group,
                metric="edge",
                values=tuple(row.edge for row in group_rows),
                buckets=(
                    (0.04, "lt_0_04"),
                    (0.06, "0_04_to_0_06"),
                    (0.08, "0_06_to_0_08"),
                    (0.12, "0_08_to_0_12"),
                ),
            ),
        )
    return tuple(rows)


def build_race_example_rows(
    *,
    score_name: str,
    baseline_only: tuple[CandidateBetRow, ...],
    score_variant_only: tuple[CandidateBetRow, ...],
) -> tuple[RankingScoreRaceExampleRow, ...]:
    baseline_by_race: dict[str, list[CandidateBetRow]] = {}
    variant_by_race: dict[str, list[CandidateBetRow]] = {}
    for row in baseline_only:
        baseline_by_race.setdefault(row.race_key, []).append(row)
    for row in score_variant_only:
        variant_by_race.setdefault(row.race_key, []).append(row)
    shared_races = sorted(set(baseline_by_race) & set(variant_by_race))
    return tuple(
        RankingScoreRaceExampleRow(
            score_name=score_name,
            race_key=race_key,
            baseline_only_count=len(baseline_rows),
            score_variant_only_count=len(variant_rows),
            baseline_only_horse_numbers=",".join(str(row.horse_number) for row in baseline_rows),
            score_variant_only_horse_numbers=",".join(
                str(row.horse_number) for row in variant_rows
            ),
            baseline_only_avg_edge=average_optional_float(tuple(row.edge for row in baseline_rows)),
            score_variant_only_avg_edge=average_optional_float(
                tuple(row.edge for row in variant_rows)
            ),
            baseline_only_avg_win_odds=average_optional_float(
                tuple(row.win_odds for row in baseline_rows)
            ),
            score_variant_only_avg_win_odds=average_optional_float(
                tuple(row.win_odds for row in variant_rows)
            ),
            baseline_only_avg_place_basis_odds=average_optional_float(
                tuple(row.place_basis_odds for row in baseline_rows)
            ),
            score_variant_only_avg_place_basis_odds=average_optional_float(
                tuple(row.place_basis_odds for row in variant_rows)
            ),
        )
        for race_key in shared_races[:10]
        for baseline_rows, variant_rows in [
            (
                tuple(sorted(baseline_by_race[race_key], key=lambda row: row.horse_number)),
                tuple(sorted(variant_by_race[race_key], key=lambda row: row.horse_number)),
            )
        ]
    )


def bucket_distribution_rows(
    *,
    score_name: str,
    set_group: str,
    metric: str,
    values: tuple[float | None, ...],
    buckets: tuple[tuple[float, str], ...],
) -> tuple[RankingScoreDistributionRow, ...]:
    non_null_values = tuple(value for value in values if value is not None)
    total = len(non_null_values)
    counts: dict[str, int] = {label: 0 for _, label in buckets}
    counts["other"] = 0
    for value in non_null_values:
        assigned = False
        previous_boundary = 0.0
        for boundary, label in buckets:
            if previous_boundary <= value < boundary:
                counts[label] += 1
                assigned = True
                break
            previous_boundary = boundary
        if not assigned:
            counts["other"] += 1
    return tuple(
        RankingScoreDistributionRow(
            score_name=score_name,
            set_group=set_group,
            metric=metric,
            bucket_label=label,
            count=count,
            share=(count / total) if total > 0 else 0.0,
        )
        for label, count in counts.items()
    )


def identity_key(row: CandidateBetRow) -> tuple[str, str, int]:
    return (row.test_window_label, row.race_key, row.horse_number)


def average_optional_float(values: tuple[float | None, ...]) -> float | None:
    filtered = tuple(value for value in values if value is not None)
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def parse_optional_float(value: str) -> float | None:
    if value == "":
        return None
    return float(value)


def parse_optional_int(value: str) -> int | None:
    if value == "":
        return None
    return int(value)


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
