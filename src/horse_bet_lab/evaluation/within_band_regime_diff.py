from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, TypeAlias

import duckdb

from horse_bet_lab.config import (
    WithinBandRegimeDiffConfig,
    load_ranking_rule_comparison_config,
    load_reference_strategy_diagnostics_config,
)
from horse_bet_lab.evaluation.ranking_rule_rollforward import (
    CandidateBetRow,
    build_consensus_rows,
    run_ranking_rule_comparison,
)


@dataclass(frozen=True)
class BandCandidateRow:
    regime_label: str
    window_label: str
    race_key: str
    horse_number: int
    result_date: str
    target_value: int
    finish_position: int | None
    pred_probability: float
    market_prob: float | None
    edge: float | None
    pred_times_place_basis_odds: float | None
    win_odds: float | None
    popularity: int | None
    place_basis_odds: float | None
    place_payout: float
    adopted: bool


@dataclass(frozen=True)
class WithinBandSummaryRow:
    regime_label: str
    selection_status: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_pred_probability: float
    avg_market_prob: float
    avg_edge: float
    avg_payout: float


@dataclass(frozen=True)
class WithinBandFinishPositionRow:
    regime_label: str
    selection_status: str
    finish_position_label: str
    bet_count: int
    share: float


@dataclass(frozen=True)
class WithinBandRankingRow:
    regime_label: str
    selection_status: str
    bet_count: int
    avg_edge_rank: float
    avg_pred_times_place_rank: float
    avg_pred_rank: float
    top1_share_by_edge: float
    top1_share_by_pred_times_place: float


@dataclass(frozen=True)
class WithinBandCalibrationRow:
    regime_label: str
    selection_status: str
    metric: str
    bucket_label: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_pred_probability: float
    avg_edge: float
    avg_payout: float


@dataclass(frozen=True)
class WithinBandRepresentativeRow:
    regime_label: str
    example_type: str
    result_date: str
    window_label: str
    race_key: str
    horse_number: int
    adopted: bool
    finish_position: int | None
    pred_probability: float
    market_prob: float | None
    edge: float | None
    pred_times_place_basis_odds: float | None
    win_odds: float | None
    popularity: int | None
    place_basis_odds: float | None
    place_payout: float


@dataclass(frozen=True)
class WithinBandRegimeDiffResult:
    output_dir: Path
    summaries: tuple[WithinBandSummaryRow, ...]
    finish_position_rows: tuple[WithinBandFinishPositionRow, ...]
    ranking_rows: tuple[WithinBandRankingRow, ...]
    calibration_rows: tuple[WithinBandCalibrationRow, ...]
    representative_rows: tuple[WithinBandRepresentativeRow, ...]


SerializableRow: TypeAlias = (
    WithinBandSummaryRow
    | WithinBandFinishPositionRow
    | WithinBandRankingRow
    | WithinBandCalibrationRow
    | WithinBandRepresentativeRow
)


def run_within_band_regime_diff_analysis(
    config: WithinBandRegimeDiffConfig,
) -> WithinBandRegimeDiffResult:
    reference_config = load_reference_strategy_diagnostics_config(
        config.reference_strategy_config_path,
    )
    ranking_config = load_ranking_rule_comparison_config(
        reference_config.ranking_rule_comparison_config_path,
    )
    ranking_result = run_ranking_rule_comparison(ranking_config)
    consensus_keys_by_window = build_consensus_keys_by_test_window(
        selected_summaries=ranking_result.selected_summaries,
        selected_rows_by_candidate=ranking_result.selected_rows_by_candidate,
    )
    rows = load_band_candidate_rows(
        config=config,
        ranking_config=ranking_config,
        consensus_keys_by_window=consensus_keys_by_window,
    )
    summaries = build_summary_rows(rows)
    finish_position_rows = build_finish_position_rows(rows)
    ranking_rows = build_ranking_rows(rows)
    calibration_rows = build_calibration_rows(rows)
    representative_rows = build_representative_rows(
        rows,
        examples_per_regime=config.representative_examples_per_regime,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = WithinBandRegimeDiffResult(
        output_dir=config.output_dir,
        summaries=summaries,
        finish_position_rows=finish_position_rows,
        ranking_rows=ranking_rows,
        calibration_rows=calibration_rows,
        representative_rows=representative_rows,
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "finish_position_distribution.csv", result.finish_position_rows)
    write_json(
        config.output_dir / "finish_position_distribution.json",
        {"analysis": {"rows": result.finish_position_rows}},
    )
    write_csv(config.output_dir / "ranking_summary.csv", result.ranking_rows)
    write_json(
        config.output_dir / "ranking_summary.json",
        {"analysis": {"rows": result.ranking_rows}},
    )
    write_csv(config.output_dir / "calibration_summary.csv", result.calibration_rows)
    write_json(
        config.output_dir / "calibration_summary.json",
        {"analysis": {"rows": result.calibration_rows}},
    )
    write_csv(config.output_dir / "representative_examples.csv", result.representative_rows)
    write_json(
        config.output_dir / "representative_examples.json",
        {"analysis": {"rows": result.representative_rows}},
    )
    return result


def build_consensus_keys_by_test_window(
    *,
    selected_summaries: tuple[Any, ...],
    selected_rows_by_candidate: dict[tuple[object, ...], tuple[CandidateBetRow, ...]],
) -> dict[str, set[tuple[str, int]]]:
    edge_rows_by_test_window: dict[str, tuple[CandidateBetRow, ...]] = {}
    pred_rows_by_test_window: dict[str, tuple[CandidateBetRow, ...]] = {}
    for summary in selected_summaries:
        if summary.applied_to_split != "test":
            continue
        candidate_rows = selected_rows_by_candidate[
            (
                summary.test_window_label,
                "test",
                summary.ranking_score_rule,
                summary.threshold,
                summary.min_win_odds,
                summary.max_win_odds,
                summary.min_place_basis_odds,
                summary.max_place_basis_odds,
                summary.min_popularity,
                summary.max_popularity,
            )
        ]
        if summary.ranking_score_rule == "edge":
            edge_rows_by_test_window[summary.test_window_label] = candidate_rows
        if summary.ranking_score_rule == "pred_times_place_basis_odds":
            pred_rows_by_test_window[summary.test_window_label] = candidate_rows
    output: dict[str, set[tuple[str, int]]] = {}
    for test_window_label in sorted(set(edge_rows_by_test_window) | set(pred_rows_by_test_window)):
        consensus_rows = build_consensus_rows(
            edge_rows_by_test_window.get(test_window_label, ()),
            pred_rows_by_test_window.get(test_window_label, ()),
        )
        output[test_window_label] = {(row.race_key, row.horse_number) for row in consensus_rows}
    return output


def load_band_candidate_rows(
    *,
    config: WithinBandRegimeDiffConfig,
    ranking_config: Any,
    consensus_keys_by_window: dict[str, set[tuple[str, int]]],
) -> tuple[BandCandidateRow, ...]:
    connection = duckdb.connect(str(ranking_config.duckdb_path), read_only=True)
    try:
        prediction_table = "prediction_rows_within_band_regime_diff"
        connection.execute(f"DROP TABLE IF EXISTS {prediction_table}")
        connection.execute(
            f"""
            CREATE TEMP TABLE {prediction_table} AS
            SELECT * FROM read_csv_auto(?)
            """,
            [str(ranking_config.rolling_predictions_path)],
        )
        rows: list[BandCandidateRow] = []
        for window_label in sorted(consensus_keys_by_window):
            regime_label = resolve_regime_label(window_label, config.regimes)
            if regime_label is None:
                continue
            fetched = connection.execute(
                f"""
                WITH payout_rows AS (
                    SELECT
                        race_key,
                        place_horse_number_1 AS horse_number,
                        place_payout_1 AS place_payout
                    FROM jrdb_hjc_staging
                    UNION ALL
                    SELECT
                        race_key,
                        place_horse_number_2 AS horse_number,
                        place_payout_2 AS place_payout
                    FROM jrdb_hjc_staging
                    UNION ALL
                    SELECT
                        race_key,
                        place_horse_number_3 AS horse_number,
                        place_payout_3 AS place_payout
                    FROM jrdb_hjc_staging
                )
                SELECT
                    p.window_label,
                    p.race_key,
                    p.horse_number,
                    s.result_date,
                    p.{ranking_config.target_column} AS target_value,
                    s.finish_position,
                    p.{ranking_config.probability_column} AS pred_probability,
                    CASE
                        WHEN o.place_basis_odds IS NOT NULL AND o.place_basis_odds > 0.0
                        THEN 1.0 / o.place_basis_odds
                        ELSE NULL
                    END AS market_prob,
                    CASE
                        WHEN o.place_basis_odds IS NOT NULL AND o.place_basis_odds > 0.0
                        THEN p.{ranking_config.probability_column} - (1.0 / o.place_basis_odds)
                        ELSE NULL
                    END AS edge,
                    CASE
                        WHEN o.place_basis_odds IS NOT NULL AND o.place_basis_odds > 0.0
                        THEN p.{ranking_config.probability_column} * o.place_basis_odds
                        ELSE NULL
                    END AS pred_times_place_basis_odds,
                    TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                    s.popularity,
                    o.place_basis_odds,
                    COALESCE(j.place_payout, 0.0) AS place_payout
                FROM {prediction_table} p
                INNER JOIN jrdb_sed_staging s
                    ON p.race_key = s.race_key
                    AND p.horse_number = s.horse_number
                LEFT JOIN jrdb_oz_staging o
                    ON p.race_key = o.race_key
                    AND p.horse_number = o.horse_number
                LEFT JOIN payout_rows j
                    ON p.race_key = j.race_key
                    AND p.horse_number = j.horse_number
                WHERE p.window_label = ?
                    AND p.{ranking_config.split_column} = 'test'
                    AND s.popularity >= ?
                    AND s.popularity <= ?
                    AND o.place_basis_odds >= ?
                    AND o.place_basis_odds <= ?
                ORDER BY s.result_date, p.race_key, p.horse_number
                """,
                [
                    window_label,
                    config.min_popularity,
                    config.max_popularity,
                    config.min_place_basis_odds,
                    config.max_place_basis_odds,
                ],
            ).fetchall()
            selected_keys = consensus_keys_by_window[window_label]
            rows.extend(
                BandCandidateRow(
                    regime_label=regime_label,
                    window_label=str(row[0]),
                    race_key=str(row[1]),
                    horse_number=int(row[2]),
                    result_date=row[3].isoformat(),
                    target_value=int(row[4]),
                    finish_position=int(row[5]) if row[5] is not None else None,
                    pred_probability=float(row[6]),
                    market_prob=float(row[7]) if row[7] is not None else None,
                    edge=float(row[8]) if row[8] is not None else None,
                    pred_times_place_basis_odds=float(row[9]) if row[9] is not None else None,
                    win_odds=float(row[10]) if row[10] is not None else None,
                    popularity=int(row[11]) if row[11] is not None else None,
                    place_basis_odds=float(row[12]) if row[12] is not None else None,
                    place_payout=float(row[13]),
                    adopted=(str(row[1]), int(row[2])) in selected_keys,
                )
                for row in fetched
            )
        return tuple(rows)
    finally:
        connection.close()


def resolve_regime_label(
    window_label: str,
    regimes: tuple[tuple[str, date | None, date | None], ...],
) -> str | None:
    parts = window_label.split("_")
    if len(parts) < 2:
        return None
    year_text = parts[0]
    month_number = parts[1]
    window_date = date(int(year_text), int(month_number), 1)
    for label, start_date, end_date in regimes:
        if start_date is not None and window_date < start_date:
            continue
        if end_date is not None and window_date > end_date:
            continue
        return label
    return None


def build_summary_rows(rows: tuple[BandCandidateRow, ...]) -> tuple[WithinBandSummaryRow, ...]:
    output: list[WithinBandSummaryRow] = []
    for regime_label in sorted({row.regime_label for row in rows}):
        regime_rows = tuple(row for row in rows if row.regime_label == regime_label)
        output.append(summarize_rows(regime_label, "all", regime_rows))
        adopted_rows = tuple(row for row in regime_rows if row.adopted)
        non_adopted_rows = tuple(row for row in regime_rows if not row.adopted)
        output.append(summarize_rows(regime_label, "adopted", adopted_rows))
        output.append(summarize_rows(regime_label, "non_adopted", non_adopted_rows))
    return tuple(output)


def summarize_rows(
    regime_label: str,
    selection_status: str,
    rows: tuple[BandCandidateRow, ...],
) -> WithinBandSummaryRow:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout > 0.0)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout for row in rows)
    total_profit = sum(row.place_payout - 100.0 for row in rows)
    return WithinBandSummaryRow(
        regime_label=regime_label,
        selection_status=selection_status,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * 100.0)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_pred_probability=(
            average_optional_float(tuple(row.pred_probability for row in rows)) or 0.0
        ),
        avg_market_prob=average_optional_float(tuple(row.market_prob for row in rows)) or 0.0,
        avg_edge=average_optional_float(tuple(row.edge for row in rows)) or 0.0,
        avg_payout=(
            sum(row.place_payout for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
    )


def build_finish_position_rows(
    rows: tuple[BandCandidateRow, ...],
) -> tuple[WithinBandFinishPositionRow, ...]:
    output: list[WithinBandFinishPositionRow] = []
    for regime_label in sorted({row.regime_label for row in rows}):
        adopted_rows = tuple(
            row for row in rows if row.regime_label == regime_label and row.adopted
        )
        non_adopted_rows = tuple(
            row for row in rows if row.regime_label == regime_label and not row.adopted
        )
        for selection_status, grouped_rows in (
            ("adopted", adopted_rows),
            ("non_adopted", non_adopted_rows),
        ):
            total_count = len(grouped_rows)
            grouped: dict[str, int] = {}
            for row in grouped_rows:
                label = str(row.finish_position) if row.finish_position is not None else "unknown"
                grouped[label] = grouped.get(label, 0) + 1
            for finish_label, count in sorted(grouped.items(), key=lambda item: item[0]):
                output.append(
                    WithinBandFinishPositionRow(
                        regime_label=regime_label,
                        selection_status=selection_status,
                        finish_position_label=finish_label,
                        bet_count=count,
                        share=(count / total_count) if total_count > 0 else 0.0,
                    ),
                )
    return tuple(output)


def build_ranking_rows(rows: tuple[BandCandidateRow, ...]) -> tuple[WithinBandRankingRow, ...]:
    rank_maps = build_rank_maps(rows)
    output: list[WithinBandRankingRow] = []
    for regime_label in sorted({row.regime_label for row in rows}):
        adopted_rows = tuple(
            row for row in rows if row.regime_label == regime_label and row.adopted
        )
        non_adopted_rows = tuple(
            row for row in rows if row.regime_label == regime_label and not row.adopted
        )
        for selection_status, grouped_rows in (
            ("adopted", adopted_rows),
            ("non_adopted", non_adopted_rows),
        ):
            bet_count = len(grouped_rows)
            output.append(
                WithinBandRankingRow(
                    regime_label=regime_label,
                    selection_status=selection_status,
                    bet_count=bet_count,
                    avg_edge_rank=average_optional_float(
                        tuple(rank_maps["edge"].get(identity_key(row)) for row in grouped_rows),
                    )
                    or 0.0,
                    avg_pred_times_place_rank=average_optional_float(
                        tuple(
                            rank_maps["pred_times_place"].get(identity_key(row))
                            for row in grouped_rows
                        ),
                    )
                    or 0.0,
                    avg_pred_rank=average_optional_float(
                        tuple(rank_maps["pred"].get(identity_key(row)) for row in grouped_rows),
                    )
                    or 0.0,
                    top1_share_by_edge=top1_share(grouped_rows, rank_maps["edge"]),
                    top1_share_by_pred_times_place=top1_share(
                        grouped_rows,
                        rank_maps["pred_times_place"],
                    ),
                ),
            )
    return tuple(output)


def build_rank_maps(
    rows: tuple[BandCandidateRow, ...],
) -> dict[str, dict[tuple[str, int], float]]:
    output: dict[str, dict[tuple[str, int], float]] = {
        "edge": {},
        "pred_times_place": {},
        "pred": {},
    }
    by_race: dict[tuple[str, str], list[BandCandidateRow]] = {}
    for row in rows:
        by_race.setdefault((row.regime_label, row.race_key), []).append(row)
    for race_rows in by_race.values():
        metric_fns: tuple[tuple[str, Callable[[BandCandidateRow], float]], ...] = (
            ("edge", lambda row: row.edge if row.edge is not None else -999.0),
            (
                "pred_times_place",
                lambda row: (
                    row.pred_times_place_basis_odds
                    if row.pred_times_place_basis_odds is not None
                    else -999.0
                ),
            ),
            ("pred", lambda row: row.pred_probability),
        )
        for metric_name, value_fn in metric_fns:
            sorted_rows = sorted(
                race_rows,
                key=lambda row: (
                    value_fn(row),
                    row.pred_probability,
                    row.place_basis_odds if row.place_basis_odds is not None else -999.0,
                    row.horse_number,
                ),
                reverse=True,
            )
            for index, row in enumerate(sorted_rows, start=1):
                output[metric_name][identity_key(row)] = float(index)
    return output


def top1_share(
    rows: tuple[BandCandidateRow, ...],
    rank_map: dict[tuple[str, int], float],
) -> float:
    if not rows:
        return 0.0
    top1_count = sum(1 for row in rows if rank_map.get(identity_key(row)) == 1.0)
    return top1_count / len(rows)


def build_calibration_rows(
    rows: tuple[BandCandidateRow, ...],
) -> tuple[WithinBandCalibrationRow, ...]:
    output: list[WithinBandCalibrationRow] = []
    for regime_label in sorted({row.regime_label for row in rows}):
        adopted_rows = tuple(
            row for row in rows if row.regime_label == regime_label and row.adopted
        )
        non_adopted_rows = tuple(
            row for row in rows if row.regime_label == regime_label and not row.adopted
        )
        for selection_status, grouped_rows in (
            ("adopted", adopted_rows),
            ("non_adopted", non_adopted_rows),
        ):
            output.extend(
                calibration_metric_rows(
                    regime_label=regime_label,
                    selection_status=selection_status,
                    metric="edge",
                    rows=grouped_rows,
                    bucket_fn=lambda row: bucket_edge(row.edge),
                ),
            )
            output.extend(
                calibration_metric_rows(
                    regime_label=regime_label,
                    selection_status=selection_status,
                    metric="pred_probability",
                    rows=grouped_rows,
                    bucket_fn=lambda row: bucket_pred(row.pred_probability),
                ),
            )
    return tuple(output)


def calibration_metric_rows(
    *,
    regime_label: str,
    selection_status: str,
    metric: str,
    rows: tuple[BandCandidateRow, ...],
    bucket_fn: Callable[[BandCandidateRow], str],
) -> tuple[WithinBandCalibrationRow, ...]:
    grouped: dict[str, list[BandCandidateRow]] = {}
    for row in rows:
        grouped.setdefault(str(bucket_fn(row)), []).append(row)
    output: list[WithinBandCalibrationRow] = []
    for bucket_label in sorted(grouped):
        bucket_rows = tuple(grouped[bucket_label])
        summary = summarize_rows(regime_label, selection_status, bucket_rows)
        output.append(
            WithinBandCalibrationRow(
                regime_label=regime_label,
                selection_status=selection_status,
                metric=metric,
                bucket_label=bucket_label,
                bet_count=summary.bet_count,
                hit_count=summary.hit_count,
                hit_rate=summary.hit_rate,
                roi=summary.roi,
                total_profit=summary.total_profit,
                avg_pred_probability=summary.avg_pred_probability,
                avg_edge=summary.avg_edge,
                avg_payout=summary.avg_payout,
            ),
        )
    return tuple(output)


def build_representative_rows(
    rows: tuple[BandCandidateRow, ...],
    *,
    examples_per_regime: int,
) -> tuple[WithinBandRepresentativeRow, ...]:
    output: list[WithinBandRepresentativeRow] = []
    for regime_label in sorted({row.regime_label for row in rows}):
        regime_rows = tuple(row for row in rows if row.regime_label == regime_label)
        output.extend(
            representative_subset(
                regime_label=regime_label,
                example_type="adopted_hit",
                rows=tuple(row for row in regime_rows if row.adopted and row.place_payout > 0.0),
                limit=examples_per_regime,
                reverse=True,
            ),
        )
        output.extend(
            representative_subset(
                regime_label=regime_label,
                example_type="adopted_miss",
                rows=tuple(row for row in regime_rows if row.adopted and row.place_payout <= 0.0),
                limit=examples_per_regime,
                reverse=False,
            ),
        )
        output.extend(
            representative_subset(
                regime_label=regime_label,
                example_type="non_adopted_hit",
                rows=tuple(
                    row
                    for row in regime_rows
                    if (not row.adopted) and row.place_payout > 0.0
                ),
                limit=examples_per_regime,
                reverse=True,
            ),
        )
    return tuple(output)


def representative_subset(
    *,
    regime_label: str,
    example_type: str,
    rows: tuple[BandCandidateRow, ...],
    limit: int,
    reverse: bool,
) -> tuple[WithinBandRepresentativeRow, ...]:
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            row.place_payout - 100.0,
            row.edge if row.edge is not None else -999.0,
            row.pred_probability,
            row.result_date,
            row.race_key,
            row.horse_number,
        ),
        reverse=reverse,
    )
    return tuple(
        WithinBandRepresentativeRow(
            regime_label=regime_label,
            example_type=example_type,
            result_date=row.result_date,
            window_label=row.window_label,
            race_key=row.race_key,
            horse_number=row.horse_number,
            adopted=row.adopted,
            finish_position=row.finish_position,
            pred_probability=row.pred_probability,
            market_prob=row.market_prob,
            edge=row.edge,
            pred_times_place_basis_odds=row.pred_times_place_basis_odds,
            win_odds=row.win_odds,
            popularity=row.popularity,
            place_basis_odds=row.place_basis_odds,
            place_payout=row.place_payout,
        )
        for row in sorted_rows[:limit]
    )


def identity_key(row: BandCandidateRow) -> tuple[str, int]:
    return (row.race_key, row.horse_number)


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


def bucket_pred(value: float) -> str:
    if value < 0.35:
        return "lt_0_35"
    if value < 0.40:
        return "0_35_to_0_40"
    if value < 0.45:
        return "0_40_to_0_45"
    if value < 0.50:
        return "0_45_to_0_50"
    return "0_50_plus"


def average_optional_float(values: tuple[float | None, ...]) -> float | None:
    present = tuple(value for value in values if value is not None)
    if not present:
        return None
    return sum(present) / len(present)


def write_csv(path: Path, rows: tuple[SerializableRow, ...]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    first_row = asdict(rows[0])
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(first_row.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(
        json.dumps(serialize_json(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def serialize_json(value: Any) -> Any:
    if isinstance(value, tuple):
        return [serialize_json(item) for item in value]
    if isinstance(value, list):
        return [serialize_json(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize_json(item) for key, item in value.items()}
    if hasattr(value, "__dataclass_fields__"):
        return serialize_json(asdict(value))
    return value
