from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, TypeAlias

from horse_bet_lab.config import (
    CalibrationDriftConfig,
    WithinBandRegimeDiffConfig,
    load_ranking_rule_comparison_config,
    load_reference_strategy_diagnostics_config,
)
from horse_bet_lab.evaluation.ranking_rule_rollforward import run_ranking_rule_comparison
from horse_bet_lab.evaluation.within_band_regime_diff import (
    BandCandidateRow,
    build_consensus_keys_by_test_window,
    load_band_candidate_rows,
)


@dataclass(frozen=True)
class CalibrationDriftSummaryRow:
    regime_label: str
    candidate_count: int
    adopted_count: int
    non_adopted_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_pred_probability: float
    avg_market_prob: float
    avg_edge: float
    avg_payout: float
    pred_minus_empirical: float
    market_minus_empirical: float


@dataclass(frozen=True)
class CalibrationDriftBucketRow:
    regime_label: str
    metric: str
    bucket_label: str
    candidate_count: int
    adopted_count: int
    non_adopted_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_pred_probability: float
    avg_market_prob: float
    avg_edge: float
    avg_payout: float
    pred_minus_empirical: float
    market_minus_empirical: float


@dataclass(frozen=True)
class CalibrationDriftRepresentativeRow:
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
    win_odds: float | None
    popularity: int | None
    place_basis_odds: float | None
    place_payout: float


@dataclass(frozen=True)
class CalibrationDriftResult:
    output_dir: Path
    summaries: tuple[CalibrationDriftSummaryRow, ...]
    bucket_rows: tuple[CalibrationDriftBucketRow, ...]
    representative_rows: tuple[CalibrationDriftRepresentativeRow, ...]


SerializableRow: TypeAlias = (
    CalibrationDriftSummaryRow
    | CalibrationDriftBucketRow
    | CalibrationDriftRepresentativeRow
)


def run_calibration_drift_analysis(config: CalibrationDriftConfig) -> CalibrationDriftResult:
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
    band_config = WithinBandRegimeDiffConfig(
        name=config.name,
        reference_strategy_config_path=config.reference_strategy_config_path,
        output_dir=config.output_dir,
        min_popularity=config.min_popularity,
        max_popularity=config.max_popularity,
        min_place_basis_odds=config.min_place_basis_odds,
        max_place_basis_odds=config.max_place_basis_odds,
        regimes=config.regimes,
        representative_examples_per_regime=config.representative_examples_per_regime,
    )
    rows = load_band_candidate_rows(
        config=band_config,
        ranking_config=ranking_config,
        consensus_keys_by_window=consensus_keys_by_window,
    )
    summaries = build_summary_rows(rows)
    bucket_rows = build_bucket_rows(rows)
    representative_rows = build_representative_rows(
        rows,
        examples_per_regime=config.representative_examples_per_regime,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = CalibrationDriftResult(
        output_dir=config.output_dir,
        summaries=summaries,
        bucket_rows=bucket_rows,
        representative_rows=representative_rows,
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "bucket_summary.csv", result.bucket_rows)
    write_json(
        config.output_dir / "bucket_summary.json",
        {"analysis": {"rows": result.bucket_rows}},
    )
    write_csv(config.output_dir / "representative_examples.csv", result.representative_rows)
    write_json(
        config.output_dir / "representative_examples.json",
        {"analysis": {"rows": result.representative_rows}},
    )
    return result


def build_summary_rows(
    rows: tuple[BandCandidateRow, ...],
) -> tuple[CalibrationDriftSummaryRow, ...]:
    return tuple(
        summarize_rows(
            regime_label=regime_label,
            rows=tuple(row for row in rows if row.regime_label == regime_label),
        )
        for regime_label in sorted({row.regime_label for row in rows})
    )


def summarize_rows(
    *,
    regime_label: str,
    rows: tuple[BandCandidateRow, ...],
) -> CalibrationDriftSummaryRow:
    candidate_count = len(rows)
    adopted_count = sum(1 for row in rows if row.adopted)
    hit_rows = tuple(row for row in rows if row.place_payout > 0.0)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout for row in rows)
    total_profit = sum(row.place_payout - 100.0 for row in rows)
    hit_rate = (hit_count / candidate_count) if candidate_count > 0 else 0.0
    avg_pred_probability = (
        average_optional_float(tuple(row.pred_probability for row in rows)) or 0.0
    )
    avg_market_prob = average_optional_float(tuple(row.market_prob for row in rows)) or 0.0
    return CalibrationDriftSummaryRow(
        regime_label=regime_label,
        candidate_count=candidate_count,
        adopted_count=adopted_count,
        non_adopted_count=candidate_count - adopted_count,
        hit_count=hit_count,
        hit_rate=hit_rate,
        roi=(total_return / (candidate_count * 100.0)) if candidate_count > 0 else 0.0,
        total_profit=total_profit,
        avg_pred_probability=avg_pred_probability,
        avg_market_prob=avg_market_prob,
        avg_edge=average_optional_float(tuple(row.edge for row in rows)) or 0.0,
        avg_payout=(
            sum(row.place_payout for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        pred_minus_empirical=avg_pred_probability - hit_rate,
        market_minus_empirical=avg_market_prob - hit_rate,
    )


def build_bucket_rows(
    rows: tuple[BandCandidateRow, ...],
) -> tuple[CalibrationDriftBucketRow, ...]:
    output: list[CalibrationDriftBucketRow] = []
    metric_specs = (
        (
            "market_prob",
            cast_bucket_fn(lambda row: bucket_market_prob(row.market_prob)),
        ),
        (
            "pred_probability",
            cast_bucket_fn(lambda row: bucket_pred_probability(row.pred_probability)),
        ),
        ("edge", cast_bucket_fn(lambda row: bucket_edge(row.edge))),
    )
    for regime_label in sorted({row.regime_label for row in rows}):
        regime_rows = tuple(row for row in rows if row.regime_label == regime_label)
        for metric_name, bucket_fn in metric_specs:
            grouped: dict[str, list[BandCandidateRow]] = {}
            for row in regime_rows:
                grouped.setdefault(bucket_fn(row), []).append(row)
            for bucket_label in ordered_bucket_labels(metric_name, tuple(grouped.keys())):
                bucket_rows = tuple(grouped[bucket_label])
                summary = summarize_rows(regime_label=regime_label, rows=bucket_rows)
                output.append(
                    CalibrationDriftBucketRow(
                        regime_label=regime_label,
                        metric=metric_name,
                        bucket_label=bucket_label,
                        candidate_count=summary.candidate_count,
                        adopted_count=summary.adopted_count,
                        non_adopted_count=summary.non_adopted_count,
                        hit_count=summary.hit_count,
                        hit_rate=summary.hit_rate,
                        roi=summary.roi,
                        total_profit=summary.total_profit,
                        avg_pred_probability=summary.avg_pred_probability,
                        avg_market_prob=summary.avg_market_prob,
                        avg_edge=summary.avg_edge,
                        avg_payout=summary.avg_payout,
                        pred_minus_empirical=summary.pred_minus_empirical,
                        market_minus_empirical=summary.market_minus_empirical,
                    ),
                )
    return tuple(output)


def build_representative_rows(
    rows: tuple[BandCandidateRow, ...],
    *,
    examples_per_regime: int,
) -> tuple[CalibrationDriftRepresentativeRow, ...]:
    output: list[CalibrationDriftRepresentativeRow] = []
    for regime_label in sorted({row.regime_label for row in rows}):
        regime_rows = tuple(row for row in rows if row.regime_label == regime_label)
        example_specs = (
            (
                "high_pred_miss",
                tuple(row for row in regime_rows if row.place_payout <= 0.0),
                lambda row: (
                    row.pred_probability,
                    row.edge if row.edge is not None else -999.0,
                    row.result_date,
                ),
                True,
            ),
            (
                "low_pred_hit",
                tuple(row for row in regime_rows if row.place_payout > 0.0),
                lambda row: (
                    -(row.pred_probability),
                    row.place_payout,
                    row.result_date,
                ),
                False,
            ),
            (
                "negative_edge_hit",
                tuple(
                    row
                    for row in regime_rows
                    if row.place_payout > 0.0 and (row.edge is not None and row.edge < 0.0)
                ),
                lambda row: (
                    row.edge if row.edge is not None else 999.0,
                    -(row.place_payout),
                    row.result_date,
                ),
                False,
            ),
        )
        for example_type, example_rows, sort_key, reverse in example_specs:
            sorted_rows = sorted(example_rows, key=sort_key, reverse=reverse)
            output.extend(
                CalibrationDriftRepresentativeRow(
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
                    win_odds=row.win_odds,
                    popularity=row.popularity,
                    place_basis_odds=row.place_basis_odds,
                    place_payout=row.place_payout,
                )
                for row in sorted_rows[:examples_per_regime]
            )
    return tuple(output)


def bucket_market_prob(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value < 0.36:
        return "lt_0_36"
    if value < 0.38:
        return "0_36_to_0_38"
    if value < 0.40:
        return "0_38_to_0_40"
    if value < 0.42:
        return "0_40_to_0_42"
    return "0_42_plus"


def bucket_pred_probability(value: float) -> str:
    if value < 0.35:
        return "lt_0_35"
    if value < 0.40:
        return "0_35_to_0_40"
    if value < 0.45:
        return "0_40_to_0_45"
    if value < 0.50:
        return "0_45_to_0_50"
    return "0_50_plus"


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


def cast_bucket_fn(fn: Callable[[BandCandidateRow], str]) -> Callable[[BandCandidateRow], str]:
    return fn


def ordered_bucket_labels(metric: str, labels: tuple[str, ...]) -> tuple[str, ...]:
    orders = {
        "market_prob": (
            "lt_0_36",
            "0_36_to_0_38",
            "0_38_to_0_40",
            "0_40_to_0_42",
            "0_42_plus",
            "unknown",
        ),
        "pred_probability": (
            "lt_0_35",
            "0_35_to_0_40",
            "0_40_to_0_45",
            "0_45_to_0_50",
            "0_50_plus",
            "unknown",
        ),
        "edge": (
            "lt_0_06",
            "0_06_to_0_08",
            "0_08_to_0_10",
            "0_10_to_0_12",
            "0_12_plus",
            "unknown",
        ),
    }
    order_map = {label: index for index, label in enumerate(orders[metric])}
    return tuple(sorted(labels, key=lambda label: order_map.get(label, len(order_map))))


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
