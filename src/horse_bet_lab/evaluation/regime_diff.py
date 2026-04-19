from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, TypeAlias

from horse_bet_lab.config import (
    RegimeDiffAnalysisConfig,
    load_reference_strategy_diagnostics_config,
)
from horse_bet_lab.evaluation.reference_strategy import (
    ReferenceStrategyEquityRow,
    run_reference_strategy_diagnostics,
)


@dataclass(frozen=True)
class RegimeSummaryRow:
    regime_label: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RegimeDistributionRow:
    regime_label: str
    metric: str
    bucket_label: str
    bet_count: int
    share: float
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RegimeConditionBandRow:
    regime_label: str
    popularity_bucket: str
    place_basis_odds_bucket: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class RegimeConditionBandDiffRow:
    popularity_bucket: str
    place_basis_odds_bucket: str
    left_regime_label: str
    left_bet_count: int
    left_roi: float
    left_total_profit: float
    right_regime_label: str
    right_bet_count: int
    right_roi: float
    right_total_profit: float
    roi_diff_right_minus_left: float
    profit_diff_right_minus_left: float


@dataclass(frozen=True)
class RegimeRepresentativeExampleRow:
    regime_label: str
    example_type: str
    result_date: str
    window_label: str
    race_key: str
    horse_number: int
    popularity: int | None
    popularity_bucket: str
    win_odds: float | None
    win_odds_bucket: str
    place_basis_odds: float | None
    place_basis_odds_bucket: str
    edge: float | None
    edge_bucket: str
    place_payout: float
    bet_profit: float


@dataclass(frozen=True)
class RegimeDiffAnalysisResult:
    output_dir: Path
    summaries: tuple[RegimeSummaryRow, ...]
    distributions: tuple[RegimeDistributionRow, ...]
    condition_bands: tuple[RegimeConditionBandRow, ...]
    condition_band_diffs: tuple[RegimeConditionBandDiffRow, ...]
    representative_examples: tuple[RegimeRepresentativeExampleRow, ...]


SerializableRow: TypeAlias = (
    RegimeSummaryRow
    | RegimeDistributionRow
    | RegimeConditionBandRow
    | RegimeConditionBandDiffRow
    | RegimeRepresentativeExampleRow
)


def run_regime_diff_analysis(config: RegimeDiffAnalysisConfig) -> RegimeDiffAnalysisResult:
    reference_config = load_reference_strategy_diagnostics_config(
        config.reference_strategy_config_path,
    )
    reference_result = run_reference_strategy_diagnostics(reference_config)
    equity_rows = reference_result.equity_curve
    rows_by_regime = build_rows_by_regime(equity_rows, config.regimes)
    summaries = tuple(
        build_summary_row(regime_label=regime_label, rows=rows)
        for regime_label, rows in rows_by_regime.items()
    )
    distributions = build_distribution_rows(rows_by_regime)
    condition_bands = build_condition_band_rows(rows_by_regime)
    condition_band_diffs = build_condition_band_diffs(condition_bands, config.regimes)
    representative_examples = build_representative_examples(
        rows_by_regime=rows_by_regime,
        condition_bands=condition_bands,
        examples_per_regime=config.representative_examples_per_regime,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = RegimeDiffAnalysisResult(
        output_dir=config.output_dir,
        summaries=summaries,
        distributions=distributions,
        condition_bands=condition_bands,
        condition_band_diffs=condition_band_diffs,
        representative_examples=representative_examples,
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "distribution.csv", result.distributions)
    write_json(
        config.output_dir / "distribution.json",
        {"analysis": {"rows": result.distributions}},
    )
    write_csv(config.output_dir / "condition_band_summary.csv", result.condition_bands)
    write_json(
        config.output_dir / "condition_band_summary.json",
        {"analysis": {"rows": result.condition_bands}},
    )
    write_csv(config.output_dir / "condition_band_diff.csv", result.condition_band_diffs)
    write_json(
        config.output_dir / "condition_band_diff.json",
        {"analysis": {"rows": result.condition_band_diffs}},
    )
    write_csv(config.output_dir / "representative_examples.csv", result.representative_examples)
    write_json(
        config.output_dir / "representative_examples.json",
        {"analysis": {"rows": result.representative_examples}},
    )
    return result


def build_rows_by_regime(
    rows: tuple[ReferenceStrategyEquityRow, ...],
    regimes: tuple[tuple[str, date | None, date | None], ...],
) -> dict[str, tuple[ReferenceStrategyEquityRow, ...]]:
    grouped: dict[str, list[ReferenceStrategyEquityRow]] = {
        label: [] for label, _, _ in regimes
    }
    for row in rows:
        row_date = date.fromisoformat(row.result_date)
        for label, start_date, end_date in regimes:
            if start_date is not None and row_date < start_date:
                continue
            if end_date is not None and row_date > end_date:
                continue
            grouped[label].append(row)
            break
    return {label: tuple(values) for label, values in grouped.items()}


def build_summary_row(
    *,
    regime_label: str,
    rows: tuple[ReferenceStrategyEquityRow, ...],
) -> RegimeSummaryRow:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout > 0.0)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout for row in rows)
    total_profit = sum(row.bet_profit for row in rows)
    return RegimeSummaryRow(
        regime_label=regime_label,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * 100.0)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout for row in hit_rows) / hit_count if hit_count > 0 else 0.0
        ),
        avg_edge=average_optional_float(tuple(row.edge for row in rows)) or 0.0,
    )


def build_distribution_rows(
    rows_by_regime: dict[str, tuple[ReferenceStrategyEquityRow, ...]],
) -> tuple[RegimeDistributionRow, ...]:
    rows: list[RegimeDistributionRow] = []
    for regime_label, regime_rows in rows_by_regime.items():
        rows.extend(
            build_bucket_metric_rows(
                regime_label=regime_label,
                metric="popularity",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_popularity(row.popularity),
            ),
        )
        rows.extend(
            build_bucket_metric_rows(
                regime_label=regime_label,
                metric="place_basis_odds",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_place_basis_odds(row.place_basis_odds),
            ),
        )
        rows.extend(
            build_bucket_metric_rows(
                regime_label=regime_label,
                metric="win_odds",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_win_odds(row.win_odds),
            ),
        )
        rows.extend(
            build_bucket_metric_rows(
                regime_label=regime_label,
                metric="edge",
                rows=regime_rows,
                bucket_fn=lambda row: bucket_edge(row.edge),
            ),
        )
    return tuple(rows)


def build_bucket_metric_rows(
    *,
    regime_label: str,
    metric: str,
    rows: tuple[ReferenceStrategyEquityRow, ...],
    bucket_fn: Any,
) -> tuple[RegimeDistributionRow, ...]:
    grouped: dict[str, list[ReferenceStrategyEquityRow]] = {}
    for row in rows:
        bucket_label = str(bucket_fn(row))
        grouped.setdefault(bucket_label, []).append(row)
    output: list[RegimeDistributionRow] = []
    total_count = len(rows)
    for bucket_label in sorted(grouped):
        bucket_rows = tuple(grouped[bucket_label])
        summary = build_summary_row(regime_label=regime_label, rows=bucket_rows)
        output.append(
            RegimeDistributionRow(
                regime_label=regime_label,
                metric=metric,
                bucket_label=bucket_label,
                bet_count=summary.bet_count,
                share=(summary.bet_count / total_count) if total_count > 0 else 0.0,
                hit_count=summary.hit_count,
                hit_rate=summary.hit_rate,
                roi=summary.roi,
                total_profit=summary.total_profit,
                avg_payout=summary.avg_payout,
                avg_edge=summary.avg_edge,
            ),
        )
    return tuple(output)


def build_condition_band_rows(
    rows_by_regime: dict[str, tuple[ReferenceStrategyEquityRow, ...]],
) -> tuple[RegimeConditionBandRow, ...]:
    rows: list[RegimeConditionBandRow] = []
    for regime_label, regime_rows in rows_by_regime.items():
        grouped: dict[tuple[str, str], list[ReferenceStrategyEquityRow]] = {}
        for row in regime_rows:
            key = (
                bucket_popularity(row.popularity),
                bucket_place_basis_odds(row.place_basis_odds),
            )
            grouped.setdefault(key, []).append(row)
        for (popularity_bucket, place_basis_bucket), bucket_rows in sorted(grouped.items()):
            summary = build_summary_row(regime_label=regime_label, rows=tuple(bucket_rows))
            rows.append(
                RegimeConditionBandRow(
                    regime_label=regime_label,
                    popularity_bucket=popularity_bucket,
                    place_basis_odds_bucket=place_basis_bucket,
                    bet_count=summary.bet_count,
                    hit_count=summary.hit_count,
                    hit_rate=summary.hit_rate,
                    roi=summary.roi,
                    total_profit=summary.total_profit,
                    avg_payout=summary.avg_payout,
                    avg_edge=summary.avg_edge,
                ),
            )
    return tuple(rows)


def build_condition_band_diffs(
    condition_bands: tuple[RegimeConditionBandRow, ...],
    regimes: tuple[tuple[str, date | None, date | None], ...],
) -> tuple[RegimeConditionBandDiffRow, ...]:
    if len(regimes) < 2:
        return ()
    left_label = regimes[0][0]
    right_label = regimes[1][0]
    left_map = {
        (row.popularity_bucket, row.place_basis_odds_bucket): row
        for row in condition_bands
        if row.regime_label == left_label
    }
    right_map = {
        (row.popularity_bucket, row.place_basis_odds_bucket): row
        for row in condition_bands
        if row.regime_label == right_label
    }
    all_keys = sorted(set(left_map) | set(right_map))
    rows: list[RegimeConditionBandDiffRow] = []
    for key in all_keys:
        left_row = left_map.get(key)
        right_row = right_map.get(key)
        rows.append(
            RegimeConditionBandDiffRow(
                popularity_bucket=key[0],
                place_basis_odds_bucket=key[1],
                left_regime_label=left_label,
                left_bet_count=left_row.bet_count if left_row is not None else 0,
                left_roi=left_row.roi if left_row is not None else 0.0,
                left_total_profit=left_row.total_profit if left_row is not None else 0.0,
                right_regime_label=right_label,
                right_bet_count=right_row.bet_count if right_row is not None else 0,
                right_roi=right_row.roi if right_row is not None else 0.0,
                right_total_profit=right_row.total_profit if right_row is not None else 0.0,
                roi_diff_right_minus_left=(
                    (right_row.roi if right_row is not None else 0.0)
                    - (left_row.roi if left_row is not None else 0.0)
                ),
                profit_diff_right_minus_left=(
                    (right_row.total_profit if right_row is not None else 0.0)
                    - (left_row.total_profit if left_row is not None else 0.0)
                ),
            ),
        )
    return tuple(rows)


def build_representative_examples(
    *,
    rows_by_regime: dict[str, tuple[ReferenceStrategyEquityRow, ...]],
    condition_bands: tuple[RegimeConditionBandRow, ...],
    examples_per_regime: int,
) -> tuple[RegimeRepresentativeExampleRow, ...]:
    rows: list[RegimeRepresentativeExampleRow] = []
    condition_band_map: dict[str, tuple[RegimeConditionBandRow, ...]] = {}
    for row in condition_bands:
        condition_band_map.setdefault(row.regime_label, ())
        condition_band_map[row.regime_label] = condition_band_map[row.regime_label] + (row,)
    for regime_label, regime_rows in rows_by_regime.items():
        band_rows = tuple(
            row for row in condition_band_map.get(regime_label, ()) if row.bet_count > 0
        )
        if not band_rows:
            continue
        weakest_band = min(band_rows, key=lambda row: (row.roi, row.bet_count))
        strongest_band = max(band_rows, key=lambda row: (row.roi, row.bet_count))
        rows.extend(
            pick_examples_for_band(
                regime_label=regime_label,
                example_type="weak_band_example",
                rows=regime_rows,
                popularity_bucket=weakest_band.popularity_bucket,
                place_basis_odds_bucket=weakest_band.place_basis_odds_bucket,
                limit=examples_per_regime,
                reverse=False,
            ),
        )
        rows.extend(
            pick_examples_for_band(
                regime_label=regime_label,
                example_type="strong_band_example",
                rows=regime_rows,
                popularity_bucket=strongest_band.popularity_bucket,
                place_basis_odds_bucket=strongest_band.place_basis_odds_bucket,
                limit=examples_per_regime,
                reverse=True,
            ),
        )
    return tuple(rows)


def pick_examples_for_band(
    *,
    regime_label: str,
    example_type: str,
    rows: tuple[ReferenceStrategyEquityRow, ...],
    popularity_bucket: str,
    place_basis_odds_bucket: str,
    limit: int,
    reverse: bool,
) -> tuple[RegimeRepresentativeExampleRow, ...]:
    matched = tuple(
        row
        for row in rows
        if bucket_popularity(row.popularity) == popularity_bucket
        and bucket_place_basis_odds(row.place_basis_odds) == place_basis_odds_bucket
    )
    sorted_rows = sorted(
        matched,
        key=lambda row: (
            row.bet_profit,
            row.edge if row.edge is not None else -999.0,
            row.result_date,
            row.race_key,
            row.horse_number,
        ),
        reverse=reverse,
    )
    return tuple(
        RegimeRepresentativeExampleRow(
            regime_label=regime_label,
            example_type=example_type,
            result_date=row.result_date,
            window_label=row.window_label,
            race_key=row.race_key,
            horse_number=row.horse_number,
            popularity=row.popularity,
            popularity_bucket=bucket_popularity(row.popularity),
            win_odds=row.win_odds,
            win_odds_bucket=bucket_win_odds(row.win_odds),
            place_basis_odds=row.place_basis_odds,
            place_basis_odds_bucket=bucket_place_basis_odds(row.place_basis_odds),
            edge=row.edge,
            edge_bucket=bucket_edge(row.edge),
            place_payout=row.place_payout,
            bet_profit=row.bet_profit,
        )
        for row in sorted_rows[:limit]
    )


def bucket_popularity(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value <= 1:
        return "1"
    if value <= 2:
        return "2"
    if value <= 3:
        return "3"
    if value <= 6:
        return "4_to_6"
    if value <= 9:
        return "7_to_9"
    return "10_plus"


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
    return "3_2_plus"


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
