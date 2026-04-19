from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, TypeAlias

from horse_bet_lab.config import (
    ResidualLossAnalysisConfig,
    load_reference_guard_compare_config,
)
from horse_bet_lab.evaluation.reference_guard_compare import (
    GuardVariantEquityRow,
    ReferenceGuardCompareResult,
    run_reference_guard_compare,
)
from horse_bet_lab.evaluation.reference_strategy import (
    average_optional_float,
    write_csv,
    write_json,
)


@dataclass(frozen=True)
class ResidualLossSummaryRow:
    variant: str
    regime_label: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class ResidualLossBucketRow:
    variant: str
    regime_label: str
    bucket_type: str
    bucket_label: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class ResidualLossTopBandRow:
    regime_label: str
    bucket_type: str
    bucket_label: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge: float


@dataclass(frozen=True)
class ResidualLossRepresentativeRow:
    regime_label: str
    example_type: str
    result_date: str
    window_label: str
    race_key: str
    horse_number: int
    place_payout: float
    bet_profit: float
    win_odds: float | None
    place_basis_odds: float | None
    popularity: int | None
    edge: float | None


@dataclass(frozen=True)
class ResidualLossAnalysisResult:
    output_dir: Path
    summaries: tuple[ResidualLossSummaryRow, ...]
    bucket_rows: tuple[ResidualLossBucketRow, ...]
    top_loss_bands: tuple[ResidualLossTopBandRow, ...]
    representative_rows: tuple[ResidualLossRepresentativeRow, ...]


EquityRows: TypeAlias = tuple[GuardVariantEquityRow, ...]


def run_residual_loss_analysis(
    config: ResidualLossAnalysisConfig,
) -> ResidualLossAnalysisResult:
    guard_config = load_reference_guard_compare_config(
        config.reference_guard_compare_config_path,
    )
    guard_result = run_reference_guard_compare(guard_config)
    summaries = build_summaries(guard_result, config)
    bucket_rows = build_bucket_rows(guard_result, config)
    top_loss_bands = build_top_loss_bands(bucket_rows)
    representative_rows = build_representative_rows(guard_result, config)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ResidualLossAnalysisResult(
        output_dir=config.output_dir,
        summaries=summaries,
        bucket_rows=bucket_rows,
        top_loss_bands=top_loss_bands,
        representative_rows=representative_rows,
    )
    write_csv(config.output_dir / "summary.csv", result.summaries)
    write_json(config.output_dir / "summary.json", {"analysis": {"rows": result.summaries}})
    write_csv(config.output_dir / "bucket_summary.csv", result.bucket_rows)
    write_json(
        config.output_dir / "bucket_summary.json",
        {"analysis": {"rows": result.bucket_rows}},
    )
    write_csv(config.output_dir / "top_loss_bands.csv", result.top_loss_bands)
    write_json(
        config.output_dir / "top_loss_bands.json",
        {"analysis": {"rows": result.top_loss_bands}},
    )
    write_csv(config.output_dir / "representative_examples.csv", result.representative_rows)
    write_json(
        config.output_dir / "representative_examples.json",
        {"analysis": {"rows": result.representative_rows}},
    )
    return result


def build_summaries(
    guard_result: ReferenceGuardCompareResult,
    config: ResidualLossAnalysisConfig,
) -> tuple[ResidualLossSummaryRow, ...]:
    rows = tuple(
        row
        for row in guard_result.equity_rows
        if row.variant in {config.baseline_variant, config.guarded_variant}
    )
    output: list[ResidualLossSummaryRow] = []
    for variant in (config.baseline_variant, config.guarded_variant):
        variant_rows = tuple(row for row in rows if row.variant == variant)
        for regime_label in ("2023", "2024_2025"):
            regime_rows = tuple(
                row
                for row in variant_rows
                if regime_for_date(row.result_date) == regime_label
            )
            output.append(
                summarize_rows(
                    variant=variant,
                    regime_label=regime_label,
                    rows=regime_rows,
                ),
            )
    return tuple(output)


def build_bucket_rows(
    guard_result: ReferenceGuardCompareResult,
    config: ResidualLossAnalysisConfig,
) -> tuple[ResidualLossBucketRow, ...]:
    rows = tuple(
        row
        for row in guard_result.equity_rows
        if row.variant in {config.baseline_variant, config.guarded_variant}
    )
    output: list[ResidualLossBucketRow] = []
    bucket_specs = (
        ("popularity", cast_bucket_fn(lambda row: bucket_popularity(row.popularity))),
        (
            "place_basis_odds",
            cast_bucket_fn(lambda row: bucket_place_basis_odds(row.place_basis_odds)),
        ),
        ("win_odds", cast_bucket_fn(lambda row: bucket_win_odds(row.win_odds))),
        ("edge", cast_bucket_fn(lambda row: bucket_edge(row.edge))),
    )
    for variant in (config.baseline_variant, config.guarded_variant):
        variant_rows = tuple(row for row in rows if row.variant == variant)
        for regime_label in ("2023", "2024_2025"):
            regime_rows = tuple(
                row
                for row in variant_rows
                if regime_for_date(row.result_date) == regime_label
            )
            for bucket_type, bucket_fn in bucket_specs:
                grouped: dict[str, list[GuardVariantEquityRow]] = {}
                for row in regime_rows:
                    grouped.setdefault(str(bucket_fn(row)), []).append(row)
                for bucket_label in ordered_labels(bucket_type, tuple(grouped.keys())):
                    bucket_rows = tuple(grouped[bucket_label])
                    summary = summarize_rows(
                        variant=variant,
                        regime_label=regime_label,
                        rows=bucket_rows,
                    )
                    output.append(
                        ResidualLossBucketRow(
                            variant=variant,
                            regime_label=regime_label,
                            bucket_type=bucket_type,
                            bucket_label=bucket_label,
                            bet_count=summary.bet_count,
                            hit_count=summary.hit_count,
                            hit_rate=summary.hit_rate,
                            roi=summary.roi,
                            total_profit=summary.total_profit,
                            avg_payout=summary.avg_payout,
                            avg_edge=summary.avg_edge,
                        ),
                    )
    return tuple(output)


def build_top_loss_bands(
    bucket_rows: tuple[ResidualLossBucketRow, ...],
) -> tuple[ResidualLossTopBandRow, ...]:
    output: list[ResidualLossTopBandRow] = []
    guarded_rows = tuple(row for row in bucket_rows if row.variant == "problematic_band_excluded")
    for regime_label in ("2023", "2024_2025"):
        regime_rows = tuple(row for row in guarded_rows if row.regime_label == regime_label)
        for row in sorted(
            regime_rows,
            key=lambda item: (item.total_profit, item.roi, -item.bet_count),
        )[:5]:
            output.append(
                ResidualLossTopBandRow(
                    regime_label=regime_label,
                    bucket_type=row.bucket_type,
                    bucket_label=row.bucket_label,
                    bet_count=row.bet_count,
                    hit_count=row.hit_count,
                    hit_rate=row.hit_rate,
                    roi=row.roi,
                    total_profit=row.total_profit,
                    avg_payout=row.avg_payout,
                    avg_edge=row.avg_edge,
                ),
            )
    return tuple(output)


def build_representative_rows(
    guard_result: ReferenceGuardCompareResult,
    config: ResidualLossAnalysisConfig,
) -> tuple[ResidualLossRepresentativeRow, ...]:
    rows = tuple(
        row
        for row in guard_result.equity_rows
        if row.variant in {config.baseline_variant, config.guarded_variant}
    )
    guarded_keys = {
        (row.window_label, row.race_key, row.horse_number)
        for row in rows
        if row.variant == config.guarded_variant
    }
    output: list[ResidualLossRepresentativeRow] = []
    for regime_label in ("2023", "2024_2025"):
        guarded_rows = tuple(
            row
            for row in rows
            if row.variant == config.guarded_variant
            and regime_for_date(row.result_date) == regime_label
        )
        removed_rows = tuple(
            row
            for row in rows
            if row.variant == config.baseline_variant
            and regime_for_date(row.result_date) == regime_label
            and (row.window_label, row.race_key, row.horse_number) not in guarded_keys
        )
        kept_loss_rows = tuple(row for row in guarded_rows if row.bet_profit < 0.0)
        kept_hit_rows = tuple(row for row in guarded_rows if row.bet_profit > 0.0)
        example_specs = (
            ("guarded_loss", kept_loss_rows, False),
            ("removed_loss", tuple(row for row in removed_rows if row.bet_profit < 0.0), False),
            ("guarded_hit", kept_hit_rows, True),
        )
        for example_type, example_rows, reverse in example_specs:
            sorted_rows = sorted(
                example_rows,
                key=lambda row: (
                    row.bet_profit,
                    row.edge if row.edge is not None else -999.0,
                    row.result_date,
                ),
                reverse=reverse,
            )
            for row in sorted_rows[: config.representative_examples_per_regime]:
                output.append(
                    ResidualLossRepresentativeRow(
                        regime_label=regime_label,
                        example_type=example_type,
                        result_date=row.result_date,
                        window_label=row.window_label,
                        race_key=row.race_key,
                        horse_number=row.horse_number,
                        place_payout=row.place_payout,
                        bet_profit=row.bet_profit,
                        win_odds=row.win_odds,
                        place_basis_odds=row.place_basis_odds,
                        popularity=row.popularity,
                        edge=row.edge,
                    ),
                )
    return tuple(output)


def summarize_rows(
    *,
    variant: str,
    regime_label: str,
    rows: EquityRows,
) -> ResidualLossSummaryRow:
    hit_rows = tuple(row for row in rows if row.place_payout > 0.0)
    bet_count = len(rows)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout for row in rows)
    total_profit = sum(row.bet_profit for row in rows)
    return ResidualLossSummaryRow(
        variant=variant,
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


def regime_for_date(result_date: str) -> str:
    return "2023" if result_date.startswith("2023-") else "2024_2025"


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


def ordered_labels(bucket_type: str, labels: tuple[str, ...]) -> tuple[str, ...]:
    orders = {
        "popularity": ("1_2", "3_4", "5_6", "7_plus", "unknown"),
        "place_basis_odds": (
            "lt_2_0",
            "2_0_to_2_4",
            "2_4_to_2_8",
            "2_8_to_3_2",
            "3_2_to_4_0",
            "4_0_plus",
            "unknown",
        ),
        "win_odds": ("lt_5", "5_to_10", "10_to_20", "20_plus", "unknown"),
        "edge": (
            "lt_0_06",
            "0_06_to_0_08",
            "0_08_to_0_10",
            "0_10_to_0_12",
            "0_12_plus",
            "unknown",
        ),
    }
    order_map = {label: index for index, label in enumerate(orders[bucket_type])}
    return tuple(sorted(labels, key=lambda label: order_map.get(label, len(order_map))))


def cast_bucket_fn(
    fn: Callable[[GuardVariantEquityRow], str],
) -> Callable[[GuardVariantEquityRow], str]:
    return fn
