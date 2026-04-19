from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from horse_bet_lab.config import BetSetDiffAnalysisConfig


@dataclass(frozen=True)
class SelectedBetRow:
    model_label: str
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
    threshold: float
    min_win_odds: float | None
    max_win_odds: float | None
    min_place_basis_odds: float | None
    max_place_basis_odds: float | None
    min_popularity: int | None
    max_popularity: int | None


@dataclass(frozen=True)
class BetSetDiffSummaryRow:
    comparison_label: str
    set_group: str
    bet_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    avg_payout: float
    avg_edge_baseline: float | None
    avg_edge_variant: float | None
    avg_win_odds: float | None
    avg_place_basis_odds: float | None
    avg_popularity: float | None


@dataclass(frozen=True)
class BetSetDiffDistributionRow:
    comparison_label: str
    set_group: str
    metric: str
    bucket_label: str
    count: int
    share: float


@dataclass(frozen=True)
class BetSetDiffRaceExampleRow:
    comparison_label: str
    race_key: str
    baseline_only_count: int
    variant_only_count: int
    baseline_only_horse_numbers: str
    variant_only_horse_numbers: str
    baseline_only_avg_edge: float | None
    variant_only_avg_edge: float | None
    baseline_only_avg_win_odds: float | None
    variant_only_avg_win_odds: float | None
    baseline_only_avg_place_basis_odds: float | None
    variant_only_avg_place_basis_odds: float | None


@dataclass(frozen=True)
class BetSetDiffResult:
    output_dir: Path
    summaries: tuple[BetSetDiffSummaryRow, ...]
    distributions: tuple[BetSetDiffDistributionRow, ...]
    race_examples: tuple[BetSetDiffRaceExampleRow, ...]


def analyze_bet_set_diff(config: BetSetDiffAnalysisConfig) -> BetSetDiffResult:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(config.duckdb_path))
    try:
        summaries: list[BetSetDiffSummaryRow] = []
        distributions: list[BetSetDiffDistributionRow] = []
        race_examples: list[BetSetDiffRaceExampleRow] = []
        for comparison in config.comparisons:
            baseline_rows = load_selected_bet_rows(
                connection=connection,
                config=config,
                feature_set_name=comparison.baseline_feature_set,
            )
            variant_rows = load_selected_bet_rows(
                connection=connection,
                config=config,
                feature_set_name=comparison.variant_feature_set,
            )
            comparison_label = (
                f"{comparison.baseline_feature_set}__vs__{comparison.variant_feature_set}"
            )
            grouped = build_diff_groups(baseline_rows, variant_rows)
            summaries.extend(
                build_summary_rows(
                    comparison_label=comparison_label,
                    grouped=grouped,
                    stake_per_bet=config.stake_per_bet,
                ),
            )
            distributions.extend(
                build_distribution_rows(
                    comparison_label=comparison_label,
                    grouped=grouped,
                ),
            )
            race_examples.extend(
                build_race_example_rows(
                    comparison_label=comparison_label,
                    baseline_only=grouped["baseline_only"],
                    variant_only=grouped["variant_only"],
                ),
            )
    finally:
        connection.close()

    result = BetSetDiffResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        distributions=tuple(distributions),
        race_examples=tuple(race_examples),
    )
    write_summary_csv(config.output_dir / "summary.csv", result.summaries)
    write_summary_json(config.output_dir / "summary.json", result.summaries)
    write_distribution_csv(config.output_dir / "distribution.csv", result.distributions)
    write_distribution_json(config.output_dir / "distribution.json", result.distributions)
    write_race_examples_csv(config.output_dir / "race_examples.csv", result.race_examples)
    write_race_examples_json(config.output_dir / "race_examples.json", result.race_examples)
    return result


def load_selected_bet_rows(
    *,
    connection: duckdb.DuckDBPyConnection,
    config: BetSetDiffAnalysisConfig,
    feature_set_name: str,
) -> tuple[SelectedBetRow, ...]:
    backtest_dir = config.comparison_root_dir / feature_set_name / "backtest"
    predictions_path = backtest_dir / "rolling_predictions.csv"
    selected_summary_path = backtest_dir / "selected_summary.csv"
    selection_rows = load_selected_test_rows(selected_summary_path)
    if not selection_rows:
        return ()
    prediction_table = f"prediction_rows_{feature_set_name.replace('-', '_')}"
    connection.execute(f"DROP TABLE IF EXISTS {prediction_table}")
    connection.execute(
        f"""
        CREATE TEMP TABLE {prediction_table} AS
        SELECT * FROM read_csv_auto(?)
        """,
        [str(predictions_path)],
    )

    selected_rows: list[SelectedBetRow] = []
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
    market_prob_expression = build_market_prob_expression(config.market_prob_method)
    for selection in selection_rows:
        selection_expression = build_selection_expression(config.selection_metric)
        market_conditions = build_market_conditions(
            min_win_odds=selection.min_win_odds,
            max_win_odds=selection.max_win_odds,
            min_place_basis_odds=selection.min_place_basis_odds,
            max_place_basis_odds=selection.max_place_basis_odds,
            min_popularity=selection.min_popularity,
            max_popularity=selection.max_popularity,
        )
        parameters: list[object] = [selection.test_window_label, "test", selection.threshold]
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
                    p.window_label,
                    TRY_CAST(s.win_odds AS DOUBLE) AS win_odds,
                    s.popularity AS popularity,
                    o.place_basis_odds,
                    CASE
                        WHEN TRY_CAST(s.win_odds AS DOUBLE) IS NOT NULL
                            AND TRY_CAST(s.win_odds AS DOUBLE) > 0.0
                        THEN 1.0 / TRY_CAST(s.win_odds AS DOUBLE)
                        ELSE NULL
                    END AS inverse_win_odds
                FROM {prediction_table} p
                INNER JOIN jrdb_sed_staging s
                    ON p.race_key = s.race_key
                    AND p.horse_number = s.horse_number
                LEFT JOIN jrdb_oz_staging o
                    ON p.race_key = o.race_key
                    AND p.horse_number = o.horse_number
                WHERE p.window_label = ?
                    AND p.{config.split_column} = ?
            ),
            scored AS (
                SELECT
                    *,
                    {market_prob_expression} AS market_prob,
                    CASE
                        WHEN {market_prob_expression} IS NOT NULL
                        THEN pred_probability - {market_prob_expression}
                        ELSE NULL
                    END AS edge
                FROM base_rows
            ),
            adopted AS (
                SELECT *
                FROM scored
                WHERE {selection_expression}
                    {market_conditions}
            )
            SELECT
                ? AS model_label,
                ? AS test_window_label,
                a.race_key,
                a.horse_number,
                a.target_value,
                a.pred_probability,
                a.market_prob,
                a.edge,
                a.win_odds,
                a.popularity,
                a.place_basis_odds,
                j.place_payout
            FROM adopted a
            LEFT JOIN {payout_sql} j
                ON a.race_key = j.race_key
                AND a.horse_number = j.horse_number
            ORDER BY a.race_key, a.horse_number
            """,
            [
                *parameters,
                feature_set_name,
                selection.test_window_label,
            ],
        ).fetchall()
        selected_rows.extend(
            SelectedBetRow(
                model_label=str(row[0]),
                test_window_label=str(row[1]),
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
    return tuple(selected_rows)


def load_selected_test_rows(path: Path) -> tuple[SelectedConditionRow, ...]:
    rows: list[SelectedConditionRow] = []
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["applied_to_split"] != "test":
                continue
            rows.append(
                SelectedConditionRow(
                    window_label=row["window_label"],
                    test_window_label=row["test_window_label"] or row["window_label"],
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


def build_selection_expression(selection_metric: str) -> str:
    if selection_metric == "probability":
        return "pred_probability >= ?"
    return "edge >= ?"


def build_market_prob_expression(market_prob_method: str) -> str:
    if market_prob_method == "inverse_win_odds":
        return "inverse_win_odds"
    if market_prob_method == "oz_place_basis_inverse":
        return (
            "CASE "
            "WHEN place_basis_odds IS NOT NULL AND place_basis_odds > 0.0 "
            "THEN 1.0 / place_basis_odds "
            "ELSE NULL END"
        )
    return (
        "CASE "
        "WHEN inverse_win_odds IS NOT NULL "
        "AND SUM(inverse_win_odds) OVER (PARTITION BY race_key) > 0.0 "
        "THEN inverse_win_odds / SUM(inverse_win_odds) OVER (PARTITION BY race_key) "
        "ELSE NULL END"
    )


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
        conditions.append("AND win_odds >= ?")
    if max_win_odds is not None:
        conditions.append("AND win_odds <= ?")
    if min_place_basis_odds is not None:
        conditions.append("AND place_basis_odds >= ?")
    if max_place_basis_odds is not None:
        conditions.append("AND place_basis_odds <= ?")
    if min_popularity is not None:
        conditions.append("AND popularity >= ?")
    if max_popularity is not None:
        conditions.append("AND popularity <= ?")
    if not conditions:
        return ""
    return "\n                    " + "\n                    ".join(conditions)


def build_diff_groups(
    baseline_rows: tuple[SelectedBetRow, ...],
    variant_rows: tuple[SelectedBetRow, ...],
) -> dict[str, tuple[SelectedBetRow, ...]]:
    baseline_map = {identity_key(row): row for row in baseline_rows}
    variant_map = {identity_key(row): row for row in variant_rows}
    common_keys = tuple(sorted(set(baseline_map) & set(variant_map)))
    baseline_only_keys = tuple(sorted(set(baseline_map) - set(variant_map)))
    variant_only_keys = tuple(sorted(set(variant_map) - set(baseline_map)))
    return {
        "common_baseline": tuple(baseline_map[key] for key in common_keys),
        "common_variant": tuple(variant_map[key] for key in common_keys),
        "baseline_only": tuple(baseline_map[key] for key in baseline_only_keys),
        "variant_only": tuple(variant_map[key] for key in variant_only_keys),
    }


def build_summary_rows(
    *,
    comparison_label: str,
    grouped: dict[str, tuple[SelectedBetRow, ...]],
    stake_per_bet: float,
) -> tuple[BetSetDiffSummaryRow, ...]:
    common_baseline = grouped["common_baseline"]
    common_variant = grouped["common_variant"]
    baseline_only = grouped["baseline_only"]
    variant_only = grouped["variant_only"]
    return (
        summary_row(
            comparison_label=comparison_label,
            set_group="common",
            rows=common_baseline,
            stake_per_bet=stake_per_bet,
            avg_edge_baseline=average_edge(common_baseline),
            avg_edge_variant=average_edge(common_variant),
        ),
        summary_row(
            comparison_label=comparison_label,
            set_group="baseline_only",
            rows=baseline_only,
            stake_per_bet=stake_per_bet,
            avg_edge_baseline=average_edge(baseline_only),
            avg_edge_variant=None,
        ),
        summary_row(
            comparison_label=comparison_label,
            set_group="variant_only",
            rows=variant_only,
            stake_per_bet=stake_per_bet,
            avg_edge_baseline=None,
            avg_edge_variant=average_edge(variant_only),
        ),
    )


def summary_row(
    *,
    comparison_label: str,
    set_group: str,
    rows: tuple[SelectedBetRow, ...],
    stake_per_bet: float,
    avg_edge_baseline: float | None,
    avg_edge_variant: float | None,
) -> BetSetDiffSummaryRow:
    bet_count = len(rows)
    hit_rows = tuple(row for row in rows if row.place_payout is not None)
    hit_count = len(hit_rows)
    total_return = sum(row.place_payout or 0.0 for row in rows)
    total_profit = total_return - (bet_count * stake_per_bet)
    return BetSetDiffSummaryRow(
        comparison_label=comparison_label,
        set_group=set_group,
        bet_count=bet_count,
        hit_count=hit_count,
        hit_rate=(hit_count / bet_count) if bet_count > 0 else 0.0,
        roi=(total_return / (bet_count * stake_per_bet)) if bet_count > 0 else 0.0,
        total_profit=total_profit,
        avg_payout=(
            sum(row.place_payout or 0.0 for row in hit_rows) / hit_count
            if hit_count > 0
            else 0.0
        ),
        avg_edge_baseline=avg_edge_baseline,
        avg_edge_variant=avg_edge_variant,
        avg_win_odds=average_optional_float(tuple(row.win_odds for row in rows)),
        avg_place_basis_odds=average_optional_float(tuple(row.place_basis_odds for row in rows)),
        avg_popularity=average_optional_float(
            tuple(float(row.popularity) if row.popularity is not None else None for row in rows),
        ),
    )


def build_distribution_rows(
    *,
    comparison_label: str,
    grouped: dict[str, tuple[SelectedBetRow, ...]],
) -> tuple[BetSetDiffDistributionRow, ...]:
    rows: list[BetSetDiffDistributionRow] = []
    named_groups = {
        "common": grouped["common_baseline"],
        "baseline_only": grouped["baseline_only"],
        "variant_only": grouped["variant_only"],
    }
    for set_group, group_rows in named_groups.items():
        rows.extend(
            bucket_distribution_rows(
                comparison_label=comparison_label,
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
                comparison_label=comparison_label,
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
                comparison_label=comparison_label,
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
                comparison_label=comparison_label,
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
    comparison_label: str,
    baseline_only: tuple[SelectedBetRow, ...],
    variant_only: tuple[SelectedBetRow, ...],
) -> tuple[BetSetDiffRaceExampleRow, ...]:
    baseline_by_race: dict[str, list[SelectedBetRow]] = {}
    variant_by_race: dict[str, list[SelectedBetRow]] = {}
    for row in baseline_only:
        baseline_by_race.setdefault(row.race_key, []).append(row)
    for row in variant_only:
        variant_by_race.setdefault(row.race_key, []).append(row)
    shared_races = sorted(set(baseline_by_race) & set(variant_by_race))
    examples: list[BetSetDiffRaceExampleRow] = []
    for race_key in shared_races[:10]:
        baseline_rows = tuple(sorted(baseline_by_race[race_key], key=lambda row: row.horse_number))
        variant_rows = tuple(sorted(variant_by_race[race_key], key=lambda row: row.horse_number))
        examples.append(
            BetSetDiffRaceExampleRow(
                comparison_label=comparison_label,
                race_key=race_key,
                baseline_only_count=len(baseline_rows),
                variant_only_count=len(variant_rows),
                baseline_only_horse_numbers=",".join(
                    str(row.horse_number) for row in baseline_rows
                ),
                variant_only_horse_numbers=",".join(str(row.horse_number) for row in variant_rows),
                baseline_only_avg_edge=average_edge(baseline_rows),
                variant_only_avg_edge=average_edge(variant_rows),
                baseline_only_avg_win_odds=average_optional_float(
                    tuple(row.win_odds for row in baseline_rows),
                ),
                variant_only_avg_win_odds=average_optional_float(
                    tuple(row.win_odds for row in variant_rows),
                ),
                baseline_only_avg_place_basis_odds=average_optional_float(
                    tuple(row.place_basis_odds for row in baseline_rows),
                ),
                variant_only_avg_place_basis_odds=average_optional_float(
                    tuple(row.place_basis_odds for row in variant_rows),
                ),
            ),
        )
    return tuple(examples)


def bucket_distribution_rows(
    *,
    comparison_label: str,
    set_group: str,
    metric: str,
    values: tuple[float | None, ...],
    buckets: tuple[tuple[float, str], ...],
) -> tuple[BetSetDiffDistributionRow, ...]:
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
        BetSetDiffDistributionRow(
            comparison_label=comparison_label,
            set_group=set_group,
            metric=metric,
            bucket_label=label,
            count=count,
            share=(count / total) if total > 0 else 0.0,
        )
        for label, count in counts.items()
    )


def identity_key(row: SelectedBetRow) -> tuple[str, str, int]:
    return (row.test_window_label, row.race_key, row.horse_number)


def average_edge(rows: tuple[SelectedBetRow, ...]) -> float | None:
    return average_optional_float(tuple(row.edge for row in rows))


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


def write_summary_csv(path: Path, rows: tuple[BetSetDiffSummaryRow, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=tuple(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_summary_json(path: Path, rows: tuple[BetSetDiffSummaryRow, ...]) -> None:
    payload = {"analysis": {"rows": [asdict(row) for row in rows]}}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_distribution_csv(path: Path, rows: tuple[BetSetDiffDistributionRow, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=tuple(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_distribution_json(path: Path, rows: tuple[BetSetDiffDistributionRow, ...]) -> None:
    payload = {"analysis": {"rows": [asdict(row) for row in rows]}}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_race_examples_csv(path: Path, rows: tuple[BetSetDiffRaceExampleRow, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        fieldnames = tuple(BetSetDiffRaceExampleRow.__dataclass_fields__.keys())
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def write_race_examples_json(path: Path, rows: tuple[BetSetDiffRaceExampleRow, ...]) -> None:
    payload = {"analysis": {"rows": [asdict(row) for row in rows]}}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
