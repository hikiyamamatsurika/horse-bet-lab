from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from horse_bet_lab.config import DatasetBuildConfig
from horse_bet_lab.dataset.targets import (
    result_target_expression_sql,
    result_target_filter_sql,
    target_definition,
)
from horse_bet_lab.features.registry import dataset_feature_columns
from horse_bet_lab.features.provenance import (
    build_feature_provenance_payload,
    dataset_model_feature_columns,
    write_feature_provenance_sidecar,
)
from horse_bet_lab.ingest.specs import CONFIRMED, SUPPORTED_FILE_SPECS, dataset_allowlist

METADATA_COLUMNS = ("race_key", "horse_number", "race_date", "split", "target_name")
TARGET_COLUMNS = ("target_value",)
TARGET_SOURCE_COLUMNS = ("finish_position", "result_date")


@dataclass(frozen=True)
class DatasetBuildSummary:
    output_path: Path
    row_count: int


def build_horse_dataset(config: DatasetBuildConfig) -> DatasetBuildSummary:
    config.output_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(config.duckdb_path))
    try:
        query = dataset_query(config)
        connection.execute(
            f"COPY ({query}) TO ? (FORMAT PARQUET)",
            [str(config.output_path)],
        )
        row_count = connection.execute(
            f"SELECT COUNT(*) FROM ({query})",
        ).fetchone()
        if row_count is None:
            raise RuntimeError("failed to count dataset rows")
        validate_built_dataset(connection, config)
        write_feature_provenance_sidecar(
            config.output_path,
            build_feature_provenance_payload(
                artifact_kind="dataset_parquet",
                generated_by="horse_bet_lab.dataset.service.build_horse_dataset",
                config_identifier=config.name,
                dataset_feature_set=config.feature_set,
                include_popularity=config.include_popularity,
                model_feature_columns=dataset_model_feature_columns(
                    config.feature_set,
                    include_popularity=config.include_popularity,
                ),
                artifact_path=str(config.output_path),
                extra={
                    "duckdb_path": str(config.duckdb_path),
                    "target_name": config.target_name,
                    "date_range": {
                        "start_date": config.start_date.isoformat(),
                        "end_date": config.end_date.isoformat(),
                        "train_end_date": (
                            config.train_end_date.isoformat()
                            if config.train_end_date is not None
                            else None
                        ),
                        "valid_end_date": (
                            config.valid_end_date.isoformat()
                            if config.valid_end_date is not None
                            else None
                        ),
                    },
                },
            ),
        )
        return DatasetBuildSummary(output_path=config.output_path, row_count=int(row_count[0]))
    finally:
        connection.close()


def dataset_query(config: DatasetBuildConfig) -> str:
    if config.feature_set == "odds_only":
        return odds_only_dataset_query(config)
    if config.feature_set == "win_market_only":
        return win_market_only_dataset_query(config)
    if config.feature_set in {
        "current_win_market",
        "place_market_only",
        "place_market_plus_popularity",
        "dual_market",
        "dual_market_for_win",
        "dual_market_plus_headcount",
        "dual_market_plus_headcount_place_slots",
        "dual_market_plus_headcount_place_slots_distance",
        "dual_market_plus_log_diff",
        "dual_market_plus_implied_probs",
        "dual_market_plus_prob_diff",
        "dual_market_plus_ratio",
    }:
        return market_feature_dataset_query(config)

    definition = target_definition(config.target_name)
    extra_joins = ()
    if config.feature_set == "market_plus_workout_minimal":
        extra_joins = (
            "LEFT JOIN jrdb_win_market_snapshot_v1 w "
            "ON c.race_key = w.race_key AND c.horse_number = w.horse_number",
        )
    select_parts = [
        "c.race_key AS race_key",
        "c.horse_number AS horse_number",
        "b.race_date AS race_date",
        f"{split_case_sql(config)} AS split",
        f"'{config.target_name}' AS target_name",
        *feature_select_parts(config),
        f"{definition.expression_sql} AS target_value",
    ]

    return f"""
        SELECT
            {", ".join(select_parts)}
        FROM jrdb_cha_staging c
        INNER JOIN jrdb_bac_staging b
            ON c.race_key = b.race_key
        {definition.join_sql}
        {" ".join(extra_joins)}
        WHERE b.race_date BETWEEN DATE '{config.start_date.isoformat()}'
            AND DATE '{config.end_date.isoformat()}'
            AND {definition.filter_sql}
        ORDER BY c.race_key, c.horse_number
    """


def odds_only_dataset_query(config: DatasetBuildConfig) -> str:
    if config.target_name not in {"is_place", "is_win"}:
        raise ValueError(
            "odds_only feature_set currently supports target_name='is_place' or 'is_win' only",
        )

    select_parts = [
        "r.race_key AS race_key",
        "r.horse_number AS horse_number",
        "r.result_date AS race_date",
        f"{split_case_sql(config, table_alias='r', date_column='result_date')} AS split",
        f"'{config.target_name}' AS target_name",
        "w.win_odds AS win_odds",
    ]
    if config.include_popularity:
        sed_columns = selected_columns("SED", allowed_statuses=(CONFIRMED, "provisional"))
        select_parts.append(f"r.{sed_columns['popularity']} AS popularity")
    select_parts.append(
        f"{result_target_expression_sql(config.target_name, table_alias='r')} AS target_value",
    )

    return f"""
        SELECT
            {", ".join(select_parts)}
        FROM jrdb_sed_staging r
        LEFT JOIN jrdb_win_market_snapshot_v1 w
            ON r.race_key = w.race_key
            AND r.horse_number = w.horse_number
        WHERE r.result_date BETWEEN DATE '{config.start_date.isoformat()}'
            AND DATE '{config.end_date.isoformat()}'
            AND {result_target_filter_sql(config.target_name, table_alias='r')}
        ORDER BY r.race_key, r.horse_number
    """


def win_market_only_dataset_query(config: DatasetBuildConfig) -> str:
    if config.target_name not in {"is_place", "is_win"}:
        raise ValueError(
            "win_market_only currently supports target_name='is_place' or 'is_win' only",
        )

    sed_columns = selected_columns("SED", allowed_statuses=(CONFIRMED, "provisional"))
    return f"""
        SELECT
            r.race_key AS race_key,
            r.horse_number AS horse_number,
            r.result_date AS race_date,
            {split_case_sql(config, table_alias='r', date_column='result_date')} AS split,
            '{config.target_name}' AS target_name,
            w.win_odds AS win_odds,
            r.{sed_columns['popularity']} AS popularity,
            {result_target_expression_sql(config.target_name, table_alias='r')} AS target_value
        FROM jrdb_sed_staging r
        LEFT JOIN jrdb_win_market_snapshot_v1 w
            ON r.race_key = w.race_key
            AND r.horse_number = w.horse_number
        WHERE r.result_date BETWEEN DATE '{config.start_date.isoformat()}'
            AND DATE '{config.end_date.isoformat()}'
            AND {result_target_filter_sql(config.target_name, table_alias='r')}
        ORDER BY r.race_key, r.horse_number
    """


def market_feature_dataset_query(config: DatasetBuildConfig) -> str:
    if config.target_name not in {"is_place", "is_win"}:
        raise ValueError(
            "market feature sets currently support target_name='is_place' or 'is_win' only",
        )

    sed_columns = selected_columns("SED", allowed_statuses=(CONFIRMED, "provisional"))
    oz_columns = selected_columns("OZ", allowed_statuses=(CONFIRMED, "provisional"))
    select_parts = [
        "r.race_key AS race_key",
        "r.horse_number AS horse_number",
        "r.result_date AS race_date",
        f"{split_case_sql(config, table_alias='r', date_column='result_date')} AS split",
        f"'{config.target_name}' AS target_name",
        *market_feature_select_parts(config.feature_set, sed_columns, oz_columns),
        f"{result_target_expression_sql(config.target_name, table_alias='r')} AS target_value",
    ]

    return f"""
        SELECT
            {", ".join(select_parts)}
        FROM jrdb_sed_staging r
        LEFT JOIN jrdb_win_market_snapshot_v1 w
            ON r.race_key = w.race_key
            AND r.horse_number = w.horse_number
        INNER JOIN jrdb_oz_staging o
            ON r.race_key = o.race_key
            AND r.horse_number = o.horse_number
        WHERE r.result_date BETWEEN DATE '{config.start_date.isoformat()}'
            AND DATE '{config.end_date.isoformat()}'
            AND {result_target_filter_sql(config.target_name, table_alias='r')}
        ORDER BY r.race_key, r.horse_number
    """


def feature_select_parts(config: DatasetBuildConfig) -> tuple[str, ...]:
    if config.feature_set == "minimal":
        bac_columns = selected_columns("BAC", allowed_statuses=(CONFIRMED,))
        cha_columns = selected_columns("CHA", allowed_statuses=(CONFIRMED,))
        return (
            f"b.{bac_columns['distance_m']} AS distance_m",
            f"b.{bac_columns['race_name']} AS race_name",
            f"c.{cha_columns['workout_weekday']} AS workout_weekday",
            f"c.{cha_columns['workout_date']} AS workout_date",
        )
    if config.feature_set == "odds_only":
        if config.target_name not in {"is_place", "is_win"}:
            raise ValueError(
                "odds_only feature_set currently supports target_name='is_place' or 'is_win' only",
            )
        select_parts = ["w.win_odds AS win_odds"]
        if config.include_popularity:
            sed_columns = selected_columns("SED", allowed_statuses=(CONFIRMED, "provisional"))
            select_parts.append(f"r.{sed_columns['popularity']} AS popularity")
        return tuple(select_parts)
    if config.feature_set == "win_market_only":
        if config.target_name not in {"is_place", "is_win"}:
            raise ValueError(
                "win_market_only currently supports target_name='is_place' or 'is_win' only",
            )
        sed_columns = selected_columns("SED", allowed_statuses=(CONFIRMED, "provisional"))
        return (
            "w.win_odds AS win_odds",
            f"r.{sed_columns['popularity']} AS popularity",
        )
    if config.feature_set in {
        "current_win_market",
        "place_market_only",
        "place_market_plus_popularity",
        "dual_market",
        "dual_market_for_win",
        "dual_market_plus_headcount",
        "dual_market_plus_headcount_place_slots",
        "dual_market_plus_headcount_place_slots_distance",
        "dual_market_plus_log_diff",
        "dual_market_plus_implied_probs",
        "dual_market_plus_prob_diff",
        "dual_market_plus_ratio",
    }:
        sed_columns = selected_columns("SED", allowed_statuses=(CONFIRMED, "provisional"))
        oz_columns = selected_columns("OZ", allowed_statuses=(CONFIRMED, "provisional"))
        return market_feature_select_parts(config.feature_set, sed_columns, oz_columns)
    if config.feature_set == "market_plus_workout_minimal":
        if config.target_name != "is_place":
            raise ValueError(
                "market_plus_workout_minimal currently supports target_name='is_place' only",
            )
        cha_columns = selected_columns("CHA", allowed_statuses=(CONFIRMED,))
        sed_columns = selected_columns("SED", allowed_statuses=(CONFIRMED, "provisional"))
        return (
            "w.win_odds AS win_odds",
            f"r.{sed_columns['popularity']} AS popularity",
            "DATE_DIFF('day', c.workout_date, b.race_date) AS workout_gap_days",
            f"{workout_weekday_case_sql(cha_columns['workout_weekday'])} AS workout_weekday_code",
        )
    raise ValueError(f"unsupported feature_set: {config.feature_set}")


def market_feature_select_parts(
    feature_set: str,
    sed_columns: dict[str, str],
    oz_columns: dict[str, str],
) -> tuple[str, ...]:
    win_odds_sql = "w.win_odds AS win_odds"
    popularity_sql = f"r.{sed_columns['popularity']} AS popularity"
    place_basis_sql = (
        f"TRY_CAST(o.{oz_columns['place_basis_odds']} AS DOUBLE) AS place_basis_odds"
    )
    if feature_set == "current_win_market":
        return (win_odds_sql, popularity_sql)
    if feature_set == "place_market_only":
        return (place_basis_sql,)
    if feature_set == "place_market_plus_popularity":
        return (place_basis_sql, popularity_sql)
    if feature_set == "dual_market":
        return (win_odds_sql, place_basis_sql, popularity_sql)
    if feature_set == "dual_market_for_win":
        return (win_odds_sql, place_basis_sql, popularity_sql)
    if feature_set == "dual_market_plus_headcount":
        return (
            win_odds_sql,
            place_basis_sql,
            popularity_sql,
            "o.headcount AS headcount",
        )
    if feature_set == "dual_market_plus_headcount_place_slots":
        return (
            win_odds_sql,
            place_basis_sql,
            popularity_sql,
            "o.headcount AS headcount",
            (
                "CASE "
                "WHEN o.headcount <= 7 THEN 2 "
                "WHEN o.headcount IS NULL THEN NULL "
                "ELSE 3 END AS place_slot_count"
            ),
        )
    if feature_set == "dual_market_plus_headcount_place_slots_distance":
        return (
            win_odds_sql,
            place_basis_sql,
            popularity_sql,
            "o.headcount AS headcount",
            (
                "CASE "
                "WHEN o.headcount <= 7 THEN 2 "
                "WHEN o.headcount IS NULL THEN NULL "
                "ELSE 3 END AS place_slot_count"
            ),
            "r.distance_m AS distance_m",
        )
    if feature_set == "dual_market_plus_log_diff":
        return (
            win_odds_sql,
            place_basis_sql,
            popularity_sql,
            (
                "LN(TRY_CAST(o.place_basis_odds AS DOUBLE)) "
                "- LN(w.win_odds) AS log_place_minus_log_win"
            ),
        )
    if feature_set == "dual_market_plus_implied_probs":
        return (
            win_odds_sql,
            place_basis_sql,
            popularity_sql,
            (
                "1.0 / NULLIF(TRY_CAST(o.place_basis_odds AS DOUBLE), 0.0) "
                "AS implied_place_prob"
            ),
            (
                "1.0 / NULLIF(w.win_odds, 0.0) "
                "AS implied_win_prob"
            ),
        )
    if feature_set == "dual_market_plus_prob_diff":
        return (
            win_odds_sql,
            place_basis_sql,
            popularity_sql,
            (
                "(1.0 / NULLIF(TRY_CAST(o.place_basis_odds AS DOUBLE), 0.0)) "
                "- (1.0 / NULLIF(w.win_odds, 0.0)) "
                "AS implied_place_prob_minus_implied_win_prob"
            ),
        )
    if feature_set == "dual_market_plus_ratio":
        return (
            win_odds_sql,
            place_basis_sql,
            popularity_sql,
            (
                "TRY_CAST(o.place_basis_odds AS DOUBLE) "
                "/ NULLIF(w.win_odds, 0.0) AS place_to_win_ratio"
            ),
        )
    raise ValueError(f"unsupported market feature_set: {feature_set}")


def processed_columns(config: DatasetBuildConfig) -> tuple[str, ...]:
    feature_columns = dataset_feature_columns(
        config.feature_set,
        include_popularity=config.include_popularity,
    )
    columns = METADATA_COLUMNS + feature_columns + TARGET_COLUMNS
    return columns


def workout_weekday_case_sql(column_name: str) -> str:
    return (
        "CASE "
        f"WHEN c.{column_name} = '月' THEN 0 "
        f"WHEN c.{column_name} = '火' THEN 1 "
        f"WHEN c.{column_name} = '水' THEN 2 "
        f"WHEN c.{column_name} = '木' THEN 3 "
        f"WHEN c.{column_name} = '金' THEN 4 "
        f"WHEN c.{column_name} = '土' THEN 5 "
        f"WHEN c.{column_name} = '日' THEN 6 "
        "ELSE NULL END"
    )


def selected_columns(
    file_kind: str,
    *,
    allowed_statuses: tuple[str, ...],
) -> dict[str, str]:
    allowlisted = set(dataset_allowlist(file_kind))
    for spec in SUPPORTED_FILE_SPECS:
        if spec.file_kind != file_kind:
            continue
        return {
            column.name: column.name
            for column in spec.columns
            if column.name in allowlisted and column.contract_status in allowed_statuses
        }
    raise KeyError(file_kind)


def split_case_sql(
    config: DatasetBuildConfig,
    *,
    table_alias: str = "b",
    date_column: str = "race_date",
) -> str:
    if config.train_end_date is None or config.valid_end_date is None:
        return "'train'"
    train_end_date = config.train_end_date.isoformat()
    valid_end_date = config.valid_end_date.isoformat()
    return (
        "CASE "
        f"WHEN {table_alias}.{date_column} <= DATE '{train_end_date}' THEN 'train' "
        f"WHEN {table_alias}.{date_column} <= DATE '{valid_end_date}' THEN 'valid' "
        "ELSE 'test' END"
    )


def validate_built_dataset(
    connection: duckdb.DuckDBPyConnection,
    config: DatasetBuildConfig,
) -> None:
    validate_split_config(config)
    validate_dataset_schema(connection, config)
    validate_dataset_splits(connection, config)


def validate_split_config(config: DatasetBuildConfig) -> None:
    if config.train_end_date is None and config.valid_end_date is None:
        return
    if config.train_end_date is None or config.valid_end_date is None:
        raise ValueError("train_end_date and valid_end_date must be set together")
    if not (config.start_date <= config.train_end_date < config.valid_end_date <= config.end_date):
        raise ValueError(
            "split dates must satisfy start_date <= train_end_date < valid_end_date <= end_date",
        )


def validate_dataset_schema(
    connection: duckdb.DuckDBPyConnection,
    config: DatasetBuildConfig,
) -> None:
    rows = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [str(config.output_path)],
    ).fetchall()
    actual_columns = tuple(row[0] for row in rows)
    expected_columns = processed_columns(config)
    if actual_columns != expected_columns:
        raise ValueError(f"unexpected dataset columns: {actual_columns}")
    forbidden_in_schema = sorted(set(actual_columns) & set(TARGET_SOURCE_COLUMNS))
    if forbidden_in_schema:
        raise ValueError(f"result-only columns leaked into dataset schema: {forbidden_in_schema}")


def validate_dataset_splits(
    connection: duckdb.DuckDBPyConnection,
    config: DatasetBuildConfig,
) -> None:
    race_key_cross_split = connection.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT race_key
            FROM read_parquet(?)
            GROUP BY race_key
            HAVING COUNT(DISTINCT split) > 1
        )
        """,
        [str(config.output_path)],
    ).fetchone()
    if race_key_cross_split is None or int(race_key_cross_split[0]) != 0:
        raise ValueError("race_key spans multiple splits")

    if config.train_end_date is None or config.valid_end_date is None:
        return

    split_violations = connection.execute(
        """
        SELECT COUNT(*)
        FROM read_parquet(?)
        WHERE
            (split = 'train' AND race_date > ?)
            OR (split = 'valid' AND (race_date <= ? OR race_date > ?))
            OR (split = 'test' AND race_date <= ?)
        """,
        [
            str(config.output_path),
            config.train_end_date,
            config.train_end_date,
            config.valid_end_date,
            config.valid_end_date,
        ],
    ).fetchone()
    if split_violations is None or int(split_violations[0]) != 0:
        raise ValueError("dataset contains split/date leakage")
