from __future__ import annotations

from dataclasses import dataclass

SUPPORTED_TARGETS = ("workout_gap_days", "is_place", "is_win")


@dataclass(frozen=True)
class TargetDefinition:
    join_sql: str
    expression_sql: str
    filter_sql: str = "TRUE"


def target_definition(target_name: str) -> TargetDefinition:
    if target_name == "workout_gap_days":
        return TargetDefinition(
            join_sql="",
            expression_sql="date_diff('day', c.workout_date, b.race_date)",
        )
    if target_name == "is_place":
        return TargetDefinition(
            join_sql=(
                "INNER JOIN jrdb_sed_staging r "
                "ON c.race_key = r.race_key AND c.horse_number = r.horse_number"
            ),
            expression_sql="CASE WHEN r.finish_position <= 3 THEN 1 ELSE 0 END",
            filter_sql="r.finish_position BETWEEN 1 AND 18",
        )
    if target_name == "is_win":
        return TargetDefinition(
            join_sql=(
                "INNER JOIN jrdb_sed_staging r "
                "ON c.race_key = r.race_key AND c.horse_number = r.horse_number"
            ),
            expression_sql="CASE WHEN r.finish_position = 1 THEN 1 ELSE 0 END",
            filter_sql="r.finish_position BETWEEN 1 AND 18",
        )
    raise KeyError(target_name)


def result_target_expression_sql(target_name: str, *, table_alias: str = "r") -> str:
    if target_name == "is_place":
        return f"CASE WHEN {table_alias}.finish_position <= 3 THEN 1 ELSE 0 END"
    if target_name == "is_win":
        return f"CASE WHEN {table_alias}.finish_position = 1 THEN 1 ELSE 0 END"
    raise KeyError(target_name)


def result_target_filter_sql(target_name: str, *, table_alias: str = "r") -> str:
    if target_name in {"is_place", "is_win"}:
        return f"{table_alias}.finish_position BETWEEN 1 AND 18"
    raise KeyError(target_name)
