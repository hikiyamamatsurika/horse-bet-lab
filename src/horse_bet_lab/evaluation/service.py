from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from horse_bet_lab.config import BetCandidateEvalConfig

SPLIT_ORDER = ("train", "valid", "test")
SUMMARY_COLUMNS = (
    "split",
    "threshold",
    "candidate_count",
    "adopted_count",
    "adoption_rate",
    "hit_rate",
    "avg_prediction",
)


@dataclass(frozen=True)
class BetCandidateSummary:
    split: str
    threshold: float
    candidate_count: int
    adopted_count: int
    adoption_rate: float
    hit_rate: float
    avg_prediction: float


@dataclass(frozen=True)
class BetCandidateEvalResult:
    output_dir: Path
    summaries: tuple[BetCandidateSummary, ...]


def evaluate_bet_candidates(config: BetCandidateEvalConfig) -> BetCandidateEvalResult:
    validate_eval_config(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    try:
        summaries = tuple(build_split_summaries(connection, config))
    finally:
        connection.close()

    write_summary_csv(config.output_dir / "summary.csv", summaries)
    write_summary_json(config.output_dir / "summary.json", config, summaries)
    return BetCandidateEvalResult(output_dir=config.output_dir, summaries=summaries)


def validate_eval_config(config: BetCandidateEvalConfig) -> None:
    if not config.thresholds:
        raise ValueError("thresholds must not be empty")
    for threshold in config.thresholds:
        if not (0.0 <= threshold <= 1.0):
            raise ValueError("each threshold must be between 0.0 and 1.0")


def build_split_summaries(
    connection: duckdb.DuckDBPyConnection,
    config: BetCandidateEvalConfig,
) -> list[BetCandidateSummary]:
    summaries: list[BetCandidateSummary] = []
    for threshold in config.thresholds:
        rows = connection.execute(
            f"""
            SELECT
                {config.split_column} AS split,
                COUNT(*) AS candidate_count,
                SUM(CASE WHEN {config.probability_column} >= ? THEN 1 ELSE 0 END) AS adopted_count,
                AVG(
                    CASE WHEN {config.probability_column} >= ? THEN {config.target_column} END
                ) AS hit_rate,
                AVG(
                    CASE WHEN {config.probability_column} >= ? THEN {config.probability_column} END
                ) AS avg_prediction
            FROM read_csv_auto(?, header = true)
            GROUP BY 1
            """,
            [
                threshold,
                threshold,
                threshold,
                str(config.predictions_path),
            ],
        ).fetchall()
        summary_by_split = {
            str(row[0]): BetCandidateSummary(
                split=str(row[0]),
                threshold=threshold,
                candidate_count=int(row[1]),
                adopted_count=int(row[2] or 0),
                adoption_rate=(float(row[2] or 0) / int(row[1])) if int(row[1]) > 0 else 0.0,
                hit_rate=float(row[3]) if row[3] is not None else 0.0,
                avg_prediction=float(row[4]) if row[4] is not None else 0.0,
            )
            for row in rows
        }
        summaries.extend(
            summary_by_split[split_name]
            for split_name in SPLIT_ORDER
            if split_name in summary_by_split
        )
    return summaries


def write_summary_csv(path: Path, summaries: tuple[BetCandidateSummary, ...]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_summary_json(
    path: Path,
    config: BetCandidateEvalConfig,
    summaries: tuple[BetCandidateSummary, ...],
) -> None:
    payload = {
        "evaluation": {
            "name": config.name,
            "predictions_path": str(config.predictions_path),
            "thresholds": list(config.thresholds),
            "split_column": config.split_column,
            "target_column": config.target_column,
            "probability_column": config.probability_column,
        },
        "summaries": [asdict(summary) for summary in summaries],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
