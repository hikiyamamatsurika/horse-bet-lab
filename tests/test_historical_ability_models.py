from __future__ import annotations

import csv
import json
from datetime import date, timedelta
from pathlib import Path

import duckdb
import numpy as np

from horse_bet_lab.research.historical_ability_models import (
    HISTORY_NUMERIC_FEATURE_COLUMNS,
    InputAudit,
    constrain_probabilities_by_race,
    load_comparison_dataset,
    place_slots_for_field_size,
    run_model_comparison,
    write_comparison_result,
)
from horse_bet_lab.research.historical_ability_source import READY_VERDICT


def test_place_slots_use_active_field_size() -> None:
    assert place_slots_for_field_size(5) == 2
    assert place_slots_for_field_size(7) == 2
    assert place_slots_for_field_size(8) == 3
    assert place_slots_for_field_size(18) == 3


def test_race_constraint_matches_place_slot_sum() -> None:
    probabilities = np.asarray([0.1, 0.2, 0.3, 0.4, 0.2, 0.2, 0.2, 0.2], dtype=np.float64)
    race_keys = np.asarray(["a"] * 4 + ["b"] * 4, dtype=object)
    place_slots = np.asarray([2] * 8, dtype=np.int64)

    constrained = constrain_probabilities_by_race(probabilities, race_keys, place_slots)

    assert abs(float(constrained[:4].sum()) - 2.0) < 1e-9
    assert abs(float(constrained[4:].sum()) - 2.0) < 1e-9
    assert np.all((constrained > 0.0) & (constrained < 1.0))


def test_loader_uses_place_slot_target_and_audits_old_market_target(tmp_path: Path) -> None:
    history_path, summary_path, market_path = _write_inputs(
        tmp_path,
        years=(2023,),
        races_per_year=1,
        field_size=7,
        force_market_third_positive=True,
    )

    dataset, audit = load_comparison_dataset(
        history_surface_path=history_path,
        source_summary_path=summary_path,
        market_dataset_path=market_path,
    )

    assert dataset.targets.sum() == 2
    assert dataset.market_targets.sum() == 3
    assert audit.existing_market_target_mismatch_count == 1
    assert audit.mismatch_direction_counts == {"history_0_market_1": 1}
    assert audit.mismatch_by_active_field_size == {"7": 1}


def test_model_comparison_runs_fixed_chronological_contract(tmp_path: Path) -> None:
    history_path, summary_path, market_path = _write_inputs(
        tmp_path,
        years=(2023, 2024, 2025),
        races_per_year=18,
        field_size=8,
    )
    dataset, audit = load_comparison_dataset(
        history_surface_path=history_path,
        source_summary_path=summary_path,
        market_dataset_path=market_path,
    )

    result = run_model_comparison(
        dataset,
        audit,
        c_grid=(0.1, 1.0),
        bootstrap_repetitions=100,
    )

    assert {row["model_name"] for row in result.validation_metrics} == {
        "M0_race_prior",
        "M1_market",
        "M1C_race_constrained_market",
        "M2_history",
        "M3_market_history",
        "M4_market_offset_history",
        "M5_race_constrained_offset",
    }
    assert {row["model_name"] for row in result.holdout_metrics} == {
        "M0_race_prior",
        "M1_market",
        "M1C_race_constrained_market",
        "M2_history",
        "M3_market_history",
        "M4_market_offset_history",
        "M5_race_constrained_offset",
    }
    assert result.summary["split_contract"]["one_time_holdout"] == 2025
    assert result.summary["roi_or_betting_used_for_selection"] is False
    assert len(result.bootstrap_rows) == 10
    assert any(
        row["baseline_name"] == "M1C_race_constrained_market"
        and row["candidate_name"] == "M5_race_constrained_offset"
        for row in result.bootstrap_rows
    )


def test_result_writer_emits_only_probability_comparison_outputs(tmp_path: Path) -> None:
    audit = InputAudit(
        source_verdict=READY_VERDICT,
        history_row_count=1,
        market_row_count=1,
        joined_row_count=1,
        missing_history_row_count=0,
        missing_history_race_count=0,
        partially_joined_race_count=0,
        fully_missing_unsupported_race_count=0,
        duplicate_history_identity_count=0,
        duplicate_market_identity_count=0,
        existing_market_target_mismatch_count=0,
        mismatch_direction_counts={},
        mismatch_by_active_field_size={},
        joined_rows_by_year={"2023": 1},
    )
    history_path, summary_path, market_path = _write_inputs(
        tmp_path,
        years=(2023, 2024, 2025),
        races_per_year=12,
        field_size=8,
    )
    dataset, _ = load_comparison_dataset(
        history_surface_path=history_path,
        source_summary_path=summary_path,
        market_dataset_path=market_path,
    )
    result = run_model_comparison(
        dataset,
        audit,
        c_grid=(0.3,),
        bootstrap_repetitions=20,
    )
    output_dir = tmp_path / "reports"

    write_comparison_result(result, output_dir)

    assert {path.name for path in output_dir.iterdir()} == {
        "phase651_summary.json",
        "phase651_input_audit.json",
        "phase651_validation_metrics.csv",
        "phase651_holdout_metrics.csv",
        "phase651_hyperparameters.csv",
        "phase651_bootstrap.csv",
        "phase651_findings.md",
    }
    assert not any("roi" in path.name or "bet" in path.name for path in output_dir.iterdir())


def _write_inputs(
    root: Path,
    *,
    years: tuple[int, ...],
    races_per_year: int,
    field_size: int,
    force_market_third_positive: bool = False,
) -> tuple[Path, Path, Path]:
    history_path = root / "history.csv"
    summary_path = root / "summary.json"
    market_path = root / "market.parquet"
    history_columns = [
        "race_date",
        "race_key",
        "horse_number",
        "is_place",
        "venue_code",
        *HISTORY_NUMERIC_FEATURE_COLUMNS,
    ]
    history_rows: list[dict[str, object]] = []
    market_rows: list[tuple[object, ...]] = []
    for year in years:
        for race_index in range(races_per_year):
            race_date = date(year, 1, 1) + timedelta(days=race_index)
            race_key = f"{year}{race_index:04d}"
            horse_values: list[tuple[int, float, float]] = []
            for horse_number in range(1, field_size + 1):
                history_signal = ((horse_number * 7 + race_index * 3) % field_size) / field_size
                market_signal = ((horse_number * 5 + race_index) % field_size) / field_size
                latent = 2.2 * history_signal + 0.7 * market_signal
                horse_values.append((horse_number, latent, market_signal))
            placed = {
                horse_number
                for horse_number, _, _ in sorted(
                    horse_values,
                    key=lambda value: value[1],
                    reverse=True,
                )[: place_slots_for_field_size(field_size)]
            }
            for horse_number, _, market_signal in horse_values:
                target = int(horse_number in placed)
                old_market_target = target
                if force_market_third_positive and horse_number == 3:
                    old_market_target = 1
                prior_count = (horse_number + race_index) % 12
                history_row: dict[str, object] = {
                    "race_date": race_date.isoformat(),
                    "race_key": race_key,
                    "horse_number": horse_number,
                    "is_place": target,
                    "venue_code": f"{1 + race_index % 3:02d}",
                    "current_distance_m": 1600 + 200 * (race_index % 3),
                    "card_field_size": field_size,
                    "prior_start_count": prior_count,
                    "days_since_last_start": 20 + horse_number if prior_count else "",
                    "last_1_finish_percentile": (horse_number * 7 + race_index * 3)
                    % field_size
                    / field_size
                    if prior_count
                    else "",
                    "last_3_mean_finish_percentile": (horse_number * 7 + race_index * 3)
                    % field_size
                    / field_size
                    if prior_count
                    else "",
                    "last_5_mean_finish_percentile": (horse_number * 7 + race_index * 3)
                    % field_size
                    / field_size
                    if prior_count
                    else "",
                    "last_3_top3_rate": (horse_number * 7 + race_index * 3)
                    % field_size
                    / field_size
                    if prior_count
                    else "",
                    "last_5_top3_rate": (horse_number * 7 + race_index * 3)
                    % field_size
                    / field_size
                    if prior_count
                    else "",
                    "last_5_recency_weighted_form": (horse_number * 7 + race_index * 3)
                    % field_size
                    / field_size
                    if prior_count
                    else "",
                    "last_5_distance_compatibility_rate": 0.5 if prior_count else "",
                    "last_5_venue_compatibility_rate": 0.5 if prior_count else "",
                }
                history_rows.append(history_row)
                market_rows.append(
                    (
                        race_key,
                        horse_number,
                        race_date,
                        "train",
                        "is_place",
                        2.0 + 8.0 * (1.0 - market_signal),
                        1.2 + 3.0 * (1.0 - market_signal),
                        1 + int((1.0 - market_signal) * field_size),
                        old_market_target,
                    )
                )

    with history_path.open("w", newline="") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=history_columns)
        writer.writeheader()
        writer.writerows(history_rows)
    summary_path.write_text(
        json.dumps(
            {
                "final_verdict": READY_VERDICT,
                "model_feature_columns": [
                    "venue_code",
                    *HISTORY_NUMERIC_FEATURE_COLUMNS,
                ],
            }
        )
    )
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE market (
                race_key VARCHAR,
                horse_number INTEGER,
                race_date DATE,
                split VARCHAR,
                target_name VARCHAR,
                win_odds DOUBLE,
                place_basis_odds DOUBLE,
                popularity INTEGER,
                target_value INTEGER
            )
            """
        )
        connection.executemany("INSERT INTO market VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", market_rows)
        connection.execute("COPY market TO ? (FORMAT PARQUET)", [str(market_path)])
    finally:
        connection.close()
    return history_path, summary_path, market_path
