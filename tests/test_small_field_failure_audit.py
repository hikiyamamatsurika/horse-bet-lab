from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np

from horse_bet_lab.research.historical_ability_models import (
    ComparisonDataset,
    place_slots_for_field_size,
)
from horse_bet_lab.research.small_field_failure_audit import (
    field_group_masks,
    run_small_field_failure_audit,
    write_small_field_audit,
)


def test_field_group_masks_separate_exact_sizes_bands_and_place_slots() -> None:
    dataset = _synthetic_mixed_field_dataset(races_per_year=6, years=(2023,))
    masks = {(dimension, name): mask for dimension, name, mask in field_group_masks(dataset)}

    assert masks[("exact_field_size", "5")].sum() == 0
    assert masks[("exact_field_size", "6")].sum() > 0
    assert masks[("exact_field_size", "7")].sum() == 0
    assert masks[("field_size_band", "5-7")].sum() == masks[("place_slots", "2")].sum()
    assert masks[("field_size_band", "13+")].sum() > 0
    assert masks[("place_slots", "3")].sum() > 0
    assert masks[("small_field_prior_starts", "0")].sum() > 0


def test_small_field_audit_keeps_diagnostic_boundary() -> None:
    dataset = _synthetic_mixed_field_dataset(races_per_year=9)

    result = run_small_field_failure_audit(
        dataset,
        crossfit_folds=2,
        bootstrap_repetitions=20,
        minimum_bootstrap_races=2,
    )

    assert result.summary["period_contract"]["2025"].startswith("reused confirmation")
    assert result.summary["exclusion_rule_created"] is False
    assert result.summary["model_or_hyperparameter_selected"] is False
    assert result.summary["roi_or_betting_used"] is False
    assert "supported_nested_history_harm" in result.summary
    assert "stable_slot2_specific_recovery_pairs" in result.summary
    assert "slot2_supported_better_than_pooled_rows" in result.summary
    assert len(result.field_performance_rows) == 96
    assert len(result.decomposition_rows) == 96
    assert len(result.history_profile_rows) == 24
    assert len(result.slot2_training_rows) == 16
    constrained_context = [
        row
        for row in result.field_performance_rows
        if row["candidate_name"] == "M5_history_only" and row["group_dimension"] == "place_slots"
    ]
    assert constrained_context


def test_context_only_common_shift_disappears_after_race_constraint() -> None:
    dataset = _synthetic_mixed_field_dataset(races_per_year=9)
    result = run_small_field_failure_audit(
        dataset,
        crossfit_folds=2,
        bootstrap_repetitions=10,
        minimum_bootstrap_races=2,
    )

    rows = [
        row
        for row in result.decomposition_rows
        if row["candidate_name"] == "M5_context_only" and int(row["row_count"]) > 0
    ]
    assert rows
    assert all(float(row["mean_absolute_probability_shift"]) < 1e-12 for row in rows)


def test_writer_emits_only_diagnostic_outputs(tmp_path: Path) -> None:
    dataset = _synthetic_mixed_field_dataset(races_per_year=6)
    result = run_small_field_failure_audit(
        dataset,
        crossfit_folds=2,
        bootstrap_repetitions=10,
        minimum_bootstrap_races=2,
    )
    output_dir = tmp_path / "reports"

    write_small_field_audit(result, output_dir)

    assert {path.name for path in output_dir.iterdir()} == {
        "phase653_summary.json",
        "phase653_field_performance.csv",
        "phase653_error_decomposition.csv",
        "phase653_history_profile.csv",
        "phase653_slot2_training_diagnostic.csv",
        "phase653_findings.md",
    }
    assert not any(
        token in path.name
        for path in output_dir.iterdir()
        for token in ("roi", "bet", "candidate", "exclusion")
    )


def _synthetic_mixed_field_dataset(
    *,
    races_per_year: int,
    years: tuple[int, ...] = (2023, 2024, 2025),
) -> ComparisonDataset:
    race_dates: list[date] = []
    race_keys: list[str] = []
    horse_numbers: list[int] = []
    targets: list[int] = []
    market_features: list[list[float]] = []
    history_features: list[list[float]] = []
    venue_codes: list[str] = []
    active_sizes: list[int] = []
    place_slots: list[int] = []
    field_sizes = (6, 10, 14)
    for year in years:
        for race_index in range(races_per_year):
            field_size = field_sizes[race_index % len(field_sizes)]
            slots = place_slots_for_field_size(field_size)
            race_date = date(year, 1, 1) + timedelta(days=12 * race_index)
            race_key = f"{year}{race_index:04d}"
            latent_rows: list[tuple[int, float, float, float]] = []
            for horse_number in range(1, field_size + 1):
                history_signal = ((horse_number * 7 + race_index * 3) % field_size) / field_size
                market_signal = ((horse_number * 5 + race_index) % field_size) / field_size
                latent = 1.5 * history_signal + market_signal
                latent_rows.append((horse_number, latent, history_signal, market_signal))
            placed = {
                horse_number
                for horse_number, _, _, _ in sorted(
                    latent_rows,
                    key=lambda value: value[1],
                    reverse=True,
                )[:slots]
            }
            for horse_number, _, history_signal, market_signal in latent_rows:
                prior_starts = (horse_number + race_index) % 9
                race_dates.append(race_date)
                race_keys.append(race_key)
                horse_numbers.append(horse_number)
                targets.append(int(horse_number in placed))
                market_features.append(
                    [
                        np.log1p(2.0 + 8.0 * (1.0 - market_signal)),
                        np.log1p(1.2 + 3.0 * (1.0 - market_signal)),
                        float(1 + int((1.0 - market_signal) * field_size)),
                    ]
                )
                history_features.append(
                    [
                        1400.0 + 200.0 * (race_index % 4),
                        float(field_size),
                        float(prior_starts),
                        18.0 + horse_number if prior_starts else np.nan,
                        history_signal if prior_starts else np.nan,
                        history_signal if prior_starts else np.nan,
                        history_signal if prior_starts else np.nan,
                        history_signal if prior_starts else np.nan,
                        history_signal if prior_starts else np.nan,
                        history_signal if prior_starts else np.nan,
                        0.5 if prior_starts else np.nan,
                        0.5 if prior_starts else np.nan,
                    ]
                )
                venue_codes.append(f"{1 + race_index % 3:02d}")
                active_sizes.append(field_size)
                place_slots.append(slots)
    target_array = np.asarray(targets, dtype=np.int64)
    return ComparisonDataset(
        race_dates=tuple(race_dates),
        race_keys=np.asarray(race_keys, dtype=object),
        horse_numbers=np.asarray(horse_numbers, dtype=np.int64),
        targets=target_array,
        market_targets=target_array.copy(),
        market_features=np.asarray(market_features, dtype=np.float64),
        history_numeric_features=np.asarray(history_features, dtype=np.float64),
        venue_codes=np.asarray(venue_codes, dtype=object),
        active_field_sizes=np.asarray(active_sizes, dtype=np.int64),
        place_slots=np.asarray(place_slots, dtype=np.int64),
    )
