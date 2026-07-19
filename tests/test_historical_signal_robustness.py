from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from horse_bet_lab.research.historical_ability_models import (
    HISTORY_NUMERIC_FEATURE_COLUMNS,
    ComparisonDataset,
    place_slots_for_field_size,
)
from horse_bet_lab.research.historical_signal_robustness import (
    FEATURE_GROUPS,
    cross_fitted_market_probabilities,
    feature_subsets,
    run_signal_robustness,
    write_robustness_result,
)


def test_feature_groups_cover_phase651_history_contract() -> None:
    grouped = {feature for values in FEATURE_GROUPS.values() for feature in values}
    assert grouped == {"venue_code", *HISTORY_NUMERIC_FEATURE_COLUMNS}
    subsets = {subset.name: subset for subset in feature_subsets()}
    assert "last_3_top3_rate" not in subsets["without_recent_form"].raw_feature_names
    assert (
        "last_5_venue_compatibility_rate" not in subsets["without_compatibility"].raw_feature_names
    )
    assert subsets["without_current_context"].use_venue is False
    assert subsets["current_context_only"].raw_feature_names == (
        "venue_code",
        "current_distance_m",
        "card_field_size",
    )


def test_cross_fitted_market_probabilities_are_complete_and_deterministic() -> None:
    dataset = _synthetic_dataset(years=(2023,), races_per_year=12)

    first = cross_fitted_market_probabilities(dataset, folds=3, random_seed=11)
    second = cross_fitted_market_probabilities(dataset, folds=3, random_seed=11)

    assert np.all(np.isfinite(first))
    assert np.all((first > 0.0) & (first < 1.0))
    assert np.array_equal(first, second)


def test_robustness_run_keeps_2025_as_reused_confirmation() -> None:
    dataset = _synthetic_dataset(years=(2023, 2024, 2025), races_per_year=14)

    result = run_signal_robustness(
        dataset,
        crossfit_folds=3,
        bootstrap_repetitions=50,
        subgroup_bootstrap_repetitions=20,
        minimum_subgroup_rows=10,
    )

    assert result.summary["period_contract"]["2025"].startswith("reused confirmation")
    assert result.summary["feature_ablation_used_for_selection"] is False
    assert result.summary["roi_or_betting_used"] is False
    assert result.summary["operational_recommendation"] == (
        "RETAIN_AS_PROBABILITY_DIAGNOSTIC_NOT_BETTING_RULE"
    )
    assert result.summary["uniform_subgroup_improvement_supported"] is False
    assert {row["model_name"] for row in result.metrics} == {
        "M1_market",
        "M1C_race_constrained_market",
        "M4_crossfit_full",
        "M5_crossfit_full",
        "M4_in_sample_offset_reference",
    }
    assert len(result.ablation_rows) == 24
    assert len(result.bootstrap_rows) == 14
    assert result.subgroup_rows


def test_robustness_writer_emits_only_diagnostic_outputs(tmp_path: Path) -> None:
    dataset = _synthetic_dataset(years=(2023, 2024, 2025), races_per_year=10)
    result = run_signal_robustness(
        dataset,
        crossfit_folds=2,
        bootstrap_repetitions=20,
        subgroup_bootstrap_repetitions=10,
        minimum_subgroup_rows=10,
    )
    output_dir = tmp_path / "reports"

    write_robustness_result(result, output_dir)

    assert {path.name for path in output_dir.iterdir()} == {
        "phase652_summary.json",
        "phase652_metrics.csv",
        "phase652_ablation.csv",
        "phase652_bootstrap.csv",
        "phase652_subgroup_stability.csv",
        "phase652_findings.md",
    }
    assert not any("roi" in path.name or "bet" in path.name for path in output_dir.iterdir())


def test_robustness_omits_popularity_subgroups_for_two_column_market() -> None:
    dataset = _synthetic_dataset(years=(2023, 2024, 2025), races_per_year=10)
    dataset = replace(dataset, market_features=dataset.market_features[:, :2])

    result = run_signal_robustness(
        dataset,
        crossfit_folds=2,
        bootstrap_repetitions=20,
        subgroup_bootstrap_repetitions=10,
        minimum_subgroup_rows=10,
    )

    assert result.summary["market_popularity_subgroups_included"] is False
    assert not any(row["dimension"] == "market_popularity" for row in result.subgroup_rows)


def _synthetic_dataset(
    *,
    years: tuple[int, ...],
    races_per_year: int,
    field_size: int = 8,
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
    for year in years:
        for race_index in range(races_per_year):
            race_date = date(year, 1, 1) + timedelta(days=15 * race_index)
            race_key = f"{year}{race_index:04d}"
            latent_rows: list[tuple[int, float, float, float]] = []
            for horse_number in range(1, field_size + 1):
                history_signal = ((horse_number * 7 + race_index * 3) % field_size) / field_size
                market_signal = ((horse_number * 5 + race_index) % field_size) / field_size
                latent = 2.0 * history_signal + 0.8 * market_signal
                latent_rows.append((horse_number, latent, history_signal, market_signal))
            placed = {
                horse_number
                for horse_number, _, _, _ in sorted(
                    latent_rows,
                    key=lambda value: value[1],
                    reverse=True,
                )[: place_slots_for_field_size(field_size)]
            }
            for horse_number, _, history_signal, market_signal in latent_rows:
                prior_starts = (horse_number + race_index) % 10
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
                        1600.0 + 200.0 * (race_index % 3),
                        float(field_size),
                        float(prior_starts),
                        20.0 + horse_number if prior_starts else np.nan,
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
                place_slots.append(place_slots_for_field_size(field_size))
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
