from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np

from horse_bet_lab.research.historical_ability_models import (
    HISTORY_NUMERIC_FEATURE_COLUMNS,
    ComparisonDataset,
    InputAudit,
)
from horse_bet_lab.research.popularity_carrier_comparison import (
    DECISION_TIME_WIN_RANK,
    FINAL_VERDICT,
    NO_POPULARITY,
    decision_time_win_midranks,
    market_variant_dataset,
    run_popularity_carrier_comparison,
    write_popularity_carrier_comparison,
)


def test_decision_time_rank_uses_midrank_for_tied_visible_odds() -> None:
    values = np.asarray([1.0, 1.0, 2.0, 0.5, 1.5], dtype=np.float64)
    race_keys = np.asarray(["A", "A", "A", "B", "B"], dtype=object)

    ranks = decision_time_win_midranks(values, race_keys)

    assert ranks.tolist() == [1.5, 1.5, 3.0, 1.0, 2.0]


def test_market_variants_remove_or_rederive_legacy_popularity() -> None:
    dataset, _ = _synthetic_inputs()

    no_popularity = market_variant_dataset(dataset, NO_POPULARITY)
    decision_time_rank = market_variant_dataset(dataset, DECISION_TIME_WIN_RANK)

    assert no_popularity.market_features.shape[1] == 2
    assert decision_time_rank.market_features.shape[1] == 3
    assert not np.array_equal(
        decision_time_rank.market_features[:, 2],
        dataset.market_features[:, 2],
    )


def test_comparison_selects_only_a_safe_candidate_and_writes_reports(tmp_path: Path) -> None:
    dataset, audit = _synthetic_inputs()

    result = run_popularity_carrier_comparison(
        dataset,
        audit,
        c_grid=(0.1, 1.0),
        bootstrap_repetitions=20,
    )
    output_dir = tmp_path / "reports"
    write_popularity_carrier_comparison(result, output_dir)

    assert result.summary["final_verdict"] == FINAL_VERDICT
    assert result.summary["selected_safe_variant"] in {
        NO_POPULARITY,
        DECISION_TIME_WIN_RANK,
    }
    assert result.summary["2026_data_used"] is False
    assert result.summary["roi_or_betting_used"] is False
    assert result.summary["phase654_contract_changed"] is False
    assert {path.name for path in output_dir.iterdir()} == {
        "phase656_summary.json",
        "phase656_market_metrics.csv",
        "phase656_bootstrap.csv",
        "phase656_rank_agreement.csv",
        "phase656_hyperparameters.csv",
        "phase656_findings.md",
    }


def _synthetic_inputs() -> tuple[ComparisonDataset, InputAudit]:
    race_dates: list[date] = []
    race_keys: list[str] = []
    horse_numbers: list[int] = []
    targets: list[int] = []
    market_features: list[list[float]] = []
    for year in (2023, 2024, 2025):
        for race_index in range(5):
            race_key = f"{year}-R{race_index}"
            for horse_number in range(1, 9):
                race_dates.append(date(year, 1, race_index + 1))
                race_keys.append(race_key)
                horse_numbers.append(horse_number)
                targets.append(int(horse_number <= 3))
                win_odds = 1.2 + horse_number + (race_index * 0.03)
                place_odds = 1.0 + (horse_number * 0.35)
                legacy_popularity = 9 - horse_number
                market_features.append(
                    [
                        float(np.log1p(win_odds)),
                        float(np.log1p(place_odds)),
                        float(legacy_popularity),
                    ]
                )
    row_count = len(targets)
    dataset = ComparisonDataset(
        race_dates=tuple(race_dates),
        race_keys=np.asarray(race_keys, dtype=object),
        horse_numbers=np.asarray(horse_numbers, dtype=np.int64),
        targets=np.asarray(targets, dtype=np.int64),
        market_targets=np.asarray(targets, dtype=np.int64),
        market_features=np.asarray(market_features, dtype=np.float64),
        history_numeric_features=np.zeros(
            (row_count, len(HISTORY_NUMERIC_FEATURE_COLUMNS)),
            dtype=np.float64,
        ),
        venue_codes=np.asarray(["01"] * row_count, dtype=object),
        active_field_sizes=np.asarray([8] * row_count, dtype=np.int64),
        place_slots=np.asarray([3] * row_count, dtype=np.int64),
    )
    audit = InputAudit(
        source_verdict="HISTORICAL_ABILITY_SOURCE_BRIDGE_READY",
        history_row_count=row_count,
        market_row_count=row_count,
        joined_row_count=row_count,
        missing_history_row_count=0,
        missing_history_race_count=0,
        partially_joined_race_count=0,
        fully_missing_unsupported_race_count=0,
        duplicate_history_identity_count=0,
        duplicate_market_identity_count=0,
        existing_market_target_mismatch_count=0,
        mismatch_direction_counts={},
        mismatch_by_active_field_size={},
        joined_rows_by_year={"2023": 40, "2024": 40, "2025": 40},
    )
    return dataset, audit
