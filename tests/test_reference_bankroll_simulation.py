from __future__ import annotations

from datetime import date
from pathlib import Path

from horse_bet_lab.config import (
    ReferenceBankrollSimulationConfig,
    load_reference_bankroll_simulation_config,
)
from horse_bet_lab.evaluation.ranking_rule_rollforward import CandidateBetRow
from horse_bet_lab.evaluation.reference_bankroll_simulation import simulate_rows
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantRow


def test_simulate_rows_updates_bankroll_statefully() -> None:
    row = GuardVariantRow(
        variant="selected_per_window",
        window_label="test_2025_01",
        row=CandidateBetRow(
            race_key="race-1",
            horse_number=1,
            result_date=date(2025, 1, 1),
            target_value=1,
            pred_probability=0.7,
            market_prob=0.5,
            edge=0.2,
            win_odds=4.5,
            popularity=2,
            place_basis_odds=2.5,
            place_payout=250.0,
        ),
    )
    config = ReferenceBankrollSimulationConfig(
        name="test",
        reference_label_guard_compare_config_path=Path("unused.toml"),
        output_dir=Path("unused"),
        stake_variants=("capped_fractional_kelly_like",),
        initial_bankrolls=(1000.0,),
        kelly_fraction=0.5,
        kelly_cap_stake=500.0,
        per_race_cap_stake=300.0,
        per_day_cap_stake=800.0,
        drawdown_reduction_threshold=500.0,
        drawdown_reduction_factor=0.5,
        bootstrap_iterations=10,
        random_seed=42,
    )

    placed = simulate_rows(
        rows=(row, row),
        stake_variant="capped_fractional_kelly_like",
        initial_bankroll=1000.0,
        config=config,
    )

    assert len(placed) == 2
    assert placed[0].stake == 200.0
    assert placed[1].stake == 300.0
    assert placed[1].bankroll_before > placed[0].bankroll_before


def test_reference_bankroll_simulation_loader(tmp_path: Path) -> None:
    compare_config = tmp_path / "reference_label_guard_compare.toml"
    compare_config.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_label_guard_compare_test'",
                (
                    "second_guard_selection_config_path = "
                    "'configs/second_guard_selection_dual_market_logreg_2023_2025.toml'"
                ),
                f"output_dir = '{tmp_path / 'compare_output'}'",
                "extra_guard_variants = ['no_extra_label_guard']",
                "",
            ],
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "reference_bankroll_simulation.toml"
    config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_bankroll_simulation_test'",
                f"reference_label_guard_compare_config_path = '{compare_config}'",
                f"output_dir = '{tmp_path / 'output'}'",
                "stake_variants = ['flat_100', 'capped_fractional_kelly_like_drawdown_reduction']",
                "initial_bankrolls = [5000, 10000]",
                "bootstrap_iterations = 10",
                "random_seed = 7",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_reference_bankroll_simulation_config(config_path)
    assert config.initial_bankrolls == (5000.0, 10000.0)
    assert config.bootstrap_iterations == 10
    assert config.random_seed == 7
