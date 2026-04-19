from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_per_race_cap_drawdown_compare_config
from horse_bet_lab.evaluation.reference_per_race_cap_drawdown_compare import (
    run_reference_per_race_cap_drawdown_compare,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare per-race-cap sizing with and without drawdown reduction.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_reference_per_race_cap_drawdown_compare(
        load_reference_per_race_cap_drawdown_compare_config(args.config),
    )
    print(f"Reference per-race-cap drawdown compare completed: output_dir={result.output_dir}")
    for row in result.summaries:
        print(
            f"  variant={row.stake_variant} cap={row.per_race_cap_stake:.0f} "
            f"bankroll={row.initial_bankroll:.0f} final_bankroll={row.final_bankroll:.1f} "
            f"roi={row.roi:.4f} profit={row.total_profit:.1f} "
            f"max_dd={row.max_drawdown:.1f} roi_gt_1_ratio={row.roi_gt_1_ratio:.4f}",
        )


if __name__ == "__main__":
    main()
