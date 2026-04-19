from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_bankroll_simulation_uncertainty_config
from horse_bet_lab.evaluation.reference_bankroll_simulation_uncertainty import (
    run_reference_bankroll_simulation_uncertainty,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run bootstrap uncertainty comparison for stateful bankroll candidates.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_reference_bankroll_simulation_uncertainty(
        load_reference_bankroll_simulation_uncertainty_config(args.config),
    )
    print(f"Reference bankroll simulation uncertainty completed: output_dir={result.output_dir}")
    for row in result.summaries:
        print(
            f"  variant={row.stake_variant} bankroll={row.initial_bankroll:.0f} "
            f"final_bankroll={row.final_bankroll:.1f} roi={row.roi:.4f} "
            f"roi_gt_1_ratio={row.roi_gt_1_ratio:.4f} "
            f"bankroll_below_initial_ratio={row.final_bankroll_below_initial_ratio:.4f}",
        )


if __name__ == "__main__":
    main()
