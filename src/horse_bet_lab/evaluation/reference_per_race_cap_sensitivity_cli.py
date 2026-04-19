from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_per_race_cap_sensitivity_config
from horse_bet_lab.evaluation.reference_per_race_cap_sensitivity import (
    run_reference_per_race_cap_sensitivity,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run per-race cap sensitivity on the fixed stateful bankroll strategy.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_reference_per_race_cap_sensitivity(
        load_reference_per_race_cap_sensitivity_config(args.config),
    )
    print(f"Reference per-race-cap sensitivity completed: output_dir={result.output_dir}")
    for row in result.summaries:
        print(
            f"  cap={row.per_race_cap_stake:.0f} bankroll={row.initial_bankroll:.0f} "
            f"final_bankroll={row.final_bankroll:.1f} roi={row.roi:.4f} "
            f"profit={row.total_profit:.1f} max_dd={row.max_drawdown:.1f} "
            f"roi_gt_1_ratio={row.roi_gt_1_ratio:.4f}",
        )


if __name__ == "__main__":
    main()
