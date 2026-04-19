from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_uncertainty_config
from horse_bet_lab.evaluation.reference_uncertainty import run_reference_uncertainty


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run bootstrap uncertainty diagnostics on the fixed reference strategy.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_reference_uncertainty(load_reference_uncertainty_config(args.config))
    row = result.summary
    print(
        f"{row.strategy_name} bets={row.bet_count} hit_rate={row.hit_rate:.4f} "
        f"roi={row.roi:.4f} profit={row.total_profit:.1f} "
        f"roi_ci=({row.roi_p02_5:.4f},{row.roi_p97_5:.4f}) "
        f"roi_gt_1_ratio={row.roi_gt_1_ratio:.4f} "
        f"max_dd={row.max_drawdown:.1f}",
    )


if __name__ == "__main__":
    main()
