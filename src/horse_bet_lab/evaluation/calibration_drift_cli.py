from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_calibration_drift_config
from horse_bet_lab.evaluation.calibration_drift import run_calibration_drift_analysis


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze year-wise calibration drift within a fixed band.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_calibration_drift_analysis(load_calibration_drift_config(args.config))
    for row in result.summaries:
        print(
            f"{row.regime_label} bets={row.candidate_count} "
            f"adopted={row.adopted_count} hit_rate={row.hit_rate:.4f} "
            f"roi={row.roi:.4f} pred_bias={row.pred_minus_empirical:.4f} "
            f"market_bias={row.market_minus_empirical:.4f}",
        )


if __name__ == "__main__":
    main()
