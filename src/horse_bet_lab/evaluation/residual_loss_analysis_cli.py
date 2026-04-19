from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_residual_loss_analysis_config
from horse_bet_lab.evaluation.residual_loss_analysis import run_residual_loss_analysis


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze residual loss concentration after applying the "
            "problematic-band guard."
        ),
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_residual_loss_analysis(load_residual_loss_analysis_config(args.config))
    for row in result.summaries:
        print(
            f"{row.variant} {row.regime_label} bets={row.bet_count} "
            f"hit_rate={row.hit_rate:.4f} roi={row.roi:.4f} profit={row.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
