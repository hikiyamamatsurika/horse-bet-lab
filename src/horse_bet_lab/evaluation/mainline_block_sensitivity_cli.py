from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_mainline_block_sensitivity_config
from horse_bet_lab.evaluation.mainline_block_sensitivity import (
    run_mainline_block_sensitivity,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare mainline uncertainty sensitivity across block bootstrap units.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the mainline block sensitivity config TOML.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_mainline_block_sensitivity(
        load_mainline_block_sensitivity_config(args.config),
    )
    print(f"Mainline block sensitivity completed: output_dir={result.output_dir}")
    for row in result.summary_rows:
        suffix = (
            f" bankroll={int(row.initial_bankroll)} stake_variant={row.stake_variant}"
            if row.initial_bankroll is not None
            else ""
        )
        print(
            "  "
            f"scope={row.scope} block={row.block_unit}{suffix} "
            f"roi_ci=[{row.roi_p02_5:.4f}, {row.roi_p97_5:.4f}] "
            f"profit_ci=[{row.total_profit_p02_5:.1f}, {row.total_profit_p97_5:.1f}] "
            f"dd_range=[{row.max_drawdown_p02_5:.1f}, {row.max_drawdown_p97_5:.1f}] "
            f"roi_gt_1={row.roi_gt_1_ratio:.4f}",
        )


if __name__ == "__main__":
    main()
