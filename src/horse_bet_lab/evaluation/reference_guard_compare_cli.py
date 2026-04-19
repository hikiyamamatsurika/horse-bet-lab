from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_guard_compare_config
from horse_bet_lab.evaluation.reference_guard_compare import run_reference_guard_compare


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare band-specific guards on the fixed reference strategy.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_reference_guard_compare(load_reference_guard_compare_config(args.config))
    for row in result.summaries:
        print(
            f"{row.variant} bets={row.bet_count} hit_rate={row.hit_rate:.4f} "
            f"roi={row.roi:.4f} profit={row.total_profit:.1f} "
            f"max_dd={row.max_drawdown:.1f} max_losing_streak={row.max_losing_streak}",
        )


if __name__ == "__main__":
    main()
