from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_stake_sizing_compare_config
from horse_bet_lab.evaluation.reference_stake_sizing_compare import (
    run_reference_stake_sizing_compare,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare stake sizing variants on the fixed updated reference strategy.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_reference_stake_sizing_compare(
        load_reference_stake_sizing_compare_config(args.config),
    )
    print(f"Reference stake sizing compare completed: output_dir={result.output_dir}")
    for row in result.summaries:
        print(
            f"  variant={row.stake_variant} bets={row.bet_count} "
            f"hit_rate={row.hit_rate:.4f} roi={row.roi:.4f} "
            f"profit={row.total_profit:.1f} max_dd={row.max_drawdown:.1f}",
        )


if __name__ == "__main__":
    main()
