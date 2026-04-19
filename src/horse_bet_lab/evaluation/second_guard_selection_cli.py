from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_second_guard_selection_config
from horse_bet_lab.evaluation.second_guard_selection import run_second_guard_selection


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Select a second guard on valid windows and apply it to test windows.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_second_guard_selection(
        load_second_guard_selection_config(args.config),
    )
    for row in result.selected_summaries:
        print(
            f"{row.selection_window_label} {row.applied_to_split} "
            f"guard={row.selected_second_guard} bets={row.bet_count} "
            f"hit_rate={row.hit_rate:.4f} roi={row.roi:.4f} "
            f"profit={row.total_profit:.1f} max_dd={row.max_drawdown:.1f} "
            f"max_losing_streak={row.max_losing_streak}",
        )


if __name__ == "__main__":
    main()
