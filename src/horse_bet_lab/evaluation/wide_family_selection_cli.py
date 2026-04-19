from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_wide_family_selection_config
from horse_bet_lab.evaluation.wide_family_selection import run_wide_family_selection


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Select between fixed wide families using valid-window performance.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a wide family selection TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_wide_family_selection_config(args.config)
    result = run_wide_family_selection(config)
    print(f"Wide family selection completed: output_dir={result.output_dir}")
    for summary in result.selected_family_summaries:
        print(
            f"  selection_rule={summary.selection_rule}: "
            f"pairs={summary.pair_count} hits={summary.hit_count} "
            f"hit_rate={summary.hit_rate:.4f} roi={summary.roi:.4f} "
            f"profit={summary.total_profit:.1f} window_wins={summary.window_win_count} "
            f"mean_roi={summary.mean_roi:.4f} roi_std={summary.roi_std:.4f} "
            f"selected_v3={summary.selected_v3_window_count} "
            f"selected_v6={summary.selected_v6_window_count}",
        )


if __name__ == "__main__":
    main()
