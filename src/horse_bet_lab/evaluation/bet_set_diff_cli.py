from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_bet_set_diff_analysis_config
from horse_bet_lab.evaluation.bet_set_diff import analyze_bet_set_diff


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare selected bet sets between baseline and variant model backtests.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a bet set diff TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_bet_set_diff_analysis_config(args.config)
    result = analyze_bet_set_diff(config)
    print(f"Bet set diff analysis completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        print(
            f"  comparison={summary.comparison_label} set_group={summary.set_group} "
            f"bets={summary.bet_count} hits={summary.hit_count} "
            f"hit_rate={summary.hit_rate:.4f} roi={summary.roi:.4f} "
            f"profit={summary.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
