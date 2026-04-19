from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_wide_research_diff_config
from horse_bet_lab.evaluation.wide_research_diff import analyze_wide_research_diff


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze the rolling-OOS pair swap between wide research baselines.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a wide research diff TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_wide_research_diff_config(args.config)
    result = analyze_wide_research_diff(config)
    print(f"Wide research diff completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        print(
            f"  set_group={summary.set_group}: pairs={summary.pair_count} "
            f"hits={summary.hit_count} hit_rate={summary.hit_rate:.4f} "
            f"roi={summary.roi:.4f} profit={summary.total_profit:.1f} "
            f"avg_payout={summary.avg_payout:.1f} avg_pair_score={summary.avg_pair_score:.4f}",
        )


if __name__ == "__main__":
    main()
