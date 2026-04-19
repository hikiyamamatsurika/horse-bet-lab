from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_ranking_score_diff_config
from horse_bet_lab.evaluation.ranking_score_diff import analyze_ranking_score_diff


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze ranking score differences for fixed bet candidate conditions.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a ranking score diff TOML config.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_ranking_score_diff_config(args.config)
    result = analyze_ranking_score_diff(config)
    print(f"Ranking score diff analysis completed: output_dir={result.output_dir}")


if __name__ == "__main__":
    main()
