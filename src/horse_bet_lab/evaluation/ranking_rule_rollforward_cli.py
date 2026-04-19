from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_ranking_rule_comparison_config
from horse_bet_lab.evaluation.ranking_rule_rollforward import run_ranking_rule_comparison


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare ranking rules on rolling OOS place backtests.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a ranking rule comparison TOML config.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_ranking_rule_comparison_config(args.config)
    result = run_ranking_rule_comparison(config)
    print(f"Ranking rule comparison completed: output_dir={result.output_dir}")


if __name__ == "__main__":
    main()
