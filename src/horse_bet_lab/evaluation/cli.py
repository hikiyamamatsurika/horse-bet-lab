from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_bet_candidate_eval_config
from horse_bet_lab.evaluation.service import evaluate_bet_candidates


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate threshold-based betting candidates from model predictions.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a bet candidate evaluation TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_bet_candidate_eval_config(args.config)
    result = evaluate_bet_candidates(config)
    print(f"Bet candidate evaluation completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        print(
            f"  threshold={summary.threshold:.2f} {summary.split}: "
            f"candidates={summary.candidate_count} "
            f"adopted={summary.adopted_count} adoption_rate={summary.adoption_rate:.4f} "
            f"hit_rate={summary.hit_rate:.4f} avg_prediction={summary.avg_prediction:.4f}",
        )


if __name__ == "__main__":
    main()
