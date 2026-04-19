from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_regime_diff_analysis_config
from horse_bet_lab.evaluation.regime_diff import run_regime_diff_analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze regime differences for a fixed reference strategy.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a regime diff TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_regime_diff_analysis_config(args.config)
    result = run_regime_diff_analysis(config)
    print(f"Regime diff analysis completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        print(
            f"  regime={summary.regime_label} bets={summary.bet_count} "
            f"hit_rate={summary.hit_rate:.4f} roi={summary.roi:.4f} "
            f"profit={summary.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
