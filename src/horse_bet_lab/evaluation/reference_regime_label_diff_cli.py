from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_regime_label_diff_config
from horse_bet_lab.evaluation.reference_regime_label_diff import run_reference_regime_label_diff


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze structural regime labels for the fixed reference strategy.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a reference regime label diff TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_reference_regime_label_diff_config(args.config)
    result = run_reference_regime_label_diff(config)
    print(f"Reference regime label diff completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        print(
            f"  regime={summary.regime_label} bets={summary.bet_count} "
            f"hit_rate={summary.hit_rate:.4f} roi={summary.roi:.4f} "
            f"profit={summary.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
