from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_market_feature_comparison_config
from horse_bet_lab.model.comparison import run_market_feature_comparison


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare rolling OOS market-oriented baseline feature sets.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a market feature comparison TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_market_feature_comparison_config(args.config)
    summary = run_market_feature_comparison(config)
    print(f"Market feature comparison completed: output_dir={summary.output_dir}")
    for row in summary.rows:
        print(
            f"  feature_set={row.feature_set_name} auc={row.auc:.4f} "
            f"logloss={row.logloss:.4f} brier={row.brier_score:.4f} "
            f"bets={row.bet_count} hit_rate={row.hit_rate:.4f} "
            f"roi={row.roi:.4f} profit={row.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
