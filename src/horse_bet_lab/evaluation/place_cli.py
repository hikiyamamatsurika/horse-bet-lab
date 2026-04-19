from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_place_backtest_config
from horse_bet_lab.evaluation.place_backtest import run_place_backtest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a minimal place-payout backtest from predictions and HJC payouts.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a place backtest TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_place_backtest_config(args.config)
    result = run_place_backtest(config)
    print(f"Place backtest completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        odds_band = (
            f" min_win_odds={summary.min_win_odds} max_win_odds={summary.max_win_odds}"
            if summary.min_win_odds is not None or summary.max_win_odds is not None
            else ""
        )
        popularity_band = (
            f" min_popularity={summary.min_popularity} max_popularity={summary.max_popularity}"
            if summary.min_popularity is not None or summary.max_popularity is not None
            else ""
        )
        print(
            f"  window={summary.window_label} selection_metric={summary.selection_metric} "
            f"threshold={summary.threshold:.2f}"
            f"{odds_band}{popularity_band} {summary.split}: "
            f"bets={summary.bet_count} hits={summary.hit_count} hit_rate={summary.hit_rate:.4f} "
            f"roi={summary.roi:.4f} profit={summary.total_profit:.1f} "
            f"avg_payout={summary.avg_payout:.1f}",
        )


if __name__ == "__main__":
    main()
