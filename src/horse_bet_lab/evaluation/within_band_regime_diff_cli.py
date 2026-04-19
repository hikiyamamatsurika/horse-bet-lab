from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_within_band_regime_diff_config
from horse_bet_lab.evaluation.within_band_regime_diff import (
    run_within_band_regime_diff_analysis,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze within-band ranking and calibration regime differences.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a within-band regime diff TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_within_band_regime_diff_config(args.config)
    result = run_within_band_regime_diff_analysis(config)
    print(f"Within-band regime diff analysis completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        print(
            f"  regime={summary.regime_label} status={summary.selection_status} "
            f"bets={summary.bet_count} hit_rate={summary.hit_rate:.4f} "
            f"roi={summary.roi:.4f} profit={summary.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
