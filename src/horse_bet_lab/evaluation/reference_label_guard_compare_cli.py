from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_label_guard_compare_config
from horse_bet_lab.evaluation.reference_label_guard_compare import (
    run_reference_label_guard_compare,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare global weak-label guards on top of the fixed reference strategy.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a reference label guard comparison TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_reference_label_guard_compare_config(args.config)
    result = run_reference_label_guard_compare(config)
    print(f"Reference label guard compare completed: output_dir={result.output_dir}")
    for summary in result.selected_test_rollup:
        print(
            f"  group={summary.group_key} bets={summary.bet_count} "
            f"hit_rate={summary.hit_rate:.4f} roi={summary.roi:.4f} "
            f"profit={summary.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
