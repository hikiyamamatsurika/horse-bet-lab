from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_bet_logic_only_config
from horse_bet_lab.evaluation.bet_logic_only import run_bet_logic_only_analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare place BET logic variants on fixed mainline rolling predictions.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a BET logic only comparison TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_bet_logic_only_config(args.config)
    result = run_bet_logic_only_analysis(config)
    print(
        "BET logic only comparison completed: "
        f"run_mode={config.active_run_mode} output_dir={result.output_dir}"
    )


if __name__ == "__main__":
    main()
