from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_strategy_diagnostics_config
from horse_bet_lab.evaluation.reference_strategy import run_reference_strategy_diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run reference strategy diagnostics for the consensus ranking strategy.",
    )
    parser.add_argument("--config", required=True, help="Path to the diagnostics TOML config.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_reference_strategy_diagnostics_config(Path(args.config))
    result = run_reference_strategy_diagnostics(config)
    print(f"Reference strategy diagnostics completed: output_dir={result.output_dir}")


if __name__ == "__main__":
    main()
