from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import ExperimentConfig, load_experiment_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a dummy horse-bet-lab experiment.")
    parser.add_argument("--config", type=Path, required=True, help="Path to a TOML config file.")
    return parser


def render_run_summary(config: ExperimentConfig) -> str:
    lines = [
        "Running dummy experiment:",
        f"  name: {config.name}",
        f"  model: {config.model}",
        f"  feature_set: {config.feature_set}",
        f"  strategy: {config.strategy}",
        f"  period: {config.period}",
    ]
    return "\n".join(lines)


def main() -> None:
    args = build_parser().parse_args()
    config = load_experiment_config(args.config)
    print(render_run_summary(config))


if __name__ == "__main__":
    main()

