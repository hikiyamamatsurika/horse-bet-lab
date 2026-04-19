from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_dataset_build_config
from horse_bet_lab.dataset.service import build_horse_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a horse-level dataset from staging tables.")
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a dataset build TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_dataset_build_config(args.config)
    summary = build_horse_dataset(config)
    print(f"Dataset build completed: {summary.output_path} rows={summary.row_count}")


if __name__ == "__main__":
    main()

