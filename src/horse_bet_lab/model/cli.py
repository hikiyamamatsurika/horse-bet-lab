from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_model_train_config
from horse_bet_lab.model.service import train_logistic_regression_baseline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train and evaluate a baseline classification model.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a model training TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_model_train_config(args.config)
    summary = train_logistic_regression_baseline(config)
    print(f"Model training completed: output_dir={summary.output_dir}")
    for split_name, metrics in summary.metrics_by_split.items():
        print(
            f"  {split_name}: auc={metrics.auc:.4f} "
            f"logloss={metrics.logloss:.4f} "
            f"brier={metrics.brier_score:.4f} "
            f"positive_rate={metrics.positive_rate:.4f}",
        )


if __name__ == "__main__":
    main()
