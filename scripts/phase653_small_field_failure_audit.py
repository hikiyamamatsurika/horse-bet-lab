from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.historical_ability_models import load_comparison_dataset
from horse_bet_lab.research.small_field_failure_audit import (
    run_small_field_failure_audit,
    write_small_field_audit,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Diagnose the frozen Phase652 failure in five-to-seven-runner fields.",
    )
    parser.add_argument("--history-surface", type=Path, required=True)
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument("--market-dataset", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase653_small_field_failure_audit"),
    )
    parser.add_argument("--bootstrap-repetitions", type=int, default=1_000)
    parser.add_argument("--minimum-bootstrap-races", type=int, default=30)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    dataset, _ = load_comparison_dataset(
        history_surface_path=args.history_surface,
        source_summary_path=args.source_summary,
        market_dataset_path=args.market_dataset,
    )
    result = run_small_field_failure_audit(
        dataset,
        bootstrap_repetitions=args.bootstrap_repetitions,
        minimum_bootstrap_races=args.minimum_bootstrap_races,
    )
    write_small_field_audit(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
