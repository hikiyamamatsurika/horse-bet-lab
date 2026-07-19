from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.historical_ability_models import load_comparison_dataset
from horse_bet_lab.research.historical_signal_robustness import (
    run_signal_robustness,
    write_robustness_result,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cross-fit and diagnose the Phase651 historical residual signal.",
    )
    parser.add_argument("--history-surface", type=Path, required=True)
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument("--market-dataset", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase652_historical_signal_robustness"),
    )
    parser.add_argument("--bootstrap-repetitions", type=int, default=2_000)
    parser.add_argument("--subgroup-bootstrap-repetitions", type=int, default=500)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    dataset, _ = load_comparison_dataset(
        history_surface_path=args.history_surface,
        source_summary_path=args.source_summary,
        market_dataset_path=args.market_dataset,
    )
    result = run_signal_robustness(
        dataset,
        bootstrap_repetitions=args.bootstrap_repetitions,
        subgroup_bootstrap_repetitions=args.subgroup_bootstrap_repetitions,
    )
    write_robustness_result(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
