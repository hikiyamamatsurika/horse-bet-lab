from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.historical_ability_models import (
    load_comparison_dataset,
    run_model_comparison,
    write_comparison_result,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare market and strictly-prior history place-probability baselines.",
    )
    parser.add_argument("--history-surface", type=Path, required=True)
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument("--market-dataset", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase651_historical_ability_model_comparison"),
    )
    parser.add_argument("--bootstrap-repetitions", type=int, default=2_000)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    dataset, input_audit = load_comparison_dataset(
        history_surface_path=args.history_surface,
        source_summary_path=args.source_summary,
        market_dataset_path=args.market_dataset,
    )
    result = run_model_comparison(
        dataset,
        input_audit,
        bootstrap_repetitions=args.bootstrap_repetitions,
    )
    write_comparison_result(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
