from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.historical_ability_models import load_comparison_dataset
from horse_bet_lab.research.popularity_carrier_comparison import (
    run_popularity_carrier_comparison,
    write_popularity_carrier_comparison,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare decision-time-safe replacements for legacy SED popularity.",
    )
    parser.add_argument("--history-surface", type=Path, required=True)
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument("--market-dataset", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase656_popularity_carrier_comparison"),
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
    result = run_popularity_carrier_comparison(
        dataset,
        input_audit,
        bootstrap_repetitions=args.bootstrap_repetitions,
    )
    write_popularity_carrier_comparison(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
