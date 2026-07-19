from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.historical_ability_models import load_comparison_dataset
from horse_bet_lab.research.no_popularity_rebase import (
    run_no_popularity_rebase,
    write_no_popularity_rebase,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rerun Phase651-653 with the selected no-popularity market contract.",
    )
    parser.add_argument("--history-surface", type=Path, required=True)
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument("--market-dataset", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase657_no_popularity_rebase"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    dataset, input_audit = load_comparison_dataset(
        history_surface_path=args.history_surface,
        source_summary_path=args.source_summary,
        market_dataset_path=args.market_dataset,
    )
    result = run_no_popularity_rebase(dataset, input_audit)
    write_no_popularity_rebase(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
