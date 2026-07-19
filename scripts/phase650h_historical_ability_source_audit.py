from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.historical_ability_source import (
    BLOCKED_VERDICT,
    build_source_audit,
    write_source_audit,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit KYI-to-SED identity and build a strictly-prior history surface.",
    )
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase650h_historical_ability_source"),
    )
    parser.add_argument("--history-years", type=int, nargs="+", default=list(range(2019, 2026)))
    parser.add_argument("--evaluation-years", type=int, nargs="+", default=[2023, 2024, 2025])
    parser.add_argument("--minimum-identity-match-rate", type=float, default=0.999)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = build_source_audit(
        raw_root=args.raw_root,
        history_years=args.history_years,
        evaluation_years=args.evaluation_years,
        minimum_identity_match_rate=args.minimum_identity_match_rate,
    )
    write_source_audit(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return int(result.summary["final_verdict"] == BLOCKED_VERDICT)


if __name__ == "__main__":
    raise SystemExit(main())
