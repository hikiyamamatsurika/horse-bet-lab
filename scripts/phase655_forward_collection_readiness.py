from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.forward_collection_readiness import (
    BLOCKED_VERDICT,
    run_forward_collection_readiness,
    write_forward_collection_readiness,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit static readiness for the Phase654 prospective collection window.",
    )
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("configs/phase654_2026_forward_preregistered_validation.json"),
    )
    parser.add_argument(
        "--checksum",
        type=Path,
        default=Path("configs/phase654_2026_forward_preregistered_validation.sha256"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase655_forward_collection_readiness"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_forward_collection_readiness(
        contract_path=args.contract,
        checksum_path=args.checksum,
    )
    write_forward_collection_readiness(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return int(result.summary["final_verdict"] == BLOCKED_VERDICT)


if __name__ == "__main__":
    raise SystemExit(main())
