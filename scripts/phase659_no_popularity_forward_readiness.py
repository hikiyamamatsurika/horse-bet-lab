from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.no_popularity_forward_readiness import (
    BLOCKED_VERDICT,
    run_no_popularity_forward_readiness,
    write_no_popularity_forward_readiness,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit static readiness for the Phase658 no-popularity forward contract.",
    )
    parser.add_argument(
        "--contract",
        type=Path,
        default=Path("configs/phase658_2026_forward_preregistered_validation.json"),
    )
    parser.add_argument(
        "--checksum",
        type=Path,
        default=Path("configs/phase658_2026_forward_preregistered_validation.sha256"),
    )
    parser.add_argument(
        "--superseded-contract",
        type=Path,
        default=Path("configs/phase654_2026_forward_preregistered_validation.json"),
    )
    parser.add_argument(
        "--superseded-checksum",
        type=Path,
        default=Path("configs/phase654_2026_forward_preregistered_validation.sha256"),
    )
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase659_no_popularity_forward_readiness"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_no_popularity_forward_readiness(
        contract_path=args.contract,
        checksum_path=args.checksum,
        superseded_contract_path=args.superseded_contract,
        superseded_checksum_path=args.superseded_checksum,
        repository_root=args.repository_root,
    )
    write_no_popularity_forward_readiness(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return int(result.summary["final_verdict"] == BLOCKED_VERDICT)


if __name__ == "__main__":
    raise SystemExit(main())
