from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.preregistered_validation_amendment import (
    load_amended_registered_contract,
    verify_implementation_snapshot,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the Phase658 no-popularity forward preregistration amendment.",
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    registration = load_amended_registered_contract(
        args.contract,
        args.checksum,
        args.superseded_contract,
        args.superseded_checksum,
    )
    verify_implementation_snapshot(registration, args.repository_root)
    print(json.dumps(registration.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
