from __future__ import annotations

import argparse
import json
from pathlib import Path

from horse_bet_lab.research.preregistered_validation_contract import (
    load_registered_contract,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the locked Phase654 2026 forward evaluation preregistration.",
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    registration = load_registered_contract(args.contract, args.checksum)
    print(json.dumps(registration.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
