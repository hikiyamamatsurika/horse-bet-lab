from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from horse_bet_lab.research.prospective_collection_monitor import (
    MONITOR_BLOCKED,
    run_prospective_collection_monitor,
    write_prospective_collection_monitor,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Monitor prospective snapshot schema and coverage without outcomes or metrics.",
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
    parser.add_argument("--snapshot", type=Path, action="append", default=[])
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=date.today())
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/artifacts/phase660_prospective_collection_monitor"),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    result = run_prospective_collection_monitor(
        contract_path=args.contract,
        checksum_path=args.checksum,
        superseded_contract_path=args.superseded_contract,
        superseded_checksum_path=args.superseded_checksum,
        repository_root=args.repository_root,
        snapshot_paths=tuple(args.snapshot),
        as_of_date=args.as_of_date,
    )
    write_prospective_collection_monitor(result, args.output_dir)
    print(json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True))
    return int(result.summary["final_verdict"] == MONITOR_BLOCKED)


if __name__ == "__main__":
    raise SystemExit(main())
