from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from horse_bet_lab.research.prospective_collection_ops import (
    DEFAULT_CARRIER_IDENTITY,
    DEFAULT_INPUT_SOURCE_NAME,
    ProspectiveCollectionOpsConfig,
    run_prospective_collection_ops,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Collect an append-only TYB/OZ contract snapshot and run coverage-only monitoring."
        )
    )
    parser.add_argument("--unit-id", required=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/forward_test/prospective_collection"),
    )
    parser.add_argument("--input-source-name", default=DEFAULT_INPUT_SOURCE_NAME)
    parser.add_argument("--input-source-url", required=True)
    parser.add_argument("--input-source-timestamp", required=True)
    parser.add_argument("--odds-observation-timestamp", required=True)
    parser.add_argument("--carrier-identity", default=DEFAULT_CARRIER_IDENTITY)
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--repository-root", type=Path, default=Path("."))
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
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = run_prospective_collection_ops(
            ProspectiveCollectionOpsConfig(
                unit_id=args.unit_id,
                source_dir=args.source_dir,
                output_root=args.output_root,
                input_source_name=args.input_source_name,
                input_source_url=args.input_source_url,
                input_source_timestamp=args.input_source_timestamp,
                odds_observation_timestamp=args.odds_observation_timestamp,
                carrier_identity=args.carrier_identity,
                as_of_date=args.as_of_date,
                repository_root=args.repository_root,
                contract_path=args.contract,
                checksum_path=args.checksum,
                superseded_contract_path=args.superseded_contract,
                superseded_checksum_path=args.superseded_checksum,
            )
        )
    except (FileExistsError, FileNotFoundError, ValueError) as exc:
        print(
            json.dumps(
                {"status": "blocked", "reason": str(exc)},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {
                "unit_id": result.unit_id,
                "run_dir": str(result.run_dir),
                "contract_snapshot_path": str(result.contract_snapshot_path),
                "row_count": result.row_count,
                "race_count": result.race_count,
                "monitor_verdict": result.monitor_verdict,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
