from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_pack_config
from horse_bet_lab.evaluation.reference_pack import run_reference_pack
from horse_bet_lab.evaluation.reference_pack_verify import verify_reference_pack


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the mainline reference pack summary.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the reference pack config TOML.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_reference_pack_config(args.config)
    result = run_reference_pack(config, config_path=args.config)
    print(f"Reference pack completed: output_dir={result.output_dir}")
    verify_result = verify_reference_pack(result.output_dir)
    print(
        "  "
        f"manifest={verify_result.manifest_path.name} "
        f"valid={verify_result.is_valid}",
    )
    for row in result.stateful_summaries:
        print(
            "  "
            f"bankroll={int(row.initial_bankroll)} "
            f"cap={int(row.per_race_cap_stake)} "
            f"final_bankroll={row.final_bankroll:.1f} "
            f"roi={row.roi:.4f} "
            f"profit={row.total_profit:.1f} "
            f"max_dd={row.max_drawdown:.1f} "
            f"roi_gt_1_ratio={row.roi_gt_1_ratio:.4f}",
        )


if __name__ == "__main__":
    main()
