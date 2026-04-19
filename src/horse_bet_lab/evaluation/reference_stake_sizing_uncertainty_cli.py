from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_stake_sizing_uncertainty_config
from horse_bet_lab.evaluation.reference_stake_sizing_uncertainty import (
    run_reference_stake_sizing_uncertainty,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run bootstrap uncertainty diagnostics for stake sizing variants.",
    )
    parser.add_argument("--config", type=Path, required=True, help="Path to TOML config.")
    args = parser.parse_args()

    result = run_reference_stake_sizing_uncertainty(
        load_reference_stake_sizing_uncertainty_config(args.config),
    )
    print(f"Reference stake sizing uncertainty completed: output_dir={result.output_dir}")
    for row in result.summaries:
        print(
            f"  variant={row.stake_variant} roi={row.roi:.4f} "
            f"roi_ci=({row.roi_p02_5:.4f},{row.roi_p97_5:.4f}) "
            f"roi_gt_1_ratio={row.roi_gt_1_ratio:.4f} "
            f"profit={row.total_profit:.1f}",
        )


if __name__ == "__main__":
    main()
