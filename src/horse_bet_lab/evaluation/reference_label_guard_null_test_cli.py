from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_reference_label_guard_null_test_config
from horse_bet_lab.evaluation.reference_label_guard_null_test import (
    run_reference_label_guard_null_test,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run shuffled-label null calibration for valid-selected extra label guard.",
    )
    parser.add_argument("--config", required=True, help="Path to the null test config TOML.")
    args = parser.parse_args()

    run_reference_label_guard_null_test(
        load_reference_label_guard_null_test_config(Path(args.config)),
    )


if __name__ == "__main__":
    main()
