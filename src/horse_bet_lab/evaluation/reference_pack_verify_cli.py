from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.evaluation.reference_pack_verify import verify_reference_pack


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify the frozen mainline reference pack manifest.",
    )
    parser.add_argument(
        "--pack-dir",
        type=Path,
        required=True,
        help="Path to the reference pack artifact directory.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = verify_reference_pack(args.pack_dir)
    print(
        "Reference pack verify: "
        f"pack_dir={result.pack_dir} "
        f"manifest_sha256={result.manifest_sha256} "
        f"valid={result.is_valid}",
    )
    print(
        "  "
        f"reference_config_sha_matches="
        f"{result.expected_reference_config_sha256 == result.actual_reference_config_sha256}",
    )
    print(
        "  "
        f"code_commit_matches={result.code_commit_matches} "
        f"expected_commit={result.expected_code_commit_sha} "
        f"actual_commit={result.actual_code_commit_sha}",
    )
    for row in result.rows:
        print(
            "  "
            f"{row.category}:{row.identifier} matches={row.matches} "
            f"expected_size={row.expected_byte_size} "
            f"actual_size={row.actual_byte_size}",
        )
    if not result.is_valid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
