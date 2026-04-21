from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.jrdb_ingestion.downloader import load_download_auth
from horse_bet_lab.jrdb_ingestion.orchestration import (
    DEFAULT_RAW_DIR,
    DEFAULT_WORKSPACE_ROOT,
    run_jrdb_auto_ingestion_job,
)
from horse_bet_lab.jrdb_ingestion.trigger import load_trigger_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the local-only JRDB auto-ingestion MVP orchestrator.",
    )
    parser.add_argument(
        "--trigger-manifest",
        type=Path,
        required=True,
        help="Path to a JRDB auto-ingestion trigger JSON manifest.",
    )
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=DEFAULT_WORKSPACE_ROOT,
        help="Local-only orchestration workspace root.",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=DEFAULT_RAW_DIR,
        help="JRDB raw placement root used for existing ingest handoff.",
    )
    parser.add_argument(
        "--auth-config",
        type=Path,
        help="Optional local auth config file with username/password entries.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    trigger = load_trigger_manifest(args.trigger_manifest)
    auth = load_download_auth(auth_config_path=args.auth_config)
    result = run_jrdb_auto_ingestion_job(
        trigger,
        workspace_root=args.workspace_root,
        raw_dir=args.raw_dir,
        auth=auth,
    )
    print(
        "JRDB auto-ingestion completed: "
        f"job_id={result.job_id} status={result.status} report_path={result.report_path}"
    )


if __name__ == "__main__":
    main()
