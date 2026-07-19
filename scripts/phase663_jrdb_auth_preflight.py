from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from horse_bet_lab.jrdb_ingestion.downloader import (
    DEFAULT_PASSWORD_ENV,
    DEFAULT_USERNAME_ENV,
    load_download_auth,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check JRDB authentication without printing secrets."
    )
    parser.add_argument(
        "--auth-config",
        type=Path,
        default=Path(".local/jrdb_auto_ingestion_auth.toml"),
    )
    args = parser.parse_args()
    username_present = bool(os.environ.get(DEFAULT_USERNAME_ENV))
    password_present = bool(os.environ.get(DEFAULT_PASSWORD_ENV))
    try:
        auth = load_download_auth(auth_config_path=args.auth_config)
    except (FileNotFoundError, PermissionError, ValueError) as exc:
        print(json.dumps({"status": "blocked", "reason": str(exc)}, indent=2))
        return 1
    if auth is None:
        print(json.dumps({"status": "blocked", "reason": "credentials are missing"}, indent=2))
        return 1
    source = "environment" if username_present and password_present else "local_config"
    print(
        json.dumps(
            {
                "status": "ready",
                "source": source,
                "username_present": True,
                "password_present": True,
                "secret_values_printed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
