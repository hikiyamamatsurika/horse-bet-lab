from __future__ import annotations

from pathlib import Path

import pytest

from horse_bet_lab.jrdb_ingestion.downloader import load_download_auth


def test_auth_config_requires_private_permissions(tmp_path: Path) -> None:
    path = tmp_path / "auth.toml"
    path.write_text("username=test-user\npassword=test-password\n", encoding="utf-8")
    path.chmod(0o644)

    with pytest.raises(PermissionError, match="chmod 600"):
        load_download_auth(auth_config_path=path)


def test_auth_config_loads_without_printing_when_private(tmp_path: Path) -> None:
    path = tmp_path / "auth.toml"
    path.write_text("username=test-user\npassword=test-password\n", encoding="utf-8")
    path.chmod(0o600)

    auth = load_download_auth(auth_config_path=path)

    assert auth is not None
    assert auth.username == "test-user"
    assert auth.password == "test-password"
