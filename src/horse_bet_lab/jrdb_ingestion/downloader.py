from __future__ import annotations

import base64
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from horse_bet_lab.jrdb_ingestion.trigger import JRDBArchiveTrigger

DEFAULT_USERNAME_ENV = "JRDB_AUTO_INGESTION_USERNAME"
DEFAULT_PASSWORD_ENV = "JRDB_AUTO_INGESTION_PASSWORD"


@dataclass(frozen=True)
class JRDBDownloadAuth:
    username: str
    password: str


@dataclass(frozen=True)
class DownloadedArchive:
    archive_name: str
    archive_path: Path
    sha256: str
    byte_count: int


def load_download_auth(
    *,
    auth_config_path: Path | None = None,
    username_env: str = DEFAULT_USERNAME_ENV,
    password_env: str = DEFAULT_PASSWORD_ENV,
) -> JRDBDownloadAuth | None:
    username = os.environ.get(username_env)
    password = os.environ.get(password_env)
    if username and password:
        return JRDBDownloadAuth(username=username, password=password)

    if auth_config_path is None:
        return None
    if not auth_config_path.exists():
        raise FileNotFoundError(f"JRDB auth config does not exist: {auth_config_path}")

    payload = {
        key.strip(): value.strip()
        for key, value in (
            line.split("=", 1)
            for line in auth_config_path.read_text(encoding="utf-8").splitlines()
            if "=" in line and not line.strip().startswith("#")
        )
    }
    file_username = payload.get("username")
    file_password = payload.get("password")
    if not file_username or not file_password:
        raise ValueError(
            f"JRDB auth config {auth_config_path} must contain non-empty username/password entries"
        )
    return JRDBDownloadAuth(username=file_username, password=file_password)


def download_archive(
    archive: JRDBArchiveTrigger,
    *,
    destination_dir: Path,
    auth: JRDBDownloadAuth | None,
) -> DownloadedArchive:
    destination_dir.mkdir(parents=True, exist_ok=True)
    temp_path = destination_dir / f"{archive.name}.part"
    final_path = destination_dir / archive.name
    if temp_path.exists():
        temp_path.unlink()
    if final_path.exists():
        raise FileExistsError(f"download destination already exists: {final_path}")

    try:
        byte_count, sha256 = _copy_source_to_file(
            source_uri=archive.source_uri,
            destination_path=temp_path,
            auth=auth,
        )
        if archive.expected_sha256 is not None and sha256 != archive.expected_sha256:
            raise ValueError(
                "hash_mismatch for "
                f"{archive.name}: expected {archive.expected_sha256}, got {sha256}"
            )
        temp_path.rename(final_path)
        return DownloadedArchive(
            archive_name=archive.name,
            archive_path=final_path,
            sha256=sha256,
            byte_count=byte_count,
        )
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise


def _copy_source_to_file(
    *,
    source_uri: str,
    destination_path: Path,
    auth: JRDBDownloadAuth | None,
) -> tuple[int, str]:
    parsed = urlparse(source_uri)
    if parsed.scheme in ("", "file"):
        source_path = Path(parsed.path if parsed.scheme == "file" else source_uri)
        if not source_path.exists():
            raise FileNotFoundError(f"download source file does not exist: {source_path}")
        return _stream_copy(source_path.open("rb"), destination_path)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"unsupported download source URI scheme: {parsed.scheme!r}")

    request = Request(source_uri)
    if auth is not None:
        token = base64.b64encode(f"{auth.username}:{auth.password}".encode("utf-8")).decode("ascii")
        request.add_header("Authorization", f"Basic {token}")
    with urlopen(request) as response:
        return _stream_copy(response, destination_path)


def _stream_copy(readable, destination_path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    with destination_path.open("wb") as file:
        while True:
            chunk = readable.read(1024 * 1024)
            if not chunk:
                break
            file.write(chunk)
            digest.update(chunk)
            total += len(chunk)
    return total, digest.hexdigest()
