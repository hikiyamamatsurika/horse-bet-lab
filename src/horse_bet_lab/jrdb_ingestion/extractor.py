from __future__ import annotations

import shutil
import subprocess
import zipfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ExtractedArchive:
    archive_path: Path
    archive_kind: str
    extracted_files: tuple[Path, ...]


def extract_archive(
    archive_path: Path,
    *,
    destination_dir: Path,
    archive_kind: str | None = None,
) -> ExtractedArchive:
    destination_dir.mkdir(parents=True, exist_ok=True)
    resolved_kind = archive_kind or infer_archive_kind(archive_path)
    if resolved_kind == "zip":
        with zipfile.ZipFile(archive_path) as archive:
            archive.extractall(destination_dir)
    elif resolved_kind in {"lzh", "lha"}:
        _extract_lzh_archive(archive_path=archive_path, destination_dir=destination_dir)
    elif resolved_kind == "plain":
        shutil.copy2(archive_path, destination_dir / archive_path.name)
    else:
        raise ValueError(f"unsupported_archive_kind: {resolved_kind}")

    extracted_files = tuple(
        path
        for path in sorted(destination_dir.rglob("*"))
        if path.is_file()
    )
    if not extracted_files:
        raise ValueError(f"extraction produced no files for archive: {archive_path}")
    return ExtractedArchive(
        archive_path=archive_path,
        archive_kind=resolved_kind,
        extracted_files=extracted_files,
    )


def infer_archive_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".zip":
        return "zip"
    if suffix in {".lzh", ".lha"}:
        return "lzh"
    return "plain"


def _extract_lzh_archive(*, archive_path: Path, destination_dir: Path) -> None:
    for candidate in ("7z", "7za", "unar", "lha"):
        executable = shutil.which(candidate)
        if executable is None:
            continue
        if candidate in {"7z", "7za"}:
            command = [executable, "x", "-y", f"-o{destination_dir}", str(archive_path)]
            subprocess.run(command, check=True, capture_output=True, text=True)
            return
        if candidate == "unar":
            command = [executable, "-output-directory", str(destination_dir), str(archive_path)]
            subprocess.run(command, check=True, capture_output=True, text=True)
            return
        command = [executable, "xf", str(archive_path)]
        subprocess.run(command, check=True, cwd=destination_dir, capture_output=True, text=True)
        return
    raise RuntimeError(
        "extractor_not_available: lzh/lha extraction requires one of 7z, 7za, unar, or lha"
    )

