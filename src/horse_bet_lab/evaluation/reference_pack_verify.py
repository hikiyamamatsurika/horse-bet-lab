from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping

from horse_bet_lab.evaluation.reference_pack import (
    ReferencePackInputEntry,
    compute_combined_hash,
    compute_sha256,
    detect_git_commit_sha,
)


@dataclass(frozen=True)
class ReferencePackVerifyRow:
    category: str
    identifier: str
    path: str
    exists: bool
    expected_byte_size: int
    actual_byte_size: int | None
    expected_sha256: str
    actual_sha256: str | None
    matches: bool


@dataclass(frozen=True)
class ReferencePackVerifyResult:
    pack_dir: Path
    manifest_path: Path
    manifest_sha256: str
    is_valid: bool
    rows: tuple[ReferencePackVerifyRow, ...]
    expected_reference_config_sha256: str | None
    actual_reference_config_sha256: str | None
    expected_code_commit_sha: str | None
    actual_code_commit_sha: str | None
    code_commit_matches: bool | None


def verify_reference_pack(pack_dir: Path) -> ReferencePackVerifyResult:
    manifest_path = pack_dir / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows: list[ReferencePackVerifyRow] = []
    rows.extend(
        build_verify_rows(
            entries=payload.get("entries", []),
            category="artifact",
            path_resolver=lambda entry: pack_dir / str(entry["file_name"]),
            identifier_key="file_name",
        ),
    )
    config_rows = build_verify_rows(
        entries=payload.get("referenced_configs", []),
        category="config",
        path_resolver=lambda entry: Path(str(entry["path"])),
        identifier_key="path",
    )
    dataset_rows = build_verify_rows(
        entries=payload.get("dataset_parquets", []),
        category="dataset",
        path_resolver=lambda entry: Path(str(entry["path"])),
        identifier_key="path",
    )
    rows.extend(config_rows)
    rows.extend(dataset_rows)
    actual_reference_config_sha256 = compute_combined_hash(
        tuple(
            entry
            for entry in (
                _to_input_entry(row)
                for row in config_rows
                if (
                    row.matches
                    and row.actual_byte_size is not None
                    and row.actual_sha256 is not None
                )
            )
            if entry is not None
        ),
    )
    expected_reference_config_sha256 = payload.get("reference_config_sha256")
    expected_code_commit_sha = payload.get("code_commit_sha")
    actual_code_commit_sha = detect_git_commit_sha()
    code_commit_matches = None
    if expected_code_commit_sha is not None and actual_code_commit_sha is not None:
        code_commit_matches = expected_code_commit_sha == actual_code_commit_sha
    return ReferencePackVerifyResult(
        pack_dir=pack_dir,
        manifest_path=manifest_path,
        manifest_sha256=compute_sha256(manifest_path),
        is_valid=(
            all(row.matches for row in rows)
            and expected_reference_config_sha256 == actual_reference_config_sha256
            and (code_commit_matches is not False)
        ),
        rows=tuple(rows),
        expected_reference_config_sha256=expected_reference_config_sha256,
        actual_reference_config_sha256=actual_reference_config_sha256,
        expected_code_commit_sha=expected_code_commit_sha,
        actual_code_commit_sha=actual_code_commit_sha,
        code_commit_matches=code_commit_matches,
    )


def verify_reference_pack_or_raise(pack_dir: Path) -> ReferencePackVerifyResult:
    result = verify_reference_pack(pack_dir)
    if not result.is_valid:
        raise RuntimeError(
            f"Reference pack verification failed for {pack_dir}. "
            "Do not use this pack as a comparison baseline."
        )
    return result


def build_verify_rows(
    *,
    entries: list[Mapping[str, object]],
    category: str,
    path_resolver: Callable[[Mapping[str, object]], Path],
    identifier_key: str,
) -> list[ReferencePackVerifyRow]:
    rows: list[ReferencePackVerifyRow] = []
    for entry in entries:
        path = path_resolver(entry)
        exists = path.exists()
        actual_size = path.stat().st_size if exists else None
        actual_sha = compute_sha256(path) if exists else None
        matches = (
            exists
            and actual_size == int(str(entry["byte_size"]))
            and actual_sha == str(entry["sha256"])
        )
        rows.append(
            ReferencePackVerifyRow(
                category=category,
                identifier=str(entry[identifier_key]),
                path=str(path),
                exists=exists,
                expected_byte_size=int(str(entry["byte_size"])),
                actual_byte_size=actual_size,
                expected_sha256=str(entry["sha256"]),
                actual_sha256=actual_sha,
                matches=matches,
            ),
        )
    return rows


def _to_input_entry(row: ReferencePackVerifyRow) -> ReferencePackInputEntry | None:
    if row.actual_byte_size is None or row.actual_sha256 is None:
        return None
    return ReferencePackInputEntry(
        path=row.identifier,
        byte_size=row.actual_byte_size,
        sha256=row.actual_sha256,
    )
