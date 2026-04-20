from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from horse_bet_lab.forward_test.snapshot_bridge import load_snapshot_bridge_config


RAW_SNAPSHOT_INTAKE_MANIFEST_FILENAME = "raw_snapshot_intake_manifest.json"
REQUIRED_RAW_SNAPSHOT_COLUMNS = ("race_key", "horse_number", "win_odds")
PLACE_COLUMN_ALTERNATIVES = (
    ("place_basis_odds_proxy",),
    ("place_odds_min", "place_odds_max"),
)
OPTIONAL_RAW_SNAPSHOT_COLUMNS = (
    "popularity",
    "snapshot_status",
    "retry_count",
    "timeout_seconds",
    "snapshot_failure_reason",
)
PLACEHOLDER_PREFIX = "replace_with_"
PLACEHOLDER_HOST = "replace.example"


@dataclass(frozen=True)
class PlaceForwardRawSnapshotIntakeManifest:
    unit_id: str
    raw_snapshot_path: str
    source_family: str
    input_source_name: str
    input_source_url: str
    input_source_timestamp: str
    odds_observation_timestamp: str
    carrier_identity: str
    expected_required_columns: tuple[str, ...] = REQUIRED_RAW_SNAPSHOT_COLUMNS
    expected_place_columns_any_of: tuple[tuple[str, ...], ...] = PLACE_COLUMN_ALTERNATIVES
    expected_optional_columns: tuple[str, ...] = OPTIONAL_RAW_SNAPSHOT_COLUMNS
    notes: str = (
        "Confirm the raw snapshot file is the intended source for this unit before running the bridge."
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlaceForwardRawSnapshotPrecheckResult:
    manifest_path: Path
    raw_snapshot_path: Path
    bridge_config_path: Path
    unit_id: str
    header_columns: tuple[str, ...]
    source_family: str


def build_raw_snapshot_intake_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Precheck recurring raw snapshot intake before running the place forward-test bridge.",
    )
    parser.add_argument(
        "--bridge-config",
        type=Path,
        required=True,
        help="Path to a recurring rehearsal bridge TOML config.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help=(
            "Path to raw snapshot intake manifest JSON. Defaults to "
            "the raw snapshot directory next to the configured input file."
        ),
    )
    return parser


def default_intake_manifest_path(raw_snapshot_path: Path) -> Path:
    return raw_snapshot_path.parent / RAW_SNAPSHOT_INTAKE_MANIFEST_FILENAME


def build_default_raw_snapshot_intake_manifest(
    *,
    unit_id: str,
    raw_snapshot_path: Path,
    source_family: str,
    input_source_name: str,
    input_source_url: str,
    input_source_timestamp: str,
    odds_observation_timestamp: str,
    carrier_identity: str,
) -> PlaceForwardRawSnapshotIntakeManifest:
    return PlaceForwardRawSnapshotIntakeManifest(
        unit_id=unit_id,
        raw_snapshot_path=str(raw_snapshot_path),
        source_family=source_family,
        input_source_name=input_source_name,
        input_source_url=input_source_url,
        input_source_timestamp=input_source_timestamp,
        odds_observation_timestamp=odds_observation_timestamp,
        carrier_identity=carrier_identity,
    )


def write_raw_snapshot_intake_manifest(
    path: Path,
    manifest: PlaceForwardRawSnapshotIntakeManifest,
) -> None:
    path.write_text(
        json.dumps(manifest.to_dict(), ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )


def load_raw_snapshot_intake_manifest(path: Path) -> PlaceForwardRawSnapshotIntakeManifest:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return PlaceForwardRawSnapshotIntakeManifest(
        unit_id=str(payload["unit_id"]),
        raw_snapshot_path=str(payload["raw_snapshot_path"]),
        source_family=str(payload["source_family"]),
        input_source_name=str(payload["input_source_name"]),
        input_source_url=str(payload["input_source_url"]),
        input_source_timestamp=str(payload["input_source_timestamp"]),
        odds_observation_timestamp=str(payload["odds_observation_timestamp"]),
        carrier_identity=str(payload["carrier_identity"]),
        expected_required_columns=tuple(payload.get("expected_required_columns", REQUIRED_RAW_SNAPSHOT_COLUMNS)),
        expected_place_columns_any_of=tuple(
            tuple(group) for group in payload.get("expected_place_columns_any_of", PLACE_COLUMN_ALTERNATIVES)
        ),
        expected_optional_columns=tuple(payload.get("expected_optional_columns", OPTIONAL_RAW_SNAPSHOT_COLUMNS)),
        notes=str(payload.get("notes", "")),
    )


def run_raw_snapshot_intake_precheck(
    *,
    bridge_config_path: Path,
    manifest_path: Path | None = None,
) -> PlaceForwardRawSnapshotPrecheckResult:
    bridge_config = load_snapshot_bridge_config(bridge_config_path)
    if len(bridge_config.sources) != 1:
        raise ValueError(
            "raw snapshot intake precheck currently supports exactly one bridge source; "
            f"got {len(bridge_config.sources)} sources"
        )
    source = bridge_config.sources[0]
    manifest_path = manifest_path or default_intake_manifest_path(source.path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"raw snapshot intake manifest does not exist: {manifest_path}")

    manifest = load_raw_snapshot_intake_manifest(manifest_path)
    raw_snapshot_path = Path(manifest.raw_snapshot_path)
    if raw_snapshot_path != source.path:
        raise ValueError(
            "raw snapshot intake manifest raw_snapshot_path does not match bridge source path: "
            f"{raw_snapshot_path} != {source.path}"
        )
    if not raw_snapshot_path.exists():
        raise FileNotFoundError(f"raw snapshot file does not exist: {raw_snapshot_path}")

    _require_non_placeholder(manifest.unit_id, "unit_id")
    _require_non_placeholder(manifest.source_family, "source_family")
    _require_non_placeholder(manifest.input_source_name, "input_source_name")
    _require_non_placeholder(manifest.input_source_url, "input_source_url")
    _require_non_placeholder(manifest.input_source_timestamp, "input_source_timestamp")
    _require_non_placeholder(manifest.odds_observation_timestamp, "odds_observation_timestamp")
    _require_non_placeholder(manifest.carrier_identity, "carrier_identity")

    _require_matching_text(manifest.input_source_name, source.input_source_name, "input_source_name")
    _require_matching_text(manifest.input_source_url, source.input_source_url, "input_source_url")
    _require_matching_text(
        manifest.input_source_timestamp,
        source.input_source_timestamp,
        "input_source_timestamp",
    )
    _require_matching_text(
        manifest.odds_observation_timestamp,
        source.odds_observation_timestamp,
        "odds_observation_timestamp",
    )
    _require_matching_text(manifest.carrier_identity, source.carrier_identity, "carrier_identity")

    with raw_snapshot_path.open(encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError(f"raw snapshot CSV is missing a header row: {raw_snapshot_path}")
        header_columns = tuple(reader.fieldnames)

    missing_required = [column for column in manifest.expected_required_columns if column not in header_columns]
    if missing_required:
        raise ValueError(
            "raw snapshot CSV is missing required columns before bridge: "
            f"{missing_required}"
        )
    if not any(all(column in header_columns for column in group) for group in manifest.expected_place_columns_any_of):
        raise ValueError(
            "raw snapshot CSV must contain place odds columns matching one of "
            f"{list(manifest.expected_place_columns_any_of)}"
        )

    inferred_unit_id = _infer_unit_id_from_raw_path(raw_snapshot_path)
    if inferred_unit_id is not None and manifest.unit_id != inferred_unit_id:
        raise ValueError(
            f"raw snapshot intake manifest unit_id {manifest.unit_id!r} does not match raw path unit_id {inferred_unit_id!r}"
        )

    return PlaceForwardRawSnapshotPrecheckResult(
        manifest_path=manifest_path,
        raw_snapshot_path=raw_snapshot_path,
        bridge_config_path=bridge_config_path,
        unit_id=manifest.unit_id,
        header_columns=header_columns,
        source_family=manifest.source_family,
    )


def _infer_unit_id_from_raw_path(raw_snapshot_path: Path) -> str | None:
    parts = raw_snapshot_path.parts
    try:
        runs_index = parts.index("runs")
    except ValueError:
        return None
    if runs_index + 1 >= len(parts):
        return None
    return parts[runs_index + 1]


def _require_non_placeholder(value: str, field_name: str) -> None:
    text = value.strip()
    if text == "":
        raise ValueError(f"raw snapshot intake manifest field {field_name!r} must be non-empty")
    if text.startswith(PLACEHOLDER_PREFIX) or PLACEHOLDER_HOST in text:
        raise ValueError(
            f"raw snapshot intake manifest field {field_name!r} still contains a scaffold/template placeholder: {text!r}"
        )


def _require_matching_text(expected: str, observed: str | None, field_name: str) -> None:
    observed_text = (observed or "").strip()
    if expected != observed_text:
        raise ValueError(
            f"raw snapshot intake manifest {field_name!r} does not match bridge config: {expected!r} != {observed_text!r}"
        )


def main() -> int:
    parser = build_raw_snapshot_intake_parser()
    args = parser.parse_args()
    run_raw_snapshot_intake_precheck(
        bridge_config_path=args.bridge_config,
        manifest_path=args.manifest_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
