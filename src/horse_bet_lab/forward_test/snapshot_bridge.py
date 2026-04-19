from __future__ import annotations

import argparse
import csv
import json
import hashlib
import tomllib
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path

from horse_bet_lab.features.provenance import build_feature_provenance_payload, write_feature_provenance_sidecar
from horse_bet_lab.forward_test.contracts import (
    PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS,
    PLACE_FORWARD_TEST_SNAPSHOT_STATUSES,
    PlaceForwardInputRecord,
    validate_place_forward_input_records,
)


@dataclass(frozen=True)
class SnapshotBridgeColumns:
    race_key: str = "race_key"
    horse_number: str = "horse_number"
    win_odds: str = "win_odds"
    place_basis_odds: str = "place_basis_odds"
    place_basis_odds_proxy: str = "place_basis_odds_proxy"
    place_odds_min: str = "place_odds_min"
    place_odds_max: str = "place_odds_max"
    popularity: str = "popularity"
    odds_observation_timestamp: str = "odds_observation_timestamp"
    input_source_name: str = "input_source_name"
    input_source_url: str = "input_source_url"
    input_source_timestamp: str = "input_source_timestamp"
    carrier_identity: str = "carrier_identity"
    snapshot_status: str = "snapshot_status"
    retry_count: str = "retry_count"
    timeout_seconds: str = "timeout_seconds"
    snapshot_failure_reason: str = "snapshot_failure_reason"
    popularity_input_source: str = "popularity_input_source"


@dataclass(frozen=True)
class SnapshotBridgeSourceConfig:
    path: Path
    odds_observation_timestamp: str | None
    input_source_name: str | None
    input_source_url: str | None
    input_source_timestamp: str | None
    carrier_identity: str | None
    default_retry_count: int
    default_timeout_seconds: float | None
    default_popularity_input_source: str | None


@dataclass(frozen=True)
class PlaceForwardSnapshotBridgeConfig:
    name: str
    output_path: Path
    columns: SnapshotBridgeColumns
    strict_race_key: bool
    infer_snapshot_status: bool
    write_json_copy: bool
    sources: tuple[SnapshotBridgeSourceConfig, ...]


@dataclass(frozen=True)
class PlaceForwardSnapshotBridgeResult:
    output_path: Path
    record_count: int
    source_count: int
    snapshot_status_counts: dict[str, int]


def build_snapshot_bridge_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert raw/live-ish place snapshot rows into the Phase 1 forward-test contract CSV.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a place forward snapshot bridge TOML config.",
    )
    return parser


def load_snapshot_bridge_config(path: Path) -> PlaceForwardSnapshotBridgeConfig:
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    section = payload["place_forward_snapshot_bridge"]
    columns_payload = section.get("columns", {})
    columns = SnapshotBridgeColumns(
        race_key=str(columns_payload.get("race_key", "race_key")),
        horse_number=str(columns_payload.get("horse_number", "horse_number")),
        win_odds=str(columns_payload.get("win_odds", "win_odds")),
        place_basis_odds=str(columns_payload.get("place_basis_odds", "place_basis_odds")),
        place_basis_odds_proxy=str(
            columns_payload.get("place_basis_odds_proxy", "place_basis_odds_proxy")
        ),
        place_odds_min=str(columns_payload.get("place_odds_min", "place_odds_min")),
        place_odds_max=str(columns_payload.get("place_odds_max", "place_odds_max")),
        popularity=str(columns_payload.get("popularity", "popularity")),
        odds_observation_timestamp=str(
            columns_payload.get("odds_observation_timestamp", "odds_observation_timestamp")
        ),
        input_source_name=str(columns_payload.get("input_source_name", "input_source_name")),
        input_source_url=str(columns_payload.get("input_source_url", "input_source_url")),
        input_source_timestamp=str(
            columns_payload.get("input_source_timestamp", "input_source_timestamp")
        ),
        carrier_identity=str(columns_payload.get("carrier_identity", "carrier_identity")),
        snapshot_status=str(columns_payload.get("snapshot_status", "snapshot_status")),
        retry_count=str(columns_payload.get("retry_count", "retry_count")),
        timeout_seconds=str(columns_payload.get("timeout_seconds", "timeout_seconds")),
        snapshot_failure_reason=str(
            columns_payload.get("snapshot_failure_reason", "snapshot_failure_reason")
        ),
        popularity_input_source=str(
            columns_payload.get("popularity_input_source", "popularity_input_source")
        ),
    )

    source_entries = section.get("sources", [])
    if not source_entries:
        raise ValueError("place_forward_snapshot_bridge config requires at least one [[...sources]] entry")

    sources: list[SnapshotBridgeSourceConfig] = []
    for entry in source_entries:
        sources.append(
            SnapshotBridgeSourceConfig(
                path=Path(str(entry["path"])),
                odds_observation_timestamp=_optional_config_text(entry.get("odds_observation_timestamp")),
                input_source_name=_optional_config_text(entry.get("input_source_name")),
                input_source_url=_optional_config_text(entry.get("input_source_url")),
                input_source_timestamp=_optional_config_text(entry.get("input_source_timestamp")),
                carrier_identity=_optional_config_text(entry.get("carrier_identity"))
                or "place_forward_live_snapshot_v1",
                default_retry_count=int(entry.get("default_retry_count", 1)),
                default_timeout_seconds=(
                    float(entry["default_timeout_seconds"])
                    if entry.get("default_timeout_seconds") is not None
                    else None
                ),
                default_popularity_input_source=_optional_config_text(
                    entry.get("default_popularity_input_source")
                ),
            )
        )

    return PlaceForwardSnapshotBridgeConfig(
        name=str(section["name"]),
        output_path=Path(str(section["output_path"])),
        columns=columns,
        strict_race_key=bool(section.get("strict_race_key", True)),
        infer_snapshot_status=bool(section.get("infer_snapshot_status", True)),
        write_json_copy=bool(section.get("write_json_copy", True)),
        sources=tuple(sources),
    )


def run_snapshot_bridge(config: PlaceForwardSnapshotBridgeConfig) -> PlaceForwardSnapshotBridgeResult:
    records = build_contract_records(config)
    write_csv_records(config.output_path, records)
    if config.write_json_copy:
        write_json_records(config.output_path.with_suffix(".json"), records)
    write_bridge_manifest(config, records, config.output_path.with_suffix(".manifest.json"))
    write_bridge_summary(config, records, config.output_path.with_suffix(".summary.txt"))
    return PlaceForwardSnapshotBridgeResult(
        output_path=config.output_path,
        record_count=len(records),
        source_count=len(config.sources),
        snapshot_status_counts=count_snapshot_statuses(records),
    )


def build_contract_records(
    config: PlaceForwardSnapshotBridgeConfig,
) -> tuple[PlaceForwardInputRecord, ...]:
    records: list[PlaceForwardInputRecord] = []
    for source in config.sources:
        if not source.path.exists():
            raise FileNotFoundError(f"snapshot bridge source CSV does not exist: {source.path}")
        with source.path.open(encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            if reader.fieldnames is None:
                raise ValueError(f"snapshot bridge source CSV is missing a header row: {source.path}")
            for row_index, row in enumerate(reader, start=1):
                records.append(build_contract_record_from_row(row, row_index=row_index, source=source, config=config))
    if not records:
        raise ValueError("snapshot bridge produced no rows from the configured sources")
    return validate_place_forward_input_records(records)


def build_contract_record_from_row(
    row: dict[str, str],
    *,
    row_index: int,
    source: SnapshotBridgeSourceConfig,
    config: PlaceForwardSnapshotBridgeConfig,
) -> PlaceForwardInputRecord:
    cleaned = {key: (value or "").strip() for key, value in row.items() if key is not None}
    columns = config.columns

    def row_text(column_name: str) -> str | None:
        value = cleaned.get(column_name, "")
        return value if value != "" else None

    def parse_float(raw_value: str | None, *, field_name: str) -> float | None:
        if raw_value is None:
            return None
        try:
            return float(raw_value)
        except ValueError as exc:
            raise ValueError(
                f"snapshot bridge row {row_index} in {source.path} has non-float {field_name!r}: {raw_value!r}"
            ) from exc

    def parse_int(raw_value: str | None, *, field_name: str) -> int | None:
        if raw_value is None:
            return None
        try:
            return int(raw_value)
        except ValueError as exc:
            raise ValueError(
                f"snapshot bridge row {row_index} in {source.path} has non-integer {field_name!r}: {raw_value!r}"
            ) from exc

    race_key = row_text(columns.race_key)
    if race_key is None:
        raise ValueError(
            f"snapshot bridge row {row_index} in {source.path} is missing required race_key"
        )
    if config.strict_race_key and (len(race_key) != 8 or not race_key.isdigit()):
        raise ValueError(
            "snapshot bridge requires an explicit 8-digit race_key; "
            f"row {row_index} in {source.path} has {race_key!r}"
        )

    horse_number = parse_int(row_text(columns.horse_number), field_name=columns.horse_number)
    if horse_number is None:
        raise ValueError(
            f"snapshot bridge row {row_index} in {source.path} is missing required horse_number"
        )

    win_odds = parse_float(row_text(columns.win_odds), field_name=columns.win_odds)
    place_basis_odds = _resolve_place_basis_odds(
        cleaned,
        columns=columns,
        source=source,
        row_index=row_index,
    )
    popularity = parse_int(row_text(columns.popularity), field_name=columns.popularity)

    explicit_status = row_text(columns.snapshot_status)
    if explicit_status is not None and explicit_status not in PLACE_FORWARD_TEST_SNAPSHOT_STATUSES:
        raise ValueError(
            "snapshot bridge row "
            f"{row_index} in {source.path} has unsupported snapshot_status {explicit_status!r}; "
            f"expected one of {sorted(PLACE_FORWARD_TEST_SNAPSHOT_STATUSES)}"
        )

    snapshot_status = infer_snapshot_status(
        explicit_status=explicit_status,
        infer_snapshot_status=config.infer_snapshot_status,
        win_odds=win_odds,
        place_basis_odds=place_basis_odds,
    )
    snapshot_failure_reason = row_text(columns.snapshot_failure_reason)
    if snapshot_status == "ok" and (win_odds is None or place_basis_odds is None):
        snapshot_status = "required_odds_missing"
    if snapshot_status != "ok":
        snapshot_failure_reason = snapshot_failure_reason or build_default_failure_reason(
            snapshot_status=snapshot_status,
            win_odds=win_odds,
            place_basis_odds=place_basis_odds,
        )

    odds_observation_timestamp = (
        row_text(columns.odds_observation_timestamp) or source.odds_observation_timestamp or ""
    )
    input_source_name = row_text(columns.input_source_name) or source.input_source_name or ""
    input_source_url = row_text(columns.input_source_url) or source.input_source_url
    input_source_timestamp = (
        row_text(columns.input_source_timestamp) or source.input_source_timestamp
    )
    carrier_identity = row_text(columns.carrier_identity) or source.carrier_identity or ""
    retry_count = parse_int(row_text(columns.retry_count), field_name=columns.retry_count)
    if retry_count is None:
        retry_count = source.default_retry_count
    timeout_seconds = parse_float(row_text(columns.timeout_seconds), field_name=columns.timeout_seconds)
    if timeout_seconds is None:
        timeout_seconds = source.default_timeout_seconds

    popularity_input_source = None
    popularity_contract_status = None
    if popularity is not None:
        popularity_input_source = (
            row_text(columns.popularity_input_source)
            or source.default_popularity_input_source
            or input_source_name
        )
        popularity_contract_status = PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS

    return PlaceForwardInputRecord(
        race_key=race_key,
        horse_number=horse_number,
        win_odds=win_odds,
        place_basis_odds=place_basis_odds,
        popularity=popularity,
        odds_observation_timestamp=odds_observation_timestamp,
        input_source_name=input_source_name,
        input_source_url=input_source_url,
        input_source_timestamp=input_source_timestamp,
        carrier_identity=carrier_identity,
        snapshot_status=snapshot_status,
        retry_count=retry_count,
        timeout_seconds=timeout_seconds,
        snapshot_failure_reason=snapshot_failure_reason,
        popularity_input_source=popularity_input_source,
        popularity_contract_status=popularity_contract_status,
        input_schema_version=PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    )


def infer_snapshot_status(
    *,
    explicit_status: str | None,
    infer_snapshot_status: bool,
    win_odds: float | None,
    place_basis_odds: float | None,
) -> str:
    if explicit_status is not None:
        return explicit_status
    if not infer_snapshot_status:
        raise ValueError(
            "snapshot_status must be present in the raw snapshot row when infer_snapshot_status=false"
        )
    if win_odds is not None and place_basis_odds is not None:
        return "ok"
    return "required_odds_missing"


def build_default_failure_reason(
    *,
    snapshot_status: str,
    win_odds: float | None,
    place_basis_odds: float | None,
) -> str:
    if snapshot_status == "required_odds_missing":
        missing_fields: list[str] = []
        if win_odds is None:
            missing_fields.append("win_odds")
        if place_basis_odds is None:
            missing_fields.append("place_basis_odds")
        return "required odds missing in raw snapshot input: " + ", ".join(missing_fields)
    if snapshot_status == "timeout":
        return "timed out while waiting for final odds snapshot"
    if snapshot_status == "retry_exhausted":
        return "retry budget exhausted before a valid odds snapshot was captured"
    return "snapshot source did not produce a valid odds record"


def _resolve_place_basis_odds(
    cleaned: dict[str, str],
    *,
    columns: SnapshotBridgeColumns,
    source: SnapshotBridgeSourceConfig,
    row_index: int,
) -> float | None:
    direct_value = cleaned.get(columns.place_basis_odds, "").strip()
    if direct_value != "":
        try:
            return float(direct_value)
        except ValueError as exc:
            raise ValueError(
                f"snapshot bridge row {row_index} in {source.path} has non-float "
                f"{columns.place_basis_odds!r}: {direct_value!r}"
            ) from exc

    proxy_value = cleaned.get(columns.place_basis_odds_proxy, "").strip()
    if proxy_value != "":
        try:
            return float(proxy_value)
        except ValueError as exc:
            raise ValueError(
                f"snapshot bridge row {row_index} in {source.path} has non-float "
                f"{columns.place_basis_odds_proxy!r}: {proxy_value!r}"
            ) from exc

    min_value = cleaned.get(columns.place_odds_min, "").strip()
    max_value = cleaned.get(columns.place_odds_max, "").strip()
    if min_value == "" and max_value == "":
        return None
    if min_value == "" or max_value == "":
        return None
    try:
        return (float(min_value) + float(max_value)) / 2.0
    except ValueError as exc:
        raise ValueError(
            f"snapshot bridge row {row_index} in {source.path} has non-float "
            f"{columns.place_odds_min!r}/{columns.place_odds_max!r}: {min_value!r}/{max_value!r}"
        ) from exc


def write_csv_records(path: Path, records: tuple[PlaceForwardInputRecord, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = tuple(field.name for field in fields(records[0]))
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def write_json_records(path: Path, records: tuple[PlaceForwardInputRecord, ...]) -> None:
    path.write_text(
        json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def count_snapshot_statuses(records: tuple[PlaceForwardInputRecord, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        counts[record.snapshot_status] = counts.get(record.snapshot_status, 0) + 1
    return counts


def write_bridge_manifest(
    config: PlaceForwardSnapshotBridgeConfig,
    records: tuple[PlaceForwardInputRecord, ...],
    path: Path,
) -> None:
    payload = {
        "run_name": config.name,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "output_path": str(config.output_path),
        "output_csv_hash": hashlib.sha256(config.output_path.read_bytes()).hexdigest(),
        "record_count": len(records),
        "source_count": len(config.sources),
        "source_paths": [str(source.path) for source in config.sources],
        "snapshot_status_counts": count_snapshot_statuses(records),
        "input_schema_version": PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
        "strict_race_key": config.strict_race_key,
        "infer_snapshot_status": config.infer_snapshot_status,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    write_feature_provenance_sidecar(
        path,
        build_feature_provenance_payload(
            artifact_kind="place_forward_snapshot_bridge_manifest_json",
            generated_by="horse_bet_lab.forward_test.snapshot_bridge.run_snapshot_bridge",
            config_identifier=config.name,
            model_feature_columns=(),
            artifact_path=str(path),
            extra={
                "input_schema_version": PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
                "source_paths": [str(source.path) for source in config.sources],
            },
        ),
    )


def write_bridge_summary(
    config: PlaceForwardSnapshotBridgeConfig,
    records: tuple[PlaceForwardInputRecord, ...],
    path: Path,
) -> None:
    snapshot_status_counts = count_snapshot_statuses(records)
    lines = [
        f"name={config.name}",
        f"output_path={config.output_path}",
        f"record_count={len(records)}",
        f"source_count={len(config.sources)}",
        "snapshot_status_counts:",
    ]
    for key in sorted(snapshot_status_counts):
        lines.append(f"  - {key}: {snapshot_status_counts[key]}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _optional_config_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def main() -> None:
    args = build_snapshot_bridge_parser().parse_args()
    config = load_snapshot_bridge_config(args.config)
    result = run_snapshot_bridge(config)
    counts = ", ".join(
        f"{key}={value}" for key, value in sorted(result.snapshot_status_counts.items())
    )
    print(
        "Place forward snapshot bridge completed: "
        f"name={config.name} records={result.record_count} "
        f"sources={result.source_count} output_path={result.output_path} {counts}"
    )


if __name__ == "__main__":
    main()
