from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

HANDOFF_MODE_NONE = "none"
HANDOFF_MODE_FORWARD_PRE_RACE_CONTRACT_LIKE = "forward_pre_race_contract_like_csv_v1"
HANDOFF_MODE_FORWARD_PRE_RACE_OZ = "forward_pre_race_oz_v1"
HANDOFF_MODE_FORWARD_PRE_RACE_TYB_OZ = "forward_pre_race_tyb_oz_v1"
SUPPORTED_HANDOFF_MODES = frozenset(
    {
        HANDOFF_MODE_NONE,
        HANDOFF_MODE_FORWARD_PRE_RACE_CONTRACT_LIKE,
        HANDOFF_MODE_FORWARD_PRE_RACE_OZ,
        HANDOFF_MODE_FORWARD_PRE_RACE_TYB_OZ,
    }
)


@dataclass(frozen=True)
class JRDBArchiveTrigger:
    name: str
    source_uri: str
    expected_sha256: str | None = None
    archive_kind: str | None = None


@dataclass(frozen=True)
class JRDBForwardPreRaceHandoffConfig:
    unit_id: str
    source_path: Path | None
    dataset_path: Path
    duckdb_path: Path
    model_version: str
    settled_as_of: str
    input_source_name: str
    input_source_url: str
    input_source_timestamp: str
    odds_observation_timestamp: str
    popularity_input_source: str


@dataclass(frozen=True)
class JRDBHandoffConfig:
    mode: str
    ingest_ready_files: bool
    duckdb_path: Path | None = None
    pre_race: JRDBForwardPreRaceHandoffConfig | None = None


@dataclass(frozen=True)
class JRDBAutoIngestionTrigger:
    trigger_kind: str
    detected_at: str
    archives: tuple[JRDBArchiveTrigger, ...]
    handoff: JRDBHandoffConfig
    message_id: str | None = None


class EmailTriggerWatcher(Protocol):
    def poll(self) -> tuple[JRDBAutoIngestionTrigger, ...]:
        ...


@dataclass(frozen=True)
class FixtureTriggerWatcher:
    manifest_paths: tuple[Path, ...]

    def poll(self) -> tuple[JRDBAutoIngestionTrigger, ...]:
        return tuple(load_trigger_manifest(path) for path in self.manifest_paths)


def load_trigger_manifest(path: Path) -> JRDBAutoIngestionTrigger:
    payload = json.loads(path.read_text(encoding="utf-8"))
    archives = tuple(
        JRDBArchiveTrigger(
            name=str(entry["name"]),
            source_uri=str(entry["source_uri"]),
            expected_sha256=_optional_text(entry.get("expected_sha256")),
            archive_kind=_optional_text(entry.get("archive_kind")),
        )
        for entry in payload.get("archives", [])
    )
    if not archives:
        raise ValueError("JRDB auto-ingestion trigger manifest requires at least one archive")

    handoff_payload = payload.get("handoff", {})
    mode = str(handoff_payload.get("mode", HANDOFF_MODE_NONE))
    if mode not in SUPPORTED_HANDOFF_MODES:
        raise ValueError(
            f"unsupported JRDB handoff mode {mode!r}; "
            f"expected one of {sorted(SUPPORTED_HANDOFF_MODES)}"
        )

    pre_race_payload = (
        handoff_payload
        if mode
        in {
            HANDOFF_MODE_FORWARD_PRE_RACE_CONTRACT_LIKE,
            HANDOFF_MODE_FORWARD_PRE_RACE_OZ,
            HANDOFF_MODE_FORWARD_PRE_RACE_TYB_OZ,
        }
        else None
    )
    pre_race = (
        JRDBForwardPreRaceHandoffConfig(
            unit_id=str(pre_race_payload["unit_id"]),
            source_path=(
                Path(str(pre_race_payload["source_path"]))
                if pre_race_payload.get("source_path") is not None
                else None
            ),
            dataset_path=Path(str(pre_race_payload["dataset_path"])),
            duckdb_path=Path(str(pre_race_payload["duckdb_path"])),
            model_version=str(pre_race_payload["model_version"]),
            settled_as_of=str(pre_race_payload["settled_as_of"]),
            input_source_name=str(pre_race_payload["input_source_name"]),
            input_source_url=str(pre_race_payload["input_source_url"]),
            input_source_timestamp=str(pre_race_payload["input_source_timestamp"]),
            odds_observation_timestamp=str(pre_race_payload["odds_observation_timestamp"]),
            popularity_input_source=str(pre_race_payload["popularity_input_source"]),
        )
        if pre_race_payload is not None
        else None
    )
    duckdb_path = (
        Path(str(handoff_payload["duckdb_path"]))
        if handoff_payload.get("duckdb_path") is not None
        else (pre_race.duckdb_path if pre_race is not None else None)
    )

    return JRDBAutoIngestionTrigger(
        trigger_kind=str(payload["trigger_kind"]),
        message_id=_optional_text(payload.get("message_id")),
        detected_at=str(payload["detected_at"]),
        archives=archives,
        handoff=JRDBHandoffConfig(
            mode=mode,
            ingest_ready_files=bool(handoff_payload.get("ingest_ready_files", True)),
            duckdb_path=duckdb_path,
            pre_race=pre_race,
        ),
    )


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text != "" else None
