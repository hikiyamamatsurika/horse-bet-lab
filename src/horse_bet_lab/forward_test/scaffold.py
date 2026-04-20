from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.forward_test.raw_snapshot_intake import (
    build_default_raw_snapshot_intake_manifest,
    default_intake_manifest_path,
    write_raw_snapshot_intake_manifest,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TEMPLATE_DIR = REPO_ROOT / "configs" / "templates"
DEFAULT_CANDIDATE_LOGIC_ID = "guard_0_01_plus_proxy_domain_overlay"
DEFAULT_FALLBACK_LOGIC_ID = "no_bet_guard_stronger"
DEFAULT_THRESHOLD = 0.08
DEFAULT_STAKE_PER_BET = 100.0
DEFAULT_SURCHARGE = 0.01
DEFAULT_INPUT_SCHEMA_VERSION = "place_forward_test_input_v1"
DEFAULT_BRIDGE_SOURCE_NAME = "replace_with_source_name"
DEFAULT_BRIDGE_SOURCE_URL = "https://replace.example/source"
DEFAULT_BRIDGE_SOURCE_TIMESTAMP = "2026-04-20T15:30:00+09:00"
DEFAULT_ODDS_OBSERVATION_TIMESTAMP = "2026-04-20T15:30:00+09:00"
DEFAULT_CARRIER_IDENTITY = "place_forward_live_snapshot_v1"
DEFAULT_RETRY_COUNT = 1
DEFAULT_TIMEOUT_SECONDS = 15.0
DEFAULT_POPULARITY_INPUT_SOURCE = "replace_with_popularity_source_or_leave_same_as_source"


@dataclass(frozen=True)
class PlaceForwardScaffoldConfig:
    unit_id: str
    raw_input_path: Path
    contract_output_path: Path
    pre_race_output_dir: Path
    reconciliation_output_dir: Path
    dataset_path: Path
    duckdb_path: Path
    model_version: str
    candidate_logic_id: str
    fallback_logic_id: str
    threshold: float
    settled_as_of: str
    config_dir: Path
    bridge_config_path: Path
    pre_race_config_path: Path
    reconciliation_config_path: Path
    notes_dir: Path
    input_source_name: str
    input_source_url: str
    input_source_timestamp: str
    odds_observation_timestamp: str
    carrier_identity: str
    retry_count: int
    timeout_seconds: float
    popularity_input_source: str
    force: bool


@dataclass(frozen=True)
class PlaceForwardScaffoldResult:
    bridge_config_path: Path
    pre_race_config_path: Path
    reconciliation_config_path: Path
    intake_manifest_path: Path
    raw_dir: Path
    contract_dir: Path
    notes_dir: Path
    pre_race_output_dir: Path
    reconciliation_output_dir: Path


def build_scaffold_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate recurring rehearsal bridge/pre-race/reconciliation configs for one run unit.",
    )
    parser.add_argument("--unit-id", required=True, help="Rehearsal unit id, e.g. 20260426_example_meeting.")
    parser.add_argument("--dataset-path", type=Path, required=True, help="Reference model dataset parquet path.")
    parser.add_argument("--duckdb-path", type=Path, required=True, help="JRDB DuckDB path for reconciliation.")
    parser.add_argument("--model-version", required=True, help="Model version label written into pre-race config.")
    parser.add_argument("--settled-as-of", required=True, help="ISO-8601 timestamp for reconciliation settled_as_of.")
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path("configs/recurring_rehearsal"),
        help="Directory where generated runtime configs are written.",
    )
    parser.add_argument(
        "--raw-input-path",
        type=Path,
        help="Raw input CSV path for bridge source. Defaults to data/forward_test/runs/<unit_id>/raw/input_snapshot_raw.csv.",
    )
    parser.add_argument(
        "--contract-output-path",
        type=Path,
        help="Bridge contract CSV output path. Defaults to data/forward_test/runs/<unit_id>/contract/input_snapshot_<unit_id>.csv.",
    )
    parser.add_argument(
        "--pre-race-output-dir",
        type=Path,
        help="Pre-race artifact output dir. Defaults to data/artifacts/place_forward_test/<unit_id>/pre_race.",
    )
    parser.add_argument(
        "--reconciliation-output-dir",
        type=Path,
        help="Reconciliation artifact output dir. Defaults to data/artifacts/place_forward_test/<unit_id>/reconciliation.",
    )
    parser.add_argument(
        "--candidate-logic-id",
        default=DEFAULT_CANDIDATE_LOGIC_ID,
        help="BET logic candidate id. Defaults to the current hard-adopt candidate.",
    )
    parser.add_argument(
        "--fallback-logic-id",
        default=DEFAULT_FALLBACK_LOGIC_ID,
        help="BET logic fallback id. Defaults to the current hard-adopt fallback.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="BET logic threshold to write into the pre-race config.",
    )
    parser.add_argument("--input-source-name", default=DEFAULT_BRIDGE_SOURCE_NAME, help="Bridge source name.")
    parser.add_argument("--input-source-url", default=DEFAULT_BRIDGE_SOURCE_URL, help="Bridge source URL.")
    parser.add_argument(
        "--input-source-timestamp",
        default=DEFAULT_BRIDGE_SOURCE_TIMESTAMP,
        help="Bridge source timestamp in ISO-8601 format.",
    )
    parser.add_argument(
        "--odds-observation-timestamp",
        default=DEFAULT_ODDS_OBSERVATION_TIMESTAMP,
        help="Odds observation timestamp in ISO-8601 format.",
    )
    parser.add_argument(
        "--carrier-identity",
        default=DEFAULT_CARRIER_IDENTITY,
        help="Carrier identity to write into bridge config.",
    )
    parser.add_argument(
        "--retry-count",
        type=int,
        default=DEFAULT_RETRY_COUNT,
        help="Default retry count for bridge sources.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Default timeout seconds for bridge sources.",
    )
    parser.add_argument(
        "--popularity-input-source",
        default=DEFAULT_POPULARITY_INPUT_SOURCE,
        help="Popularity input source label kept as unresolved auxiliary metadata.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow overwriting existing generated runtime config files.",
    )
    return parser


def build_scaffold_config_from_args(args: argparse.Namespace) -> PlaceForwardScaffoldConfig:
    unit_id = validate_unit_id(str(args.unit_id))
    raw_input_path = args.raw_input_path or Path(
        f"data/forward_test/runs/{unit_id}/raw/input_snapshot_raw.csv"
    )
    contract_output_path = args.contract_output_path or Path(
        f"data/forward_test/runs/{unit_id}/contract/input_snapshot_{unit_id}.csv"
    )
    pre_race_output_dir = args.pre_race_output_dir or Path(
        f"data/artifacts/place_forward_test/{unit_id}/pre_race"
    )
    reconciliation_output_dir = args.reconciliation_output_dir or Path(
        f"data/artifacts/place_forward_test/{unit_id}/reconciliation"
    )
    config_dir = Path(args.config_dir)
    return PlaceForwardScaffoldConfig(
        unit_id=unit_id,
        raw_input_path=raw_input_path,
        contract_output_path=contract_output_path,
        pre_race_output_dir=pre_race_output_dir,
        reconciliation_output_dir=reconciliation_output_dir,
        dataset_path=Path(args.dataset_path),
        duckdb_path=Path(args.duckdb_path),
        model_version=str(args.model_version),
        candidate_logic_id=str(args.candidate_logic_id),
        fallback_logic_id=str(args.fallback_logic_id),
        threshold=float(args.threshold),
        settled_as_of=str(args.settled_as_of),
        config_dir=config_dir,
        bridge_config_path=config_dir / f"{unit_id}.bridge.toml",
        pre_race_config_path=config_dir / f"{unit_id}.pre_race.toml",
        reconciliation_config_path=config_dir / f"{unit_id}.reconciliation.toml",
        notes_dir=Path(f"data/forward_test/runs/{unit_id}/notes"),
        input_source_name=str(args.input_source_name),
        input_source_url=str(args.input_source_url),
        input_source_timestamp=str(args.input_source_timestamp),
        odds_observation_timestamp=str(args.odds_observation_timestamp),
        carrier_identity=str(args.carrier_identity),
        retry_count=int(args.retry_count),
        timeout_seconds=float(args.timeout_seconds),
        popularity_input_source=str(args.popularity_input_source),
        force=bool(args.force),
    )


def validate_unit_id(unit_id: str) -> str:
    cleaned = unit_id.strip()
    if not cleaned:
        raise ValueError("unit_id must be non-empty")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    invalid = sorted({character for character in cleaned if character not in allowed})
    if invalid:
        raise ValueError(
            "unit_id may contain only letters, digits, underscore, and hyphen; "
            f"got invalid characters: {invalid}"
        )
    return cleaned


def run_scaffold(config: PlaceForwardScaffoldConfig) -> PlaceForwardScaffoldResult:
    intake_manifest_path = default_intake_manifest_path(config.raw_input_path)
    ensure_outputs_do_not_exist(
        (
            config.bridge_config_path,
            config.pre_race_config_path,
            config.reconciliation_config_path,
            intake_manifest_path,
        ),
        force=config.force,
    )
    template_dir = DEFAULT_TEMPLATE_DIR
    bridge_text = render_bridge_template(
        (template_dir / "place_forward_snapshot_bridge_runtime.template.toml").read_text(
            encoding="utf-8"
        ),
        config,
    )
    pre_race_text = render_pre_race_template(
        (template_dir / "place_forward_test_phase1_runtime.template.toml").read_text(
            encoding="utf-8"
        ),
        config,
    )
    reconciliation_text = render_reconciliation_template(
        (template_dir / "place_forward_test_reconciliation_runtime.template.toml").read_text(
            encoding="utf-8"
        ),
        config,
    )

    config.config_dir.mkdir(parents=True, exist_ok=True)
    config.raw_input_path.parent.mkdir(parents=True, exist_ok=True)
    config.contract_output_path.parent.mkdir(parents=True, exist_ok=True)
    config.notes_dir.mkdir(parents=True, exist_ok=True)
    config.pre_race_output_dir.mkdir(parents=True, exist_ok=True)
    config.reconciliation_output_dir.mkdir(parents=True, exist_ok=True)

    config.bridge_config_path.write_text(bridge_text, encoding="utf-8")
    config.pre_race_config_path.write_text(pre_race_text, encoding="utf-8")
    config.reconciliation_config_path.write_text(reconciliation_text, encoding="utf-8")
    write_raw_snapshot_intake_manifest(
        intake_manifest_path,
        build_default_raw_snapshot_intake_manifest(
            unit_id=config.unit_id,
            raw_snapshot_path=config.raw_input_path,
            source_family=config.input_source_name,
            input_source_name=config.input_source_name,
            input_source_url=config.input_source_url,
            input_source_timestamp=config.input_source_timestamp,
            odds_observation_timestamp=config.odds_observation_timestamp,
            carrier_identity=config.carrier_identity,
        ),
    )

    return PlaceForwardScaffoldResult(
        bridge_config_path=config.bridge_config_path,
        pre_race_config_path=config.pre_race_config_path,
        reconciliation_config_path=config.reconciliation_config_path,
        intake_manifest_path=intake_manifest_path,
        raw_dir=config.raw_input_path.parent,
        contract_dir=config.contract_output_path.parent,
        notes_dir=config.notes_dir,
        pre_race_output_dir=config.pre_race_output_dir,
        reconciliation_output_dir=config.reconciliation_output_dir,
    )


def ensure_outputs_do_not_exist(paths: tuple[Path, ...], *, force: bool) -> None:
    existing_paths = [path for path in paths if path.exists()]
    if existing_paths and not force:
        raise FileExistsError(
            "run-unit scaffold refuses to overwrite existing files without --force: "
            f"{[str(path) for path in existing_paths]}"
        )


def render_bridge_template(template_text: str, config: PlaceForwardScaffoldConfig) -> str:
    rendered = template_text.replace("<unit_id>", config.unit_id)
    lines: list[str] = []
    for line in rendered.splitlines():
        stripped = line.strip()
        if stripped.startswith("output_path = "):
            lines.append(f'output_path = "{toml_string(config.contract_output_path)}"')
        elif stripped.startswith("path = "):
            lines.append(f'path = "{toml_string(config.raw_input_path)}"')
        elif stripped.startswith("input_source_name = "):
            lines.append(f'input_source_name = "{toml_string(config.input_source_name)}"')
        elif stripped.startswith("input_source_url = "):
            lines.append(f'input_source_url = "{toml_string(config.input_source_url)}"')
        elif stripped.startswith("input_source_timestamp = "):
            lines.append(
                f'input_source_timestamp = "{toml_string(config.input_source_timestamp)}"'
            )
        elif stripped.startswith("odds_observation_timestamp = "):
            lines.append(
                f'odds_observation_timestamp = "{toml_string(config.odds_observation_timestamp)}"'
            )
        elif stripped.startswith("carrier_identity = "):
            lines.append(f'carrier_identity = "{toml_string(config.carrier_identity)}"')
        elif stripped.startswith("default_retry_count = "):
            lines.append(f"default_retry_count = {config.retry_count}")
        elif stripped.startswith("default_timeout_seconds = "):
            timeout_value = int(config.timeout_seconds) if config.timeout_seconds.is_integer() else config.timeout_seconds
            lines.append(f"default_timeout_seconds = {timeout_value}")
        elif stripped.startswith("default_popularity_input_source = "):
            lines.append(
                f'default_popularity_input_source = "{toml_string(config.popularity_input_source)}"'
            )
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def render_pre_race_template(template_text: str, config: PlaceForwardScaffoldConfig) -> str:
    rendered = template_text.replace("<unit_id>", config.unit_id)
    lines: list[str] = []
    for line in rendered.splitlines():
        stripped = line.strip()
        if stripped.startswith("input_path = "):
            lines.append(f'input_path = "{toml_string(config.contract_output_path)}"')
        elif stripped.startswith("output_dir = "):
            lines.append(f'output_dir = "{toml_string(config.pre_race_output_dir)}"')
        elif stripped.startswith("dataset_path = "):
            lines.append(f'dataset_path = "{toml_string(config.dataset_path)}"')
        elif stripped.startswith("model_version = "):
            lines.append(f'model_version = "{toml_string(config.model_version)}"')
        elif stripped.startswith("threshold = "):
            lines.append(f"threshold = {config.threshold}")
        elif stripped.startswith("candidate_logic_id = "):
            lines.append(f'candidate_logic_id = "{toml_string(config.candidate_logic_id)}"')
        elif stripped.startswith("fallback_logic_id = "):
            lines.append(f'fallback_logic_id = "{toml_string(config.fallback_logic_id)}"')
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def render_reconciliation_template(template_text: str, config: PlaceForwardScaffoldConfig) -> str:
    rendered = template_text.replace("<unit_id>", config.unit_id)
    lines: list[str] = []
    for line in rendered.splitlines():
        stripped = line.strip()
        if stripped.startswith("forward_output_dir = "):
            lines.append(f'forward_output_dir = "{toml_string(config.pre_race_output_dir)}"')
        elif stripped.startswith("duckdb_path = "):
            lines.append(f'duckdb_path = "{toml_string(config.duckdb_path)}"')
        elif stripped.startswith("output_dir = "):
            lines.append(
                f'output_dir = "{toml_string(config.reconciliation_output_dir)}"'
            )
        elif stripped.startswith("settled_as_of = "):
            lines.append(f'settled_as_of = "{toml_string(config.settled_as_of)}"')
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def toml_string(path_or_text: Path | str) -> str:
    text = str(path_or_text).replace("\\", "\\\\").replace('"', '\\"')
    return text


def main() -> int:
    parser = build_scaffold_parser()
    args = parser.parse_args()
    run_scaffold(build_scaffold_config_from_args(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
