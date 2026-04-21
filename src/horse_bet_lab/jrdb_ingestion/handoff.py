from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.forward_test.raw_snapshot_intake import run_raw_snapshot_intake_precheck
from horse_bet_lab.forward_test.raw_snapshot_prepare import (
    KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE,
    run_raw_snapshot_prepare,
)
from horse_bet_lab.forward_test.runner import load_config as load_pre_race_config
from horse_bet_lab.forward_test.runner import run_place_forward_test
from horse_bet_lab.forward_test.scaffold import PlaceForwardScaffoldConfig, run_scaffold
from horse_bet_lab.forward_test.snapshot_bridge import (
    load_snapshot_bridge_config,
    run_snapshot_bridge,
)
from horse_bet_lab.ingest.service import IngestionSummary, ingest_jrdb_directory
from horse_bet_lab.jrdb_ingestion.oz_pre_race_adapter import (
    discover_oz_source_paths,
    run_oz_pre_race_adapter,
)
from horse_bet_lab.jrdb_ingestion.trigger import (
    HANDOFF_MODE_FORWARD_PRE_RACE_CONTRACT_LIKE,
    HANDOFF_MODE_FORWARD_PRE_RACE_OZ,
    HANDOFF_MODE_FORWARD_PRE_RACE_TYB_OZ,
    HANDOFF_MODE_NONE,
    JRDBAutoIngestionTrigger,
    JRDBForwardPreRaceHandoffConfig,
)
from horse_bet_lab.jrdb_ingestion.tyb_oz_pre_race_adapter import (
    discover_tyb_source_paths,
    run_tyb_oz_pre_race_adapter,
)


@dataclass(frozen=True)
class JRDBHandoffResult:
    raw_target_dir: Path
    ingest_summary: IngestionSummary | None
    pre_race_output_dir: Path | None
    unit_id: str | None


def run_handoff(
    trigger: JRDBAutoIngestionTrigger,
    *,
    raw_target_dir: Path,
) -> JRDBHandoffResult:
    ingest_summary = None
    if trigger.handoff.ingest_ready_files:
        duckdb_path = trigger.handoff.duckdb_path
        if duckdb_path is None:
            raise ValueError("handoff.ingest_ready_files=true requires duckdb_path")
        ingest_summary = ingest_jrdb_directory(raw_dir=raw_target_dir, duckdb_path=duckdb_path)

    if trigger.handoff.mode == HANDOFF_MODE_NONE:
        return JRDBHandoffResult(
            raw_target_dir=raw_target_dir,
            ingest_summary=ingest_summary,
            pre_race_output_dir=None,
            unit_id=None,
        )
    if trigger.handoff.mode not in {
        HANDOFF_MODE_FORWARD_PRE_RACE_CONTRACT_LIKE,
        HANDOFF_MODE_FORWARD_PRE_RACE_OZ,
        HANDOFF_MODE_FORWARD_PRE_RACE_TYB_OZ,
    }:
        raise ValueError(f"unsupported handoff mode: {trigger.handoff.mode!r}")
    if trigger.handoff.pre_race is None:
        raise ValueError("forward pre-race handoff requires pre_race config")

    if trigger.handoff.mode == HANDOFF_MODE_FORWARD_PRE_RACE_CONTRACT_LIKE:
        pre_race_output_dir = _run_forward_pre_race_contract_like_handoff(trigger.handoff.pre_race)
    elif trigger.handoff.mode == HANDOFF_MODE_FORWARD_PRE_RACE_OZ:
        pre_race_output_dir = _run_forward_pre_race_oz_handoff(
            trigger.handoff.pre_race,
            raw_target_dir=raw_target_dir,
        )
    else:
        pre_race_output_dir = _run_forward_pre_race_tyb_oz_handoff(
            trigger.handoff.pre_race,
            raw_target_dir=raw_target_dir,
        )
    return JRDBHandoffResult(
        raw_target_dir=raw_target_dir,
        ingest_summary=ingest_summary,
        pre_race_output_dir=pre_race_output_dir,
        unit_id=trigger.handoff.pre_race.unit_id,
    )


def _run_forward_pre_race_contract_like_handoff(
    config: JRDBForwardPreRaceHandoffConfig,
) -> Path:
    if config.source_path is None:
        raise ValueError("forward_pre_race_contract_like_csv_v1 requires source_path")
    scaffold_result = run_scaffold(
        PlaceForwardScaffoldConfig(
            unit_id=config.unit_id,
            raw_input_path=Path(f"data/forward_test/runs/{config.unit_id}/raw/input_snapshot_raw.csv"),
            contract_output_path=Path(
                f"data/forward_test/runs/{config.unit_id}/contract/input_snapshot_{config.unit_id}.csv"
            ),
            pre_race_output_dir=Path(f"data/artifacts/place_forward_test/{config.unit_id}/pre_race"),
            reconciliation_output_dir=Path(
                f"data/artifacts/place_forward_test/{config.unit_id}/reconciliation"
            ),
            dataset_path=config.dataset_path,
            duckdb_path=config.duckdb_path,
            model_version=config.model_version,
            candidate_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
            threshold=0.08,
            settled_as_of=config.settled_as_of,
            config_dir=Path("configs/recurring_rehearsal"),
            bridge_config_path=Path(f"configs/recurring_rehearsal/{config.unit_id}.bridge.toml"),
            pre_race_config_path=Path(
                f"configs/recurring_rehearsal/{config.unit_id}.pre_race.toml"
            ),
            reconciliation_config_path=Path(
                f"configs/recurring_rehearsal/{config.unit_id}.reconciliation.toml"
            ),
            notes_dir=Path(f"data/forward_test/runs/{config.unit_id}/notes"),
            input_source_name=config.input_source_name,
            input_source_url=config.input_source_url,
            input_source_timestamp=config.input_source_timestamp,
            odds_observation_timestamp=config.odds_observation_timestamp,
            carrier_identity="place_forward_live_snapshot_v1",
            retry_count=1,
            timeout_seconds=15.0,
            popularity_input_source=config.popularity_input_source,
            force=True,
        )
    )
    run_raw_snapshot_prepare(
        preset_name=KNOWN_PRESET_PLACE_FORWARD_CONTRACT_LIKE,
        input_path=config.source_path,
        output_path=scaffold_result.raw_dir / "input_snapshot_raw.csv",
        force=True,
    )
    run_raw_snapshot_intake_precheck(bridge_config_path=scaffold_result.bridge_config_path)
    run_snapshot_bridge(load_snapshot_bridge_config(scaffold_result.bridge_config_path))
    run_place_forward_test(load_pre_race_config(scaffold_result.pre_race_config_path))
    return scaffold_result.pre_race_output_dir


def _run_forward_pre_race_oz_handoff(
    config: JRDBForwardPreRaceHandoffConfig,
    *,
    raw_target_dir: Path,
) -> Path:
    scaffold_result = run_scaffold(
        PlaceForwardScaffoldConfig(
            unit_id=config.unit_id,
            raw_input_path=Path(f"data/forward_test/runs/{config.unit_id}/raw/input_snapshot_raw.csv"),
            contract_output_path=Path(
                f"data/forward_test/runs/{config.unit_id}/contract/input_snapshot_{config.unit_id}.csv"
            ),
            pre_race_output_dir=Path(f"data/artifacts/place_forward_test/{config.unit_id}/pre_race"),
            reconciliation_output_dir=Path(
                f"data/artifacts/place_forward_test/{config.unit_id}/reconciliation"
            ),
            dataset_path=config.dataset_path,
            duckdb_path=config.duckdb_path,
            model_version=config.model_version,
            candidate_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
            threshold=0.08,
            settled_as_of=config.settled_as_of,
            config_dir=Path("configs/recurring_rehearsal"),
            bridge_config_path=Path(f"configs/recurring_rehearsal/{config.unit_id}.bridge.toml"),
            pre_race_config_path=Path(
                f"configs/recurring_rehearsal/{config.unit_id}.pre_race.toml"
            ),
            reconciliation_config_path=Path(
                f"configs/recurring_rehearsal/{config.unit_id}.reconciliation.toml"
            ),
            notes_dir=Path(f"data/forward_test/runs/{config.unit_id}/notes"),
            input_source_name=config.input_source_name,
            input_source_url=config.input_source_url,
            input_source_timestamp=config.input_source_timestamp,
            odds_observation_timestamp=config.odds_observation_timestamp,
            carrier_identity="place_forward_live_snapshot_v1",
            retry_count=1,
            timeout_seconds=15.0,
            popularity_input_source=config.popularity_input_source,
            force=True,
        )
    )
    run_oz_pre_race_adapter(
        source_paths=discover_oz_source_paths(raw_target_dir),
        output_path=scaffold_result.raw_dir / "input_snapshot_raw.csv",
        force=True,
    )
    run_raw_snapshot_intake_precheck(bridge_config_path=scaffold_result.bridge_config_path)
    run_snapshot_bridge(load_snapshot_bridge_config(scaffold_result.bridge_config_path))
    run_place_forward_test(load_pre_race_config(scaffold_result.pre_race_config_path))
    return scaffold_result.pre_race_output_dir


def _run_forward_pre_race_tyb_oz_handoff(
    config: JRDBForwardPreRaceHandoffConfig,
    *,
    raw_target_dir: Path,
) -> Path:
    scaffold_result = run_scaffold(
        PlaceForwardScaffoldConfig(
            unit_id=config.unit_id,
            raw_input_path=Path(f"data/forward_test/runs/{config.unit_id}/raw/input_snapshot_raw.csv"),
            contract_output_path=Path(
                f"data/forward_test/runs/{config.unit_id}/contract/input_snapshot_{config.unit_id}.csv"
            ),
            pre_race_output_dir=Path(f"data/artifacts/place_forward_test/{config.unit_id}/pre_race"),
            reconciliation_output_dir=Path(
                f"data/artifacts/place_forward_test/{config.unit_id}/reconciliation"
            ),
            dataset_path=config.dataset_path,
            duckdb_path=config.duckdb_path,
            model_version=config.model_version,
            candidate_logic_id="guard_0_01_plus_proxy_domain_overlay",
            fallback_logic_id="no_bet_guard_stronger",
            threshold=0.08,
            settled_as_of=config.settled_as_of,
            config_dir=Path("configs/recurring_rehearsal"),
            bridge_config_path=Path(f"configs/recurring_rehearsal/{config.unit_id}.bridge.toml"),
            pre_race_config_path=Path(
                f"configs/recurring_rehearsal/{config.unit_id}.pre_race.toml"
            ),
            reconciliation_config_path=Path(
                f"configs/recurring_rehearsal/{config.unit_id}.reconciliation.toml"
            ),
            notes_dir=Path(f"data/forward_test/runs/{config.unit_id}/notes"),
            input_source_name=config.input_source_name,
            input_source_url=config.input_source_url,
            input_source_timestamp=config.input_source_timestamp,
            odds_observation_timestamp=config.odds_observation_timestamp,
            carrier_identity="place_forward_live_snapshot_v1",
            retry_count=1,
            timeout_seconds=15.0,
            popularity_input_source=config.popularity_input_source,
            force=True,
        )
    )
    run_tyb_oz_pre_race_adapter(
        tyb_source_paths=discover_tyb_source_paths(raw_target_dir),
        oz_source_paths=discover_oz_source_paths(raw_target_dir),
        output_path=scaffold_result.raw_dir / "input_snapshot_raw.csv",
        force=True,
    )
    run_raw_snapshot_intake_precheck(bridge_config_path=scaffold_result.bridge_config_path)
    run_snapshot_bridge(load_snapshot_bridge_config(scaffold_result.bridge_config_path))
    run_place_forward_test(load_pre_race_config(scaffold_result.pre_race_config_path))
    return scaffold_result.pre_race_output_dir
