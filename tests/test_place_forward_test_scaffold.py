from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest

from horse_bet_lab.forward_test.reconciliation import load_reconciliation_config
from horse_bet_lab.forward_test.runner import load_config
from horse_bet_lab.forward_test.scaffold import (
    build_scaffold_config_from_args,
    run_scaffold,
    validate_unit_id,
)
from horse_bet_lab.forward_test.snapshot_bridge import load_snapshot_bridge_config


def make_args(tmp_path: Path, *, force: bool = False) -> Namespace:
    return Namespace(
        unit_id="20260426_example_meeting",
        dataset_path=tmp_path / "dataset.parquet",
        duckdb_path=tmp_path / "jrdb.duckdb",
        model_version="odds_only_logreg_is_place@scaffold-test",
        settled_as_of="2026-04-26T18:00:00+09:00",
        config_dir=tmp_path / "configs",
        raw_input_path=None,
        contract_output_path=None,
        pre_race_output_dir=None,
        reconciliation_output_dir=None,
        candidate_logic_id="guard_0_01_plus_proxy_domain_overlay",
        fallback_logic_id="no_bet_guard_stronger",
        threshold=0.08,
        input_source_name="keibalab_public_pre_race_odds",
        input_source_url="https://www.keibalab.jp/db/race/202604260611/odds.html",
        input_source_timestamp="2026-04-26T15:38:00+09:00",
        odds_observation_timestamp="2026-04-26T15:38:00+09:00",
        carrier_identity="place_forward_live_snapshot_v1",
        retry_count=1,
        timeout_seconds=15.0,
        popularity_input_source="keibalab_public_pre_race_odds",
        force=force,
    )


def test_scaffold_generates_three_runtime_configs_and_directories(tmp_path: Path) -> None:
    config = build_scaffold_config_from_args(make_args(tmp_path))

    result = run_scaffold(config)

    assert result.bridge_config_path.exists()
    assert result.pre_race_config_path.exists()
    assert result.reconciliation_config_path.exists()
    assert result.raw_dir.exists()
    assert result.contract_dir.exists()
    assert result.notes_dir.exists()
    assert result.pre_race_output_dir.exists()
    assert result.reconciliation_output_dir.exists()

    bridge_config = load_snapshot_bridge_config(result.bridge_config_path)
    assert bridge_config.output_path == config.contract_output_path
    assert bridge_config.sources[0].path == config.raw_input_path
    assert bridge_config.sources[0].input_source_name == "keibalab_public_pre_race_odds"

    pre_race_config = load_config(result.pre_race_config_path)
    assert pre_race_config.input_path == config.contract_output_path
    assert pre_race_config.output_dir == config.pre_race_output_dir
    assert pre_race_config.reference_model.dataset_path == config.dataset_path
    assert pre_race_config.reference_model.model_version == config.model_version
    assert pre_race_config.bet_logic.threshold == pytest.approx(0.08)

    reconciliation_config = load_reconciliation_config(result.reconciliation_config_path)
    assert reconciliation_config.forward_output_dir == config.pre_race_output_dir
    assert reconciliation_config.duckdb_path == config.duckdb_path
    assert reconciliation_config.output_dir == config.reconciliation_output_dir
    assert reconciliation_config.settled_as_of == "2026-04-26T18:00:00+09:00"


def test_scaffold_rejects_existing_runtime_configs_without_force(tmp_path: Path) -> None:
    config = build_scaffold_config_from_args(make_args(tmp_path))
    run_scaffold(config)

    with pytest.raises(FileExistsError, match="refuses to overwrite existing files"):
        run_scaffold(config)


def test_scaffold_force_overwrites_existing_runtime_configs(tmp_path: Path) -> None:
    config = build_scaffold_config_from_args(make_args(tmp_path))
    run_scaffold(config)

    bridge_before = config.bridge_config_path.read_text(encoding="utf-8")
    config.bridge_config_path.write_text("# overwritten\n", encoding="utf-8")

    forced = build_scaffold_config_from_args(make_args(tmp_path, force=True))
    run_scaffold(forced)

    bridge_after = config.bridge_config_path.read_text(encoding="utf-8")
    assert bridge_after != "# overwritten\n"
    assert bridge_after == bridge_before


@pytest.mark.parametrize("unit_id", ["", "2026/0426", "2026 0426", "2026?0426"])
def test_validate_unit_id_rejects_invalid_values(unit_id: str) -> None:
    with pytest.raises(ValueError):
        validate_unit_id(unit_id)
