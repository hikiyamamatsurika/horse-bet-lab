from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path

import pytest

from horse_bet_lab.forward_test.raw_snapshot_intake import (
    default_intake_manifest_path,
    run_raw_snapshot_intake_precheck,
)
from horse_bet_lab.forward_test.scaffold import (
    build_scaffold_config_from_args,
    run_scaffold,
)


def make_args(tmp_path: Path, *, force: bool = False) -> Namespace:
    return Namespace(
        unit_id="20260426_example_meeting",
        dataset_path=tmp_path / "dataset.parquet",
        duckdb_path=tmp_path / "jrdb.duckdb",
        model_version="odds_only_logreg_is_place@scaffold-test",
        settled_as_of="2026-04-26T18:00:00+09:00",
        config_dir=tmp_path / "configs",
        raw_input_path=tmp_path / "runs" / "20260426_example_meeting" / "raw" / "input_snapshot_raw.csv",
        contract_output_path=tmp_path / "runs" / "20260426_example_meeting" / "contract" / "input_snapshot_20260426_example_meeting.csv",
        pre_race_output_dir=tmp_path / "artifacts" / "20260426_example_meeting" / "pre_race",
        reconciliation_output_dir=tmp_path / "artifacts" / "20260426_example_meeting" / "reconciliation",
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


def _write_raw_snapshot_csv(path: Path, *, include_place_proxy: bool = True) -> None:
    header = ["race_key", "horse_number", "win_odds"]
    if include_place_proxy:
        header.append("place_basis_odds_proxy")
        row = ["20250420", "2", "2.4", "1.3"]
    else:
        row = ["20250420", "2", "2.4"]
    path.write_text(",".join(header) + "\n" + ",".join(row) + "\n", encoding="utf-8")


def test_raw_snapshot_intake_precheck_passes_for_scaffold_output(tmp_path: Path) -> None:
    config = build_scaffold_config_from_args(make_args(tmp_path))
    result = run_scaffold(config)
    _write_raw_snapshot_csv(config.raw_input_path)

    precheck = run_raw_snapshot_intake_precheck(bridge_config_path=result.bridge_config_path)

    assert precheck.manifest_path == result.intake_manifest_path
    assert precheck.raw_snapshot_path == config.raw_input_path
    assert precheck.unit_id == config.unit_id
    assert "place_basis_odds_proxy" in precheck.header_columns


def test_raw_snapshot_intake_precheck_rejects_missing_place_columns(tmp_path: Path) -> None:
    config = build_scaffold_config_from_args(make_args(tmp_path))
    result = run_scaffold(config)
    _write_raw_snapshot_csv(config.raw_input_path, include_place_proxy=False)

    with pytest.raises(ValueError, match="must contain place odds columns"):
        run_raw_snapshot_intake_precheck(bridge_config_path=result.bridge_config_path)


def test_raw_snapshot_intake_precheck_rejects_manifest_bridge_mismatch(tmp_path: Path) -> None:
    config = build_scaffold_config_from_args(make_args(tmp_path))
    result = run_scaffold(config)
    _write_raw_snapshot_csv(config.raw_input_path)

    manifest_payload = json.loads(result.intake_manifest_path.read_text(encoding="utf-8"))
    manifest_payload["input_source_name"] = "mismatched_source_name"
    result.intake_manifest_path.write_text(
        json.dumps(manifest_payload, ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="does not match bridge config"):
        run_raw_snapshot_intake_precheck(bridge_config_path=result.bridge_config_path)


def test_scaffold_default_manifest_path_uses_raw_directory(tmp_path: Path) -> None:
    raw_snapshot_path = tmp_path / "runs" / "20260426_example_meeting" / "raw" / "input_snapshot_raw.csv"
    assert default_intake_manifest_path(raw_snapshot_path) == raw_snapshot_path.parent / "raw_snapshot_intake_manifest.json"
