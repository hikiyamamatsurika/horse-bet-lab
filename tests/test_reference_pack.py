from __future__ import annotations

import json
import subprocess
from pathlib import Path

from horse_bet_lab.config import load_reference_pack_config
from horse_bet_lab.evaluation.reference_pack import write_reference_pack_manifest
from horse_bet_lab.evaluation.reference_pack_verify import (
    verify_reference_pack,
    verify_reference_pack_or_raise,
)


def test_load_reference_pack_config(tmp_path: Path) -> None:
    config_path = tmp_path / "reference_pack.toml"
    config_path.write_text(
        """
[analysis]
name = "reference_pack_test"
reference_label_guard_compare_config_path = "configs/a.toml"
reference_label_guard_uncertainty_config_path = "configs/b.toml"
reference_per_race_cap_sensitivity_config_path = "configs/c.toml"
output_dir = "data/artifacts/reference_pack_test"
model_name = "dual_market_logreg"
first_guard_name = "problematic_band_excluded"
extra_label_guard_name = "valid-selected"
ranking_rule_name = "current_consensus_ranking"
selection_rule_name = "current_valid_selected_label_guard"
stateful_stake_variant = "capped_fractional_kelly_like_per_race_cap"
mainline_per_race_cap_stake = 200
standard_initial_bankroll = 10000
reference_initial_bankrolls = [10000, 30000]
research_candidates = [
  "dual_market_histgb_small",
  "capped_fractional_kelly_like_drawdown_reduction",
]
""".strip(),
        encoding="utf-8",
    )

    config = load_reference_pack_config(config_path)

    assert config.name == "reference_pack_test"
    assert config.model_name == "dual_market_logreg"
    assert config.mainline_per_race_cap_stake == 200.0
    assert config.standard_initial_bankroll == 10000.0
    assert config.reference_initial_bankrolls == (10000.0, 30000.0)
    assert config.research_candidates == (
        "dual_market_histgb_small",
        "capped_fractional_kelly_like_drawdown_reduction",
    )


def test_mainline_reference_runbook_exists() -> None:
    runbook_path = Path("docs/mainline_reference_runbook.md")
    content = runbook_path.read_text(encoding="utf-8")

    assert "reference_pack_dual_market_logreg_mainline_2023_2025" in content
    assert "Popularity Carrier Decision" in content
    assert "unresolved_keep_legacy_for_non-mainline_only" in content
    assert "strategy_summary.csv" in content
    assert "oos_backtest_summary.csv" in content
    assert "uncertainty_summary.csv" in content
    assert "stateful_bankroll_summary.csv" in content
    assert "Current Reading" in content
    assert "race_date block bootstrap" in content
    assert "race_internal_permutation" in content
    assert "古い数値" in content


def test_reference_pack_manifest_verify_roundtrip(tmp_path: Path) -> None:
    config_path = tmp_path / "reference_pack.toml"
    config_path.write_text(
        """
[analysis]
name = "reference_pack_test"
reference_label_guard_compare_config_path = "configs/a.toml"
reference_label_guard_uncertainty_config_path = "configs/b.toml"
reference_per_race_cap_sensitivity_config_path = "configs/c.toml"
output_dir = "data/artifacts/reference_pack_test"
model_name = "dual_market_logreg"
first_guard_name = "problematic_band_excluded"
extra_label_guard_name = "valid-selected"
ranking_rule_name = "current_consensus_ranking"
selection_rule_name = "current_valid_selected_label_guard"
stateful_stake_variant = "capped_fractional_kelly_like_per_race_cap"
mainline_per_race_cap_stake = 200
standard_initial_bankroll = 10000
reference_initial_bankrolls = [10000, 30000]
research_candidates = []
""".strip(),
        encoding="utf-8",
    )
    pack_dir = tmp_path / "reference_pack"
    pack_dir.mkdir()
    (pack_dir / "strategy_summary.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    (pack_dir / "oos_backtest_summary.csv").write_text("c,d\n3,4\n", encoding="utf-8")

    manifest_path = write_reference_pack_manifest(pack_dir, config_path=config_path)
    assert manifest_path.exists()

    verified = verify_reference_pack(pack_dir)
    assert verified.is_valid is True
    assert verified.manifest_sha256
    assert {row.identifier for row in verified.rows if row.category == "artifact"} == {
        "strategy_summary.csv",
        "oos_backtest_summary.csv",
    }

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert all(set(entry) == {"file_name", "byte_size", "sha256"} for entry in payload["entries"])
    assert "referenced_configs" in payload
    assert "reference_config_sha256" in payload
    assert "dataset_parquets" in payload
    assert "code_commit_sha" in payload
    assert payload["code_commit_sha"]
    assert verified.expected_code_commit_sha == payload["code_commit_sha"]
    assert verified.actual_code_commit_sha == payload["code_commit_sha"]
    assert verified.code_commit_matches is True


def test_reference_pack_manifest_contains_current_git_commit() -> None:
    pack_dir = Path("data/artifacts/reference_pack_dual_market_logreg_mainline_2023_2025")
    manifest_path = pack_dir / "manifest.json"
    if not manifest_path.exists():
        return

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    actual_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        text=True,
    ).strip()

    assert payload["code_commit_sha"] == actual_commit
    verified = verify_reference_pack(pack_dir)
    assert verified.expected_code_commit_sha == actual_commit
    assert verified.actual_code_commit_sha == actual_commit
    assert verified.code_commit_matches is True


def test_reference_pack_verify_or_raise_fails_on_missing_artifact(tmp_path: Path) -> None:
    config_path = tmp_path / "reference_pack.toml"
    config_path.write_text(
        """
[analysis]
name = "reference_pack_test"
reference_label_guard_compare_config_path = "configs/a.toml"
reference_label_guard_uncertainty_config_path = "configs/b.toml"
reference_per_race_cap_sensitivity_config_path = "configs/c.toml"
output_dir = "data/artifacts/reference_pack_test"
model_name = "dual_market_logreg"
first_guard_name = "problematic_band_excluded"
extra_label_guard_name = "valid-selected"
ranking_rule_name = "current_consensus_ranking"
selection_rule_name = "current_valid_selected_label_guard"
stateful_stake_variant = "capped_fractional_kelly_like_per_race_cap"
mainline_per_race_cap_stake = 200
standard_initial_bankroll = 10000
reference_initial_bankrolls = [10000, 30000]
research_candidates = []
""".strip(),
        encoding="utf-8",
    )
    pack_dir = tmp_path / "reference_pack_missing"
    pack_dir.mkdir()
    artifact_path = pack_dir / "strategy_summary.csv"
    artifact_path.write_text("a,b\n1,2\n", encoding="utf-8")
    write_reference_pack_manifest(pack_dir, config_path=config_path)
    artifact_path.unlink()

    try:
        verify_reference_pack_or_raise(pack_dir)
    except RuntimeError as exc:
        assert "verification failed" in str(exc).lower()
    else:
        raise AssertionError("verify_reference_pack_or_raise should fail on missing artifact")


def test_reference_pack_verify_or_raise_fails_on_corrupt_manifest_entry(tmp_path: Path) -> None:
    config_path = tmp_path / "reference_pack.toml"
    config_path.write_text(
        """
[analysis]
name = "reference_pack_test"
reference_label_guard_compare_config_path = "configs/a.toml"
reference_label_guard_uncertainty_config_path = "configs/b.toml"
reference_per_race_cap_sensitivity_config_path = "configs/c.toml"
output_dir = "data/artifacts/reference_pack_test"
model_name = "dual_market_logreg"
first_guard_name = "problematic_band_excluded"
extra_label_guard_name = "valid-selected"
ranking_rule_name = "current_consensus_ranking"
selection_rule_name = "current_valid_selected_label_guard"
stateful_stake_variant = "capped_fractional_kelly_like_per_race_cap"
mainline_per_race_cap_stake = 200
standard_initial_bankroll = 10000
reference_initial_bankrolls = [10000, 30000]
research_candidates = []
""".strip(),
        encoding="utf-8",
    )
    pack_dir = tmp_path / "reference_pack_corrupt"
    pack_dir.mkdir()
    (pack_dir / "strategy_summary.csv").write_text("a,b\n1,2\n", encoding="utf-8")
    manifest_path = write_reference_pack_manifest(pack_dir, config_path=config_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["entries"][0]["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    try:
        verify_reference_pack_or_raise(pack_dir)
    except RuntimeError as exc:
        assert "verification failed" in str(exc).lower()
    else:
        raise AssertionError("verify_reference_pack_or_raise should fail on corrupt manifest")
