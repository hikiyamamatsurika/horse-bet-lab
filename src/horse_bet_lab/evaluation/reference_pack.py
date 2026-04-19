from __future__ import annotations

import hashlib
import json
import subprocess
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from horse_bet_lab.config import (
    ReferencePackConfig,
    load_reference_label_guard_compare_config,
    load_reference_label_guard_uncertainty_config,
    load_reference_per_race_cap_sensitivity_config,
)
from horse_bet_lab.evaluation.reference_guard_compare import GuardVariantGroupSummary
from horse_bet_lab.evaluation.reference_label_guard_compare import (
    run_reference_label_guard_compare,
)
from horse_bet_lab.evaluation.reference_label_guard_uncertainty import (
    run_reference_label_guard_uncertainty,
)
from horse_bet_lab.evaluation.reference_per_race_cap_sensitivity import (
    PerRaceCapSensitivityBootstrapRow,
    PerRaceCapSensitivityGroupSummary,
    PerRaceCapSensitivityPathRow,
    PerRaceCapSensitivitySummary,
    run_reference_per_race_cap_sensitivity,
)
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json
from horse_bet_lab.evaluation.reference_uncertainty import ReferenceUncertaintySummary


@dataclass(frozen=True)
class ReferencePackStrategySummary:
    reference_name: str
    model_name: str
    first_guard_name: str
    extra_label_guard_name: str
    ranking_rule_name: str
    selection_rule_name: str
    stateful_stake_variant: str
    mainline_per_race_cap_stake: float
    standard_initial_bankroll: float
    reference_initial_bankrolls: str
    research_candidates: str


@dataclass(frozen=True)
class ReferencePackResult:
    output_dir: Path
    strategy_summary: ReferencePackStrategySummary
    oos_summaries: tuple[GuardVariantGroupSummary, ...]
    uncertainty_summaries: tuple[ReferenceUncertaintySummary, ...]
    stateful_summaries: tuple[PerRaceCapSensitivitySummary, ...]
    stateful_yearly_summaries: tuple[PerRaceCapSensitivityGroupSummary, ...]
    stateful_monthly_summaries: tuple[PerRaceCapSensitivityGroupSummary, ...]
    stateful_equity_rows: tuple[PerRaceCapSensitivityPathRow, ...]
    stateful_bootstrap_rows: tuple[PerRaceCapSensitivityBootstrapRow, ...]
    manifest_path: Path


@dataclass(frozen=True)
class ReferencePackManifestEntry:
    file_name: str
    byte_size: int
    sha256: str


@dataclass(frozen=True)
class ReferencePackInputEntry:
    path: str
    byte_size: int
    sha256: str


def run_reference_pack(
    config: ReferencePackConfig,
    *,
    config_path: Path | None = None,
) -> ReferencePackResult:
    compare_config = load_reference_label_guard_compare_config(
        config.reference_label_guard_compare_config_path,
    )
    compare_result = run_reference_label_guard_compare(compare_config)

    uncertainty_config = load_reference_label_guard_uncertainty_config(
        config.reference_label_guard_uncertainty_config_path,
    )
    uncertainty_result = run_reference_label_guard_uncertainty(uncertainty_config)

    per_race_cap_config = load_reference_per_race_cap_sensitivity_config(
        config.reference_per_race_cap_sensitivity_config_path,
    )
    per_race_cap_result = run_reference_per_race_cap_sensitivity(per_race_cap_config)

    strategy_summary = ReferencePackStrategySummary(
        reference_name=config.name,
        model_name=config.model_name,
        first_guard_name=config.first_guard_name,
        extra_label_guard_name=config.extra_label_guard_name,
        ranking_rule_name=config.ranking_rule_name,
        selection_rule_name=config.selection_rule_name,
        stateful_stake_variant=config.stateful_stake_variant,
        mainline_per_race_cap_stake=config.mainline_per_race_cap_stake,
        standard_initial_bankroll=config.standard_initial_bankroll,
        reference_initial_bankrolls=",".join(
            str(int(value)) if float(value).is_integer() else str(value)
            for value in config.reference_initial_bankrolls
        ),
        research_candidates=",".join(config.research_candidates),
    )

    oos_summaries = tuple(
        row
        for row in compare_result.selected_test_rollup
        if row.variant == "selected_per_window"
    )
    uncertainty_summaries = (uncertainty_result.summary,)
    stateful_summaries = tuple(
        row
        for row in per_race_cap_result.summaries
        if row.per_race_cap_stake == config.mainline_per_race_cap_stake
        and row.initial_bankroll in config.reference_initial_bankrolls
    )
    stateful_yearly_summaries = tuple(
        row
        for row in per_race_cap_result.yearly_summaries
        if row.per_race_cap_stake == config.mainline_per_race_cap_stake
        and row.initial_bankroll in config.reference_initial_bankrolls
    )
    stateful_monthly_summaries = tuple(
        row
        for row in per_race_cap_result.monthly_summaries
        if row.per_race_cap_stake == config.mainline_per_race_cap_stake
        and row.initial_bankroll in config.reference_initial_bankrolls
    )
    stateful_equity_rows = tuple(
        row
        for row in per_race_cap_result.equity_rows
        if row.per_race_cap_stake == config.mainline_per_race_cap_stake
        and row.initial_bankroll in config.reference_initial_bankrolls
    )
    stateful_bootstrap_rows = tuple(
        row
        for row in per_race_cap_result.bootstrap_rows
        if row.per_race_cap_stake == config.mainline_per_race_cap_stake
        and row.initial_bankroll in config.reference_initial_bankrolls
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    result = ReferencePackResult(
        output_dir=config.output_dir,
        strategy_summary=strategy_summary,
        oos_summaries=oos_summaries,
        uncertainty_summaries=uncertainty_summaries,
        stateful_summaries=stateful_summaries,
        stateful_yearly_summaries=stateful_yearly_summaries,
        stateful_monthly_summaries=stateful_monthly_summaries,
        stateful_equity_rows=stateful_equity_rows,
        stateful_bootstrap_rows=stateful_bootstrap_rows,
        manifest_path=config.output_dir / "manifest.json",
    )
    write_csv(config.output_dir / "strategy_summary.csv", (result.strategy_summary,))
    write_json(config.output_dir / "strategy_summary.json", {"analysis": result.strategy_summary})
    write_csv(config.output_dir / "oos_backtest_summary.csv", result.oos_summaries)
    write_json(
        config.output_dir / "oos_backtest_summary.json",
        {"analysis": {"rows": result.oos_summaries}},
    )
    write_csv(config.output_dir / "uncertainty_summary.csv", result.uncertainty_summaries)
    write_json(
        config.output_dir / "uncertainty_summary.json",
        {"analysis": {"rows": result.uncertainty_summaries}},
    )
    write_csv(config.output_dir / "stateful_bankroll_summary.csv", result.stateful_summaries)
    write_json(
        config.output_dir / "stateful_bankroll_summary.json",
        {"analysis": {"rows": result.stateful_summaries}},
    )
    write_csv(
        config.output_dir / "stateful_yearly_profit.csv",
        result.stateful_yearly_summaries,
    )
    write_json(
        config.output_dir / "stateful_yearly_profit.json",
        {"analysis": {"rows": result.stateful_yearly_summaries}},
    )
    write_csv(
        config.output_dir / "stateful_monthly_profit.csv",
        result.stateful_monthly_summaries,
    )
    write_json(
        config.output_dir / "stateful_monthly_profit.json",
        {"analysis": {"rows": result.stateful_monthly_summaries}},
    )
    write_csv(config.output_dir / "stateful_equity_curve.csv", result.stateful_equity_rows)
    write_json(
        config.output_dir / "stateful_equity_curve.json",
        {"analysis": {"rows": result.stateful_equity_rows}},
    )
    write_csv(
        config.output_dir / "stateful_bootstrap_distribution.csv",
        result.stateful_bootstrap_rows,
    )
    write_json(
        config.output_dir / "stateful_bootstrap_distribution.json",
        {"analysis": {"rows": result.stateful_bootstrap_rows}},
    )
    write_reference_pack_manifest(config.output_dir, config_path=config_path)
    return result


def write_reference_pack_manifest(
    output_dir: Path,
    *,
    config_path: Path | None = None,
) -> Path:
    entries = build_reference_pack_manifest_entries(output_dir)
    referenced_configs = (
        build_reference_pack_input_entries(collect_referenced_config_paths(config_path))
        if config_path is not None
        else ()
    )
    dataset_parquets = (
        build_reference_pack_input_entries(collect_dataset_parquet_paths(config_path))
        if config_path is not None
        else ()
    )
    payload = {
        "pack_dir": output_dir.name,
        "entries": [
            {
                "file_name": entry.file_name,
                "byte_size": entry.byte_size,
                "sha256": entry.sha256,
            }
            for entry in entries
        ],
        "referenced_configs": [
            {
                "path": entry.path,
                "byte_size": entry.byte_size,
                "sha256": entry.sha256,
            }
            for entry in referenced_configs
        ],
        "reference_config_sha256": compute_combined_hash(referenced_configs),
        "dataset_parquets": [
            {
                "path": entry.path,
                "byte_size": entry.byte_size,
                "sha256": entry.sha256,
            }
            for entry in dataset_parquets
        ],
        "code_commit_sha": detect_git_commit_sha(),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest_path


def build_reference_pack_manifest_entries(
    output_dir: Path,
) -> tuple[ReferencePackManifestEntry, ...]:
    entries: list[ReferencePackManifestEntry] = []
    for path in sorted(output_dir.iterdir()):
        if not path.is_file() or path.name == "manifest.json":
            continue
        entries.append(
            ReferencePackManifestEntry(
                file_name=path.name,
                byte_size=path.stat().st_size,
                sha256=compute_sha256(path),
            ),
        )
    return tuple(entries)


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_reference_pack_input_entries(
    paths: tuple[Path, ...],
) -> tuple[ReferencePackInputEntry, ...]:
    return tuple(
        ReferencePackInputEntry(
            path=str(path),
            byte_size=path.stat().st_size,
            sha256=compute_sha256(path),
        )
        for path in paths
    )


def compute_combined_hash(entries: tuple[ReferencePackInputEntry, ...]) -> str | None:
    if not entries:
        return None
    digest = hashlib.sha256()
    for entry in entries:
        digest.update(entry.path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(entry.sha256.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def collect_referenced_config_paths(root_config_path: Path | None) -> tuple[Path, ...]:
    if root_config_path is None:
        return ()
    visited: set[Path] = set()
    ordered: list[Path] = []

    def walk(path: Path) -> None:
        resolved = path.resolve()
        if resolved in visited:
            return
        if not path.exists():
            return
        visited.add(resolved)
        ordered.append(path)
        with path.open("rb") as file:
            payload = tomllib.load(file)
        for child in discover_config_paths(payload, base_dir=path.parent):
            walk(child)

    walk(root_config_path)
    return tuple(ordered)


def discover_config_paths(payload: Any, *, base_dir: Path) -> tuple[Path, ...]:
    discovered: list[Path] = []

    def walk(node: Any, key_name: str | None = None) -> None:
        if isinstance(node, dict):
            for child_key, child_value in node.items():
                walk(child_value, key_name=str(child_key))
            return
        if isinstance(node, list):
            for child in node:
                walk(child, key_name=key_name)
            return
        if (
            key_name is not None
            and key_name.endswith("_config_path")
            and isinstance(node, str)
        ):
            discovered.append(resolve_reference_path(node, base_dir=base_dir))

    walk(payload)
    return tuple(sorted(set(discovered)))


def collect_dataset_parquet_paths(root_config_path: Path | None) -> tuple[Path, ...]:
    if root_config_path is None:
        return ()
    config_paths = collect_referenced_config_paths(root_config_path)
    discovered: set[Path] = set()
    for config_path in config_paths:
        with config_path.open("rb") as file:
            payload = tomllib.load(file)
        discovered.update(discover_dataset_paths(payload, base_dir=config_path.parent))
    return tuple(sorted(discovered))


def discover_dataset_paths(payload: Any, *, base_dir: Path) -> set[Path]:
    discovered: set[Path] = set()

    def walk(node: Any, key_name: str | None = None) -> None:
        if isinstance(node, dict):
            for child_key, child_value in node.items():
                walk(child_value, key_name=str(child_key))
            return
        if isinstance(node, list):
            for child in node:
                walk(child, key_name=key_name)
            return
        if not isinstance(node, str) or key_name is None:
            return
        if key_name == "dataset_path" and node.endswith(".parquet"):
            path = resolve_reference_path(node, base_dir=base_dir)
            if path.exists():
                discovered.add(path)
        if key_name == "rolling_predictions_path" and node.endswith(".csv"):
            predictions_path = resolve_reference_path(node, base_dir=base_dir)
            dataset_path = predictions_path.parent.parent / "dataset.parquet"
            if dataset_path.exists():
                discovered.add(dataset_path)

    walk(payload)
    return discovered


def detect_git_commit_sha() -> str | None:
    repo_root = Path(__file__).resolve().parents[3]
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip() or None


def resolve_reference_path(raw_path: str, *, base_dir: Path) -> Path:
    candidate = (base_dir / raw_path).resolve()
    if candidate.exists():
        return candidate
    return Path(raw_path).resolve()
