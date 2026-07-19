from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from horse_bet_lab.research.historical_ability_models import (
    SIGNAL_SUPPORTED,
    ComparisonDataset,
    ComparisonResult,
    InputAudit,
    run_model_comparison,
    write_comparison_result,
)
from horse_bet_lab.research.historical_signal_robustness import (
    ROBUSTNESS_DIAGNOSTIC_ONLY,
    ROBUSTNESS_NOT_CONFIRMED,
    RobustnessResult,
    run_signal_robustness,
    write_robustness_result,
)
from horse_bet_lab.research.popularity_carrier_comparison import (
    NO_POPULARITY,
    market_variant_dataset,
)
from horse_bet_lab.research.small_field_failure_audit import (
    SmallFieldAuditResult,
    run_small_field_failure_audit,
    write_small_field_audit,
)

REBASE_REPRODUCED = "NO_POPULARITY_REBASE_HISTORICAL_CONCLUSIONS_REPRODUCED"
REBASE_CHANGED = "NO_POPULARITY_REBASE_HISTORICAL_CONCLUSION_CHANGED"


@dataclass(frozen=True)
class NoPopularityRebaseResult:
    summary: dict[str, Any]
    phase651: ComparisonResult
    phase652: RobustnessResult
    phase653: SmallFieldAuditResult


def run_no_popularity_rebase(
    dataset: ComparisonDataset,
    input_audit: InputAudit,
    *,
    phase651_bootstrap_repetitions: int = 2_000,
    phase652_bootstrap_repetitions: int = 2_000,
    phase652_subgroup_bootstrap_repetitions: int = 500,
    phase653_bootstrap_repetitions: int = 1_000,
    phase653_minimum_bootstrap_races: int = 30,
) -> NoPopularityRebaseResult:
    safe_dataset = market_variant_dataset(dataset, NO_POPULARITY)
    phase651 = run_model_comparison(
        safe_dataset,
        input_audit,
        market_feature_names=("win_odds", "place_basis_odds"),
        bootstrap_repetitions=phase651_bootstrap_repetitions,
    )
    selected_c = phase651.summary["selected_c"]
    market_c = float(selected_c["M1_market"])
    offset_c = float(selected_c["M4_market_offset_history"])
    phase652 = run_signal_robustness(
        safe_dataset,
        market_c=market_c,
        offset_c=offset_c,
        bootstrap_repetitions=phase652_bootstrap_repetitions,
        subgroup_bootstrap_repetitions=phase652_subgroup_bootstrap_repetitions,
    )
    phase653 = run_small_field_failure_audit(
        safe_dataset,
        market_c=market_c,
        offset_c=offset_c,
        bootstrap_repetitions=phase653_bootstrap_repetitions,
        minimum_bootstrap_races=phase653_minimum_bootstrap_races,
    )
    summary = build_no_popularity_rebase_summary(
        phase651.summary,
        phase652.summary,
        phase653.summary,
    )
    return NoPopularityRebaseResult(
        summary=summary,
        phase651=phase651,
        phase652=phase652,
        phase653=phase653,
    )


def build_no_popularity_rebase_summary(
    phase651_summary: dict[str, Any],
    phase652_summary: dict[str, Any],
    phase653_summary: dict[str, Any],
) -> dict[str, Any]:
    phase651_supported = phase651_summary["final_verdict"] == SIGNAL_SUPPORTED
    phase652_not_failed = phase652_summary["final_verdict"] != ROBUSTNESS_NOT_CONFIRMED
    supported_small_field_harm = phase653_summary["supported_small_field_harm"]
    stable_slot2_pairs = phase653_summary["stable_slot2_specific_recovery_pairs"]
    small_field_harm_reproduced = bool(supported_small_field_harm)
    slot2_recovery_signal_reproduced = bool(stable_slot2_pairs)
    conclusions_reproduced = (
        phase651_supported
        and phase652_not_failed
        and small_field_harm_reproduced
        and slot2_recovery_signal_reproduced
    )
    return {
        "analysis_version": "phase657_no_popularity_rebase_v1",
        "final_verdict": REBASE_REPRODUCED if conclusions_reproduced else REBASE_CHANGED,
        "market_feature_contract": ["win_odds", "place_basis_odds"],
        "phase651_final_verdict": phase651_summary["final_verdict"],
        "phase652_final_verdict": phase652_summary["final_verdict"],
        "phase652_remains_diagnostic_only": (
            phase652_summary["final_verdict"] == ROBUSTNESS_DIAGNOSTIC_ONLY
        ),
        "phase653_final_verdict": phase653_summary["final_verdict"],
        "phase653_supported_small_field_harm_count": len(supported_small_field_harm),
        "phase653_small_field_harm_reproduced": small_field_harm_reproduced,
        "phase653_stable_slot2_recovery_pairs": stable_slot2_pairs,
        "phase653_slot2_recovery_signal_reproduced": slot2_recovery_signal_reproduced,
        "recommendation": (
            "AMEND_PREREGISTRATION_TO_NO_POPULARITY_BEFORE_FORWARD_MODEL_EVALUATION"
            if conclusions_reproduced
            else "DO_NOT_AMEND_PREREGISTRATION_UNTIL_CHANGED_CONCLUSION_IS_REVIEWED"
        ),
        "2026_data_used": False,
        "2025_claimed_fresh": False,
        "roi_or_betting_used": False,
    }


def write_no_popularity_rebase(
    result: NoPopularityRebaseResult,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_comparison_result(result.phase651, output_dir / "phase651")
    write_robustness_result(result.phase652, output_dir / "phase652")
    write_small_field_audit(result.phase653, output_dir / "phase653")
    (output_dir / "phase657_summary.json").write_text(
        json.dumps(result.summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "phase657_findings.md").write_text(
        _findings_markdown(result),
        encoding="utf-8",
    )


def _findings_markdown(result: NoPopularityRebaseResult) -> str:
    return "\n".join(
        [
            "# Phase657 No-Popularity Rebase",
            "",
            "## Verdict",
            "",
            f"`{result.summary['final_verdict']}`",
            "",
            "## Reproduced phase verdicts",
            "",
            f"- Phase651: `{result.summary['phase651_final_verdict']}`",
            f"- Phase652: `{result.summary['phase652_final_verdict']}`",
            f"- Phase653: `{result.summary['phase653_final_verdict']}`",
            "",
            "The rerun uses only log win odds and log place-basis odds for the market model. "
            "Popularity-dependent subgroup descriptions are omitted.",
            "",
            "## Next gate",
            "",
            f"`{result.summary['recommendation']}`",
            "",
            "No 2026 data, ROI, payout, threshold, stake, or betting rule was used.",
            "",
        ]
    )
