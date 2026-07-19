from __future__ import annotations

from horse_bet_lab.research.historical_ability_models import SIGNAL_SUPPORTED
from horse_bet_lab.research.historical_signal_robustness import ROBUSTNESS_DIAGNOSTIC_ONLY
from horse_bet_lab.research.no_popularity_rebase import (
    REBASE_CHANGED,
    REBASE_REPRODUCED,
    build_no_popularity_rebase_summary,
)


def test_rebase_summary_allows_diagnostic_phase652_when_signal_is_reproduced() -> None:
    summary = build_no_popularity_rebase_summary(
        {"final_verdict": SIGNAL_SUPPORTED},
        {"final_verdict": ROBUSTNESS_DIAGNOSTIC_ONLY},
        {
            "final_verdict": "SMALL_FIELD_FAILURE_CAUSE_AUDITED_DIAGNOSTIC_ONLY",
            "supported_small_field_harm": [{"group": "5-7"}],
            "stable_slot2_specific_recovery_pairs": [
                {"feature_scope": "full", "model_kind": "offset"}
            ],
        },
    )

    assert summary["final_verdict"] == REBASE_REPRODUCED
    assert summary["recommendation"].startswith("AMEND_PREREGISTRATION")
    assert summary["2026_data_used"] is False
    assert summary["roi_or_betting_used"] is False


def test_rebase_summary_stops_when_phase651_signal_changes() -> None:
    summary = build_no_popularity_rebase_summary(
        {"final_verdict": "HISTORICAL_ABILITY_SIGNAL_NOT_SUPPORTED"},
        {"final_verdict": ROBUSTNESS_DIAGNOSTIC_ONLY},
        {
            "final_verdict": "SMALL_FIELD_FAILURE_CAUSE_AUDITED_DIAGNOSTIC_ONLY",
            "supported_small_field_harm": [],
            "stable_slot2_specific_recovery_pairs": [],
        },
    )

    assert summary["final_verdict"] == REBASE_CHANGED
    assert summary["recommendation"].startswith("DO_NOT_AMEND")
