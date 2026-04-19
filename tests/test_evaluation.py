from __future__ import annotations

import json
from pathlib import Path

from horse_bet_lab.config import load_bet_candidate_eval_config
from horse_bet_lab.evaluation.service import evaluate_bet_candidates


def test_evaluate_bet_candidates_writes_summary_artifacts(tmp_path: Path) -> None:
    predictions_path = tmp_path / "predictions.csv"
    predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,target_value,pred_probability",
                "r1,1,train,1,0.70",
                "r2,1,train,0,0.30",
                "r3,1,valid,1,0.80",
                "r4,1,valid,0,0.60",
                "r5,1,test,0,0.40",
                "r6,1,test,1,0.90",
            ],
        )
        + "\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "bet_eval.toml"
    config_path.write_text(
        (
            "[evaluation]\n"
            "name = 'bet_candidate_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"output_dir = '{tmp_path / 'artifacts'}'\n"
            "thresholds = [0.50, 0.65]\n"
        ),
        encoding="utf-8",
    )

    result = evaluate_bet_candidates(load_bet_candidate_eval_config(config_path))

    summary_csv = result.output_dir / "summary.csv"
    summary_json = result.output_dir / "summary.json"

    assert summary_csv.exists()
    assert summary_json.exists()

    rows = summary_csv.read_text(encoding="utf-8").strip().splitlines()
    assert rows[0] == (
        "split,threshold,candidate_count,adopted_count,adoption_rate,hit_rate,avg_prediction"
    )
    assert len(rows) == 7

    payload = json.loads(summary_json.read_text(encoding="utf-8"))
    assert payload["evaluation"]["thresholds"] == [0.5, 0.65]
    assert [summary["split"] for summary in payload["summaries"][:3]] == ["train", "valid", "test"]
    assert [summary["split"] for summary in payload["summaries"][3:]] == ["train", "valid", "test"]
    assert payload["summaries"][0]["threshold"] == 0.5
    assert payload["summaries"][3]["threshold"] == 0.65
    assert payload["summaries"][0]["adopted_count"] == 1
    assert payload["summaries"][1]["hit_rate"] == 0.5
    assert payload["summaries"][4]["hit_rate"] == 1.0
