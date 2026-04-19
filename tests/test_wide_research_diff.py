from __future__ import annotations

import json
from pathlib import Path

from horse_bet_lab.config import load_wide_research_diff_config
from horse_bet_lab.evaluation.wide_research_diff import analyze_wide_research_diff
from tests.test_wide_research_backtest import make_oz_line


def test_wide_research_diff_reports_common_and_swaps(tmp_path: Path) -> None:
    v2_path = tmp_path / "v2_selected_pairs.csv"
    v3_path = tmp_path / "v3_selected_pairs.csv"
    raw_dir = tmp_path / "data" / "raw" / "jrdb" / "OZ_2025"
    raw_dir.mkdir(parents=True)
    (raw_dir / "OZ250105.txt").write_bytes(
        make_oz_line(
            race_key="race_a",
            win_basis_odds=(2.0, 3.0, 5.0, 6.0),
            place_basis_odds=(1.2, 1.4, 2.0, 2.5),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="race_b",
            win_basis_odds=(2.1, 3.1, 4.1, 6.1),
            place_basis_odds=(1.3, 1.5, 2.1, 2.6),
        )
        + b"\r\n",
    )

    header = (
        "window_label,split,race_key,score_method,pair_generation_method,candidate_top_k,"
        "adopted_pair_count,rank_in_race,horse_number_1,horse_number_2,horse_probability_1,"
        "horse_probability_2,place_basis_odds_1,place_basis_odds_2,geometric_mean_place_basis_odds,"
        "ow_wide_odds,abs_prob_gap,pair_score,wide_payout,is_hit\n"
    )
    v2_path.write_text(
        header
        + "w1,test,race_a,pair_model_score,symmetric_top_k_pairs,4,1,1,1,2,0.7,0.6,1.2,1.4,1.2961,5.0,0.1,0.54,250.0,True\n"
        + "w2,test,race_b,pair_model_score,symmetric_top_k_pairs,4,1,1,1,3,0.7,0.5,1.3,2.1,1.6523,6.0,0.2,0.58,,False\n",
        encoding="utf-8",
    )
    v3_path.write_text(
        header
        + "w1,test,race_a,pair_model_score,anchor_top1_partner_pred_times_place,4,1,1,1,2,0.7,0.6,1.2,1.4,1.2961,5.0,0.1,0.54,250.0,True\n"
        + "w2,test,race_b,pair_model_score,anchor_top1_partner_pred_times_place,4,1,1,2,4,0.65,0.52,1.5,2.6,1.9748,7.0,0.13,0.67,320.0,True\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "wide_research_diff.toml"
    config_path.write_text(
        (
            "[wide_research_diff]\n"
            "name = 'wide_research_diff_smoke'\n"
            f"v2_selected_pairs_path = '{v2_path}'\n"
            f"v3_selected_pairs_path = '{v3_path}'\n"
            "v2_label = 'v3'\n"
            "v3_label = 'v5'\n"
            f"raw_dir = '{tmp_path / 'data' / 'raw' / 'jrdb'}'\n"
            f"output_dir = '{tmp_path / 'output'}'\n"
            "split = 'test'\n"
            "stake_per_pair = 100\n"
            "representative_example_count = 2\n"
            "v2_score_method = 'pair_model_score'\n"
            "v2_pair_generation_method = 'symmetric_top_k_pairs'\n"
            "v3_score_method = 'pair_model_score'\n"
            "v3_pair_generation_method = 'anchor_top1_partner_pred_times_place'\n"
        ),
        encoding="utf-8",
    )

    result = analyze_wide_research_diff(load_wide_research_diff_config(config_path))

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "distribution.csv").exists()
    assert (result.output_dir / "window_summary.csv").exists()
    assert (result.output_dir / "payout_bucket_summary.csv").exists()
    assert (result.output_dir / "representative_examples.csv").exists()

    summary_by_group = {row.set_group: row for row in result.summaries}
    assert summary_by_group["common"].pair_count == 1
    assert summary_by_group["common"].hit_count == 1
    assert round(summary_by_group["common"].roi, 4) == 2.5
    assert summary_by_group["v3_only"].pair_count == 1
    assert summary_by_group["v3_only"].hit_count == 0
    assert round(summary_by_group["v3_only"].roi, 4) == 0.0
    assert summary_by_group["v5_only"].pair_count == 1
    assert summary_by_group["v5_only"].hit_count == 1
    assert round(summary_by_group["v5_only"].roi, 4) == 3.2

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["analysis"]["v2_label"] == "v3"
    assert payload["analysis"]["v3_label"] == "v5"
    assert payload["analysis"]["popularity_bucket_status"] == "unavailable_in_current_artifacts"
    assert any(row.bucket_type == "place_basis_odds" for row in result.distributions)
    assert any(row.bucket_type == "win_odds" for row in result.distributions)
    assert any(row.bucket_type == "pair_edge_equivalent" for row in result.distributions)
    assert any(row.window_label == "w1" for row in result.window_rows)
    assert any(row.window_label == "w2" for row in result.window_rows)
    assert any(row.payout_bucket == "miss" for row in result.payout_bucket_rows)
    assert any(row.payout_bucket == "200_to_400" for row in result.payout_bucket_rows)
    assert any(row.set_group == "v3_only" for row in result.representative_examples)
    assert any(row.set_group == "v5_only" and row.is_hit for row in result.representative_examples)
