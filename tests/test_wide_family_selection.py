from __future__ import annotations

import json
from pathlib import Path

from horse_bet_lab.config import load_wide_family_selection_config
from horse_bet_lab.evaluation.wide_family_selection import run_wide_family_selection


def test_wide_family_selection_prefers_valid_winner(tmp_path: Path) -> None:
    summary_header = (
        "window_label,split,score_method,pair_generation_method,partner_weight,candidate_top_k,"
        "adopted_pair_count,pair_count,hit_count,hit_rate,roi,total_profit\n"
    )
    v3_summary_path = tmp_path / "v3_summary.csv"
    v3_summary_path.write_text(
        summary_header
        + "w1,valid,pair_model_score,anchor_top1_partner_pred_times_place,,4,1,10,3,0.3,0.90,-100.0\n"
        + "w1,test,pair_model_score,anchor_top1_partner_pred_times_place,,4,1,10,3,0.3,0.92,-80.0\n"
        + "w2,valid,pair_model_score,anchor_top1_partner_pred_times_place,,4,1,10,4,0.4,1.10,100.0\n"
        + "w2,test,pair_model_score,anchor_top1_partner_pred_times_place,,4,1,10,4,0.4,1.05,50.0\n",
        encoding="utf-8",
    )
    v6_summary_path = tmp_path / "v6_summary.csv"
    v6_summary_path.write_text(
        summary_header
        + "w1,valid,anchor_plus_partner_edge_like,v6_win_edge_top1_partner_pred_times_place,,4,1,10,5,0.5,1.20,200.0\n"
        + "w1,test,anchor_plus_partner_edge_like,v6_win_edge_top1_partner_pred_times_place,,4,1,10,5,0.5,1.10,100.0\n"
        + "w2,valid,anchor_plus_partner_edge_like,v6_win_edge_top1_partner_pred_times_place,,4,1,10,2,0.2,0.80,-200.0\n"
        + "w2,test,anchor_plus_partner_edge_like,v6_win_edge_top1_partner_pred_times_place,,4,1,10,2,0.2,0.85,-150.0\n",
        encoding="utf-8",
    )
    selected_pairs_header = (
        "window_label,split,race_key,score_method,pair_generation_method,partner_weight,candidate_top_k,"
        "adopted_pair_count,rank_in_race,horse_number_1,horse_number_2,horse_probability_1,horse_probability_2,"
        "place_basis_odds_1,place_basis_odds_2,geometric_mean_place_basis_odds,ow_wide_odds,abs_prob_gap,pair_score,"
        "wide_payout,is_hit\n"
    )
    v3_selected_pairs_path = tmp_path / "v3_selected_pairs.csv"
    v3_selected_pairs_path.write_text(
        selected_pairs_header
        + "w1,test,race_a,pair_model_score,anchor_top1_partner_pred_times_place,,4,1,1,1,2,0.6,0.5,1.2,1.4,1.296,5.0,0.1,0.5,,False\n"
        + "w2,test,race_b,pair_model_score,anchor_top1_partner_pred_times_place,,4,1,1,1,3,0.6,0.5,1.2,1.4,1.296,5.0,0.1,0.5,150.0,True\n",
        encoding="utf-8",
    )
    v6_selected_pairs_path = tmp_path / "v6_selected_pairs.csv"
    v6_selected_pairs_path.write_text(
        selected_pairs_header
        + "w1,test,race_c,anchor_plus_partner_edge_like,v6_win_edge_top1_partner_pred_times_place,,4,1,1,1,4,0.6,0.5,1.2,1.4,1.296,5.0,0.1,0.7,200.0,True\n"
        + "w2,test,race_d,anchor_plus_partner_edge_like,v6_win_edge_top1_partner_pred_times_place,,4,1,1,2,4,0.6,0.5,1.2,1.4,1.296,5.0,0.1,0.7,,False\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "wide_family_selection.toml"
    config_path.write_text(
        (
            "[wide_family_selection]\n"
            "name = 'wide_family_selection_smoke'\n"
            f"v3_summary_path = '{v3_summary_path}'\n"
            f"v3_selected_pairs_path = '{v3_selected_pairs_path}'\n"
            f"v6_summary_path = '{v6_summary_path}'\n"
            f"v6_selected_pairs_path = '{v6_selected_pairs_path}'\n"
            "v3_label = 'v3'\n"
            "v6_label = 'v6'\n"
            f"output_dir = '{tmp_path / 'output'}'\n"
            "split = 'test'\n"
            "valid_split = 'valid'\n"
            "stake_per_pair = 100\n"
            "selection_rules = [\n"
            "  'total_valid_roi_max',\n"
            "  'window_win_count_max',\n"
            "  'mean_valid_roi_minus_std'\n"
            "]\n"
            "v3_score_method = 'pair_model_score'\n"
            "v3_pair_generation_method = 'anchor_top1_partner_pred_times_place'\n"
            "v6_score_method = 'anchor_plus_partner_edge_like'\n"
            "v6_pair_generation_method = 'v6_win_edge_top1_partner_pred_times_place'\n"
        ),
        encoding="utf-8",
    )

    result = run_wide_family_selection(load_wide_family_selection_config(config_path))

    assert (result.output_dir / "selected_family_summary.csv").exists()
    assert (result.output_dir / "selected_family_windows.csv").exists()

    by_rule = {row.selection_rule: row for row in result.selected_family_summaries}
    assert set(by_rule) == {
        "total_valid_roi_max",
        "window_win_count_max",
        "mean_valid_roi_minus_std",
    }
    assert all(row.pair_count == 20 for row in result.selected_family_summaries)

    window_rows = result.selected_family_windows
    assert any(
        row.selection_rule == "total_valid_roi_max"
        and row.test_window_label == "w1"
        and row.selected_family == "v6"
        for row in window_rows
    )
    assert any(
        row.selection_rule == "total_valid_roi_max"
        and row.test_window_label == "w2"
        and row.selected_family == "v3"
        for row in window_rows
    )

    payload = json.loads((result.output_dir / "selected_family_summary.json").read_text(encoding="utf-8"))
    assert payload["analysis"]["v3_label"] == "v3"
    assert payload["analysis"]["v6_label"] == "v6"
    assert len(payload["analysis"]["rows"]) == 3
