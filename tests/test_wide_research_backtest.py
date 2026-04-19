from __future__ import annotations

import json
from pathlib import Path

from horse_bet_lab.config import load_wide_research_backtest_config
from horse_bet_lab.evaluation.wide_research_backtest import run_wide_research_backtest


def test_run_wide_research_backtest_writes_rule_based_best_settings(tmp_path: Path) -> None:
    predictions_path = tmp_path / "rolling_predictions.csv"
    predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.90,w0",
                "v1,2,valid,0.80,w0",
                "v1,3,valid,0.70,w0",
                "v1,4,valid,0.60,w0",
                "t1,1,test,0.90,w1",
                "t1,2,test,0.80,w1",
                "t1,3,test,0.70,w1",
                "t1,4,test,0.60,w1",
                "t2,1,test,0.90,w2",
                "t2,2,test,0.80,w2",
                "t2,3,test,0.70,w2",
                "t2,4,test,0.60,w2",
                "t3,1,test,0.90,w3",
                "t3,2,test,0.80,w3",
                "t3,3,test,0.70,w3",
                "t3,4,test,0.60,w3",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    hjc_dir = tmp_path / "data" / "raw" / "jrdb" / "HJC_2025"
    hjc_dir.mkdir(parents=True)
    oz_dir = tmp_path / "data" / "raw" / "jrdb" / "OZ_2025"
    oz_dir.mkdir(parents=True)
    (hjc_dir / "HJC250105.txt").write_bytes(
        make_hjc_line(
            race_key="v1",
            wide_pairs=((1, 3, 220), (2, 3, 0), (1, 2, 0)),
        )
        + b"\r\n"
        + make_hjc_line(
            race_key="t1",
            wide_pairs=((1, 3, 200), (2, 3, 0), (1, 2, 0)),
        )
        + b"\r\n"
        + make_hjc_line(
            race_key="t2",
            wide_pairs=((1, 3, 180), (2, 3, 60), (1, 2, 0)),
        )
        + b"\r\n"
        + make_hjc_line(
            race_key="t3",
            wide_pairs=((1, 3, 500), (2, 3, 80), (1, 2, 0)),
        )
        + b"\r\n",
    )
    ow_dir = tmp_path / "data" / "raw" / "jrdb" / "OW_2025"
    ow_dir.mkdir(parents=True)
    (oz_dir / "OZ250105.txt").write_bytes(
            make_oz_line(
                race_key="v1",
                win_basis_odds=(2.0, 3.0, 4.0, 5.0),
                place_basis_odds=(1.2, 1.4, 1.6, 1.2),
            )
        + b"\r\n"
        + make_oz_line(
            race_key="t1",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.2, 1.4, 1.6, 1.2),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t2",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.2, 1.4, 1.6, 1.2),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t3",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.2, 1.4, 1.6, 1.2),
        )
        + b"\r\n",
    )
    (ow_dir / "OW250105.txt").write_bytes(
        make_ow_line("v1", 4, [2.0, 4.0, 50.0, 6.0, 60.0, 70.0])
        + b"\r\n"
        + make_ow_line("t1", 4, [2.0, 4.0, 50.0, 6.0, 60.0, 70.0])
        + b"\r\n"
        + make_ow_line("t2", 4, [2.0, 4.0, 50.0, 6.0, 60.0, 70.0])
        + b"\r\n"
        + make_ow_line("t3", 4, [2.0, 4.0, 50.0, 6.0, 60.0, 70.0])
        + b"\r\n",
    )

    config_path = tmp_path / "wide_research.toml"
    config_path.write_text(
        (
            "[wide_research]\n"
            "name = 'wide_research_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"hjc_raw_dir = '{tmp_path / 'data' / 'raw' / 'jrdb'}'\n"
            f"output_dir = '{tmp_path / 'artifacts'}'\n"
            "score_methods = [\n"
            "  'no_ow_guard',\n"
            "  'low_wide_payout_guard',\n"
            "  'extreme_ow_implied_prob_guard'\n"
            "]\n"
            "candidate_top_k_values = [4]\n"
            "adopted_pair_count_values = [1]\n"
            "stake_per_pair = 100\n"
            "bootstrap_iterations = 200\n"
            "random_seed = 11\n"
            "split_column = 'split'\n"
            "probability_column = 'pred_probability'\n"
            "window_label_column = 'window_label'\n"
        ),
        encoding="utf-8",
    )

    result = run_wide_research_backtest(load_wide_research_backtest_config(config_path))

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "summary.json").exists()
    assert (result.output_dir / "comparison.csv").exists()
    assert (result.output_dir / "comparison.json").exists()
    assert (result.output_dir / "best_settings.csv").exists()
    assert (result.output_dir / "best_settings.json").exists()
    assert (result.output_dir / "selected_pairs.csv").exists()
    assert (result.output_dir / "selected_pairs.json").exists()

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["backtest"]["score_methods"] == [
        "no_ow_guard",
        "low_wide_payout_guard",
        "extreme_ow_implied_prob_guard",
    ]
    assert payload["backtest"]["candidate_top_k_values"] == [4]
    assert payload["backtest"]["adopted_pair_count_values"] == [1]
    assert payload["backtest"]["bootstrap_iterations"] == 200
    assert payload["backtest"]["random_seed"] == 11
    assert len(payload["comparisons"]) == 3
    assert len(payload["best_settings"]) == 3
    assert len(result.summaries) == 12
    assert len(result.comparisons) == 3
    assert len(result.best_settings) == 3
    assert len(result.selected_pairs) == 12

    no_guard_top4_w1 = next(
        summary
        for summary in result.summaries
        if summary.score_method == "no_ow_guard"
        and summary.window_label == "w1"
        and summary.split == "test"
        and summary.candidate_top_k == 4
        and summary.adopted_pair_count == 1
    )
    low_guard_top4_w1 = next(
        summary
        for summary in result.summaries
        if summary.score_method == "low_wide_payout_guard"
        and summary.window_label == "w1"
        and summary.split == "test"
        and summary.candidate_top_k == 4
        and summary.adopted_pair_count == 1
    )
    assert no_guard_top4_w1.pair_count == 1
    assert no_guard_top4_w1.hit_count == 0
    assert round(no_guard_top4_w1.roi, 4) == 0.0
    assert round(no_guard_top4_w1.total_profit, 1) == -100.0
    assert low_guard_top4_w1.pair_count == 1
    assert low_guard_top4_w1.hit_count == 1
    assert round(low_guard_top4_w1.roi, 4) == 2.0
    assert round(low_guard_top4_w1.total_profit, 1) == 100.0

    no_guard_top4_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "no_ow_guard"
        and row.window_label == "w1"
        and row.split == "test"
        and row.candidate_top_k == 4
        and row.adopted_pair_count == 1
        and row.rank_in_race == 1
    )
    low_guard_top4_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "low_wide_payout_guard"
        and row.window_label == "w1"
        and row.split == "test"
        and row.candidate_top_k == 4
        and row.adopted_pair_count == 1
        and row.rank_in_race == 1
    )
    extreme_guard_top4_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "extreme_ow_implied_prob_guard"
        and row.window_label == "w1"
        and row.split == "test"
        and row.candidate_top_k == 4
        and row.adopted_pair_count == 1
        and row.rank_in_race == 1
    )
    assert (no_guard_top4_pair.horse_number_1, no_guard_top4_pair.horse_number_2) == (1, 2)
    assert (low_guard_top4_pair.horse_number_1, low_guard_top4_pair.horse_number_2) == (1, 3)
    assert (extreme_guard_top4_pair.horse_number_1, extreme_guard_top4_pair.horse_number_2) == (2, 3)
    assert round(no_guard_top4_pair.geometric_mean_place_basis_odds, 4) == 1.2961
    assert round(low_guard_top4_pair.ow_wide_odds or 0.0, 4) == 4.0
    assert round(extreme_guard_top4_pair.ow_wide_odds or 0.0, 4) == 6.0

    no_guard_comparison = next(
        row for row in result.comparisons if row.score_method == "no_ow_guard"
    )
    low_guard_comparison = next(
        row for row in result.comparisons if row.score_method == "low_wide_payout_guard"
    )
    extreme_guard_comparison = next(
        row for row in result.comparisons if row.score_method == "extreme_ow_implied_prob_guard"
    )
    assert no_guard_comparison.candidate_top_k == 4
    assert no_guard_comparison.adopted_pair_count == 1
    assert no_guard_comparison.pair_count == 3
    assert no_guard_comparison.hit_count == 0
    assert round(no_guard_comparison.hit_rate, 4) == 0.0
    assert round(no_guard_comparison.roi, 4) == 0.0
    assert round(no_guard_comparison.total_profit, 1) == -300.0
    assert no_guard_comparison.window_win_count == 0
    assert round(no_guard_comparison.mean_roi, 4) == 0.0
    assert round(no_guard_comparison.roi_std, 4) == 0.0
    assert no_guard_comparison.roi_ci_lower <= no_guard_comparison.roi <= no_guard_comparison.roi_ci_upper
    assert no_guard_comparison.profit_ci_lower <= no_guard_comparison.total_profit <= no_guard_comparison.profit_ci_upper
    assert 0.0 <= no_guard_comparison.roi_gt_1_ratio <= 1.0

    assert low_guard_comparison.pair_count == 3
    assert low_guard_comparison.hit_count == 3
    assert round(low_guard_comparison.hit_rate, 4) == 1.0
    assert round(low_guard_comparison.roi, 4) == 2.9333
    assert round(low_guard_comparison.total_profit, 1) == 580.0
    assert low_guard_comparison.window_win_count == 3
    assert round(low_guard_comparison.mean_roi, 4) == 2.9333
    assert round(low_guard_comparison.roi_std, 4) == 1.4636
    assert low_guard_comparison.roi_std > no_guard_comparison.roi_std
    assert low_guard_comparison.roi_gt_1_ratio > no_guard_comparison.roi_gt_1_ratio

    assert extreme_guard_comparison.pair_count == 3
    assert extreme_guard_comparison.hit_count == 2
    assert round(extreme_guard_comparison.hit_rate, 4) == 0.6667
    assert round(extreme_guard_comparison.roi, 4) == 0.4667
    assert round(extreme_guard_comparison.total_profit, 1) == -160.0
    assert extreme_guard_comparison.window_win_count == 0
    assert round(extreme_guard_comparison.mean_roi, 4) == 0.4667
    assert round(extreme_guard_comparison.roi_std, 4) == 0.3399
    assert extreme_guard_comparison.roi < low_guard_comparison.roi

    by_rule = {row.selection_rule: row for row in result.best_settings}
    assert set(by_rule) == {
        "total_oos_roi_max",
        "window_win_count_max",
        "mean_roi_minus_std",
    }

    total_roi_best = by_rule["total_oos_roi_max"]
    assert total_roi_best.score_method == "low_wide_payout_guard"
    assert total_roi_best.candidate_top_k == 4
    assert total_roi_best.adopted_pair_count == 1
    assert total_roi_best.pair_count == 3
    assert total_roi_best.hit_count == 3
    assert round(total_roi_best.hit_rate, 4) == 1.0
    assert round(total_roi_best.roi, 4) == 2.9333
    assert round(total_roi_best.total_profit, 1) == 580.0
    assert total_roi_best.window_win_count == 3
    assert round(total_roi_best.mean_roi, 4) == 2.9333
    assert round(total_roi_best.roi_std, 4) == 1.4636

    window_win_best = by_rule["window_win_count_max"]
    assert window_win_best.score_method == "low_wide_payout_guard"
    assert window_win_best.candidate_top_k == 4
    assert window_win_best.adopted_pair_count == 1
    assert window_win_best.pair_count == 3
    assert window_win_best.hit_count == 3
    assert round(window_win_best.hit_rate, 4) == 1.0
    assert round(window_win_best.roi, 4) == 2.9333
    assert round(window_win_best.total_profit, 1) == 580.0
    assert window_win_best.window_win_count == 3
    assert round(window_win_best.mean_roi, 4) == 2.9333
    assert round(window_win_best.roi_std, 4) == 1.4636

    stability_best = by_rule["mean_roi_minus_std"]
    assert stability_best.score_method == "low_wide_payout_guard"
    assert stability_best.candidate_top_k == 4
    assert stability_best.adopted_pair_count == 1
    assert round(stability_best.mean_roi - stability_best.roi_std, 4) == 1.4697


def test_run_wide_research_backtest_supports_v2_market_baseline_layout(tmp_path: Path) -> None:
    predictions_path = tmp_path / "rolling_predictions.csv"
    predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.90,w0",
                "v1,2,valid,0.80,w0",
                "v1,3,valid,0.70,w0",
                "v1,4,valid,0.60,w0",
                "t1,1,test,0.90,w1",
                "t1,2,test,0.80,w1",
                "t1,3,test,0.70,w1",
                "t1,4,test,0.60,w1",
                "t2,1,test,0.90,w2",
                "t2,2,test,0.80,w2",
                "t2,3,test,0.70,w2",
                "t2,4,test,0.60,w2",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    hjc_dir = tmp_path / "data" / "raw" / "jrdb" / "HJC_2025"
    hjc_dir.mkdir(parents=True)
    oz_dir = tmp_path / "data" / "raw" / "jrdb" / "OZ_2025"
    oz_dir.mkdir(parents=True)
    ow_dir = tmp_path / "data" / "raw" / "jrdb" / "OW_2025"
    ow_dir.mkdir(parents=True)
    (hjc_dir / "HJC250105.txt").write_bytes(
        make_hjc_line(race_key="v1", wide_pairs=((1, 3, 220), (1, 2, 0), (2, 3, 0)))
        + b"\r\n"
        + make_hjc_line(race_key="t1", wide_pairs=((1, 3, 200), (1, 2, 0), (2, 3, 0)))
        + b"\r\n"
        + make_hjc_line(race_key="t2", wide_pairs=((1, 3, 180), (1, 2, 0), (2, 3, 0)))
        + b"\r\n",
    )
    (oz_dir / "OZ250105.txt").write_bytes(
        make_oz_line(
            race_key="v1",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.2, 1.4, 1.6, 1.2),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t1",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.2, 1.4, 1.6, 1.2),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t2",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.2, 1.4, 1.6, 1.2),
        )
        + b"\r\n",
    )
    (ow_dir / "OW250105.txt").write_bytes(
        make_ow_line("v1", 4, [8.0, 4.0, 6.0, 50.0, 60.0, 70.0])
        + b"\r\n"
        + make_ow_line("t1", 4, [8.0, 4.0, 6.0, 50.0, 60.0, 70.0])
        + b"\r\n"
        + make_ow_line("t2", 4, [8.0, 4.0, 6.0, 50.0, 60.0, 70.0])
        + b"\r\n",
    )

    config_path = tmp_path / "wide_research_v2.toml"
    config_path.write_text(
        (
            "[wide_research_v2]\n"
            "name = 'wide_research_v2_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"hjc_raw_dir = '{tmp_path / 'data' / 'raw' / 'jrdb'}'\n"
            f"output_dir = '{tmp_path / 'artifacts' / 'wide_research_v2_smoke'}'\n"
            "score_methods = [\n"
            "  'pair_model_score',\n"
            "  'ow_wide_market_prob',\n"
            "  'pair_edge'\n"
            "]\n"
            "candidate_top_k_values = [4]\n"
            "adopted_pair_count_values = [1]\n"
            "stake_per_pair = 100\n"
            "bootstrap_iterations = 50\n"
            "random_seed = 11\n"
            "split_column = 'split'\n"
            "probability_column = 'pred_probability'\n"
            "window_label_column = 'window_label'\n"
        ),
        encoding="utf-8",
    )

    result = run_wide_research_backtest(load_wide_research_backtest_config(config_path))

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["backtest"]["config_section"] == "wide_research_v2"
    assert payload["backtest"]["score_methods"] == [
        "pair_model_score",
        "ow_wide_market_prob",
        "pair_edge",
    ]
    assert payload["backtest"]["pair_generation_methods"] == ["symmetric_top_k_pairs"]

    comparison_by_method = {row.score_method: row for row in result.comparisons}
    assert set(comparison_by_method) == {
        "pair_model_score",
        "ow_wide_market_prob",
        "pair_edge",
    }
    assert comparison_by_method["pair_model_score"].score_role == "model_primary"
    assert comparison_by_method["ow_wide_market_prob"].score_role == "market_baseline"
    assert comparison_by_method["pair_edge"].score_role == "model_minus_market_edge"
    assert comparison_by_method["pair_model_score"].candidate_top_k == 4
    assert comparison_by_method["pair_model_score"].adopted_pair_count == 1

    by_rule = {row.selection_rule: row for row in result.best_settings}
    assert set(by_rule) == {
        "total_oos_roi_max",
        "window_win_count_max",
        "mean_roi_minus_std",
    }
    assert all(row.score_role in {"model_primary", "market_baseline", "model_minus_market_edge"} for row in by_rule.values())

    csv_lines = (result.output_dir / "best_settings.csv").read_text(encoding="utf-8").splitlines()
    assert "score_role" in csv_lines[0]


def test_run_wide_research_backtest_supports_v3_asymmetric_pair_generation(tmp_path: Path) -> None:
    predictions_path = tmp_path / "rolling_predictions.csv"
    predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.90,w0",
                "v1,2,valid,0.72,w0",
                "v1,3,valid,0.60,w0",
                "v1,4,valid,0.58,w0",
                "t1,1,test,0.90,w1",
                "t1,2,test,0.72,w1",
                "t1,3,test,0.60,w1",
                "t1,4,test,0.58,w1",
                "t2,1,test,0.90,w2",
                "t2,2,test,0.72,w2",
                "t2,3,test,0.60,w2",
                "t2,4,test,0.58,w2",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    hjc_dir = tmp_path / "data" / "raw" / "jrdb" / "HJC_2025"
    hjc_dir.mkdir(parents=True)
    oz_dir = tmp_path / "data" / "raw" / "jrdb" / "OZ_2025"
    oz_dir.mkdir(parents=True)
    ow_dir = tmp_path / "data" / "raw" / "jrdb" / "OW_2025"
    ow_dir.mkdir(parents=True)
    (hjc_dir / "HJC250105.txt").write_bytes(
        make_hjc_line(race_key="v1", wide_pairs=((1, 3, 180), (3, 4, 0), (1, 4, 0)))
        + b"\r\n"
        + make_hjc_line(race_key="t1", wide_pairs=((1, 3, 180), (3, 4, 0), (1, 4, 0)))
        + b"\r\n"
        + make_hjc_line(race_key="t2", wide_pairs=((1, 3, 0), (3, 4, 260), (1, 4, 0)))
        + b"\r\n",
    )
    (oz_dir / "OZ250105.txt").write_bytes(
        make_oz_line(
            race_key="v1",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.0, 1.1, 3.0, 2.0),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t1",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.0, 1.1, 3.0, 2.0),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t2",
            win_basis_odds=(2.0, 3.0, 4.0, 5.0),
            place_basis_odds=(1.0, 1.1, 3.0, 2.0),
        )
        + b"\r\n",
    )
    (ow_dir / "OW250105.txt").write_bytes(
        make_ow_line("v1", 4, [8.0, 6.0, 7.0, 5.0, 9.0, 11.0])
        + b"\r\n"
        + make_ow_line("t1", 4, [8.0, 6.0, 7.0, 5.0, 9.0, 11.0])
        + b"\r\n"
        + make_ow_line("t2", 4, [8.0, 6.0, 7.0, 5.0, 9.0, 11.0])
        + b"\r\n",
    )

    config_path = tmp_path / "wide_research_v3.toml"
    config_path.write_text(
        (
            "[wide_research_v3]\n"
            "name = 'wide_research_v3_smoke'\n"
            f"predictions_path = '{predictions_path}'\n"
            f"hjc_raw_dir = '{tmp_path / 'data' / 'raw' / 'jrdb'}'\n"
            f"output_dir = '{tmp_path / 'artifacts' / 'wide_research_v3_smoke'}'\n"
            "score_methods = [\n"
            "  'pair_model_score'\n"
            "]\n"
            "pair_generation_methods = [\n"
            "  'anchor_top1_partner_place_edge',\n"
            "  'anchor_top1_partner_pred_times_place',\n"
            "  'anchor_top2_partner_reference_mix'\n"
            "]\n"
            "candidate_top_k_values = [4]\n"
            "adopted_pair_count_values = [2]\n"
            "stake_per_pair = 100\n"
            "bootstrap_iterations = 50\n"
            "random_seed = 11\n"
            "split_column = 'split'\n"
            "probability_column = 'pred_probability'\n"
            "window_label_column = 'window_label'\n"
        ),
        encoding="utf-8",
    )

    result = run_wide_research_backtest(load_wide_research_backtest_config(config_path))

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["backtest"]["config_section"] == "wide_research_v3"
    assert payload["backtest"]["score_methods"] == ["pair_model_score"]
    assert payload["backtest"]["pair_generation_methods"] == [
        "anchor_top1_partner_place_edge",
        "anchor_top1_partner_pred_times_place",
        "anchor_top2_partner_reference_mix",
    ]

    comparison_by_generation = {
        row.pair_generation_method: row
        for row in result.comparisons
    }
    assert set(comparison_by_generation) == {
        "anchor_top1_partner_place_edge",
        "anchor_top1_partner_pred_times_place",
        "anchor_top2_partner_reference_mix",
    }
    assert all(row.score_method == "pair_model_score" for row in result.comparisons)
    assert all(row.score_role == "model_primary" for row in result.comparisons)

    method1_pair = next(
        row
        for row in result.selected_pairs
        if row.pair_generation_method == "anchor_top1_partner_place_edge"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    method2_pair = next(
        row
        for row in result.selected_pairs
        if row.pair_generation_method == "anchor_top1_partner_pred_times_place"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    method3_pair = next(
        row
        for row in result.selected_pairs
        if row.pair_generation_method == "anchor_top2_partner_reference_mix"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    assert (method1_pair.horse_number_1, method1_pair.horse_number_2) == (3, 4)
    assert (method2_pair.horse_number_1, method2_pair.horse_number_2) == (3, 4)
    assert (method3_pair.horse_number_1, method3_pair.horse_number_2) == (1, 3)
    assert (
        comparison_by_generation["anchor_top2_partner_reference_mix"].pair_count
        > comparison_by_generation["anchor_top1_partner_place_edge"].pair_count
    )

    csv_lines = (result.output_dir / "comparison.csv").read_text(encoding="utf-8").splitlines()
    assert "pair_generation_method" in csv_lines[0]


def test_run_wide_research_backtest_supports_v4_single_win_anchor_layout(
    tmp_path: Path,
) -> None:
    anchor_predictions_path = tmp_path / "single_win_rolling_predictions.csv"
    anchor_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.55,w0",
                "v1,2,valid,0.45,w0",
                "v1,3,valid,0.35,w0",
                "v1,4,valid,0.25,w0",
                "t1,1,test,0.55,w1",
                "t1,2,test,0.45,w1",
                "t1,3,test,0.35,w1",
                "t1,4,test,0.25,w1",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    partner_predictions_path = tmp_path / "place_rolling_predictions.csv"
    partner_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.20,w0",
                "v1,2,valid,0.30,w0",
                "v1,3,valid,0.65,w0",
                "v1,4,valid,0.40,w0",
                "t1,1,test,0.20,w1",
                "t1,2,test,0.30,w1",
                "t1,3,test,0.65,w1",
                "t1,4,test,0.40,w1",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    hjc_dir = tmp_path / "data" / "raw" / "jrdb" / "HJC_2025"
    hjc_dir.mkdir(parents=True)
    oz_dir = tmp_path / "data" / "raw" / "jrdb" / "OZ_2025"
    oz_dir.mkdir(parents=True)
    ow_dir = tmp_path / "data" / "raw" / "jrdb" / "OW_2025"
    ow_dir.mkdir(parents=True)
    (hjc_dir / "HJC250105.txt").write_bytes(
        make_hjc_line(
            race_key="v1",
            wide_pairs=((1, 3, 220), (2, 3, 260), (1, 2, 0)),
        )
        + b"\r\n"
        + make_hjc_line(
            race_key="t1",
            wide_pairs=((1, 3, 200), (2, 3, 260), (1, 2, 0)),
        )
        + b"\r\n",
    )
    (oz_dir / "OZ250105.txt").write_bytes(
        make_oz_line(
            race_key="v1",
            win_basis_odds=(2.0, 3.0, 6.0, 8.0),
            place_basis_odds=(1.2, 1.3, 2.0, 1.1),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t1",
            win_basis_odds=(2.0, 3.0, 6.0, 8.0),
            place_basis_odds=(1.2, 1.3, 2.0, 1.1),
        )
        + b"\r\n",
    )
    (ow_dir / "OW250105.txt").write_bytes(
        make_ow_line("v1", 4, [5.0, 4.0, 7.0, 6.0, 8.0, 9.0])
        + b"\r\n"
        + make_ow_line("t1", 4, [5.0, 4.0, 7.0, 6.0, 8.0, 9.0])
        + b"\r\n",
    )

    config_path = tmp_path / "wide_research_v4.toml"
    config_path.write_text(
        (
            "[wide_research_v4]\n"
            "name = 'wide_research_v4_smoke'\n"
            f"predictions_path = '{anchor_predictions_path}'\n"
            f"partner_predictions_path = '{partner_predictions_path}'\n"
            f"hjc_raw_dir = '{tmp_path / 'data' / 'raw' / 'jrdb'}'\n"
            f"output_dir = '{tmp_path / 'artifacts' / 'wide_research_v4_smoke'}'\n"
            "score_methods = [\n"
            "  'pair_model_score'\n"
            "]\n"
            "pair_generation_methods = [\n"
            "  'win_pred_top1_partner_place_edge',\n"
            "  'win_edge_top1_partner_place_edge',\n"
            "  'win_pred_top2_partner_reference_mix'\n"
            "]\n"
            "candidate_top_k_values = [4]\n"
            "adopted_pair_count_values = [2]\n"
            "stake_per_pair = 100\n"
            "bootstrap_iterations = 50\n"
            "random_seed = 11\n"
            "split_column = 'split'\n"
            "probability_column = 'pred_probability'\n"
            "partner_probability_column = 'pred_probability'\n"
            "window_label_column = 'window_label'\n"
            "partner_weight_values = [0.2, 0.5]\n"
        ),
        encoding="utf-8",
    )

    result = run_wide_research_backtest(load_wide_research_backtest_config(config_path))

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["backtest"]["config_section"] == "wide_research_v4"
    assert payload["backtest"]["partner_predictions_path"] == str(partner_predictions_path)
    assert payload["backtest"]["pair_generation_methods"] == [
        "win_pred_top1_partner_place_edge",
        "win_edge_top1_partner_place_edge",
        "win_pred_top2_partner_reference_mix",
    ]

    comparison_by_generation = {
        row.pair_generation_method: row
        for row in result.comparisons
    }
    assert set(comparison_by_generation) == {
        "win_pred_top1_partner_place_edge",
        "win_edge_top1_partner_place_edge",
        "win_pred_top2_partner_reference_mix",
    }

    pred_anchor_pair = next(
        row
        for row in result.selected_pairs
        if row.pair_generation_method == "win_pred_top1_partner_place_edge"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    edge_anchor_pair = next(
        row
        for row in result.selected_pairs
        if row.pair_generation_method == "win_edge_top1_partner_place_edge"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    top2_anchor_first_pair = next(
        row
        for row in result.selected_pairs
        if row.pair_generation_method == "win_pred_top2_partner_reference_mix"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    assert (pred_anchor_pair.horse_number_1, pred_anchor_pair.horse_number_2) == (1, 3)
    assert (edge_anchor_pair.horse_number_1, edge_anchor_pair.horse_number_2) == (2, 3)
    assert (top2_anchor_first_pair.horse_number_1, top2_anchor_first_pair.horse_number_2) == (1, 3)
    assert comparison_by_generation["win_pred_top2_partner_reference_mix"].pair_count > (
        comparison_by_generation["win_pred_top1_partner_place_edge"].pair_count
    )


def test_run_wide_research_backtest_supports_v5_role_aware_pair_scores(
    tmp_path: Path,
) -> None:
    anchor_predictions_path = tmp_path / "single_win_rolling_predictions.csv"
    anchor_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.60,w0",
                "v1,2,valid,0.55,w0",
                "v1,3,valid,0.50,w0",
                "v1,4,valid,0.45,w0",
                "t1,1,test,0.60,w1",
                "t1,2,test,0.55,w1",
                "t1,3,test,0.50,w1",
                "t1,4,test,0.45,w1",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    partner_predictions_path = tmp_path / "place_rolling_predictions.csv"
    partner_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.10,w0",
                "v1,2,valid,0.30,w0",
                "v1,3,valid,0.25,w0",
                "v1,4,valid,0.40,w0",
                "t1,1,test,0.10,w1",
                "t1,2,test,0.30,w1",
                "t1,3,test,0.25,w1",
                "t1,4,test,0.40,w1",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    hjc_dir = tmp_path / "data" / "raw" / "jrdb" / "HJC_2025"
    hjc_dir.mkdir(parents=True)
    oz_dir = tmp_path / "data" / "raw" / "jrdb" / "OZ_2025"
    oz_dir.mkdir(parents=True)
    ow_dir = tmp_path / "data" / "raw" / "jrdb" / "OW_2025"
    ow_dir.mkdir(parents=True)
    (hjc_dir / "HJC250105.txt").write_bytes(
        make_hjc_line(
            race_key="v1",
            wide_pairs=((1, 4, 210), (1, 3, 320), (2, 4, 0)),
        )
        + b"\r\n"
        + make_hjc_line(
            race_key="t1",
            wide_pairs=((1, 4, 210), (1, 3, 320), (2, 4, 0)),
        )
        + b"\r\n",
    )
    (oz_dir / "OZ250105.txt").write_bytes(
        make_oz_line(
            race_key="v1",
            win_basis_odds=(2.0, 2.2, 5.0, 8.0),
            place_basis_odds=(1.1, 1.2, 3.0, 1.5),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t1",
            win_basis_odds=(2.0, 2.2, 5.0, 8.0),
            place_basis_odds=(1.1, 1.2, 3.0, 1.5),
        )
        + b"\r\n",
    )
    (ow_dir / "OW250105.txt").write_bytes(
        make_ow_line("v1", 4, [5.0, 6.0, 4.0, 7.0, 8.0, 9.0])
        + b"\r\n"
        + make_ow_line("t1", 4, [5.0, 6.0, 4.0, 7.0, 8.0, 9.0])
        + b"\r\n",
    )

    config_path = tmp_path / "wide_research_v5.toml"
    config_path.write_text(
        (
            "[wide_research_v5]\n"
            "name = 'wide_research_v5_smoke'\n"
            f"predictions_path = '{anchor_predictions_path}'\n"
            f"partner_predictions_path = '{partner_predictions_path}'\n"
            f"hjc_raw_dir = '{tmp_path / 'data' / 'raw' / 'jrdb'}'\n"
            f"output_dir = '{tmp_path / 'artifacts' / 'wide_research_v5_smoke'}'\n"
            "score_methods = [\n"
            "  'anchor_win_pred_times_partner_place_pred',\n"
            "  'anchor_win_edge_times_partner_place_edge',\n"
            "  'anchor_win_pred_times_partner_pred_times_place',\n"
            "  'anchor_plus_weighted_partner'\n"
            "]\n"
            "pair_generation_methods = [\n"
            "  'v5_win_pred_top1_partner_place_edge',\n"
            "  'v5_win_edge_top1_partner_reference_mix'\n"
            "]\n"
            "candidate_top_k_values = [4]\n"
            "adopted_pair_count_values = [1]\n"
            "stake_per_pair = 100\n"
            "bootstrap_iterations = 50\n"
            "random_seed = 11\n"
            "split_column = 'split'\n"
            "probability_column = 'pred_probability'\n"
            "partner_probability_column = 'pred_probability'\n"
            "window_label_column = 'window_label'\n"
            "partner_weight_values = [0.2, 0.5]\n"
        ),
        encoding="utf-8",
    )

    result = run_wide_research_backtest(load_wide_research_backtest_config(config_path))

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["backtest"]["config_section"] == "wide_research_v5"
    assert payload["backtest"]["partner_weight_values"] == [0.2, 0.5]
    assert payload["backtest"]["pair_generation_methods"] == [
        "v5_win_pred_top1_partner_place_edge",
        "v5_win_edge_top1_partner_reference_mix",
    ]

    comparison_by_score = {
        (row.score_method, row.pair_generation_method, row.partner_weight): row
        for row in result.comparisons
    }
    assert comparison_by_score[
        ("anchor_win_pred_times_partner_place_pred", "v5_win_pred_top1_partner_place_edge", None)
    ].score_role == "role_aware_pair_score"
    assert comparison_by_score[
        ("anchor_win_pred_times_partner_place_pred", "v5_win_pred_top1_partner_place_edge", None)
    ].partner_weight is None

    place_pred_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "anchor_win_pred_times_partner_place_pred"
        and row.pair_generation_method == "v5_win_pred_top1_partner_place_edge"
        and row.partner_weight is None
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    pred_times_place_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "anchor_win_pred_times_partner_pred_times_place"
        and row.pair_generation_method == "v5_win_pred_top1_partner_place_edge"
        and row.partner_weight is None
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    weighted_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "anchor_plus_weighted_partner"
        and row.pair_generation_method == "v5_win_pred_top1_partner_place_edge"
        and row.partner_weight == 0.2
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    weighted_pair_heavy = next(
        row
        for row in result.selected_pairs
        if row.score_method == "anchor_plus_weighted_partner"
        and row.pair_generation_method == "v5_win_pred_top1_partner_place_edge"
        and row.partner_weight == 0.5
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    assert (place_pred_pair.horse_number_1, place_pred_pair.horse_number_2) == (1, 4)
    assert (pred_times_place_pair.horse_number_1, pred_times_place_pair.horse_number_2) == (1, 3)
    assert (weighted_pair.horse_number_1, weighted_pair.horse_number_2) == (1, 3)
    assert (weighted_pair_heavy.horse_number_1, weighted_pair_heavy.horse_number_2) == (1, 3)
    assert weighted_pair.partner_weight == 0.2
    assert weighted_pair_heavy.partner_weight == 0.5

    assert any(
        row.score_method == "anchor_plus_weighted_partner" and row.partner_weight in {0.2, 0.5}
        for row in result.comparisons
    )
    assert all(
        row.partner_weight in {None, 0.2, 0.5}
        for row in result.best_settings
    )


def test_run_wide_research_v6_smoke(tmp_path: Path) -> None:
    anchor_predictions_path = tmp_path / "anchor_predictions.csv"
    anchor_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.44,w0",
                "v1,2,valid,0.32,w0",
                "v1,3,valid,0.18,w0",
                "v1,4,valid,0.06,w0",
                "t1,1,test,0.55,w1",
                "t1,2,test,0.20,w1",
                "t1,3,test,0.15,w1",
                "t1,4,test,0.10,w1",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    partner_predictions_path = tmp_path / "partner_predictions.csv"
    partner_predictions_path.write_text(
        "\n".join(
            [
                "race_key,horse_number,split,pred_probability,window_label",
                "v1,1,valid,0.18,w0",
                "v1,2,valid,0.15,w0",
                "v1,3,valid,0.52,w0",
                "v1,4,valid,0.15,w0",
                "t1,1,test,0.14,w1",
                "t1,2,test,0.12,w1",
                "t1,3,test,0.56,w1",
                "t1,4,test,0.18,w1",
            ],
        )
        + "\n",
        encoding="utf-8",
    )
    hjc_dir = tmp_path / "data" / "raw" / "jrdb" / "HJC_2025"
    hjc_dir.mkdir(parents=True)
    oz_dir = tmp_path / "data" / "raw" / "jrdb" / "OZ_2025"
    oz_dir.mkdir(parents=True)
    ow_dir = tmp_path / "data" / "raw" / "jrdb" / "OW_2025"
    ow_dir.mkdir(parents=True)
    (hjc_dir / "HJC250105.txt").write_bytes(
        make_hjc_line(
            race_key="v1",
            wide_pairs=((1, 3, 260), (1, 4, 210), (2, 3, 180)),
        )
        + b"\r\n"
        + make_hjc_line(
            race_key="t1",
            wide_pairs=((1, 3, 260), (1, 4, 210), (2, 3, 180)),
        )
        + b"\r\n",
    )
    (oz_dir / "OZ250105.txt").write_bytes(
        make_oz_line(
            race_key="v1",
            win_basis_odds=(2.0, 2.6, 5.2, 7.5),
            place_basis_odds=(1.2, 1.5, 3.4, 1.8),
        )
        + b"\r\n"
        + make_oz_line(
            race_key="t1",
            win_basis_odds=(2.0, 2.6, 5.2, 7.5),
            place_basis_odds=(1.2, 1.5, 3.4, 1.8),
        )
        + b"\r\n",
    )
    (ow_dir / "OW250105.txt").write_bytes(
        make_ow_line("v1", 4, [5.0, 6.0, 4.5, 7.0, 8.0, 9.0])
        + b"\r\n"
        + make_ow_line("t1", 4, [5.0, 6.0, 4.5, 7.0, 8.0, 9.0])
        + b"\r\n",
    )

    config_path = tmp_path / "wide_research_v6.toml"
    config_path.write_text(
        (
            "[wide_research_v6]\n"
            "name = 'wide_research_v6_smoke'\n"
            f"predictions_path = '{anchor_predictions_path}'\n"
            f"partner_predictions_path = '{partner_predictions_path}'\n"
            f"hjc_raw_dir = '{tmp_path / 'data' / 'raw' / 'jrdb'}'\n"
            f"output_dir = '{tmp_path / 'artifacts' / 'wide_research_v6_smoke'}'\n"
            "score_methods = [\n"
            "  'anchor_times_partner',\n"
            "  'min_anchor_partner_times_payout_tilt',\n"
            "  'geometric_mean_anchor_partner_times_payout_tilt',\n"
            "  'anchor_plus_partner_edge_like'\n"
            "]\n"
            "pair_generation_methods = [\n"
            "  'v6_win_pred_top1_partner_place_edge',\n"
            "  'v6_win_edge_top1_partner_reference_mix'\n"
            "]\n"
            "candidate_top_k_values = [4]\n"
            "adopted_pair_count_values = [1]\n"
            "stake_per_pair = 100\n"
            "bootstrap_iterations = 50\n"
            "random_seed = 13\n"
            "split_column = 'split'\n"
            "probability_column = 'pred_probability'\n"
            "partner_probability_column = 'pred_probability'\n"
            "window_label_column = 'window_label'\n"
        ),
        encoding="utf-8",
    )

    result = run_wide_research_backtest(load_wide_research_backtest_config(config_path))

    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert payload["backtest"]["config_section"] == "wide_research_v6"
    assert payload["backtest"]["pair_generation_methods"] == [
        "v6_win_pred_top1_partner_place_edge",
        "v6_win_edge_top1_partner_reference_mix",
    ]

    assert all(row.score_role == "role_aware_pair_score" for row in result.comparisons)
    assert all(row.partner_weight is None for row in result.comparisons)

    geometric_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "geometric_mean_anchor_partner_times_payout_tilt"
        and row.pair_generation_method == "v6_win_pred_top1_partner_place_edge"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    additive_pair = next(
        row
        for row in result.selected_pairs
        if row.score_method == "anchor_plus_partner_edge_like"
        and row.pair_generation_method == "v6_win_edge_top1_partner_reference_mix"
        and row.window_label == "w1"
        and row.split == "test"
        and row.rank_in_race == 1
    )
    assert geometric_pair.pair_score != 0.0
    assert additive_pair.pair_score != 0.0


def make_hjc_line(
    *,
    race_key: str,
    wide_pairs: tuple[tuple[int, int, int], tuple[int, int, int], tuple[int, int, int]],
) -> bytes:
    chars = [" "] * 442
    chars[0:8] = list(race_key[:8].ljust(8))
    wide_block_start = 143
    block_width = 12
    for index, (first_horse, second_horse, payout) in enumerate(wide_pairs):
        block_start = wide_block_start + (index * block_width)
        chars[block_start : block_start + 4] = list(f"{first_horse:02d}{second_horse:02d}")
        chars[block_start + 4 : block_start + 12] = list(f"{payout:>8d}")
    return "".join(chars).encode("ascii")


def make_oz_line(
    *,
    race_key: str,
    win_basis_odds: tuple[float, ...],
    place_basis_odds: tuple[float, ...],
) -> bytes:
    headcount = len(win_basis_odds)
    win_text = "".join(f"{value:>5.1f}" for value in win_basis_odds)
    place_text = "".join(f"{value:>5.1f}" for value in place_basis_odds)
    return f"{race_key[:8].ljust(8)}{headcount:02d}{win_text}{' ' * 12}{place_text}".encode("ascii")


def make_ow_line(race_key: str, headcount: int, pair_odds: list[float]) -> bytes:
    expected_count = (headcount * (headcount - 1)) // 2
    if len(pair_odds) != expected_count:
        raise ValueError(f"expected {expected_count} pair odds, got {len(pair_odds)}")
    body = "".join(f"{value:>5.1f}" for value in pair_odds)
    return f"{race_key[:8].ljust(8)}{headcount:02d}{body}".ljust(778).encode("ascii")
