from __future__ import annotations

import json
from pathlib import Path

import duckdb

from horse_bet_lab.config import load_bet_logic_only_config
from horse_bet_lab.evaluation.bet_logic_only import run_bet_logic_only_analysis


def test_bet_logic_only_writes_expected_artifacts_and_variants(tmp_path: Path) -> None:
    duckdb_path = tmp_path / "jrdb.duckdb"
    rolling_predictions_path = tmp_path / "rolling_predictions.csv"
    base_config_path = tmp_path / "base_backtest.toml"
    ranking_rule_config_path = tmp_path / "ranking_rule_compare.toml"
    reference_strategy_config_path = tmp_path / "reference_strategy.toml"
    reference_guard_compare_config_path = tmp_path / "reference_guard_compare.toml"
    second_guard_selection_config_path = tmp_path / "second_guard_selection.toml"
    reference_label_guard_compare_config_path = tmp_path / "reference_label_guard_compare.toml"
    analysis_config_path = tmp_path / "bet_logic_only.toml"
    output_dir = tmp_path / "output"

    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_sed_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                registration_id VARCHAR,
                result_date DATE,
                horse_name VARCHAR,
                win_odds DOUBLE,
                popularity INTEGER
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_sed_staging VALUES
                ('race_v1', 1, 'rv1_h1', '2025-01-10', 'Horse V1-1', 5.0, 2),
                ('race_v1', 2, 'rv1_h2', '2025-01-10', 'Horse V1-2', 8.0, 3),
                ('02_t1', 1, 'rt1_h1', '2025-02-10', 'Horse T1-1', 5.0, 2),
                ('02_t1', 2, 'rt1_h2', '2025-02-10', 'Horse T1-2', 8.0, 3),
                ('03_t2', 1, 'rt2_h1', '2025-02-17', 'Horse T2-1', 6.0, 2),
                ('03_t2', 2, 'rt2_h2', '2025-02-17', 'Horse T2-2', 9.0, 3)
            """,
        )
        connection.execute(
            """
            CREATE TABLE jrdb_oz_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                place_basis_odds DOUBLE
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_oz_staging VALUES
                ('race_v1', 1, 2.0),
                ('race_v1', 2, 2.8),
                ('02_t1', 1, 2.0),
                ('02_t1', 2, 2.8),
                ('03_t2', 1, 2.1),
                ('03_t2', 2, 3.0)
            """,
        )
        connection.execute(
            """
            CREATE TABLE jrdb_hjc_staging (
                race_key VARCHAR,
                place_horse_number_1 INTEGER,
                place_payout_1 DOUBLE,
                place_horse_number_2 INTEGER,
                place_payout_2 DOUBLE,
                place_horse_number_3 INTEGER,
                place_payout_3 DOUBLE
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_hjc_staging VALUES
                ('race_v1', 1, 140.0, NULL, NULL, NULL, NULL),
                ('02_t1', 2, 260.0, NULL, NULL, NULL, NULL),
                ('03_t2', 1, 170.0, NULL, NULL, NULL, NULL)
            """,
        )
    finally:
        connection.close()

    rolling_predictions_path.write_text(
        (
            "race_key,horse_number,split,target_value,pred_probability,window_label\n"
            "race_v1,1,valid,1,0.60,2025_01_to_02\n"
            "race_v1,2,valid,0,0.39,2025_01_to_02\n"
            "02_t1,1,test,0,0.60,2025_01_to_02\n"
            "02_t1,2,test,1,0.39,2025_01_to_02\n"
            "03_t2,1,test,1,0.60,2025_01_to_02\n"
            "03_t2,2,test,0,0.36,2025_01_to_02\n"
        ),
        encoding="utf-8",
    )

    base_config_path.write_text(
        "\n".join(
            [
                "[backtest]",
                "name = 'base'",
                f"predictions_path = '{rolling_predictions_path}'",
                f"duckdb_path = '{duckdb_path}'",
                f"output_dir = '{output_dir}'",
                "selection_metric = 'edge'",
                "market_prob_method = 'oz_place_basis_inverse'",
                "thresholds = [0.08]",
                "stake_per_bet = 100",
                "min_bets_valid = 1",
                "",
                "[[backtest.popularity_bands]]",
                "min = 1",
                "max = 3",
                "",
                "[[backtest.place_basis_bands]]",
                "min = 2.0",
                "max = 3.0",
                "",
                "[[backtest.evaluation_window_pairs]]",
                "label = '2025_01_to_02'",
                "valid_start_date = '2025-01-01'",
                "valid_end_date = '2025-01-31'",
                "test_start_date = '2025-02-01'",
                "test_end_date = '2025-02-28'",
                "",
                "[[backtest.selection_window_groups]]",
                "label = 'test_2025_02'",
                "valid_window_labels = ['2025_01_to_02']",
                "test_window_label = '2025_01_to_02'",
                "",
            ],
        ),
        encoding="utf-8",
    )

    ranking_rule_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'ranking_rule_compare_test'",
                f"base_backtest_config_path = '{base_config_path}'",
                f"rolling_predictions_path = '{rolling_predictions_path}'",
                f"output_dir = '{tmp_path / 'ranking_rule_output'}'",
                (
                    "aggregate_selection_score_rule = "
                    "'positive_window_count_then_mean_roi_then_min_roi'"
                ),
                "min_bets_valid = 1",
                (
                    "ranking_score_rules = ["
                    "'edge', 'pred_times_place_basis_odds', "
                    "'edge_times_place_basis_odds', 'edge_plus_payout_tilt']"
                ),
                "",
            ],
        ),
        encoding="utf-8",
    )
    reference_strategy_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_strategy_test'",
                f"ranking_rule_comparison_config_path = '{ranking_rule_config_path}'",
                f"output_dir = '{tmp_path / 'reference_strategy_output'}'",
                "",
            ],
        ),
        encoding="utf-8",
    )
    reference_guard_compare_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_guard_compare_test'",
                f"reference_strategy_config_path = '{reference_strategy_config_path}'",
                f"output_dir = '{tmp_path / 'reference_guard_output'}'",
                "problematic_min_popularity = 3",
                "problematic_max_popularity = 3",
                "problematic_min_place_basis_odds = 2.4",
                "problematic_max_place_basis_odds = 2.8",
                "edge_surcharges = [0.02]",
                "exclude_win_odds_below = 5.0",
                "exclude_edge_below = 0.06",
                "",
            ],
        ),
        encoding="utf-8",
    )
    second_guard_selection_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'second_guard_selection_test'",
                f"reference_guard_compare_config_path = '{reference_guard_compare_config_path}'",
                f"output_dir = '{tmp_path / 'second_guard_output'}'",
                "first_guard_variant = 'problematic_band_excluded'",
                "second_guard_variants = ['no_second_guard']",
                "",
            ],
        ),
        encoding="utf-8",
    )
    reference_label_guard_compare_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'reference_label_guard_compare_test'",
                f"second_guard_selection_config_path = '{second_guard_selection_config_path}'",
                f"output_dir = '{tmp_path / 'reference_label_guard_output'}'",
                "extra_guard_variants = ['no_extra_label_guard']",
                "",
            ],
        ),
        encoding="utf-8",
    )

    analysis_config_path.write_text(
        "\n".join(
            [
                "[analysis]",
                "name = 'bet_logic_only_test'",
                f"base_backtest_config_path = '{base_config_path}'",
                f"rolling_predictions_path = '{rolling_predictions_path}'",
                (
                    "reference_label_guard_compare_config_path = "
                    f"'{reference_label_guard_compare_config_path}'"
                ),
                f"output_dir = '{output_dir}'",
                (
                    "aggregate_selection_score_rule = "
                    "'positive_window_count_then_mean_roi_then_min_roi'"
                ),
                "min_bets_valid = 1",
                "bootstrap_iterations = 100",
                "random_seed = 7",
                "stronger_guard_edge_surcharge = 0.01",
                "sizing_tilt_step = 0.2",
                "sizing_tilt_min_multiplier = 0.8",
                "sizing_tilt_max_multiplier = 1.2",
                "active_run_mode = 'candidate_provisional'",
                "formal_domain_mapping_confirmed = true",
                "no_bet_guard_sensitivity_levels = [0.01, 0.02, 0.03]",
                "sizing_tilt_max_multiplier_sensitivity_levels = [1.1, 1.2, 1.3]",
                "initial_bankroll = 10000",
                "",
            ],
        ),
        encoding="utf-8",
    )

    config = load_bet_logic_only_config(analysis_config_path)
    result = run_bet_logic_only_analysis(config)

    assert (result.output_dir / "summary.csv").exists()
    assert (result.output_dir / "summary.json").exists()
    assert (result.output_dir / "selected_summary.csv").exists()
    assert (result.output_dir / "selected_summary.json").exists()
    assert (result.output_dir / "comparison_readout.csv").exists()
    assert (result.output_dir / "parity_summary.csv").exists()
    assert (result.output_dir / "parity_detail.csv").exists()
    assert (result.output_dir / "diff_summary.csv").exists()
    assert (result.output_dir / "diff_summary.json").exists()
    assert (result.output_dir / "diff_detail.csv").exists()
    assert (result.output_dir / "bankroll_summary.csv").exists()
    assert (result.output_dir / "bankroll_summary.json").exists()
    assert (result.output_dir / "sensitivity_summary.csv").exists()
    assert (result.output_dir / "no_bet_guard_dropped_summary.csv").exists()
    assert (result.output_dir / "no_bet_guard_dropped_detail.csv").exists()
    assert (result.output_dir / "no_bet_guard_kept_summary.csv").exists()
    assert (result.output_dir / "no_bet_guard_stability.csv").exists()
    assert (result.output_dir / "chaos_summary.csv").exists()
    assert (result.output_dir / "chaos_detail.csv").exists()
    assert (result.output_dir / "chaos_correlation.csv").exists()
    assert (result.output_dir / "chaos_bucket_readout.csv").exists()
    assert (result.output_dir / "chaos_dropped_summary.csv").exists()
    assert (result.output_dir / "chaos_dropped_detail.csv").exists()
    assert (result.output_dir / "chaos_stability.csv").exists()
    assert (result.output_dir / "logic_status.csv").exists()
    assert (result.output_dir / "guard_weak_regime_summary.csv").exists()
    assert (result.output_dir / "guard_region_candidates.csv").exists()
    assert (result.output_dir / "overlay_diff_summary.csv").exists()
    assert (result.output_dir / "overlay_diff_detail.csv").exists()
    assert (result.output_dir / "overlay_stability.csv").exists()
    assert (result.output_dir / "final_bet_instructions_candidate.csv").exists()
    assert (result.output_dir / "final_bet_instructions_fallback.csv").exists()
    assert (result.output_dir / "final_race_instructions_candidate.csv").exists()
    assert (result.output_dir / "final_race_instructions_fallback.csv").exists()
    assert (result.output_dir / "final_candidate_vs_fallback_diff.csv").exists()
    assert (result.output_dir / "final_instruction_package_manifest.csv").exists()
    assert (result.output_dir / "final_instruction_package_summary.csv").exists()
    assert (result.output_dir / "monitoring_summary.csv").exists()
    assert (result.output_dir / "regression_gate_report.csv").exists()
    assert (result.output_dir / "artifact_compare_report.csv").exists()
    assert (result.output_dir / "domain_mapping_audit.csv").exists()
    assert (result.output_dir / "domain_mapping_report.csv").exists()
    assert (result.output_dir / "domain_mapping_adoption_memo.md").exists()
    assert (result.output_dir / "hard_adopt_decision.csv").exists()
    assert (result.output_dir / "hard_adopt_decision.json").exists()
    assert (result.output_dir / "hard_adopt_decision_memo.md").exists()

    assert {row.logic_variant for row in result.summaries} == {
        "baseline_current_logic",
        "no_bet_guard_stronger",
        "guard_0_01_plus_proxy_domain_overlay",
        "guard_0_01_plus_near_threshold_overlay",
        "guard_0_01_plus_place_basis_overlay",
        "guard_0_01_plus_domain_x_threshold_overlay",
    }
    baseline = next(row for row in result.summaries if row.logic_variant == "baseline_current_logic")
    stronger_guard = next(
        row for row in result.summaries if row.logic_variant == "no_bet_guard_stronger"
    )
    assert baseline.bet_count >= 0
    assert stronger_guard.bet_count <= baseline.bet_count
    assert result.parity_summary.parity_ok
    assert result.parity_summary.row_diff_count == 0

    diff_row = next(
        row
        for row in result.diff_summaries
        if row.logic_variant == "guard_0_01_plus_proxy_domain_overlay" and row.set_group == "common"
    )
    assert diff_row.baseline_bet_count >= 0
    assert diff_row.logic_variant_bet_count >= 0
    payload = json.loads((result.output_dir / "summary.json").read_text(encoding="utf-8"))
    assert len(payload["analysis"]["rows"]) == 6
    assert len(result.chaos_summary_rows) == 3
    assert any(row.metric_name == "corr_chaos_vs_max_edge" for row in result.chaos_correlation_rows)
    assert any(
        row.logic_variant == "guard_0_01_plus_proxy_domain_overlay"
        and row.status == "hard_adopt"
        for row in result.logic_status_rows
    )
    assert any(
        row.logic_variant == "guard_0_01_plus_proxy_domain_overlay"
        for row in result.overlay_diff_summaries
    )
    assert any(row.decision == "SKIP" for row in result.final_bet_instructions_candidate)
    assert any(row.decision == "BET" for row in result.final_bet_instructions_fallback)
    assert any(row.diff_level == "bet" for row in result.final_candidate_vs_fallback_diff)
    assert any(row.run_mode == "candidate_provisional" for row in result.final_bet_instructions_candidate)
    assert any(row.run_mode == "fallback_stable" for row in result.final_bet_instructions_fallback)
    assert any(row.run_mode == "candidate_provisional" for row in result.instruction_package_manifest_rows)
    assert any(row.run_mode == "fallback_stable" for row in result.instruction_package_manifest_rows)
    assert any(row.run_mode == "candidate_provisional" for row in result.monitoring_summary_rows)
    assert any(row.metric_name == "total_profit" for row in result.regression_gate_rows)
    assert any(row.compare_group == "candidate_vs_fallback_profit_gap" for row in result.artifact_compare_rows)
    assert any(
        row.formal_mapping_status == "project_mapping_formalized"
        for row in result.domain_mapping_report_rows
    )
    assert result.hard_adopt_decision_rows[0].decision_status == "mapping_confirmed"
    memo = (result.output_dir / "domain_mapping_adoption_memo.md").read_text(encoding="utf-8")
    assert "hard adopt" in memo
    hard_memo = (result.output_dir / "hard_adopt_decision_memo.md").read_text(encoding="utf-8")
    assert "hard adopt に上げてよい" in hard_memo
