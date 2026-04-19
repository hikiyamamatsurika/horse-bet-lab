from __future__ import annotations

import argparse
from pathlib import Path

from horse_bet_lab.config import load_wide_research_backtest_config
from horse_bet_lab.evaluation.wide_research_backtest import run_wide_research_backtest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a research-only wide backtest lifted from horse-level predictions.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to a wide research TOML config.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = load_wide_research_backtest_config(args.config)
    result = run_wide_research_backtest(config)
    print(f"Wide research backtest completed: output_dir={result.output_dir}")
    for summary in result.summaries:
        print(
            f"  window={summary.window_label} split={summary.split} "
            f"score_method={summary.score_method} "
            f"pair_generation_method={summary.pair_generation_method} "
            f"candidate_top_k={summary.candidate_top_k} "
            f"adopted_pairs={summary.adopted_pair_count}: "
            f"pairs={summary.pair_count} hits={summary.hit_count} "
            f"hit_rate={summary.hit_rate:.4f} roi={summary.roi:.4f} "
            f"profit={summary.total_profit:.1f}",
        )
    print("Rolling OOS comparison:")
    for comparison in result.comparisons:
        print(
            f"  split={comparison.split} score_role={comparison.score_role} "
            f"score_method={comparison.score_method} "
            f"pair_generation_method={comparison.pair_generation_method} "
            f"candidate_top_k={comparison.candidate_top_k} adopted_pairs={comparison.adopted_pair_count}: "
            f"pairs={comparison.pair_count} hits={comparison.hit_count} "
            f"hit_rate={comparison.hit_rate:.4f} roi={comparison.roi:.4f} "
            f"profit={comparison.total_profit:.1f} window_wins={comparison.window_win_count} "
            f"mean_roi={comparison.mean_roi:.4f} roi_std={comparison.roi_std:.4f} "
            f"roi_ci=[{comparison.roi_ci_lower:.4f},{comparison.roi_ci_upper:.4f}] "
            f"profit_ci=[{comparison.profit_ci_lower:.1f},{comparison.profit_ci_upper:.1f}] "
            f"roi_gt_1_ratio={comparison.roi_gt_1_ratio:.4f}",
        )
    print("Best settings by selection rule:")
    for best_setting in result.best_settings:
        print(
            f"  selection_rule={best_setting.selection_rule} split={best_setting.split} "
            f"score_role={best_setting.score_role} score_method={best_setting.score_method} "
            f"pair_generation_method={best_setting.pair_generation_method} "
            f"candidate_top_k={best_setting.candidate_top_k} "
            f"adopted_pairs={best_setting.adopted_pair_count}: "
            f"pairs={best_setting.pair_count} hits={best_setting.hit_count} "
            f"hit_rate={best_setting.hit_rate:.4f} roi={best_setting.roi:.4f} "
            f"profit={best_setting.total_profit:.1f} "
            f"window_wins={best_setting.window_win_count} "
            f"mean_roi={best_setting.mean_roi:.4f} roi_std={best_setting.roi_std:.4f}",
        )


if __name__ == "__main__":
    main()
