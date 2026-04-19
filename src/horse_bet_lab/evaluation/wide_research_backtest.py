from __future__ import annotations

import csv
import json
import math
import random
import re
from dataclasses import asdict, dataclass
from itertools import combinations
from pathlib import Path

from horse_bet_lab.config import WideResearchBacktestConfig

WIDE_RESEARCH_SUMMARY_COLUMNS = (
    "window_label",
    "split",
    "score_method",
    "pair_generation_method",
    "partner_weight",
    "candidate_top_k",
    "adopted_pair_count",
    "pair_count",
    "hit_count",
    "hit_rate",
    "roi",
    "total_profit",
)
WIDE_RESEARCH_BEST_SETTING_COLUMNS = (
    "selection_rule",
    "split",
    "score_role",
    "score_method",
    "pair_generation_method",
    "partner_weight",
    "candidate_top_k",
    "adopted_pair_count",
    "pair_count",
    "hit_count",
    "hit_rate",
    "roi",
    "total_profit",
    "window_win_count",
    "mean_roi",
    "roi_std",
    "roi_ci_lower",
    "roi_ci_upper",
    "profit_ci_lower",
    "profit_ci_upper",
    "roi_gt_1_ratio",
)
WIDE_RESEARCH_COMPARISON_COLUMNS = (
    "split",
    "score_role",
    "score_method",
    "pair_generation_method",
    "partner_weight",
    "candidate_top_k",
    "adopted_pair_count",
    "pair_count",
    "hit_count",
    "hit_rate",
    "roi",
    "total_profit",
    "window_win_count",
    "mean_roi",
    "roi_std",
    "roi_ci_lower",
    "roi_ci_upper",
    "profit_ci_lower",
    "profit_ci_upper",
    "roi_gt_1_ratio",
)
WIDE_RESEARCH_SELECTED_PAIR_COLUMNS = (
    "window_label",
    "split",
    "race_key",
    "score_method",
    "pair_generation_method",
    "partner_weight",
    "candidate_top_k",
    "adopted_pair_count",
    "rank_in_race",
    "horse_number_1",
    "horse_number_2",
    "horse_probability_1",
    "horse_probability_2",
    "place_basis_odds_1",
    "place_basis_odds_2",
    "geometric_mean_place_basis_odds",
    "ow_wide_odds",
    "abs_prob_gap",
    "pair_score",
    "wide_payout",
    "is_hit",
)
SUPPORTED_SCORE_METHODS = {
    "product",
    "min_prob",
    "sum_logit",
    "product_times_geom_place_basis",
    "min_prob_times_geom_place_basis",
    "product_minus_prob_gap_penalty",
    "pair_model_score",
    "ow_wide_implied_prob",
    "ow_wide_market_prob",
    "pair_edge",
    "no_ow_guard",
    "low_wide_payout_guard",
    "extreme_ow_implied_prob_guard",
    "anchor_win_pred_times_partner_place_pred",
    "anchor_win_edge_times_partner_place_edge",
    "anchor_win_pred_times_partner_pred_times_place",
    "anchor_plus_weighted_partner",
    "anchor_times_partner",
    "min_anchor_partner_times_payout_tilt",
    "geometric_mean_anchor_partner_times_payout_tilt",
    "anchor_plus_partner_edge_like",
}
WIDE_BLOCK_START = 143
WIDE_BLOCK_WIDTH = 12
WIDE_BLOCK_COUNT = 3
DEFAULT_WINDOW_LABEL = "all"
PROB_GAP_PENALTY_WEIGHT = 0.25
LOW_WIDE_PAYOUT_GUARD_MIN_ODDS = 3.0
EXTREME_OW_IMPLIED_PROB_GUARD_MAX = 0.20
PARTNER_SIGNAL_WEIGHT = 0.5
OOS_SPLIT = "test"
SUPPORTED_PAIR_GENERATION_METHODS = (
    "symmetric_top_k_pairs",
    "anchor_top1_partner_place_edge",
    "anchor_top1_partner_pred_times_place",
    "anchor_top2_partner_reference_mix",
    "win_pred_top1_partner_place_edge",
    "win_pred_top1_partner_pred_times_place",
    "win_pred_top1_partner_reference_mix",
    "win_edge_top1_partner_place_edge",
    "win_edge_top1_partner_pred_times_place",
    "win_edge_top1_partner_reference_mix",
    "win_pred_top2_partner_place_edge",
    "win_pred_top2_partner_pred_times_place",
    "win_pred_top2_partner_reference_mix",
    "v5_win_pred_top1_partner_place_edge",
    "v5_win_pred_top1_partner_pred_times_place",
    "v5_win_pred_top1_partner_reference_mix",
    "v5_win_edge_top1_partner_place_edge",
    "v5_win_edge_top1_partner_pred_times_place",
    "v5_win_edge_top1_partner_reference_mix",
    "v6_win_pred_top1_partner_place_edge",
    "v6_win_pred_top1_partner_pred_times_place",
    "v6_win_pred_top1_partner_reference_mix",
    "v6_win_edge_top1_partner_place_edge",
    "v6_win_edge_top1_partner_pred_times_place",
    "v6_win_edge_top1_partner_reference_mix",
)
SUPPORTED_SELECTION_RULES = (
    "total_oos_roi_max",
    "window_win_count_max",
    "mean_roi_minus_std",
)


@dataclass(frozen=True)
class PredictionRow:
    race_key: str
    horse_number: int
    split: str
    window_label: str
    pred_probability: float
    partner_probability: float


@dataclass(frozen=True)
class PairScoreInputs:
    anchor_signal: float
    partner_signal: float
    anchor_win_pred: float
    anchor_win_edge: float
    partner_place_pred: float
    partner_place_edge: float
    partner_pred_times_place: float
    partner_reference_mix: float


@dataclass(frozen=True)
class SelectedPairRow:
    window_label: str
    split: str
    race_key: str
    score_method: str
    pair_generation_method: str
    partner_weight: float | None
    candidate_top_k: int
    adopted_pair_count: int
    rank_in_race: int
    horse_number_1: int
    horse_number_2: int
    horse_probability_1: float
    horse_probability_2: float
    place_basis_odds_1: float
    place_basis_odds_2: float
    geometric_mean_place_basis_odds: float
    ow_wide_odds: float | None
    abs_prob_gap: float
    pair_score: float
    wide_payout: float | None
    is_hit: bool


@dataclass(frozen=True)
class WideResearchSummary:
    window_label: str
    split: str
    score_method: str
    pair_generation_method: str
    partner_weight: float | None
    candidate_top_k: int
    adopted_pair_count: int
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float


@dataclass(frozen=True)
class WideResearchBestSetting:
    selection_rule: str
    split: str
    score_role: str
    score_method: str
    pair_generation_method: str
    partner_weight: float | None
    candidate_top_k: int
    adopted_pair_count: int
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    window_win_count: int
    mean_roi: float
    roi_std: float
    roi_ci_lower: float
    roi_ci_upper: float
    profit_ci_lower: float
    profit_ci_upper: float
    roi_gt_1_ratio: float


@dataclass(frozen=True)
class WideResearchComparisonRow:
    split: str
    score_role: str
    score_method: str
    pair_generation_method: str
    partner_weight: float | None
    candidate_top_k: int
    adopted_pair_count: int
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    window_win_count: int
    mean_roi: float
    roi_std: float
    roi_ci_lower: float
    roi_ci_upper: float
    profit_ci_lower: float
    profit_ci_upper: float
    roi_gt_1_ratio: float


@dataclass(frozen=True)
class WideResearchBacktestResult:
    output_dir: Path
    summaries: tuple[WideResearchSummary, ...]
    comparisons: tuple[WideResearchComparisonRow, ...]
    best_settings: tuple[WideResearchBestSetting, ...]
    selected_pairs: tuple[SelectedPairRow, ...]


def run_wide_research_backtest(
    config: WideResearchBacktestConfig,
) -> WideResearchBacktestResult:
    validate_wide_research_config(config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    predictions = load_prediction_rows(config)
    wide_payouts = load_wide_payouts(config.hjc_raw_dir)
    win_basis_odds, place_basis_odds = load_oz_basis_odds(config.hjc_raw_dir)
    ow_wide_odds = load_ow_wide_odds(config.hjc_raw_dir)
    race_payouts = group_wide_payouts_by_race(wide_payouts)
    selected_pairs = build_selected_pair_rows(
        predictions=predictions,
        race_payouts=race_payouts,
        win_basis_odds=win_basis_odds,
        place_basis_odds=place_basis_odds,
        ow_wide_odds=ow_wide_odds,
        config=config,
    )
    summaries = build_wide_research_summaries(selected_pairs, config.stake_per_pair)
    comparisons = build_wide_research_comparisons(
        summaries,
        stake_per_pair=config.stake_per_pair,
        bootstrap_iterations=config.bootstrap_iterations,
        random_seed=config.random_seed,
    )
    best_settings = build_wide_research_best_settings(comparisons)

    write_wide_research_summary_csv(config.output_dir / "summary.csv", summaries)
    write_wide_research_summary_json(
        config.output_dir / "summary.json",
        config,
        summaries,
        comparisons,
        best_settings,
    )
    write_wide_research_comparison_csv(config.output_dir / "comparison.csv", comparisons)
    write_wide_research_comparison_json(config.output_dir / "comparison.json", comparisons)
    write_wide_research_best_settings_csv(config.output_dir / "best_settings.csv", best_settings)
    write_wide_research_best_settings_json(
        config.output_dir / "best_settings.json",
        best_settings,
    )
    write_wide_research_selected_pairs_csv(config.output_dir / "selected_pairs.csv", selected_pairs)
    write_wide_research_selected_pairs_json(
        config.output_dir / "selected_pairs.json",
        selected_pairs,
    )
    return WideResearchBacktestResult(
        output_dir=config.output_dir,
        summaries=tuple(summaries),
        comparisons=tuple(comparisons),
        best_settings=tuple(best_settings),
        selected_pairs=tuple(selected_pairs),
    )


def validate_wide_research_config(config: WideResearchBacktestConfig) -> None:
    unsupported_methods = sorted(set(config.score_methods) - SUPPORTED_SCORE_METHODS)
    if unsupported_methods:
        raise ValueError(f"unsupported score_methods: {unsupported_methods}")
    if not config.score_methods:
        raise ValueError("score_methods must not be empty")
    unsupported_generation_methods = sorted(
        method
        for method in config.pair_generation_methods
        if not is_supported_pair_generation_method(method)
    )
    if unsupported_generation_methods:
        raise ValueError(f"unsupported pair_generation_methods: {unsupported_generation_methods}")
    if not config.pair_generation_methods:
        raise ValueError("pair_generation_methods must not be empty")
    if not config.candidate_top_k_values:
        raise ValueError("candidate_top_k_values must not be empty")
    if any(value < 2 for value in config.candidate_top_k_values):
        raise ValueError("candidate_top_k_values must be at least 2")
    if not config.adopted_pair_count_values:
        raise ValueError("adopted_pair_count_values must not be empty")
    if any(value <= 0 for value in config.adopted_pair_count_values):
        raise ValueError("adopted_pair_count_values must be positive")
    if not config.partner_weight_values:
        raise ValueError("partner_weight_values must not be empty")
    if any(value < 0.0 for value in config.partner_weight_values):
        raise ValueError("partner_weight_values must be non-negative")
    if config.stake_per_pair <= 0.0:
        raise ValueError("stake_per_pair must be positive")


def is_supported_pair_generation_method(pair_generation_method: str) -> bool:
    if pair_generation_method in SUPPORTED_PAIR_GENERATION_METHODS:
        return True
    return (
        re.fullmatch(
            r"(?:v[56]_)?(win_pred|win_edge)_top(\d+)_partner_(place_edge|pred_times_place|reference_mix)",
            pair_generation_method,
        )
        is not None
    )


def load_prediction_rows(config: WideResearchBacktestConfig) -> tuple[PredictionRow, ...]:
    primary_rows = load_prediction_probability_rows(
        predictions_path=config.predictions_path,
        split_column=config.split_column,
        probability_column=config.probability_column,
        window_label_column=config.window_label_column,
    )
    if config.partner_predictions_path is None:
        return tuple(
            PredictionRow(
                race_key=row.race_key,
                horse_number=row.horse_number,
                split=row.split,
                window_label=row.window_label,
                pred_probability=row.pred_probability,
                partner_probability=row.pred_probability,
            )
            for row in primary_rows
        )
    partner_rows = load_prediction_probability_rows(
        predictions_path=config.partner_predictions_path,
        split_column=config.split_column,
        probability_column=config.partner_probability_column,
        window_label_column=config.window_label_column,
    )
    partner_index = {
        (row.window_label, row.split, row.race_key, row.horse_number): row.pred_probability
        for row in partner_rows
    }
    merged_rows: list[PredictionRow] = []
    missing_partner_keys: list[tuple[str, str, str, int]] = []
    for row in primary_rows:
        key = (row.window_label, row.split, row.race_key, row.horse_number)
        partner_probability = partner_index.get(key)
        if partner_probability is None:
            missing_partner_keys.append(key)
            continue
        merged_rows.append(
            PredictionRow(
                race_key=row.race_key,
                horse_number=row.horse_number,
                split=row.split,
                window_label=row.window_label,
                pred_probability=row.pred_probability,
                partner_probability=partner_probability,
            ),
        )
    if missing_partner_keys:
        sample = ", ".join(
            f"{window}:{split}:{race}:{horse}"
            for window, split, race, horse in missing_partner_keys[:5]
        )
        raise ValueError(f"partner predictions missing keys: {sample}")
    return tuple(merged_rows)


def load_prediction_probability_rows(
    *,
    predictions_path: Path,
    split_column: str,
    probability_column: str,
    window_label_column: str,
) -> tuple[PredictionRow, ...]:
    rows: list[PredictionRow] = []
    with predictions_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError("predictions file is missing a header row")
        required_columns = {"race_key", "horse_number", split_column, probability_column}
        missing_columns = sorted(required_columns - set(reader.fieldnames))
        if missing_columns:
            raise ValueError(f"predictions file missing columns: {missing_columns}")
        has_window_label = window_label_column in reader.fieldnames
        for row in reader:
            window_label = (
                str(row[window_label_column]).strip()
                if has_window_label and row[window_label_column] is not None
                else DEFAULT_WINDOW_LABEL
            )
            rows.append(
                PredictionRow(
                    race_key=str(row["race_key"]).strip(),
                    horse_number=int(row["horse_number"]),
                    split=str(row[split_column]).strip(),
                    window_label=window_label or DEFAULT_WINDOW_LABEL,
                    pred_probability=float(row[probability_column]),
                    partner_probability=float(row[probability_column]),
                ),
            )
    return tuple(rows)


def load_wide_payouts(hjc_raw_dir: Path) -> dict[tuple[str, str], float]:
    payouts: dict[tuple[str, str], float] = {}
    for path in sorted(hjc_raw_dir.rglob("HJC*.txt")):
        with path.open("rb") as file:
            for raw_line in file:
                line = raw_line.rstrip(b"\r\n")
                if len(line) < WIDE_BLOCK_START + (WIDE_BLOCK_WIDTH * WIDE_BLOCK_COUNT):
                    continue
                race_key = line[0:8].decode("ascii").strip()
                if not race_key:
                    continue
                for block_index in range(WIDE_BLOCK_COUNT):
                    block_start = WIDE_BLOCK_START + (block_index * WIDE_BLOCK_WIDTH)
                    pair_raw = line[block_start : block_start + 4].decode("ascii").strip()
                    payout_raw = line[block_start + 4 : block_start + 12].decode("ascii").strip()
                    if len(pair_raw) != 4 or not payout_raw:
                        continue
                    payout = int(payout_raw)
                    if payout <= 0:
                        continue
                    pair_key = normalize_pair_key(int(pair_raw[0:2]), int(pair_raw[2:4]))
                    payouts[(race_key, pair_key)] = float(payout)
    if not payouts:
        raise ValueError(f"no wide payouts found under hjc_raw_dir={hjc_raw_dir}")
    return payouts


def group_wide_payouts_by_race(
    payouts: dict[tuple[str, str], float],
) -> dict[str, dict[str, float]]:
    grouped: dict[str, dict[str, float]] = {}
    for (race_key, pair_key), payout in payouts.items():
        grouped.setdefault(race_key, {})[pair_key] = payout
    return grouped


def load_oz_basis_odds(
    raw_dir: Path,
) -> tuple[dict[tuple[str, int], float], dict[tuple[str, int], float]]:
    win_basis_odds: dict[tuple[str, int], float] = {}
    place_basis_odds: dict[tuple[str, int], float] = {}
    for path in sorted(raw_dir.rglob("OZ*.txt")):
        with path.open("rb") as file:
            for line_number, raw_line in enumerate(file, start=1):
                line = raw_line.rstrip(b"\r\n")
                if not line.strip():
                    continue
                text = line.decode("cp932", errors="ignore")
                race_key = text[0:8].strip()
                headcount_raw = text[8:10].strip()
                if not race_key or not headcount_raw:
                    continue
                headcount = int(headcount_raw)
                odds_values = re.findall(r"\d+\.\d", text[10:])
                expected_count = headcount * 2
                if len(odds_values) < expected_count:
                    raise ValueError(
                        f"{path} line {line_number}: expected at least {expected_count} OZ odds values",
                    )
                win_values = odds_values[:headcount]
                place_values = odds_values[headcount:expected_count]
                for horse_number in range(1, headcount + 1):
                    win_basis_odds[(race_key, horse_number)] = float(win_values[horse_number - 1])
                    place_basis_odds[(race_key, horse_number)] = float(place_values[horse_number - 1])
    if not win_basis_odds:
        raise ValueError(f"no OZ win basis odds found under raw_dir={raw_dir}")
    if not place_basis_odds:
        raise ValueError(f"no OZ place basis odds found under raw_dir={raw_dir}")
    return win_basis_odds, place_basis_odds


def load_ow_wide_odds(raw_dir: Path) -> dict[tuple[str, str], float]:
    wide_odds: dict[tuple[str, str], float] = {}
    for path in sorted(raw_dir.rglob("OW*.txt")):
        with path.open("rb") as file:
            for line_number, raw_line in enumerate(file, start=1):
                line = raw_line.rstrip(b"\r\n")
                if len(line) != 778:
                    continue
                text = line.decode("cp932", errors="ignore")
                race_key = text[:8].strip()
                headcount_raw = text[8:10].strip()
                if not race_key or not headcount_raw:
                    continue
                headcount = int(headcount_raw)
                odds_values = parse_ow_odds_values(text[10:])
                expected_count = math.comb(headcount, 2)
                if len(odds_values) != expected_count:
                    raise ValueError(
                        f"{path} line {line_number}: expected {expected_count} OW odds values, got {len(odds_values)}",
                    )
                for pair_odds, (first_horse, second_horse) in zip(
                    odds_values,
                    combinations(range(1, headcount + 1), 2),
                    strict=True,
                ):
                    wide_odds[(race_key, normalize_pair_key(first_horse, second_horse))] = pair_odds
    if not wide_odds:
        raise ValueError(f"no OW wide odds found under raw_dir={raw_dir}")
    return wide_odds


def parse_ow_odds_values(body: str) -> list[float]:
    values: list[float] = []
    index = 0
    while index < len(body):
        while index < len(body) and body[index] == " ":
            index += 1
        if index >= len(body):
            break
        cursor = index
        while cursor < len(body) and body[cursor].isdigit():
            cursor += 1
        if cursor == index or cursor >= len(body) or body[cursor] != ".":
            index += 1
            continue
        if cursor + 1 >= len(body) or not body[cursor + 1].isdigit():
            index += 1
            continue
        values.append(float(body[index : cursor + 2]))
        index = cursor + 2
    return values


def build_selected_pair_rows(
    *,
    predictions: tuple[PredictionRow, ...],
    race_payouts: dict[str, dict[str, float]],
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
    config: WideResearchBacktestConfig,
) -> list[SelectedPairRow]:
    grouped_predictions: dict[tuple[str, str, str], list[PredictionRow]] = {}
    for row in predictions:
        grouped_predictions.setdefault((row.window_label, row.split, row.race_key), []).append(row)

    missing_race_keys = sorted(
        race_key
        for (_, _, race_key) in grouped_predictions
        if race_key not in race_payouts
    )
    if missing_race_keys:
        sample = ", ".join(missing_race_keys[:5])
        raise ValueError(f"wide payouts missing for race_keys: {sample}")

    selected_rows: list[SelectedPairRow] = []
    for score_method in config.score_methods:
        for pair_generation_method in config.pair_generation_methods:
            for partner_weight in config.partner_weight_values:
                for candidate_top_k in config.candidate_top_k_values:
                    for (window_label, split, race_key), race_rows in sorted(grouped_predictions.items()):
                        ranked_pairs = rank_pairs_for_race(
                            race_rows=race_rows,
                            score_method=score_method,
                            pair_generation_method=pair_generation_method,
                            partner_weight=partner_weight,
                            candidate_top_k=candidate_top_k,
                            win_basis_odds=win_basis_odds,
                            place_basis_odds=place_basis_odds,
                            ow_wide_odds=ow_wide_odds,
                        )
                        payouts_for_race = race_payouts[race_key]
                        for adopted_pair_count in config.adopted_pair_count_values:
                            for rank_in_race, pair in enumerate(
                                ranked_pairs[:adopted_pair_count],
                                start=1,
                            ):
                                payout = payouts_for_race.get(pair["pair_key"])
                                selected_rows.append(
                                    SelectedPairRow(
                                        window_label=window_label,
                                        split=split,
                                        race_key=race_key,
                                        score_method=score_method,
                                        pair_generation_method=pair_generation_method,
                                        partner_weight=float(pair["partner_weight"])
                                        if pair["partner_weight"] is not None
                                        else None,
                                        candidate_top_k=candidate_top_k,
                                        adopted_pair_count=adopted_pair_count,
                                        rank_in_race=rank_in_race,
                                        horse_number_1=int(pair["horse_number_1"]),
                                        horse_number_2=int(pair["horse_number_2"]),
                                        horse_probability_1=float(pair["horse_probability_1"]),
                                        horse_probability_2=float(pair["horse_probability_2"]),
                                        place_basis_odds_1=float(pair["place_basis_odds_1"]),
                                        place_basis_odds_2=float(pair["place_basis_odds_2"]),
                                        geometric_mean_place_basis_odds=float(
                                            pair["geometric_mean_place_basis_odds"],
                                        ),
                                        ow_wide_odds=(
                                            float(pair["ow_wide_odds"])
                                            if pair["ow_wide_odds"] is not None
                                            else None
                                        ),
                                        abs_prob_gap=float(pair["abs_prob_gap"]),
                                        pair_score=float(pair["pair_score"]),
                                        wide_payout=payout,
                                        is_hit=payout is not None,
                                    ),
                                )
    return selected_rows


def rank_pairs_for_race(
    *,
    race_rows: list[PredictionRow],
    score_method: str,
    pair_generation_method: str,
    partner_weight: float,
    candidate_top_k: int,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
) -> list[dict[str, object]]:
    if pair_generation_method == "symmetric_top_k_pairs":
        return rank_symmetric_pairs_for_race(
            race_rows=race_rows,
            score_method=score_method,
            partner_weight=partner_weight,
            candidate_top_k=candidate_top_k,
            place_basis_odds=place_basis_odds,
            ow_wide_odds=ow_wide_odds,
        )
    return rank_asymmetric_pairs_for_race(
        race_rows=race_rows,
        score_method=score_method,
        pair_generation_method=pair_generation_method,
        partner_weight=partner_weight,
        candidate_top_k=candidate_top_k,
        win_basis_odds=win_basis_odds,
        place_basis_odds=place_basis_odds,
        ow_wide_odds=ow_wide_odds,
    )


def rank_symmetric_pairs_for_race(
    *,
    race_rows: list[PredictionRow],
    score_method: str,
    partner_weight: float,
    candidate_top_k: int,
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
) -> list[dict[str, object]]:
    ranked_pairs: list[dict[str, object]] = []
    sorted_rows = sorted(
        race_rows,
        key=lambda row: (-row.pred_probability, row.horse_number),
    )
    candidate_rows = sorted_rows[:candidate_top_k]
    for first_row, second_row in combinations(candidate_rows, 2):
        pair = build_pair_candidate(
            first_row=first_row,
            second_row=second_row,
            score_method=score_method,
            partner_weight=partner_weight,
            place_basis_odds=place_basis_odds,
            ow_wide_odds=ow_wide_odds,
        )
        if pair is not None:
            ranked_pairs.append(pair)
    sort_ranked_pairs(ranked_pairs)
    return ranked_pairs


def rank_asymmetric_pairs_for_race(
    *,
    race_rows: list[PredictionRow],
    score_method: str,
    pair_generation_method: str,
    partner_weight: float,
    candidate_top_k: int,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
) -> list[dict[str, object]]:
    candidate_rows = sorted(
        race_rows,
        key=lambda row: (-row.pred_probability, row.horse_number),
    )[:candidate_top_k]
    if len(candidate_rows) < 2:
        return []
    ranked_pairs: list[dict[str, object]] = []
    anchor_rule, anchor_count, partner_rule, selection_style = parse_asymmetric_pair_generation_method(
        pair_generation_method,
    )
    anchor_sorted_rows = sorted(
        candidate_rows,
        key=lambda row: (
            -horse_anchor_score(
                row,
                anchor_rule=anchor_rule,
                win_basis_odds=win_basis_odds,
                place_basis_odds=place_basis_odds,
            ),
            -row.pred_probability,
            row.horse_number,
        ),
    )
    for pair in choose_anchor_partner_pairs(
        anchor_rows=anchor_sorted_rows[:anchor_count],
        partner_rows=candidate_rows,
        anchor_rule=anchor_rule,
        partner_rule=partner_rule,
        selection_style=selection_style,
        score_method=score_method,
        partner_weight=partner_weight,
        win_basis_odds=win_basis_odds,
        place_basis_odds=place_basis_odds,
        ow_wide_odds=ow_wide_odds,
    ):
        ranked_pairs.append(pair)
    deduped_pairs: dict[str, dict[str, object]] = {}
    for pair in ranked_pairs:
        deduped_pairs[str(pair["pair_key"])] = pair
    ranked_pairs = list(deduped_pairs.values())
    sort_ranked_pairs(ranked_pairs)
    return ranked_pairs


def choose_anchor_partner_pair(
    *,
    anchor_rows: list[PredictionRow],
    partner_rows: list[PredictionRow],
    anchor_rule: str,
    partner_rule: str,
    selection_style: str,
    score_method: str,
    partner_weight: float,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
) -> dict[str, object] | None:
    pairs = choose_anchor_partner_pairs(
        anchor_rows=anchor_rows,
        partner_rows=partner_rows,
        anchor_rule=anchor_rule,
        partner_rule=partner_rule,
        selection_style=selection_style,
        score_method=score_method,
        partner_weight=partner_weight,
        win_basis_odds=win_basis_odds,
        place_basis_odds=place_basis_odds,
        ow_wide_odds=ow_wide_odds,
    )
    return pairs[0] if pairs else None


def choose_anchor_partner_pairs(
    *,
    anchor_rows: list[PredictionRow],
    partner_rows: list[PredictionRow],
    anchor_rule: str,
    partner_rule: str,
    selection_style: str,
    score_method: str,
    partner_weight: float,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
) -> list[dict[str, object]]:
    selected_pairs: list[dict[str, object]] = []
    for anchor_row in anchor_rows:
        eligible_partner_rows = [row for row in partner_rows if row.horse_number != anchor_row.horse_number]
        if selection_style == "partner_first":
            sorted_partners = sort_partner_rows(
                eligible_partner_rows,
                partner_rule=partner_rule,
                place_basis_odds=place_basis_odds,
            )
            for partner_row in sorted_partners:
                pair = build_asymmetric_pair_candidate(
                    anchor_row=anchor_row,
                    partner_row=partner_row,
                    anchor_rule=anchor_rule,
                    partner_rule=partner_rule,
                    score_method=score_method,
                    partner_weight=partner_weight,
                    win_basis_odds=win_basis_odds,
                    place_basis_odds=place_basis_odds,
                    ow_wide_odds=ow_wide_odds,
                )
                if pair is not None:
                    selected_pairs.append(pair)
                    break
            continue
        if selection_style == "pair_score_ranked":
            for partner_row in eligible_partner_rows:
                pair = build_asymmetric_pair_candidate(
                    anchor_row=anchor_row,
                    partner_row=partner_row,
                    anchor_rule=anchor_rule,
                    partner_rule=partner_rule,
                    score_method=score_method,
                    partner_weight=partner_weight,
                    win_basis_odds=win_basis_odds,
                    place_basis_odds=place_basis_odds,
                    ow_wide_odds=ow_wide_odds,
                )
                if pair is not None:
                    selected_pairs.append(pair)
            continue
        raise ValueError(f"unsupported selection_style: {selection_style}")
    return selected_pairs


def sort_partner_rows(
    rows: list[PredictionRow],
    *,
    partner_rule: str,
    place_basis_odds: dict[tuple[str, int], float],
) -> list[PredictionRow]:
    if partner_rule == "place_edge":
        return sorted(
            rows,
            key=lambda row: (
                -horse_place_edge_score(row, place_basis_odds),
                -row.partner_probability,
                -lookup_place_basis_odds(place_basis_odds, row),
                row.horse_number,
            ),
        )
    if partner_rule == "pred_times_place_basis":
        return sorted(
            rows,
            key=lambda row: (
                -horse_pred_times_place_basis_score(row, place_basis_odds),
                -row.partner_probability,
                row.horse_number,
            ),
        )
    if partner_rule == "reference_mix":
        return sorted(
            rows,
            key=lambda row: (
                -horse_reference_partner_score(row, place_basis_odds),
                -horse_place_edge_score(row, place_basis_odds),
                -row.partner_probability,
                row.horse_number,
            ),
        )
    raise ValueError(f"unsupported partner_rule: {partner_rule}")


def build_pair_candidate(
    *,
    first_row: PredictionRow,
    second_row: PredictionRow,
    first_probability: float | None = None,
    second_probability: float | None = None,
    score_method: str,
    partner_weight: float,
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
    pair_score_inputs: PairScoreInputs | None = None,
) -> dict[str, object] | None:
    first_probability_value = (
        first_row.pred_probability if first_probability is None else first_probability
    )
    second_probability_value = (
        second_row.pred_probability if second_probability is None else second_probability
    )
    first_place_basis_odds = lookup_place_basis_odds(place_basis_odds, first_row)
    second_place_basis_odds = lookup_place_basis_odds(place_basis_odds, second_row)
    geometric_mean_place_basis_odds = math.sqrt(
        first_place_basis_odds * second_place_basis_odds,
    )
    abs_prob_gap = abs(first_probability_value - second_probability_value)
    horse_number_1, horse_number_2 = sorted((first_row.horse_number, second_row.horse_number))
    pair_key = normalize_pair_key(horse_number_1, horse_number_2)
    pair_ow_wide_odds = ow_wide_odds.get((first_row.race_key, pair_key))
    if not passes_ow_guard(score_method, pair_ow_wide_odds):
        return None
    return {
        "pair_key": pair_key,
        "horse_number_1": horse_number_1,
        "horse_number_2": horse_number_2,
        "partner_weight": (
            partner_weight if score_method == "anchor_plus_weighted_partner" else None
        ),
        "horse_probability_1": first_probability_value,
        "horse_probability_2": second_probability_value,
        "place_basis_odds_1": first_place_basis_odds,
        "place_basis_odds_2": second_place_basis_odds,
        "geometric_mean_place_basis_odds": geometric_mean_place_basis_odds,
        "ow_wide_odds": pair_ow_wide_odds,
        "abs_prob_gap": abs_prob_gap,
        "pair_score": score_pair(
            score_method,
            first_probability_value,
            second_probability_value,
            geometric_mean_place_basis_odds,
            abs_prob_gap,
            pair_ow_wide_odds,
            pair_score_inputs,
            partner_weight,
        ),
    }


def build_asymmetric_pair_candidate(
    *,
    anchor_row: PredictionRow,
    partner_row: PredictionRow,
    anchor_rule: str,
    partner_rule: str,
    score_method: str,
    partner_weight: float,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
    ow_wide_odds: dict[tuple[str, str], float],
) -> dict[str, object] | None:
    return build_pair_candidate(
        first_row=anchor_row,
        second_row=partner_row,
        first_probability=anchor_row.pred_probability,
        second_probability=partner_row.partner_probability,
        score_method=score_method,
        partner_weight=partner_weight,
        place_basis_odds=place_basis_odds,
        ow_wide_odds=ow_wide_odds,
        pair_score_inputs=build_pair_score_inputs(
            anchor_row=anchor_row,
            partner_row=partner_row,
            anchor_rule=anchor_rule,
            partner_rule=partner_rule,
            win_basis_odds=win_basis_odds,
            place_basis_odds=place_basis_odds,
        ),
    )


def sort_ranked_pairs(ranked_pairs: list[dict[str, object]]) -> None:
    ranked_pairs.sort(
        key=lambda item: (
            -float(item["pair_score"]),
            -min(float(item["horse_probability_1"]), float(item["horse_probability_2"])),
            -(float(item["horse_probability_1"]) + float(item["horse_probability_2"])),
            str(item["pair_key"]),
        ),
    )


def parse_asymmetric_pair_generation_method(
    pair_generation_method: str,
) -> tuple[str, int, str, str]:
    legacy_map = {
        "anchor_top1_partner_place_edge": ("model_anchor", 1, "place_edge", "partner_first"),
        "anchor_top1_partner_pred_times_place": (
            "model_anchor",
            1,
            "pred_times_place_basis",
            "partner_first",
        ),
        "anchor_top2_partner_reference_mix": ("model_anchor", 2, "reference_mix", "partner_first"),
    }
    if pair_generation_method in legacy_map:
        return legacy_map[pair_generation_method]
    if pair_generation_method.startswith(("v5_", "v6_")):
        match = re.fullmatch(
            r"v[56]_(win_pred|win_edge)_top(\d+)_partner_(place_edge|pred_times_place|reference_mix)",
            pair_generation_method,
        )
        if match is None:
            raise ValueError(f"unsupported pair_generation_method: {pair_generation_method}")
        partner_rule = match.group(3)
        if partner_rule == "pred_times_place":
            partner_rule = "pred_times_place_basis"
        return match.group(1), int(match.group(2)), partner_rule, "pair_score_ranked"
    match = re.fullmatch(
        r"(win_pred|win_edge)_top(\d+)_partner_(place_edge|pred_times_place|reference_mix)",
        pair_generation_method,
    )
    if match is None:
        raise ValueError(f"unsupported pair_generation_method: {pair_generation_method}")
    partner_rule = match.group(3)
    if partner_rule == "pred_times_place":
        partner_rule = "pred_times_place_basis"
    return match.group(1), int(match.group(2)), partner_rule, "partner_first"


def horse_anchor_score(
    row: PredictionRow,
    *,
    anchor_rule: str,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
) -> float:
    if anchor_rule == "model_anchor":
        return horse_model_anchor_score(row, place_basis_odds)
    if anchor_rule == "win_pred":
        return row.pred_probability
    if anchor_rule == "win_edge":
        return row.pred_probability - horse_win_market_prob(row, win_basis_odds)
    raise ValueError(f"unsupported anchor_rule: {anchor_rule}")


def horse_model_anchor_score(
    row: PredictionRow,
    place_basis_odds: dict[tuple[str, int], float],
) -> float:
    return row.pred_probability * math.sqrt(lookup_place_basis_odds(place_basis_odds, row))


def horse_pred_times_place_basis_score(
    row: PredictionRow,
    place_basis_odds: dict[tuple[str, int], float],
) -> float:
    return row.partner_probability * lookup_place_basis_odds(place_basis_odds, row)


def horse_market_prob(
    row: PredictionRow,
    place_basis_odds: dict[tuple[str, int], float],
) -> float:
    return 1.0 / lookup_place_basis_odds(place_basis_odds, row)


def horse_place_edge_score(
    row: PredictionRow,
    place_basis_odds: dict[tuple[str, int], float],
) -> float:
    return row.partner_probability - horse_market_prob(row, place_basis_odds)


def horse_reference_partner_score(
    row: PredictionRow,
    place_basis_odds: dict[tuple[str, int], float],
) -> float:
    place_basis = lookup_place_basis_odds(place_basis_odds, row)
    edge = horse_place_edge_score(row, place_basis_odds)
    return (row.partner_probability * place_basis) + (edge * place_basis)


def horse_win_market_prob(
    row: PredictionRow,
    win_basis_odds: dict[tuple[str, int], float],
) -> float:
    return 1.0 / lookup_win_basis_odds(win_basis_odds, row)


def lookup_place_basis_odds(
    place_basis_odds: dict[tuple[str, int], float],
    row: PredictionRow,
) -> float:
    key = (row.race_key, row.horse_number)
    if key not in place_basis_odds:
        raise ValueError(f"place_basis_odds missing for race_key={row.race_key} horse={row.horse_number}")
    return place_basis_odds[key]


def lookup_win_basis_odds(
    win_basis_odds: dict[tuple[str, int], float],
    row: PredictionRow,
) -> float:
    key = (row.race_key, row.horse_number)
    if key not in win_basis_odds:
        raise ValueError(f"win_basis_odds missing for race_key={row.race_key} horse={row.horse_number}")
    return win_basis_odds[key]


def score_pair(
    score_method: str,
    first_probability: float,
    second_probability: float,
    geometric_mean_place_basis_odds: float,
    abs_prob_gap: float,
    ow_wide_odds: float | None = None,
    pair_score_inputs: PairScoreInputs | None = None,
    partner_weight: float = PARTNER_SIGNAL_WEIGHT,
) -> float:
    if score_method == "product":
        return first_probability * second_probability
    if score_method == "min_prob":
        return min(first_probability, second_probability)
    if score_method == "sum_logit":
        return logit(first_probability) + logit(second_probability)
    if score_method == "product_times_geom_place_basis":
        return (first_probability * second_probability) * geometric_mean_place_basis_odds
    if score_method == "pair_model_score":
        return (first_probability * second_probability) * geometric_mean_place_basis_odds
    if score_method == "min_prob_times_geom_place_basis":
        return min(first_probability, second_probability) * geometric_mean_place_basis_odds
    if score_method == "product_minus_prob_gap_penalty":
        return (first_probability * second_probability) - (
            PROB_GAP_PENALTY_WEIGHT * abs_prob_gap
        )
    if score_method == "ow_wide_implied_prob":
        if ow_wide_odds is None or ow_wide_odds <= 0.0:
            raise ValueError("ow_wide_odds must be positive for ow_wide_implied_prob")
        return 1.0 / ow_wide_odds
    if score_method == "ow_wide_market_prob":
        if ow_wide_odds is None or ow_wide_odds <= 0.0:
            raise ValueError("ow_wide_odds must be positive for ow_wide_market_prob")
        return 1.0 / ow_wide_odds
    if score_method == "pair_edge":
        if ow_wide_odds is None or ow_wide_odds <= 0.0:
            raise ValueError("ow_wide_odds must be positive for pair_edge")
        return (
            (first_probability * second_probability) * geometric_mean_place_basis_odds
        ) - (1.0 / ow_wide_odds)
    if score_method == "no_ow_guard":
        return (first_probability * second_probability) * geometric_mean_place_basis_odds
    if score_method == "low_wide_payout_guard":
        return (first_probability * second_probability) * geometric_mean_place_basis_odds
    if score_method == "extreme_ow_implied_prob_guard":
        return (first_probability * second_probability) * geometric_mean_place_basis_odds
    if score_method == "anchor_win_pred_times_partner_place_pred":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return inputs.anchor_win_pred * inputs.partner_place_pred
    if score_method == "anchor_win_edge_times_partner_place_edge":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return inputs.anchor_win_edge * inputs.partner_place_edge
    if score_method == "anchor_win_pred_times_partner_pred_times_place":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return inputs.anchor_win_pred * inputs.partner_pred_times_place
    if score_method == "anchor_plus_weighted_partner":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return inputs.anchor_signal + (partner_weight * inputs.partner_signal)
    if score_method == "anchor_times_partner":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return inputs.anchor_signal * inputs.partner_signal
    if score_method == "min_anchor_partner_times_payout_tilt":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return min(inputs.anchor_signal, inputs.partner_signal) * geometric_mean_place_basis_odds
    if score_method == "geometric_mean_anchor_partner_times_payout_tilt":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return (
            signed_geometric_mean(inputs.anchor_signal, inputs.partner_signal)
            * geometric_mean_place_basis_odds
        )
    if score_method == "anchor_plus_partner_edge_like":
        inputs = require_pair_score_inputs(score_method, pair_score_inputs)
        return inputs.anchor_signal + (inputs.partner_signal * geometric_mean_place_basis_odds)
    raise ValueError(f"unsupported score_method: {score_method}")


def require_pair_score_inputs(
    score_method: str,
    pair_score_inputs: PairScoreInputs | None,
) -> PairScoreInputs:
    if pair_score_inputs is None:
        raise ValueError(f"{score_method} requires asymmetric pair score inputs")
    return pair_score_inputs


def build_pair_score_inputs(
    *,
    anchor_row: PredictionRow,
    partner_row: PredictionRow,
    anchor_rule: str,
    partner_rule: str,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
) -> PairScoreInputs:
    anchor_win_pred = anchor_row.pred_probability
    anchor_win_edge = anchor_row.pred_probability - horse_win_market_prob(anchor_row, win_basis_odds)
    partner_place_pred = partner_row.partner_probability
    partner_place_edge = horse_place_edge_score(partner_row, place_basis_odds)
    partner_pred_times_place = horse_pred_times_place_basis_score(partner_row, place_basis_odds)
    partner_reference_mix = horse_reference_partner_score(partner_row, place_basis_odds)
    return PairScoreInputs(
        anchor_signal=resolve_anchor_signal(anchor_rule, anchor_row, win_basis_odds, place_basis_odds),
        partner_signal=resolve_partner_signal(
            partner_rule,
            partner_place_pred=partner_place_pred,
            partner_place_edge=partner_place_edge,
            partner_pred_times_place=partner_pred_times_place,
            partner_reference_mix=partner_reference_mix,
        ),
        anchor_win_pred=anchor_win_pred,
        anchor_win_edge=anchor_win_edge,
        partner_place_pred=partner_place_pred,
        partner_place_edge=partner_place_edge,
        partner_pred_times_place=partner_pred_times_place,
        partner_reference_mix=partner_reference_mix,
    )


def resolve_anchor_signal(
    anchor_rule: str,
    row: PredictionRow,
    win_basis_odds: dict[tuple[str, int], float],
    place_basis_odds: dict[tuple[str, int], float],
) -> float:
    if anchor_rule == "model_anchor":
        return horse_model_anchor_score(row, place_basis_odds)
    if anchor_rule == "win_pred":
        return row.pred_probability
    if anchor_rule == "win_edge":
        return row.pred_probability - horse_win_market_prob(row, win_basis_odds)
    raise ValueError(f"unsupported anchor_rule: {anchor_rule}")


def resolve_partner_signal(
    partner_rule: str,
    *,
    partner_place_pred: float,
    partner_place_edge: float,
    partner_pred_times_place: float,
    partner_reference_mix: float,
) -> float:
    if partner_rule == "place_edge":
        return partner_place_edge
    if partner_rule == "pred_times_place_basis":
        return partner_pred_times_place
    if partner_rule == "reference_mix":
        return partner_reference_mix
    raise ValueError(f"unsupported partner_rule: {partner_rule}")


def signed_geometric_mean(left: float, right: float) -> float:
    product = left * right
    if product == 0.0:
        return 0.0
    return math.copysign(math.sqrt(abs(product)), product)


def passes_ow_guard(score_method: str, ow_wide_odds: float | None) -> bool:
    if score_method in {
        "product",
        "min_prob",
        "sum_logit",
        "product_times_geom_place_basis",
        "min_prob_times_geom_place_basis",
        "product_minus_prob_gap_penalty",
        "pair_model_score",
        "no_ow_guard",
        "anchor_win_pred_times_partner_place_pred",
        "anchor_win_edge_times_partner_place_edge",
        "anchor_win_pred_times_partner_pred_times_place",
        "anchor_plus_weighted_partner",
        "anchor_times_partner",
        "min_anchor_partner_times_payout_tilt",
        "geometric_mean_anchor_partner_times_payout_tilt",
        "anchor_plus_partner_edge_like",
    }:
        return True
    if score_method in {"ow_wide_implied_prob", "ow_wide_market_prob", "pair_edge"}:
        return ow_wide_odds is not None and ow_wide_odds > 0.0
    if score_method == "low_wide_payout_guard":
        return ow_wide_odds is not None and ow_wide_odds >= LOW_WIDE_PAYOUT_GUARD_MIN_ODDS
    if score_method == "extreme_ow_implied_prob_guard":
        return ow_wide_odds is not None and (1.0 / ow_wide_odds) <= EXTREME_OW_IMPLIED_PROB_GUARD_MAX
    raise ValueError(f"unsupported score_method: {score_method}")


def logit(value: float) -> float:
    clipped = min(max(value, 1e-6), 1.0 - 1e-6)
    return math.log(clipped / (1.0 - clipped))


def normalize_pair_key(first_horse_number: int, second_horse_number: int) -> str:
    lower, upper = sorted((first_horse_number, second_horse_number))
    return f"{lower:02d}{upper:02d}"


def build_wide_research_summaries(
    selected_rows: list[SelectedPairRow],
    stake_per_pair: float,
) -> list[WideResearchSummary]:
    grouped_rows: dict[tuple[str, str, str, str, float | None, int, int], list[SelectedPairRow]] = {}
    for row in selected_rows:
        grouped_rows.setdefault(
            (
                row.window_label,
                row.split,
                row.score_method,
                row.pair_generation_method,
                row.partner_weight,
                row.candidate_top_k,
                row.adopted_pair_count,
            ),
            [],
        ).append(row)

    return [
        build_wide_research_summary(
            window_label=window_label,
            split=split,
            score_method=score_method,
            pair_generation_method=pair_generation_method,
            partner_weight=partner_weight,
            candidate_top_k=candidate_top_k,
            adopted_pair_count=adopted_pair_count,
            rows=rows,
            stake_per_pair=stake_per_pair,
        )
        for (
            window_label,
            split,
            score_method,
            pair_generation_method,
            partner_weight,
            candidate_top_k,
            adopted_pair_count,
        ), rows in sorted(grouped_rows.items())
    ]


def build_wide_research_summary(
    *,
    window_label: str,
    split: str,
    score_method: str,
    pair_generation_method: str,
    partner_weight: float | None,
    candidate_top_k: int,
    adopted_pair_count: int,
    rows: list[SelectedPairRow],
    stake_per_pair: float,
) -> WideResearchSummary:
    pair_count = len(rows)
    hit_count = sum(1 for row in rows if row.is_hit)
    total_return = sum(row.wide_payout or 0.0 for row in rows)
    total_stake = pair_count * stake_per_pair
    return WideResearchSummary(
        window_label=window_label,
        split=split,
        score_method=score_method,
        pair_generation_method=pair_generation_method,
        partner_weight=partner_weight,
        candidate_top_k=candidate_top_k,
        adopted_pair_count=adopted_pair_count,
        pair_count=pair_count,
        hit_count=hit_count,
        hit_rate=(hit_count / pair_count) if pair_count > 0 else 0.0,
        roi=(total_return / total_stake) if total_stake > 0.0 else 0.0,
        total_profit=total_return - total_stake,
    )


def build_wide_research_comparisons(
    summaries: list[WideResearchSummary],
    *,
    stake_per_pair: float,
    bootstrap_iterations: int,
    random_seed: int,
) -> list[WideResearchComparisonRow]:
    grouped: dict[tuple[str, str, float | None, int, int], list[WideResearchSummary]] = {}
    for summary in summaries:
        if summary.split != OOS_SPLIT:
            continue
        grouped.setdefault(
            (
                summary.score_method,
                summary.pair_generation_method,
                summary.partner_weight,
                summary.candidate_top_k,
                summary.adopted_pair_count,
            ),
            [],
        ).append(summary)

    if not grouped:
        return []

    window_winners = build_window_winner_counts(grouped)
    return [
        build_comparison_row(
            score_method=score_method,
            pair_generation_method=pair_generation_method,
            partner_weight=partner_weight,
            candidate_top_k=candidate_top_k,
            adopted_pair_count=adopted_pair_count,
            summaries=group_summaries,
            window_win_count=window_winners.get(
                (
                    score_method,
                    pair_generation_method,
                    partner_weight,
                    candidate_top_k,
                    adopted_pair_count,
                ),
                0,
            ),
            stake_per_pair=stake_per_pair,
            bootstrap_iterations=bootstrap_iterations,
            random_seed=random_seed,
        )
        for (
            score_method,
            pair_generation_method,
            partner_weight,
            candidate_top_k,
            adopted_pair_count,
        ), group_summaries in sorted(grouped.items())
    ]


def build_wide_research_best_settings(
    comparisons: list[WideResearchComparisonRow],
) -> list[WideResearchBestSetting]:
    if not comparisons:
        return []

    return [
        select_best_setting("total_oos_roi_max", comparisons),
        select_best_setting("window_win_count_max", comparisons),
        select_best_setting("mean_roi_minus_std", comparisons),
    ]

    return [
        select_best_setting("total_oos_roi_max", candidate_rows),
        select_best_setting("window_win_count_max", candidate_rows),
        select_best_setting("mean_roi_minus_std", candidate_rows),
    ]


def build_window_winner_counts(
    grouped: dict[tuple[str, str, float | None, int, int], list[WideResearchSummary]],
) -> dict[tuple[str, str, float | None, int, int], int]:
    window_groups: dict[str, list[WideResearchSummary]] = {}
    for summaries in grouped.values():
        for summary in summaries:
            window_groups.setdefault(summary.window_label, []).append(summary)

    counts: dict[tuple[str, str, int, int], int] = {}
    for window_label, summaries in sorted(window_groups.items()):
        winner = max(
            summaries,
            key=lambda summary: (
                summary.roi,
                summary.total_profit,
                summary.hit_rate,
                -(summary.partner_weight if summary.partner_weight is not None else -1.0),
                -summary.candidate_top_k,
                -summary.adopted_pair_count,
                summary.pair_generation_method,
                summary.score_method,
            ),
        )
        key = (
            winner.score_method,
            winner.pair_generation_method,
            winner.partner_weight,
            winner.candidate_top_k,
            winner.adopted_pair_count,
        )
        counts[key] = counts.get(key, 0) + 1
    return counts


def build_comparison_row(
    *,
    score_method: str,
    pair_generation_method: str,
    partner_weight: float | None,
    candidate_top_k: int,
    adopted_pair_count: int,
    summaries: list[WideResearchSummary],
    window_win_count: int,
    stake_per_pair: float,
    bootstrap_iterations: int,
    random_seed: int,
) -> WideResearchComparisonRow:
    pair_count = sum(summary.pair_count for summary in summaries)
    hit_count = sum(summary.hit_count for summary in summaries)
    total_profit = sum(summary.total_profit for summary in summaries)
    total_stake = pair_count * stake_per_pair
    total_return = total_profit + total_stake
    roi_values = [summary.roi for summary in summaries]
    mean_roi = sum(roi_values) / len(roi_values)
    roi_std = math.sqrt(
        sum((roi - mean_roi) ** 2 for roi in roi_values) / len(roi_values),
    )
    roi_ci_lower, roi_ci_upper, profit_ci_lower, profit_ci_upper, roi_gt_1_ratio = bootstrap_intervals(
        summaries=summaries,
        stake_per_pair=stake_per_pair,
        iterations=bootstrap_iterations,
        random_seed=random_seed
        + stable_string_seed(
            f"{score_method}:{pair_generation_method}:{partner_weight}:{candidate_top_k}:{adopted_pair_count}",
        ),
    )
    return WideResearchComparisonRow(
        split=OOS_SPLIT,
        score_role=describe_score_role(score_method),
        score_method=score_method,
        pair_generation_method=pair_generation_method,
        partner_weight=partner_weight,
        candidate_top_k=candidate_top_k,
        adopted_pair_count=adopted_pair_count,
        pair_count=pair_count,
        hit_count=hit_count,
        hit_rate=(hit_count / pair_count) if pair_count > 0 else 0.0,
        roi=(total_return / total_stake) if total_stake > 0.0 else 0.0,
        total_profit=total_profit,
        window_win_count=window_win_count,
        mean_roi=mean_roi,
        roi_std=roi_std,
        roi_ci_lower=roi_ci_lower,
        roi_ci_upper=roi_ci_upper,
        profit_ci_lower=profit_ci_lower,
        profit_ci_upper=profit_ci_upper,
        roi_gt_1_ratio=roi_gt_1_ratio,
    )


def select_best_setting(
    selection_rule: str,
    candidates: list[WideResearchComparisonRow],
) -> WideResearchBestSetting:
    if selection_rule not in SUPPORTED_SELECTION_RULES:
        raise ValueError(f"unsupported selection_rule: {selection_rule}")
    if selection_rule == "total_oos_roi_max":
        best_candidate = max(
            candidates,
            key=lambda candidate: (
                candidate.roi,
                candidate.total_profit,
                candidate.window_win_count,
                candidate.mean_roi - candidate.roi_std,
                -(candidate.partner_weight if candidate.partner_weight is not None else -1.0),
                -candidate.candidate_top_k,
                -candidate.adopted_pair_count,
                candidate.pair_generation_method,
                candidate.score_method,
            ),
        )
    elif selection_rule == "window_win_count_max":
        best_candidate = max(
            candidates,
            key=lambda candidate: (
                candidate.window_win_count,
                candidate.mean_roi,
                -candidate.roi_std,
                candidate.roi,
                candidate.total_profit,
                -(candidate.partner_weight if candidate.partner_weight is not None else -1.0),
                -candidate.candidate_top_k,
                -candidate.adopted_pair_count,
                candidate.pair_generation_method,
                candidate.score_method,
            ),
        )
    else:
        best_candidate = max(
            candidates,
            key=lambda candidate: (
                candidate.mean_roi - candidate.roi_std,
                candidate.mean_roi,
                candidate.window_win_count,
                candidate.roi,
                candidate.total_profit,
                -(candidate.partner_weight if candidate.partner_weight is not None else -1.0),
                -candidate.candidate_top_k,
                -candidate.adopted_pair_count,
                candidate.pair_generation_method,
                candidate.score_method,
            ),
        )
    return WideResearchBestSetting(
        selection_rule=selection_rule,
        split=best_candidate.split,
        score_role=best_candidate.score_role,
        score_method=best_candidate.score_method,
        pair_generation_method=best_candidate.pair_generation_method,
        partner_weight=best_candidate.partner_weight,
        candidate_top_k=best_candidate.candidate_top_k,
        adopted_pair_count=best_candidate.adopted_pair_count,
        pair_count=best_candidate.pair_count,
        hit_count=best_candidate.hit_count,
        hit_rate=best_candidate.hit_rate,
        roi=best_candidate.roi,
        total_profit=best_candidate.total_profit,
        window_win_count=best_candidate.window_win_count,
        mean_roi=best_candidate.mean_roi,
        roi_std=best_candidate.roi_std,
        roi_ci_lower=best_candidate.roi_ci_lower,
        roi_ci_upper=best_candidate.roi_ci_upper,
        profit_ci_lower=best_candidate.profit_ci_lower,
        profit_ci_upper=best_candidate.profit_ci_upper,
        roi_gt_1_ratio=best_candidate.roi_gt_1_ratio,
    )


def bootstrap_intervals(
    *,
    summaries: list[WideResearchSummary],
    stake_per_pair: float,
    iterations: int,
    random_seed: int,
) -> tuple[float, float, float, float, float]:
    if iterations <= 0:
        raise ValueError("bootstrap_iterations must be positive")
    rng = random.Random(random_seed)
    roi_samples: list[float] = []
    profit_samples: list[float] = []
    summary_count = len(summaries)
    for _ in range(iterations):
        sampled = [summaries[rng.randrange(summary_count)] for _ in range(summary_count)]
        pair_count = sum(summary.pair_count for summary in sampled)
        total_profit = sum(summary.total_profit for summary in sampled)
        total_stake = pair_count * stake_per_pair
        total_return = total_profit + total_stake
        roi_samples.append((total_return / total_stake) if total_stake > 0.0 else 0.0)
        profit_samples.append(total_profit)
    roi_samples.sort()
    profit_samples.sort()
    return (
        percentile(roi_samples, 0.025),
        percentile(roi_samples, 0.975),
        percentile(profit_samples, 0.025),
        percentile(profit_samples, 0.975),
        sum(1 for value in roi_samples if value > 1.0) / len(roi_samples),
    )


def percentile(values: list[float], q: float) -> float:
    if not values:
        raise ValueError("values must not be empty")
    index = int(round((len(values) - 1) * q))
    return values[index]


def stable_string_seed(value: str) -> int:
    return sum(ord(char) for char in value)


def describe_score_role(score_method: str) -> str:
    if score_method in {
        "product",
        "min_prob",
        "sum_logit",
        "product_times_geom_place_basis",
        "min_prob_times_geom_place_basis",
        "product_minus_prob_gap_penalty",
        "pair_model_score",
        "no_ow_guard",
        "low_wide_payout_guard",
        "extreme_ow_implied_prob_guard",
    }:
        return "model_primary"
    if score_method in {
        "anchor_win_pred_times_partner_place_pred",
        "anchor_win_edge_times_partner_place_edge",
        "anchor_win_pred_times_partner_pred_times_place",
        "anchor_plus_weighted_partner",
        "anchor_times_partner",
        "min_anchor_partner_times_payout_tilt",
        "geometric_mean_anchor_partner_times_payout_tilt",
        "anchor_plus_partner_edge_like",
    }:
        return "role_aware_pair_score"
    if score_method in {"ow_wide_implied_prob", "ow_wide_market_prob"}:
        return "market_baseline"
    if score_method == "pair_edge":
        return "model_minus_market_edge"
    raise ValueError(f"unsupported score_method: {score_method}")


def write_wide_research_summary_csv(
    path: Path,
    summaries: list[WideResearchSummary],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=WIDE_RESEARCH_SUMMARY_COLUMNS)
        writer.writeheader()
        for summary in summaries:
            writer.writerow(asdict(summary))


def write_wide_research_summary_json(
    path: Path,
    config: WideResearchBacktestConfig,
    summaries: list[WideResearchSummary],
    comparisons: list[WideResearchComparisonRow],
    best_settings: list[WideResearchBestSetting],
) -> None:
    payload = {
        "backtest": {
            "config_section": config.config_section,
            "name": config.name,
            "predictions_path": str(config.predictions_path),
            "partner_predictions_path": (
                str(config.partner_predictions_path)
                if config.partner_predictions_path is not None
                else None
            ),
            "hjc_raw_dir": str(config.hjc_raw_dir),
            "output_dir": str(config.output_dir),
            "score_methods": list(config.score_methods),
            "pair_generation_methods": list(config.pair_generation_methods),
            "candidate_top_k_values": list(config.candidate_top_k_values),
            "adopted_pair_count_values": list(config.adopted_pair_count_values),
            "partner_weight_values": list(config.partner_weight_values),
            "partner_probability_column": config.partner_probability_column,
            "stake_per_pair": config.stake_per_pair,
            "bootstrap_iterations": config.bootstrap_iterations,
            "random_seed": config.random_seed,
        },
        "summaries": [asdict(summary) for summary in summaries],
        "comparisons": [asdict(comparison) for comparison in comparisons],
        "best_settings": [asdict(best_setting) for best_setting in best_settings],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_wide_research_comparison_csv(
    path: Path,
    comparisons: list[WideResearchComparisonRow],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=WIDE_RESEARCH_COMPARISON_COLUMNS)
        writer.writeheader()
        for comparison in comparisons:
            writer.writerow(asdict(comparison))


def write_wide_research_comparison_json(
    path: Path,
    comparisons: list[WideResearchComparisonRow],
) -> None:
    path.write_text(
        json.dumps([asdict(comparison) for comparison in comparisons], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_wide_research_best_settings_csv(
    path: Path,
    best_settings: list[WideResearchBestSetting],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=WIDE_RESEARCH_BEST_SETTING_COLUMNS)
        writer.writeheader()
        for best_setting in best_settings:
            writer.writerow(asdict(best_setting))


def write_wide_research_best_settings_json(
    path: Path,
    best_settings: list[WideResearchBestSetting],
) -> None:
    path.write_text(
        json.dumps([asdict(best_setting) for best_setting in best_settings], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_wide_research_selected_pairs_csv(
    path: Path,
    selected_rows: list[SelectedPairRow],
) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=WIDE_RESEARCH_SELECTED_PAIR_COLUMNS)
        writer.writeheader()
        for row in selected_rows:
            writer.writerow(asdict(row))


def write_wide_research_selected_pairs_json(
    path: Path,
    selected_rows: list[SelectedPairRow],
) -> None:
    path.write_text(
        json.dumps([asdict(row) for row in selected_rows], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
