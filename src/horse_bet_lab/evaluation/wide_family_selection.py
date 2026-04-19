from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from horse_bet_lab.config import WideFamilySelectionConfig
from horse_bet_lab.evaluation.reference_strategy import write_csv, write_json

SUPPORTED_SELECTION_RULES = (
    "total_valid_roi_max",
    "window_win_count_max",
    "mean_valid_roi_minus_std",
)


@dataclass(frozen=True)
class FamilyWindowSummary:
    window_label: str
    split: str
    score_method: str
    pair_generation_method: str
    partner_weight: float | None
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float


@dataclass(frozen=True)
class SelectedFamilyWindowRow:
    selection_rule: str
    test_window_label: str
    selected_family: str
    selected_score_method: str
    selected_pair_generation_method: str
    selected_partner_weight: float | None
    valid_window_count: int
    total_valid_roi: float
    valid_window_win_count: int
    mean_valid_roi: float
    valid_roi_std: float
    test_pair_count: int
    test_hit_count: int
    test_hit_rate: float
    test_roi: float
    test_total_profit: float
    test_window_win: bool


@dataclass(frozen=True)
class SelectedFamilySummaryRow:
    selection_rule: str
    pair_count: int
    hit_count: int
    hit_rate: float
    roi: float
    total_profit: float
    window_win_count: int
    mean_roi: float
    roi_std: float
    selected_v3_window_count: int
    selected_v6_window_count: int


@dataclass(frozen=True)
class WideFamilySelectionResult:
    output_dir: Path
    selected_family_summaries: tuple[SelectedFamilySummaryRow, ...]
    selected_family_windows: tuple[SelectedFamilyWindowRow, ...]


def run_wide_family_selection(config: WideFamilySelectionConfig) -> WideFamilySelectionResult:
    validate_config(config)
    family_specs = (
        (
            config.v3_label,
            config.v3_summary_path,
            config.v3_score_method,
            config.v3_pair_generation_method,
            config.v3_partner_weight,
        ),
        (
            config.v6_label,
            config.v6_summary_path,
            config.v6_score_method,
            config.v6_pair_generation_method,
            config.v6_partner_weight,
        ),
    )
    family_summaries = {
        label: load_family_summaries(
            path=summary_path,
            score_method=score_method,
            pair_generation_method=pair_generation_method,
            partner_weight=partner_weight,
        )
        for label, summary_path, score_method, pair_generation_method, partner_weight in family_specs
    }
    window_labels = sorted(
        set(family_summaries[config.v3_label][config.valid_split])
        & set(family_summaries[config.v3_label][config.split])
        & set(family_summaries[config.v6_label][config.valid_split])
        & set(family_summaries[config.v6_label][config.split]),
    )
    selected_windows: list[SelectedFamilyWindowRow] = []
    selected_summaries: list[SelectedFamilySummaryRow] = []
    for selection_rule in config.selection_rules:
        chosen_rows = [
            choose_family_for_window(
                config=config,
                family_summaries=family_summaries,
                window_labels=window_labels,
                selection_rule=selection_rule,
                current_index=index,
            )
            for index in range(len(window_labels))
        ]
        selected_windows.extend(chosen_rows)
        selected_summaries.append(
            build_selected_family_summary(
                selection_rule,
                chosen_rows,
                stake_per_pair=config.stake_per_pair,
                v3_label=config.v3_label,
                v6_label=config.v6_label,
            ),
        )

    config.output_dir.mkdir(parents=True, exist_ok=True)
    write_csv(config.output_dir / "selected_family_summary.csv", tuple(selected_summaries))
    write_json(
        config.output_dir / "selected_family_summary.json",
        {
            "analysis": {
                "name": config.name,
                "split": config.split,
                "valid_split": config.valid_split,
                "selection_rules": list(config.selection_rules),
                "v3_label": config.v3_label,
                "v6_label": config.v6_label,
                "rows": selected_summaries,
                "window_rows": chosen_window_rows_by_rule(selected_windows),
            },
        },
    )
    write_csv(config.output_dir / "selected_family_windows.csv", tuple(selected_windows))
    write_json(
        config.output_dir / "selected_family_windows.json",
        {"analysis": {"rows": selected_windows}},
    )
    return WideFamilySelectionResult(
        output_dir=config.output_dir,
        selected_family_summaries=tuple(selected_summaries),
        selected_family_windows=tuple(selected_windows),
    )


def validate_config(config: WideFamilySelectionConfig) -> None:
    unsupported_rules = sorted(set(config.selection_rules) - set(SUPPORTED_SELECTION_RULES))
    if unsupported_rules:
        raise ValueError(f"unsupported selection_rules: {unsupported_rules}")
    if not config.selection_rules:
        raise ValueError("selection_rules must not be empty")


def load_family_summaries(
    *,
    path: Path,
    score_method: str,
    pair_generation_method: str,
    partner_weight: float | None,
) -> dict[str, dict[str, FamilyWindowSummary]]:
    grouped: dict[str, dict[str, FamilyWindowSummary]] = {}
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row["score_method"] != score_method:
                continue
            row_pair_generation_method = row.get("pair_generation_method") or "symmetric_top_k_pairs"
            if row_pair_generation_method != pair_generation_method:
                continue
            row_partner_weight_raw = row.get("partner_weight")
            row_partner_weight = (
                float(row_partner_weight_raw)
                if row_partner_weight_raw not in (None, "", "None")
                else None
            )
            if row_partner_weight != partner_weight:
                continue
            summary = FamilyWindowSummary(
                window_label=row["window_label"],
                split=row["split"],
                score_method=row["score_method"],
                pair_generation_method=row_pair_generation_method,
                partner_weight=row_partner_weight,
                pair_count=int(row["pair_count"]),
                hit_count=int(row["hit_count"]),
                hit_rate=float(row["hit_rate"]),
                roi=float(row["roi"]),
                total_profit=float(row["total_profit"]),
            )
            grouped.setdefault(summary.split, {})[summary.window_label] = summary
    return grouped


def choose_family_for_window(
    *,
    config: WideFamilySelectionConfig,
    family_summaries: dict[str, dict[str, dict[str, FamilyWindowSummary]]],
    window_labels: list[str],
    selection_rule: str,
    current_index: int,
) -> SelectedFamilyWindowRow:
    history_labels = window_labels[: current_index + 1]
    candidates = []
    for family_label in (config.v3_label, config.v6_label):
        valid_rows = [
            family_summaries[family_label][config.valid_split][label]
            for label in history_labels
        ]
        test_row = family_summaries[family_label][config.split][window_labels[current_index]]
        candidates.append(
            (
                family_label,
                summarize_valid_history(
                    family_label=family_label,
                    valid_rows=valid_rows,
                    family_summaries=family_summaries,
                    config=config,
                    history_labels=history_labels,
                ),
                test_row,
            ),
        )
    selected_family, valid_summary, test_row = max(
        candidates,
        key=lambda item: selection_key(
            selection_rule=selection_rule,
            valid_summary=item[1],
            test_row=item[2],
        ),
    )
    best_test_key = max(window_summary_key(item[2]) for item in candidates)
    return SelectedFamilyWindowRow(
        selection_rule=selection_rule,
        test_window_label=test_row.window_label,
        selected_family=selected_family,
        selected_score_method=test_row.score_method,
        selected_pair_generation_method=test_row.pair_generation_method,
        selected_partner_weight=test_row.partner_weight,
        valid_window_count=valid_summary["valid_window_count"],
        total_valid_roi=valid_summary["total_valid_roi"],
        valid_window_win_count=valid_summary["valid_window_win_count"],
        mean_valid_roi=valid_summary["mean_valid_roi"],
        valid_roi_std=valid_summary["valid_roi_std"],
        test_pair_count=test_row.pair_count,
        test_hit_count=test_row.hit_count,
        test_hit_rate=test_row.hit_rate,
        test_roi=test_row.roi,
        test_total_profit=test_row.total_profit,
        test_window_win=(window_summary_key(test_row) == best_test_key),
    )


def summarize_valid_history(
    *,
    family_label: str,
    valid_rows: list[FamilyWindowSummary],
    family_summaries: dict[str, dict[str, dict[str, FamilyWindowSummary]]],
    config: WideFamilySelectionConfig,
    history_labels: list[str],
) -> dict[str, float | int]:
    pair_count = sum(row.pair_count for row in valid_rows)
    total_profit = sum(row.total_profit for row in valid_rows)
    total_stake = pair_count * config.stake_per_pair
    total_valid_roi = (total_stake + total_profit) / total_stake if total_stake > 0 else 0.0
    valid_rois = [row.roi for row in valid_rows]
    mean_valid_roi = sum(valid_rois) / len(valid_rois)
    valid_roi_std = (
        sum((roi - mean_valid_roi) ** 2 for roi in valid_rois) / len(valid_rois)
    ) ** 0.5
    valid_window_win_count = 0
    for label in history_labels:
        current = family_summaries[family_label][config.valid_split][label]
        peers = [
            family_summaries[other_label][config.valid_split][label]
            for other_label in (config.v3_label, config.v6_label)
        ]
        best_key = max(window_summary_key(row) for row in peers)
        if window_summary_key(current) == best_key:
            valid_window_win_count += 1
    return {
        "valid_window_count": len(valid_rows),
        "total_valid_roi": total_valid_roi,
        "valid_window_win_count": valid_window_win_count,
        "mean_valid_roi": mean_valid_roi,
        "valid_roi_std": valid_roi_std,
        "total_valid_profit": total_profit,
    }


def window_summary_key(row: FamilyWindowSummary) -> tuple[float, float, int, int]:
    return (row.roi, row.total_profit, row.hit_count, row.pair_count)


def selection_key(
    *,
    selection_rule: str,
    valid_summary: dict[str, float | int],
    test_row: FamilyWindowSummary,
) -> tuple[float, ...]:
    total_valid_roi = float(valid_summary["total_valid_roi"])
    valid_window_win_count = float(valid_summary["valid_window_win_count"])
    mean_valid_roi = float(valid_summary["mean_valid_roi"])
    valid_roi_std = float(valid_summary["valid_roi_std"])
    total_valid_profit = float(valid_summary["total_valid_profit"])
    if selection_rule == "total_valid_roi_max":
        return (
            total_valid_roi,
            total_valid_profit,
            valid_window_win_count,
            mean_valid_roi - valid_roi_std,
            test_row.roi,
        )
    if selection_rule == "window_win_count_max":
        return (
            valid_window_win_count,
            mean_valid_roi,
            -valid_roi_std,
            total_valid_roi,
            total_valid_profit,
        )
    if selection_rule == "mean_valid_roi_minus_std":
        return (
            mean_valid_roi - valid_roi_std,
            mean_valid_roi,
            valid_window_win_count,
            total_valid_roi,
            total_valid_profit,
        )
    raise ValueError(f"unsupported selection_rule: {selection_rule}")


def build_selected_family_summary(
    selection_rule: str,
    rows: list[SelectedFamilyWindowRow],
    *,
    stake_per_pair: float,
    v3_label: str,
    v6_label: str,
) -> SelectedFamilySummaryRow:
    pair_count = sum(row.test_pair_count for row in rows)
    hit_count = sum(row.test_hit_count for row in rows)
    total_profit = sum(row.test_total_profit for row in rows)
    total_stake = pair_count * stake_per_pair
    roi = (total_stake + total_profit) / total_stake if total_stake > 0 else 0.0
    window_rois = [row.test_roi for row in rows]
    mean_roi = sum(window_rois) / len(window_rois)
    roi_std = (sum((roi_value - mean_roi) ** 2 for roi_value in window_rois) / len(window_rois)) ** 0.5
    return SelectedFamilySummaryRow(
        selection_rule=selection_rule,
        pair_count=pair_count,
        hit_count=hit_count,
        hit_rate=(hit_count / pair_count) if pair_count > 0 else 0.0,
        roi=roi,
        total_profit=total_profit,
        window_win_count=sum(1 for row in rows if row.test_window_win),
        mean_roi=mean_roi,
        roi_std=roi_std,
        selected_v3_window_count=sum(1 for row in rows if row.selected_family == v3_label),
        selected_v6_window_count=sum(1 for row in rows if row.selected_family == v6_label),
    )


def chosen_window_rows_by_rule(
    rows: list[SelectedFamilyWindowRow],
) -> dict[str, list[SelectedFamilyWindowRow]]:
    grouped: dict[str, list[SelectedFamilyWindowRow]] = {}
    for row in rows:
        grouped.setdefault(row.selection_rule, []).append(row)
    return grouped
