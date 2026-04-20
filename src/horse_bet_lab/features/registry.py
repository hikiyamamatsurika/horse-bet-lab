from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Sequence


@dataclass(frozen=True)
class FeatureDefinition:
    canonical_name: str
    in_dataset: bool
    in_model_parity_path: bool
    numeric_or_not: bool
    source_class: str
    timing_class: str
    carrier_identity: str
    leakage_allowed: bool


@dataclass(frozen=True)
class DatasetFeatureSetDefinition:
    name: str
    dataset_columns: tuple[str, ...]
    model_parity_columns: tuple[str, ...] | None
    optional_dataset_columns: tuple[str, ...] = ()
    optional_model_parity_columns: tuple[str, ...] | None = None


@dataclass(frozen=True)
class FeatureMissingNullPolicy:
    canonical_name: str
    dataset_null_allowed: bool
    dataset_null_condition: str
    model_null_allowed: bool
    model_null_condition: str


FEATURE_REGISTRY: dict[str, FeatureDefinition] = {
    "distance_m": FeatureDefinition(
        canonical_name="distance_m",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="upstream_raw_structural",
        timing_class="pre_race",
        carrier_identity="jrdb_bac_staging",
        leakage_allowed=True,
    ),
    "race_name": FeatureDefinition(
        canonical_name="race_name",
        in_dataset=True,
        in_model_parity_path=False,
        numeric_or_not=False,
        source_class="upstream_raw_structural",
        timing_class="pre_race",
        carrier_identity="jrdb_bac_staging",
        leakage_allowed=True,
    ),
    "workout_weekday": FeatureDefinition(
        canonical_name="workout_weekday",
        in_dataset=True,
        in_model_parity_path=False,
        numeric_or_not=False,
        source_class="upstream_raw_workout",
        timing_class="pre_race",
        carrier_identity="jrdb_cha_staging",
        leakage_allowed=True,
    ),
    "workout_date": FeatureDefinition(
        canonical_name="workout_date",
        in_dataset=True,
        in_model_parity_path=False,
        numeric_or_not=False,
        source_class="upstream_raw_workout",
        timing_class="pre_race",
        carrier_identity="jrdb_cha_staging",
        leakage_allowed=True,
    ),
    "win_odds": FeatureDefinition(
        canonical_name="win_odds",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="upstream_raw_market",
        timing_class="pre_race",
        carrier_identity="win_market_snapshot_v1",
        leakage_allowed=True,
    ),
    "popularity": FeatureDefinition(
        canonical_name="popularity",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="upstream_raw_market",
        timing_class="pre_race_semantic_post_race_carrier",
        carrier_identity="legacy_sed_only_non_mainline",
        leakage_allowed=True,
    ),
    "place_basis_odds": FeatureDefinition(
        canonical_name="place_basis_odds",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="upstream_raw_market",
        timing_class="pre_race",
        carrier_identity="jrdb_oz_staging",
        leakage_allowed=True,
    ),
    "headcount": FeatureDefinition(
        canonical_name="headcount",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="upstream_raw_market_support",
        timing_class="pre_race",
        carrier_identity="jrdb_oz_staging",
        leakage_allowed=True,
    ),
    "workout_gap_days": FeatureDefinition(
        canonical_name="workout_gap_days",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_workout",
        timing_class="pre_race",
        carrier_identity="derived_from_bac_cha",
        leakage_allowed=True,
    ),
    "workout_weekday_code": FeatureDefinition(
        canonical_name="workout_weekday_code",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_workout",
        timing_class="pre_race",
        carrier_identity="derived_from_bac_cha",
        leakage_allowed=True,
    ),
    "place_slot_count": FeatureDefinition(
        canonical_name="place_slot_count",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_market",
        timing_class="pre_race",
        carrier_identity="derived_from_oz",
        leakage_allowed=True,
    ),
    "log_place_minus_log_win": FeatureDefinition(
        canonical_name="log_place_minus_log_win",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_market",
        timing_class="pre_race",
        carrier_identity="derived_from_win_market_snapshot_v1_and_oz",
        leakage_allowed=True,
    ),
    "implied_place_prob": FeatureDefinition(
        canonical_name="implied_place_prob",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_market",
        timing_class="pre_race",
        carrier_identity="derived_from_oz",
        leakage_allowed=True,
    ),
    "implied_win_prob": FeatureDefinition(
        canonical_name="implied_win_prob",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_market",
        timing_class="pre_race",
        carrier_identity="derived_from_win_market_snapshot_v1",
        leakage_allowed=True,
    ),
    "implied_place_prob_minus_implied_win_prob": FeatureDefinition(
        canonical_name="implied_place_prob_minus_implied_win_prob",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_market",
        timing_class="pre_race",
        carrier_identity="derived_from_win_market_snapshot_v1_and_oz",
        leakage_allowed=True,
    ),
    "place_to_win_ratio": FeatureDefinition(
        canonical_name="place_to_win_ratio",
        in_dataset=True,
        in_model_parity_path=True,
        numeric_or_not=True,
        source_class="project_derived_market",
        timing_class="pre_race",
        carrier_identity="derived_from_win_market_snapshot_v1_and_oz",
        leakage_allowed=True,
    ),
    "finish_position": FeatureDefinition(
        canonical_name="finish_position",
        in_dataset=False,
        in_model_parity_path=False,
        numeric_or_not=True,
        source_class="result_side",
        timing_class="post_race",
        carrier_identity="jrdb_sed_staging",
        leakage_allowed=False,
    ),
    "result_date": FeatureDefinition(
        canonical_name="result_date",
        in_dataset=False,
        in_model_parity_path=False,
        numeric_or_not=False,
        source_class="result_side",
        timing_class="post_race",
        carrier_identity="jrdb_sed_staging",
        leakage_allowed=False,
    ),
    "win_payout": FeatureDefinition(
        canonical_name="win_payout",
        in_dataset=False,
        in_model_parity_path=False,
        numeric_or_not=True,
        source_class="result_side",
        timing_class="post_race",
        carrier_identity="jrdb_hjc_staging",
        leakage_allowed=False,
    ),
    "place_payout_1": FeatureDefinition(
        canonical_name="place_payout_1",
        in_dataset=False,
        in_model_parity_path=False,
        numeric_or_not=True,
        source_class="result_side",
        timing_class="post_race",
        carrier_identity="jrdb_hjc_staging",
        leakage_allowed=False,
    ),
    "place_payout_2": FeatureDefinition(
        canonical_name="place_payout_2",
        in_dataset=False,
        in_model_parity_path=False,
        numeric_or_not=True,
        source_class="result_side",
        timing_class="post_race",
        carrier_identity="jrdb_hjc_staging",
        leakage_allowed=False,
    ),
    "place_payout_3": FeatureDefinition(
        canonical_name="place_payout_3",
        in_dataset=False,
        in_model_parity_path=False,
        numeric_or_not=True,
        source_class="result_side",
        timing_class="post_race",
        carrier_identity="jrdb_hjc_staging",
        leakage_allowed=False,
    ),
}


DATASET_FEATURE_SET_REGISTRY: dict[str, DatasetFeatureSetDefinition] = {
    "minimal": DatasetFeatureSetDefinition(
        name="minimal",
        dataset_columns=("distance_m", "race_name", "workout_weekday", "workout_date"),
        model_parity_columns=None,
    ),
    "odds_only": DatasetFeatureSetDefinition(
        name="odds_only",
        dataset_columns=("win_odds",),
        model_parity_columns=("win_odds",),
        optional_dataset_columns=("popularity",),
        optional_model_parity_columns=("win_odds", "popularity"),
    ),
    "win_market_only": DatasetFeatureSetDefinition(
        name="win_market_only",
        dataset_columns=("win_odds", "popularity"),
        model_parity_columns=("win_odds", "popularity"),
    ),
    "current_win_market": DatasetFeatureSetDefinition(
        name="current_win_market",
        dataset_columns=("win_odds", "popularity"),
        model_parity_columns=("win_odds", "popularity"),
    ),
    "place_market_only": DatasetFeatureSetDefinition(
        name="place_market_only",
        dataset_columns=("place_basis_odds",),
        model_parity_columns=("place_basis_odds",),
    ),
    "place_market_plus_popularity": DatasetFeatureSetDefinition(
        name="place_market_plus_popularity",
        dataset_columns=("place_basis_odds", "popularity"),
        model_parity_columns=("place_basis_odds", "popularity"),
    ),
    "dual_market": DatasetFeatureSetDefinition(
        name="dual_market",
        dataset_columns=("win_odds", "place_basis_odds", "popularity"),
        model_parity_columns=("win_odds", "place_basis_odds", "popularity"),
    ),
    "dual_market_for_win": DatasetFeatureSetDefinition(
        name="dual_market_for_win",
        dataset_columns=("win_odds", "place_basis_odds", "popularity"),
        model_parity_columns=("win_odds", "place_basis_odds", "popularity"),
    ),
    "dual_market_plus_headcount": DatasetFeatureSetDefinition(
        name="dual_market_plus_headcount",
        dataset_columns=("win_odds", "place_basis_odds", "popularity", "headcount"),
        model_parity_columns=("win_odds", "place_basis_odds", "popularity", "headcount"),
    ),
    "dual_market_plus_headcount_place_slots": DatasetFeatureSetDefinition(
        name="dual_market_plus_headcount_place_slots",
        dataset_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "headcount",
            "place_slot_count",
        ),
        model_parity_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "headcount",
            "place_slot_count",
        ),
    ),
    "dual_market_plus_headcount_place_slots_distance": DatasetFeatureSetDefinition(
        name="dual_market_plus_headcount_place_slots_distance",
        dataset_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "headcount",
            "place_slot_count",
            "distance_m",
        ),
        model_parity_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "headcount",
            "place_slot_count",
            "distance_m",
        ),
    ),
    "dual_market_plus_log_diff": DatasetFeatureSetDefinition(
        name="dual_market_plus_log_diff",
        dataset_columns=("win_odds", "place_basis_odds", "popularity", "log_place_minus_log_win"),
        model_parity_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "log_place_minus_log_win",
        ),
    ),
    "dual_market_plus_implied_probs": DatasetFeatureSetDefinition(
        name="dual_market_plus_implied_probs",
        dataset_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "implied_place_prob",
            "implied_win_prob",
        ),
        model_parity_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "implied_place_prob",
            "implied_win_prob",
        ),
    ),
    "dual_market_plus_prob_diff": DatasetFeatureSetDefinition(
        name="dual_market_plus_prob_diff",
        dataset_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "implied_place_prob_minus_implied_win_prob",
        ),
        model_parity_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "implied_place_prob_minus_implied_win_prob",
        ),
    ),
    "dual_market_plus_ratio": DatasetFeatureSetDefinition(
        name="dual_market_plus_ratio",
        dataset_columns=("win_odds", "place_basis_odds", "popularity", "place_to_win_ratio"),
        model_parity_columns=(
            "win_odds",
            "place_basis_odds",
            "popularity",
            "place_to_win_ratio",
        ),
    ),
    "market_plus_workout_minimal": DatasetFeatureSetDefinition(
        name="market_plus_workout_minimal",
        dataset_columns=("win_odds", "popularity", "workout_gap_days", "workout_weekday_code"),
        model_parity_columns=("win_odds", "popularity", "workout_gap_days", "workout_weekday_code"),
    ),
}


FEATURE_MISSING_NULL_POLICY_REGISTRY: dict[str, FeatureMissingNullPolicy] = {
    "distance_m": FeatureMissingNullPolicy(
        canonical_name="distance_m",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when upstream BAC distance is missing or unparsable",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "race_name": FeatureMissingNullPolicy(
        canonical_name="race_name",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when upstream BAC race_name is missing",
        model_null_allowed=False,
        model_null_condition="dataset-only text feature; not permitted in model parity path",
    ),
    "workout_weekday": FeatureMissingNullPolicy(
        canonical_name="workout_weekday",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when upstream CHA workout_weekday is missing",
        model_null_allowed=False,
        model_null_condition="dataset-only text feature; not permitted in model parity path",
    ),
    "workout_date": FeatureMissingNullPolicy(
        canonical_name="workout_date",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when upstream CHA workout_date is missing",
        model_null_allowed=False,
        model_null_condition="dataset-only date feature; not permitted in model parity path",
    ),
    "win_odds": FeatureMissingNullPolicy(
        canonical_name="win_odds",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when the upstream carrier is missing, parse fails, or the snapshot is unavailable",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "popularity": FeatureMissingNullPolicy(
        canonical_name="popularity",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when the legacy SED carrier is missing or parse fails",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "place_basis_odds": FeatureMissingNullPolicy(
        canonical_name="place_basis_odds",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when OZ place odds are missing or parse fails",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "headcount": FeatureMissingNullPolicy(
        canonical_name="headcount",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when OZ headcount is missing or parse fails",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "workout_gap_days": FeatureMissingNullPolicy(
        canonical_name="workout_gap_days",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when workout_date or race_date needed for the derivation is missing",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "workout_weekday_code": FeatureMissingNullPolicy(
        canonical_name="workout_weekday_code",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when workout_weekday is missing or unmapped",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "place_slot_count": FeatureMissingNullPolicy(
        canonical_name="place_slot_count",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when headcount is missing and the derivation cannot be completed",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "log_place_minus_log_win": FeatureMissingNullPolicy(
        canonical_name="log_place_minus_log_win",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when source odds are missing, non-positive, or invalid for the log derivation",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "implied_place_prob": FeatureMissingNullPolicy(
        canonical_name="implied_place_prob",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when place_basis_odds is missing or non-positive",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "implied_win_prob": FeatureMissingNullPolicy(
        canonical_name="implied_win_prob",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when win_odds is missing or non-positive",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "implied_place_prob_minus_implied_win_prob": FeatureMissingNullPolicy(
        canonical_name="implied_place_prob_minus_implied_win_prob",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when either implied probability source is missing",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "place_to_win_ratio": FeatureMissingNullPolicy(
        canonical_name="place_to_win_ratio",
        dataset_null_allowed=True,
        dataset_null_condition="allowed only when source odds are missing or invalid for the ratio derivation",
        model_null_allowed=False,
        model_null_condition="current v1 numeric model path requires non-null values; no fillna/coercion",
    ),
    "finish_position": FeatureMissingNullPolicy(
        canonical_name="finish_position",
        dataset_null_allowed=False,
        dataset_null_condition="result-side field is outside the feature dataset contract",
        model_null_allowed=False,
        model_null_condition="forbidden post-race/result-side feature",
    ),
    "result_date": FeatureMissingNullPolicy(
        canonical_name="result_date",
        dataset_null_allowed=False,
        dataset_null_condition="result-side field is outside the feature dataset contract",
        model_null_allowed=False,
        model_null_condition="forbidden post-race/result-side feature",
    ),
    "win_payout": FeatureMissingNullPolicy(
        canonical_name="win_payout",
        dataset_null_allowed=False,
        dataset_null_condition="result-side field is outside the feature dataset contract",
        model_null_allowed=False,
        model_null_condition="forbidden post-race/result-side feature",
    ),
    "place_payout_1": FeatureMissingNullPolicy(
        canonical_name="place_payout_1",
        dataset_null_allowed=False,
        dataset_null_condition="result-side field is outside the feature dataset contract",
        model_null_allowed=False,
        model_null_condition="forbidden post-race/result-side feature",
    ),
    "place_payout_2": FeatureMissingNullPolicy(
        canonical_name="place_payout_2",
        dataset_null_allowed=False,
        dataset_null_condition="result-side field is outside the feature dataset contract",
        model_null_allowed=False,
        model_null_condition="forbidden post-race/result-side feature",
    ),
    "place_payout_3": FeatureMissingNullPolicy(
        canonical_name="place_payout_3",
        dataset_null_allowed=False,
        dataset_null_condition="result-side field is outside the feature dataset contract",
        model_null_allowed=False,
        model_null_condition="forbidden post-race/result-side feature",
    ),
}


def dataset_feature_set_names() -> tuple[str, ...]:
    return tuple(DATASET_FEATURE_SET_REGISTRY)


def feature_missing_null_policy(feature_name: str) -> FeatureMissingNullPolicy:
    try:
        return FEATURE_MISSING_NULL_POLICY_REGISTRY[feature_name]
    except KeyError as exc:
        raise ValueError(f"missing/null policy is not defined for feature: {feature_name!r}") from exc


def validate_dataset_feature_set(feature_set: str, *, include_popularity: bool = False) -> None:
    definition = DATASET_FEATURE_SET_REGISTRY.get(feature_set)
    if definition is None:
        supported = ", ".join(sorted(DATASET_FEATURE_SET_REGISTRY))
        raise ValueError(
            f"unsupported dataset_feature_set: {feature_set!r}; supported feature_sets: {supported}",
        )
    if include_popularity and not definition.optional_dataset_columns:
        raise ValueError(
            f"dataset_feature_set {feature_set!r} does not support include_popularity=true",
        )
    validate_feature_names(
        dataset_feature_columns(feature_set, include_popularity=include_popularity),
        context=f"dataset_feature_set {feature_set!r}",
        require_in_dataset=True,
    )


def dataset_feature_columns(feature_set: str, *, include_popularity: bool = False) -> tuple[str, ...]:
    definition = DATASET_FEATURE_SET_REGISTRY.get(feature_set)
    if definition is None:
        supported = ", ".join(sorted(DATASET_FEATURE_SET_REGISTRY))
        raise ValueError(
            f"unsupported dataset_feature_set: {feature_set!r}; supported feature_sets: {supported}",
        )
    if include_popularity and not definition.optional_dataset_columns:
        raise ValueError(
            f"dataset_feature_set {feature_set!r} does not support include_popularity=true",
        )
    if include_popularity:
        return definition.dataset_columns + definition.optional_dataset_columns
    return definition.dataset_columns


def supported_model_feature_sequences() -> tuple[tuple[str, ...], ...]:
    sequences: list[tuple[str, ...]] = []
    for definition in DATASET_FEATURE_SET_REGISTRY.values():
        if definition.model_parity_columns is not None:
            sequences.append(definition.model_parity_columns)
        if definition.optional_model_parity_columns is not None:
            sequences.append(definition.optional_model_parity_columns)
    seen: set[tuple[str, ...]] = set()
    ordered: list[tuple[str, ...]] = []
    for sequence in sequences:
        if sequence not in seen:
            seen.add(sequence)
            ordered.append(sequence)
    return tuple(ordered)


def model_feature_sequence_supported(feature_columns: tuple[str, ...]) -> bool:
    return feature_columns in set(supported_model_feature_sequences())


def validate_feature_names(
    feature_names: tuple[str, ...],
    *,
    context: str,
    require_in_dataset: bool = False,
    require_model_parity: bool = False,
    require_numeric: bool = False,
) -> None:
    unknown_features = sorted(set(feature_names) - set(FEATURE_REGISTRY))
    if unknown_features:
        raise ValueError(f"{context} contains unknown features: {unknown_features}")

    forbidden_leakage_features = sorted(
        {
            name
            for name in feature_names
            if not FEATURE_REGISTRY[name].leakage_allowed
        }
    )
    if forbidden_leakage_features:
        raise ValueError(
            f"{context} contains forbidden post-race/result-side features: "
            f"{forbidden_leakage_features}",
        )

    if require_in_dataset:
        not_in_dataset = sorted({name for name in feature_names if not FEATURE_REGISTRY[name].in_dataset})
        if not_in_dataset:
            raise ValueError(f"{context} contains features not allowed in dataset: {not_in_dataset}")

    if require_model_parity:
        not_in_model_parity = sorted(
            {name for name in feature_names if not FEATURE_REGISTRY[name].in_model_parity_path},
        )
        if not_in_model_parity:
            raise ValueError(
                f"{context} contains features not allowed in model parity path: "
                f"{not_in_model_parity}",
            )

    if require_numeric:
        non_numeric_features = sorted(
            {name for name in feature_names if not FEATURE_REGISTRY[name].numeric_or_not},
        )
        if non_numeric_features:
            raise ValueError(
                f"{context} contains non-numeric features not allowed in numeric model path: "
                f"{non_numeric_features}",
            )


def feature_value_is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, float):
        return math.isnan(value)
    return False


def validate_model_feature_missing_values(
    feature_columns: tuple[str, ...],
    feature_rows: Sequence[Sequence[object]],
    *,
    context: str,
    row_labels: Sequence[str] | None = None,
) -> None:
    validate_model_feature_columns(feature_columns, context=context)
    violations: dict[str, list[str]] = {}
    for row_index, row in enumerate(feature_rows, start=1):
        row_label = (
            row_labels[row_index - 1]
            if row_labels is not None
            else f"row_index={row_index}"
        )
        for feature_name, value in zip(feature_columns, row, strict=True):
            policy = feature_missing_null_policy(feature_name)
            if not feature_value_is_missing(value):
                continue
            if policy.model_null_allowed:
                continue
            violations.setdefault(feature_name, []).append(row_label)

    if not violations:
        return

    parts: list[str] = []
    for feature_name in feature_columns:
        labels = violations.get(feature_name)
        if not labels:
            continue
        policy = feature_missing_null_policy(feature_name)
        preview = ", ".join(labels[:3])
        if len(labels) > 3:
            preview = f"{preview}, ..."
        parts.append(
            f"{feature_name} at {preview} "
            f"(dataset: {policy.dataset_null_condition}; model path: {policy.model_null_condition})",
        )
    raise ValueError(
        f"{context} contains unsupported null/missing values under feature contract v1: "
        + "; ".join(parts),
    )


def validate_model_feature_columns(
    feature_columns: tuple[str, ...],
    *,
    context: str,
) -> None:
    validate_feature_names(
        feature_columns,
        context=context,
        require_in_dataset=True,
        require_model_parity=True,
        require_numeric=True,
    )
    if not model_feature_sequence_supported(feature_columns):
        supported = [list(sequence) for sequence in supported_model_feature_sequences()]
        raise ValueError(
            f"{context} is not a supported parity-safe feature sequence: {list(feature_columns)}; "
            f"supported sequences: {supported}",
        )


def validate_model_feature_columns_for_dataset_feature_set(
    *,
    dataset_feature_set: str,
    include_popularity: bool,
    feature_columns: tuple[str, ...],
    context: str,
) -> None:
    validate_dataset_feature_set(dataset_feature_set, include_popularity=include_popularity)
    definition = DATASET_FEATURE_SET_REGISTRY[dataset_feature_set]
    expected_model_columns = (
        definition.optional_model_parity_columns
        if include_popularity and definition.optional_model_parity_columns is not None
        else definition.model_parity_columns
    )
    if expected_model_columns is None:
        raise ValueError(
            f"dataset_feature_set {dataset_feature_set!r} is dataset-only and has no model parity path",
        )
    validate_feature_names(
        feature_columns,
        context=context,
        require_in_dataset=True,
        require_model_parity=True,
        require_numeric=True,
    )
    if feature_columns != expected_model_columns:
        raise ValueError(
            f"{context} must match dataset_feature_set {dataset_feature_set!r}: "
            f"expected {list(expected_model_columns)}, got {list(feature_columns)}",
        )
    expected_columns = dataset_feature_columns(
        dataset_feature_set,
        include_popularity=include_popularity,
    )
    missing_from_dataset = sorted(set(feature_columns) - set(expected_columns))
    if missing_from_dataset:
        raise ValueError(
            f"{context} contains features not produced by dataset_feature_set "
            f"{dataset_feature_set!r}: {missing_from_dataset}",
        )
