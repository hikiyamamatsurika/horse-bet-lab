from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from horse_bet_lab.features.registry import (
    DATASET_FEATURE_SET_REGISTRY,
    FEATURE_REGISTRY,
    dataset_feature_columns,
)

FEATURE_CONTRACT_VERSION = "v1"


def dataset_model_feature_columns(
    feature_set: str,
    *,
    include_popularity: bool = False,
) -> tuple[str, ...] | None:
    definition = DATASET_FEATURE_SET_REGISTRY[feature_set]
    if include_popularity and definition.optional_model_parity_columns is not None:
        return definition.optional_model_parity_columns
    return definition.model_parity_columns


def build_feature_source_summary(feature_names: tuple[str, ...]) -> dict[str, Any]:
    if not feature_names:
        return {
            "feature_count": 0,
            "by_source_class": {},
            "by_timing_class": {},
            "by_carrier_identity": {},
            "numeric_feature_count": 0,
            "dataset_feature_count": 0,
            "model_parity_feature_count": 0,
        }

    by_source_class: dict[str, int] = {}
    by_timing_class: dict[str, int] = {}
    by_carrier_identity: dict[str, int] = {}
    numeric_feature_count = 0
    dataset_feature_count = 0
    model_parity_feature_count = 0
    for feature_name in feature_names:
        definition = FEATURE_REGISTRY[feature_name]
        by_source_class[definition.source_class] = by_source_class.get(definition.source_class, 0) + 1
        by_timing_class[definition.timing_class] = by_timing_class.get(definition.timing_class, 0) + 1
        by_carrier_identity[definition.carrier_identity] = (
            by_carrier_identity.get(definition.carrier_identity, 0) + 1
        )
        if definition.numeric_or_not:
            numeric_feature_count += 1
        if definition.in_dataset:
            dataset_feature_count += 1
        if definition.in_model_parity_path:
            model_parity_feature_count += 1
    return {
        "feature_count": len(feature_names),
        "by_source_class": by_source_class,
        "by_timing_class": by_timing_class,
        "by_carrier_identity": by_carrier_identity,
        "numeric_feature_count": numeric_feature_count,
        "dataset_feature_count": dataset_feature_count,
        "model_parity_feature_count": model_parity_feature_count,
    }


def build_feature_definitions(feature_names: tuple[str, ...]) -> list[dict[str, Any]]:
    return [
        {
            "canonical_name": feature_name,
            "in_dataset": FEATURE_REGISTRY[feature_name].in_dataset,
            "in_model_parity_path": FEATURE_REGISTRY[feature_name].in_model_parity_path,
            "numeric_or_not": FEATURE_REGISTRY[feature_name].numeric_or_not,
            "source_class": FEATURE_REGISTRY[feature_name].source_class,
            "timing_class": FEATURE_REGISTRY[feature_name].timing_class,
            "carrier_identity": FEATURE_REGISTRY[feature_name].carrier_identity,
            "leakage_allowed": FEATURE_REGISTRY[feature_name].leakage_allowed,
        }
        for feature_name in feature_names
    ]


def build_feature_provenance_payload(
    *,
    artifact_kind: str,
    generated_by: str,
    config_identifier: str,
    dataset_feature_set: str | None = None,
    include_popularity: bool = False,
    model_feature_columns: tuple[str, ...] | None = None,
    config_path: str | None = None,
    artifact_path: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    dataset_columns: tuple[str, ...] | None = None
    if dataset_feature_set is not None:
        dataset_columns = dataset_feature_columns(
            dataset_feature_set,
            include_popularity=include_popularity,
        )
    feature_columns_order = (
        model_feature_columns
        if model_feature_columns is not None
        else dataset_columns
        if dataset_columns is not None
        else ()
    )
    payload: dict[str, Any] = {
        "feature_contract_version": FEATURE_CONTRACT_VERSION,
        "artifact_kind": artifact_kind,
        "generated_by": generated_by,
        "config_identifier": config_identifier,
        "config_path": config_path,
        "artifact_path": artifact_path,
        "dataset_feature_set": dataset_feature_set,
        "dataset_feature_columns": list(dataset_columns) if dataset_columns is not None else None,
        "model_feature_columns": list(model_feature_columns) if model_feature_columns is not None else None,
        "feature_columns_order": list(feature_columns_order),
        "feature_source_summary": build_feature_source_summary(feature_columns_order),
        "feature_definitions": build_feature_definitions(feature_columns_order),
        "registry_interpretation": "horse_bet_lab.features.registry",
    }
    if extra:
        payload["extra"] = extra
    return payload


def provenance_sidecar_path(artifact_path: Path) -> Path:
    return artifact_path.with_name(f"{artifact_path.name}.provenance.json")


def write_feature_provenance_sidecar(artifact_path: Path, payload: dict[str, Any]) -> Path:
    output_path = provenance_sidecar_path(artifact_path)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path
