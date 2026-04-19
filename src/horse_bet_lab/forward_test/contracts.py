from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Sequence

from horse_bet_lab.features.provenance import FEATURE_CONTRACT_VERSION

PLACE_FORWARD_TEST_CONTRACT_VERSION = "v1"
PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION = "place_forward_test_input_v1"
PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION = "place_forward_test_output_v1"
PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS = "unresolved_auxiliary"

PLACE_FORWARD_TEST_SNAPSHOT_STATUSES = frozenset(
    {
        "ok",
        "snapshot_failure",
        "timeout",
        "retry_exhausted",
        "required_odds_missing",
    }
)

PLACE_FORWARD_TEST_NO_BET_REASONS = frozenset(
    {
        "snapshot_failure",
        "timeout",
        "retry_exhausted",
        "required_odds_missing",
        "logic_filtered",
        "manual_skip",
    }
)


def _require_non_empty_text(value: str, field_name: str) -> str:
    text = value.strip()
    if text == "":
        raise ValueError(f"{field_name} must be a non-empty string")
    return text


def _require_iso_timestamp(value: str, field_name: str) -> str:
    text = _require_non_empty_text(value, field_name)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO-8601 timestamp, got {text!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone offset, got {text!r}")
    return text


def _require_race_key(value: str) -> str:
    race_key = _require_non_empty_text(value, "race_key")
    if len(race_key) != 8 or not race_key.isdigit():
        raise ValueError(
            f"race_key must be an 8-digit upstream race identifier, got {race_key!r}"
        )
    return race_key


def _require_positive_int(value: int | None, field_name: str) -> int:
    if value is None:
        raise ValueError(f"{field_name} must be provided")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive, got {value}")
    return value


def _require_non_negative_int(value: int, field_name: str) -> int:
    if value < 0:
        raise ValueError(f"{field_name} must be >= 0, got {value}")
    return value


def _require_positive_float(value: float | None, field_name: str) -> float:
    if value is None:
        raise ValueError(f"{field_name} must be provided")
    if value <= 0.0:
        raise ValueError(f"{field_name} must be > 0, got {value}")
    return value


@dataclass(frozen=True)
class PlaceForwardInputRecord:
    race_key: str
    horse_number: int
    win_odds: float | None
    place_basis_odds: float | None
    popularity: int | None
    odds_observation_timestamp: str
    input_source_name: str
    input_source_url: str | None
    input_source_timestamp: str | None
    carrier_identity: str
    snapshot_status: str
    retry_count: int
    timeout_seconds: float | None
    snapshot_failure_reason: str | None = None
    popularity_input_source: str | None = None
    popularity_contract_status: str | None = None
    input_schema_version: str = PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlaceForwardPredictionOutputRecord:
    race_key: str
    horse_number: int
    prediction_probability: float
    model_version: str
    feature_contract_version: str
    carrier_identity: str
    odds_observation_timestamp: str
    input_schema_version: str = PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION
    output_schema_version: str = PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlaceForwardBetDecisionRecord:
    race_key: str
    horse_number: int
    bet_action: str
    decision_reason: str
    no_bet_reason: str | None
    feature_contract_version: str
    model_version: str
    carrier_identity: str
    odds_observation_timestamp: str
    baseline_logic_id: str | None = None
    fallback_logic_id: str | None = None
    input_schema_version: str = PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION
    run_manifest_hash: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlaceForwardArtifactProvenance:
    feature_contract_version: str
    model_version: str
    carrier_identity: str
    odds_observation_timestamp: str
    decision_reason: str
    baseline_logic_id: str | None = None
    fallback_logic_id: str | None = None
    input_schema_version: str = PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION
    run_manifest_hash: str | None = None
    place_forward_test_contract_version: str = PLACE_FORWARD_TEST_CONTRACT_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_place_forward_input_record(record: PlaceForwardInputRecord) -> PlaceForwardInputRecord:
    race_key = _require_race_key(record.race_key)
    horse_number = _require_positive_int(record.horse_number, "horse_number")
    odds_observation_timestamp = _require_iso_timestamp(
        record.odds_observation_timestamp,
        "odds_observation_timestamp",
    )
    input_source_name = _require_non_empty_text(record.input_source_name, "input_source_name")
    carrier_identity = _require_non_empty_text(record.carrier_identity, "carrier_identity")
    snapshot_status = _require_non_empty_text(record.snapshot_status, "snapshot_status")
    if snapshot_status not in PLACE_FORWARD_TEST_SNAPSHOT_STATUSES:
        raise ValueError(
            "snapshot_status must be one of "
            f"{sorted(PLACE_FORWARD_TEST_SNAPSHOT_STATUSES)}, got {snapshot_status!r}"
        )
    _require_non_negative_int(record.retry_count, "retry_count")
    if record.timeout_seconds is not None and record.timeout_seconds <= 0.0:
        raise ValueError(f"timeout_seconds must be > 0 when provided, got {record.timeout_seconds}")
    if record.input_source_timestamp is not None:
        _require_iso_timestamp(record.input_source_timestamp, "input_source_timestamp")
    if record.input_source_url is not None and record.input_source_url.strip() == "":
        raise ValueError("input_source_url must be omitted or non-empty")

    if record.snapshot_status == "ok":
        _require_positive_float(record.win_odds, "win_odds")
        _require_positive_float(record.place_basis_odds, "place_basis_odds")
        if record.snapshot_failure_reason is not None and record.snapshot_failure_reason.strip() != "":
            raise ValueError("snapshot_failure_reason must be empty when snapshot_status='ok'")
    else:
        if record.snapshot_failure_reason is None or record.snapshot_failure_reason.strip() == "":
            raise ValueError(
                "snapshot_failure_reason must be provided when snapshot_status is not 'ok'"
            )

    if record.popularity is not None:
        _require_positive_int(record.popularity, "popularity")
        popularity_contract_status = _require_non_empty_text(
            record.popularity_contract_status or "",
            "popularity_contract_status",
        )
        if popularity_contract_status != PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS:
            raise ValueError(
                "popularity_contract_status must remain "
                f"{PLACE_FORWARD_TEST_POPULARITY_CONTRACT_STATUS!r}, got {popularity_contract_status!r}"
            )
        if record.popularity_input_source is None or record.popularity_input_source.strip() == "":
            raise ValueError(
                "popularity_input_source must be provided when popularity is present"
            )
    else:
        if record.popularity_contract_status is not None:
            raise ValueError(
                "popularity_contract_status must be omitted when popularity is not provided"
            )
        if record.popularity_input_source is not None:
            raise ValueError(
                "popularity_input_source must be omitted when popularity is not provided"
            )

    if record.input_schema_version != PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION:
        raise ValueError(
            "input_schema_version must be "
            f"{PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION!r}, got {record.input_schema_version!r}"
        )

    return PlaceForwardInputRecord(
        race_key=race_key,
        horse_number=horse_number,
        win_odds=record.win_odds,
        place_basis_odds=record.place_basis_odds,
        popularity=record.popularity,
        odds_observation_timestamp=odds_observation_timestamp,
        input_source_name=input_source_name,
        input_source_url=record.input_source_url,
        input_source_timestamp=record.input_source_timestamp,
        carrier_identity=carrier_identity,
        snapshot_status=snapshot_status,
        retry_count=record.retry_count,
        timeout_seconds=record.timeout_seconds,
        snapshot_failure_reason=record.snapshot_failure_reason,
        popularity_input_source=record.popularity_input_source,
        popularity_contract_status=record.popularity_contract_status,
        input_schema_version=record.input_schema_version,
    )


def validate_place_forward_input_records(
    records: Sequence[PlaceForwardInputRecord],
) -> tuple[PlaceForwardInputRecord, ...]:
    seen: set[tuple[str, int]] = set()
    validated: list[PlaceForwardInputRecord] = []
    for record in records:
        validated_record = validate_place_forward_input_record(record)
        key = (validated_record.race_key, validated_record.horse_number)
        if key in seen:
            raise ValueError(
                "duplicate forward-test input record for "
                f"race_key={validated_record.race_key} horse_number={validated_record.horse_number}"
            )
        seen.add(key)
        validated.append(validated_record)
    return tuple(validated)


def validate_place_forward_prediction_output_record(
    record: PlaceForwardPredictionOutputRecord,
) -> PlaceForwardPredictionOutputRecord:
    race_key = _require_race_key(record.race_key)
    horse_number = _require_positive_int(record.horse_number, "horse_number")
    if not 0.0 <= record.prediction_probability <= 1.0:
        raise ValueError(
            "prediction_probability must be between 0 and 1 inclusive, "
            f"got {record.prediction_probability}"
        )
    model_version = _require_non_empty_text(record.model_version, "model_version")
    feature_contract_version = _require_non_empty_text(
        record.feature_contract_version,
        "feature_contract_version",
    )
    carrier_identity = _require_non_empty_text(record.carrier_identity, "carrier_identity")
    odds_observation_timestamp = _require_iso_timestamp(
        record.odds_observation_timestamp,
        "odds_observation_timestamp",
    )
    if record.input_schema_version != PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION:
        raise ValueError(
            "input_schema_version must be "
            f"{PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION!r}, got {record.input_schema_version!r}"
        )
    if record.output_schema_version != PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION:
        raise ValueError(
            "output_schema_version must be "
            f"{PLACE_FORWARD_TEST_OUTPUT_SCHEMA_VERSION!r}, got {record.output_schema_version!r}"
        )
    return PlaceForwardPredictionOutputRecord(
        race_key=race_key,
        horse_number=horse_number,
        prediction_probability=record.prediction_probability,
        model_version=model_version,
        feature_contract_version=feature_contract_version,
        carrier_identity=carrier_identity,
        odds_observation_timestamp=odds_observation_timestamp,
        input_schema_version=record.input_schema_version,
        output_schema_version=record.output_schema_version,
    )


def validate_place_forward_bet_decision_record(
    record: PlaceForwardBetDecisionRecord,
) -> PlaceForwardBetDecisionRecord:
    race_key = _require_race_key(record.race_key)
    horse_number = _require_positive_int(record.horse_number, "horse_number")
    bet_action = _require_non_empty_text(record.bet_action, "bet_action")
    if bet_action not in {"bet", "no_bet"}:
        raise ValueError(f"bet_action must be 'bet' or 'no_bet', got {bet_action!r}")
    decision_reason = _require_non_empty_text(record.decision_reason, "decision_reason")
    feature_contract_version = _require_non_empty_text(
        record.feature_contract_version,
        "feature_contract_version",
    )
    model_version = _require_non_empty_text(record.model_version, "model_version")
    carrier_identity = _require_non_empty_text(record.carrier_identity, "carrier_identity")
    odds_observation_timestamp = _require_iso_timestamp(
        record.odds_observation_timestamp,
        "odds_observation_timestamp",
    )
    if bet_action == "bet":
        if record.no_bet_reason is not None:
            raise ValueError("no_bet_reason must be omitted when bet_action='bet'")
    else:
        no_bet_reason = _require_non_empty_text(record.no_bet_reason or "", "no_bet_reason")
        if no_bet_reason not in PLACE_FORWARD_TEST_NO_BET_REASONS:
            raise ValueError(
                "no_bet_reason must be one of "
                f"{sorted(PLACE_FORWARD_TEST_NO_BET_REASONS)}, got {no_bet_reason!r}"
            )
    if record.input_schema_version != PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION:
        raise ValueError(
            "input_schema_version must be "
            f"{PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION!r}, got {record.input_schema_version!r}"
        )
    if record.run_manifest_hash is not None and record.run_manifest_hash.strip() == "":
        raise ValueError("run_manifest_hash must be omitted or non-empty")
    return PlaceForwardBetDecisionRecord(
        race_key=race_key,
        horse_number=horse_number,
        bet_action=bet_action,
        decision_reason=decision_reason,
        no_bet_reason=record.no_bet_reason,
        feature_contract_version=feature_contract_version,
        model_version=model_version,
        carrier_identity=carrier_identity,
        odds_observation_timestamp=odds_observation_timestamp,
        baseline_logic_id=record.baseline_logic_id,
        fallback_logic_id=record.fallback_logic_id,
        input_schema_version=record.input_schema_version,
        run_manifest_hash=record.run_manifest_hash,
    )


def build_place_forward_artifact_provenance(
    *,
    model_version: str,
    carrier_identity: str,
    odds_observation_timestamp: str,
    decision_reason: str,
    baseline_logic_id: str | None = None,
    fallback_logic_id: str | None = None,
    input_schema_version: str = PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION,
    run_manifest_hash: str | None = None,
    feature_contract_version: str = FEATURE_CONTRACT_VERSION,
) -> dict[str, Any]:
    payload = PlaceForwardArtifactProvenance(
        feature_contract_version=feature_contract_version,
        model_version=_require_non_empty_text(model_version, "model_version"),
        carrier_identity=_require_non_empty_text(carrier_identity, "carrier_identity"),
        odds_observation_timestamp=_require_iso_timestamp(
            odds_observation_timestamp,
            "odds_observation_timestamp",
        ),
        decision_reason=_require_non_empty_text(decision_reason, "decision_reason"),
        baseline_logic_id=baseline_logic_id,
        fallback_logic_id=fallback_logic_id,
        input_schema_version=input_schema_version,
        run_manifest_hash=run_manifest_hash,
    )
    if input_schema_version != PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION:
        raise ValueError(
            "input_schema_version must be "
            f"{PLACE_FORWARD_TEST_INPUT_SCHEMA_VERSION!r}, got {input_schema_version!r}"
        )
    return payload.to_dict()
