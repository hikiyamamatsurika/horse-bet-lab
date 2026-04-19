from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from horse_bet_lab.forward_test.runner import load_config, run_place_forward_test


def create_training_dataset(path: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT * FROM (
                    VALUES
                        ('11111111', 1, 'train', 1, 1.8, 2.8),
                        ('11111111', 2, 'train', 0, 8.5, 1.4),
                        ('22222222', 1, 'valid', 1, 2.0, 3.0),
                        ('22222222', 2, 'valid', 0, 9.2, 1.5),
                        ('33333333', 1, 'test', 1, 1.9, 2.9),
                        ('33333333', 2, 'test', 0, 10.0, 1.6)
                ) AS t(
                    race_key,
                    horse_number,
                    split,
                    target_value,
                    win_odds,
                    place_basis_odds
                )
            ) TO ? (FORMAT PARQUET)
            """,
            [str(path)],
        )
    finally:
        connection.close()


def write_forward_input(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [
        "race_key",
        "horse_number",
        "win_odds",
        "place_basis_odds",
        "popularity",
        "odds_observation_timestamp",
        "input_source_name",
        "input_source_url",
        "input_source_timestamp",
        "carrier_identity",
        "snapshot_status",
        "retry_count",
        "timeout_seconds",
        "snapshot_failure_reason",
        "popularity_input_source",
        "popularity_contract_status",
        "input_schema_version",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_config(path: Path, dataset_path: Path, input_path: Path, output_dir: Path) -> None:
    path.write_text(
        (
            "[place_forward_test]\n"
            "name = 'phase1_forward_test'\n"
            f"input_path = '{input_path}'\n"
            f"output_dir = '{output_dir}'\n"
            "input_schema_version = 'place_forward_test_input_v1'\n"
            "\n"
            "[place_forward_test.reference_model]\n"
            f"dataset_path = '{dataset_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds']\n"
            "feature_transforms = ['log1p']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            "training_splits = ['train', 'valid', 'test']\n"
            "max_iter = 200\n"
            "model_params = {}\n"
            "model_version = 'dual_market_logreg@phase1-test'\n"
            "\n"
            "[place_forward_test.bet_logic]\n"
            "selection_metric = 'edge'\n"
            "threshold = 0.05\n"
            "stake_per_bet = 100\n"
            "stronger_guard_edge_surcharge = 0.01\n"
            "candidate_logic_id = 'guard_0_01_plus_proxy_domain_overlay'\n"
            "fallback_logic_id = 'no_bet_guard_stronger'\n"
            "formal_domain_mapping_confirmed = true\n"
        ),
        encoding="utf-8",
    )


def test_place_forward_test_runner_writes_predictions_and_decisions(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.parquet"
    input_path = tmp_path / "forward_input.csv"
    output_dir = tmp_path / "artifacts"
    config_path = tmp_path / "config.toml"

    create_training_dataset(dataset_path)
    write_forward_input(
        input_path,
        [
            {
                "race_key": "01260101",
                "horse_number": "1",
                "win_odds": "1.8",
                "place_basis_odds": "2.8",
                "popularity": "",
                "odds_observation_timestamp": "2026-04-19T10:00:00+09:00",
                "input_source_name": "operator_csv",
                "input_source_url": "https://example.com/odds",
                "input_source_timestamp": "2026-04-19T09:59:00+09:00",
                "carrier_identity": "place_forward_live_snapshot_v1",
                "snapshot_status": "ok",
                "retry_count": "1",
                "timeout_seconds": "15",
                "snapshot_failure_reason": "",
                "popularity_input_source": "",
                "popularity_contract_status": "",
                "input_schema_version": "place_forward_test_input_v1",
            },
            {
                "race_key": "01260101",
                "horse_number": "2",
                "win_odds": "9.0",
                "place_basis_odds": "1.4",
                "popularity": "2",
                "odds_observation_timestamp": "2026-04-19T10:00:00+09:00",
                "input_source_name": "operator_csv",
                "input_source_url": "https://example.com/odds",
                "input_source_timestamp": "2026-04-19T09:59:00+09:00",
                "carrier_identity": "place_forward_live_snapshot_v1",
                "snapshot_status": "ok",
                "retry_count": "1",
                "timeout_seconds": "15",
                "snapshot_failure_reason": "",
                "popularity_input_source": "operator_csv",
                "popularity_contract_status": "unresolved_auxiliary",
                "input_schema_version": "place_forward_test_input_v1",
            },
            {
                "race_key": "02260101",
                "horse_number": "1",
                "win_odds": "",
                "place_basis_odds": "",
                "popularity": "",
                "odds_observation_timestamp": "2026-04-19T10:01:00+09:00",
                "input_source_name": "operator_csv",
                "input_source_url": "https://example.com/odds",
                "input_source_timestamp": "2026-04-19T10:00:30+09:00",
                "carrier_identity": "place_forward_live_snapshot_v1",
                "snapshot_status": "timeout",
                "retry_count": "3",
                "timeout_seconds": "15",
                "snapshot_failure_reason": "timed out while waiting for final odds snapshot",
                "popularity_input_source": "",
                "popularity_contract_status": "",
                "input_schema_version": "place_forward_test_input_v1",
            },
        ],
    )
    write_config(config_path, dataset_path, input_path, output_dir)

    result = run_place_forward_test(load_config(config_path))

    assert result.input_record_count == 3
    assert result.prediction_record_count == 2
    assert result.decision_record_count == 3
    assert (output_dir / "input_snapshot_records.csv").exists()
    assert (output_dir / "prediction_output_records.csv").exists()
    assert (output_dir / "bet_decision_records.csv").exists()
    assert (output_dir / "run_manifest.json").exists()

    predictions = json.loads((output_dir / "prediction_output_records.json").read_text(encoding="utf-8"))
    assert len(predictions) == 2
    assert predictions[0]["feature_contract_version"] == "v1"

    decisions = json.loads((output_dir / "bet_decision_records.json").read_text(encoding="utf-8"))
    assert len(decisions) == 3
    assert any(row["bet_action"] == "bet" for row in decisions)
    timeout_row = next(row for row in decisions if row["race_key"] == "02260101")
    assert timeout_row["bet_action"] == "no_bet"
    assert timeout_row["no_bet_reason"] == "timeout"

    manifest = json.loads((output_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["record_counts"]["input_records"] == 3
    assert manifest["bet_logic"]["candidate_logic_id"] == "guard_0_01_plus_proxy_domain_overlay"
    assert manifest["artifact_contract"]["feature_contract_version"] == "v1"


@pytest.mark.parametrize(
    ("snapshot_status", "failure_reason"),
    [
        ("snapshot_failure", "source page returned invalid snapshot"),
        ("timeout", "timed out"),
        ("retry_exhausted", "retry budget exhausted"),
        ("required_odds_missing", "required odds missing"),
    ],
)
def test_place_forward_test_runner_writes_explicit_no_bet_for_snapshot_failures(
    tmp_path: Path,
    snapshot_status: str,
    failure_reason: str,
) -> None:
    dataset_path = tmp_path / "dataset.parquet"
    input_path = tmp_path / "forward_input.csv"
    output_dir = tmp_path / "artifacts"
    config_path = tmp_path / "config.toml"

    create_training_dataset(dataset_path)
    write_forward_input(
        input_path,
        [
            {
                "race_key": "03260101",
                "horse_number": "1",
                "win_odds": "",
                "place_basis_odds": "",
                "popularity": "",
                "odds_observation_timestamp": "2026-04-19T10:02:00+09:00",
                "input_source_name": "operator_csv",
                "input_source_url": "",
                "input_source_timestamp": "",
                "carrier_identity": "place_forward_live_snapshot_v1",
                "snapshot_status": snapshot_status,
                "retry_count": "2",
                "timeout_seconds": "15",
                "snapshot_failure_reason": failure_reason,
                "popularity_input_source": "",
                "popularity_contract_status": "",
                "input_schema_version": "place_forward_test_input_v1",
            },
        ],
    )
    write_config(config_path, dataset_path, input_path, output_dir)

    result = run_place_forward_test(load_config(config_path))

    assert result.prediction_record_count == 0
    decisions = json.loads((output_dir / "bet_decision_records.json").read_text(encoding="utf-8"))
    assert decisions == [
        {
            "race_key": "03260101",
            "horse_number": 1,
            "bet_action": "no_bet",
            "decision_reason": failure_reason,
            "no_bet_reason": snapshot_status,
            "feature_contract_version": "v1",
            "model_version": "dual_market_logreg@phase1-test",
            "carrier_identity": "place_forward_live_snapshot_v1",
            "odds_observation_timestamp": "2026-04-19T10:02:00+09:00",
            "baseline_logic_id": "guard_0_01_plus_proxy_domain_overlay",
            "fallback_logic_id": "no_bet_guard_stronger",
            "input_schema_version": "place_forward_test_input_v1",
            "run_manifest_hash": None,
        }
    ]


def test_place_forward_test_runner_rejects_duplicate_input_rows(tmp_path: Path) -> None:
    dataset_path = tmp_path / "dataset.parquet"
    input_path = tmp_path / "forward_input.csv"
    output_dir = tmp_path / "artifacts"
    config_path = tmp_path / "config.toml"

    create_training_dataset(dataset_path)
    duplicate_row = {
        "race_key": "04260101",
        "horse_number": "1",
        "win_odds": "2.0",
        "place_basis_odds": "3.1",
        "popularity": "",
        "odds_observation_timestamp": "2026-04-19T10:03:00+09:00",
        "input_source_name": "operator_csv",
        "input_source_url": "",
        "input_source_timestamp": "",
        "carrier_identity": "place_forward_live_snapshot_v1",
        "snapshot_status": "ok",
        "retry_count": "1",
        "timeout_seconds": "15",
        "snapshot_failure_reason": "",
        "popularity_input_source": "",
        "popularity_contract_status": "",
        "input_schema_version": "place_forward_test_input_v1",
    }
    write_forward_input(input_path, [duplicate_row, duplicate_row])
    write_config(config_path, dataset_path, input_path, output_dir)

    with pytest.raises(ValueError, match="duplicate forward-test input record"):
        run_place_forward_test(load_config(config_path))
