from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import duckdb


def load_proxy_live_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "live_inference_score.py"
    spec = importlib.util.spec_from_file_location("proxy_live_inference", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load proxy_live_inference module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def create_training_dataset(path: Path) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT * FROM (
                    VALUES
                        ('11111111', 1, 'train', 1, 3.0, 1.8, 1),
                        ('11111111', 2, 'train', 0, 9.0, 3.3, 2),
                        ('22222222', 1, 'valid', 1, 4.0, 2.0, 1),
                        ('22222222', 2, 'valid', 0, 10.0, 3.5, 2),
                        ('33333333', 1, 'test', 1, 5.0, 2.4, 1),
                        ('33333333', 2, 'test', 0, 11.0, 4.0, 2)
                ) AS t(
                    race_key,
                    horse_number,
                    split,
                    target_value,
                    win_odds,
                    place_basis_odds,
                    popularity
                )
            ) TO ? (FORMAT PARQUET)
            """,
            [str(path)],
        )
    finally:
        connection.close()


def create_historical_duckdb(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE jrdb_oz_staging (
                race_key VARCHAR,
                horse_number INTEGER,
                headcount INTEGER,
                win_basis_odds DOUBLE,
                place_basis_odds DOUBLE,
                source_file_path VARCHAR,
                source_file_hash VARCHAR,
                ingestion_run_id BIGINT,
                ingested_at TIMESTAMP
            )
            """,
        )
        connection.execute(
            """
            INSERT INTO jrdb_oz_staging VALUES
                ('44444444', 1, 2, 3.2, 1.8, 'x', 'y', 1, NOW()),
                ('44444444', 2, 2, 7.5, 3.0, 'x', 'y', 1, NOW())
            """
        )
    finally:
        connection.close()


def write_live_input(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "race_key",
                "horse_number",
                "horse_name",
                "win_odds",
                "popularity",
                "place_odds_min",
                "place_odds_max",
                "place_basis_odds_proxy",
            ]
        )
        writer.writerow(["44444444", "1", "Alpha", "3.2", "1", "1.6", "2.0", "1.8"])
        writer.writerow(["44444444", "2", "Beta", "7.5", "2", "2.6", "3.4", "3.0"])


def write_config(path: Path, dataset_path: Path, input_path: Path, output_path: Path, db_path: Path) -> None:
    path.write_text(
        (
            "[live_inference]\n"
            "name = 'proxy_live_test_race'\n"
            "race_name = 'Test Race'\n"
            "race_date = '2026-04-19'\n"
            f"input_path = '{input_path}'\n"
            f"output_path = '{output_path}'\n"
            "input_source = 'Public odds page'\n"
            "source_url = 'https://example.com/odds'\n"
            "source_timestamp = '2026-04-19T08:00:00+09:00'\n"
            "proxy_rule = 'place_basis_odds_proxy = (place_odds_min + place_odds_max) / 2'\n"
            "caveat = [\n"
            "  'proxy_score is relative-only',\n"
            "  'not comparable to mainline scores',\n"
            "]\n"
            "\n"
            "[live_inference.reference_model]\n"
            f"dataset_path = '{dataset_path}'\n"
            f"historical_duckdb_path = '{db_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'place_basis_odds', 'popularity']\n"
            "feature_transforms = ['log1p', 'log1p', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            "training_splits = ['train', 'valid', 'test']\n"
            "max_iter = 200\n"
            "model_params = {}\n"
            "live_feature_aliases = { place_basis_odds = 'place_basis_odds_proxy' }\n"
        ),
        encoding="utf-8",
    )


def test_proxy_live_pipeline_writes_manifest_and_gap_summary(tmp_path: Path) -> None:
    module = load_proxy_live_module()

    dataset_path = tmp_path / "dataset.parquet"
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "artifacts" / "scores.csv"
    config_path = tmp_path / "config.toml"
    db_path = tmp_path / "historical.duckdb"

    create_training_dataset(dataset_path)
    create_historical_duckdb(db_path)
    write_live_input(input_path)
    write_config(config_path, dataset_path, input_path, output_path, db_path)

    config = module.load_config(config_path)
    X_train, y_train = module.load_training_matrix(config.reference_model)
    model = module.fit_model(
        model_name=config.reference_model.model_name,
        X=X_train,
        y=y_train,
        max_iter=config.reference_model.max_iter,
        model_params=config.reference_model.model_params,
    )
    live_rows, X_live = module.load_live_rows(config)
    probabilities = module.predict_probabilities(model, X_live)
    output_rows = module.build_output_rows(live_rows, probabilities)
    module.write_output(output_path, output_rows)
    module.write_manifest(config, output_path.parent / "run_manifest.json")
    module.write_summary(config, output_rows, output_path.parent / "summary.txt")
    module.write_proxy_gap_diagnostic(
        config,
        live_rows,
        output_path.parent / "proxy_feature_gap_summary.json",
    )

    manifest = json.loads((output_path.parent / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["source_url"] == "https://example.com/odds"
    assert manifest["source_timestamp"] == "2026-04-19T08:00:00+09:00"
    assert manifest["input_csv_hash"] == hashlib.sha256(input_path.read_bytes()).hexdigest()
    assert manifest["provenance"]["feature_contract_version"] == "v1"
    assert manifest["provenance"]["model_feature_columns"] == [
        "win_odds",
        "place_basis_odds",
        "popularity",
    ]

    gap_summary = json.loads(
        (output_path.parent / "proxy_feature_gap_summary.json").read_text(encoding="utf-8")
    )
    assert gap_summary["status"] == "ok"
    assert gap_summary["row_count"] == 2
    assert gap_summary["metrics"]["mean_diff"] == 0.0

    score_lines = output_path.read_text(encoding="utf-8").splitlines()
    assert score_lines[0] == (
        "race_key,horse_number,horse_name,win_odds,place_basis_odds_proxy,"
        "popularity,place_odds_min,place_odds_max,proxy_score,rank"
    )


def test_proxy_live_rejects_forbidden_leakage_columns(tmp_path: Path) -> None:
    module = load_proxy_live_module()

    dataset_path = tmp_path / "dataset.parquet"
    input_path = tmp_path / "input_with_leakage.csv"
    output_path = tmp_path / "artifacts" / "scores.csv"
    config_path = tmp_path / "config.toml"
    db_path = tmp_path / "historical.duckdb"

    create_training_dataset(dataset_path)
    create_historical_duckdb(db_path)
    write_config(config_path, dataset_path, input_path, output_path, db_path)
    input_path.write_text(
        (
            "race_key,horse_number,horse_name,win_odds,popularity,place_odds_min,"
            "place_odds_max,place_basis_odds_proxy,finish_position\n"
            "44444444,1,Alpha,3.2,1,1.6,2.0,1.8,1\n"
        ),
        encoding="utf-8",
    )

    config = module.load_config(config_path)
    try:
        module.load_live_rows(config)
    except ValueError as exc:
        assert "forbidden leakage columns" in str(exc)
    else:
        raise AssertionError("expected leakage columns to be rejected")


def test_proxy_live_config_rejects_dataset_only_feature_columns(tmp_path: Path) -> None:
    module = load_proxy_live_module()

    dataset_path = tmp_path / "dataset.parquet"
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "artifacts" / "scores.csv"
    config_path = tmp_path / "config_invalid.toml"
    db_path = tmp_path / "historical.duckdb"

    create_training_dataset(dataset_path)
    create_historical_duckdb(db_path)
    write_live_input(input_path)
    config_path.write_text(
        (
            "[live_inference]\n"
            "name = 'proxy_live_invalid'\n"
            "race_name = 'Test Race'\n"
            "race_date = '2026-04-19'\n"
            f"input_path = '{input_path}'\n"
            f"output_path = '{output_path}'\n"
            "input_source = 'Public odds page'\n"
            "source_url = 'https://example.com/odds'\n"
            "source_timestamp = '2026-04-19T08:00:00+09:00'\n"
            "proxy_rule = 'place_basis_odds_proxy = (place_odds_min + place_odds_max) / 2'\n"
            "\n"
            "[live_inference.reference_model]\n"
            f"dataset_path = '{dataset_path}'\n"
            f"historical_duckdb_path = '{db_path}'\n"
            "model_name = 'logistic_regression'\n"
            "feature_columns = ['win_odds', 'race_name']\n"
            "feature_transforms = ['log1p', 'identity']\n"
            "target_column = 'target_value'\n"
            "split_column = 'split'\n"
            "training_splits = ['train', 'valid', 'test']\n"
            "max_iter = 200\n"
            "model_params = {}\n"
        ),
        encoding="utf-8",
    )

    try:
        module.load_config(config_path)
    except ValueError as exc:
        assert "features not allowed in model parity path" in str(exc)
    else:
        raise AssertionError("expected dataset-only live feature config to be rejected")
