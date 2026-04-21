# JRDB Auto-Ingestion MVP Runbook

## Purpose

この runbook は、JRDB auto-ingestion MVP を local-only で回す最小手順を固定する。

対象:

- archive download
- extraction / staging / ready 化
- optional JRDB ingest
- optional Phase 1 pre-race handoff

## What This Does Not Change

- BET logic は変えない
- baseline は変えない
- `popularity` は unresolved auxiliary のまま
- frozen `OZ.win_basis_odds` migration は再開しない
- purchase automation はやらない

## Local Secrets

次のどちらかを使う。

1. env var
   - `JRDB_AUTO_INGESTION_USERNAME`
   - `JRDB_AUTO_INGESTION_PASSWORD`
2. local auth config
   - 例: `.local/jrdb_auto_ingestion_auth.toml`

repo には入れない。

## Trigger Manifest Example

```json
{
  "trigger_kind": "manual_fixture",
  "message_id": "fixture-message-001",
  "detected_at": "2026-04-21T11:00:00+09:00",
  "archives": [
    {
      "name": "PACI250223.zip",
      "source_uri": "/absolute/path/to/PACI250223.zip"
    }
  ],
  "handoff": {
    "mode": "forward_pre_race_tyb_oz_v1",
    "ingest_ready_files": true,
    "unit_id": "20260426_example_meeting",
    "dataset_path": "/absolute/path/to/horse_dataset_odds_only_is_place.parquet",
    "duckdb_path": "/absolute/path/to/jrdb.duckdb",
    "model_version": "odds_only_logreg_is_place@20260426_example_meeting",
    "settled_as_of": "2026-04-26T18:00:00+09:00",
    "input_source_name": "jrdb_tyb_oz_official",
    "input_source_url": "https://example.invalid/jrdb/tyb-oz",
    "input_source_timestamp": "2026-04-26T15:38:00+09:00",
    "odds_observation_timestamp": "2026-04-26T15:38:00+09:00",
    "popularity_input_source": "jrdb_tyb_oz_official"
  }
}
```

`mode = "none"` にすると ready 化と result-side ingest だけ行う。
`mode = "forward_pre_race_contract_like_csv_v1"` は従来どおり sanctioned local contract-like CSV を source にする開発/fixture path。
`mode = "forward_pre_race_oz_v1"` は `OZ` basis-odds だけを使う既存 path。
`mode = "forward_pre_race_tyb_oz_v1"` は `TYB` の current horse-level market と `OZ` の `place_basis_odds` を join する sanctioned mainline live path。

## CLI

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.jrdb_ingestion.cli \
  --trigger-manifest /absolute/path/to/trigger.json
```

optional:

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.jrdb_ingestion.cli \
  --trigger-manifest /absolute/path/to/trigger.json \
  --workspace-root data/local/jrdb_auto_ingestion \
  --raw-dir data/raw/jrdb \
  --auth-config .local/jrdb_auto_ingestion_auth.toml
```

## Expected Outputs

local orchestration:

- `data/local/jrdb_auto_ingestion/incoming/`
- `data/local/jrdb_auto_ingestion/staging/`
- `data/local/jrdb_auto_ingestion/ready/<job_id>/`
- `data/local/jrdb_auto_ingestion/state/`

official raw placement:

- `data/raw/jrdb/<job_id>/`

optional Phase 1 artifacts:

- `data/forward_test/runs/<unit_id>/raw/`
- `data/forward_test/runs/<unit_id>/contract/`
- `data/artifacts/place_forward_test/<unit_id>/pre_race/`

## Minimal Checks

1. `state/processed/<dedupe_key>.json` ができている
2. duplicate trigger で `duplicate_skipped` になる
3. `ready/<job_id>/ready_manifest.json` がある
4. `data/raw/jrdb/<job_id>/` に extracted file がある
5. pre-race handoff mode のとき
   - `notes/unit_metadata_manifest.json`
   - `raw/raw_snapshot_intake_manifest.json`
   - `contract/input_snapshot_<unit_id>.csv`
   - `pre_race/run_manifest.json`
   がある

## Failure Reading

最初に見る場所:

- `data/local/jrdb_auto_ingestion/state/runs/<job_id>.json`

典型例:

- `hash_mismatch`
  - download 完了判定失敗。ready 不成立
- `unsupported_archive_kind`
  - extractor が未対応。ready 不成立
- `extractor_not_available`
  - lzh extractor が local に無い。ready 不成立
- `forward_handoff_failed`
  - existing Phase 1 起動で止まった。report の `failure_detail` を見る

## Operational Notes

- production intent は email-triggered local run
- ただし mailbox spec が未固定なので、MVP では manual / fixture trigger も正規の開発入口として持つ
- sanctioned mainline live/current pre-race adapter は当面 `TYB*.txt + OZ*.txt`
- `OZ*.txt` 単独 adapter は basis-odds only path として残す
- `popularity` は unresolved auxiliary のまま使わない
- `SED` / `SRB` / `JOA` は current mainline pre-race adapter scope 外
- fixture smoke では `forward_pre_race_tyb_oz_v1` / `forward_pre_race_oz_v1` / `forward_pre_race_contract_like_csv_v1` を使って existing Phase 1 handoff を検証できる
