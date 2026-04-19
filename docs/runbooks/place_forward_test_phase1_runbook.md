# Place Forward-Test Phase 1 Runbook

## Purpose

この runbook は、place-only forward-test runner を手元で迷わず回すための最小運用手順を固定するものです。

対象は `horse_bet_lab.forward_test.runner` の Phase 1 path です。runner 本体のロジックはここでは変更しません。

## Scope

- 複勝一本
- current baseline は維持
  - current candidate: `guard_0_01_plus_proxy_domain_overlay`
  - status: `hard_adopt`
  - fallback: `no_bet_guard_stronger surcharge=0.01`
- `popularity` は unresolved auxiliary のまま扱う
- snapshot failure は silent skip ではなく explicit `no_bet`

## Required Files

- sample config:
  - [configs/place_forward_test_phase1.sample.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_phase1.sample.toml)
- runtime config example:
  - [configs/place_forward_test_phase1_mainline.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_phase1_mainline.toml)
- multi-race dry-run config example:
  - [configs/place_forward_test_phase1_mainline_multi_race.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_phase1_mainline_multi_race.toml)
- contract spec:
  - [docs/spec/place_forward_test_contract_v1.md](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/spec/place_forward_test_contract_v1.md)

## Minimal Input File Format

runner input は UTF-8 CSV です。1 行が 1 頭の live snapshot record です。

required columns:

- `race_key`
- `horse_number`
- `odds_observation_timestamp`
- `input_source_name`
- `carrier_identity`
- `snapshot_status`
- `retry_count`

conditionally required columns:

- `win_odds`
  - `snapshot_status=ok` のとき required
- `place_basis_odds`
  - `snapshot_status=ok` のとき required
- `snapshot_failure_reason`
  - `snapshot_status!=ok` のとき required
- `timeout_seconds`
  - timeout を実際に使った run では保持推奨

optional columns:

- `popularity`
- `popularity_input_source`
- `popularity_contract_status`
- `input_source_url`
- `input_source_timestamp`
- `input_schema_version`

### Input Policy Notes

- `popularity` は confirmed carrier ではありません
- `popularity` がある場合でも auxiliary field として保持するだけです
- `popularity` があるなら次も必須です
  - `popularity_input_source`
  - `popularity_contract_status=unresolved_auxiliary`
- `popularity` が無いことだけでは `no_bet` にしません
- standard Phase 1 path では、`popularity` unresolved を壊さないため、runtime config の model feature sequence も `popularity` 非依存に保つ
- `snapshot_status` は次のどれかです
  - `ok`
  - `snapshot_failure`
  - `timeout`
  - `retry_exhausted`
  - `required_odds_missing`

## Minimal Sample Input Header

```csv
race_key,horse_number,win_odds,place_basis_odds,popularity,odds_observation_timestamp,input_source_name,input_source_url,input_source_timestamp,carrier_identity,snapshot_status,retry_count,timeout_seconds,snapshot_failure_reason,popularity_input_source,popularity_contract_status,input_schema_version
```

## How To Run

1. sample config をコピーして、少なくとも次を自分の環境に合わせて置き換える
   - `input_path`
   - `output_dir`
   - `reference_model.dataset_path`
   - `reference_model.model_version`
2. current mainline baseline を維持したい場合は、bet logic の次を変えない
   - `candidate_logic_id = "guard_0_01_plus_proxy_domain_overlay"`
   - `fallback_logic_id = "no_bet_guard_stronger"`
   - `stronger_guard_edge_surcharge = 0.01`
3. 実行する

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.cli --config configs/place_forward_test_phase1.sample.toml
```

multi-race dry-run の例:

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.cli --config configs/place_forward_test_phase1_mainline_multi_race.toml
```

## What Appears Under output_dir

- `input_snapshot_records.csv`
- `input_snapshot_records.json`
- `prediction_output_records.csv`
- `prediction_output_records.json`
- `bet_decision_records.csv`
- `bet_decision_records.json`
- `run_manifest.json`
- `run_manifest.json.provenance.json`
- `summary.txt`

## Which Artifact To Read

- prediction を見たい:
  - `prediction_output_records.csv`
- bet / no-bet decision を見たい:
  - `bet_decision_records.csv`
- snapshot failure / timeout / retry exhaustion の reason を見たい:
  - `bet_decision_records.csv` の `bet_action`, `decision_reason`, `no_bet_reason`
- run 全体の provenance を見たい:
  - `run_manifest.json`
  - `run_manifest.json.provenance.json`
- race をまたいだ snapshot status の内訳を見たい:
  - `run_manifest.json` の `snapshot_status_counts`
- `logic_filtered` と snapshot failure 系を分けて見たい:
  - `bet_decision_records.csv` の `no_bet_reason`
  - `logic_filtered`, `timeout`, `required_odds_missing` を別カテゴリとして読む

## Minimal Artifact Schema Notes

### input_snapshot_records.csv

主に見る列:

- `race_key`
- `horse_number`
- `snapshot_status`
- `win_odds`
- `place_basis_odds`
- `popularity`
- `odds_observation_timestamp`
- `input_source_name`
- `carrier_identity`
- `snapshot_failure_reason`

### prediction_output_records.csv

主に見る列:

- `race_key`
- `horse_number`
- `prediction_probability`
- `model_version`
- `feature_contract_version`
- `carrier_identity`
- `odds_observation_timestamp`

### bet_decision_records.csv

主に見る列:

- `race_key`
- `horse_number`
- `bet_action`
- `decision_reason`
- `no_bet_reason`
- `baseline_logic_id`
- `fallback_logic_id`
- `model_version`
- `feature_contract_version`
- `carrier_identity`
- `odds_observation_timestamp`

## Operational Reading Rule

- valid `ok` snapshot だけが prediction に進みます
- `snapshot_failure` / `timeout` / `retry_exhausted` / `required_odds_missing` は prediction を進めず、explicit `no_bet` artifact を出します
- hidden fallback はありません
- baseline の読み方は [docs/mainline_reference_runbook.md](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/mainline_reference_runbook.md:1) を優先します

## Operator Checklist

- input CSV header が contract と一致している
- `odds_observation_timestamp` は timezone 付き ISO-8601
- `snapshot_status=ok` の行には `win_odds` と `place_basis_odds` がある
- `popularity` を入れたなら `unresolved_auxiliary` と source を一緒に残している
- output は `bet_decision_records.csv` と `run_manifest.json` まで確認する
