# Place Forward-Test Phase 1 Runbook

## Purpose

この runbook は、place-only forward-test runner を手元で迷わず回すための最小運用手順を固定するものです。

対象は `horse_bet_lab.forward_test.runner` の Phase 1 path です。runner 本体のロジックはここでは変更しません。

繰り返し運用の cadence / checklist / naming rule は [docs/runbooks/place_forward_test_phase1_recurring_rehearsal.md](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/runbooks/place_forward_test_phase1_recurring_rehearsal.md:1) を参照してください。

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
- snapshot bridge sample config:
  - [configs/place_forward_snapshot_bridge.sample.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_snapshot_bridge.sample.toml)
- runtime config example:
  - [configs/place_forward_test_phase1_mainline.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_phase1_mainline.toml)
- multi-race dry-run config example:
  - [configs/place_forward_test_phase1_mainline_multi_race.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_phase1_mainline_multi_race.toml)
- end-to-end operator rehearsal example:
  - [configs/place_forward_test_phase1_mainline_example.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_phase1_mainline_example.toml)
  - [configs/place_forward_test_reconciliation_mainline_example.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_reconciliation_mainline_example.toml)
  - [data/forward_test/place_phase1_example/input_snapshot_satsuki_sho_2025_04_20.csv](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/data/forward_test/place_phase1_example/input_snapshot_satsuki_sho_2025_04_20.csv)
- bridge-based multi-source operator rehearsal example:
  - [configs/place_forward_snapshot_bridge_mainline_example.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_snapshot_bridge_mainline_example.toml)
  - [configs/place_forward_test_phase1_bridge_rehearsal.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_phase1_bridge_rehearsal.toml)
  - [configs/place_forward_test_reconciliation_bridge_rehearsal.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_reconciliation_bridge_rehearsal.toml)
  - [raw_snapshot_manual_failures_2026.csv](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/data/forward_test/place_phase1_bridge_example/raw_snapshot_manual_failures_2026.csv)
- reconciliation sample config:
  - [configs/place_forward_test_reconciliation.sample.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_forward_test_reconciliation.sample.toml)
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

## Snapshot Bridge Input Rules

bridge input は raw/live-ish CSV を想定します。最低限必要なのは次です。

- `race_key`
- `horse_number`
- `win_odds`
- `place_basis_odds_proxy` か `place_odds_min` / `place_odds_max`

optional:

- `popularity`
- `snapshot_status`
- `retry_count`
- `timeout_seconds`
- `snapshot_failure_reason`

bridge の明示ルール:

- `race_key` は silent fix しない
- 8桁数字でない `race_key` は明示エラーで止める
- `snapshot_status` が空なら、required odds が揃っている行だけ `ok`、欠けている行は `required_odds_missing` に落とす
- `snapshot_status=ok` でも required odds が欠けていれば、hidden fallback ではなく `required_odds_missing` に落とす
- `popularity` がある場合でも `unresolved_auxiliary` のまま contract CSV に書く

bridge output:

- generated contract CSV
- generated contract JSON copy
- bridge manifest
- bridge summary text

bridge 前に recurring operator path の raw intake を固定したい場合は、`raw_snapshot_intake_manifest.json` と precheck helper を使って次を確認する:

- raw snapshot file present
- expected raw columns
- source family / source name
- source URL
- input source timestamp
- odds observation timestamp

recurring operator path で metadata entry を 1 箇所に寄せたい場合は、`notes/unit_metadata_manifest.json` を source of truth にして、bridge / pre-race / reconciliation config を再生成する。

## How To Run

1. raw/live-ish snapshot CSV を用意する
2. recurring operator path では、必要なら raw intake manifest を用意して precheck を通す
3. recurring operator path では、metadata を見直すときは最初に `notes/unit_metadata_manifest.json` を開く
4. metadata を更新した場合は scaffold sync で bridge / pre-race / reconciliation config を再生成する

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.raw_snapshot_intake_cli --bridge-config configs/place_forward_snapshot_bridge.sample.toml
```

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.scaffold_cli sync \
  --metadata-manifest-path data/forward_test/runs/<unit_id>/notes/unit_metadata_manifest.json \
  --force
```

5. bridge sample config をコピーして、少なくとも次を自分の環境に合わせて置き換える
   - `sources[].path`
   - `sources[].odds_observation_timestamp`
   - `sources[].input_source_name`
   - `output_path`
6. bridge を実行して contract CSV を生成する

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.snapshot_bridge_cli --config configs/place_forward_snapshot_bridge.sample.toml
```

7. runner config をコピーして、少なくとも次を自分の環境に合わせて置き換える
   - `input_path`
   - `output_dir`
   - `reference_model.dataset_path`
   - `reference_model.model_version`
8. current mainline baseline を維持したい場合は、bet logic の次を変えない
   - `candidate_logic_id = "guard_0_01_plus_proxy_domain_overlay"`
   - `fallback_logic_id = "no_bet_guard_stronger"`
   - `stronger_guard_edge_surcharge = 0.01`
9. pre-race runner を実行する

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.cli --config configs/place_forward_test_phase1.sample.toml
```

multi-race dry-run の例:

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.cli --config configs/place_forward_test_phase1_mainline_multi_race.toml
```

bridge-based multi-source operator rehearsal 例:

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.snapshot_bridge_cli --config configs/place_forward_snapshot_bridge_mainline_example.toml
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.cli --config configs/place_forward_test_phase1_bridge_rehearsal.toml
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.reconciliation_cli --config configs/place_forward_test_reconciliation_bridge_rehearsal.toml
```

この rehearsal は 2025 settled race と 2026 unsettled race をまたいで、bridge -> contract CSV -> runner -> reconciliation の一本線を確認するための例です。

## Minimal Phase 1 Operation Flow

1. raw/live-ish snapshot CSV を確認する
2. raw intake manifest / precheck で bridge 前確認を行う
3. bridge を実行して contract CSV を生成する
4. generated contract CSV と bridge manifest を確認する
5. pre-race runner を実行する
6. `bet_decision_records.csv` と `run_manifest.json` を確認する
7. レース結果が確定したら reconciliation config を用意する
8. result DB availability check を実行する
9. `result_availability_check.txt` / `.json` を見て `should_settle` か `expected_pending_or_stale_db` かを確認する
10. `expected_pending_or_stale_db` のときは `DB freshness summary` を見て、`result_side_freshness_vs_settled_as_of` と `operator_freshness_hint` を先に読む
11. reconciliation を実行する
12. `reconciled_records.csv` と `reconciliation_summary.json` を確認する

operator rehearsal で最短確認したい点:

1. `bet_decision_records.csv` に `bet` と `logic_filtered` が両方出ているか
2. `reconciled_records.csv` に `settled_hit`, `settled_miss`, `settled_no_bet` が分かれているか
3. `reconciliation_summary.json` の `settled_bets`, `hit_count`, `total_simulated_profit_loss` が読めるか
4. provenance が `run_manifest.json` と `reconciliation_manifest.json` に残っているか

pre-race mainline 例:

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.cli --config configs/place_forward_test_phase1_mainline.toml
```

post-race reconciliation 例:

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.result_db_availability_cli --config configs/place_forward_test_reconciliation.sample.toml
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.reconciliation_cli --config configs/place_forward_test_reconciliation.sample.toml
```

availability check の読み方:

- `result_side_freshness_vs_settled_as_of = result_side_behind_settled_as_of`
  - local DuckDB が今回の reconciliation window に追いついていない可能性を先に疑う
- `result_side_freshness_vs_settled_as_of = result_side_covers_settled_as_of`
  - DB 全体は window をカバーしているので、target race missing / partial loading を疑う
- `payout_side_freshness_vs_settled_as_of = payout_side_behind_settled_as_of`
  - finish result は見えていても payout side の反映待ちの可能性がある

end-to-end operator rehearsal 例:

```bash
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.cli --config configs/place_forward_test_phase1_mainline_example.toml
PYTHONPATH=src .venv/bin/python -m horse_bet_lab.forward_test.reconciliation_cli --config configs/place_forward_test_reconciliation_mainline_example.toml
```

この example は local にある 2025-04-20 皐月賞 snapshot を contract CSV 化したものです。Phase 1 の最小 operator rehearsal として、pre-race で `bet` が出て、そのまま result-side reconciliation まで通せます。

reconciliation config で最低限埋める項目:

- `forward_output_dir`
  - pre-race run の `output_dir`
- `duckdb_path`
  - `jrdb_sed_staging` と `jrdb_hjc_staging` が入った result-side source
- `output_dir`
  - reconciliation artifacts の出力先
- `settled_as_of`
  - operator が「どの時点まで結果確認済みか」を残したいときの任意メタデータ

result DB availability check で最初に見たいもの:

- `result_availability_check.txt`
- `result_availability_check.json`

最短判断:

- `should_settle`
  - 対象 race の finish result と payout side が揃って見えているので、そのまま reconciliation を回してよい
- `expected_pending_or_stale_db`
  - まだ結果が入っていない想定か、DuckDB が古い可能性がある
- `result_db_partial_results`
  - 一部 race だけ見えているので、まだ待つ方が安全
- `result_db_incomplete_payout_side`
  - finish result は見えているが payout side が不完全なので、settlement 判断にはまだ早い

## What Appears Under output_dir

bridge output の例:

- `input_snapshot_*.csv`
- `input_snapshot_*.json`
- `input_snapshot_*.manifest.json`
- `input_snapshot_*.summary.txt`

- `input_snapshot_records.csv`
- `input_snapshot_records.json`
- `prediction_output_records.csv`
- `prediction_output_records.json`
- `bet_decision_records.csv`
- `bet_decision_records.json`
- `run_manifest.json`
- `run_manifest.json.provenance.json`
- `summary.txt`

reconciliation output の例:

- `reconciled_records.csv`
- `reconciled_records.json`
- `reconciliation_summary.json`
- `reconciliation_summary.txt`
- `reconciliation_manifest.json`

## Which Artifact To Read

- bridge で contract 化が通ったかを見たい:
  - generated contract CSV
  - generated bridge manifest
  - generated bridge summary text
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

reconciliation 後に結果と突合したい:

- `reconciled_records.csv`
  - `bet_action`
  - `reconciliation_status`
  - `place_hit`
  - `official_place_payout`
  - `simulated_profit_loss`
- `reconciliation_summary.json`
  - `settled_bets`
  - `unsettled_bets`
  - `hit_count`
  - `total_simulated_payout`
  - `total_simulated_profit_loss`
  - `no_bet_reason_counts`

## Reconciliation Status Reading Guide

- `settled_hit`
  - 当時 `bet` だった行に結果が付き、複勝的中として payout が確定した
- `settled_miss`
  - 当時 `bet` だった行に結果が付き、複勝不的中として損益が確定した
- `settled_no_bet`
  - 結果は確定したが、当時の decision は `no_bet` だった
- `unsettled_result_pending`
  - result-side source にまだ対象 `race_key + horse_number` が見つからず、結果未確定のまま残っている
- `unsettled_result_incomplete`
  - result-side row はあるが、finish/payout のどちらかが欠けていて settlement を確定できない

見分け方の最短ルール:

- pre-race の理由を見る:
  - `bet_decision_records.csv` の `decision_reason` と `no_bet_reason`
- post-race の確定状態を見る:
  - `reconciled_records.csv` の `reconciliation_status`
- snapshot failure 系と logic-filter 系を分ける:
  - `no_bet_reason=logic_filtered` は logic 側
  - `timeout`, `required_odds_missing`, `snapshot_failure`, `retry_exhausted` は snapshot failure 側

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

pre-race:

- raw/live-ish snapshot CSV の `race_key` が 8桁数字か確認する
- bridge config の `sources[].path` と timestamp/source metadata を確認する
- bridge 実行後に generated contract CSV と bridge manifest を確認する
- input CSV header が contract と一致している
- `odds_observation_timestamp` は timezone 付き ISO-8601
- `snapshot_status=ok` の行には `win_odds` と `place_basis_odds` がある
- `popularity` を入れたなら `unresolved_auxiliary` と source を一緒に残している
- pre-race run 後に `bet_decision_records.csv` と `run_manifest.json` を確認する

post-race:

- reconciliation config の `forward_output_dir` が pre-race run の `output_dir` を指している
- `duckdb_path` に `jrdb_sed_staging` と `jrdb_hjc_staging` が入っている
- reconciliation 後に `reconciled_records.csv` と `reconciliation_summary.json` を確認する
- `settled_*` と `unsettled_*` が想定どおり分かれているかを見る
