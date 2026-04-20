# Place Forward-Test Phase 1 Recurring Rehearsal

## Purpose

この文書は、place-only forward-test Phase 1 を単発 example ではなく、同じ手順で繰り返し回せる最小運用リハーサルとして固定するための cadence / checklist / naming rule をまとめるものです。

対象は current mainline baseline の複勝一本 path です。

- current candidate: `guard_0_01_plus_proxy_domain_overlay`
- status: `hard_adopt`
- fallback: `no_bet_guard_stronger surcharge=0.01`

## Recurring Cadence

### Race day pre-race run

- raw/live-ish snapshot を当日分として収集する
- bridge で contract CSV に変換する
- pre-race runner を回して prediction / decision artifact を保存する

### Result-confirmed post-race reconciliation

- result DB が更新済みかを確認する
- reconciliation を回して settled / unsettled を確定する
- 当日の run と reconciliation artifact を同じ rehearsal unit として残す

### Weekly review / archive

- その週の run units をざっと見直す
- `logic_filtered` と snapshot failure 系 reason の比率を確認する
- settled / unsettled の残りを確認し、必要なら archive note を残す
- result DB 未更新による `unsettled_result_pending` と operator 側の手順ミスを混同しない

## Recommended Directory And Naming Convention

rehearsal unit は `YYYYMMDD_meeting_slug` を基本単位にする。

例:

- unit id: `20250420_satsuki_sho`

推奨配置:

```text
data/forward_test/runs/<unit_id>/raw/
data/forward_test/runs/<unit_id>/contract/
data/artifacts/place_forward_test/<unit_id>/pre_race/
data/artifacts/place_forward_test/<unit_id>/reconciliation/
data/forward_test/runs/<unit_id>/notes/
```

役割:

- `raw/`
  - live-ish snapshot input を置く
- `contract/`
  - bridge で生成した contract CSV / JSON / manifest を置く
- `pre_race/`
  - runner output を置く
- `reconciliation/`
  - reconciliation output を置く
- `notes/`
  - operator memo や週次メモを置く

命名原則:

- bridge output:
  - `input_snapshot_<unit_id>.csv`
  - `input_snapshot_<unit_id>.json`
  - `input_snapshot_<unit_id>.manifest.json`
  - `input_snapshot_<unit_id>.summary.txt`
- pre-race output dir:
  - `data/artifacts/place_forward_test/<unit_id>/pre_race`
- reconciliation output dir:
  - `data/artifacts/place_forward_test/<unit_id>/reconciliation`
- optional note:
  - `data/forward_test/runs/<unit_id>/notes/operator_note.md`

## Checklist

### Pre-race checklist

1. 今日の rehearsal unit id を決める
2. raw snapshot CSV を `raw/` に置く
3. `configs/templates/place_forward_snapshot_bridge_runtime.template.toml` をコピーして source path と metadata を埋める
4. bridge を実行する
5. generated contract CSV と bridge manifest を確認する
6. `configs/templates/place_forward_test_phase1_runtime.template.toml` をコピーして `input_path` / `output_dir` / `dataset_path` / `model_version` を埋める
7. current baseline の bet logic identifiers が変わっていないことを確認する
8. pre-race runner を実行する
9. `bet_decision_records.csv` と `run_manifest.json` を確認する

### Post-race checklist

1. result DB が対象開催まで更新済みかを確認する
2. `configs/templates/place_forward_test_reconciliation_runtime.template.toml` をコピーして `forward_output_dir` / `duckdb_path` / `output_dir` / `settled_as_of` を埋める
3. reconciliation を実行する
4. `reconciled_records.csv` を確認する
5. `reconciliation_summary.json` を確認する
6. `settled_hit`, `settled_miss`, `settled_no_bet`, `unsettled_*` の件数を確認する

### Weekly / periodic checklist

1. その週の rehearsal units を列挙する
2. snapshot failure 系 reason が増えていないか確認する
3. `logic_filtered` が極端に偏っていないか確認する
4. unsettled のまま残っている unit を確認する
5. 次週も同じ命名規則と template で回せるか確認する

週次メモを残すときの雛形は [docs/runbooks/place_forward_test_phase1_weekly_review_template.md](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/runbooks/place_forward_test_phase1_weekly_review_template.md:1) を使う。

## Artifact Reading Guide

最初に見る場所:

- bridge 成功確認:
  - `contract/input_snapshot_<unit_id>.summary.txt`
  - `contract/input_snapshot_<unit_id>.manifest.json`
- pre-race 判断確認:
  - `pre_race/bet_decision_records.csv`
  - `pre_race/run_manifest.json`
- post-race 結果確認:
  - `reconciliation/reconciled_records.csv`
  - `reconciliation/reconciliation_summary.json`

読み分けの最短ルール:

- `logic_filtered`
  - contract は通って prediction まで進んだが、BET logic 条件で `no_bet`
- `timeout`, `required_odds_missing`, `snapshot_failure`, `retry_exhausted`
  - snapshot failure 系の `no_bet`
- `settled_hit`, `settled_miss`
  - 当時 `bet` だった行の post-race outcome
- `settled_no_bet`
  - 結果は出たが当時は `no_bet`
- `unsettled_result_pending`, `unsettled_result_incomplete`
  - まだ結果確認が閉じていない

## Template Files

- bridge runtime template:
  - [configs/templates/place_forward_snapshot_bridge_runtime.template.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/templates/place_forward_snapshot_bridge_runtime.template.toml)
- pre-race runtime template:
  - [configs/templates/place_forward_test_phase1_runtime.template.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/templates/place_forward_test_phase1_runtime.template.toml)
- reconciliation runtime template:
  - [configs/templates/place_forward_test_reconciliation_runtime.template.toml](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/templates/place_forward_test_reconciliation_runtime.template.toml)

## Boundaries

- BET logic は変えない
- baseline は変えない
- `popularity` は unresolved auxiliary のまま扱う
- frozen `win_odds` migration は再開しない
- 自動購入 / 外部連携は Phase 1 に含めない
