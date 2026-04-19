# Place Forward Test Plan

## Purpose

この文書は、複勝一本の短期〜中期開発方針を repo 内の正本として固定するための roadmap です。
アイデア置き場ではなく、「今どこに向かうか」と「今は何を広げないか」を明示する文書として扱います。

既存の mainline reference / hard-adopt baseline と矛盾する判断は、この文書では行いません。

## Final Goal

- 年間の複勝自動運用を、再現可能な比較基盤の上で回せる状態にする

## Intermediate Goals

- 発走直前オッズ取得を、複勝 forward-test 用の入力として安定化する
- 購入シミュレーションを、mainline BET logic と切り離さずに回せるようにする
- フォワードテストを、比較可能な artifact と運用ログ付きで継続できるようにする

## Current Baseline

- current candidate:
  - `guard_0_01_plus_proxy_domain_overlay`
- status:
  - `hard_adopt`
- fallback:
  - `no_bet_guard_stronger surcharge=0.01`

この baseline は [docs/mainline_reference_runbook.md](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/mainline_reference_runbook.md:1) の読み方に従って固定済みです。

## Current Direction

- 複勝一本で進める
- モデルは差し替え可能に保つ
- BET logic は当面 mainline baseline を維持する

補足:

- ここでいう「差し替え可能」は、比較基盤と artifact 契約を壊さずに model family を入れ替えられることを意味します
- ここでいう「維持」は、BET logic の新規最適化よりも、forward-test 運用基盤の確立を優先するという意味です

## Implementation Phases

### 1. 複勝 forward-test 運用基盤

- 発走直前オッズの取り込み経路を固める
- 購入判断の入力 snapshot と出力 artifact を固定する
- 日次 / レース単位での forward-test 実行と保存を再現可能にする
- mainline baseline を comparison anchor として参照できる状態にする

Phase 1 の実装順は以下に固定する。

1. input contract
2. odds snapshot
3. prediction
4. bet decision
5. artifact write
6. reconciliation

Phase 1 では CSV schema 合意までを含める。WebUI 実装は Phase 3 に分離する。

Phase 1 で先に固定しておく acceptance / contract は次のとおり。

詳細 schema の source of truth は [docs/spec/place_forward_test_contract_v1.md](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/spec/place_forward_test_contract_v1.md:1) と `horse_bet_lab.forward_test.contracts` に置く。

- popularity live input policy:
  - `popularity` carrier は unresolved のまま扱う
  - forward-test では `popularity` を required input にしない
  - live input に `popularity` が無い場合でも、それだけを理由に補完や推定はしない
  - live input に `popularity` がある場合も、upstream-confirmed carrier ではなく operator-supplied auxiliary field として扱う
  - provenance には少なくとも以下を残す
    - `popularity_input_present`
    - `popularity_input_source`
    - `popularity_contract_status=unresolved_auxiliary`
  - `popularity` unavailable 時は no-bet reason にしない
    - ただし利用モデルが `popularity` を必須 feature とするなら、その model は Phase 1 の standard path に載せない

- odds snapshot failure/timing policy:
  - 発走直前オッズ取得は retry を持つ
  - retry 上限、timeout、最終取得失敗は artifact に残す
  - observation timestamp は必須で記録する
  - observation timestamp が無い snapshot は valid decision input とみなさない
  - timeout / retry exhaustion / required odds missing の場合は hidden fallback で進めず、明示的に `no_bet` に倒す
  - no-bet は BET logic の改善としてではなく、forward-test input failure handling として扱う
  - race ごとに少なくとも以下を保存する
    - `odds_observation_timestamp`
    - `odds_snapshot_status`
    - `retry_count`
    - `timeout_seconds`
    - `no_bet_due_to_snapshot_failure`

- forward-test artifact provenance requirements:
  - artifact には少なくとも以下を紐付ける
    - `feature_contract_version`
    - `model_version`
    - `carrier_identity`
    - `odds_observation_timestamp`
    - `decision_reason`
  - 必要なら以下も残す
    - `baseline_logic_id`
    - `fallback_logic_id`
    - `input_schema_version`
    - `run_manifest_hash`
  - provenance は「どの入力契約で、どのモデルが、どの観測時点の odds を使って、なぜその判断を出したか」を後から辿れることを acceptance とする

### 2. 資金運用ロジック

- 複勝一本の運用前提で bankroll management を整理する
- 実運用用の sizing / cap / stop 条件を comparison-friendly に管理する
- model score と betting instruction を分離したまま扱う

### 3. CSV / WebUI 出力

- forward-test の判断根拠を CSV で追えるようにする
- 最小の WebUI / view layer で確認できるようにする
- 運用者が「何を買うか」「なぜそうなったか」を迷わず読める形にする

### 4. 将来の自動実行アダプタ

- scheduler / runner / external adapter を将来追加できる境界を決める
- ただし本段階では、自動実行そのものよりも入力契約と出力契約を先に固める

## Non-Goals

- 単勝拡張を今は進めない
- ワイド再開を今は進めない
- frozen `win_odds` migration を再開しない

## Known Pending Items

- `popularity` carrier は unresolved のまま
- `OZ.win_basis_odds` replacement path は not adopted / frozen のまま
- narrow upstream-source follow-up は別件として扱う

## What This Roadmap Does Not Change

- current mainline baseline は書き換えない
- BET logic の threshold / guard / surcharge はこの roadmap では変更しない
- 新しい賭け種や research line を mainline に追加しない

## Reading Rule

- mainline baseline の正本は runbook を優先する
- この roadmap は、その baseline を前提に「次に何を作るか」を固定する
- 実装優先順位で迷ったら、複勝 forward-test 運用基盤に戻る
