# Feature Contract v1

## Purpose

この文書は、`train / eval / backtest parity` のために、feature の入力契約を v1 として固定するための spec です。

実装面では、registry と config-load validation が repo 内に追加されており、`dataset_feature_set` と
`model_feature_columns` の境界は code path でもこの v1 に従って検証されます。

artifact provenance も v1 に紐づけて保存され、少なくとも dataset / prediction / backtest / live proxy の
一部 artifact では `feature_contract_version` と feature interpretation を追跡できます。

今回の目的は model 改善ではなく、比較基盤の固定です。したがって本書は、

- feature の source
- feature の availability timing
- `train / eval / backtest` での利用可否
- leakage prohibition
- missing handling

を feature 単位で明文化します。

## Scope

v1 の対象は、現在 repo で dataset build と model/backtest rolling retrain に現れている horse-level feature です。

対象外:

- BET rule の変更
- model family の改善
- pair/wide research 固有の score engineering
- post-race outcome や payout を feature 化すること

## Contract Layers

feature parity は v1 では次の 2 層で定義します。

1. `dataset_feature_set`
   - dataset builder が upstream staging からどの feature 列を materialize してよいかを決める
2. `model_feature_columns`
   - train / rolling retrain / live proxy がどの列をどの順序で数値入力として使うかを決める

v1 では `model_feature_columns` は必ず dataset schema 上の feature 列の subset でなければならず、
列順も train / rolling retrain / live proxy で一致しなければならない。

## Parity Definitions

### Train

- dataset parquet の feature 列をそのまま学習入力として使う
- `feature_columns` と `feature_transforms` は順序まで含めて契約の一部

### Eval

- v1 の `eval` は model 予測評価と rolling window 評価を指す
- static eval は `predictions.csv` を読むため direct feature input は持たない
- ただし predictions の provenance は同一 `dataset_feature_set + feature_columns + feature_transforms` に固定されるべき

### Backtest

- static backtest は `predictions.csv` 消費なので direct feature input は持たない
- `rolling_retrain` backtest は dataset を再読込して再学習するため、train と同じ feature contract を守る必要がある

### Live Proxy

- live proxy は parity 補助であり mainline parity source ではない
- 現在は `place_basis_odds` のみ alias/proxy を明示して扱う
- runbook にある通り、live proxy 出力を mainline backtest と横並び比較してはいけない

## Global Rules

### Feature Eligibility

- pre-race only feature だけを model input に使ってよい
- post-race outcome / payout / result confirmation列は feature にしてはいけない
- upstream raw field と project-derived feature を区別して記録する
- market-derived feature は market proxy であることを明示する

### Timing Rules

- feature の `availability timing` は「その値を正当に意思決定に使える最も早い時点」で判定する
- ただし v1 では `historical carrier timing` も別途意識する
- 今回の整理では `win_odds` だけを先行して pre-race carrier contract へ移し、`popularity` は未確認のため `legacy_sed_only_non_mainline` として残す
- popularity carrier decision は `unresolved_keep_legacy_for_non-mainline_only` とする

### Leakage Prohibition

- `finish_position`, `result_date`, `win_payout`, `place_payout_*` を feature に流してはいけない
- target source column は dataset schema に出してはいけない
- 同一 `race_key` は split を跨いではいけない
- `train / valid / test` は `race_date` 時系列でのみ分ける
- derived feature も input source に post-race 列を含んではいけない
- live input には `target_value`, `split`, `pred_probability`, `finish_position`, `payout` など leakage 列を含めてはいけない

### Missing Handling

- v1 では missing を「暗黙補完しない」が原則
- upstream null / parse failure / zero-division は `NULL` のまま dataset に出る
- source of truth は `horse_bet_lab.features.registry.FEATURE_MISSING_NULL_POLICY_REGISTRY` とする
- `dataset で NULL を持ってよい` と `model path で NULL を持ってよい` は別判定にする
- current v1 numeric model path では hidden imputation / silent coercion / silent row drop を禁止する
- model 側は train / rolling_retrain / live proxy の各 path で unsupported null pattern を clear error で止める
- missing strategy を導入する場合は feature contract v2 で明示する

### Missing/Null Enforcement Boundary

- dataset layer:
  - feature ごとの upstream null / parse failure / derivation failure は registry に定義した条件の範囲で `NULL` のまま保持してよい
  - これは「値が未確定または作れなかった」という記録であり、補完完了を意味しない
- current model parity path:
  - `win_odds`, `popularity`, `place_basis_odds`, `headcount`, `distance_m`, `workout_gap_days`, `workout_weekday_code`, `place_slot_count`, `log_place_minus_log_win`, `implied_place_prob`, `implied_win_prob`, `implied_place_prob_minus_implied_win_prob`, `place_to_win_ratio` は current v1 では non-null required
  - つまり dataset では nullable でも、train / rolling_retrain / live proxy の numeric input として読む時点では `NULL` を許可しない
  - failure 時は feature 名と row identifier を含む human-readable error にする
- dataset-only features:
  - `race_name`, `workout_weekday`, `workout_date` は dataset では nullable でもよい
  - ただし current v1 model parity path には載せない
- result-side features:
  - post-race/result-side feature は null policy の前に feature eligibility で禁止する

## Feature Inventory

| feature | class | source | availability timing | train | eval | backtest | leakage risk | missing handling | notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `win_odds` | upstream raw, market-derived | `jrdb_win_market_snapshot_v1.win_odds` | pre-race | allow | allow via provenance | allow | low-to-medium | carrier missing -> `NULL` | current carrier row is project-owned and populated from `OZ.win_basis_odds`; provenance carrier identity is `win_market_snapshot_v1` |
| `popularity` | upstream raw, market-derived | `SED.popularity` | pre-race semantic, post-race carrier in current offline build | allow for legacy/research only | allow via provenance | allow for legacy/research only | medium | parse failure -> `NULL` | ordinal rank。pre-race carrier is not yet confirmed; provenance carrier identity is `legacy_sed_only_non_mainline` |
| `place_basis_odds` | upstream raw, market-derived | `OZ.place_basis_odds` | pre-race | allow | allow via provenance | allow | low | parse failure -> `NULL` | live proxy では `place_basis_odds_proxy` alias を使う |
| `headcount` | upstream raw, market-derived support field | `OZ.headcount` | pre-race | allow | allow via provenance | allow | low | parse failure -> `NULL` | provisional semantics。place market array header 由来 |
| `distance_m` | upstream raw | `BAC.distance_m` | pre-race | allow | allow via provenance | allow | low | upstream null -> `NULL` | model parity では numeric feature としてのみ使用可 |
| `race_name` | upstream raw text | `BAC.race_name` | pre-race | deny in current parity path | not used directly | deny in rolling parity | low | upstream null -> `NULL` | dataset には出るが current model contract には入れない |
| `workout_weekday` | upstream raw text | `CHA.workout_weekday` | pre-race | deny in current parity path | not used directly | deny in rolling parity | low | upstream null -> `NULL` | raw text のままでは model input にしない |
| `workout_date` | upstream raw date | `CHA.workout_date` | pre-race | deny in current parity path | not used directly | deny in rolling parity | low | upstream null -> `NULL` | raw date のままでは model input にしない |
| `workout_gap_days` | project-derived | `BAC.race_date - CHA.workout_date` | pre-race | allow | allow via provenance | allow | low | source null -> `NULL` | race date 基準の derived feature |
| `workout_weekday_code` | project-derived | mapping from `CHA.workout_weekday` | pre-race | allow | allow via provenance | allow | low | unmapped/null -> `NULL` | `月..日 -> 0..6` |
| `place_slot_count` | project-derived | derived from `OZ.headcount` | pre-race | allow | allow via provenance | allow | low | `headcount NULL` -> `NULL` | `<=7 => 2 else 3` |
| `log_place_minus_log_win` | project-derived, market-derived | `LN(place_basis_odds) - LN(win_odds)` | pre-race semantic | allow | allow via provenance | allow | medium | invalid log input -> `NULL` | current source mixes `OZ` + `SED` carrier |
| `implied_place_prob` | project-derived, market-derived | `1 / place_basis_odds` | pre-race | allow | allow via provenance | allow | low | zero/null -> `NULL` | current transform は `identity` のみ |
| `implied_win_prob` | project-derived, market-derived | `1 / win_odds` | pre-race semantic | allow | allow via provenance | allow | medium | zero/null -> `NULL` | current source carrier は `SED` |
| `implied_place_prob_minus_implied_win_prob` | project-derived, market-derived | `implied_place_prob - implied_win_prob` | pre-race semantic | allow | allow via provenance | allow | medium | source null -> `NULL` | dual-market relation feature |
| `place_to_win_ratio` | project-derived, market-derived | `place_basis_odds / win_odds` | pre-race semantic | allow | allow via provenance | allow | medium | zero/null -> `NULL` | dual-market relation feature |

## Feature Classes

### Upstream Raw Pre-Race Fields

- `BAC.distance_m`
- `BAC.race_name`
- `CHA.workout_weekday`
- `CHA.workout_date`
- `SED.popularity`
- `jrdb_win_market_snapshot_v1.win_odds`
- `OZ.place_basis_odds`
- `OZ.headcount`

### Project-Derived Pre-Race Features

- `workout_gap_days`
- `workout_weekday_code`
- `place_slot_count`
- `log_place_minus_log_win`
- `implied_place_prob`
- `implied_win_prob`
- `implied_place_prob_minus_implied_win_prob`
- `place_to_win_ratio`

### Pre-Race Only vs Post-Race

pre-race only:

- 上表の feature はすべて semantic には pre-race only として扱う

post-race or result-only:

- `finish_position`
- `result_date`
- `win_horse_number`
- `win_payout`
- `place_horse_number_*`
- `place_payout_*`
- any label/target column such as `target_value`

これらは feature contract v1 では全面禁止。

### Market-Derived

- `win_odds`
- `popularity`
- `place_basis_odds`
- `headcount`
- `place_slot_count`
- `log_place_minus_log_win`
- `implied_place_prob`
- `implied_win_prob`
- `implied_place_prob_minus_implied_win_prob`
- `place_to_win_ratio`

### Non-Market Structural / Workout

- `distance_m`
- `race_name`
- `workout_weekday`
- `workout_date`
- `workout_gap_days`
- `workout_weekday_code`

## Current Parity Status

### Fully Parity-Safe In Current Numeric Model Path

- `win_odds`
- `place_basis_odds`
- `headcount`
- `distance_m`
- `workout_gap_days`
- `workout_weekday_code`
- `place_slot_count`
- `log_place_minus_log_win`
- `implied_place_prob`
- `implied_win_prob`
- `implied_place_prob_minus_implied_win_prob`
- `place_to_win_ratio`

意味:

- dataset build で列化されている
- `validate_model_feature_spec(...)` が train/rolling_retrain 入力として受理する
- backtest rolling retrain でも同じ列順契約を持てる

### Technically Supported But Non-Mainline

- `popularity`

意味:

- 実装上は dataset / train / rolling_retrain / provenance で引き続き扱える
- ただし carrier decision は `unresolved_keep_legacy_for_non-mainline_only`
- mainline confirmed feature としては扱わない

### Dataset-Only Or Non-Parity-Safe In Current Path

- `race_name`
- `workout_weekday`
- `workout_date`

意味:

- dataset には出せる
- ただし current model/backtest parity path の numeric input 契約には入っていない
- これらを使う場合は encoding と missing policy を先に固定する必要がある

## Approved Feature Sets In v1

`dataset_feature_set` と `model_feature_columns` の対応は、少なくとも以下の範囲に固定する。

| dataset_feature_set | intended model_feature_columns |
| --- | --- |
| `odds_only` | `win_odds` |
| `odds_only` + `include_popularity=true` | `win_odds`, `popularity` |
| `win_market_only` | `win_odds`, `popularity` |
| `current_win_market` | `win_odds`, `popularity` |
| `place_market_only` | `place_basis_odds` |
| `place_market_plus_popularity` | `place_basis_odds`, `popularity` |
| `dual_market` | `win_odds`, `place_basis_odds`, `popularity` |
| `dual_market_for_win` | `win_odds`, `place_basis_odds`, `popularity` |
| `dual_market_plus_headcount` | `win_odds`, `place_basis_odds`, `popularity`, `headcount` |
| `dual_market_plus_headcount_place_slots` | `win_odds`, `place_basis_odds`, `popularity`, `headcount`, `place_slot_count` |
| `dual_market_plus_headcount_place_slots_distance` | `win_odds`, `place_basis_odds`, `popularity`, `headcount`, `place_slot_count`, `distance_m` |
| `dual_market_plus_log_diff` | `win_odds`, `place_basis_odds`, `popularity`, `log_place_minus_log_win` |
| `dual_market_plus_implied_probs` | `win_odds`, `place_basis_odds`, `popularity`, `implied_place_prob`, `implied_win_prob` |
| `dual_market_plus_prob_diff` | `win_odds`, `place_basis_odds`, `popularity`, `implied_place_prob_minus_implied_win_prob` |
| `dual_market_plus_ratio` | `win_odds`, `place_basis_odds`, `popularity`, `place_to_win_ratio` |
| `market_plus_workout_minimal` | `win_odds`, `popularity`, `workout_gap_days`, `workout_weekday_code` |

`minimal` は dataset exploration 用に残すが、current parity-safe numeric model path の feature contract には含めない。

## Timestamp And Timing Assumptions

- split 判定は `race_date` 基準で行う
- `workout_gap_days` は `race_date - workout_date` で算出する
- `place_slot_count` は race day 前に既知の `headcount` を前提にする
- `win_odds` / `popularity` / `place_basis_odds` は race start 前に観測される market snapshot を意味する
- ただし historical build では `win_odds` / `popularity` は `SED` result carrier を経由しており、strict source parity は未達

## Source Confirmation Freeze Status

判定日時:

- 2026-04-19

判定:

- `popularity` source status: `unresolved`
- `win_odds` source status: `confirmed_pre_race_proxy_exists_but_not_adopted_as_sed_replacement`
- carrier relationship: `unresolved`

confirmed facts:

- JRDB `SED` official spec では `確定単勝オッズ` と `確定単勝人気順位` が同一 result-side file に存在する
- repo の `jrdb_sed_staging` も `win_odds` と `popularity` をこの carrier から取り込んでいる
- repo の `jrdb_oz_staging` / OZ parser は pre-race odds array を `win_basis_odds` / `place_basis_odds` に展開している
- 現在の repo 内根拠では、OZ 側に `popularity` を upstream-formalized field として確認できていない

freeze decision:

- `jrdb_win_market_snapshot_v1` は `win_odds + popularity` の single contract としてはまだ凍結しない
- `OZ.win_basis_odds` は legacy `SED.win_odds` の drop-in replacement としては採用しない
  - carrier migration evaluation の recommendation は `rollback`
  - discrepancy audit の recommendation は `hold_and_deeper_source_audit`
  - 最有力な理由は broad join collapse ではなく、semantic mismatch に timing mismatch 成分が重なっている可能性が高いこと
- 実装前の contract 表現は次で保留する
  - `win_odds`: pre-race market proxy candidate は存在するが、mainline replacement としては未採用
  - `popularity`: true upstream pre-race carrier 未確認
  - `jrdb_win_market_snapshot_v1`: `popularity` は provisional/optional candidate として扱い、required column にしない

blocked reason:

- `popularity` を `win_odds` と同一の pre-race carrier で取得できる根拠が repo 内では未確認
- `OZ.win_basis_odds` は `SED.win_odds` と比べて drift が極めて大きく、source audit でも broad join collapse より semantic/timing mismatch が有力

## Leakage Rules By Feature Class

### Rule A: Result-Derived Inputs Forbidden

禁止:

- `finish_position`
- `result_date`
- `win_payout`
- `place_payout_*`
- any outcome-derived aggregation

### Rule B: Semantic Pre-Race But Post-Race Carrier Requires Care

対象:

- `win_odds`
- `popularity`
- `implied_win_prob`
- `log_place_minus_log_win`
- `implied_place_prob_minus_implied_win_prob`
- `place_to_win_ratio`

ルール:

- v1 では offline comparison には使ってよい
- ただし `SED` carrier を理由に live parity completed と見なしてはいけない
- live 比較では必ず proxy / alias / caveat を残す

### Rule C: Derived Feature Must Inherit Strictest Source Rule

- derived feature の leakage risk は source の最大値を継承する
- `OZ` と `SED` を混ぜる derived feature は `SED` 側の carrier risk を引き継ぐ

### Rule D: Config Must Not Invent New Features

- `feature_columns` に指定できるのは dataset schema 上の既存列だけ
- docs に未登録の feature を config だけで追加してはいけない
- 新 feature は source / timing / missing / leakage を先に spec 化する

## Follow-Up TODO

1. feature registry をコード化し、`dataset_feature_set` と `feature_columns` の整合を config load 時に検証する
2. low-correlation race tail を対象に raw upstream line を監査し、`OZ.win_basis_odds` の snapshot semantics を狭く確認する
3. `popularity` の true upstream pre-race carrier を確認する
4. dataset artifact に `feature_contract_version` と feature provenance manifest を同梱する
5. `minimal` feature set の text/date feature を parity path に入れるか、exploration-only として固定するかを決める
6. live proxy contract を feature registry 参照型にして、`place_basis_odds_proxy` 以外の alias も管理可能にする

## Non-Goals For This Issue

- feature 自体の追加
- BET logic の変更
- model 改善実装
- large-scale training pipeline refactor
