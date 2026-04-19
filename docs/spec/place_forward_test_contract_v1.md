# Place Forward-Test Contract v1

## Purpose

この文書は、place-only forward-test Phase 1 の input/output contract を固定するための spec です。

対象は roadmap にある最初の境界です。

1. input contract
2. odds snapshot
3. prediction
4. bet decision
5. artifact write
6. reconciliation

本書の source of truth は次の 2 つです。

- code:
  - `horse_bet_lab.forward_test.contracts`
- docs:
  - `docs/spec/place_forward_test_contract_v1.md`

## Non-goals

- BET logic の変更
- baseline の変更
- `popularity` unresolved の解決
- frozen `win_odds` migration の再開
- single-win / wide への拡張

## Contract Versions

- place forward-test contract version:
  - `v1`
- input schema version:
  - `place_forward_test_input_v1`
- output schema version:
  - `place_forward_test_output_v1`

## Input Contract

Phase 1 の標準 input record は horse-level row として扱う。

required fields:

- `race_key`
- `horse_number`
- `odds_observation_timestamp`
- `input_source_name`
- `carrier_identity`
- `snapshot_status`
- `retry_count`

conditionally required fields:

- `win_odds`
  - `snapshot_status = ok` のとき required
- `place_basis_odds`
  - `snapshot_status = ok` のとき required
- `snapshot_failure_reason`
  - `snapshot_status != ok` のとき required
- `timeout_seconds`
  - timeout policy を実行した run では保持する

optional fields:

- `popularity`
- `popularity_input_source`
- `popularity_contract_status`
- `input_source_url`
- `input_source_timestamp`

### Popularity Policy

- `popularity` は Phase 1 では required confirmed carrier ではない
- live input に存在しても `unresolved_auxiliary` として保持するだけで、confirmed upstream carrier とは扱わない
- `popularity` が無いことだけでは `no_bet` にしない
- `popularity` を入れる場合は provenance として次を残す
  - `popularity_input_source`
  - `popularity_contract_status=unresolved_auxiliary`

### Snapshot Status Values

- `ok`
- `snapshot_failure`
- `timeout`
- `retry_exhausted`
- `required_odds_missing`

解釈:

- hidden fallback は使わない
- `ok` 以外は snapshot failure class として明示的に残す

## Output / Artifact Contract

Phase 1 で最低限固定する record は次の 3 つ。

### Prediction Input Record

validated input row そのものを prediction input record として扱う。

理由:

- Phase 1 の first step では、prediction 前の live snapshot contract を先に固定する方が重要だから

### Prediction Output Record

required fields:

- `race_key`
- `horse_number`
- `prediction_probability`
- `model_version`
- `feature_contract_version`
- `carrier_identity`
- `odds_observation_timestamp`
- `input_schema_version`
- `output_schema_version`

### Bet Decision Record

required fields:

- `race_key`
- `horse_number`
- `bet_action`
- `decision_reason`
- `feature_contract_version`
- `model_version`
- `carrier_identity`
- `odds_observation_timestamp`
- `input_schema_version`

conditionally required fields:

- `no_bet_reason`
  - `bet_action = no_bet` のとき required

optional provenance fields:

- `baseline_logic_id`
- `fallback_logic_id`
- `run_manifest_hash`

## Provenance Minimum

forward-test artifacts は少なくとも次を残す。

- `feature_contract_version`
- `model_version`
- `carrier_identity`
- `odds_observation_timestamp`
- `decision_reason`

optional but recommended:

- `baseline_logic_id`
- `fallback_logic_id`
- `input_schema_version`
- `run_manifest_hash`

## Validation Rules

- `race_key` は 8-digit upstream race identifier
- `horse_number` は positive integer
- `odds_observation_timestamp` は timezone 付き ISO-8601
- `snapshot_status = ok` では `win_odds` / `place_basis_odds` が positive required
- `snapshot_status != ok` では `snapshot_failure_reason` が required
- `popularity` を持つ場合:
  - positive integer
  - `popularity_contract_status=unresolved_auxiliary`
  - `popularity_input_source` required
- `bet_action = no_bet` では human-readable `no_bet_reason` required

## Human-readable Failure Boundary

contract validation は human-readable error で止める。

例:

- invalid `race_key`
- missing `odds_observation_timestamp`
- `snapshot_status=ok` なのに required odds が missing
- `popularity` があるのに unresolved auxiliary として記録されていない
- `bet_action=no_bet` なのに reason が空
