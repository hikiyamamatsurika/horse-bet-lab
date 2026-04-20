# JRDB Staging Contract

この文書は、`raw -> staging` の取り込み結果を downstream がどう扱ってよいかを定義するための contract です。

この contract の主対象は、dataset builder が安全に参照してよい列、粒度、主キー候補、列の安定度です。現時点の dataset-builder scope は `BAC`、`CHA`、`SED` と、project-owned pre-race market carrier contract に限定します。

`OZ` / `HJC` も ingest では staging 化していますが、こちらは evaluation-side の参照用です。`OZ` は upstream raw odds array を project parser が staging に展開した pre-race market proxy source、`HJC` は upstream payout/result source として扱います。

ただし `win_odds` については、single contract freeze が blocked なままでも source parity を前進させるため、`OZ.win_basis_odds` を carrier 候補として `jrdb_pre_race_market_staging` に materialize し、stable contract として `jrdb_win_market_snapshot_v1` を追加しました。`popularity` は pre-race carrier 未確認のため、今回も `SED` legacy path に残します。

`jrdb_oz_staging` については、`horse_number` と array position の対応は repo 内で検証している parser convention であり、upstream-formalized な列定義としては扱いません。また `win_basis_odds` / `place_basis_odds` は repo 内で使う operational name であり、upstream の formal field name をそのまま写したものではありません。

## Status Definitions

- `confirmed`: 実サンプルと構造確認の両方が取れており、dataset builder が参照してよい
- `provisional`: 位置や値の形は安定しているが、意味づけが未確定。dataset builder での利用は allowlist に含まれる列だけに限定する
- `opaque`: traceability のために保持している未解読ブロック。dataset builder は参照しない

## Table Grain And Keys

`race_key` は upstream の race identifier として扱います。project 内では `race_key[:2]` を
upstream-defined `venue_code` として参照してよく、`JRA` / `Nankan` のような broader
domain/group は upstream enum ではなく project-owned derived mapping として扱います。

### `jrdb_bac_staging`

- grain: 1 row per race
- primary key candidate: `race_key`

### `jrdb_cha_staging`

- grain: 1 row per `race_key + horse_number`
- primary key candidate: `race_key`, `horse_number`

### `jrdb_sed_staging`

- grain: 1 row per `race_key + horse_number`
- primary key candidate: `race_key`, `horse_number`

### `jrdb_pre_race_market_staging`

- grain: 1 row per `race_key + horse_number`
- primary key candidate: `race_key`, `horse_number`
- role:
  - project-owned physical staging for pre-race market carrier rows
  - current scope is `win_odds` only

### `jrdb_win_market_snapshot_v1`

- grain: 1 row per `race_key + horse_number`
- required columns:
  - `race_key`
  - `horse_number`
  - `win_odds`
- metadata columns:
  - `market_snapshot_source`
  - `market_snapshot_date`
- role:
  - stable downstream contract for `win_odds`
  - `popularity` は含めない

## Dataset Builder Allowlist

dataset builder が参照してよい列は以下に限定します。

### BAC allowlist

- `race_key`
- `race_date`
- `post_time`
- `distance_m`
- `entry_count`
- `race_name`

### CHA allowlist

- `race_key`
- `horse_number`
- `workout_weekday`
- `workout_date`
- `workout_code`

### SED allowlist

- `popularity`

`SED` は主に target 生成用 staging として扱います。`popularity` は pre-race carrier 未確認のため、
当面は optional feature として `SED` legacy path からだけ参照します。

### Pre-race win market contract

- physical staging:
  - `jrdb_pre_race_market_staging`
- stable view:
  - `jrdb_win_market_snapshot_v1`
- current carrier mapping:
  - `win_odds <- OZ.win_basis_odds -> jrdb_pre_race_market_staging -> jrdb_win_market_snapshot_v1`
- current non-goal:
  - `popularity` を同じ contract に載せない

## Data Dictionary

### `jrdb_bac_staging`

| column | status | dataset use | notes |
| --- | --- | --- | --- |
| `race_key` | confirmed | allow | race-level key |
| `race_date` | confirmed | allow | race date from fixed-width bytes |
| `post_time` | provisional | allow | appears to be HHMM post time |
| `distance_m` | confirmed | allow | distance in meters in sample |
| `race_conditions_code` | provisional | deny | code block, semantics incomplete |
| `race_class_code` | provisional | deny | code block, semantics incomplete |
| `entry_count` | provisional | allow | looks like field size in sample |
| `race_name` | confirmed | allow | visible race name text |
| `odds_block` | opaque | deny | opaque numeric block |
| `flags_block` | opaque | deny | opaque trailing block |

### `jrdb_cha_staging`

| column | status | dataset use | notes |
| --- | --- | --- | --- |
| `race_key` | confirmed | allow | race-level key |
| `horse_number` | confirmed | allow | horse number within race |
| `workout_weekday` | confirmed | allow | Japanese weekday character |
| `workout_date` | confirmed | allow | workout date from fixed-width bytes |
| `workout_code` | provisional | allow | stable positional code, semantics incomplete |
| `workout_time_block` | opaque | deny | opaque time-related block |
| `workout_metrics_block` | opaque | deny | opaque metrics block |
| `workout_comment_code` | provisional | deny | comment/status code block |

### `jrdb_sed_staging`

| column | status | dataset use | notes |
| --- | --- | --- | --- |
| `race_key` | confirmed | deny | result join key |
| `horse_number` | confirmed | deny | result join key |
| `registration_id` | confirmed | deny | horse registration id |
| `result_date` | confirmed | deny | result date from official spec |
| `horse_name` | confirmed | deny | horse name in result file |
| `distance_m` | provisional | deny | redundant with BAC distance |
| `finish_position` | confirmed | deny | target generation source column |
| `win_odds` | provisional | deny | legacy result-side value retained for traceability only; dataset builder uses `jrdb_win_market_snapshot_v1.win_odds` |
| `popularity` | provisional | allow | optional and allowed only on the `legacy_sed_only_non_mainline` path |

### `jrdb_pre_race_market_staging`

| column | status | dataset use | notes |
| --- | --- | --- | --- |
| `race_key` | confirmed | allow | market snapshot join key |
| `horse_number` | confirmed | allow | market snapshot join key |
| `win_odds` | provisional | allow | current pre-race carrier value derived from `OZ.win_basis_odds` |
| `market_snapshot_source` | provisional | allow | current value is `oz_win_basis_odds` |
| `market_snapshot_date` | provisional | allow | current implementation uses BAC race_date associated with the race_key |

### `jrdb_win_market_snapshot_v1`

| column | status | dataset use | notes |
| --- | --- | --- | --- |
| `race_key` | confirmed | allow | stable contract key |
| `horse_number` | confirmed | allow | stable contract key |
| `win_odds` | provisional | allow | stable downstream carrier for win market odds |
| `market_snapshot_source` | provisional | allow | provenance metadata |
| `market_snapshot_date` | provisional | allow | provenance metadata |

## Confirmed vs Provisional

confirmed:
- fixed-width record length
- CP932 decoding
- `BAC` race-level grain
- `CHA` race+horse grain
- key columns listed above
- visible text/date/integer columns called out as `confirmed`

provisional:
- `post_time`
- `entry_count`
- `race_conditions_code`
- `race_class_code`
- `workout_code`
- `workout_comment_code`

opaque:
- `odds_block`
- `flags_block`
- `workout_time_block`
- `workout_metrics_block`

## Downstream Rules

- dataset builder は allowlist 外の列を参照しない
- `opaque` 列は traceability 専用であり、特徴量化や join key に使わない
- `provisional` 列を dataset builder で使う場合も、この contract の allowlist に含まれる列のみに限定する
- `win_odds` は `jrdb_win_market_snapshot_v1` から参照する
- `popularity` は pre-race carrier 未確認のため、`SED` legacy path からだけ参照する
- `SED` の `finish_position` は target 生成専用であり feature に流さない
- parser を更新しても、allowlist と grain を壊す変更は別 Issue で明示的に扱う
