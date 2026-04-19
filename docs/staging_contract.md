# JRDB Staging Contract

この文書は、`raw -> staging` の取り込み結果を downstream がどう扱ってよいかを定義するための contract です。

対象は現時点で `BAC`、`CHA`、`SED` です。ここでいう contract は、dataset builder が安全に参照してよい列、粒度、主キー候補、列の安定度を明文化したものです。

## Status Definitions

- `confirmed`: 実サンプルと構造確認の両方が取れており、dataset builder が参照してよい
- `provisional`: 位置や値の形は安定しているが、意味づけが未確定。dataset builder での利用は allowlist に含まれる列だけに限定する
- `opaque`: traceability のために保持している未解読ブロック。dataset builder は参照しない

## Table Grain And Keys

### `jrdb_bac_staging`

- grain: 1 row per race
- primary key candidate: `race_key`

### `jrdb_cha_staging`

- grain: 1 row per `race_key + horse_number`
- primary key candidate: `race_key`, `horse_number`

### `jrdb_sed_staging`

- grain: 1 row per `race_key + horse_number`
- primary key candidate: `race_key`, `horse_number`

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

- `win_odds`
- `popularity`

`SED` は主に target 生成用 staging として扱いますが、`odds_only` feature set では
`win_odds` を必須、`popularity` を optional feature として参照できます。

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
| `win_odds` | provisional | allow | allowed only for `odds_only` feature set |
| `popularity` | provisional | allow | optional and allowed only for `odds_only` feature set |

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
- `SED` の `win_odds` / `popularity` は `odds_only` feature set からだけ参照する
- `SED` の `finish_position` は target 生成専用であり feature に流さない
- parser を更新しても、allowlist と grain を壊す変更は別 Issue で明示的に扱う
