# JRDB Auto-Ingestion MVP

## Purpose

この spec は、JRDB 更新メールを補助トリガーにして、ローカルで公式ファイルを取得し、保存・解凍・配置し、既存の place-only forward-test Phase 1 へ handoff する local-only MVP を固定する。

今回の正本は「メールそのもの」ではない。正本は、必要ファイルがローカルで `ready` 状態になったことと、その状態を示す manifest / log である。

## Scope

- local-only 運用
- JRDB proprietary archive の download / stage / extract / ready 化
- duplicate trigger の抑止
- failure reason の明示
- existing Phase 1 pre-race handoff の起動点整理
- result-side JRDB ingest への接続

## Non-Goals

- BET logic の変更
- baseline の変更
- `popularity` unresolved auxiliary の解決
- frozen `OZ.win_basis_odds` migration の再開
- mailbox provider abstraction の作り込み
- purchase automation の実装
- downloaded proprietary data / credentials の repo commit

## Baseline Constraints

- current candidate: `guard_0_01_plus_proxy_domain_overlay`
- status: `hard_adopt`
- fallback: `no_bet_guard_stronger surcharge=0.01`
- place-only forward-test Phase 1 stack は current `main` を再利用する

## Authoritative Trigger Rule

- メールは downloader 起動の補助トリガー
- local run の authoritative event は `ready` state
- `ready` は次を満たしたときだけ成立する
  - archive download 完了
  - expected hash がある場合は一致
  - extraction 完了
  - extracted payload を raw placement 先へ atomic move/copy 完了
  - run manifest / log 書き込み完了

## Directory Layout

local-only の orchestration workspace は `data/local/jrdb_auto_ingestion/` を既定とする。

```text
data/local/jrdb_auto_ingestion/
  incoming/
  staging/
  ready/
  state/
```

役割:

- `incoming/`
  - download 完了前の `.part` と download 済み archive
- `staging/`
  - extraction 中の一時作業領域
- `ready/`
  - extraction / placement / manifest 書き込みまで完了した job
- `state/`
  - processed trigger keys
  - run reports
  - duplicate suppression records

既存 ingest との接続先:

- extracted official files の配置先:
  - `data/raw/jrdb/<job_id>/`

既存 forward-test handoff の配置先:

- `data/forward_test/runs/<unit_id>/raw/`
- `data/forward_test/runs/<unit_id>/contract/`
- `data/artifacts/place_forward_test/<unit_id>/pre_race/`

## Trigger Payload

MVP の trigger payload は JSON file で表現する。production intent は email-triggered local run だが、mailbox spec 未確定のため、まずは次を受ける。

- manual trigger
- fixture email trigger

最低限の項目:

- `trigger_kind`
- `message_id` optional
- `detected_at`
- `archives[]`
  - `name`
  - `source_uri`
  - `expected_sha256` optional
  - `archive_kind` optional
- `handoff`
  - `mode`
  - `ingest_ready_files`
  - optional forward-test runtime fields

## Auth

MVP では credentials を repo に保存しない。

許可する経路:

- env var
  - `JRDB_AUTO_INGESTION_USERNAME`
  - `JRDB_AUTO_INGESTION_PASSWORD`
- local auth config file
  - 例: `.local/jrdb_auto_ingestion_auth.toml`
  - gitignore 対象

MVP では HTTP Basic Auth を最小対応とする。mail provider / cookie automation は含めない。

## Download Staging

download sequence:

1. `incoming/<job_id>/` を作る
2. archive を `<name>.part` に書く
3. download 完了後に hash を計算する
4. `expected_sha256` がある場合は一致を確認する
5. `.part` を final archive 名へ rename する

失敗時:

- `.part` のまま残ってもよい
- `ready` へは進めない
- state report に failure reason を書く

## Extraction

対応:

- `zip`: Python stdlib
- `lzh` / `lha`: local external extractor を使う
  - `7z`
  - `7za`
  - `unar`
  - `lha`

非対応または extractor 不在は明示エラー。

extraction sequence:

1. `staging/<job_id>/extract/` に展開する
2. extracted file list を作る
3. 1 ファイルも出なければエラー
4. 完了後に `ready/<job_id>/` へ atomic move する

## Ready Completion Rule

`ready` は次を満たすときだけ書く。

- download 済み archive hash が確定
- extraction 成功
- raw placement 先 `data/raw/jrdb/<job_id>/` へ atomic copy/move 完了
- `ready_manifest.json` が書かれている

不完全 download / hash mismatch / extraction failure / placement failure では `ready` 不成立。

## Idempotency

重複抑止 key は次の優先順で作る。

1. `message_id`
2. `message_id` が無い場合は archive source URI 群 + expected hash 群 + trigger kind の digest

duplicate key が `state/processed/` にあれば、同じ trigger は再実行せず `duplicate_skipped` とする。

## Logging

各 job は report JSON を持つ。

最低限残すもの:

- `job_id`
- `status`
- `dedupe_key`
- `trigger_kind`
- `message_id`
- `downloaded_archives`
- `extracted_files`
- `raw_target_dir`
- `ready_dir`
- `failure_reason` optional
- `handoff_summary` optional

## Handoff Design

handoff は 2 段に分ける。

### 1. Result-side ingest handoff

official files を `data/raw/jrdb/<job_id>/` へ配置し、必要なら既存 `horse_bet_lab.ingest.service.ingest_jrdb_directory` を起動する。

### 2. Phase 1 pre-race handoff

current Phase 1 standard path は次を必要とする。

- `unit_id`
- `notes/unit_metadata_manifest.json`
- `raw/input_snapshot_raw.csv`
- scaffold generated configs
- bridge output contract CSV
- pre-race output dir

current frozen boundaryに合わせ、MVP は pre-race handoff mode を明示的に分ける。

- `none`
  - official files の ready 化と result-side ingest だけ行う
- `forward_pre_race_contract_like_csv_v1`
  - sanctioned local contract-like source CSV を既存 `raw_snapshot_prepare_cli` 相当で raw-ish input に変換し、Phase 1 pre-race まで起動する
- `forward_pre_race_oz_v1`
  - extracted official files から `OZ*.txt` を発見し、`race_key` / `horse_number` / `win_odds` / `place_basis_odds_proxy` を raw-ish input へ落として Phase 1 pre-race まで起動する
  - sanctioned mainline pre-race adapter は当面 `OZ` のみを読む
  - `popularity` は unresolved のまま扱い、raw-ish input に埋めない
  - `SED` / `SRB` はこの adapter scope に含めない

production intent は email-triggered local run だが、fixture smoke では `forward_pre_race_contract_like_csv_v1` を使う。

## Failure Policy

次では即停止:

- duplicate suppression 以外の download failure
- hash mismatch
- extraction failure
- raw placement failure
- scaffold / intake / bridge / runner failure

停止時は:

- `ready` 不成立なら ready を作らない
- state report に failure reason を残す
- half-written forward-test artifacts を silent success としない

## Security Rule

- credentials / cookie / token は repo に入れない
- downloaded proprietary data は repo に commit しない
- local-only path は `.gitignore` に置く

## Acceptance Mapping

この MVP の acceptance は次に対応する。

- spec がある
  - この文書
- local-only downloader / staging / handoff skeleton がある
  - `src/horse_bet_lab/jrdb_ingestion/`
- current Phase 1 への接続点が明確
  - `handoff.py` と runbook
- fixture で smoke できる
  - test 追加
- secrets を repo に入れていない
  - env var / local auth config に限定
