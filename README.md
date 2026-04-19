# horse-bet-lab

競馬の予測・バックテスト基盤を長期的に育てるための最小スタート地点です。

今の段階では、比較実験の土台になる Python プロジェクト構成、config を 1 つ読み込んで動くダミー runner、そして JRDB 生データを raw から staging に取り込む最小 ingest 基盤を用意しています。モデル実装や学習データ生成にはまだ入っていません。

## Directory Layout

```text
.
├── configs/
├── data/
│   ├── artifacts/
│   ├── raw/
│   │   └── jrdb/
│   └── staging/
├── scripts/
├── src/
│   └── horse_bet_lab/
└── tests/
```

`src/horse_bet_lab/` の下は、将来的に以下のような分割を想定しています。

- `ingest/`
- `dataset/`
- `features/`
- `models/`
- `strategies/`
- `backtest/`
- `evaluation/`

データレイヤは `raw / staging / mart` を意識しますが、今回の実装範囲は `raw -> staging` までです。

- `data/raw/jrdb/`: JRDB の受領済み生ファイル置き場
- `data/staging/`: 中間生成物の置き場。今回は方針のみ作成
- `data/artifacts/`: DuckDB ファイルなどローカル成果物の置き場

## Setup

このプロジェクトは Python 3.11 以上を前提にします。設定ファイル形式は TOML を正式採用しています。

まず Python 3.11 以上が使えることを確認してください。

```bash
python3.11 --version
```

`python3.11` が無い環境では、各自の方法で 3.11 以上を先に用意してください。その後、以下の手順でローカル環境を作成します。

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

## Config Format

実験設定ファイルは TOML を使います。現時点のサンプルは [configs/default.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/default.toml:1) です。

## Run

サンプル config を使ってダミー runner を実行します。

```bash
PYTHONPATH=src python -m horse_bet_lab.runner --config configs/default.toml
```

`pip install -e ".[dev]"` 後にコンソールスクリプトを使うこともできます。

```bash
horse-bet-lab-run --config configs/default.toml
```

期待される出力例:

```text
Running dummy experiment:
  name: baseline_dummy
  model: dummy
  feature_set: minimal
  strategy: flat
 period: 2020-01-01_to_2020-12-31
```

## Wide Research Candidate

ワイド系は本線ではなく、research candidate として別系統で扱います。既存の複勝 reference pack、mainline config、既存 artifact は変更せず、horse-level prediction を pair に持ち上げる最小比較と backtest だけを追加しています。

サンプル config は [configs/wide_research_candidate_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_research_candidate_dual_market_logreg_lift.toml:1) です。

```bash
horse-bet-lab-wide-research-backtest \
  --config configs/wide_research_candidate_dual_market_logreg_lift.toml
```

出力は `summary.csv/json`, `comparison.csv/json`, `best_settings.csv/json`, `selected_pairs.csv/json` です。現在の比較対象は research candidate に限定した `no_ow_guard`, `low_wide_payout_guard`, `extreme_ow_implied_prob_guard` の 3 本で、`comparison` では rolling OOS 集約と bootstrap 区間を確認できます。

今回の candidate generation は、候補馬数 `2 / 3 / 4 / 5` と 1レースあたり採用点数 `1 / 2` を比較できる形を残しつつ、ranking score 自体は `pair_model_score = product_times_geom_place_basis x candidate_top_k=4 x adopted_pair_count=1` に固定しています。

- wide 市場 proxy の raw 調査では、repo 内で使いやすい候補は `jrdb_oz_staging.win_basis_odds / place_basis_odds / headcount` だった
- `HJC` は結果払戻だけなので事前 proxy には使えず、`SED.win_odds / popularity` は単勝寄り補助にはなるが wide 専用 market proxy としては一段遠い
- `OW` raw は research-only で直接確認し、`race_key(8) + headcount(2) + pair odds flattened array` の 778 byte fixed-length と読め、値数は `headcountC2` と一致した
- そのため `OW` は ranking winner ではなく、wide 市場の market baseline 兼 guard 候補として使う方針にした
- rolling OOS 集約では `low_wide_payout_guard` が `roi=0.8749 / total_profit=-107950 / window_win_count=10` で総合 ROI は最良だった
- 一方で `no_ow_guard` は `roi=0.8716 / total_profit=-110910 / window_win_count=12 / roi_std=0.0739` で、window 勝数と安定性では上回った
- `extreme_ow_implied_prob_guard` は `roi=0.8506 / total_profit=-123800 / window_win_count=8` で、guard を強くしすぎると逆効果だった
- つまり今回の比較では、`OW` は主 score ではなく pruning 用 guard としては試す価値があり、現状は「総合 ROI を少し取りに行くなら low payout guard」「安定性を優先するなら no guard」という状態になった
- bootstrap の `ROI > 1.0 ratio` は今回の 3 本とも `0.0` で、現時点では本線候補へは上げない
- HJC / OZ / OW の fixed-length 読みは引き続き research 実装のままで、本線 ingest には持ち込んでいない

`wide_research_v2` は旧 wide research の上に別系統で重ねる整理で、reference pack には触れず、既存 parser / harness / artifact 保存を活かしたまま、OW の役割だけを `market baseline` に限定します。比較対象は `pair_model_score`, `ow_wide_market_prob`, `pair_edge` の 3 本に固定し、v2 のサンプル config は [configs/wide_research_v2_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_research_v2_dual_market_logreg_lift.toml:1) です。出力は `data/artifacts/wide_research_v2_*` 配下に保存されます。実データの rolling OOS では `pair_model_score` が `pair_count=8637 / hit_count=2797 / hit_rate=0.3238 / roi=0.8716 / total_profit=-110910 / window_win_count=17` で 3 本中の最上位になり、`best_settings.csv` でも 3 つの selection rule すべてで `score_role=model_primary` が選ばれました。`pair_edge` は `roi=0.8551 / window_win_count=12`、`ow_wide_market_prob` は `roi=0.7718 / window_win_count=1` で、v2 の比較でも OW 単体を主 score にしない判断を裏づけています。

`wide_research_v3` では score 式の微修正ではなく pair generation 自体を掘り下げ、対称な top-k 全組合せから、`anchor` と `partner` の役割を分けた非対称生成へ進みます。`anchor` は wide 主 score で固定し、`partner` は複勝ロジック由来の score で選ぶ方針に寄せ、比較対象は `anchor_top1_partner_place_edge`, `anchor_top1_partner_pred_times_place`, `anchor_top2_partner_reference_mix` の 3 本です。OW は引き続き ranking score ではなく `market baseline / diagnostic` に限定します。実データの rolling OOS では `anchor_top1_partner_pred_times_place` が `pair_count=8637 / hit_count=2369 / hit_rate=0.2743 / roi=0.8868 / total_profit=-97760 / window_win_count=9` で v3 内の総合最良になり、v2 baseline の `pair_model_score (roi=0.8716 / total_profit=-110910)` より ROI と profit は改善しました。一方で `window_win_count` は v2 baseline の `17` に届かず、v3 は「総合収益は少し改善、安定勝ち数はまだ未改善」という位置づけです。差分を見ると `common=4876` は `roi=0.9069` で強く、改善は主に `v2_only=3761 (roi=0.8258 / profit=-65510)` を `v3_only=3761 (roi=0.8608 / profit=-52360)` に入れ替えられたことから来ており、共有 signal は維持したまま partner 選びで損失を圧縮できた形になっています。さらに window 分解では v3 改善は複数 window にまたがる一方、配当帯では `200-400` と `400-800` の中高配当 hit の寄与が大きく、完全に少数の特大 hit だけに依存した改善ではないと読めます。

`wide_research_v4` は single-win を standalone 本線には上げず、`anchor` 候補として wide に持ち込む別導線です。config は [configs/wide_research_v4_single_win_anchor_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_research_v4_single_win_anchor_dual_market_logreg_lift.toml:1) で、anchor には single-win `dual_market_for_win` の raw score を使い、partner は `place_edge / pred_times_place_basis / reference_mix` の 3 本を比較します。OW は v3 と同じく `market baseline / diagnostic` に限定し、ranking の主役にはしません。

実データの rolling OOS では、v4 内の総合最良は `win_edge_top1_partner_place_edge` で `pair_count=5109 / hit_count=1066 / hit_rate=0.2087 / roi=0.8584 / total_profit=-72360 / window_win_count=5 / mean_roi=0.8635 / roi_std=0.1399` でした。安定性寄りでは `win_pred_top2_partner_place_edge (roi=0.8557 / window_win_count=4 / roi_std=0.1134)` も候補でしたが、同じ 2024-2025 overlap 18 window で見た v3 baseline `anchor_top1_partner_pred_times_place (pair_count=5109 / hit_count=1369 / hit_rate=0.2680 / roi=0.8647 / total_profit=-69100 / mean_roi=0.8624 / roi_std=0.0885)` を上回れませんでした。つまり single-win は standalone より wide anchor 候補として使う価値はあるものの、現時点の v4 では v3 baseline 置換には至っていません。

`wide_research_v5` は v4 の続きとして、anchor を single-win に差し替えるだけではなく、anchor / partner の役割を pair score に明示的に埋め込む研究候補です。sample config は [configs/wide_research_v5_single_win_anchor_pair_score_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_research_v5_single_win_anchor_pair_score_dual_market_logreg_lift.toml:1) で、anchor は `win_pred_top1 / win_edge_top1`、partner は `place_edge / pred_times_place_basis / reference_mix` に固定し、`anchor_win_pred_times_partner_place_pred`, `anchor_win_edge_times_partner_place_edge`, `anchor_win_pred_times_partner_pred_times_place`, `anchor_plus_weighted_partner` の 4 つを比較します。`v5_...` pair generation は top1 anchor に対して候補 partner を列挙したうえで role-aware pair score で順位づけし、baseline 差分は [configs/wide_research_diff_v3_v5_single_win_anchor_pair_score_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_research_diff_v3_v5_single_win_anchor_pair_score_dual_market_logreg_lift.toml:1) から `common / v3_only / v5_only` を確認できます。

実データの rolling OOS では、v5 内の総合最良は `anchor_plus_weighted_partner + v5_win_edge_top1_partner_place_edge + partner_weight=0.2` で `pair_count=5109 / hit_count=1066 / hit_rate=0.2087 / roi=0.8584 / total_profit=-72360 / window_win_count=9 / mean_roi=0.8635 / roi_std=0.1399` でした。ただし今回の weight sweep `0.2 / 0.35 / 0.5 / 0.65 / 0.8` では採用集合と集約指標が実質同一で、`0.2` は tie-break で best_settings に残っただけです。同じ 2024_06_to_07 から 2025_11_to_12 までの 18 window でそろえた v3 baseline `anchor_top1_partner_pred_times_place` は `pair_count=5109 / hit_count=1369 / hit_rate=0.2680 / roi=0.8647 / total_profit=-69100 / mean_roi=0.8624 / roi_std=0.0885` で、v5 はまだ更新できていません。差分を見ると `common=2734 (roi=0.8922 / profit=-29480)` は維持できた一方、置換部分は `v3_only=2375 (roi=0.8332 / profit=-39620)` に対して `v5_only=2375 (roi=0.8195 / profit=-42880)` と悪化しており、今回の weight 調整だけでは置換集合の質改善までは届きませんでした。

`wide_research_v6` は v5 family をこれ以上触らず、anchor / partner の結合式そのものを別 family に置き換える比較です。sample config は [configs/wide_research_v6_single_win_anchor_new_pair_family_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_research_v6_single_win_anchor_new_pair_family_dual_market_logreg_lift.toml:1) で、`anchor_times_partner`, `min_anchor_partner_times_payout_tilt`, `geometric_mean_anchor_partner_times_payout_tilt`, `anchor_plus_partner_edge_like` の 4 family を、single-win anchor 候補と place 系 partner 候補のままで比較します。baseline 差分は [configs/wide_research_diff_v3_v6_single_win_anchor_new_pair_family_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_research_diff_v3_v6_single_win_anchor_new_pair_family_dual_market_logreg_lift.toml:1) から確認できます。

実データの rolling OOS では、v6 内の総合最良は `anchor_plus_partner_edge_like + v6_win_edge_top1_partner_pred_times_place` で `pair_count=5109 / hit_count=1001 / hit_rate=0.1959 / roi=0.8768 / total_profit=-62930 / window_win_count=3 / mean_roi=0.8824 / roi_std=0.1564` でした。同じ 18 window の v3 baseline `anchor_top1_partner_pred_times_place (roi=0.8647 / total_profit=-69100 / window_win_count=9 / mean_roi=0.8624 / roi_std=0.0885)` と比べると、ROI と total_profit では更新できています。overlap 差分では `common=2084 (roi=0.9104 / profit=-18670)` を維持しつつ、置換部分が `v3_only=3025 (roi=0.8333 / profit=-50430)` から `v6_only=3025 (roi=0.8537 / profit=-44260)` に改善しており、改善の主因は置換集合の質が v5 より明確に持ち上がったことです。一方で `window_win_count` と `roi_std` はまだ v3 に劣るので、v6 は「収益は更新、安定性は未更新」という位置づけです。

`wide_family_selection` はここから先の research candidate で、`v3` を安定候補、`v6` を収益候補として固定し、各 test window で直前までの valid 成績から family を選ぶ比較です。sample config は [configs/wide_family_selection_v3_vs_v6_dual_market_logreg_lift.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/wide_family_selection_v3_vs_v6_dual_market_logreg_lift.toml:1) です。今回の実データ rolling OOS では `total_valid_roi_max / window_win_count_max / mean_valid_roi_minus_std` の 3 ルールとも 18 window すべてで `v3` を選び、結果も `pair_count=5109 / hit_count=1369 / hit_rate=0.2680 / roi=0.8647 / total_profit=-69100 / window_win_count=10 / mean_roi=0.8624 / roi_std=0.0885` で v3 baseline と同一でした。つまり現時点では `v6` は test 収益候補としては有力でも、valid で family selection する段階ではまだ `v3` の安定性優位を崩せていません。

## JRDB Ingest

ローカルファイル DB には DuckDB を採用しています。デフォルトの DB ファイルは `data/artifacts/jrdb.duckdb` です。

今回の最小実装では、JRDB ファイル種別のうち以下を対象にしています。

- `BAC`
- `CHA`
- `SED`
- `HJC`
- `OZ`

`BAC*.txt` / `CHA*.txt` は、手元で確認した `PACI250223.zip` サンプルを前提に、固定長テキストとして読み込みます。文字コード、欠損値、日付、型変換は ingest 内の関数で分離しています。

確認済み事項:

- `BAC`: 1 レコード 182 byte の固定長。1 レース 1 レコードに見える
- `CHA`: 1 レコード 62 byte の固定長。`レースキー + 馬番` 単位のレコードに見える
- どちらも CP932 で問題なく読める

staging に出している列は、実サンプルから位置と意味を確認しやすかったものに限定しています。意味が未確定の後半ブロックは `*_block` として保持しています。

未解決点:

- `BAC` の後半数値ブロックの厳密な意味づけは未完了
- `CHA` の `workout_code` / `workout_time_block` / `workout_metrics_block` / `workout_comment_code` の詳細仕様は今後詰める
- `SED` の `distance_m` / `win_odds` / `popularity` は位置確認までで、feature 用 contract には入れていない
- 今回確認したのは `PACI250223.zip` 内の実サンプルであり、他日付ファイルで追加差異が出る可能性はある

結果系ファイル:

- 結果系の最小 ingest 対象は `SED`
- `SED` は JRDB 公式仕様を参照して固定長 374 byte として扱う
- 粒度は `race_key + horse_number`
- 現時点では feature 用ではなく target 生成用 staging として扱う
- 複勝払戻の最小 backtest 用に `HJC` も取り込み対象とする
- `HJC` は race-level の払戻テーブルとして扱い、今回は複勝3本の horse number / payout だけを切り出す
- `OZ` は race-level の基準オッズ配列として扱い、今回は `win_basis_odds` と `place_basis_odds` の先頭2ブロックだけを `race_key + horse_number` に展開する

取り込みコマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.ingest.cli
```

`pip install -e ".[dev]"` 後にコンソールスクリプトを使うこともできます。

```bash
horse-bet-lab-ingest
```

パスを明示したい場合:

```bash
PYTHONPATH=src python -m horse_bet_lab.ingest.cli \
  --raw-dir data/raw/jrdb \
  --duckdb-path data/artifacts/jrdb.duckdb
```

取り込み結果は以下のテーブルに保存されます。

- `ingestion_runs`
- `ingested_files`
- `jrdb_bac_staging`
- `jrdb_cha_staging`
- `jrdb_sed_staging`
- `jrdb_hjc_staging`

同じ入力で再実行した場合も、同一 `source_file_path` の staging 行を置き換えてから再投入するため、staging 側の重複を増やさずに再現可能です。

## Staging Contract

dataset builder が参照してよい staging 列、粒度、主キー候補、列の安定度は [docs/staging_contract.md](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/staging_contract.md:1) に固定しています。

要点だけ先に書くと、

- `jrdb_bac_staging` の粒度は 1 row per race、主キー候補は `race_key`
- `jrdb_cha_staging` の粒度は 1 row per `race_key + horse_number`、主キー候補は `race_key`, `horse_number`
- `jrdb_sed_staging` の粒度は 1 row per `race_key + horse_number`、主キー候補は `race_key`, `horse_number`
- dataset builder が参照してよい列は allowlist に限定
- `opaque` 列は traceability 用であり downstream では使わない
- `provisional` 列は allowlist に含まれるものだけを downstream 利用可とする

## Dataset Build

horse-level dataset は `race_key + horse_number` を基本粒度として、staging contract の allowlist を参照しながら build します。今回の最小実装では confirmed 列を優先し、opaque 列は使いません。

出力先は `data/processed/` です。サンプル config は [configs/dataset_minimal.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/dataset_minimal.toml:1) にあります。

今回の最小 dataset は以下の列を出力します。

- `race_key`
- `horse_number`
- `race_date`
- `split`
- `distance_m`
- `race_name`
- `workout_weekday`
- `workout_date`
- `target_name`
- `target_value`

feature set:

- `minimal`: `distance_m`, `race_name`, `workout_weekday`, `workout_date`
- `odds_only`: `win_odds`
- `odds_only` で `include_popularity = true` を指定した場合は `popularity` も追加
- `market_plus_workout_minimal`: `win_odds`, `popularity`, `workout_gap_days`, `workout_weekday_code`

feature set は config の `feature_set` で切り替えます。`odds_only` は現時点では
`is_place` target と組み合わせる最初の比較基準用 dataset です。

`market_plus_workout_minimal` は市場ベースラインに対する最小追加特徴量比較用です。
今回の追加は confirmed 優先で絞り、以下だけを使います。

- `workout_gap_days`: `race_date - workout_date`
- `workout_weekday_code`: `月..日` を `0..6` に写像

`workout_code` は provisional 列なので、今回の比較にはまだ入れていません。

target は以下の 2 種を選べます。

- `workout_gap_days`: 暫定 target。`race_date - workout_date` の日数
- `is_place`: 正式な結果系 target 候補。`finish_position` が `1..3` を 1、`4..18` を 0 とし、非通常値は build から除外

`is_place` は `jrdb_sed_staging` を `race_key + horse_number` で join して生成します。将来 `is_win` や払戻ベース target に差し替えやすいよう、target 定義は関数分離しています。

`configs/dataset_is_place.toml` は 2025 通年を対象にした実データ検証用 config です。

時系列 split:

- split は `race_date` ベースで固定
- config では `train_end_date` と `valid_end_date` を指定する
- `start_date .. train_end_date` を `train`
- `train_end_date` より後かつ `valid_end_date` までを `valid`
- `valid_end_date` より後を `test`
- split を設定しない場合は全件を `train` として出力

feature / target / metadata の境界:

- metadata: `race_key`, `horse_number`, `race_date`, `split`, `target_name`
- feature: feature set ごとに切り替える
- target: `target_value`
- `win_odds` / `popularity` は `odds_only` では feature として許可する
- `finish_position` など結果系列の source 列は dataset schema に出さない

leakage ルール:

- 同一 `race_key` は split を跨がない
- `train` に `train_end_date` より未来の `race_date` を入れない
- `valid` に `train_end_date` 以前や `valid_end_date` より未来の `race_date` を入れない
- `test` に `valid_end_date` 以前の `race_date` を入れない
- build 後に dataset validation を実行し、違反があれば失敗させる

2025 実データ確認結果:

- build 出力件数: `46,768`
- `CHA -> BAC` join 成功: `47,149 / 47,149`
- `CHA + BAC -> SED` join 成功: `47,149 / 47,149`
- join 落ち: `0`
- `finish_position = 0` の非通常値: `381`
- 非通常値は `is_place` build から除外
- `is_place = 1`: `10,217`
- `is_place = 0`: `36,551`
- 月次件数は `3,054` から `4,772` の範囲で推移し、極端な欠損月は見られない
- `configs/dataset_is_place.toml` の split は `train=2025-01..09`, `valid=2025-10..11`, `test=2025-12`
- split 別件数は `train=35,780`, `valid=7,934`, `test=3,054`、`race_key` の split 跨ぎは `0`

build コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.dataset.cli --config configs/dataset_minimal.toml
```

`pip install -e ".[dev]"` 後はコンソールスクリプトも使えます。

```bash
horse-bet-lab-dataset-build --config configs/dataset_minimal.toml
```

結果系 target のサンプル config:

```bash
horse-bet-lab-dataset-build --config configs/dataset_is_place.toml
```

odds-only baseline 用 config:

```bash
horse-bet-lab-dataset-build --config configs/dataset_odds_only_is_place.toml
```

odds-only + popularity 比較用 config:

```bash
horse-bet-lab-dataset-build --config configs/dataset_odds_only_plus_popularity_is_place.toml
```

市場 + 調教最小追加版 config:

```bash
horse-bet-lab-dataset-build --config configs/dataset_market_plus_workout_minimal_is_place.toml
```

## Baseline Model

最初のベースラインは `odds_only` dataset を使う Logistic Regression です。target は
`is_place` で固定し、feature set / feature transformation は以下の 4 通りを比較できます。

- `win_odds`
- `log(win_odds)` (`log1p` で実装)
- `win_odds + popularity`
- `log(win_odds) + popularity`
- `log(win_odds) + popularity + workout_gap_days + workout_weekday_code`

学習 config は [configs/model_odds_only_logreg_is_place.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/model_odds_only_logreg_is_place.toml:1) です。

比較用 config は [configs/model_odds_only_plus_popularity_logreg_is_place.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/model_odds_only_plus_popularity_logreg_is_place.toml:1) です。

log 変換比較用 config:

- [configs/model_odds_log1p_logreg_is_place.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/model_odds_log1p_logreg_is_place.toml:1)
- [configs/model_odds_log1p_plus_popularity_logreg_is_place.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/model_odds_log1p_plus_popularity_logreg_is_place.toml:1)
- [configs/model_market_plus_workout_minimal_logreg_is_place.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/model_market_plus_workout_minimal_logreg_is_place.toml:1)

学習コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.model.cli --config configs/model_odds_only_logreg_is_place.toml
```

`pip install -e ".[dev]"` 後はコンソールスクリプトも使えます。

```bash
horse-bet-lab-model-train --config configs/model_odds_only_logreg_is_place.toml
```

popularity 付き比較実験:

```bash
horse-bet-lab-model-train --config configs/model_odds_only_plus_popularity_logreg_is_place.toml
```

log 変換比較実験:

```bash
horse-bet-lab-model-train --config configs/model_odds_log1p_logreg_is_place.toml
horse-bet-lab-model-train --config configs/model_odds_log1p_plus_popularity_logreg_is_place.toml
horse-bet-lab-model-train --config configs/model_market_plus_workout_minimal_logreg_is_place.toml
```

出力先:

- `data/artifacts/odds_only_logreg_is_place/metrics.json`
- `data/artifacts/odds_only_logreg_is_place/predictions.csv`
- `data/artifacts/odds_only_logreg_is_place/calibration.csv`
- `data/artifacts/odds_only_plus_popularity_logreg_is_place/metrics.json`
- `data/artifacts/odds_only_plus_popularity_logreg_is_place/predictions.csv`
- `data/artifacts/odds_only_plus_popularity_logreg_is_place/calibration.csv`
- `data/artifacts/odds_log1p_logreg_is_place/metrics.json`
- `data/artifacts/odds_log1p_logreg_is_place/predictions.csv`
- `data/artifacts/odds_log1p_logreg_is_place/calibration.csv`
- `data/artifacts/odds_log1p_plus_popularity_logreg_is_place/metrics.json`
- `data/artifacts/odds_log1p_plus_popularity_logreg_is_place/predictions.csv`
- `data/artifacts/odds_log1p_plus_popularity_logreg_is_place/calibration.csv`
- `data/artifacts/market_plus_workout_minimal_logreg_is_place/metrics.json`
- `data/artifacts/market_plus_workout_minimal_logreg_is_place/predictions.csv`
- `data/artifacts/market_plus_workout_minimal_logreg_is_place/calibration.csv`

評価指標:

- AUC
- logloss
- Brier score
- positive rate
- 予測確率の平均・標準偏差・分位点
- 10ビンの calibration 集計

評価は `split` 列の `train / valid / test` ごとに分けて出力します。

2025 実データ比較結果:

- `win_odds`
  - train: AUC `0.8148`, logloss `0.4346`, Brier `0.1407`
  - valid: AUC `0.8287`, logloss `0.4282`, Brier `0.1380`
  - test: AUC `0.8207`, logloss `0.4189`, Brier `0.1345`
- `log(win_odds)`
  - train: AUC `0.8148`, logloss `0.4101`, Brier `0.1321`
  - valid: AUC `0.8287`, logloss `0.3979`, Brier `0.1276`
  - test: AUC `0.8207`, logloss `0.3952`, Brier `0.1273`
- `win_odds + popularity`
  - train: AUC `0.8117`, logloss `0.4171`, Brier `0.1348`
  - valid: AUC `0.8269`, logloss `0.4058`, Brier `0.1305`
  - test: AUC `0.8186`, logloss `0.3996`, Brier `0.1289`
- `log(win_odds) + popularity`
  - train: AUC `0.8149`, logloss `0.4101`, Brier `0.1321`
  - valid: AUC `0.8289`, logloss `0.3978`, Brier `0.1276`
  - test: AUC `0.8208`, logloss `0.3951`, Brier `0.1273`
- `log(win_odds) + popularity + workout_gap_days + workout_weekday_code`
  - train: AUC `0.8149`, logloss `0.4100`, Brier `0.1321`
  - valid: AUC `0.8288`, logloss `0.3980`, Brier `0.1276`
  - test: AUC `0.8205`, logloss `0.3953`, Brier `0.1274`

見えた傾向:

- `log(win_odds)` は `win_odds` と比べて AUC はほぼ同じまま、logloss / Brier を明確に改善
- `popularity` の追加は、素の `win_odds` では AUC を少し落とす一方で確率系指標を改善
- 今回の 4 パターンでは `log(win_odds) + popularity` が最良で、test では AUC `0.8208`, logloss `0.3951`, Brier `0.1273`
- `workout_gap_days` と `workout_weekday_code` の最小追加は、train ではほぼ同等だが valid / test で改善は確認できなかった

初期結論:

- 市場ベースラインとしては、まず `win_odds` をそのまま使うより `log(win_odds)` のほうが安定
- `popularity` は単独では順位付け改善に直結しないが、`log(win_odds)` と組み合わせるとわずかに上積みがある
- 今回の confirmed 中心の最小調教追加では、市場ベースラインをまだ明確には超えなかった

## Bet Candidate Evaluation

本格的な払戻バックテストの前段として、baseline の `predictions.csv` から threshold
ベースで BET 候補を抽出し、最小集計だけを見る評価を追加しています。単点評価だけでなく、
複数 threshold をまとめて流して採用率と hit rate のトレードオフも比較できます。

今回の最小集計:

- 対象件数 (`candidate_count`)
- 採用件数 (`adopted_count`)
- 採用率 (`adoption_rate`)
- hit rate
- 平均予測確率 (`avg_prediction`)

現時点では `pred_probability >= threshold` を採用条件とし、`target_value = 1` を hit
として扱います。まだ払戻金や戦略最適化には入っていません。

サンプル config:

- [configs/bet_eval_odds_log1p_plus_popularity.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/bet_eval_odds_log1p_plus_popularity.toml:1)

実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.evaluation.cli --config configs/bet_eval_odds_log1p_plus_popularity.toml
```

`pip install -e ".[dev]"` 後はコンソールスクリプトも使えます。

```bash
horse-bet-lab-bet-eval --config configs/bet_eval_odds_log1p_plus_popularity.toml
```

出力先:

- `data/artifacts/bet_eval_odds_log1p_plus_popularity_threshold_sweep/summary.csv`
- `data/artifacts/bet_eval_odds_log1p_plus_popularity_threshold_sweep/summary.json`

2025 実データでの threshold sweep (`0.25, 0.30, 0.35, 0.40, 0.45, 0.50`) 例:

test split の見え方:

- `0.25`: 採用 `1,076`, 採用率 `0.3523`, hit rate `0.4340`, 平均予測確率 `0.4450`
- `0.30`: 採用 `925`, 採用率 `0.3029`, hit rate `0.4638`, 平均予測確率 `0.4729`
- `0.35`: 採用 `797`, 採用率 `0.2610`, hit rate `0.4868`, 平均予測確率 `0.4967`
- `0.40`: 採用 `638`, 採用率 `0.2089`, hit rate `0.5172`, 平均予測確率 `0.5269`
- `0.45`: 採用 `507`, 採用率 `0.1660`, hit rate `0.5523`, 平均予測確率 `0.5534`
- `0.50`: 採用 `367`, 採用率 `0.1202`, hit rate `0.6049`, 平均予測確率 `0.5829`

見方:

- threshold を上げるほど採用件数と採用率は下がる
- その代わり hit rate と平均予測確率は上がる
- つまり、候補を広く取るか、精度を優先して絞るかのトレードオフを比較できる

この集計は比較実験の入口として残し、次に進むときに払戻や資金配分のロジックへつなげます。

## Place Backtest

複勝払戻を使った最小 backtest も追加しています。入力は model の `predictions.csv` と
`jrdb_hjc_staging` で、threshold ごとに採用馬だけを 100円固定で買ったと仮定して集計します。
必要なら `jrdb_sed_staging.win_odds` を使ってオッズ帯フィルタも掛けられます。

今回の最小集計:

- BET数
- 的中数
- 的中率
- 回収率
- 総利益
- 平均払戻

サンプル config:

- [configs/place_backtest_odds_log1p_plus_popularity.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_odds_log1p_plus_popularity.toml:1)
- [configs/place_backtest_odds_log1p_plus_popularity_odds_band.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_odds_log1p_plus_popularity_odds_band.toml:1)
- [configs/place_backtest_odds_log1p_plus_popularity_popularity_band.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_odds_log1p_plus_popularity_popularity_band.toml:1)
- [configs/place_backtest_odds_log1p_plus_popularity_popularity_band_sweep.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_odds_log1p_plus_popularity_popularity_band_sweep.toml:1)
- [configs/place_backtest_odds_log1p_plus_popularity_popularity_band_rollforward.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_odds_log1p_plus_popularity_popularity_band_rollforward.toml:1)
- [configs/place_backtest_edge_odds_log1p_plus_popularity.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_edge_odds_log1p_plus_popularity.toml:1)
- [configs/place_backtest_edge_normalized_odds_log1p_plus_popularity.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_edge_normalized_odds_log1p_plus_popularity.toml:1)
- [configs/place_backtest_edge_oz_place_basis_odds_log1p_plus_popularity.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_edge_oz_place_basis_odds_log1p_plus_popularity.toml:1)
- [configs/place_backtest_edge_oz_place_basis_odds_rollforward.toml](/Users/matsurimbpblack/Library/Mobile Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/configs/place_backtest_edge_oz_place_basis_odds_rollforward.toml:1)

実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.evaluation.place_cli --config configs/place_backtest_odds_log1p_plus_popularity.toml
```

`pip install -e ".[dev]"` 後はコンソールスクリプトも使えます。

```bash
horse-bet-lab-place-backtest --config configs/place_backtest_odds_log1p_plus_popularity.toml
```

出力先:

- `data/artifacts/place_backtest_odds_log1p_plus_popularity_threshold_sweep/summary.csv`
- `data/artifacts/place_backtest_odds_log1p_plus_popularity_threshold_sweep/summary.json`

オッズ帯フィルタを使う場合の config 例:

- `min_win_odds = 5.0`
- `max_win_odds = 50.0`

このときは `jrdb_sed_staging.win_odds` を `DOUBLE` に寄せて、
`min_win_odds <= win_odds <= max_win_odds` を満たす候補だけを backtest 対象にします。

popularity 帯フィルタを使う場合の config 例:

- `min_popularity = 4`
- `max_popularity = 9`

このときは `jrdb_sed_staging.popularity` を使って、
`min_popularity <= popularity <= max_popularity` を満たす候補だけを backtest 対象にします。

複数 popularity band を比較したい場合は、以下のように複数指定できます。

```toml
[[backtest.popularity_bands]]
min = 1
max = 3

[[backtest.popularity_bands]]
min = 4
max = 6
```

時系列ロールフォワードを見たい場合は、以下のように複数の評価窓を定義できます。

```toml
[[backtest.evaluation_windows]]
label = "2025_10"
start_date = "2025-10-01"
end_date = "2025-10-31"

[[backtest.evaluation_windows]]
label = "2025_11"
start_date = "2025-11-01"
end_date = "2025-11-30"
```

edge ベースで見たい場合は、`selection_metric = "edge"` を指定します。
このとき市場確率は `market_prob_method` で切り替えます。
最小定義の `inverse_win_odds` は `market_prob = 1 / win_odds`、
`normalized_inverse_win_odds` は
`market_prob = (1 / win_odds) / race_sum(1 / win_odds)` を使います。
`edge = pred_probability - market_prob` が threshold 以上の候補だけを採用します。

```toml
[backtest]
selection_metric = "edge"
market_prob_method = "inverse_win_odds"
thresholds = [0.00, 0.02, 0.05, 0.10]
```

注意点:

- `market_prob = 1 / win_odds` は控除率や市場全体の正規化を無視した最小近似
- `normalized_inverse_win_odds` は race 内合計を 1.0 に寄せるが、控除率や券種構造は依然として無視している
- raw 調査では `OZ` が race-level の単勝・複勝の基準オッズ配列として最も複勝市場に近い候補だった
- 今回は `OZ` の第1ブロックを `win_basis_odds`、第2ブロックを `place_basis_odds` として使う最小実装にしている
- `OW` もオッズ系 raw として存在するが、今回の最小 proxy では `OZ` のほうが解釈しやすかったため先に採用した
- `win_odds` は `SED` 由来の provisional 列
- したがって edge は「歪みの粗い proxy」であり、厳密な期待値ではない

2025 実データでの test split 例:

- `0.25`: BET `1,076`, 的中 `465`, 的中率 `0.4322`, 回収率 `0.8405`, 総利益 `-17,160`, 平均払戻 `194.5`
- `0.30`: BET `925`, 的中 `427`, 的中率 `0.4616`, 回収率 `0.8561`, 総利益 `-13,310`, 平均払戻 `185.5`
- `0.35`: BET `797`, 的中 `387`, 的中率 `0.4856`, 回収率 `0.8614`, 総利益 `-11,050`, 平均払戻 `177.4`
- `0.40`: BET `638`, 的中 `329`, 的中率 `0.5157`, 回収率 `0.8528`, 総利益 `-9,390`, 平均払戻 `165.4`
- `0.45`: BET `507`, 的中 `279`, 的中率 `0.5503`, 回収率 `0.8556`, 総利益 `-7,320`, 平均払戻 `155.5`
- `0.50`: BET `367`, 的中 `221`, 的中率 `0.6022`, 回収率 `0.8708`, 総利益 `-4,740`, 平均払戻 `144.6`

2025 実データでの odds band (`5.0 <= win_odds <= 50.0`) つき test split 例:

- `0.25`: BET `652`, 的中 `220`, 的中率 `0.3374`, 回収率 `0.8242`, 総利益 `-11,460`, 平均払戻 `244.3`
- `0.30`: BET `501`, 的中 `182`, 的中率 `0.3633`, 回収率 `0.8481`, 総利益 `-7,610`, 平均払戻 `233.5`
- `0.35`: BET `373`, 的中 `142`, 的中率 `0.3807`, 回収率 `0.8566`, 総利益 `-5,350`, 平均払戻 `225.0`
- `0.40`: BET `214`, 的中 `84`, 的中率 `0.3925`, 回収率 `0.8276`, 総利益 `-3,690`, 平均払戻 `210.8`
- `0.45`: BET `83`, 的中 `34`, 的中率 `0.4096`, 回収率 `0.8048`, 総利益 `-1,620`, 平均払戻 `196.5`
- `0.50`: BET `0`, 的中 `0`, 的中率 `0.0000`, 回収率 `0.0000`, 総利益 `0`, 平均払戻 `0.0`

2025 実データでの popularity band (`4 <= popularity <= 9`) つき test split 例:

- `0.25`: BET `447`, 的中 `142`, 的中率 `0.3177`, 回収率 `0.8060`, 総利益 `-8,670`, 平均払戻 `253.7`
- `0.30`: BET `300`, 的中 `106`, 的中率 `0.3533`, 回収率 `0.8413`, 総利益 `-4,760`, 平均払戻 `238.1`
- `0.35`: BET `177`, 的中 `68`, 的中率 `0.3842`, 回収率 `0.8542`, 総利益 `-2,580`, 平均払戻 `222.4`
- `0.40`: BET `67`, 的中 `29`, 的中率 `0.4328`, 回収率 `0.8701`, 総利益 `-870`, 平均払戻 `201.0`
- `0.45`: BET `12`, 的中 `7`, 的中率 `0.5833`, 回収率 `0.9917`, 総利益 `-10`, 平均払戻 `170.0`
- `0.50`: BET `0`, 的中 `0`, 的中率 `0.0000`, 回収率 `0.0000`, 総利益 `0`, 平均払戻 `0.0`

2025 実データでの popularity band sweep の test split best:

- `1 <= popularity <= 3`: best は `threshold 0.50`, BET `367`, 回収率 `0.8708`
- `4 <= popularity <= 6`: best は `threshold 0.45`, BET `12`, 回収率 `0.9917`
- `4 <= popularity <= 9`: best は `threshold 0.45`, BET `12`, 回収率 `0.9917`
- `7 <= popularity <= 12`: best は `threshold 0.25`, BET `18`, 回収率 `0.3722`

見方:

- threshold を上げるほど BET 数は減り、的中率は上がる
- ただし今回の 2025 baseline では、複勝でも全 threshold で回収率は `1.0` 未満
- `5.0 <= win_odds <= 50.0` を入れると、threshold 単独より BET 数はかなり減り、平均払戻は上がる
- ただし test の回収率はこの条件では改善せず、best は `0.35` の `0.8566` で threshold 単独 best `0.8708` を下回った
- `4 <= popularity <= 9` では BET 数がさらに絞られる代わりに test 回収率はかなり改善し、`0.45` で `0.9917` まで近づいた
- popularity band sweep で見ると、改善が出たのは `4-6` / `4-9` 帯だけで、`1-3` は threshold 単独とほぼ同じ、`7-12` は大きく悪化した
- ただしこの改善は BET 数 `12` のかなり小さいサンプルに依存している
- `4-6` と `4-9` が同じ結果になっていて、今回の採用候補では `7-9` 人気帯が実質効いていない可能性がある
- `0.50` では odds band 条件と threshold 条件を同時に満たす候補が 0 件だった
- popularity band でも `0.50` は候補 0 件だった
- 4〜6 人気帯のロールフォワードを `2025-10 / 2025-11 / 2025-12` で見ると、
  - `2025_10`: best は `threshold 0.45`, BET `16`, 回収率 `1.0000`
  - `2025_11`: best は `threshold 0.40`, BET `70`, 回収率 `0.8529`
  - `2025_12`: best は `threshold 0.45`, BET `12`, 回収率 `0.9917`
- つまり `0.45` が常に有望というほどは安定しておらず、月による揺れが残る
- この時点では「hit rate の改善」と「回収率の改善」は一致していない
- edge backtest を 2025 実データで見ると、
  - raw `inverse_win_odds`
    - `edge >= 0.00`: test BET `3,049`, 回収率 `0.7076`
    - `edge >= 0.02`: test BET `2,496`, 回収率 `0.7947`
    - `edge >= 0.05`: test BET `1,980`, 回収率 `0.8094`
    - `edge >= 0.10`: test BET `1,574`, 回収率 `0.8240`
  - normalized `normalized_inverse_win_odds`
    - `edge >= 0.00`: test BET `3,054`, 回収率 `0.7082`
    - `edge >= 0.02`: test BET `2,533`, 回収率 `0.7892`
    - `edge >= 0.05`: test BET `2,030`, 回収率 `0.8042`
    - `edge >= 0.10`: test BET `1,647`, 回収率 `0.8252`
- raw / normalized ともに、edge threshold を上げるほど BET 数は減り、的中率と回収率は改善した
- normalized は test の `edge >= 0.10` で `0.8252` と raw `0.8240` をわずかに上回った
- ただし差はかなり小さく、今回の比較だけで normalized が明確に優位とはまだ言えない
- どちらの market_prob 定義でも test 回収率は `1.0` を超えていない
- `OZ place_basis_odds` を使うと、test はかなり改善した
  - `edge >= 0.00`: BET `317`, 回収率 `0.9631`
  - `edge >= 0.02`: BET `222`, 回収率 `0.9865`
  - `edge >= 0.05`: BET `128`, 回収率 `0.9602`
  - `edge >= 0.10`: BET `45`, 回収率 `0.9667`
- 現時点の 2025 実データでは、`OZ place_basis_odds` が複勝市場 proxy として最も有望
- ただし best の `0.9865` でもまだ `1.0` には届いていない
- `OZ place_basis_odds` のロールフォワードを `2025_10 / 2025_11 / 2025_12` で見ると、
  - `2025_10`: best は `edge >= 0.10`, BET `51`, 的中率 `0.4118`, 回収率 `0.9961`, 総利益 `-20`
  - `2025_11`: best は `edge >= 0.00`, BET `405`, 的中率 `0.2691`, 回収率 `0.7849`, 総利益 `-8,710`
  - `2025_12`: best は `edge >= 0.02`, BET `222`, 的中率 `0.3649`, 回収率 `0.9865`, 総利益 `-300`
- 読み方としては、現在の dataset split に従って `2025_10 / 2025_11` は `valid`、`2025_12` は `test` に出る
- つまり `OZ place_basis_odds` は 10月と12月ではかなり良く見える一方、11月では明確に弱く、月を跨いだ安定優位まではまだ確認できていない
- `OZ place_basis_odds` に popularity band を重ねて `1-3 / 4-6 / 4-9` を同じロールフォワードで比べると、
  - `2025_10`: best は `1-3` かつ `edge >= 0.05`, BET `43`, 的中率 `0.5349`, 回収率 `1.0814`, 総利益 `350`
  - `2025_11`: best は `1-3` かつ `edge >= 0.00`, BET `125`, 的中率 `0.4320`, 回収率 `0.8176`, 総利益 `-2,280`
  - `2025_12`: best は `1-3` かつ `edge >= 0.02`, BET `68`, 的中率 `0.5588`, 回収率 `1.1162`, 総利益 `790`
- 今回の比較では `4-6` や `4-9` より `1-3` が一貫して優位で、10月・12月では回収率が `1.0` を上回った
- ただし 11月は同じ `1-3` でも回収率 `0.8176` に落ちており、人気帯条件を足しても月を跨いだ安定優位まではまだ確認できていない
- `1-3` の 10月・12月改善は、OZ proxy と edge 条件の相性を見るうえで有望なシグナルだが、まだ単年・単月ベースの観測として扱うのが安全
- 月別の詳細分解は以下の追加 artifact に保存される
  - `monthly_summary.csv` / `monthly_summary.json`
  - `monthly_odds_bands.csv` / `monthly_odds_bands.json`
- `1-3 人気帯` の月別分解を見ると、
  - `2025_10`: best は `edge >= 0.05`, BET `43`, 回収率 `1.0814`, 平均払戻 `202.2`, 平均 edge `0.1165`
  - `2025_11`: best は `edge >= 0.00`, BET `125`, 回収率 `0.8176`, 平均払戻 `189.3`, 平均 edge `0.0618`
  - `2025_12`: best は `edge >= 0.02`, BET `68`, 回収率 `1.1162`, 平均払戻 `199.7`, 平均 edge `0.0778`
- ここで重要なのは、11月だけ `avg_edge` が低いだけでなく、採用馬の平均払戻も 10月・12月より弱いこと
- `monthly_odds_bands.csv` を見ると、`1-3 人気帯` の採用馬は各月とも主に
  - `win_odds`: `2-5` と `5-10`
  - `place_basis_odds`: `2.0-3.0` と `3.0-5.0`
  に集中している
- つまり 11月の崩れは、極端なオッズ帯に寄ったというより、同じ人気帯の中で payout の質が悪かった可能性が高い
- `1-3 人気帯` 固定で edge threshold を細かく刻むと、`configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_fine_sweep.toml` で全体窓と月窓を一緒に比較できる
- 全体窓 (`all_2025`) では、
  - `valid`: best は `edge >= 0.03`, BET `138`, 的中率 `0.4638`, 回収率 `0.9094`, 総利益 `-1,250`
  - `test`: best は `edge >= 0.02`, BET `68`, 的中率 `0.5588`, 回収率 `1.1162`, 総利益 `790`
- 月別に見ると山の位置がずれている
  - `2025_10`: `0.08` 付近が最良で、BET `31`, 回収率 `1.1097`, 平均 edge `0.1378`
  - `2025_11`: `0.03` 付近が相対的にましで、BET `80`, 回収率 `0.8237`, 平均 edge `0.0876`
  - `2025_12`: `0.02` 付近が最良で、BET `68`, 回収率 `1.1162`, 平均 edge `0.0778`
- 読み方としては、threshold を上げるほど `avg_edge` はほぼ単調に増えるが、回収率は単調ではない
- つまり「より大きい edge だけを取ればよい」ではなく、月ごとに payout の質との兼ね合いで最適帯が動いている
- `place_basis_odds` 帯の詳細分解は `monthly_place_basis_buckets.csv` / `monthly_place_basis_buckets.json` に保存される
- `1-3 人気帯` の best threshold で `place_basis_odds` 帯を見ると、
  - `2025_10` (`edge >= 0.08`)
    - `2.0-3.0`: BET `14`, 的中率 `0.8571`, 回収率 `1.6571`, 平均払戻 `193.3`, 平均 edge `0.1115`
    - `3.0-5.0`: BET `15`, 的中率 `0.3333`, 回収率 `0.7467`, 平均払戻 `224.0`, 平均 edge `0.1611`
  - `2025_11` (`edge >= 0.03`)
    - `1.5-2.0`: BET `6`, 的中率 `0.6667`, 回収率 `1.0333`, 平均払戻 `155.0`, 平均 edge `0.0502`
    - `2.0-3.0`: BET `48`, 的中率 `0.5000`, 回収率 `0.9688`, 平均払戻 `193.8`, 平均 edge `0.0760`
    - `3.0-5.0`: BET `25`, 的中率 `0.2400`, 回収率 `0.5280`, 平均払戻 `220.0`, 平均 edge `0.1147`
  - `2025_12` (`edge >= 0.02`)
    - `1.5-2.0`: BET `12`, 的中率 `0.6667`, 回収率 `1.0083`, 平均払戻 `151.3`, 平均 edge `0.0348`
    - `2.0-3.0`: BET `40`, 的中率 `0.6000`, 回収率 `1.2250`, 平均払戻 `204.2`, 平均 edge `0.0631`
    - `3.0-5.0`: BET `15`, 的中率 `0.4000`, 回収率 `0.9867`, 平均払戻 `246.7`, 平均 edge `0.1400`
- 見え方としては、11月の崩れは `avg_edge` がゼロに近いからではなく、`3.0-5.0` 帯の hit rate / ROI 悪化が大きい
- 一方で 10月・12月は `2.0-3.0` 帯がかなり強く、ここが月全体の改善を支えている
- `place_basis_odds 2.0-3.0` を明示条件にした比較は `configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_2_0_3_0.toml` で再現できる
- この条件を固定して edge threshold を振ると、
  - `all_2025 valid`: best は `edge >= 0.10`, BET `10`, 的中率 `0.9000`, 回収率 `1.8400`, 総利益 `840`, 平均払戻 `204.4`, 平均 edge `0.1252`
  - `all_2025 test`: best は `edge >= 0.10`, BET `7`, 的中率 `0.7143`, 回収率 `1.2429`, 総利益 `170`, 平均払戻 `174.0`, 平均 edge `0.1276`
  - `2025_10`: best は `edge >= 0.10`, BET `10`, 回収率 `1.8400`
  - `2025_11`: best は `edge >= 0.06`, BET `31`, 回収率 `1.0032`
  - `2025_12`: best は `edge >= 0.10`, BET `7`, 回収率 `1.2429`
- つまり `place_basis_odds 2.0-3.0` を固定すると、前回より 11月の崩れがかなり緩和し、月別 best でもすべて `1.0` 近辺かそれ以上まで改善した
- この条件下の `monthly_place_basis_buckets.csv` は境界 `3.0` を含むため、一部の採用馬が bucket 表示上は `3.0-5.0` 側に見える
- それでも実質的には `2.0-3.0` 帯中心の戦略で、`3.0` 境界をどう扱うかは次に詰める余地がある
- `place_basis_odds` の境界 sweep は `configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_band_sweep.toml` で再現できる
- 比較した近傍帯:
  - `1.8-2.8`
  - `2.0-2.8`
  - `2.0-3.0`
  - `2.2-3.0`
  - `2.2-3.2`
  - `2.0-3.5`
- `all_2025 valid` の best は `2.2-3.0` かつ `edge >= 0.10`
  - BET `22`
  - 的中率 `0.6818`
  - 回収率 `1.3409`
  - 総利益 `750`
  - 平均払戻 `196.7`
  - 平均 edge `0.1252`
- `all_2025 test` の best は `1.8-2.8` かつ `edge >= 0.10`
  - BET `5`
  - 的中率 `1.0000`
  - 回収率 `1.7400`
  - 総利益 `370`
  - 平均払戻 `174.0`
  - 平均 edge `0.1274`
- 月別 best は以下のとおり
  - `2025_10`: `2.0-3.0`, `edge >= 0.10`, BET `10`, 回収率 `1.8400`
  - `2025_11`: `2.0-2.8`, `edge >= 0.06`, BET `26`, 回収率 `1.1269`
  - `2025_12`: `1.8-2.8`, `edge >= 0.10`, BET `5`, 回収率 `1.7400`
- 見え方としては、`2.0-3.0` だけが一点で強いというより、`1.8-3.0` 近辺の狭い帯が全体に有望
- 一方で `2.0-3.5` のように上側へ広げすぎると、11月・12月の回収率が再び弱くなりやすい
- つまり改善は完全な偶然一点ではなさそうだが、`2.8-3.2` や `3.5` 側へ帯を広げると質が落ちるため、強いのはかなり近傍の狭いレンジと見てよさそう
- 条件選定ルールを固定する場合は、`configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_band_select_on_valid.toml` を使う
- ルール:
  - 候補条件群をすべて `valid` で比較
  - `ROI` 最大を best とする
  - 同率なら `BET数` が多い条件を優先
  - さらに同率なら `threshold` が小さい条件を優先
  - 選ばれた条件をそのまま `test` に固定適用する
- 出力 artifact:
  - `selected_summary.csv`
  - `selected_summary.json`
- 2025 実データで valid 選定すると、選ばれた条件は
  - `popularity 1-3`
  - `place_basis_odds 2.2-3.0`
  - `edge >= 0.10`
- このときの結果は
  - `valid`: BET `22`, 的中 `15`, 的中率 `0.6818`, 回収率 `1.3409`, 総利益 `750`, 平均払戻 `196.7`, 平均 edge `0.1305`
  - `test`: BET `6`, 的中 `4`, 的中率 `0.6667`, 回収率 `1.2167`, 総利益 `130`, 平均払戻 `182.5`, 平均 edge `0.1270`
- 読み方としては、「best 条件を後から読む」段階から一歩進めて、「valid で決めた条件が test でも残るか」を再現可能に確認できるようになった
- ただし `test` の BET 数は `6` とまだ小さいため、選定ルールを固定できたことと、条件の安定性が十分に証明されたことは分けて扱うのが安全
- 次に進むなら、threshold だけでなくオッズ帯や期待値条件も併せて見るのが自然
- 複数の valid/test 窓で同じ手続きを繰り返す場合は、`configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_band_select_on_valid_rollforward.toml` を使う
- ここでは `evaluation_window_pairs` を使って
  - `2025_09_to_10`: valid=2025-09, test=2025-10
  - `2025_10_to_11`: valid=2025-10, test=2025-11
  - `2025_11_to_12`: valid=2025-11, test=2025-12
  を同じ候補群・同じ選定ルールで繰り返し比較できる
- さらに `min_bets_valid_values = [10, 15, 20]` を入れて、valid で BET 数が少なすぎる条件をどの程度強く除外するかを感度比較できる
- 出力 artifact:
  - `selected_summary.csv`
  - `selected_summary.json`
- 2025 実データでの感度比較結果は以下
  - `min_bets_valid = 10`
    - `2025_09_to_10`: selected `2.0-2.8`, `edge >= 0.02` -> test ROI `1.2692`, BET `39`
    - `2025_10_to_11`: selected `2.0-3.0`, `edge >= 0.10` -> test ROI `0.7929`, BET `14`
    - `2025_11_to_12`: selected `2.0-2.8`, `edge >= 0.06` -> test ROI `1.2167`, BET `18`
  - `min_bets_valid = 15`
    - `2025_09_to_10`: selected `2.0-2.8`, `edge >= 0.02` -> test ROI `1.2692`, BET `39`
    - `2025_10_to_11`: selected `2.0-3.0`, `edge >= 0.08` -> test ROI `0.7647`, BET `17`
    - `2025_11_to_12`: selected `2.0-2.8`, `edge >= 0.06` -> test ROI `1.2167`, BET `18`
  - `min_bets_valid = 20`
    - `2025_09_to_10`: selected `2.0-2.8`, `edge >= 0.02` -> test ROI `1.2692`, BET `39`
    - `2025_10_to_11`: selected `2.0-3.5`, `edge >= 0.08` -> test ROI `0.8000`, BET `27`
    - `2025_11_to_12`: selected `2.0-2.8`, `edge >= 0.06` -> test ROI `1.2167`, BET `18`
- 見方としては、
  - `2025_09_to_10` と `2025_11_to_12` は `10 / 15 / 20` のどれでも選定結果が変わらず、test でもプラスを維持した
  - `2025_10_to_11` だけは `min_bets_valid` を上げると `edge >= 0.10` の極端な条件から少し広い条件へ移る
  - ただし `2025_10_to_11` の test ROI は `0.7647 - 0.8000` に留まり、制約を強めても崩れ方は大きくは改善していない
  - つまり `min_bets_valid` は極小サンプル過適合を減らすのには有効だが、それだけで再現性問題を解決する段階ではまだない
- rolling retrain 前提で同じ比較を回す場合は、`configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_band_select_on_valid_rollforward_retrain.toml` を使う
- この config では各 pair ごとに
  - valid 開始日前までの dataset で Logistic Regression を再学習
  - そのモデルで valid / test を予測
  - valid ROI 最大条件を選んで test に固定適用
 する
- 追加 artifact:
  - `rolling_predictions.csv`
  - `selected_summary.csv`
  - `selected_summary.json`
- 2025 実データでの結果は以下
  - `2025_09_to_10`
    - selected: `place_basis_odds 2.0-2.8`, `edge >= 0.02`
    - valid: BET `34`, 的中 `18`, 回収率 `1.0324`, 総利益 `110`, 平均 edge `0.0634`
    - test: BET `38`, 的中 `24`, 回収率 `1.2421`, 総利益 `920`, 平均 edge `0.0641`
  - `2025_10_to_11`
    - selected: `place_basis_odds 2.0-3.0`, `edge >= 0.10`
    - valid: BET `10`, 的中 `9`, 回収率 `1.8400`, 総利益 `840`, 平均 edge `0.1252`
    - test: BET `14`, 的中 `6`, 回収率 `0.7929`, 総利益 `-290`, 平均 edge `0.1335`
  - `2025_11_to_12`
    - selected: `place_basis_odds 2.0-2.8`, `edge >= 0.06`
    - valid: BET `27`, 的中 `15`, 回収率 `1.0852`, 総利益 `230`, 平均 edge `0.0968`
    - test: BET `19`, 的中 `13`, 回収率 `1.2684`, 総利益 `510`, 平均 edge `0.0908`
- 見方としては、
  - rolling retrain を入れても `2025_10_to_11` の崩れは残った
  - 一方で `2025_11_to_12` は固定 model より少し改善した
  - つまり「学習窓の更新」は多少効いているが、再現性問題の主因がそれだけとはまだ言えない
- valid 条件選定スコアを比較する場合も、同じ `configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_band_select_on_valid_rollforward_retrain.toml` を使う
- この config では `selection_score_rules = ["roi_max", "roi_then_bets", "roi_weighted_by_bets"]` を順に評価し、各 rule ごとに
  - rolling retrain で valid/test 予測を作る
  - valid で条件選定する
  - 同条件を test に固定適用する
- 追加 artifact:
  - `selected_summary.csv`
  - `selected_summary.json`
  - `selection_score_rule` 列つきで rule ごとの結果を保存
- 2025 実データでの比較は以下
  - `roi_max`
    - `2025_09_to_10`: selected `2.0-2.8`, `edge >= 0.02` -> test ROI `1.2421`, BET `38`
    - `2025_10_to_11`: selected `2.0-3.0`, `edge >= 0.10` -> test ROI `0.7929`, BET `14`
    - `2025_11_to_12`: selected `2.0-2.8`, `edge >= 0.06` -> test ROI `1.2684`, BET `19`
  - `roi_then_bets`
    - 今回は `roi_max` と同じ条件が選ばれた
    - つまり valid ROI の最大値に tie がなく、BET 数 tie-break が実質発動しなかった
  - `roi_weighted_by_bets`
    - `2025_09_to_10`: selected `2.0-2.8`, `edge >= 0.02` -> test ROI `1.2421`, BET `38`
    - `2025_10_to_11`: selected `2.0-3.5`, `edge >= 0.02` -> test ROI `0.8321`, BET `78`
    - `2025_11_to_12`: selected `1.8-2.8`, `edge >= 0.04` -> test ROI `1.2139`, BET `36`
- 見方としては、
  - `roi_weighted_by_bets` は BET 数を少し重視するぶん、`2025_10_to_11` で極端な高 ROI 小サンプル条件を避けて、より広い条件へ寄った
  - その結果 `2025_10_to_11` の test ROI は `0.7929 -> 0.8321` と少し改善した
  - 一方で `2025_11_to_12` では `roi_weighted_by_bets` のほうが `1.2684 -> 1.2139` と少し悪化した
  - つまり現時点では、「BET 数を少し重く見ると一部の崩れは和らぐが、全 pair で一貫して優位な scoring rule まではまだ見えていない」と読むのが安全
- 複数 valid 窓を集約して条件選定する場合は、`configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_band_select_on_aggregate_valid_rollforward_retrain.toml` を使う
- この config では
  - `test_2025_10`: valid=`2025_09_to_10`
  - `test_2025_11`: valid=`2025_09_to_10, 2025_10_to_11`
  - `test_2025_12`: valid=`2025_09_to_10, 2025_10_to_11, 2025_11_to_12`
  を集約して、次の test 窓へ固定適用する
- 集約 rule:
  - `mean_valid_roi_minus_std`
  - `positive_window_count_then_mean_roi_then_min_roi`
  - `min_valid_roi_then_mean_roi`
- 追加 artifact:
  - `selected_summary.csv`
  - `selected_summary.json`
  - `valid_window_labels`, `test_window_label`, `valid_aggregate_score`, `valid_positive_window_count`
  - `valid_mean_roi`, `valid_min_roi`, `valid_roi_std`, `valid_window_rois`
- 2025 実データでの比較は以下
  - `mean_valid_roi_minus_std`
    - `test_2025_10`: selected `2.0-2.8`, `edge >= 0.02` -> valid score `1.0324`, valid rois `[1.0324]`, test ROI `1.2421`, BET `38`
    - `test_2025_11`: selected `2.0-2.8`, `edge >= 0.02` -> valid score `1.0324`, valid rois `[1.0324, 1.2692]`, test ROI `0.8696`, BET `56`
    - `test_2025_12`: selected `2.0-2.8`, `edge >= 0.04` -> valid score `0.9581`, valid rois `[0.9520, 1.3192, 1.0641]`, test ROI `1.2548`, BET `31`
  - `positive_window_count_then_mean_roi_then_min_roi`
    - `test_2025_10`: selected `2.0-2.8`, `edge >= 0.02` -> valid score `1`, valid rois `[1.0324]`, test ROI `1.2421`, BET `38`
    - `test_2025_11`: selected `2.0-2.8`, `edge >= 0.02` -> valid score `2`, valid rois `[1.0324, 1.2692]`, test ROI `0.8696`, BET `56`
    - `test_2025_12`: selected `2.0-2.8`, `edge >= 0.06` -> valid score `2`, valid rois `[0.9333, 1.4579, 1.0852]`, test ROI `1.2684`, BET `19`
  - `min_valid_roi_then_mean_roi`
    - `test_2025_10`: selected `2.0-2.8`, `edge >= 0.02` -> valid score `1.0324`, valid rois `[1.0324]`, test ROI `1.2421`, BET `38`
    - `test_2025_11`: selected `2.0-2.8`, `edge >= 0.02` -> valid score `1.0324`, valid rois `[1.0324, 1.2692]`, test ROI `0.8696`, BET `56`
    - `test_2025_12`: selected `2.0-2.8`, `edge >= 0.04` -> valid score `0.9520`, valid rois `[0.9520, 1.3192, 1.0641]`, test ROI `1.2548`, BET `31`
- 見方としては、
  - `test_2025_10` は集約 valid が 1 窓しかないので、3 rule とも同じ
  - `test_2025_11` では 3 rule とも `2.0-2.8 / edge >= 0.02` に寄り、以前の `0.7647` よりは改善して `0.8696` になった
  - `test_2025_12` では「平均の強さ」を見る rule は `edge >= 0.04` の少し広い条件を選び、「正の窓数」を重視する rule は `edge >= 0.06` のやや厳しい条件を選んだ
  - `valid_window_rois` を見ると、`test_2025_12` では先行 valid 窓に `0.9520` や `0.9333` が混じっていて、平均だけでなく最悪窓の扱いで選定が分かれている
  - ただし今回も test ROI は `2025_10_to_11` で 1.0 未満なので、安定性ペナルティを入れても決定打にはまだ届いていない
- 2024 を含めて OOS 窓を増やす場合は、`configs/dataset_odds_only_plus_popularity_is_place_2024_2025.toml` と `configs/place_backtest_edge_oz_place_basis_odds_popularity_1_3_place_basis_band_select_on_aggregate_valid_rollforward_retrain_2024_2025.toml` を使う
- この構成では
  - dataset を `2024-01-01` から `2025-12-31` まで拡張
  - `OZ.place_basis_inverse`
  - `popularity 1-3`
  - `place_basis_odds` の狭い band 候補
  - stability 寄りの aggregate selection rule
  を維持したまま、`2024_07` から `2025_12` までの 18 本の monthly test 窓で rolling OOS を確認できる
- 追加 artifact:
  - `selected_summary.csv`
  - `selected_summary.json`
  - `selected_test_rollup.csv`
  - `selected_test_rollup.json`
- 2024-2025 の combined test rollup は以下
  - `mean_valid_roi_minus_std`
    - test window count: `18`
    - BET `607`
    - 的中 `292`
    - 的中率 `0.4811`
    - 回収率 `0.9206`
    - 総利益 `-4820`
    - 平均払戻 `191.4`
    - 平均 edge `0.1057`
  - `min_valid_roi_then_mean_roi`
    - test window count: `18`
    - BET `774`
    - 的中 `364`
    - 的中率 `0.4703`
    - 回収率 `0.9193`
    - 総利益 `-6250`
    - 平均払戻 `195.5`
    - 平均 edge `0.0939`
  - `positive_window_count_then_mean_roi_then_min_roi`
    - test window count: `18`
    - BET `509`
    - 的中 `257`
    - 的中率 `0.5049`
    - 回収率 `0.9560`
    - 総利益 `-2240`
    - 平均払戻 `189.3`
    - 平均 edge `0.1049`
- 見方としては、
  - 18 本の OOS test 窓を合算すると、今回の best は `positive_window_count_then_mean_roi_then_min_roi`
  - ただし best でも combined ROI は `0.9560` で、まだ `1.0` を超えていない
  - `2024` 後半の窓はおおむね弱く、改善は主に `2025` の一部窓に寄っている
  - つまり現時点では「有望条件群は見えたが、長い OOS で安定優位が確認できた」とまでは言えない
- 代表例として、
  - `mean_valid_roi_minus_std` は `2025_10` で `ROI 1.5333`、`2025_11` で `0.8000`
  - `positive_window_count_then_mean_roi_then_min_roi` は `2025_10` で `1.7231`、`2025_11` で `0.8667`、`2025_12` で `1.0500`
  となり、rule を安定性寄りにしても月次の揺れはまだ残る
- 複勝市場に寄せた baseline feature set の比較は、`configs/model_market_feature_comparison_2024_2025.toml` で再現できる
- 比較する feature set:
  - `current_win_market`: `log1p(win_odds) + popularity`
  - `place_market_only`: `log1p(place_basis_odds)`
  - `place_market_plus_popularity`: `log1p(place_basis_odds) + popularity`
  - `dual_market`: `log1p(win_odds) + log1p(place_basis_odds) + popularity`
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.model.comparison_cli --config configs/model_market_feature_comparison_2024_2025.toml
```

- この比較は
  - 2024-2025 dataset build
  - rolling retrain
  - `positive_window_count_then_mean_roi_then_min_roi` 固定での OOS backtest
  を feature set ごとに一気通しで回し、`summary.csv` / `summary.json` にまとめる
- 2024-2025 OOS の比較結果は以下
  - `current_win_market`
    - AUC `0.8163`
    - logloss `0.4102`
    - Brier `0.1321`
    - BET `509`
    - 的中率 `0.5049`
    - 回収率 `0.9560`
    - 総利益 `-2240`
  - `place_market_only`
    - AUC `0.7970`
    - logloss `0.4286`
    - Brier `0.1371`
    - BET `0`
    - 的中率 `0.0000`
    - 回収率 `0.0000`
    - 総利益 `0`
  - `place_market_plus_popularity`
    - AUC `0.8088`
    - logloss `0.4182`
    - Brier `0.1342`
    - BET `597`
    - 的中率 `0.4673`
    - 回収率 `0.9350`
    - 総利益 `-3880`
  - `dual_market`
    - AUC `0.8164`
    - logloss `0.4101`
    - Brier `0.1320`
    - BET `388`
    - 的中率 `0.5387`
    - 回収率 `1.0111`
    - 総利益 `430`
- 見方としては、
  - `place_basis_odds` を market proxy として strategy に入れるだけでなく、model feature にも足すと改善余地がある
  - `place_market_only` は signal 自体は出るが、今回の固定 edge 戦略では採用候補をほぼ作れなかった
  - `dual_market` は classification 指標をほぼ維持したまま、combined OOS backtest で初めて `ROI > 1.0` に乗った
  - つまり現時点の市場 baseline は、`win` 市場だけより `win + place` の dual market 表現が最有力
  - ただし `1.0111` はまだ薄い優位なので、次は window 数を増やすか、同 feature set で selection / stake を別に検証したい
- 単勝研究ラインは別導線で `configs/model_single_win_market_comparison_2024_2025.toml` を追加し、複勝本線や wide research には触れずに `is_win` target の rolling OOS 比較だけを回せるようにした。比較対象は `win_market_only = log1p(win_odds) + popularity` と `dual_market_for_win = log1p(win_odds) + log1p(place_basis_odds) + popularity` の 2 本で、backtest は `configs/win_backtest_edge_inverse_win_odds_rollforward_retrain_2024_2025.toml` から単勝払戻を使って `summary.csv/json` を保存する。
- 2024-2025 の実データで単勝研究ラインを回すと、`win_market_only` は `AUC=0.8399 / logloss=0.2058 / Brier=0.0583 / BET=1598 / 的中率=0.0695 / 回収率=0.7543 / 総利益=-39260`、`dual_market_for_win` は `AUC=0.8400 / logloss=0.2057 / Brier=0.0583 / BET=2202 / 的中率=0.0745 / 回収率=0.9306 / 総利益=-15290` だった。dual market を入れると単勝 OOS は明確に改善したが、現時点ではまだ `ROI > 1.0` には届いていない。
- その次の段として single-win の selection / guard / threshold を複勝本線と同じ作法で掘ると、2024-2025 rolling OOS の暫定 best は `dual_market_for_win + aggregate rule roi_then_bets (roi_max と同値) + min_bets_valid=5 or 10` で、`AUC=0.8400 / logloss=0.2057 / Brier=0.0583 / BET=619 / 的中率=0.1131 / 回収率=0.8414 / 総利益=-9820 / max_drawdown=10970 / max_losing_streak=3 / ROI>1.0 ratio=0.3333 / ROI95%= [0.6567, 1.0363]` だった。
- この deeper compare では `threshold=0.00` が全 window で選ばれ、popularity band は主に `1-5`、win_odds band は主に `3-10` と `5-15` が残った。一方で standalone ROI は 1.0 を超えず、前段の単純 dual-market line (`ROI=0.9306`) も上回れなかったため、現時点の single-win は本線候補というより `wide anchor` 側へ持ち込む価値が高い研究ラインとして扱うのが妥当。
- `dual_market` の頑健性確認は、`configs/dataset_dual_market_is_place_2024_2025.toml` と `configs/place_backtest_edge_oz_place_basis_dual_market_robustness_2024_2025.toml` で再現できる
- 固定するもの:
  - `dual_market = log1p(win_odds) + log1p(place_basis_odds) + popularity`
  - `market_prob_method = oz_place_basis_inverse`
  - `popularity = 1-3`
- 感度比較するもの:
  - edge threshold: `0.04 / 0.06 / 0.08 / 0.10 / 0.12`
  - `place_basis_odds` band: `1.8-2.8 / 2.0-2.8 / 2.0-3.0 / 2.2-3.0 / 2.2-3.2 / 2.0-3.5`
  - aggregate selection rule:
    - `mean_valid_roi_minus_std`
    - `positive_window_count_then_mean_roi_then_min_roi`
    - `min_valid_roi_then_mean_roi`
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.dataset.cli --config configs/dataset_dual_market_is_place_2024_2025.toml
PYTHONPATH=src python -m horse_bet_lab.evaluation.place_cli --config configs/place_backtest_edge_oz_place_basis_dual_market_robustness_2024_2025.toml
```

- combined OOS の `selected_test_rollup` は以下
  - `mean_valid_roi_minus_std`
    - BET `558`
    - 的中 `277`
    - 的中率 `0.4964`
    - 回収率 `0.9430`
    - 総利益 `-3180`
    - 平均払戻 `190.0`
    - 平均 edge `0.0923`
  - `positive_window_count_then_mean_roi_then_min_roi`
    - BET `388`
    - 的中 `209`
    - 的中率 `0.5387`
    - 回収率 `1.0111`
    - 総利益 `430`
    - 平均払戻 `187.7`
    - 平均 edge `0.0976`
  - `min_valid_roi_then_mean_roi`
    - BET `667`
    - 的中 `321`
    - 的中率 `0.4813`
    - 回収率 `0.9246`
    - 総利益 `-5030`
    - 平均払戻 `192.1`
    - 平均 edge `0.0868`
- 見方としては、
  - `dual_market` の `ROI > 1.0` は threshold / band / rule を少し振っても完全には消えていないが、best rule でも改善幅はかなり薄い
  - 今回の best は `positive_window_count_then_mean_roi_then_min_roi`
  - ただし 18 窓中の test でプラスは `7` 窓、マイナスは `11` 窓で、月次の揺れはまだ残る
  - 勝ちが完全な極小サンプルだけに依存しているわけではなく、利益寄与が大きい窓は
    - `test_2025_10`: BET `16`, ROI `1.5313`, profit `850`
    - `test_2025_12`: BET `20`, ROI `1.3550`, profit `710`
    - `test_2025_05`: BET `11`, ROI `1.6000`, profit `660`
    だった
  - 一方で `test_2024_08`, `test_2024_09`, `test_2025_11` などは 1.0 未満で、依然として外部検証窓のばらつきは大きい
  - つまり現時点の結論は「`dual_market` は有望だが、まだ十分に頑健とまでは言えない」
- `dual_market` の派生特徴量比較は、`configs/model_dual_market_derived_feature_comparison_2024_2025.toml` で再現できる
- 比較する feature set:
  - `dual_market`
  - `dual_market_plus_log_diff`
    - `log_place_minus_log_win`
  - `dual_market_plus_implied_probs`
    - `implied_place_prob`
    - `implied_win_prob`
  - `dual_market_plus_prob_diff`
    - `implied_place_prob_minus_implied_win_prob`
  - `dual_market_plus_ratio`
    - `place_to_win_ratio`
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.model.comparison_cli --config configs/model_dual_market_derived_feature_comparison_2024_2025.toml
```

- この比較は
  - 2024-2025 dataset build
  - rolling retrain
  - `positive_window_count_then_mean_roi_then_min_roi` 固定での OOS backtest
  を feature set ごとに一気通しで回し、`summary.csv` / `summary.json` にまとめる
- 2024-2025 OOS の比較結果は以下
  - `dual_market`
    - AUC `0.8164`
    - logloss `0.4101`
    - Brier `0.1320`
    - BET `388`
    - 的中率 `0.5387`
    - 回収率 `1.0111`
    - 総利益 `430`
  - `dual_market_plus_log_diff`
    - AUC `0.8160`
    - logloss `0.4096`
    - Brier `0.1318`
    - BET `481`
    - 的中率 `0.5281`
    - 回収率 `0.9923`
    - 総利益 `-370`
  - `dual_market_plus_implied_probs`
    - AUC `0.8165`
    - logloss `0.4089`
    - Brier `0.1315`
    - BET `313`
    - 的中率 `0.5431`
    - 回収率 `1.0077`
    - 総利益 `240`
  - `dual_market_plus_prob_diff`
    - AUC `0.8159`
    - logloss `0.4099`
    - Brier `0.1319`
    - BET `482`
    - 的中率 `0.5187`
    - 回収率 `0.9761`
    - 総利益 `-1150`
  - `dual_market_plus_ratio`
    - AUC `0.8160`
    - logloss `0.4095`
    - Brier `0.1317`
    - BET `325`
    - 的中率 `0.5169`
    - 回収率 `0.9594`
    - 総利益 `-1320`
- 見方としては、
  - 派生特徴量を足すと classification 指標は少し改善するものがある
  - 特に `dual_market_plus_implied_probs` は AUC / logloss / Brier で最良だった
  - ただし OOS backtest では baseline の `dual_market` が依然として最良で、派生特徴量の追加は profit 改善に直結しなかった
  - つまり現時点では、`win + place` の raw market 表現自体は有効だが、単純な差分・比率派生を足すだけでは signal 強化は限定的
  - 次に進めるなら、派生特徴量を増やすより、選定条件や戦略側との整合を見直すほうが自然
- `dual_market` の model family 比較は、`configs/model_dual_market_model_family_comparison_2024_2025.toml` で再現できる
- 比較する model:
  - `dual_market_logreg`
    - `logistic_regression`
  - `dual_market_histgb_small`
    - `HistGradientBoostingClassifier` を使った小さな木系モデル
- feature は両方とも固定:
  - `log1p(win_odds)`
  - `log1p(place_basis_odds)`
  - `popularity`
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.model.comparison_cli --config configs/model_dual_market_model_family_comparison_2024_2025.toml
```

- この比較は
  - 2024-2025 dataset build
  - rolling retrain
  - `positive_window_count_then_mean_roi_then_min_roi` 固定での OOS backtest
  を model ごとに一気通しで回し、`summary.csv` / `summary.json` にまとめる
- 2024-2025 OOS の比較結果は以下
  - `dual_market_logreg`
    - AUC `0.8164`
    - logloss `0.4101`
    - Brier `0.1320`
    - BET `388`
    - 的中率 `0.5387`
    - 回収率 `1.0111`
    - 総利益 `430`
  - `dual_market_histgb_small`
    - AUC `0.8164`
    - logloss `0.4084`
    - Brier `0.1313`
    - BET `310`
    - 的中率 `0.5645`
    - 回収率 `1.0135`
    - 総利益 `420`
- 見方としては、
  - 小さな木系モデルは AUC はほぼ同水準で、logloss / Brier を少し改善した
  - OOS backtest でも `ROI 1.0135` と、`dual_market_logreg` の `1.0111` をわずかに上回った
  - ただし改善幅はまだかなり薄く、利益額はほぼ同水準
  - つまり現時点では「dual_market に少し非線形性を足す方向は有望」だが、まだ十分に頑健な改善とまでは言えない
  - 次に進めるなら、同じ dual market を保ったまま、木モデル側のごく小さな容量差や calibration を比較するのが自然
- `dual_market_histgb_small` の頑健性比較は、`configs/model_dual_market_histgb_small_robustness_2024_2025.toml` で再現できる
- 固定するもの:
  - feature set = `dual_market`
  - rolling OOS harness
  - `positive_window_count_then_mean_roi_then_min_roi`
- 比較するもの:
  - random seed:
    - `42`
    - `7`
    - `99`
  - 小さな parameter 差:
    - `learning_rate = 0.03, min_samples_leaf = 30`
    - `max_depth = 4, min_samples_leaf = 30`
    - `min_samples_leaf = 80`
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.model.comparison_cli --config configs/model_dual_market_histgb_small_robustness_2024_2025.toml
```

- 2024-2025 OOS の比較結果は以下
  - `histgb_seed_42`
    - AUC `0.8164`
    - logloss `0.4084`
    - Brier `0.1313`
    - BET `310`
    - 的中率 `0.5645`
    - 回収率 `1.0135`
    - 総利益 `430`
  - `histgb_seed_7`
    - AUC `0.8164`
    - logloss `0.4084`
    - Brier `0.1313`
    - BET `353`
    - 的中率 `0.5411`
    - 回収率 `0.9955`
    - 総利益 `-160`
  - `histgb_seed_99`
    - AUC `0.8164`
    - logloss `0.4084`
    - Brier `0.1313`
    - BET `398`
    - 的中率 `0.5603`
    - 回収率 `1.0186`
    - 総利益 `740`
  - `histgb_lr_0_03_leaf_30`
    - AUC `0.8163`
    - logloss `0.4093`
    - Brier `0.1314`
    - BET `290`
    - 的中率 `0.5552`
    - 回収率 `1.0086`
    - 総利益 `250`
  - `histgb_depth_4_leaf_30`
    - AUC `0.8163`
    - logloss `0.4084`
    - Brier `0.1313`
    - BET `361`
    - 的中率 `0.5457`
    - 回収率 `0.9972`
    - 総利益 `-100`
  - `histgb_leaf_80`
    - AUC `0.8164`
    - logloss `0.4084`
    - Brier `0.1313`
    - BET `329`
    - 的中率 `0.5380`
    - 回収率 `0.9702`
    - 総利益 `-980`
- 見方としては、
  - seed を変えただけでも `ROI 0.9955` から `1.0186` まで揺れる
  - つまり `dual_market_histgb_small` の改善は完全固定ではなく、まだ seed sensitivity がある
  - 一方で小さな parameter 差でも `1.0086` 前後を保つ条件はあり、signal 自体が完全に消えるわけでもない
  - `min_samples_leaf = 80` のようにやや保守的にすると ROI は明確に悪化した
  - 現時点の結論は「dual_market_histgb_small は有望だが、改善幅は薄く、まだ頑健と断言するには早い」
- `dual_market_histgb_small` の multi-seed ensemble 比較は、`configs/model_dual_market_histgb_ensemble_comparison_2024_2025.toml` で再現できる
- 比較する model:
  - `dual_market_logreg`
    - `logistic_regression`
  - `dual_market_histgb_small_seed_42`
    - `HistGradientBoostingClassifier`
    - `random_state = 42`
  - `dual_market_histgb_small_ensemble_42_7_99`
    - `HistGradientBoostingClassifier` を `42 / 7 / 99` の 3 seed で学習し、予測確率を単純平均した ensemble
- feature は全条件で固定:
  - `log1p(win_odds)`
  - `log1p(place_basis_odds)`
  - `popularity`
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.model.comparison_cli --config configs/model_dual_market_histgb_ensemble_comparison_2024_2025.toml
```

- この比較は
  - 2024-2025 dataset build
  - rolling retrain
  - `positive_window_count_then_mean_roi_then_min_roi` 固定での OOS backtest
  を 3 条件で一気通しに回し、`summary.csv` / `summary.json` に保存する
- 2024-2025 OOS の比較結果は以下
  - `dual_market_logreg`
    - AUC `0.8164`
    - logloss `0.4101`
    - Brier `0.1320`
    - BET `388`
    - 的中率 `0.5387`
    - 回収率 `1.0111`
    - 総利益 `430`
  - `dual_market_histgb_small_seed_42`
    - AUC `0.8164`
    - logloss `0.4084`
    - Brier `0.1313`
    - BET `310`
    - 的中率 `0.5645`
    - 回収率 `1.0135`
    - 総利益 `420`
  - `dual_market_histgb_small_ensemble_42_7_99`
    - AUC `0.8165`
    - logloss `0.4083`
    - Brier `0.1313`
    - BET `330`
    - 的中率 `0.5455`
    - 回収率 `0.9836`
    - 総利益 `-540`
- 見方としては、
  - 3-seed ensemble は classification 指標だけ見ると 3 条件の中で最良だった
  - ただし OOS backtest では `ROI 0.9836` まで落ち、single-seed の `dual_market_histgb_small_seed_42` や `dual_market_logreg` を下回った
  - つまり今回の strategy 条件では、確率平均で signal を滑らかにしすぎて、edge に効く尖りが弱まった可能性が高い
  - 現時点では ensemble 化より、single model のまま seed / parameter の頑健性を確認する方が自然
- dual market に市場近傍の少数特徴量を追加した比較は、`configs/model_dual_market_small_market_feature_comparison_2024_2025.toml` で再現できる
- 比較する feature set:
  - `dual_market_logreg`
    - `log1p(win_odds) + log1p(place_basis_odds) + popularity`
  - `dual_market_plus_headcount_logreg`
    - `dual_market + headcount`
  - `dual_market_plus_headcount_place_slots_logreg`
    - `dual_market + headcount + place_slot_count`
  - `dual_market_plus_headcount_place_slots_distance_logreg`
    - `dual_market + headcount + place_slot_count + distance_m`
- 補足:
  - `headcount` は `OZ` 由来の出走頭数 proxy
  - `place_slot_count` は最小近似として `headcount <= 7 -> 2`, `headcount >= 8 -> 3` で導出
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.model.comparison_cli --config configs/model_dual_market_small_market_feature_comparison_2024_2025.toml
```

- この比較は
  - 2024-2025 dataset build
  - rolling retrain
  - `positive_window_count_then_mean_roi_then_min_roi` 固定での OOS backtest
  を 4 条件で一気通しに回し、`summary.csv` / `summary.json` に保存する
- 2024-2025 OOS の比較結果は以下
  - `dual_market_logreg`
    - AUC `0.8164`
    - logloss `0.4101`
    - Brier `0.1320`
    - BET `388`
    - 的中率 `0.5387`
    - 回収率 `1.0111`
    - 総利益 `430`
  - `dual_market_plus_headcount_logreg`
    - AUC `0.8172`
    - logloss `0.4090`
    - Brier `0.1316`
    - BET `457`
    - 的中率 `0.5208`
    - 回収率 `0.9840`
    - 総利益 `-730`
  - `dual_market_plus_headcount_place_slots_logreg`
    - AUC `0.8173`
    - logloss `0.4089`
    - Brier `0.1315`
    - BET `395`
    - 的中率 `0.5241`
    - 回収率 `0.9851`
    - 総利益 `-590`
  - `dual_market_plus_headcount_place_slots_distance_logreg`
    - AUC `0.8173`
    - logloss `0.4089`
    - Brier `0.1315`
    - BET `361`
    - 的中率 `0.5125`
    - 回収率 `0.9532`
    - 総利益 `-1690`
- 見方としては、
  - `headcount` や `place_slot_count` を足すと classification 指標は少し改善した
  - ただし OOS backtest ではどの追加条件も baseline の `dual_market_logreg` を超えなかった
  - 特に `distance_m` まで足すと確率指標は維持される一方で、strategy 側の ROI はさらに悪化した
  - つまり現時点では、市場に近い少数特徴量を足すだけでは signal 強化につながらず、dual market の素の表現を保つ方が自然
  - 次に進めるなら、feature 追加より selection rule や market proxy との整合を再点検する方が優先
- `dual_market_logreg` と追加特徴量版の「選ばれた BET 集合」の差分分析は、`configs/bet_set_diff_dual_market_vs_headcount_2024_2025.toml` で再現できる
- 比較する組み合わせ:
  - `dual_market_logreg` vs `dual_market_plus_headcount_place_slots_logreg`
  - `dual_market_logreg` vs `dual_market_plus_headcount_logreg`
- 実行コマンド:

```bash
PYTHONPATH=src python -m horse_bet_lab.evaluation.bet_set_diff_cli --config configs/bet_set_diff_dual_market_vs_headcount_2024_2025.toml
```

- この分析は、各 model の `selected_summary.csv` に書かれた最終 test 条件をそのまま再適用して、採用された BET 集合を
  - `common`
  - `baseline_only`
  - `variant_only`
  に分解する
- 2024-2025 OOS の差分結果は以下
  - `dual_market_logreg` vs `dual_market_plus_headcount_place_slots_logreg`
    - `common`
      - BET `251`
      - 的中率 `0.5657`
      - 回収率 `1.0442`
      - 平均払戻 `184.6`
      - 平均 edge
        - baseline `0.1103`
        - variant `0.1128`
    - `baseline_only`
      - BET `135`
      - 的中率 `0.4815`
      - 回収率 `0.9393`
      - 平均払戻 `195.1`
      - 平均 edge `0.0745`
    - `variant_only`
      - BET `143`
      - 的中率 `0.4476`
      - 回収率 `0.8741`
      - 平均払戻 `195.3`
      - 平均 edge `0.0978`
  - `dual_market_logreg` vs `dual_market_plus_headcount_logreg`
    - `common`
      - BET `302`
      - 的中率 `0.5530`
      - 回収率 `1.0331`
      - 平均払戻 `186.8`
      - 平均 edge
        - baseline `0.1045`
        - variant `0.1058`
    - `baseline_only`
      - BET `84`
      - 的中率 `0.4762`
      - 回収率 `0.9155`
      - 平均払戻 `192.3`
      - 平均 edge `0.0735`
    - `variant_only`
      - BET `154`
      - 的中率 `0.4545`
      - 回収率 `0.8812`
      - 平均払戻 `193.9`
      - 平均 edge `0.0787`
- 分布 summary も `distribution.csv` / `distribution.json` に保存される
  - `place_basis_odds`
  - `popularity`
  - `win_odds`
  の bucket ごとの件数と share を確認できる
- 見方としては、
  - `common` 集合はどちらの比較でも `ROI > 1.0` で、共通に選ばれる BET はむしろ強い
  - 悪化の主因は `variant_only` にあり、追加特徴量版が新しく拾った BET 集合の質が低い
  - `headcount_place_slots` 版では `variant_only` の `ROI 0.8741` が特に重く、baseline 側の `baseline_only` より悪い
  - 分布を見ると、差分集合は `popularity 4-6` と `place_basis_odds 2.0-3.0` 側にまだ多く残っており、単に帯を外したというより「帯の中で拾う順番」が変わって質を落としている可能性が高い
  - つまり今回の追加特徴量は、signal を強化するというより、もともと良かった集合の外側を余計に拾ってしまったと読むのが自然
- frontier 差分分析では、同じ差分集合をさらに
  - `popularity bucket`
  - `place_basis_odds bucket`
  - `edge bucket`
  - `win_odds bucket`
  で分解した `distribution.csv` / `distribution.json` を確認できる
- `dual_market_logreg` vs `dual_market_plus_headcount_place_slots_logreg` で見えること:
  - `common` は `edge 0.08-0.12` が中心で、`ROI 1.0442`
  - `baseline_only` は `edge 0.04-0.08` が多く、`ROI 0.9393`
  - `variant_only` は `edge 0.06-0.08` と `0.08-0.12` が中心で、見かけの edge はむしろ高いのに `ROI 0.8741`
  - `variant_only` の `popularity 4-6` は `58.0%` で、`baseline_only` の `47.4%` よりさらに外側へ広がっている
  - `variant_only` の `place_basis_odds 2.0-3.0` は `84.6%` で、帯そのものは悪くない
  - つまり追加特徴量版は「悪い帯」を拾ったというより、良い帯の中でも `4-6人気` 寄りで payout の弱い BET を多く混ぜている可能性が高い
- race 単位の代表例は `race_examples.csv` / `race_examples.json` に出力される
  - 例: `01242201`
    - baseline_only: 馬番 `7`, avg_edge `0.0522`, win_odds `8.0`, place_basis_odds `3.3`
    - variant_only: 馬番 `1`, avg_edge `0.0681`, win_odds `6.2`, place_basis_odds `2.1`
  - このように、追加特徴量版は同レース内で「より低 place_basis_odds / より高 edge」に見える馬へ入れ替えているのに、その入れ替えが ROI を悪化させるケースがある
- したがって次に見るべきなのは、
  - edge の絶対値そのもの
  よりも、
  - `popular 1-3` 内で `4-6人気` 側へずれていないか
  - `place_basis_odds 2.0-3.0` 内で採用順位がどう変わるか
  を ranking 観点で見ること
  だと考えられる
- `dual_market_logreg` を固定した ranking score 比較は、`configs/ranking_score_diff_dual_market_logreg_2024_2025.toml` で再現できる
  - baseline は現行の `edge threshold` 採用集合
  - variant は、同じ market 条件の候補集合から baseline と同じ BET 数だけ score 順に取り直す
  - 比較 score:
    - `edge`
    - `edge_times_place_basis_odds`
    - `pred_times_place_basis_odds`
    - `edge_div_place_basis_odds`
  - 出力:
    - `summary.csv/json`
    - `diff_summary.csv/json`
    - `distribution.csv/json`
    - `race_examples.csv/json`
- 2024-2025 OOS では、baseline `edge threshold` は `BET 388 / ROI 1.0111`
- `edge_times_place_basis_odds` と `pred_times_place_basis_odds` は同じ採用集合になり、`BET 388 / ROI 1.0129` までわずかに改善した
  - `common` は `BET 361 / ROI 1.0097`
  - `baseline_only` は `BET 25 / ROI 0.9760 / avg_edge 0.0739 / avg_payout 174.3`
  - `score_variant_only` は `BET 25 / ROI 1.0040 / avg_edge 0.0645 / avg_payout 251.0`
  - つまり、少し低 edge の BET を増やしているが、`win_odds 5-10` と `place_basis_odds 3.0-5.0` 側へ寄せて、平均払戻の厚さでわずかに上回っている
  - `score_variant_only` の分布は
    - `popularity 4-6`: `84.0%`
    - `place_basis_odds 3.0-5.0`: `40.0%`
    - `edge 0.06-0.08`: `64.0%`
    - `win_odds 5-10`: `100.0%`
- 逆方向の `edge_div_place_basis_odds` は `BET 388 / ROI 1.0005` まで悪化した
  - `baseline_only` は `BET 23 / ROI 1.2304`
  - `score_variant_only` は `BET 23 / ROI 1.0522`
  - こちらは `place_basis_odds` の高い馬を削って低い馬へ寄せるため、払戻の厚みを落として baseline を下回った
- 今回の結論は、
  - `edge` だけで並べるより、`edge * place_basis_odds` のような軽い payout-aware score は試す価値がある
  - ただし改善幅はかなり薄く、まだ頑健な優位とまでは言えない
- `pred_times_place_basis_odds` を正式な ranking rule 候補として rolling OOS に組み込んだ比較は、`configs/ranking_rule_compare_dual_market_logreg_2024_2025.toml` で再現できる
  - 固定条件:
    - `dual_market_logreg`
    - `market_prob_method = oz_place_basis_inverse`
    - `popularity 1-3`
    - `place_basis_odds` の有望 band 候補
    - `aggregate_selection_score_rule = positive_window_count_then_mean_roi_then_min_roi`
    - `min_bets_valid = 10`
  - 正式比較する ranking rule:
    - `edge`
    - `pred_times_place_basis_odds`
  - 出力:
    - `summary.csv/json`
    - `selected_summary.csv/json`
    - `selected_test_rollup.csv/json`
    - `diff_summary.csv/json`
    - `strategy_variant_summary.csv/json`
    - `strategy_variant_rollup.csv/json`
    - `strategy_variant_diff_summary.csv/json`
- 2024-2025 rolling OOS では、正式比較に組み込んでも `pred_times_place_basis_odds` の改善は残った
  - `edge`
    - `BET 388`
    - `hit_rate 0.5387`
    - `ROI 1.0111`
    - `profit 430`
    - `avg_payout 187.7`
    - `avg_edge 0.0976`
  - `pred_times_place_basis_odds`
    - `BET 356`
    - `hit_rate 0.5421`
    - `ROI 1.0320`
    - `profit 1140`
    - `avg_payout 190.4`
    - `avg_edge 0.0998`
- `consensus` 比較も同じ artifact に含めており、「`edge` と `pred_times_place_basis_odds` の両方で採用された BET のみ」を使う
  - `consensus`
    - `BET 331`
    - `hit_rate 0.5498`
    - `ROI 1.0381`
    - `profit 1260`
    - `avg_payout 188.8`
    - `avg_edge 0.1018`
- 差分の読み方:
  - `common` は `BET 329 / ROI 1.0340` で共通採用集合そのものは強い
  - `edge_only` は `BET 57 / ROI 0.8544` で弱く、`edge` 単独側に残る BET が重い
  - `pred_only` は `BET 25 / ROI 0.9520` でまだ 1.0 未満だが、`edge_only` よりは悪化が小さい
  - そのため `consensus` は、弱い disagreement BET を両側から落としたことで、今回の 3 variant では最良になっている
- 現時点の解釈としては、
  - `pred_times_place_basis_odds` は正式比較に入れても候補として残す価値がある
  - さらに、`edge` と `pred_times_place_basis_odds` の disagreement を削る `consensus` は、現時点では最も頑健寄りな候補に見える
  - ただし `pred_only` 自体はまだ `ROI < 1.0` なので、強い新規 signal を拾えているというより、「弱い BET を減らす」タイプの改善と読むのが自然
- 現時点の reference strategy 診断は、`configs/reference_strategy_dual_market_logreg_2024_2025.toml` で再現できる
  - 固定条件:
    - `dual_market_logreg`
    - `market_prob_method = oz_place_basis_inverse`
    - `popularity 1-3`
    - `place_basis_odds` の有望 band 候補
    - `aggregate_selection_score_rule = positive_window_count_then_mean_roi_then_min_roi`
    - `consensus ranking`
  - 出力:
    - `summary.csv/json`
    - `equity_curve.csv/json`
    - `monthly_profit.csv/json`
    - `window_profit.csv/json`
- 2024-2025 rolling OOS の reference strategy 指標
  - `BET 331`
  - `hit_rate 0.5498`
  - `ROI 1.0381`
  - `profit 1260`
  - `avg_payout 188.8`
  - `avg_edge 0.1018`
  - `max_drawdown 2330`
  - `max_losing_streak 6`
- 月別損益の見え方:
  - 弱い月:
    - `2024-08: -1010`
    - `2024-09: -610`
    - `2025-07: -350`
  - 強い月:
    - `2025-10: +740`
    - `2025-05: +660`
    - `2025-02: +490`
    - `2025-12: +490`
- 読み方:
  - 合算ではプラスだが、2024-08〜09 の連続悪化で DD が深い
  - `consensus` は ROI 観点では現状最良だが、「実務的に十分安定」とまではまだ言えない
  - 次に進めるなら、window をさらに増やすか、stake sizing なしのままで DD を抑える条件比較を追加するのが自然
- 2023 raw も利用可能だったため、reference strategy の OOS window を `2023-07` から `2025-12` まで広げた再現性確認も追加した
  - config:
    - `configs/model_dual_market_reference_comparison_2023_2025.toml`
    - `configs/ranking_rule_compare_dual_market_logreg_2023_2025.toml`
    - `configs/reference_strategy_dual_market_logreg_2023_2025.toml`
  - 2023-2025 の `consensus` 合算結果:
    - `BET 525`
    - `hit_rate 0.4895`
    - `ROI 0.9383`
    - `profit -3240`
    - `avg_payout 191.7`
    - `avg_edge 0.1021`
    - `max_drawdown 5090`
    - `max_losing_streak 7`
  - 月別 / window 別で特に弱かったのは
    - `2023-09: ROI 0.6136 / profit -850 / max_drawdown 950`
    - `2023-11: ROI 0.7882 / profit -720 / max_drawdown 1250`
    - `2024-01: ROI 0.7531 / profit -1210 / max_drawdown 1570`
  - 強かった月は
    - `2025-10: ROI 1.7231 / profit 940`
    - `2025-05: ROI 1.3438 / profit 550`
    - `2025-02: ROI 1.2600 / profit 390`
  - 重要なのは、2024-2025 だけではプラスだった `consensus` が、2023 を含めるとマイナスに戻ること
  - つまり現時点の reference strategy は「最近 2 年では有望」だが、「より長い OOS ではまだ再現性不足」と判断するのが自然
- `2023` と `2024-2025` の regime diff 分析は、`configs/regime_diff_reference_strategy_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/regime_diff_reference_strategy_dual_market_logreg_2023_2025.toml`
  - 出力:
    - `data/artifacts/regime_diff_reference_strategy_dual_market_logreg_2023_2025/summary.csv`
    - `distribution.csv`
    - `condition_band_summary.csv`
    - `condition_band_diff.csv`
    - `representative_examples.csv`
  - regime summary:
    - `2023: BET 148 / hit_rate 0.4054 / ROI 0.7730 / profit -3360 / avg_payout 190.7 / avg_edge 0.0825`
    - `2024-2025: BET 377 / hit_rate 0.5225 / ROI 1.0032 / profit 120 / avg_payout 192.0 / avg_edge 0.1098`
  - 分布差で目立つ点:
    - `2023` は `popularity=3` が特に弱く、`ROI 0.4982`
    - `2023` の `place_basis_odds 2.4-2.8` も弱く、`ROI 0.5699`
    - `2024-2025` では同じ近辺がかなり改善し、特に `popularity=1 x place_basis_odds 2.4-2.8` は `ROI 1.1293`
  - 条件帯 diff で最も重かったのは:
    - `popularity=3 x place_basis_odds 2.4-2.8`
    - `2023: BET 27 / ROI 0.2000 / profit -2160`
    - `2024-2025: BET 33 / ROI 0.8364 / profit -540`
  - 一方で、`2023` の中でも一様に弱いわけではなく
    - `popularity=1 x place_basis_odds 2.0-2.4` は `ROI 1.3333`
    - `popularity=2 x place_basis_odds 2.8-3.2` は `ROI 1.2143`
    と、帯によって質がかなり違う
  - つまり `2023` の弱さは単なる「全体的レジーム悪化」だけではなく、特定の条件帯、特に `popularity=3 x place_basis_odds 2.4-2.8` 付近の崩れが大きいと読むのが自然
- `2023` で弱かった帯 `popularity=3 x place_basis_odds 2.4-2.8` の within-band ranking / calibration diff は、`configs/within_band_regime_diff_reference_strategy_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/within_band_regime_diff_reference_strategy_dual_market_logreg_2023_2025.toml`
  - 出力:
    - `data/artifacts/within_band_regime_diff_reference_strategy_dual_market_logreg_2023_2025/summary.csv`
    - `ranking_summary.csv`
    - `calibration_summary.csv`
    - `finish_position_distribution.csv`
    - `representative_examples.csv`
  - regime 内 summary:
    - `2023 all: BET 241 / hit_rate 0.2905 / ROI 0.6867 / profit -7550 / avg_pred 0.3947 / avg_market_prob 0.3928 / avg_edge 0.0019 / avg_payout 236.4`
    - `2023 adopted: BET 52 / hit_rate 0.2115 / ROI 0.4135 / profit -3050 / avg_pred 0.4467 / avg_market_prob 0.3776 / avg_edge 0.0691 / avg_payout 195.5`
    - `2023 non_adopted: BET 189 / hit_rate 0.3122 / ROI 0.7619 / profit -4500 / avg_pred 0.3804 / avg_market_prob 0.3970 / avg_edge -0.0165 / avg_payout 244.1`
    - `2024-2025 adopted: BET 45 / hit_rate 0.5778 / ROI 1.1178 / profit 530 / avg_pred 0.4693 / avg_market_prob 0.3710 / avg_edge 0.0982 / avg_payout 193.5`
    - `2024-2025 non_adopted: BET 738 / hit_rate 0.3713 / ROI 0.8785 / profit -8970 / avg_pred 0.3885 / avg_market_prob 0.3927 / avg_edge -0.0042 / avg_payout 236.6`
  - 重要なのは、`2023` ではこの帯の中で
    - 採用側のほうが `avg_edge` は高い
    - しかし `ROI` はむしろかなり悪い
    という逆転が起きていること
  - calibration 的に見ると:
    - `2023 adopted` は `pred 0.45-0.50` で `ROI 0.148`
    - `2024-2025 adopted` は同じ `pred 0.45-0.50` で `ROI 1.1594`
    - `2023 adopted` は `edge 0.08-0.10` でも `ROI 0.2000`
    - `2024-2025 adopted` は `edge 0.08-0.10` で `ROI 1.1591`
  - つまりこの帯では、`2023` だけ prediction/edge が payout に結び付かず、ranking の calibration が崩れている
  - さらに `2023 non_adopted` には
    - `2023-12-02 / race_key 09235103 / horse 8 / pred 0.2942 / edge -0.1058 / payout 990`
    - `2023-07-23 / race_key 01231208 / horse 3 / pred 0.3009 / edge -0.0837 / payout 620`
    のような「低 pred・負 edge なのに大きく走る」例が残っていて、これが帯全体の質を崩している
  - 一方で ranking diff の平均順位はほぼ差が出ていない
    - この帯では race ごとに候補が 1 頭しかいないケースが大半で、順位情報より calibration の崩れのほうが本質に近い
- `2023` と `2024-2025` における market proxy / model prediction の年別 calibration drift は、`configs/calibration_drift_reference_strategy_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/calibration_drift_reference_strategy_dual_market_logreg_2023_2025.toml`
  - 出力:
    - `data/artifacts/calibration_drift_reference_strategy_dual_market_logreg_2023_2025/summary.csv`
    - `bucket_summary.csv`
    - `representative_examples.csv`
  - regime summary:
    - `2023: BET 241 / adopted 52 / hit_rate 0.2905 / ROI 0.6867 / profit -7550 / avg_pred 0.3947 / avg_market_prob 0.3928 / avg_edge 0.0019`
    - `2024-2025: BET 783 / adopted 45 / hit_rate 0.3831 / ROI 0.8922 / profit -8440 / avg_pred 0.3932 / avg_market_prob 0.3915 / avg_edge 0.0017`
    - 重要なのは year-wise bias で、
      - `2023: pred - empirical = +0.1043 / market - empirical = +0.1023`
      - `2024-2025: pred - empirical = +0.0100 / market - empirical = +0.0083`
      と、`2023` では model と market proxy の両方がこの帯の実際の hit rate をかなり過大評価している
  - bucket で見ると:
    - `2023 pred 0.45-0.50: BET 37 / hit_rate 0.2162 / ROI 0.4081`
    - `2024-2025 pred 0.45-0.50: BET 93 / hit_rate 0.4731 / ROI 0.9602`
    - `2023 edge 0.08-0.10: BET 10 / hit_rate 0.1000 / ROI 0.2000`
    - `2024-2025 edge 0.08-0.10: BET 29 / hit_rate 0.5862 / ROI 1.1552`
    - `2023 market_prob 0.40-0.42: BET 131 / hit_rate 0.2748 / ROI 0.6435`
    - `2024-2025 market_prob 0.40-0.42: BET 403 / hit_rate 0.3846 / ROI 0.8876`
  - 代表例:
    - `2023 high_pred_miss: 2023-11-18 / race_key 08233507 / horse 2 / pred 0.4893 / market_prob 0.4000 / edge 0.0893 / payout 0`
    - `2023 negative_edge_hit: 2023-11-12 / race_key 08233408 / horse 10 / pred 0.2870 / market_prob 0.4167 / edge -0.1296 / payout 220`
    - `2024-2025 low_pred_hit: 2025-06-29 / race_key 03252204 / horse 1 / pred 0.5030 / market_prob 0.4167 / edge 0.0863 / payout 160`
  - 読み方:
    - `2023` の弱さは「採用判定だけが悪い」のではなく、この帯そのものに calibration drift がある
    - しかも drift は model だけでなく market proxy にも見えている
    - つまり現時点では band 内の ranking 改善より先に、「この帯は 2023 ではそもそも期待値解釈が崩れていた」と見るほうが自然
- `2023` で壊れていた帯への band-specific guard 比較は、`configs/reference_guard_compare_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_guard_compare_dual_market_logreg_2023_2025.toml`
  - problematic band:
    - `popularity = 3`
    - `place_basis_odds = 2.4-2.8`
  - 比較 variant:
    - `baseline`
    - `problematic_band_excluded`
    - `problematic_band_edge_plus_0_02`
    - `problematic_band_edge_plus_0_04`
    - `problematic_band_edge_plus_0_06`
  - 出力:
    - `data/artifacts/reference_guard_compare_dual_market_logreg_2023_2025/summary.csv`
    - `yearly_summary.csv`
    - `window_summary.csv`
    - `equity_curve.csv`
  - 2023-2025 合算:
    - `baseline: BET 524 / hit_rate 0.4885 / ROI 0.9363 / profit -3340 / max_drawdown 5090 / max_losing_streak 7`
    - `problematic_band_excluded: BET 427 / hit_rate 0.5129 / ROI 0.9808 / profit -820 / max_drawdown 2360 / max_losing_streak 7`
    - `problematic_band_edge_plus_0_02: BET 427 / hit_rate 0.5129 / ROI 0.9808 / profit -820 / max_drawdown 2360 / max_losing_streak 7`
    - `problematic_band_edge_plus_0_04: BET 427 / hit_rate 0.5129 / ROI 0.9808 / profit -820 / max_drawdown 2360 / max_losing_streak 7`
    - `problematic_band_edge_plus_0_06: BET 427 / hit_rate 0.5129 / ROI 0.9808 / profit -820 / max_drawdown 2360 / max_losing_streak 7`
  - 年別では:
    - `baseline 2023: BET 148 / ROI 0.7730 / profit -3360 / max_drawdown 3360`
    - `guarded 2023: BET 108 / ROI 0.9028 / profit -1050 / max_drawdown 1350`
    - `baseline 2024: BET 216 / ROI 0.9583 / profit -900 / max_drawdown 1770`
    - `guarded 2024: BET 181 / ROI 0.9790 / profit -380 / max_drawdown 1070`
    - `baseline 2025: BET 160 / ROI 1.0575 / profit 920 / max_drawdown 1090`
    - `guarded 2025: BET 138 / ROI 1.0442 / profit 610 / max_drawdown 1120`
  - 解釈:
    - 問題帯を完全除外すると、長い OOS の損失はかなり縮む
    - 特に `2023` の悪化と DD を大きく抑えられる
    - 一方で `+0.02/+0.04/+0.06` がすべて完全除外と同じ結果になったため、この帯で採用されていた BET は baseline edge threshold に対してすでにかなり薄く、少し guard を掛けるだけで全て落ちる
    - つまり next step としては、edge surcharge を細かく詰めるより、「この帯を丸ごと除外するかどうか」を policy として比較するほうが自然
- guarded reference strategy の residual-loss 分析は、`configs/residual_loss_analysis_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/residual_loss_analysis_dual_market_logreg_2023_2025.toml`
  - 出力:
    - `data/artifacts/residual_loss_analysis_dual_market_logreg_2023_2025/summary.csv`
    - `bucket_summary.csv`
    - `top_loss_bands.csv`
    - `representative_examples.csv`
  - regime summary:
    - `baseline 2023: BET 148 / hit_rate 0.4054 / ROI 0.7730 / profit -3360 / avg_payout 190.7 / avg_edge 0.0825`
    - `baseline 2024-2025: BET 376 / hit_rate 0.5213 / ROI 1.0005 / profit 20 / avg_payout 191.9 / avg_edge 0.1098`
    - `problematic_band_excluded 2023: BET 108 / hit_rate 0.4722 / ROI 0.9028 / profit -1050 / avg_payout 191.2 / avg_edge 0.0877`
    - `problematic_band_excluded 2024-2025: BET 319 / hit_rate 0.5266 / ROI 1.0072 / profit 230 / avg_payout 191.3 / avg_edge 0.1130`
  - guard 後にまだ大きく負けている帯:
    - `2023`
      - `win_odds < 5: BET 66 / ROI 0.8182 / profit -1200`
      - `place_basis_odds 2.4-2.8: BET 46 / ROI 0.7870 / profit -980`
      - `edge 0.10-0.12: BET 19 / ROI 0.6474 / profit -670`
      - `edge 0.08-0.10: BET 25 / ROI 0.7480 / profit -630`
    - `2024-2025`
      - `place_basis_odds 3.2-4.0: BET 29 / ROI 0.5897 / profit -1190`
      - `edge < 0.06: BET 14 / ROI 0.3857 / profit -860`
      - `win_odds 5-10: BET 125 / ROI 0.9320 / profit -850`
  - 読み方:
    - guard により `2023` の最悪帯はかなり落とせたが、残る損失は別の帯へ移った
    - `2023` では依然として
      - 低めの `win_odds`
      - `place_basis_odds 2.4-2.8` の残存分
      - `edge 0.08+`
      が弱い
    - `2024-2025` では guarded 戦略はほぼ収支トントンまで改善するが、`place_basis_odds 3.2-4.0` と低 edge 帯が重荷
    - つまり next step としては、単純な global threshold 調整より、regime や帯ごとの追加 guard を比較するほうが自然
- guarded reference strategy の低払戻・元返し診断と軽い第二 guard 比較は、`configs/reference_guard_compare_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_guard_compare_dual_market_logreg_2023_2025.toml`
  - 追加した軽い第二 guard:
    - `problematic_band_excluded_win_odds_lt_5_excluded`
    - `problematic_band_excluded_edge_lt_0_06_excluded`
  - 2023-2025 合算:
    - `baseline: BET 524 / hit_rate 0.4885 / ROI 0.9363 / profit -3340 / max_drawdown 5090 / max_losing_streak 7`
    - `problematic_band_excluded: BET 427 / hit_rate 0.5129 / ROI 0.9808 / profit -820 / max_drawdown 2360 / max_losing_streak 7`
    - `problematic_band_excluded_edge_lt_0_06_excluded: BET 391 / hit_rate 0.5243 / ROI 0.9949 / profit -200 / max_drawdown 1530 / max_losing_streak 6`
    - `problematic_band_excluded_win_odds_lt_5_excluded: BET 167 / hit_rate 0.4491 / ROI 0.9581 / profit -700 / max_drawdown 1460 / max_losing_streak 7`
  - 年別では:
    - `edge < 0.06` 除外
      - `2023: BET 86 / ROI 0.8500 / profit -1290`
      - `2024: BET 167 / ROI 1.0287 / profit 480`
      - `2025: BET 138 / ROI 1.0442 / profit 610`
    - `win_odds < 5` 除外
      - `2023: BET 42 / ROI 1.0357 / profit 150`
      - `2024: BET 72 / ROI 0.8958 / profit -750`
      - `2025: BET 53 / ROI 0.9811 / profit -100`
  - 低払戻診断:
    - `baseline` と `problematic_band_excluded` の採用 BET では
      - `元返し (100円) 件数 = 0`
      - `100-110円帯件数 = 0`
      - `120円以下件数 = 0`
    - つまり今回の residual loss は「元返しだらけ」ではなく、もう少し上の薄い払戻帯と miss の積み重なりで起きている
  - 読み方:
    - `edge < 0.06` 除外は、guard 後の residual loss をさらにかなり削る
    - 特に `2024` をプラス転換しつつ、全体の `max_drawdown` を `2360 -> 1530`、`max_losing_streak` を `7 -> 6` に改善する
    - 一方で `win_odds < 5` 除外は `2023` には効くが、`2024-2025` を削りすぎて全体ではそこまで良くない
    - 次の候補としては、`edge < 0.06` guard を基準にさらに regime 差を見るのが自然
- first guard の上に載せる second guard を valid で選定する比較は、`configs/second_guard_selection_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/second_guard_selection_dual_market_logreg_2023_2025.toml`
  - first guard 固定:
    - `problematic_band_excluded`
  - second guard 候補:
    - `no_second_guard`
    - `problematic_band_excluded_edge_lt_0_06_excluded`
    - `problematic_band_excluded_win_odds_lt_5_excluded`
  - 選定ルール:
    - 各 test 窓に対して、その直前までの valid 窓合算 ROI 最大の second guard を選ぶ
    - tie-break は `valid BET 数`、さらに候補の固定順
  - 出力:
    - `data/artifacts/second_guard_selection_dual_market_logreg_2023_2025/candidate_summary.csv`
    - `selected_summary.csv`
    - `selected_test_rollup.csv`
  - 2023-2025 合算:
    - `selected_per_window: BET 276 / hit_rate 0.5181 / ROI 1.0207 / profit 570 / max_drawdown 1310 / max_losing_streak 5`
  - 年別:
    - `2023: BET 54 / ROI 0.9630 / profit -200 / max_drawdown 640`
    - `2024: BET 106 / ROI 0.9877 / profit -130 / max_drawdown 840`
    - `2025: BET 116 / ROI 1.0776 / profit 900 / max_drawdown 770`
  - 読み方:
    - `fixed first guard` だけだと `ROI 0.9808 / max_drawdown 2360`
    - `fixed edge < 0.06 second guard` でも `ROI 0.9949 / max_drawdown 1530`
    - そこから `valid で second guard を選ぶ` と `ROI 1.0207 / max_drawdown 1310` まで改善した
    - 実際の選定は
      - `2023 前半〜2024 前半` では `win_odds < 5` 除外
    - `2024 後半〜2025` では `edge < 0.06` 除外
      に寄っていて、regime 差をある程度吸収している
    - つまり next step としては、手動で年条件を決め打つ前に、`valid` ベースの軽い guard 選定ルールを reference strategy の本線候補にするのが自然
- valid-selected second guard reference strategy の不確実性評価は、`configs/reference_uncertainty_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_uncertainty_dual_market_logreg_2023_2025.toml`
  - 固定する strategy:
    - `first guard = problematic_band_excluded`
    - `second guard = valid-selected`
  - 出力:
    - `data/artifacts/reference_uncertainty_dual_market_logreg_2023_2025/summary.csv`
    - `bootstrap_distribution.csv`
    - `yearly_contribution.csv`
  - 実データ summary:
    - `BET 276 / hit_rate 0.5181 / ROI 1.0207 / profit 570 / avg_payout 197.0 / avg_edge 0.1044 / max_drawdown 1310 / max_losing_streak 5`
  - bootstrap:
    - `ROI 95% interval: 0.9033 - 1.1348`
    - `profit 95% interval: -2670 - 3720`
    - `max_drawdown 95% interval: 800 - 3620`
    - `ROI > 1.0 ratio: 0.6345`
  - 年別寄与:
    - `2023: BET 54 / ROI 0.9630 / profit -200`
    - `2024: BET 106 / ROI 0.9877 / profit -130`
    - `2025: BET 116 / ROI 1.0776 / profit 900`
  - 読み方:
    - point estimate はプラスだが、bootstrap では `ROI < 1.0` のケースもかなり残る
    - つまり現時点の reference strategy は「有望」ではあるが、「かなり頑健」とまではまだ言えない
    - 利益寄与は `2025` に偏っていて、`2023 / 2024` はまだ単年では弱い
    - 次の候補としては、DD 最適化より前に `2023 / 2024` 側の弱さをもう一段減らす guard 比較を続けるのが自然
- reference strategy の regime label 最小比較は、`configs/reference_regime_label_diff_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_regime_label_diff_dual_market_logreg_2023_2025.toml`
  - 固定する strategy:
    - `dual_market_logreg`
    - `first guard = problematic_band_excluded`
    - `second guard = valid-selected`
  - 軸:
    - `month`
    - `headcount bucket`
    - `distance bucket`
    - `popularity bucket`
    - `place_basis_odds bucket`
    - `win_odds bucket`
  - 出力:
    - `data/artifacts/reference_regime_label_diff_dual_market_logreg_2023_2025/summary.csv`
    - `distribution.csv`
    - `stronger_regimes.csv`
    - `weak_in_both_regimes.csv`
    - `representative_examples.csv`
  - summary:
    - `2023: BET 54 / hit_rate 0.4630 / ROI 0.9630 / profit -200 / avg_payout 208.0 / avg_edge 0.0760`
    - `2024-2025: BET 222 / hit_rate 0.5315 / ROI 1.0347 / profit 770 / avg_payout 194.7 / avg_edge 0.1114`
  - `2023 で弱く、2024-2025 で相対的に強い` label の例:
    - `place_basis_odds 2.0-2.4`
      - `2023: BET 9 / ROI 0.5333 / profit -420`
      - `2024-2025: BET 23 / ROI 1.4783 / profit 1100`
    - `win_odds < 5`
      - `2023: BET 12 / ROI 0.7083 / profit -350`
      - `2024-2025: BET 97 / ROI 1.1670 / profit 1620`
    - `headcount 8-11`
      - `2023: BET 2 / ROI 0.0000 / profit -200`
      - `2024-2025: BET 11 / ROI 1.3364 / profit 370`
    - `month 09`
      - `2023: BET 6 / ROI 0.6333 / profit -220`
      - `2024-2025: BET 17 / ROI 1.1353 / profit 230`
  - `両方で弱い` label の例:
    - `popularity 3-4`
      - `2023: BET 14 / ROI 0.6357 / profit -510`
      - `2024-2025: BET 66 / ROI 0.8742 / profit -830`
    - `month 07`
      - `2023: BET 14 / ROI 0.8929 / profit -150`
      - `2024-2025: BET 17 / ROI 0.7412 / profit -440`
    - `month 08`
      - `2023: BET 8 / ROI 0.8875 / profit -90`
      - `2024-2025: BET 16 / ROI 0.8000 / profit -320`
  - 読み方:
    - `2023 は弱い / 2025 は強い` をそのまま年差で終わらせるより、まず `popularity 3-4`, `win_odds < 5`, `7-9月`, `1400-1799m` のような安定列で言い直せるかを見る
    - 今回の reference strategy では、`2023` の弱さは特に `popularity 3-4` と `win_odds < 5` に寄り、`2024-2025` の改善は `place_basis_odds 2.0-2.4` と `headcount 8-11` が強い
    - ただし `2024-2025` でも `month 07/08` と `popularity 3-4` は still 弱いので、単純な年レジームではなく `label regime` として扱うほうが自然
- reference strategy に対する global weak-label guard 比較は、`configs/reference_label_guard_compare_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_label_guard_compare_dual_market_logreg_2023_2025.toml`
  - 固定する前提:
    - `dual_market_logreg`
    - `first guard = problematic_band_excluded`
    - `second guard = valid-selected`
  - extra guard 候補:
    - `no_extra_label_guard`
    - `popularity_3_4_excluded`
    - `month_07_08_excluded`
    - `popularity_3_4_or_month_07_08_excluded`
  - 出力:
    - `data/artifacts/reference_label_guard_compare_dual_market_logreg_2023_2025/candidate_summary.csv`
    - `candidate_test_rollup.csv`
    - `selected_summary.csv`
    - `selected_test_rollup.csv`
  - fixed candidate 比較:
    - `no_extra_label_guard`
      - `BET 276 / hit_rate 0.5181 / ROI 1.0207 / profit 570 / max_drawdown 1310 / max_losing_streak 5`
    - `popularity_3_4_excluded`
      - `BET 196 / hit_rate 0.5714 / ROI 1.0974 / profit 1910 / max_drawdown 920 / max_losing_streak 4`
    - `month_07_08_excluded`
      - `BET 221 / hit_rate 0.5475 / ROI 1.0710 / profit 1570 / max_drawdown 1010 / max_losing_streak 5`
    - `popularity_3_4_or_month_07_08_excluded`
      - `BET 160 / hit_rate 0.6063 / ROI 1.1669 / profit 2670 / max_drawdown 750 / max_losing_streak 4`
  - fixed candidate 年別:
    - `popularity_3_4_or_month_07_08_excluded`
      - `2023: BET 22 / ROI 1.2909 / profit 640`
      - `2024: BET 58 / ROI 1.1500 / profit 870`
      - `2025: BET 80 / ROI 1.1450 / profit 1160`
  - valid-selected extra guard:
    - `selected_per_window`
      - `BET 189 / hit_rate 0.5820 / ROI 1.1249 / profit 2360 / max_drawdown 750 / max_losing_streak 4`
    - 年別:
      - `2023: BET 48 / ROI 1.0417 / profit 200`
      - `2024: BET 61 / ROI 1.1639 / profit 1000`
      - `2025: BET 80 / ROI 1.1450 / profit 1160`
  - 読み方:
    - `popularity 3-4` は 2023/2024-2025 の両方で弱かったので、global weak-label guard として効きやすい
    - `month 07/08` も単独で改善するが、`popularity 3-4` のほうが寄与が大きい
    - 両方をまとめて除外すると fixed candidate 比較では最良で、DD もかなり浅くなる
    - valid 選定でも大きく崩れず、`selected_per_window` は `ROI 1.1249 / max_drawdown 750` とかなり改善した
    - つまり next step は、年条件つき手動 guard より前に、この global weak-label guard を reference strategy の本線候補として扱うのが自然
- valid-selected extra label guard の selection bias null test は、`configs/reference_label_guard_null_test_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_label_guard_null_test_dual_market_logreg_2023_2025.toml`
  - 前提:
    - valid-selected extra label guard は temporal には正しいが、複数候補から valid ROI 最大を選ぶので selection bias を含みうる
    - null test では valid 側の payout label だけを shuffle し、test 側の評価は実データのまま使う
    - 現在は 2 種の null を比較する
      - `current_shuffle`
        - valid rows 全体で row-level shuffle
      - `race_internal_permutation`
        - race ごとに payout label を permutation
  - 出力:
    - `data/artifacts/reference_label_guard_null_test_dual_market_logreg_2023_2025/summary.csv`
    - `null_distribution.csv`
    - `null_selected_guards.csv`
  - 実測 vs null:
    - 実測:
      - `test ROI 1.1249 / test profit 2360`
    - `current_shuffle`
      - `test ROI 95% interval: 1.0257 - 1.1148`
      - `test ROI median: 1.0663`
      - `test profit 95% interval: 619.8 - 2510.0`
      - `test profit median: 1490.0`
      - `observed ROI percentile in null = 0.995`
      - `observed profit percentile in null = 0.954`
      - `observed ROI - null median ROI = +0.0586`
      - `observed profit - null median profit = +870`
    - `race_internal_permutation`
      - `test ROI 95% interval: 1.0888 - 1.1305`
      - `test ROI median: 1.1113`
      - `test profit 95% interval: 1750.0 - 2480.0`
      - `test profit median: 2170.0`
      - `observed ROI percentile in null = 0.921`
      - `observed profit percentile in null = 0.921`
      - `observed ROI - null median ROI = +0.0136`
      - `observed profit - null median profit = +190`
  - 読み方:
    - valid-selected は selection bias を含みうる
    - `current_shuffle` は selection bias の存在を見るには useful だが、row-level なので少し緩い
    - mainline の主読みとしては `race_internal_permutation` を優先する
    - その stricter null でも observed は still 上にあるが、上振れ幅は `current_shuffle` よりかなり小さい
    - したがって valid-selected extra label guard の改善は「完全に虚偽」ではない一方、point estimate をそのまま信用しすぎず、やや割り引いて読むのが自然
- global weak-label guard を valid-selected で選ぶ更新後 reference strategy の不確実性評価は、`configs/reference_label_guard_uncertainty_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_label_guard_uncertainty_dual_market_logreg_2023_2025.toml`
  - 出力:
    - `data/artifacts/reference_label_guard_uncertainty_dual_market_logreg_2023_2025/summary.csv`
    - `bootstrap_distribution.csv`
    - `yearly_contribution.csv`
  - point estimate:
    - `BET 189 / hit_rate 0.5820 / ROI 1.1249 / profit 2360 / max_drawdown 750 / max_losing_streak 4`
  - bootstrap:
    - `ROI 95% interval: 0.9819 - 1.2677`
    - `profit 95% interval: -341.7 - 5060.0`
    - `max_drawdown 95% interval: 490 - 1940.5`
    - `ROI > 1.0 ratio: 0.9495`
  - 年別寄与:
    - `2023: BET 48 / ROI 1.0417 / profit 200`
    - `2024: BET 61 / ROI 1.1639 / profit 1000`
    - `2025: BET 80 / ROI 1.1450 / profit 1160`
  - 読み方:
    - point estimate だけでなく bootstrap でも `ROI > 1.0` 側がかなり優勢で、前の reference 候補より頑健性は上がっている
    - ただし下側 95% 区間は still `1.0` 未満なので、「かなり有望」ではあるが「ほぼ確実に勝てる」とまではまだ言えない
    - 利益寄与は `2023 / 2024 / 2025` に分散していて、以前のように `2025` だけへ偏ってはいない
  - block sensitivity:
    - `configs/mainline_block_sensitivity_dual_market_logreg_2023_2025.toml` で、block bootstrap の粒度を `race_date / week / month` で比較できる
    - strategy 単体の `ROI 95% interval` は
      - `race_date: 0.9940 - 1.2622`
      - `week: 0.9910 - 1.2590`
      - `month: 0.9697 - 1.2721`
    - `ROI > 1.0 ratio` は
      - `race_date: 0.9670`
      - `week: 0.9630`
      - `month: 0.9450`
    - 読み方:
      - `week` block では confidence はほぼ維持
      - `month` block では下側区間が少し広がり、`ROI > 1.0 ratio` も少し下がる
      - ただし mainline の見え方が崩れるほどではなく、「block 粒度に完全不変ではないが、大勢は維持」と読むのが自然
- 更新後 reference strategy を固定した stake sizing 比較は、`configs/reference_stake_sizing_compare_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_stake_sizing_compare_dual_market_logreg_2023_2025.toml`
  - 固定する strategy:
    - `dual_market_logreg`
    - `first guard = problematic_band_excluded`
    - `extra label guard = valid-selected`
  - sizing variants:
    - `flat_100`
    - `capped_fractional_kelly_like`
    - `capped_fractional_kelly_like_per_race_cap`
    - `capped_fractional_kelly_like_per_day_cap`
    - `capped_fractional_kelly_like_drawdown_reduction`
  - 出力:
    - `data/artifacts/reference_stake_sizing_compare_dual_market_logreg_2023_2025/summary.csv`
    - `equity_curve.csv`
    - `monthly_profit.csv`
    - `window_profit.csv`
  - 2023-2025 OOS 合算:
    - `flat_100`
      - `BET 189 / hit_rate 0.5820 / ROI 1.1249 / profit 2360 / max_drawdown 750 / max_losing_streak 4`
    - `capped_fractional_kelly_like`
      - `BET 189 / hit_rate 0.5820 / ROI 1.1522 / profit 4840 / avg_stake 168.3 / max_drawdown 1380 / max_losing_streak 4`
    - `capped_fractional_kelly_like_per_race_cap`
      - `BET 188 / hit_rate 0.5798 / ROI 1.1369 / profit 4270 / avg_stake 166.0 / max_drawdown 1380 / max_losing_streak 4`
    - `capped_fractional_kelly_like_per_day_cap`
      - `BET 189 / hit_rate 0.5820 / ROI 1.1522 / profit 4840 / avg_stake 168.3 / max_drawdown 1380 / max_losing_streak 4`
    - `capped_fractional_kelly_like_drawdown_reduction`
      - `BET 189 / hit_rate 0.5820 / ROI 1.1589 / profit 4560 / avg_stake 151.9 / max_drawdown 990 / max_losing_streak 4`
  - 年別の見え方:
    - `capped_fractional_kelly_like`
      - `2023: ROI 0.9949 / profit -30 / max_drawdown 1050`
      - `2024: ROI 1.2267 / profit 2380 / max_drawdown 410`
      - `2025: ROI 1.1617 / profit 2490 / max_drawdown 1380`
    - `capped_fractional_kelly_like_drawdown_reduction`
      - `2023: ROI 1.0036 / profit 20 / max_drawdown 920`
      - `2024: ROI 1.2267 / profit 2380 / max_drawdown 410`
      - `2025: ROI 1.1701 / profit 2160 / max_drawdown 990`
  - 月別の荒さ:
    - `flat_100` worst month は `2025-03: -660`
    - `capped_fractional_kelly_like` worst month は `2025-03: -1200`
    - `capped_fractional_kelly_like_drawdown_reduction` worst month も `2025-03` だが、落ち込みはそれより浅い
  - 読み方:
    - `capped_fractional_kelly_like` は利益を伸ばすが、DD は `flat_100` よりかなり深い
    - `per_race_cap` は少し保守的になるが、今回は ROI / profit を少し削るだけで DD 改善は限定的
    - `per_day_cap` は今回の採用列では実質効かず、base kelly と同じ結果だった
    - `drawdown_based_stake_reduction` は profit を少し落とす代わりに DD を `1380 -> 990` まで改善し、ROI もわずかに上がった
    - 現時点の constrained sizing 候補としては `capped_fractional_kelly_like_drawdown_reduction` が最もバランスが良い
- stake sizing ごとの不確実性比較は、`configs/reference_stake_sizing_uncertainty_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_stake_sizing_uncertainty_dual_market_logreg_2023_2025.toml`
  - 出力:
    - `data/artifacts/reference_stake_sizing_uncertainty_dual_market_logreg_2023_2025/summary.csv`
    - `bootstrap_distribution.csv`
    - `yearly_contribution.csv`
  - bootstrap 比較:
    - `flat_100`
      - `ROI 95% interval: 0.9819 - 1.2677`
      - `profit 95% interval: -341.7 - 5060.0`
      - `max_drawdown 95% interval: 490.0 - 1940.5`
      - `ROI > 1.0 ratio: 0.9495`
    - `capped_fractional_kelly_like`
      - `ROI 95% interval: 1.0038 - 1.3012`
      - `profit 95% interval: 119.8 - 9530.2`
      - `max_drawdown 95% interval: 779.8 - 3060.2`
      - `ROI > 1.0 ratio: 0.9785`
    - `capped_fractional_kelly_like_per_race_cap`
      - `ROI 95% interval: 0.9912 - 1.2809`
      - `profit 95% interval: -270.5 - 8720.5`
      - `max_drawdown 95% interval: 780.0 - 3200.2`
      - `ROI > 1.0 ratio: 0.9660`
    - `capped_fractional_kelly_like_per_day_cap`
      - `ROI 95% interval: 1.0038 - 1.3012`
      - `profit 95% interval: 119.8 - 9530.2`
      - `max_drawdown 95% interval: 779.8 - 3060.2`
      - `ROI > 1.0 ratio: 0.9785`
    - `capped_fractional_kelly_like_drawdown_reduction`
      - `ROI 95% interval: 1.0070 - 1.3041`
      - `profit 95% interval: 199.8 - 8662.0`
      - `max_drawdown 95% interval: 700.0 - 2620.2`
      - `ROI > 1.0 ratio: 0.9805`
  - 年別寄与:
    - `flat_100`
      - `2023: profit 200 / ROI 1.0417`
      - `2024: profit 1000 / ROI 1.1639`
      - `2025: profit 1160 / ROI 1.1450`
    - `capped_fractional_kelly_like`
      - `2023: profit -30 / ROI 0.9949`
      - `2024: profit 2380 / ROI 1.2267`
      - `2025: profit 2490 / ROI 1.1617`
    - `capped_fractional_kelly_like_drawdown_reduction`
      - `2023: profit 20 / ROI 1.0036`
      - `2024: profit 2380 / ROI 1.2267`
      - `2025: profit 2160 / ROI 1.1701`
  - 読み方:
    - `capped_fractional_kelly_like` は unconstrained でも強いが、DD 区間はまだ広い
    - `per_race_cap` は今回の reference では improvement が限定的
    - `per_day_cap` は今回の採用列では実質 non-binding
    - `drawdown_based_stake_reduction` は下側 ROI 区間も 1.0 を超え、`ROI > 1.0 ratio` も最良で、制約込み sizing の中では最も頑健
    - ただし 2023 単年の改善は still 小さいので、年差が完全に消えたわけではない
- 更新後 reference strategy の stateful bankroll simulation は、`configs/reference_bankroll_simulation_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_bankroll_simulation_dual_market_logreg_2023_2025.toml`
  - 出力:
    - `data/artifacts/reference_bankroll_simulation_dual_market_logreg_2023_2025/summary.csv`
    - `yearly_profit.csv`
    - `monthly_profit.csv`
    - `equity_curve.csv`
    - `bankroll_path.csv`
    - `bootstrap_distribution.csv`
  - 比較した初期 bankroll:
    - `5000`
    - `10000`
    - `30000`
  - 2023-2025 合算:
    - `flat_100 / bankroll=5000`
      - `final_bankroll 7360 / ROI 1.1249 / profit 2360 / max_drawdown 750`
    - `capped_fractional_kelly_like / bankroll=5000`
      - `final_bankroll 10730 / ROI 1.1235 / profit 5730 / max_drawdown 2740`
    - `capped_fractional_kelly_like_per_race_cap / bankroll=5000`
      - `final_bankroll 10040 / ROI 1.1299 / profit 5040 / max_drawdown 2270`
    - `capped_fractional_kelly_like_drawdown_reduction / bankroll=5000`
      - `final_bankroll 6860 / ROI 1.0890 / profit 1860 / max_drawdown 1400`
    - `capped_fractional_kelly_like / bankroll=10000`
      - `final_bankroll 19200 / ROI 1.1217 / profit 9200 / max_drawdown 3870`
    - `capped_fractional_kelly_like_per_race_cap / bankroll=10000`
      - `final_bankroll 15100 / ROI 1.0977 / profit 5100 / max_drawdown 2250`
    - `capped_fractional_kelly_like_drawdown_reduction / bankroll=10000`
      - `final_bankroll 13650 / ROI 1.0824 / profit 3650 / max_drawdown 1920`
    - `capped_fractional_kelly_like / bankroll=30000`
      - `final_bankroll 41660 / ROI 1.1238 / profit 11660 / max_drawdown 3750`
    - `capped_fractional_kelly_like_per_race_cap / bankroll=30000`
      - `final_bankroll 36390 / ROI 1.1145 / profit 6390 / max_drawdown 2250`
    - `capped_fractional_kelly_like_drawdown_reduction / bankroll=30000`
      - `final_bankroll 32830 / ROI 1.0590 / profit 2830 / max_drawdown 1880`
  - 読み方:
    - stateful bankroll にすると、fixed-stake 風の sizing 比較と違って `initial bankroll` 依存がかなり大きく出る
    - base `capped_fractional_kelly_like` は利益は伸びるが、bankroll が大きいほど DD も急に深くなる
    - `per_race_cap` は stateful ではかなり効いていて、特に `10000 / 30000` では DD を大きく抑える
    - 今回の設定では `drawdown_based_stake_reduction` は固定-bankroll 比較ほど優位ではなく、profit も ROI も base kelly を下回った
    - つまり stateful 運用まで含めると、現時点の本命は `capped_fractional_kelly_like_per_race_cap` 寄りで、`drawdown_reduction` は threshold 設定を再調整したくなる
    - `per_day_cap` は今回の採用列では still かなり non-binding だった
- stateful bankroll simulation の運用候補不確実性比較は、`configs/reference_bankroll_simulation_uncertainty_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_bankroll_simulation_uncertainty_dual_market_logreg_2023_2025.toml`
  - 比較対象:
    - `flat_100`
    - `capped_fractional_kelly_like_per_race_cap`
  - 出力:
    - `data/artifacts/reference_bankroll_simulation_uncertainty_dual_market_logreg_2023_2025/summary.csv`
    - `bootstrap_distribution.csv`
    - `yearly_contribution.csv`
  - bootstrap 比較:
    - `flat_100 / bankroll=5000`
      - `final_bankroll 95% interval: 4658.25 - 10060.0`
      - `ROI 95% interval: 0.9819 - 1.2677`
      - `profit 95% interval: -341.7 - 5060.0`
      - `max_drawdown 95% interval: 700.0 - 2110.0`
      - `ROI > 1.0 ratio: 0.9495`
      - `final_bankroll < initial ratio: 0.0495`
    - `per_race_cap / bankroll=5000`
      - `final_bankroll 95% interval: 4499.5 - 11690.0`
      - `ROI 95% interval: 0.9765 - 1.2337`
      - `profit 95% interval: -500.5 - 6690.0`
      - `max_drawdown 95% interval: 1020.0 - 2730.0`
      - `ROI > 1.0 ratio: 0.9445`
      - `final_bankroll < initial ratio: 0.0545`
    - `flat_100 / bankroll=10000`
      - `ROI > 1.0 ratio: 0.9495`
      - `final_bankroll < initial ratio: 0.0495`
    - `per_race_cap / bankroll=10000`
      - `final_bankroll 95% interval: 10059.75 - 17521.25`
      - `ROI 95% interval: 1.0017 - 1.2191`
      - `profit 95% interval: 59.8 - 7521.25`
      - `max_drawdown 95% interval: 1200.0 - 3060.5`
      - `ROI > 1.0 ratio: 0.9780`
      - `final_bankroll < initial ratio: 0.0215`
    - `flat_100 / bankroll=30000`
      - `ROI > 1.0 ratio: 0.9495`
      - `final_bankroll < initial ratio: 0.0495`
    - `per_race_cap / bankroll=30000`
      - `final_bankroll 95% interval: 30390.0 - 37920.75`
      - `ROI 95% interval: 1.0117 - 1.2216`
      - `profit 95% interval: 390.0 - 7920.75`
      - `max_drawdown 95% interval: 1200.0 - 3150.0`
      - `ROI > 1.0 ratio: 0.9865`
      - `final_bankroll < initial ratio: 0.0135`
  - block sensitivity:
    - 同じ mainline stateful uncertainty を `race_date / week / month` で見直すと、`per_race_cap=200` の `ROI 95% interval` は
      - `bankroll=10000`
        - `race_date: 1.0011 - 1.2165`
        - `week: 0.9988 - 1.2114`
        - `month: 0.9872 - 1.2359`
      - `bankroll=30000`
        - `race_date: 1.0103 - 1.2198`
        - `week: 1.0064 - 1.2170`
        - `month: 0.9973 - 1.2362`
    - `ROI > 1.0 ratio` は
      - `bankroll=10000`
        - `race_date: 0.9750`
        - `week: 0.9740`
        - `month: 0.9600`
      - `bankroll=30000`
        - `race_date: 0.9845`
        - `week: 0.9800`
        - `month: 0.9715`
    - 読み方:
      - stateful 側も `week` block ではほぼ変わらない
      - `month` block では少し保守的になるが、confidence が崩れるほどではない
  - 読み方:
    - `flat_100` は保守基準線としてかなり安定しているが、profit の伸びは小さい
    - `per_race_cap` は `bankroll=5000` だと still 揺れが大きく、保守基準線を明確には上回らない
    - 一方で `bankroll=10000 / 30000` では下側 ROI 区間が 1.0 を超え、`final_bankroll < initial` 比率もかなり小さくなる
    - つまり stateful 運用候補としては、`per_race_cap` は「やや大きめ bankroll を置けるなら本命」、`flat_100` は「最も単純で保守的な基準線」という整理が自然
- stateful bankroll simulation における `per_race_cap` 感度比較は、`configs/reference_per_race_cap_sensitivity_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_per_race_cap_sensitivity_dual_market_logreg_2023_2025.toml`
  - 比較対象:
    - `per_race_cap = 200 / 300 / 400 / 500`
    - `initial_bankroll = 10000 / 30000`
  - 出力:
    - `data/artifacts/reference_per_race_cap_sensitivity_dual_market_logreg_2023_2025/summary.csv`
    - `yearly_profit.csv`
    - `monthly_profit.csv`
    - `equity_curve.csv`
    - `bootstrap_distribution.csv`
  - point estimate:
    - `bankroll=10000`
      - `cap=200: final_bankroll 13970 / ROI 1.1091 / profit 3970 / max_drawdown 1500`
      - `cap=300: final_bankroll 15100 / ROI 1.0977 / profit 5100 / max_drawdown 2250`
      - `cap=400: final_bankroll 16670 / ROI 1.1042 / profit 6670 / max_drawdown 3000`
      - `cap=500: final_bankroll 17940 / ROI 1.1073 / profit 7940 / max_drawdown 3870`
    - `bankroll=30000`
      - `cap=200: final_bankroll 34260 / ROI 1.1145 / profit 4260 / max_drawdown 1500`
      - `cap=300: final_bankroll 36390 / ROI 1.1145 / profit 6390 / max_drawdown 2250`
      - `cap=400: final_bankroll 38520 / ROI 1.1145 / profit 8520 / max_drawdown 3000`
      - `cap=500: final_bankroll 40510 / ROI 1.1134 / profit 10510 / max_drawdown 3750`
  - bootstrap:
    - `bankroll=10000`
      - `cap=200`
        - `ROI 95% interval: 1.0087 - 1.2213`
        - `profit 95% interval: 209.8 - 5170.2`
        - `max_drawdown 95% interval: 800.0 - 2100.2`
        - `ROI > 1.0 ratio: 0.9825`
        - `final_bankroll < initial ratio: 0.0175`
      - `cap=300`
        - `ROI 95% interval: 1.0017 - 1.2191`
        - `profit 95% interval: 59.8 - 7521.2`
        - `max_drawdown 95% interval: 1200.0 - 3060.5`
        - `ROI > 1.0 ratio: 0.9780`
        - `final_bankroll < initial ratio: 0.0215`
      - `cap=400`
        - `ROI 95% interval: 1.0002 - 1.2153`
        - `profit 95% interval: 9.8 - 9690.2`
        - `max_drawdown 95% interval: 1560.0 - 3960.0`
        - `ROI > 1.0 ratio: 0.9750`
        - `final_bankroll < initial ratio: 0.0245`
      - `cap=500`
        - `ROI 95% interval: 0.9976 - 1.2214`
        - `profit 95% interval: -110.5 - 11871.2`
        - `max_drawdown 95% interval: 1850.0 - 4810.2`
        - `ROI > 1.0 ratio: 0.9705`
        - `final_bankroll < initial ratio: 0.0290`
    - `bankroll=30000`
      - `cap=200`
        - `ROI 95% interval: 1.0117 - 1.2216`
        - `final_bankroll < initial ratio: 0.0135`
      - `cap=300`
        - `ROI 95% interval: 1.0117 - 1.2216`
        - `final_bankroll < initial ratio: 0.0135`
      - `cap=400`
        - `ROI 95% interval: 1.0117 - 1.2216`
        - `final_bankroll < initial ratio: 0.0135`
      - `cap=500`
        - `ROI 95% interval: 1.0108 - 1.2220`
        - `final_bankroll < initial ratio: 0.0130`
  - 読み方:
    - `bankroll=10000` では `cap=200` が最も保守的で、下側区間・DD・bankroll 割れ率が一番良い
    - ただし利益は cap を上げるほど伸びるので、`200` は安全寄り、`300-400` は利益寄りの中庸
    - `cap=500` は利益最大だが、DD と下側不確実性が最も悪い
    - `bankroll=30000` では ROI は `200-400` でほぼ同じなので、cap を上げることはほぼ単なるレバレッジ増加
    - 現時点の実務候補としては
      - `bankroll=10000`: `cap=200` か `300`
      - `bankroll=30000`: `cap=200` か `300`
      が自然で、`400/500` は DD 許容がかなり必要
- `per_race_cap` と `drawdown_reduction` の併用比較は、`configs/reference_per_race_cap_drawdown_compare_dual_market_logreg_2023_2025.toml` で再現できる
  - config:
    - `configs/reference_per_race_cap_drawdown_compare_dual_market_logreg_2023_2025.toml`
  - 比較対象:
    - `per_race_cap = 200 / 300`
    - `drawdown_reduction = off / on`
    - `initial_bankroll = 10000 / 30000`
  - 出力:
    - `data/artifacts/reference_per_race_cap_drawdown_compare_dual_market_logreg_2023_2025/summary.csv`
    - `yearly_profit.csv`
    - `monthly_profit.csv`
    - `equity_curve.csv`
    - `bootstrap_distribution.csv`
  - point estimate:
    - `bankroll=10000`
      - `cap=200`
        - `base: final_bankroll 13970 / ROI 1.1091 / profit 3970 / max_drawdown 1500`
        - `+drawdown_reduction: final_bankroll 11970 / ROI 1.0752 / profit 1970 / max_drawdown 1150`
      - `cap=300`
        - `base: final_bankroll 15100 / ROI 1.0977 / profit 5100 / max_drawdown 2250`
        - `+drawdown_reduction: final_bankroll 10430 / ROI 1.0174 / profit 430 / max_drawdown 1450`
    - `bankroll=30000`
      - `cap=200`
        - `base: final_bankroll 34260 / ROI 1.1145 / profit 4260 / max_drawdown 1500`
        - `+drawdown_reduction: final_bankroll 32140 / ROI 1.0796 / profit 2140 / max_drawdown 1150`
      - `cap=300`
        - `base: final_bankroll 36390 / ROI 1.1145 / profit 6390 / max_drawdown 2250`
        - `+drawdown_reduction: final_bankroll 31550 / ROI 1.0542 / profit 1550 / max_drawdown 1240`
  - bootstrap:
    - `bankroll=10000`
      - `cap=200`
        - `base ROI 95% interval: 1.0087 - 1.2213`
        - `base ROI > 1.0 ratio: 0.9825`
        - `dd-reduced ROI 95% interval: 0.9720 - 1.2272`
        - `dd-reduced ROI > 1.0 ratio: 0.9340`
      - `cap=300`
        - `base ROI 95% interval: 1.0017 - 1.2191`
        - `base ROI > 1.0 ratio: 0.9780`
        - `dd-reduced ROI 95% interval: 0.9561 - 1.2292`
        - `dd-reduced ROI > 1.0 ratio: 0.8895`
    - `bankroll=30000`
      - `cap=200`
        - `base ROI 95% interval: 1.0117 - 1.2216`
        - `base ROI > 1.0 ratio: 0.9865`
        - `dd-reduced ROI 95% interval: 0.9782 - 1.2275`
        - `dd-reduced ROI > 1.0 ratio: 0.9465`
      - `cap=300`
        - `base ROI 95% interval: 1.0117 - 1.2216`
        - `base ROI > 1.0 ratio: 0.9865`
        - `dd-reduced ROI 95% interval: 0.9621 - 1.2247`
        - `dd-reduced ROI > 1.0 ratio: 0.9125`
  - 読み方:
    - 今回の設定では `drawdown_reduction` を重ねると DD は浅くなるが、profit と ROI を削りすぎる
    - 特に `cap=300` では改善より悪化のほうが大きく、`bankroll=10000` だと point estimate でもかなり弱くなる
    - 現時点の候補を絞るなら、`per_race_cap` 単体の `cap=200` か `300` を優先し、`drawdown_reduction` は閾値再調整なしでは採用しないほうが自然

## Mainline Reference

- 現時点の本線 reference pack は、`configs/reference_pack_dual_market_logreg_mainline_2023_2025.toml` で再現できる
  - BET logic only 比較ラインは `configs/bet_logic_only_dual_market_logreg_mainline_2023_2025.toml` を入口にし、mainline model / feature / training は固定のまま `rolling_predictions.csv` を読んで賭け方だけを比較する
  - BET logic only の現行 mainline candidate は `guard_0_01_plus_proxy_domain_overlay`
  - これは `no_bet_guard_stronger surcharge=0.01` を base にし、venue-code-based domain bucket `02 / 09` にだけ追加 surcharge を載せる最小 overlay
  - formal wording は次で固定する
    - `race_key` は upstream race identifier
    - `race_key[:2]` は upstream-defined `venue_code`
    - domain/group は `venue_code` から project 側 mapping で導出する
  - historical artifact name の `proxy_domain` は互換性維持のため残しているが、意味としては `venue_code` 由来の project-owned bucket
  - config では `provisional_proxy_domain_overlay_enabled=true` で現行 candidate を使い、`false` にすると `no_bet_guard_stronger surcharge=0.01` fallback に落とせる
  - operational run mode は `active_run_mode` で明示する
    - `candidate_provisional`
    - `fallback_stable`
    - run_mode 名は package 互換の legacy label で、adopt status そのものではない
  - `formal_domain_mapping_confirmed=true` は venue_code formalization と project-owned mapping を repo 内で固定した状態を表す
  - `no_bet_guard_stronger surcharge=0.01` は production-simple fallback として残す
  - 最終指示は `final_*instructions*` と `final_instruction_package_manifest.csv/json`, `final_instruction_package_summary.csv/json` をセットで読む
  - post-freeze monitoring:
    - 新しい rolling artifact が来たら `candidate_provisional` と `fallback_stable` の両方を再実行する
    - `final_instruction_package_manifest`, `final_instruction_package_summary`, `monitoring_summary`, `regression_gate_report`, `artifact_compare_report` を確認する
    - `regression_gate_report` が `fail` の場合は `fallback_stable` を使う
  - `baseline_current_logic` は reference parity anchor として固定し、比較前提として壊さない
  - heuristic chaos は本線採用しない
  - `chaos_edge_surcharge` は research-only、`chaos_no_bet_guard` と `no_bet_guard_plus_chaos` は stop 扱い
  - 運用手順書:
    - [docs/mainline_reference_runbook.md](/Users/matsurimbpblack/Library/Mobile%20Documents/com~apple~CloudDocs/codex_projects/horse-bet-lab/docs/mainline_reference_runbook.md)
  - config:
    - `configs/reference_pack_dual_market_logreg_mainline_2023_2025.toml`
  - strategy:
    - `dual_market_logreg`
    - `first guard = problematic_band_excluded`
    - `extra label guard = valid-selected`
    - `current consensus ranking / current valid-selected label guard`
  - stateful sizing:
    - `capped_fractional_kelly_like_per_race_cap`
    - `per_race_cap = 200`
    - `standard_initial_bankroll = 10000`
    - `reference_initial_bankrolls = 10000 / 30000`
  - pack output:
    - `data/artifacts/reference_pack_dual_market_logreg_mainline_2023_2025/strategy_summary.csv`
    - `oos_backtest_summary.csv`
    - `uncertainty_summary.csv`
    - `stateful_bankroll_summary.csv`
    - `stateful_yearly_profit.csv`
    - `stateful_monthly_profit.csv`
    - `stateful_equity_curve.csv`
    - `stateful_bootstrap_distribution.csv`
  - artifact の意味:
    - `strategy_summary.csv`
      - 本線 reference の定義そのもの。モデル、guard、ranking、sizing family、標準 bankroll を固定したメタ情報
    - `oos_backtest_summary.csv`
      - strategy 単体の OOS summary。採用 BET 集合を fixed 100 円で評価した比較基準
    - `uncertainty_summary.csv`
      - strategy 単体の bootstrap 不確実性 summary。stateful bankroll をまだ入れない
    - `stateful_bankroll_summary.csv`
      - stateful bankroll を含む OOS summary。実運用寄りの bankroll path を前提にした結果
    - `stateful_yearly_profit.csv / stateful_monthly_profit.csv / stateful_equity_curve.csv`
      - 本線運用候補の時系列挙動を見るための補助 artifact
    - `stateful_bootstrap_distribution.csv`
      - stateful bankroll path を前提にした bootstrap 分布
    - `manifest.json`
      - 本線 pack を checksum で固定する manifest
      - `dataset parquet hash / referenced config hash / code commit SHA` も含む
  - OOS backtest summary:
    - `BET 189 / hit_rate 0.5820 / ROI 1.1249 / profit 2360 / max_drawdown 750 / max_losing_streak 4`
    - year summary:
      - `2023: ROI 1.0417 / profit 200`
      - `2024: ROI 1.1639 / profit 1000`
      - `2025: ROI 1.1450 / profit 1160`
  - uncertainty summary:
    - `ROI 95% interval: 0.9940 - 1.2622`
    - `profit 95% interval: -110.2 - 4980.2`
    - `max_drawdown 95% interval: 700.0 - 2060.0`
    - `ROI > 1.0 ratio: 0.9670`
  - stateful bankroll summary:
    - `bankroll=10000`
      - `final_bankroll 13970 / ROI 1.1091 / profit 3970 / max_drawdown 1500`
      - `ROI 95% interval: 1.0034 - 1.2180`
      - `ROI > 1.0 ratio: 0.9790`
      - `final_bankroll < initial ratio: 0.0205`
    - `bankroll=30000`
      - `final_bankroll 34260 / ROI 1.1145 / profit 4260 / max_drawdown 1500`
      - `ROI 95% interval: 1.0103 - 1.2198`
      - `ROI > 1.0 ratio: 0.9845`
      - `final_bankroll < initial ratio: 0.0155`
  - uncertainty bootstrap note:
    - 旧 bootstrap は `per-bet with replacement` だった
    - 現在は `race_date block bootstrap` を使う
    - 今回の本線では、strategy 単体の confidence は大きくは崩れず、stateful 側はわずかに保守的になった
  - 読み方:
    - 比較基準としては、この pack を「本線 reference」として固定する
    - `10000` を標準運用、`30000` は参考比較として残す
    - strategy の point estimate と block-bootstrap 後の confidence はまだ本線候補として維持できる
    - 一方で stateful bankroll では DD と cap 依存をまだ見る余地がある
    - 今後の比較では、まず `oos_backtest_summary.csv` を strategy 基準線として参照し、運用比較だけを見たいときは `stateful_bankroll_summary.csv` を参照する
    - つまり「strategy 単体の比較」と「bankroll を含む運用比較」は意図的に分けて読む
    - 本線の確認順は `strategy_summary -> oos_backtest_summary -> uncertainty_summary -> stateful_bankroll_summary` に固定する
    - 本線 pack は `manifest.json` でも固定する
    - さらに `code_commit_sha` も含めて固定する
    - manifest と実ファイルが一致しない場合、その pack は比較基準として扱わない
    - manifest は一致しても commit が一致しない pack も比較基準として扱わない
    - producer だけでなく consumer 側でも verify する
      - CLI: `horse-bet-lab-reference-pack-verify`
      - code path: `verify_reference_pack_or_raise(...)`
      - mainline pack artifact を読む consumer は、読む直前にこれを通して fail fast にする

- 本線から分けて扱う研究候補:
  - `dual_market_histgb_small`
  - `capped_fractional_kelly_like_drawdown_reduction`
  - `per_race_cap > 200` の強気運用
  - これらは promising だが、現時点では比較基準そのものにはしない
  - README 内で改善結果を記載していても、`Mainline Reference` に明示的に入っていないものは研究候補として扱う
  - 本線 pack を更新するまでは、研究候補の artifact や数値を比較基準に昇格させない

- 注記:
  - `scripts/live_inference_score.py` の lint は live inference 系の補助スクリプトに属し、本線 reference pack の凍結判断とは別扱いにする
  - したがって、本線 reference の再現・比較・固定は `reference_pack_dual_market_logreg_mainline_2023_2025` とその関連 artifact を基準に行う
  - `proxy_live` 系は本線と同列比較しない。最低限の防御として、入力 CSV の schema / 欠損 / leakage 列を検査し、`run_manifest.json` に source metadata と input hash を保存し、利用可能な場合は `proxy_feature_gap_summary.json` で proxy と実 OZ の差分診断を残す

## Development

仮想環境を有効化した状態で、以下の順に再現確認できます。

```bash
source .venv/bin/activate
PYTHONPATH=src pytest
ruff check .
mypy .
PYTHONPATH=src python -m horse_bet_lab.runner --config configs/default.toml
PYTHONPATH=src python -m horse_bet_lab.ingest.cli
PYTHONPATH=src python -m horse_bet_lab.dataset.cli --config configs/dataset_minimal.toml
PYTHONPATH=src python -m horse_bet_lab.model.cli --config configs/model_odds_only_logreg_is_place.toml
PYTHONPATH=src python -m horse_bet_lab.model.comparison_cli --config configs/model_market_feature_comparison_2024_2025.toml
PYTHONPATH=src python -m horse_bet_lab.dataset.cli --config configs/dataset_odds_only_plus_popularity_is_place.toml
PYTHONPATH=src python -m horse_bet_lab.model.cli --config configs/model_odds_only_plus_popularity_logreg_is_place.toml
PYTHONPATH=src python -m horse_bet_lab.evaluation.cli --config configs/bet_eval_odds_log1p_plus_popularity.toml
PYTHONPATH=src python -m horse_bet_lab.evaluation.place_cli --config configs/place_backtest_odds_log1p_plus_popularity.toml
```

チェック手順を一本化したい場合は `make` を使えます。`Makefile` は `.venv` 内のコマンドを使うので、先に Setup を完了してください。

```bash
make check
```

個別コマンドも残しています。

```bash
make test
make lint
make typecheck
make run
make ingest
make dataset
make model
make evaluate
make place-backtest
```

シェルだけで実行したい場合は補助スクリプトも使えます。

```bash
./scripts/run_checks.sh
```
