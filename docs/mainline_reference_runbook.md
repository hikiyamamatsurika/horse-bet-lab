# Mainline Reference Runbook

## Purpose

この手順書は、`reference_pack_dual_market_logreg_mainline_2023_2025` を今後の比較基準として固定し、
research candidate と混線させないための運用ルールです。

本線 reference を更新すると明示的に決めるまでは、この手順書に書かれた config / artifact / 判断順を
比較基準として使います。

## Fixed Mainline Reference

- model:
  - `dual_market_logreg`
- first guard:
  - `problematic_band_excluded`
- extra label guard:
  - `valid-selected`
- ranking / selection:
  - `current consensus ranking`
  - `current valid-selected label guard`
- standard stateful sizing:
  - `capped_fractional_kelly_like_per_race_cap`
  - `per_race_cap = 200`
- standard bankroll:
  - `10000`
- reference comparison bankroll:
  - `30000`

## Mainline Config

- reference pack:
  - `configs/reference_pack_dual_market_logreg_mainline_2023_2025.toml`

この pack は、以下の既存比較を束ねた固定基準です。

- strategy / guard reference:
  - `configs/reference_label_guard_compare_dual_market_logreg_2023_2025.toml`
- strategy uncertainty:
  - `configs/reference_label_guard_uncertainty_dual_market_logreg_2023_2025.toml`
- stateful bankroll reference:
  - `configs/reference_per_race_cap_sensitivity_dual_market_logreg_2023_2025.toml`

## Mainline Artifacts

reference pack 出力先:

- `data/artifacts/reference_pack_dual_market_logreg_mainline_2023_2025/`

artifact の意味:

- `strategy_summary.csv`
  - 本線 reference の定義そのもの
- `oos_backtest_summary.csv`
  - strategy 単体の OOS summary
  - fixed 100 円ベースの比較基準
- `uncertainty_summary.csv`
  - strategy 単体の bootstrap 不確実性 summary
- `stateful_bankroll_summary.csv`
  - stateful bankroll を含む OOS summary
  - 実運用寄りの bankroll path 前提
- `stateful_yearly_profit.csv`
  - 本線運用候補の年別損益
- `stateful_monthly_profit.csv`
  - 本線運用候補の月別損益
- `stateful_equity_curve.csv`
  - 本線運用候補の時系列 equity curve
- `stateful_bootstrap_distribution.csv`
  - stateful bankroll path 前提の bootstrap 分布
- `manifest.json`
  - 本線 pack を固定するための checksum manifest
  - `file_name / byte_size / sha256` を保存する
  - あわせて `dataset parquet hash / referenced config hash / code commit SHA` も保存する

## Standard Check Order

本線評価の確認順は固定で以下にします。

1. `strategy_summary.csv`
   - 何を本線として固定しているか確認する
2. `oos_backtest_summary.csv`
   - strategy 単体の OOS 成績を確認する
3. `uncertainty_summary.csv`
   - strategy 単体の bootstrap 不確実性を確認する
4. `stateful_bankroll_summary.csv`
   - standard bankroll=`10000` を中心に、運用時の bankroll path を確認する
5. `manifest.json`
   - mainline artifact が静かに入れ替わっていないか確認する

補助的に必要なら以下を見る:

- `stateful_yearly_profit.csv`
- `stateful_monthly_profit.csv`
- `stateful_equity_curve.csv`
- `stateful_bootstrap_distribution.csv`

## Standard Interpretation

- strategy 単体の比較は `oos_backtest_summary.csv` を基準に読む
- bankroll を含む運用比較は `stateful_bankroll_summary.csv` を基準に読む
- 標準運用は `bankroll=10000`
- `bankroll=30000` は参考比較であり、本線の主判定は `10000` を優先する

## Research Candidates

以下は本線に含めません。

- `dual_market_histgb_small`
- `capped_fractional_kelly_like_drawdown_reduction`
- `per_race_cap > 200` の強気運用

## BET Logic Only Status

- BET logic only の比較では、mainline model / feature / training / rolling prediction generation は固定する
- `baseline_current_logic` は reference parity anchor として固定し、比較前提として壊さない
- 現時点の mainline BET logic candidate は `guard_0_01_plus_proxy_domain_overlay`
  - base は `no_bet_guard_stronger surcharge=0.01`
  - final overlay は venue-code-based domain bucket `02 / 09` に対する追加 surcharge のみ
  - year 条件は使わない
  - formal wording は次で固定する
    - `race_key` は upstream race identifier
    - `race_key[:2]` は upstream-defined `venue_code`
    - domain/group は `venue_code` から project-owned mapping で導出する
  - historical artifact name の `proxy_domain` は互換性維持のため残し、意味は `venue_code` 由来の project bucket と読む
  - config 運用:
    - `provisional_proxy_domain_overlay_enabled=true`: 現行 candidate を使う
    - `provisional_proxy_domain_overlay_enabled=false`: `no_bet_guard_stronger surcharge=0.01` fallback を使う
    - `active_run_mode=candidate_provisional`: provisional candidate package を出す
    - `active_run_mode=fallback_stable`: stable fallback package を出す
    - run_mode 名は package 互換の legacy label で、adopt status そのものではない
    - `formal_domain_mapping_confirmed=true`: venue_code formalization と project-owned mapping が repo 内で固定済みであることを示す
- `no_bet_guard_stronger surcharge=0.01` は production-simple fallback として残す
- 現時点の運用メモ:
  - candidate は数値上優位で、domain formalization 後は hard adopt 判定に上げられる
  - fallback は stable path
  - fallback は venue-code-based overlay を無効化した安定経路として残す
- post-freeze monitoring 手順:
  - 新しい rolling artifact が来たら `candidate_provisional` を実行する
  - 続けて `fallback_stable` も実行する
  - `final_instruction_package_manifest`, `final_instruction_package_summary`, `monitoring_summary`, `regression_gate_report`, `artifact_compare_report` を確認する
  - `regression_gate_report` に `fail` があれば fallback を使う
- heuristic chaos は本線採用しない
  - `chaos_edge_surcharge` は research-only
  - `chaos_no_bet_guard` と `no_bet_guard_plus_chaos` は stop
- 他の minimal overlay は本線採用しない
  - `guard_0_01_plus_near_threshold_overlay`: not adopted
  - `guard_0_01_plus_place_basis_overlay`: not adopted
  - `guard_0_01_plus_domain_x_threshold_overlay`: secondary reference only
- 理由:
  - `guard 0.01` は baseline parity を保ったまま profit を改善し、良い base line になった
  - その上で proxy-domain overlay は `guard 0.01` 比で profit と drawdown を同時改善した
  - heuristic chaos は edge の完全な別名ではないが、独立 signal としては弱く、profitable high-chaos bets や guard の勝ち筋まで削った

ルール:

- 新しい改善案は、まず research candidate として本線に並べて比較する
- 本線 pack は更新しない
- 本線更新は、明示的に reference pack を更新すると決めたときだけ行う

## Manifest Rule

- 本線 pack は `manifest.json` で固定する
- 本線 pack は `code_commit_sha` も含めて固定する
- `horse-bet-lab-reference-pack` 実行後は manifest も再生成される
- 確認時は `horse-bet-lab-reference-pack-verify --pack-dir data/artifacts/reference_pack_dual_market_logreg_mainline_2023_2025`
  を使う
- manifest と実ファイルが一致しない場合、その pack は比較基準として扱わない
- manifest は一致しても commit が一致しない pack も比較基準として扱わない
- consumer 側でも、pack を読む前に verify を通す
  - CLI なら `horse-bet-lab-reference-pack-verify`
  - code path なら `verify_reference_pack_or_raise(...)`
  - mainline pack artifact を読む consumer は、読む直前に verify を通し、失敗したら fail fast にする

## Notes

- `scripts/live_inference_score.py` の lint は live inference 補助スクリプト側の論点であり、
  本線 reference pack の凍結判断とは別扱いにする
- 本線の再現・比較・固定は、必ず `reference_pack_dual_market_logreg_mainline_2023_2025`
  とその関連 artifact を基準に行う
