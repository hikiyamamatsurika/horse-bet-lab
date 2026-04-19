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

ルール:

- 新しい改善案は、まず research candidate として本線に並べて比較する
- 本線 pack は更新しない
- 本線更新は、明示的に reference pack を更新すると決めたときだけ行う

## Manifest Rule

- 本線 pack は `manifest.json` で固定する
- `horse-bet-lab-reference-pack` 実行後は manifest も再生成される
- 確認時は `horse-bet-lab-reference-pack-verify --pack-dir data/artifacts/reference_pack_dual_market_logreg_mainline_2023_2025`
  を使う
- manifest と実ファイルが一致しない場合、その pack は比較基準として扱わない
- consumer 側でも、pack を読む前に verify を通す
  - CLI なら `horse-bet-lab-reference-pack-verify`
  - code path なら `verify_reference_pack_or_raise(...)`
  - mainline pack artifact を読む consumer は、読む直前に verify を通し、失敗したら fail fast にする

## Notes

- `scripts/live_inference_score.py` の lint は live inference 補助スクリプト側の論点であり、
  本線 reference pack の凍結判断とは別扱いにする
- 本線の再現・比較・固定は、必ず `reference_pack_dual_market_logreg_mainline_2023_2025`
  とその関連 artifact を基準に行う
