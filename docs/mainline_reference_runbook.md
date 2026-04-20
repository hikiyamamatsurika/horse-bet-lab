# Mainline Reference Runbook

## Purpose

この手順書は、`reference_pack_dual_market_logreg_mainline_2023_2025` を今後の比較基準として固定し、
research candidate と混線させないための運用ルールです。

本線 reference を更新すると明示的に決めるまでは、この手順書に書かれた config / artifact / 判断順を
比較基準として使います。

`reference-freeze` 後の mainline BET logic reference については、この手順書を
source of truth とします。README は要約、artifact は実測値の根拠であり、
「何を baseline として比較するか」の定義はこの手順書を優先します。

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

## Popularity Carrier Decision

- conclusion:
  - repo / public JRDB docs / vendor-confirmable material の範囲では、`popularity` の true pre-race carrier は確認できない
- carrier comparison:
  - `win_odds` の current carrier は `jrdb_win_market_snapshot_v1 <- OZ.win_basis_odds`
  - `popularity` は `SED` 仕様上の `確定単勝人気順位` に対応する legacy offline path しか確認できない
  - よって `win_odds` と same-carrier confirmed にはできない
- fixed decision:
  - `same-carrier / separate-carrier / unresolved` のうち `unresolved`
  - mainline decision は `unresolved_keep_legacy_for_non-mainline_only`
- operational rule:
  - `popularity` は legacy / research / auxiliary path にだけ残す
  - mainline では confirmed carrier feature として扱わない
  - historical artifact 名に `dual_market_logreg` が残っていても、carrier confirmed を意味しない

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

## Current Reading

この節を、現時点の mainline reference の正式な読みとして扱います。

- uncertainty の主読み:
  - `uncertainty_summary.csv` は `race_date block bootstrap` を基準に読む
  - 旧 `per-bet` bootstrap の CI や、それ以前の幅は引用しない
- block sensitivity の主読み:
  - `race_date` を baseline とする
  - `week / month` block でも下側区間は少し保守化するが、大勢は維持すると読む
- extra label guard uplift の主読み:
  - valid-selected uplift は `race_internal_permutation` null を基準に読む
  - `current_shuffle` の uplift は補助診断であり、主読みに使わない
  - したがって uplift は以前より小さめに読む
- frozen baseline:
  - mainline は比較基準として固定する
  - 古い数値、旧 CI、旧 uplift を比較基準として再引用しない

short current reading:

- strategy standalone:
  - `race_date block bootstrap` 基準では、`ROI 95% interval = 0.9940 - 1.2622`
  - `ROI > 1.0 ratio = 0.9670`
  - week / month にしても大勢は維持
- stateful:
  - 標準運用は `bankroll=10000`, `per_race_cap=200`
  - `ROI 95% interval = 1.0034 - 1.2180`
  - `ROI > 1.0 ratio = 0.9790`
  - month block では少し保守化するが、mainline の見え方は維持
- extra label guard:
  - `race_internal_permutation` null では `observed ROI percentile = 0.921`
  - `observed - null median ROI = +0.0136`
  - uplift は still 正だが、小さめに読む

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
- 今回の freeze は behavior change ではなく baseline fixation である
  - 新しい BET logic は作らない
  - selection 結果は変えない
  - 既存 artifact を post-hard-adopt の事実に合わせて読むための整理だけを行う
- `baseline_current_logic` は reference parity anchor として固定し、比較前提として壊さない
- 現在の mainline BET logic baseline は `guard_0_01_plus_proxy_domain_overlay`
  - current candidate: `guard_0_01_plus_proxy_domain_overlay`
  - hard_adopt status: `hard_adopt`
  - fallback: `no_bet_guard_stronger surcharge=0.01`
  - fallback は production-simple fallback として残す
  - `baseline_current_logic` は採用候補ではなく reference parity anchor
- `win_odds` carrier migration の decision freeze:
  - `OZ.win_basis_odds` は legacy `SED.win_odds` の replacement として mainline 採用しない
  - 理由は broad join collapse ではなく、semantic mismatch with a timing mismatch component が有力であるため
  - この判断は current hard-adopt baseline を変更しない
  - `popularity` の true pre-race carrier は引き続き `unresolved`
  - fixed mainline decision は `unresolved_keep_legacy_for_non-mainline_only`
  - follow-up は suspicious races に対する narrow source audit のみに限定する
- current candidate の意味:
  - `no_bet_guard_stronger surcharge=0.01` を base にし、venue-code-based bucket `02 / 09` にだけ追加 surcharge を載せる最小 overlay
  - year 条件は使わない
  - venue_code-based formalization が前提
    - `race_key` は upstream race identifier
    - `race_key[:2]` は upstream-defined `venue_code`
    - domain/group は upstream category ではなく、`venue_code` から project-owned mapping で導出する
  - historical artifact name の `proxy_domain` は互換性維持のため残し、意味は `venue_code` 由来の project-owned bucket と読む
- status の読み方:
  - 採用状態の正は `logic_status.csv` を見る
  - `guard_0_01_plus_proxy_domain_overlay` が `hard_adopt` なら、それを current baseline として扱う
  - `no_bet_guard_stronger` の `fallback_ready` は fallback path が残っていることを示す
  - `candidate_provisional` / `fallback_stable` は package 出力用の legacy `run_mode` 名であり、adopt status ではない
- config の読み方:
  - `provisional_proxy_domain_overlay_enabled=true`: frozen baseline である candidate package を出す
  - `provisional_proxy_domain_overlay_enabled=false`: `no_bet_guard_stronger surcharge=0.01` fallback package を出す
  - `active_run_mode=candidate_provisional`: candidate package の legacy label
  - `active_run_mode=fallback_stable`: fallback package の legacy label
  - `formal_domain_mapping_confirmed=true`: venue_code formalization と project-owned mapping が repo 内で固定済みであることを示す
- metrics snapshot:
  - candidate metrics:
    - `bet_count=161`
    - `hit_count=100`
    - `hit_rate=0.6211`
    - `roi_multiple=1.1944`
    - `total_profit=3130`
    - `max_drawdown=630`
    - `roi_multiple_ci_95=1.0409 - 1.3484`
  - fallback metrics:
    - `bet_count=171`
    - `hit_count=102`
    - `hit_rate=0.5965`
    - `roi_multiple=1.1480`
    - `total_profit=2530`
    - `max_drawdown=740`
    - `roi_multiple_ci_95=1.0035 - 1.3075`
  - parity anchor metrics:
    - `baseline_current_logic`: `bet_count=189`, `roi_multiple=1.1249`, `total_profit=2360`, `max_drawdown=750`
- comparison の読み方:
  - baseline として比較する対象は `guard_0_01_plus_proxy_domain_overlay`
  - fallback 比較は `no_bet_guard_stronger surcharge=0.01`
  - reference parity 確認は `baseline_current_logic`
  - metrics 比較はまず `summary.csv`
  - status 確認は `logic_status.csv`
  - hard-adopt 判定根拠は `hard_adopt_decision.csv` / `hard_adopt_decision_memo.md`
- reference artifact の読み方:
  - `summary.csv`: logic ごとの全体 metrics snapshot
  - `logic_status.csv`: current baseline / fallback / research status の正
  - `hard_adopt_decision.csv`: hard adopt の正式判定
  - `comparison_readout.csv`: baseline / fallback / parity anchor の差分要約
  - `final_instruction_package_summary.csv`: candidate package と fallback package の出力単位要約
  - `final_instruction_package_manifest.csv`: package artifact の manifest
  - `final_candidate_vs_fallback_diff.csv`: candidate と fallback の最終 instruction 差分
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

## BET Logic Source Of Truth

BET logic only の比較で「何を baseline とするか」は、以下の順で固定します。

1. この手順書
   - 定義と読み方の正
2. `configs/bet_logic_only_dual_market_logreg_mainline_2023_2025.toml`
   - frozen baseline package をどう出すかの正
3. `data/artifacts/bet_logic_only_dual_market_logreg_mainline_2023_2025/logic_status.csv`
   - current baseline / fallback / research status の実体
4. `data/artifacts/bet_logic_only_dual_market_logreg_mainline_2023_2025/summary.csv`
   - candidate / fallback / parity anchor metrics の実体
5. `data/artifacts/bet_logic_only_dual_market_logreg_mainline_2023_2025/hard_adopt_decision.csv`
   - hard_adopt 判定の実体

README や他のメモに別表現があっても、この順を優先します。

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
