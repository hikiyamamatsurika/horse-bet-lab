# Phase652 Historical Signal Robustness

## Purpose

Phase651 found a small incremental signal from strictly-prior horse history. Phase652 tests
whether that read survives a stricter stacking contract and identifies where it is fragile.

This phase does not select a model from 2025 results and does not connect the signal to betting or
expected value.

## Frozen inputs

- market logistic `C=3.0`;
- offset residual `C=1.0`;
- Phase650H history feature surface;
- Phase651 place-slot-aware target;
- 2023 training and 2024 validation;
- 2023-2024 refit and 2025 reused confirmation.

The 2025 period is not called a new holdout because its result was already inspected in Phase651.

## Cross-fit contract

Training offsets are produced by five-fold `StratifiedGroupKFold`, grouped by `race_key`.
No training row receives a market probability from a model trained on its own label. The final
evaluation market model still fits all rows available before the evaluation period.

## Feature groups

- current context: venue, distance, card field size;
- history depth and recency: prior starts, days since last start;
- recent form: finish percentiles, top-three rates, recency-weighted form;
- compatibility: recent distance and venue compatibility rates.

Each leave-one-group-out result is diagnostic only. Hyperparameters remain frozen.

Two targeted controls are evaluated with the same paired race bootstrap as the full model:

- history-only residual: removes venue, current distance, and card field size;
- current-context-only residual: removes every strictly-prior history group.

The historical-residual hypothesis is confirmed only if the history-only residual improves both
matched market baselines in both periods. This prevents current race context from being mislabeled
as horse-history signal.

## Stability slices

The full cross-fitted model is compared with its matched market baseline by:

- prior-start depth;
- active field size;
- market popularity;
- half-year.

Race-level paired bootstrap intervals are emitted for every sufficiently large subgroup.
An overall improvement does not imply uniform improvement. Statistically supported harmful
subgroups are emitted in the summary and keep the result research-only even when the overall
bootstrap verdict is positive.

## Adoption boundary

This phase may confirm that prior-history features contain incremental probability signal. It
cannot authorize a betting rule. A positive overall verdict is retained as a diagnostic when a
known subgroup fails, when payout/EV has not been evaluated, or while 2025 remains a reused
confirmation period rather than a fresh holdout.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase652_historical_signal_robustness.py \
  --history-surface /absolute/path/phase650h_history_surface.csv \
  --source-summary /absolute/path/phase650h_source_summary.json \
  --market-dataset /absolute/path/dual_market_logreg/dataset.parquet
```

Generated reports remain under `data/artifacts/` and are not committed.
