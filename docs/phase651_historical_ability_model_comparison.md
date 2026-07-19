# Phase651 Historical Ability Model Comparison

## Research question

Does strictly-prior horse history add stable place-probability signal beyond the existing
decision-time market surface?

This phase tests that question before any expected-value, payout, threshold, or bet-selection
work. It does not assume that a market-independent ability model is the production answer.

## Target correction

The existing market dataset target is retained only for an audit. It treats some third-place
finishes in races with fewer than eight active runners as positive, even though those races have
only two place slots.

Every Phase651 candidate therefore uses the Phase650H `is_place` label:

- 5-7 active runners: top two;
- 8 or more active runners: top three.

Active field size is reconstructed from the joined pre-race market rows. The old market target is
never used as a predictor.

## Fixed comparison

| ID | Definition |
|---|---|
| M0 | Place slots divided by active field size |
| M1 | Logistic market baseline: log win odds, log place-basis odds, popularity |
| M1C | M1 followed by the same per-race probability-sum constraint used by M5 |
| M2 | Strictly-prior history and pre-race context only |
| M3 | Direct logistic market plus history model |
| M4 | M1 logit fixed as an offset, with a history residual correction |
| M5 | M4 followed by a per-race logit shift so probabilities sum to place slots |

The `C` grid is fixed in code. Models train on 2023, select regularization on 2024, refit on
2023-2024, and evaluate 2025 once.

## Metrics and decision

Primary metrics are log loss and race-level paired-bootstrap log-loss difference from M1.
Brier score, ROC-AUC, average precision, calibration intercept/slope, ECE, and race probability
sum error are supporting diagnostics.

Incremental history is supported only when at least one matched comparison succeeds:

1. M3 or M4 beats M1, or M5 beats M1C, on both 2024 validation and 2025 holdout; and
2. has a race-bootstrap 95% log-loss-delta interval below zero in both periods.

M1C versus M1 is reported separately so the benefit of the race constraint cannot be attributed
to horse history.

Point improvement without that separation is diagnostic-only. ROI is not a selection metric.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase651_historical_ability_model_comparison.py \
  --history-surface /absolute/path/phase650h_history_surface.csv \
  --source-summary /absolute/path/phase650h_source_summary.json \
  --market-dataset /absolute/path/dual_market_logreg/dataset.parquet
```

Outputs are generated under `data/artifacts/` and remain uncommitted.
