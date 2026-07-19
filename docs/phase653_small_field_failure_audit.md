# Phase653 Small-field Failure Audit

## Purpose

Phase652 found a reproducible harmful pocket in five-to-seven-runner races. Phase653 decomposes
that failure without creating an exclusion rule or selecting a replacement model.

## Frozen contract

- market logistic `C=3.0`;
- offset residual `C=1.0`;
- five-fold race-group cross-fit;
- Phase652 cross-fit split seed `652`;
- Phase650H historical surface;
- Phase651 place-slot-aware target;
- Phase652 full, history-only, and current-context-only feature subsets;
- 2023 training and 2024 validation;
- 2023-2024 refit and 2025 reused confirmation.

The 2025 period is not a fresh holdout because it has already been inspected.

## Diagnostic questions

1. Is the failure only an unconstrained race-level calibration shift?
2. Does the history residual worsen within-race top-place-slot ranking?
3. Is harm concentrated in positive or negative labels?
4. Do small fields have materially different history availability?
5. Does a fixed `C=1.0` residual trained only on two-place-slot races behave differently from the
   pooled residual?

The slot-2-only fit is compared directly with the pooled fit using the same races, not only with
the market baseline.

The slot-2-only fit is a diagnostic control. It is not a selected candidate and cannot be adopted
from this phase.

## Outputs

- exact field-size and place-slot paired-bootstrap comparisons;
- positive/negative loss decomposition;
- race probability-sum and top-place-slot capture diagnostics;
- history-depth and missingness profiles;
- nested five-to-seven-runner loss comparisons by prior-start depth;
- pooled versus slot-2-only residual-training comparison;
- summary and findings.

No ROI, payout, threshold, BET, candidate, or exclusion artifact is generated.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase653_small_field_failure_audit.py \
  --history-surface /absolute/path/phase650h_history_surface.csv \
  --source-summary /absolute/path/phase650h_source_summary.json \
  --market-dataset /absolute/path/dual_market_logreg/dataset.parquet
```

Generated reports remain under `data/artifacts/` and are not committed.
