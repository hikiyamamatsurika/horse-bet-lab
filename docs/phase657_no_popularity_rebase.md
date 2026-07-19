# Phase657 No-Popularity Rebase

## Purpose

Phase656 selected `no_popularity` as the decision-time-safe market contract. Phase657 reruns the
historical Phase651-653 probability research before any preregistration is amended.

The market model uses only:

- log win odds;
- log place-basis odds.

No 2026 row is read. The 2025 period remains reused historical confirmation, not a fresh holdout.

## Reused machinery

- Phase650H corrected place-slot labels and strictly-prior history surface;
- Phase651 chronological regularization selection and race probability constraint;
- Phase652 race-group cross-fitting, ablations, and paired bootstrap;
- Phase653 small-field decomposition and slot-2-only diagnostic.

Phase651 now records the supplied market feature names rather than hard-coding popularity in its
summary. Phase652 omits popularity subgroup slices when the market matrix has no popularity column.
Phase653 leaves the descriptive mean-popularity field blank in the same case. Model formulas are
unchanged.

## Gate

The historical conclusions are considered reproduced when:

- Phase651 still supports incremental strictly-prior history signal; and
- Phase652 does not change to `HISTORY_SIGNAL_CROSSFIT_NOT_CONFIRMED`.

Phase652 may remain diagnostic-only because the known small-field pocket is the subject of
Phase653, not a reason to restore an invalid result-side market feature.

## Boundary

This phase does not amend Phase654. It produces the evidence required for that amendment. It does
not inspect 2026 data or use ROI, payouts, thresholds, stakes, selections, or BET logic.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase657_no_popularity_rebase.py \
  --history-surface /absolute/path/phase650h_history_surface.csv \
  --source-summary /absolute/path/phase650h_source_summary.json \
  --market-dataset /absolute/path/dual_market_logreg/dataset.parquet
```

Generated reports remain under `data/artifacts/` and are not committed.
