# Phase656 Popularity Carrier Comparison

## Purpose

Phase655 showed that the frozen Phase651 market baseline cannot be reproduced prospectively as
written: it uses popularity from result-side SED, while the forward contract has no confirmed
decision-time popularity carrier.

Phase656 compares safe replacements before changing Phase654. It uses only the established
2023-2025 historical surface; no 2026 row is read.

## Fixed variants

| Variant | Features | Role |
|---|---|---|
| `legacy_result_popularity` | log win odds, log place-basis odds, SED popularity | invalid prospective comparator only |
| `no_popularity` | log win odds, log place-basis odds | safe candidate |
| `decision_time_win_rank` | log win odds, log place-basis odds, midrank derived from observed win odds | safe candidate |

Equal visible win odds receive the same midrank. Horse number is not used as an artificial
tie-breaker.

## Selection and evaluation

- train each logistic market model on 2023;
- select regularization separately for each variant on 2024;
- select between the two safe variants using 2024 constrained-market (`M1C`) binary Log Loss;
- refit on 2023-2024;
- report 2025 once after selection.

The 2025 period is called historical evaluation, not a fresh holdout. It has already been inspected
by earlier phases, even though the safe popularity variants were not previously compared this way.

Race-level paired bootstrap deltas compare both safe candidates with the legacy diagnostic and
compare the two safe candidates directly. No ROI measure selects a model.

## Interpretation boundary

Historical accuracy cannot make result-side SED popularity eligible for a prospective feature.
The legacy variant is excluded from safe-candidate selection regardless of its score.

Phase656 selects a candidate for a new preregistration. It does not edit Phase654 and does not yet
rerun the full Phase651-653 history-residual and small-field research. That rerun is the next gate.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase656_popularity_carrier_comparison.py \
  --history-surface /absolute/path/phase650h_history_surface.csv \
  --source-summary /absolute/path/phase650h_source_summary.json \
  --market-dataset /absolute/path/dual_market_logreg/dataset.parquet
```

Generated artifacts remain under `data/artifacts/` and are not committed.
