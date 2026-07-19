# Phase650H Historical Ability Source Bridge

## Decision

This phase implements only the source and leakage gate required before historical-ability
modeling.

It does not fit a probability model and does not run betting, threshold, stake-sizing, or
selection logic.

## Hypothesis boundary

The testable hypothesis is that strictly prior horse history may add a stable residual signal
beyond the decision-time market probability. The market-residual model is the primary future
candidate; a market-independent model is only a diagnostic control.

## Reused source assets

- KYI pre-race card files provide current-race horse identity.
- SED result files provide historical starts and the current target.
- BAC files provide current race distance without using result-side distance as a feature.

KYI now ingests:

- `registration_id` from zero-based bytes `10:18`;
- `horse_name` from zero-based bytes `18:54`;
- the previously prototyped jockey, trainer, carried-weight, frame, and sex fields.

`registration_id` and `horse_name` are identity fields, not model features.

The summary emits separate machine-readable `identity_columns`, `label_columns`, and
`model_feature_columns` lists. Downstream modeling must use only the last list as predictors.

## Temporal contract

For a current race on date `D`, historical features may use only SED rows satisfying:

```text
prior_result_date < D
```

Same-day earlier races are intentionally excluded. Source-file order is never used as a time
proxy.

## Generated surface

The generated CSV contains current pre-race identity, current BAC distance, label metadata,
`is_place`, and these history-only candidate features:

- prior-start count;
- days since last start;
- last 1/3/5 finish percentile;
- last 3/5 top-three rate;
- last-five recency-weighted form;
- last-five distance and venue compatibility rates.

Current finish position is not emitted. It is used only to derive the `is_place` target and
explicitly label-prefixed metadata (`label_result_starter_count` and `label_place_slots`).

## Gate

The ready verdict requires:

- KYI, SED, and BAC source files;
- no duplicate current identity keys;
- at least `99.9%` exact KYI-to-SED registration/name match;
- zero strict-prior violations.

Failure produces `HISTORICAL_ABILITY_SOURCE_BRIDGE_BLOCKED`. Success produces
`HISTORICAL_ABILITY_SOURCE_BRIDGE_READY`.

Backup records with identical feature-relevant parsed content are removed deterministically and
counted. A duplicate key with different parsed content is a blocker; it is never resolved by
taking whichever file happens to be read last.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase650h_historical_ability_source_audit.py \
  --raw-root /absolute/path/to/data/raw/jrdb
```

Generated files are written under `data/artifacts/` by default and remain uncommitted.
