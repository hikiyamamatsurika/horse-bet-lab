# Phase655 Forward Collection Readiness

## Purpose

Phase654 registered a prospective 2026-07-20 through 2026-12-31 evaluation window and locked
model evaluation until 2027-01-01. Phase655 checks only whether the existing repository contracts
can collect and later reproduce every fixed input. It does not inspect forward-window files or
compute model results.

## Confirmed collection paths

- BAC, KYI, OZ, and SED are supported raw file kinds.
- Triggered JRDB archive ingestion preserves extracted raw files and can hand supported records to
  staging.
- Phase650H accepts explicit history and evaluation years, so its strictly-prior history surface
  can be rebuilt after the evaluation period and result files arrive.
- The Phase650H surface contains every historical feature required by Phase651.
- Phase654's checksum, dates, fixed models, comparisons, and metric gates validate.

## Blocking mismatch

The fixed Phase651 `dual_market` input contains:

- `win_odds`;
- `place_basis_odds`;
- `popularity`.

The current dataset query obtains popularity from result-side SED. The forward-test contract,
however, defines popularity as optional `unresolved_auxiliary`; it is not a confirmed pre-race
carrier. Consequently the exact fixed market baseline cannot currently substantiate its
"decision-time market" description in prospective collection.

This is not a missing-row problem that can be patched after collection. It is a feature-timing and
carrier-equivalence problem.

## Verdict

`FORWARD_COLLECTION_READINESS_BLOCKED`

Collection must not be called model-ready until one of these paths is chosen before forward model
results are inspected:

1. confirm and snapshot a pre-race popularity carrier equivalent to the Phase651 feature; or
2. preregister a no-popularity baseline and repeat the 2023-2025 probability research with that
   fixed feature contract before using the forward window.

Phase655 does not choose between those paths and does not change the model.

## Safety boundary

This audit reads code and committed contracts only. It does not read raw source contents, count
forward rows, condition diagnostics on outcomes, produce predictions or probability metrics, or
touch ROI, payout, selection, threshold, stake, or BET logic.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase655_forward_collection_readiness.py
```

The expected non-zero process exit reflects the blocked readiness verdict. Generated reports
remain under `data/artifacts/` and are not committed.
