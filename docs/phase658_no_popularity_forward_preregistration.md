# Phase658 No-Popularity Forward Preregistration Amendment

## Purpose

Phase654 registered a prospective 2026 evaluation, but its market baseline still depended on a
historical `popularity` column. Phase655 showed that the column came from result-side SED and had no
confirmed equivalent decision-time carrier. Phase656 selected a no-popularity market contract, and
Phase657 reproduced the historical Phase651-653 conclusions under that contract without reading
2026 data.

Phase658 records a new, immutable amendment instead of rewriting the Phase654 artifact. The original
contract and checksum remain part of the lineage.

## Amended market contract

The prospective market model uses exactly this ordered feature vector:

1. `log1p(win_odds)`;
2. `log1p(place_basis_odds)`.

`popularity` is excluded. Training-fit preprocessing remains median imputation, missing indicators,
mean centering, and population-standard-deviation scaling. The selected market logistic `C` changes
from `3.0` to `1.0`, as selected by the Phase657 no-popularity rerun. The residual logistic remains
fixed at `C=1.0`.

Periods, target, place-slot rule, model roles, comparisons, cross-fit settings, bootstrap settings,
minimum sample gates, success rules, and ROI prohibition remain unchanged from Phase654.

## Lineage and immutability

The amended JSON records:

- the exact SHA-256 of the superseded Phase654 contract;
- the Phase655-657 verdicts and merge commits;
- the no-popularity market feature order and transforms;
- the source hashes of the historical ability and robustness implementation at the Phase657 merge;
- that no 2026 data, fresh-2025 claim, ROI, payout, threshold, stake, or betting rule was used.

The Phase658 JSON has its own SHA-256 sidecar. Validation fails when the amendment bytes, Phase654
lineage, market features, preprocessing, hyperparameters, evidence, implementation hashes, periods,
comparisons, statistical rules, or phase boundary change.

## Freshness boundary

The evaluation window remains 2026-07-20 through 2026-12-31 and stays locked until 2027-01-01.
Before unlock, only schema, date, identity, missingness, and join-completeness monitoring is allowed.
Model predictions, metrics, and outcome-conditioned diagnostics remain forbidden.

No complete 2026 dataset is expected or required by this phase. Phase658 is registration-only.

## Validation

```bash
PYTHONPATH=src .venv/bin/python \
  scripts/phase658_preregister_no_popularity_forward_validation.py
```
