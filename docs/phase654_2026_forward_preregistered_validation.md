# Phase654 2026 Forward Preregistered Validation

> Superseded for prospective execution by the additive Phase658 no-popularity amendment. This
> Phase654 document and its checksummed JSON remain unchanged evidence in the registration lineage.

## Purpose

Phase652 found a small incremental probability signal from strictly-prior horse history.
Phase653 found that the full residual was unstable in two-place-slot races and that slot-2-only
training did not repair that pocket consistently across the already-inspected 2024 and 2025
periods.

Phase654 freezes a prospective evaluation before its collection window begins. No complete 2026
evaluation dataset is assumed to exist. The phase registers the question, periods, models,
comparisons, minimum sample, and decision rules. It does not execute the evaluation.

## Freshness boundary

The registered evaluation window is 2026-07-20 through 2026-12-31. Training is fixed to
2023-01-01 through 2025-12-31. Evaluation remains locked until 2027-01-01 even if partial data
arrives earlier.

The freshness claim applies only to races in the prospective window. It makes no claim about
races before 2026-07-20.

The forward source may not be inventoried until this contract is merged. After collection starts,
features, model definitions, hyperparameters, comparisons, thresholds, and minimum sample rules
may not be changed for this evaluation.

Before 2027-01-01, monitoring is limited to schema, dates, canonical identities, missingness, and
join completeness without model predictions, model metrics, or outcome-conditioned diagnostics.

## Fixed questions

### Overall history signal

The primary overall comparison is the constrained, history-only pooled residual against the
constrained market baseline on every complete supported race. This prevents current race context
from being relabeled as horse-history signal.

### Two-place-slot recovery

The full slot-2-only residual must beat both:

1. the same full residual trained across all supported place-slot counts; and
2. the constrained market baseline.

Both comparisons use exactly the complete two-place-slot evaluation races. History-only
slot-2 comparisons remain supporting diagnostics rather than alternate success paths.

## Statistical contract

- primary metric: mean binary log loss;
- paired sampling unit: race key;
- 2,000 paired race-bootstrap repetitions with seed 654;
- success requires a negative candidate-minus-baseline point delta and a 95% interval whose
  upper bound is below zero;
- market logistic `C=3.0`;
- residual logistic `C=1.0`;
- five-fold stratified race-group cross-fit with seed 652;
- race-level probability-sum constraint retained for every compared model.

The evaluation is insufficient rather than negative when fewer than 500 complete races exist
overall, or when two-place-slot coverage is below either 50 complete races or 300 rows.

## Data gates

- canonical identity is race key plus horse number;
- only complete races are evaluated;
- duplicate identities block the run;
- partially joined history races block the run;
- target or place-slot mismatches block the run;
- races with four or fewer active runners are unsupported.

## Boundary

This phase does not compute model metrics and does not inspect forward-window source coverage.
ROI, payout, bet selection, thresholds, and stakes are outside scope. The next phase after merge
is a prospective collection readiness and schema audit.

The JSON contract and its SHA-256 sidecar are committed together. The validator fails if the
contract bytes, fixed periods, model set, primary comparisons, hyperparameters, sample gates, or
phase boundary change.

## Validation

```bash
PYTHONPATH=src .venv/bin/python scripts/phase654_preregister_2026_forward_validation.py
```
