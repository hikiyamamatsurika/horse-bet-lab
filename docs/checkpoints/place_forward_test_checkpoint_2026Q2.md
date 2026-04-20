# Place Forward-Test Checkpoint 2026Q2

## Current baseline

- current candidate: `guard_0_01_plus_proxy_domain_overlay`
- status: `hard_adopt`
- fallback: `no_bet_guard_stronger surcharge=0.01`

## What is now on main

- hard-adopt baseline and data/domain formalization
- feature contract, registry, provenance, and missing-null policy
- place-only roadmap and Phase 1 acceptance
- place-only forward-test Phase 1 stack:
  - input/output contract
  - pre-race runner
  - post-race reconciliation
  - thin snapshot bridge
  - operator runbook

## What has been explicitly closed or frozen

- popularity carrier: `unresolved_keep_legacy`
- `win_odds` replacement migration: `not adopted / frozen`
- non-standard `race_key` normalization: moved to Issue `#9`

## What Phase 1 can do now

- accept pre-race input through the forward-test contract
- convert raw/live-ish snapshot input into contract CSV through the thin bridge
- run prediction and bet decision on the current place-only baseline path
- emit explicit `no_bet` on failure states such as timeout or required odds missing
- reconcile post-race results without recalculating the original decision
- support operator-run end-to-end rehearsal with provenance-bearing artifacts

## Remaining manual steps

- live snapshot to raw-like input acquisition
- operator-run execution cadence
- result DB availability check before reconciliation
- adapting placeholder/example config values to each local environment

## Next natural tasks

- define and fix the operator cadence/checklist for recurring Phase 1 runs
- run recurring operator rehearsal and only revisit Issue `#9` when a non-standard `race_key` source family needs to be supported

## Boundaries to keep

- do not change the current baseline or BET logic while Phase 1 operations are being stabilized
- do not reopen `popularity` carrier resolution or frozen `win_odds` migration work through this path
