# Phase659 No-Popularity Forward Collection Readiness

## Purpose

Phase658 replaced the unresolved result-side popularity dependency with a checksummed market
contract containing only decision-time win odds and place-basis odds. Phase659 performs the static
collection and schema audit required after that amendment is merged.

This phase does not inspect any 2026 source row, coverage count, outcome, prediction, or model
metric. Complete 2026 data is neither expected nor required.

## Static checks

- the Phase658 checksum and its unchanged Phase654 lineage validate;
- the Phase657 implementation source hashes still match;
- the registered market vector is exactly `win_odds`, `place_basis_odds`;
- `popularity` remains explicitly excluded;
- official OZ parsing supplies both win odds and place-basis odds;
- raw intake maps the OZ `place_basis_odds_proxy` field into the required forward record field;
- the forward record schema and runner accept both registered market features;
- BAC, KYI, OZ, and SED historical raw specifications remain available;
- Phase650H remains parameterized for a later evaluation period and contains every required
  historical feature;
- the evaluation window and 2027-01-01 unlock remain unchanged.

## Verdict

`NO_POPULARITY_FORWARD_COLLECTION_READINESS_READY`

The popularity-carrier blocker from Phase655 is cleared by removing the feature, not by inventing
an equivalent carrier after the fact.

## Boundary

This verdict means the repository has a statically reproducible collection path. It does not mean
that future rows already exist, that collection coverage is sufficient, or that prospective model
performance has been evaluated.

Before unlock, monitoring remains limited to schema, dates, identities, missingness, and join
completeness without predictions, outcome conditioning, or model metrics.

## Execution

```bash
PYTHONPATH=src .venv/bin/python scripts/phase659_no_popularity_forward_readiness.py
```

Generated artifacts remain under `data/artifacts/` and are not committed.
