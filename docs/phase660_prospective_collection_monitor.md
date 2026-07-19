# Phase660 Prospective Collection Monitor

## Purpose

Phase659 confirmed that the checksummed no-popularity contract has a reproducible static collection
path. Phase660 adds the pre-unlock monitor permitted by Phase658: schema, date, identity, file hash,
snapshot status, and required-odds missingness only.

The monitor never trains or runs a probability model. It does not read result files or compute
performance metrics.

## Allowed input

Each input must be a contract-stage forward snapshot CSV with at least:

- `race_key`;
- `horse_number`;
- `win_odds`;
- `place_basis_odds`;
- `odds_observation_timestamp` with timezone;
- `carrier_identity`;
- `snapshot_status`.

Files are hashed with SHA-256. Identities must be unique across all supplied snapshots, and
observation dates must remain inside 2026-07-20 through 2026-12-31.

## Hard boundary

The monitor rejects files containing target, finish, result, payout, return, profit, prediction,
probability, model-metric, ROI, stake, decision, or betting columns. It reports only aggregate
collection coverage and never copies horse-level values into its reports.

## Verdicts

- `FORWARD_COLLECTION_MONITOR_WAITING_FOR_WINDOW`: correct before 2026-07-20 with no input;
- `FORWARD_COLLECTION_MONITOR_WAITING_FOR_SNAPSHOTS`: window open, no snapshot supplied;
- `FORWARD_COLLECTION_MONITOR_OK`: supplied snapshots satisfy the collection contract;
- `FORWARD_COLLECTION_MONITOR_BLOCKED`: schema, date, identity, odds, or boundary violation.

## Commands

Before the window starts:

```bash
PYTHONPATH=src .venv/bin/python scripts/phase660_prospective_collection_monitor.py \
  --as-of-date 2026-07-19 \
  --output-dir data/artifacts/phase660_prospective_collection_monitor
```

During the window, add one `--snapshot` argument for every append-only contract snapshot file.
Do not point this monitor at reconciliation, prediction, decision, result, or payout artifacts.

Generated monitoring reports remain uncommitted.
