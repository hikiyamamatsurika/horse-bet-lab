# Phase661 Prospective Collection Operations

## Purpose

Phase661 supplies the operational path that Phase660 deliberately did not own. It converts a
local, already-extracted official JRDB TYB/OZ source directory into one append-only contract
snapshot and immediately runs the Phase660 coverage-only monitor.

The path stops after monitoring. It never invokes the forward model runner, produces predictions
or betting decisions, reads outcomes, or computes model metrics and ROI.

## Preconditions

- the current date is inside 2026-07-20 through 2026-12-31;
- the source directory contains the intended `TYB*.txt` and `OZ*.txt` files only;
- timestamps are timezone-aware and describe the actual observation;
- the unit id starts with the observation date, for example `20260720_tokyo_hakodate`;
- local JRDB authentication and archive acquisition have already completed outside this command.

## Command

```bash
PYTHONPATH=src .venv/bin/python scripts/phase661_prospective_collection_ops.py \
  --unit-id 20260720_replace_with_meeting \
  --source-dir /absolute/path/to/extracted/jrdb/files \
  --input-source-url https://jrdb.com/replace-with-actual-source \
  --input-source-timestamp 2026-07-20T15:20:00+09:00 \
  --odds-observation-timestamp 2026-07-20T15:20:00+09:00
```

Do not use placeholder metadata for a real collection unit. The command has no overwrite flag:
an existing unit directory is a hard error.

## Outputs

Under `data/forward_test/prospective_collection/<unit_id>/`:

- `raw/input_snapshot_raw.csv` and its intake manifest;
- `contract/input_snapshot_<unit_id>.csv` plus aggregate bridge provenance;
- `monitor/phase660_*` coverage-only reports;
- `notes/phase661_collection_summary.json` with file hashes and aggregate counts.

These are local collection artifacts and remain uncommitted.

## Current boundary

As of 2026-07-19 the registered window has not started, so a real run must not be performed yet.
Synthetic test fixtures exercise the complete path without claiming that 2026 data exists.
