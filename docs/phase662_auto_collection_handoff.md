# Phase662 Automatic Collection Handoff

Phase662 connects the existing local JRDB archive downloader/extractor to the Phase661
collection-only path with `handoff.mode = "forward_collection_tyb_oz_v1"`.

Unlike the older `forward_pre_race_*` modes, this mode requires no dataset, model version, DuckDB,
threshold, or betting configuration. It stops after the append-only contract snapshot and Phase660
coverage monitor are written.

```json
{
  "trigger_kind": "manual",
  "detected_at": "2026-07-20T15:20:00+09:00",
  "archives": [
    {
      "name": "replace-with-archive.zip",
      "source_uri": "/absolute/path/to/archive.zip"
    }
  ],
  "handoff": {
    "mode": "forward_collection_tyb_oz_v1",
    "ingest_ready_files": false,
    "unit_id": "20260720_replace_with_meeting",
    "input_source_name": "jrdb_tyb_oz_official",
    "input_source_url": "https://jrdb.com/replace-with-actual-source",
    "input_source_timestamp": "2026-07-20T15:19:00+09:00",
    "odds_observation_timestamp": "2026-07-20T15:20:00+09:00"
  }
}
```

Run it through the existing `horse_bet_lab.jrdb_ingestion.cli`. Real collection remains blocked
until the registered window opens and an actual TYB/OZ archive is locally available.
