# RiverWatch Data Quality

RiverWatch preserves upstream BIPAD/DHM data and evaluates its quality separately. The quality layer does not silently repair, remove, or overwrite suspicious source values.

## Quality statuses

Each evaluated row receives one of three statuses:

- `pass` — no configured quality rules were triggered.
- `warn` — the row remains usable but one or more suspicious or incomplete conditions were detected.
- `fail` — one or more required identity, lineage, coordinate, or duplicate-key requirements failed.

Warnings and failures are stored as machine-readable rule names in:

- `quality_errors`
- `quality_warnings`
- `quality_error_count`
- `quality_warning_count`
- `quality_status`

## Observation quality

Observation checks include:

### Failure rules

- missing source record ID
- missing station ID
- missing observation timestamp
- missing processing run ID
- invalid latitude
- invalid longitude
- duplicate source record ID within a run

### Warning rules

- missing water level
- missing coordinates
- suspiciously extreme water level
- warning level greater than danger level
- stale observation from the current `river-stations` endpoint
- multiple observations for the same station and timestamp within one run

Historical observations from the `river` endpoint are not evaluated using the current-data freshness rule.

The default current-observation freshness window is 24 hours.

## Station quality

Station checks include:

### Failure rules

- missing station ID
- missing station name
- missing processing run ID
- invalid latitude
- invalid longitude
- duplicate station ID within a run

### Warning rules

- missing station series ID
- missing basin name
- missing coordinates
- duplicate station series ID within a run

Optional station fields such as description, elevation, and administrative identifiers are not currently treated as quality failures.

## Duplicate detection

Duplicate detection is scoped to a processing run.

A station appearing multiple times with the same `station_id` is treated as a failure.

An observation appearing multiple times with the same `source_record_id` is treated as a failure.

Multiple observations from the same station at different timestamps are normal time-series data and are not duplicates.

Multiple observations for the same station at the same timestamp are preserved but warned rather than automatically deleted.

## Current-data health

For the current `river-stations` endpoint, RiverWatch calculates run-level health separately from row quality.

The health summary contains:

- total stations
- stations with observations
- stations without observations
- total current observations
- fresh observations
- stale observations
- future-dated observations
- observations whose freshness cannot be assessed
- station coverage ratio
- observation freshness ratio
- oldest observation timestamp
- latest observation timestamp

Current-data health is:

- `fail` when no stations exist, no observations exist, or none of the observations are fresh.
- `warn` when some stations lack observations, observations are stale, timestamps are future-dated, or freshness cannot be assessed.
- `pass` when all stations have observations and all observations are fresh.

These statuses describe RiverWatch data quality and operational freshness. They are not official flood warnings or government alert classifications.

## Dataset summaries

Quality-evaluated datasets are aggregated into run-level summaries containing:

- total rows
- pass rows
- warning rows
- failure rows
- affected rows
- pass ratio
- dataset status
- error-rule counts
- warning-rule counts

Dataset status uses the most severe row status:

- empty dataset → `fail`
- any failed row → `fail`
- otherwise any warning row → `warn`
- otherwise → `pass`

Rule counts may be greater than the number of affected rows because one row can trigger multiple rules.

## Persistent reports

Each completed processing run produces an immutable JSON quality report.

Current-run example:

```text
quality/
└── endpoint=river-stations/
    └── year=YYYY/
        └── month=MM/
            └── day=DD/
                └── hour=HH/
                    └── run-<run-id>/
                        └── report.json
```

Historical runs use the same layout under `endpoint=river`.

A `river-stations` quality report contains:

- station quality summary
- current-observation quality summary
- current-data health summary

A historical `river` quality report contains:

- observation quality summary
- `station_summary = null`
- `current_data_health = null`

Reports are create-only and immutable.

## Processing lineage

The quality layer maintains the processing-run lineage:

```text
BIPAD / DHM
    ↓
immutable raw JSON
    ↓
completed ingestion manifest
    ↓
Spark transformations
    ↓
processed Parquet
    ↓
quality evaluation
    ↓
immutable report.json
```

The same run ID is retained through processed outputs and the corresponding quality report.

## Current observed source health

Development inspection of the September 2026 BIPAD snapshot showed:

- 284 stations
- 279 stations with observations
- 5 stations without observations
- 188 fresh current observations
- 91 stale current observations
- 98.24% station coverage
- 67.38% current-observation freshness

These values describe the inspected source snapshot and are not permanent expectations or configured thresholds.