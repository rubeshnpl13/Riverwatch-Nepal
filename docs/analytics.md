# RiverWatch Analytics Layer

RiverWatch uses DuckDB as a lightweight local analytics engine over processed Parquet data.

The Parquet lake remains the source of truth. DuckDB queries the files directly rather than maintaining a second persistent copy of the data.

## Architecture

```text
Processed Parquet
      ↓
DuckDB base views
      ↓
Canonical analytics views
      ↓
AnalyticsService
      ↓
Future API / dashboard
```

## Canonical views

The analytics layer currently provides:

- `current_stations` — stations from the latest processed `river-stations` run
- `current_observations` — observations from the latest current run
- `current_river_snapshot` — one row per current station with its latest current observation
- `historical_observations` — historical observations deduplicated across processing runs
- `station_observation_history` — canonical chronological station history combining historical and current data
- `latest_observation_per_station` — latest known observation for each station
- `basin_current_summary` — current station coverage and freshness by basin
- `current_network_summary` — network-wide current-data summary

## Typed service

Application code should use `AnalyticsService` instead of executing DuckDB SQL directly.

The service currently provides:

```text
get_current_snapshot()
get_station()
get_station_history()
get_basin_summaries()
get_network_summary()
```

Service results are returned as immutable typed dataclasses.

Station-history parameters are passed to DuckDB using parameterized SQL.

## Time semantics

RiverWatch treats processed observation timestamps as UTC.

The analytics service converts DuckDB timestamps into timezone-aware UTC Python `datetime` values before returning them to application code.

## Current versus historical data

`current_*` views use only the latest processed `river-stations` run.

Historical observations may span multiple processing runs. Duplicate historical source records are removed in the canonical analytics view.

`station_observation_history` combines current and historical observations and keeps at most one record for each `(station_id, observed_at)` pair.

## Aggregate analytics

Basin and network summaries include metrics such as:

- number of stations
- stations with and without observations
- fresh and stale observations
- missing water-level measurements
- station coverage ratio
- freshness ratio
- oldest and latest observation timestamps

RiverWatch does not currently calculate basin-wide average water levels because gauge levels from different stations are not necessarily directly comparable.