# RiverWatch API

RiverWatch provides a read-only FastAPI interface over the local analytics layer.

```text
HTTP
  ↓
FastAPI
  ↓
AnalyticsService
  ↓
DuckDB
  ↓
Processed Parquet
```

The API does not query BIPAD directly. It serves data that has already passed through RiverWatch ingestion, processing, and analytics.

## Running locally

From the project virtual environment:

```bash
python -m uvicorn \
  riverwatch.api.app:create_app \
  --factory \
  --reload \
  --app-dir src
```

The API is available at:

```text
http://127.0.0.1:8000
```

Interactive Swagger documentation is available at:

```text
http://127.0.0.1:8000/docs
```

## Endpoints

### Liveness

```text
GET /api/v1/health/live
```

Checks whether the API application is running.

### Current stations

```text
GET /api/v1/stations
```

Returns the current RiverWatch station snapshot.

Stations without a current observation are still included.

### Current station

```text
GET /api/v1/stations/{station_id}
```

Returns one station from the current station snapshot.

An unknown station returns HTTP `404`.

### Station history

```text
GET /api/v1/stations/{station_id}/history
```

Optional query parameters:

```text
start_at
end_at
```

Both parameters must contain timezone-aware ISO 8601 timestamps.

The interval is interpreted as:

```text
start_at <= observed_at < end_at
```

Results are returned chronologically.

An invalid time interval returns HTTP `422`.

### Basin summaries

```text
GET /api/v1/basins
```

Returns current station-coverage and observation-freshness summaries grouped by basin.

Stations with missing basin metadata are preserved under the `Unknown` group.

### Network summary

```text
GET /api/v1/network/summary
```

Returns current RiverWatch network metrics including station coverage, freshness, missing observations, and observation time range.

## Time semantics

API timestamps are returned as timezone-aware UTC values.

Example:

```text
2026-09-28T21:45:00Z
```

## Data-health semantics

`has_observation` means that a station has an observation in the current RiverWatch snapshot. It does not mean that the observation is recent.

Freshness is evaluated separately using RiverWatch's current-data quality policy.

RiverWatch freshness and quality metrics are analytical data-health indicators. They are not official flood warnings, safety classifications, or emergency alerts.

## Resource lifecycle

The FastAPI application owns one DuckDB analytics connection.

At startup RiverWatch:

1. opens the analytics connection,
2. registers canonical analytics views,
3. creates `AnalyticsService`.

At shutdown the DuckDB connection is closed.

If the required processed Parquet datasets are unavailable, application startup fails rather than serving a partially initialized API.