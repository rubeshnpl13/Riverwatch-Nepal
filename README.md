# RiverWatch Nepal

RiverWatch Nepal is a local-first data engineering project for collecting, processing, and monitoring river data from Nepal.

The project currently uses river data exposed through the BIPAD API, with hydrological data originating from Nepal's Department of Hydrology and Meteorology (DHM).

The goal is to build a reliable near-real-time river data pipeline while preserving source data, tracking lineage, and identifying data-quality problems before the data is used by future APIs or dashboards.

## Current Pipeline

```text
BIPAD / DHM API
      ↓
Python ingestion
      ↓
Immutable raw JSON
      ↓
Run manifests
      ↓
Apache Spark
      ↓
Processed Parquet
      ↓
Data-quality checks
      ↓
Quality reports
```

Local data is organized under:

```text
data/lake/
├── raw/
├── quarantine/
├── manifests/
├── processed/
└── quality/
```

## What Has Been Implemented

### Reliable ingestion

- BIPAD API client
- retries and pagination protection
- validation and quarantine
- immutable raw storage
- ingestion metrics and run manifests
- SHA-256 raw-file lineage

### Spark processing

- manifest-driven processing
- explicit Spark schemas
- station normalization
- current and historical observation normalization
- immutable Snappy Parquet output
- run-level lineage

### Data quality

RiverWatch evaluates source data without silently changing suspicious values.

Current checks include:

- missing required IDs and timestamps
- invalid coordinates
- missing water levels
- suspicious water-level values
- inconsistent warning/danger levels
- duplicate stations and observations
- stale current observations
- station observation coverage

Each row can be classified as:

```text
pass
warn
fail
```

The pipeline also creates dataset-level summaries and immutable JSON quality reports for every processed run.

## Current Development Snapshot

A recent `river-stations` run contained:

```text
284 stations
279 stations with observations
5 stations without observations

279 current observations
188 fresh
91 stale

Station coverage: 98.24%
Observation freshness: 67.38%
```

The historical development backfill currently contains:

```text
2000 observations
2000 passing quality checks
```

These values describe one source snapshot and are not permanent thresholds.

## Run Tests

```bash
pytest
ruff check .
mypy src
```

## Run Spark Processing

```bash
python scripts/write_processed_parquet.py
```

Processed datasets are written under:

```text
data/lake/processed/
```

Quality reports are written under:

```text
data/lake/quality/
```

## Project Status

```text
Phase 1 — Engineering foundations        ✅
Phase 2 — Reliable ingestion             ✅
Phase 3 — Local raw data lake            ✅
Phase 4 — Spark processing               ✅
Phase 5 — Data quality                   ✅
```

Next stages will build analytical and serving layers on top of the processed and quality-checked data.

> RiverWatch quality statuses describe data quality and freshness. They are not official flood warnings or government emergency alerts.
