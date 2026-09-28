# ADR-001: Use BIPAD as the Initial River Data Provider

**Status:** Accepted  
**Date:** 2026-08-28

## Context

RiverWatch Nepal requires a public source of river monitoring data that can support near-real-time monitoring, historical analysis, threshold detection, data-quality processing, and cloud-based ingestion.

Nepal's BIPAD platform exposes public API endpoints containing river monitoring data originating from hydrological data providers including Nepal's Department of Hydrology and Meteorology (DHM).

During initial investigation, the following endpoints were evaluated:

- `/api/v1/river-stations/`
- `/api/v1/river/`
- `/api/v1/flood-station/`
- `/api/v1/streamflow/`

The `/river-stations/` endpoint provides current station snapshots including station location, latest water level, observation timestamp, warning and danger thresholds, trend, status, geographical identifiers, and affected-demography information.

The `/river/` endpoint contains river observations associated with station identifiers and is suitable for historical observation ingestion and backfilling.

The `/flood-station/` endpoint appears primarily related to specialized historical, calibration, and flood-model station metadata.

The `/streamflow/` endpoint returned no records during the initial investigation.

Investigation also confirmed that `river.station` corresponds to `river-stations.id`. For example, historical observations with `station = 44` and `stationSeriesId = 19260` correspond to river station `id = 44` with the same station series identifier.

## Decision

RiverWatch will initially use BIPAD as its external API provider.

The source responsibilities will be separated as follows:

- `/river-stations/` will be used for near-real-time station snapshot ingestion.
- `/river/` will be used for historical observation ingestion and backfilling.
- `/flood-station/` will not be part of the initial ingestion pipeline but may be used later for hydrological or flood-model enrichment.
- `/streamflow/` will not be used while no usable records are available.

External BIPAD payloads will not be used directly throughout the application.

Source-specific Pydantic schemas will validate BIPAD payloads and mapping functions will convert them into RiverWatch canonical domain models:

- `RiverStation`
- `RiverObservation`

The original provider will be recorded separately from the original data source to preserve data provenance.

For DHM-derived records:

- Provider: `BIPAD`
- Original data source: `DHM`

## Rationale

Separating external schemas from internal domain models prevents the RiverWatch architecture from becoming tightly coupled to BIPAD's field names and response structure.

Using `/river-stations/` for recurring ingestion provides the most recent known reading for each station.

Using `/river/` separately allows historical data to be processed without mixing historical backfills with the near-real-time polling workflow.

Keeping station metadata and observations as separate canonical models supports a one-to-many relationship in which a station may have many observations over time.

Deterministic observation identifiers based on station and observation timestamp will support idempotent processing when the same current snapshot is retrieved multiple times.

## Consequences

### Positive

- RiverWatch receives real Nepal river-monitoring data.
- Current and historical ingestion workflows can evolve independently.
- External API changes are isolated within the BIPAD adapter.
- Canonical models can later support additional providers.
- Data lineage is preserved.
- Historical observations can reference stable station identifiers.
- Repeated polling can be made idempotent.

### Negative

- BIPAD becomes an external dependency.
- API availability, schema changes, or delayed upstream data may affect ingestion.
- The API's reported `count` value cannot currently be trusted for determining the number of available records.
- Pagination therefore needs to follow the API's `next` links rather than relying on `count`.
- Source freshness must be monitored explicitly.

## Future Considerations

RiverWatch may later integrate directly with DHM if a stable and appropriate machine-consumable interface is available.

Additional hydrological providers may also be integrated without changing RiverWatch's canonical station and observation models.

The platform should track source freshness and distinguish source failures from successful responses containing no new observations.