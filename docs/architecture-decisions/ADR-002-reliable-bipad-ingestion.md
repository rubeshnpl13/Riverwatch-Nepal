# ADR-002: Reliable BIPAD Ingestion Strategy

**Status:** Accepted  
**Date:** 2026-08-29

## Context

RiverWatch Nepal ingests river monitoring data from the BIPAD
public API.

The upstream API presents several reliability and data-quality
challenges:

- transient HTTP and network failures are possible;
- HTTP 429 and selected 5xx responses may be temporary;
- BIPAD's pagination `count` is not reliable;
- BIPAD may provide a `next` URL after the dataset has been
  exhausted;
- individual records may be malformed while other records in the
  same response remain usable;
- current station snapshots can contain fresh, stale, or missing
  observations;
- historical ingestion may involve substantially more data than
  current station ingestion.

## Decision

RiverWatch uses a layered ingestion adapter for BIPAD.

### HTTP reliability

The BIPAD client retries transient request failures and the following
HTTP status codes:

- 408
- 429
- 500
- 502
- 503
- 504

Retries use exponential backoff.

Client and other non-retryable HTTP errors fail immediately.

### Pagination

RiverWatch does not derive page counts from BIPAD's `count` field.

Pagination follows the upstream `next` URL and includes:

- repeated-URL detection;
- a maximum pagination safety limit;
- termination when `next` is null;
- termination when an empty result page is returned, even if BIPAD
  still supplies another `next` URL.

Historical backfills additionally use a separate `page_limit` so a
caller can intentionally process a bounded number of pages without
triggering the pagination safety mechanism.

### Validation

External BIPAD records are validated with source-specific Pydantic
models.

A malformed individual record is quarantined rather than causing the
entire ingestion batch to fail.

A malformed page/envelope remains a pipeline-level failure.

### Domain mapping

Validated BIPAD records are converted into RiverWatch canonical
domain models.

BIPAD is recorded as the API provider while the original data source
is represented independently.

Current station snapshots may produce both:

- `RiverStation`
- `RiverObservation`

A station without an observation timestamp still produces a station
but does not produce an observation.

Historical `/river/` records map to `RiverObservation`.

### Batch timestamps

All observations produced by one ingestion run share the same
`ingested_at` timestamp.

Source observation timestamps remain unchanged.

### Historical processing

Historical records are processed page by page rather than collecting
the entire source history in memory.

A batch handler receives each completed historical page. This allows
the storage implementation to be added later without redesigning the
source ingestion logic.

### Observability

Each ingestion run produces structured metrics and structured JSON
logs including:

- records received;
- records valid;
- records invalid;
- stations emitted;
- observations emitted;
- records without observations;
- duration;
- run identifier.

Quarantined raw payloads are preserved by the quarantine contract but
are not dumped into normal application logs.

## Consequences

### Positive

- transient failures are handled predictably;
- upstream pagination defects do not create infinite ingestion loops;
- malformed records do not discard otherwise valid batches;
- source and canonical schemas remain separated;
- historical ingestion remains bounded in memory;
- ingestion behavior is observable and testable;
- storage can be introduced without coupling it to HTTP retrieval.

### Negative

- ingestion contains more explicit infrastructure than a simple API
  script;
- quarantine records require a future persistent storage strategy;
- the system must maintain tests for known upstream API quirks;
- source freshness still requires a separate data-quality policy.

## Future Work

Future phases will add:

- GCS raw and quarantine storage;
- processed Parquet datasets;
- source freshness classification;
- Spark data-quality processing;
- BigQuery curated tables;
- Cloud Monitoring metrics and alerts.