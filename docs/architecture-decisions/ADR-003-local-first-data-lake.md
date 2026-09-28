# ADR-003: Local-First Immutable Data Lake

**Status:** Accepted  
**Date:** 2026-08-29

## Context

RiverWatch requires durable source-data retention before downstream
processing.

The original architecture considered Google Cloud Storage as the raw
object-storage backend. During early development, the project instead
uses a local filesystem implementation to avoid requiring cloud
billing while preserving the same storage architecture.

The ingestion pipeline must remain independent of the physical
storage provider so a cloud object store can be introduced later
without redesigning ingestion services.

## Decision

RiverWatch uses an object-store abstraction and a local immutable data
lake during development.

The local lake root is:

```text
data/lake/