# ADR-004: Spark-Based Processed Data Lake

**Status:** Accepted  
**Date:** 2026-08-30

## Context

RiverWatch stores immutable BIPAD API responses in a raw local data
lake.

Downstream analytics should not operate directly on nested upstream
JSON because source schemas contain pagination envelopes, nested
structures, inconsistent nullability, source-specific field names,
and source-specific timestamp representations.

RiverWatch requires a normalized analytical layer while retaining
traceability to the original ingestion run.

## Decision

RiverWatch uses local Apache Spark through PySpark to transform
completed raw ingestion runs into normalized Parquet datasets.

Processing is manifest-driven.

A processing run begins with a completed ingestion manifest rather
than recursively scanning the raw data lake.

## Processing Flow

```text
completed manifest
        ↓
resolve raw page keys
        ↓
verify SHA-256 checksums
        ↓
explicit Spark source schema
        ↓
read JSON pages
        ↓
explode results arrays
        ↓
normalize source fields
        ↓
write immutable Parquet