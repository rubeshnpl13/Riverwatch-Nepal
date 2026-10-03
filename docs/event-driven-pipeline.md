# RiverWatch Event-Driven Pipeline

## Purpose

RiverWatch uses a local durable event workflow to connect ingestion and
processing while preserving a design that can later be mapped to managed
scheduler and messaging services.

The local implementation requires no paid cloud infrastructure.

## Pipeline

```text
scheduler / CLI
    |
    v
ingestion.requested
    |
    v
IngestionWorker
    |
    +--> BIPAD API
    |
    +--> raw lake
    +--> quarantine
    +--> ingestion manifest
    |
    v
ingestion.completed
    |
    v
ProcessingWorker
    |
    +--> PySpark transforms
    +--> processed Parquet
    +--> quality report
    |
    v
processing.completed