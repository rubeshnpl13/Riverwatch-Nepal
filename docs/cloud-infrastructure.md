# RiverWatch Cloud Infrastructure

## Status

The RiverWatch Google Cloud infrastructure is defined with Terraform.

The infrastructure has not yet been deployed.

Phase 10 validates infrastructure-as-code contracts only. Application container
images and runtime cloud adapters are introduced in Phase 11.

Do not interpret Terraform validation as evidence that live Google Cloud
resources have been created or tested.

## Region

The development stack currently uses:

- Region: `asia-south1`
- Environment: `dev`

Storage, BigQuery, Cloud Run, Artifact Registry, Cloud Scheduler, and Managed
Service for Apache Spark runtime contracts use the same primary region where
supported.

## Architecture

```text
Cloud Scheduler
      |
      v
Pub/Sub
ingestion.requested
      |
      | authenticated OIDC push
      v
Cloud Run ingestion service
      |
      +----> Cloud Storage
      |       raw/
      |       quarantine/
      |       manifests/
      |
      v
Pub/Sub
ingestion.completed
      |
      | authenticated OIDC push
      v
Cloud Run processing gateway
      |
      | submit event-derived batch
      v
Managed Service for Apache Spark
      |
      +----> Cloud Storage
      |       processed/
      |       quality/
      |
      v
Pub/Sub
processing.completed


Cloud Storage processed Parquet
      |
      v
BigLake external tables
      |
      v
BigQuery analytics dataset
      |
      v
Cloud Run FastAPI
      |
      v
React dashboard