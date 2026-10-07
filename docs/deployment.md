# RiverWatch Nepal — GCP Deployment Runbook

This document describes how to deploy RiverWatch Nepal to Google Cloud Platform when cloud hosting is desired.

The repository is currently designed to remain undeployed and cost-free until deployment is explicitly requested.

## Current deployment status

RiverWatch is deployment-ready, but no GCP infrastructure has been applied.

Expected undeployed state:

- GCP project exists.
- Billing may be disabled.
- Terraform has no state.
- Cloud Run services are disabled.
- Scheduled ingestion is disabled.
- API deployment is disabled.
- Container images can be built locally.
- No Artifact Registry images need to exist until deployment.

The development stack uses:

- Project: `riverwatch-nepal-dev`
- Region: `asia-south1`
- Environment: `dev`

Do not run `terraform apply` unless cloud deployment and possible GCP charges are intentionally desired.

---

## Architecture

The production event flow is:

```text
BIPAD
  |
  v
Ingestion Cloud Run
  |
  v
Google Cloud Storage
  |
  +--> ingestion.completed
  |
  v
Processing Gateway
  |
  v
Managed Service for Apache Spark
  |
  v
Processed data + quality report
  |
  v
processing-completed.json
  |
  v
GCS OBJECT_FINALIZE
  |
  v
Processing Completion Relay
  |
  v
processing.completed
  |
  v
BigQuery analytics
  |
  v
RiverWatch FastAPI