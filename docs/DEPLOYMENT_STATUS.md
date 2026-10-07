# RiverWatch Nepal — Phase 12 Deployment Status

## Purpose

Phase 12 was started as the final deployment and live-verification phase for RiverWatch Nepal.

The project owner has intentionally decided **not to deploy to Google Cloud yet** in order to avoid enabling billing or incurring cloud costs.

This document records what has already been completed for Phase 12 and what remains for a future deployment session.

---

## Current Project Status

RiverWatch core implementation is complete and deployment-ready.

Current release commit:

```text
195beb0  Added BigQuery-backed cloud API
```

The local `main` branch has already been pushed and synchronized with `origin/main`.

The relevant recent commit series is:

```text
51c4ba4  Add shared GCP runtime foundations
19e3feb  Added durable ingestion run repositories
149a3c6  Added managed cloud processing and completion relay
195beb0  Added BigQuery-backed cloud API
```

---

## Phase 12 Status

### 12A — Deployment Readiness Preflight — COMPLETE

Verified:

- Git working tree and remote synchronization.
- Current release commit.
- Google Cloud CLI installation.
- Active GCP account.
- Target project:
  - `riverwatch-nepal-dev`
- Target region:
  - `asia-south1`
- Application Default Credentials.
- Docker installation and runtime.
- Terraform installation.
- Terraform initialization and validation.
- Required Terraform variables.
- Required GCP service list.
- Billing status.
- Existing Terraform state.

Important findings:

```text
Billing enabled: false
Terraform state: none
Cloud resources applied by Terraform: none
```

This is the desired state while deployment is intentionally paused.

---

### 12B — Local Production Container Verification — COMPLETE

All production/container images successfully built locally using:

```text
linux/amd64
```

Verified images:

```text
riverwatch-api:phase12-ready
riverwatch-ingestion:phase12-ready
riverwatch-processing-gateway:phase12-ready
riverwatch-processing-completion-relay:phase12-ready
riverwatch-processing-spark:phase12-ready
riverwatch-spark-runtime-compat:phase12-ready
```

Verified production runtime users:

```text
API                         riverwatch
Ingestion                   riverwatch
Processing gateway          riverwatch
Processing completion relay riverwatch
Processing Spark            spark
```

The compatibility Spark image is local/test-only.

No images were pushed to Artifact Registry.

---

### 12C — Terraform Deployment Readiness — COMPLETE

Verified:

```text
terraform fmt -check -recursive   PASS
terraform validate               PASS
Terraform policy checks          PASS
```

Deployment safety defaults:

```hcl
enable_cloud_run_event_services = false
enable_api_service              = false
enable_ingestion_schedule       = false
```

The development `terraform.tfvars` currently keeps scheduled ingestion disabled.

Also confirmed:

```text
Terraform state files: none
Terraform plan files: none
```

Therefore no GCP infrastructure has been provisioned by this Terraform stack.

---

## Final Local Verification Already Completed

Before Phase 12 deployment preparation, the complete RiverWatch repository passed:

```text
484 tests passed
Ruff passed
mypy passed for 114 source files
Terraform validation passed
Terraform policy/security checks passed
Processing completion relay contract passed
git diff --check passed
```

The managed processing completion architecture was also verified locally:

```text
Spark processing
    -> durable processing-completed.json receipt
    -> GCS OBJECT_FINALIZE contract
    -> processing completion relay
    -> processing.completed domain event
```

---

## Current Terraform Deployment Contract

Terraform expects the following production image names:

```text
api:<release-tag>
ingestion:<release-tag>
processing:<release-tag>
processing-completion-relay:<release-tag>
processing-spark:<release-tag>
```

Important mapping:

```text
containers/processing-gateway.Dockerfile
    -> Artifact Registry image name: processing
```

The local compatibility image:

```text
spark-runtime-compat
```

is not a production image and does not need to be pushed.

The Terraform release tag currently configured is:

```text
phase-11-8d-rc1
```

Before a future real deployment, this tag should be reviewed and updated to the intended release version if necessary.

---

# Deployment Is Paused Here

Do not continue the following steps until cloud hosting is intentionally approved.

Do not run:

```bash
terraform apply
terraform destroy
gcloud services enable ...
gcloud run deploy ...
docker push ...
gcloud artifacts repositories create ...
```

Do not enable GCP billing merely to continue development.

---

# What Remains for Future Deployment

Everything remaining in Phase 12 requires, or directly supports, a real cloud deployment.

## 12D — Deployment Runbook / Final Deployment Procedure

Status:

```text
DEFERRED UNTIL DEPLOYMENT
```

This step should document the exact production deployment sequence once the deployment date/release tag is chosen.

It should cover:

- enabling billing intentionally;
- confirming GCP account/project;
- Terraform bootstrap;
- Artifact Registry creation;
- container image tagging;
- image push;
- Terraform deployment plan;
- Cloud Run/API/service deployment;
- scheduler activation;
- rollback and teardown instructions.

A deployment runbook is useful, but it is not required for the local RiverWatch implementation itself.

---

## Future Step — Bootstrap GCP Infrastructure

When deployment is approved:

1. Enable billing for the intended project.
2. Re-run deployment preflight.
3. Confirm target project and region.
4. Run Terraform plan with optional services still disabled.
5. Review the bootstrap plan.
6. Apply only after reviewing expected resources.

Terraform manages required GCP APIs, including:

```text
Artifact Registry
Cloud Run
Pub/Sub
Cloud Scheduler
Cloud Storage
Dataproc / Managed Spark
BigQuery
IAM
Cloud Resource Manager
Logging
Monitoring
```

---

## Future Step — Publish Container Images

After Artifact Registry exists:

1. Configure Docker authentication for Artifact Registry.
2. Choose one release tag.
3. Build all production images as `linux/amd64`.
4. Tag them using the Terraform-expected names.
5. Push:
   - `api`
   - `ingestion`
   - `processing`
   - `processing-completion-relay`
   - `processing-spark`
6. Verify all uploaded images before enabling Cloud Run services.

---

## Future Step — Terraform Application Deployment

After images are available:

Plan deployment with:

```text
enable_cloud_run_event_services = true
enable_api_service              = true
enable_ingestion_schedule       = false
```

The ingestion scheduler should remain disabled until manual end-to-end validation succeeds.

Review the Terraform plan carefully before applying it.

---

## Future Step — Live End-to-End Verification

A real cloud deployment is not considered complete until this path has been verified:

```text
BIPAD
  -> ingestion Cloud Run
  -> raw GCS objects
  -> ingestion manifest
  -> ingestion.completed
  -> processing gateway
  -> Managed Spark
  -> processed outputs
  -> quality report
  -> processing-completed.json
  -> GCS OBJECT_FINALIZE
  -> completion relay
  -> processing.completed
  -> BigQuery analytics
  -> FastAPI
```

Verify:

- Cloud Run logs;
- Pub/Sub delivery;
- GCS objects;
- Spark execution;
- durable completion receipt;
- completion-relay delivery;
- BigQuery views;
- API responses.

---

## Future Step — Failure and Recovery Verification

Verify in the deployed environment:

- ingestion idempotency;
- duplicate requests;
- Spark retry behavior;
- persisted completion event reuse;
- Pub/Sub redelivery;
- completion-relay retry behavior;
- dead-letter behavior;
- downstream tolerance of at-least-once event delivery.

Do not assume global exactly-once delivery.

---

## Future Step — Optional Scheduler Enablement

Only after manual cloud verification succeeds should recurring ingestion be enabled.

Current development schedule:

```text
5 * * * *
```

Timezone:

```text
Etc/UTC
```

Scheduled execution may create ongoing cloud costs and should remain disabled until explicitly desired.

---

# What Is Complete Without Cloud Deployment

The following work does **not** need to be repeated:

- local ingestion implementation;
- durable ingestion repositories;
- GCS storage implementation;
- Pub/Sub event serialization/push handling;
- managed processing gateway;
- Dataproc/Managed Spark submission logic;
- processing input/output validation;
- durable processing-completion receipt;
- processing completion relay;
- retry/redelivery integration;
- BigQuery analytics service;
- FastAPI cloud API;
- Terraform definitions;
- IAM policy checks;
- Dockerfiles;
- production container builds;
- integration tests;
- full unit/integration regression suite.

The remaining uncertainty is not implementation correctness under local/integration tests; it is **live behavior inside an actual GCP deployment**, which can only be verified when deployment is intentionally performed.

---

# Resume Point

When RiverWatch is ready to be hosted, resume Phase 12 from:

```text
PHASE 12 — CLOUD DEPLOYMENT

1. Re-run deployment preflight.
2. Enable billing intentionally.
3. Review/update the release image tag.
4. Terraform bootstrap plan.
5. Bootstrap infrastructure.
6. Build/tag/push Artifact Registry images.
7. Terraform application deployment plan.
8. Deploy Cloud Run/event/API services.
9. Run live end-to-end verification.
10. Run failure/recovery verification.
11. Optionally enable scheduled ingestion.
12. Document/tag the deployed release.
```

Until then, the recommended state is:

```text
RiverWatch core implementation: COMPLETE
Deployment readiness: COMPLETE
Cloud deployment: PAUSED
Cloud billing: DISABLED
Cloud hosting cost: NONE FROM THIS TERRAFORM DEPLOYMENT
```
