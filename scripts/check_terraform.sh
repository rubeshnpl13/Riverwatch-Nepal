#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

TERRAFORM_ROOT="${PROJECT_ROOT}/infra/terraform"
DEV_STACK="${TERRAFORM_ROOT}/stacks/dev"


echo "==> Initializing Terraform"
terraform \
  -chdir="${DEV_STACK}" \
  init \
  -backend=false \
  -input=false \
  >/dev/null


echo "==> Checking Terraform formatting"
terraform \
  -chdir="${DEV_STACK}" \
  fmt \
  -check \
  -recursive


echo "==> Validating Terraform configuration"
terraform \
  -chdir="${DEV_STACK}" \
  validate


echo "==> Checking privileged IAM patterns"

if grep -R \
  -n \
  -E 'roles/(owner|editor)|google_service_account_key' \
  "${TERRAFORM_ROOT}" \
  --exclude='.terraform.lock.hcl' \
  --exclude-dir='.terraform'
then
  echo "ERROR: Broad IAM role or service-account key found."
  exit 1
fi


echo "==> Checking public IAM principals"

if grep -R \
  -n \
  -E 'allUsers|allAuthenticatedUsers' \
  "${TERRAFORM_ROOT}" \
  --exclude-dir='.terraform'
then
  echo "ERROR: Public IAM principal found."
  exit 1
fi


echo "==> Checking Dataproc service-agent role misuse"

if grep -R \
  -n \
  'roles/dataprocrm.nodeServiceAgent' \
  "${TERRAFORM_ROOT}" \
  --exclude-dir='.terraform'
then
  echo "ERROR: Dataproc service-agent role assigned explicitly."
  exit 1
fi


echo "==> Checking Terraform-owned Spark batches"

if grep -R \
  -n \
  'google_dataproc_batch' \
  "${TERRAFORM_ROOT}" \
  --exclude-dir='.terraform'
then
  echo "ERROR: Runtime Spark batches must not be Terraform resources."
  exit 1
fi


echo "==> Terraform checks passed"