#!/usr/bin/env bash

set -euo pipefail

IMAGE="${1:-riverwatch-processing-completion-relay:11.8b3c4}"

CLOUD_RUN_EVENTS="infra/terraform/stacks/dev/cloud_run_events.tf"
PUBSUB="infra/terraform/stacks/dev/pubsub.tf"
STORAGE="infra/terraform/stacks/dev/storage.tf"
SERVICE_ACCOUNTS="infra/terraform/stacks/dev/service_accounts.tf"


fail() {
  echo "ERROR: $*" >&2
  exit 1
}


require_text() {
  local file="$1"
  local text="$2"

  grep \
    -Fq \
    "$text" \
    "$file" \
    || fail "$file is missing required contract: $text"
}


echo "==> Checking relay container image exists"

docker image inspect \
  "$IMAGE" \
  >/dev/null


echo "==> Checking relay image architecture"

architecture="$(
  docker image inspect \
    "$IMAGE" \
    --format '{{.Architecture}}'
)"

if [[ "$architecture" != "amd64" ]]; then
  fail "relay image architecture is $architecture, expected amd64"
fi


echo "==> Checking relay image runs as non-root"

runtime_user="$(
  docker image inspect \
    "$IMAGE" \
    --format '{{.Config.User}}'
)"

if [[ -z "$runtime_user" ]]; then
  fail "relay image has no explicit runtime user"
fi

if [[ "$runtime_user" == "root" || "$runtime_user" == "0" ]]; then
  fail "relay image must not run as root"
fi


echo "==> Checking relay container command"

container_command="$(
  docker image inspect \
    "$IMAGE" \
    --format '{{json .Config.Cmd}}'
)"

if [[ "$container_command" != *"riverwatch.cloud.processing_completion_relay_app:create_app"* ]]; then
  fail "relay image does not launch processing_completion_relay_app:create_app"
fi

if [[ "$container_command" != *"--factory"* ]]; then
  fail "relay Uvicorn command must use --factory"
fi

if [[ "$container_command" != *'${PORT}'* ]]; then
  fail "relay Uvicorn command must use the Cloud Run PORT variable"
fi


echo "==> Checking relay runtime dependencies and app contract"

docker run \
  --rm \
  --platform linux/amd64 \
  --env HOME=/tmp \
  --entrypoint sh \
  "$IMAGE" \
  -lc '
set -eu

python -m pip check

python - <<'"'"'PY'"'"'
import importlib.util

from riverwatch.cloud.processing_completion_relay_app import (
    create_app,
)


required_modules = (
    "fastapi",
    "uvicorn",
    "google.cloud.storage",
    "google.cloud.pubsub_v1",
)

for module_name in required_modules:
    assert (
        importlib.util.find_spec(module_name)
        is not None
    ), f"missing runtime module: {module_name}"


assert (
    importlib.util.find_spec("pyspark")
    is None
), "PySpark must not be installed in the relay image"


assert (
    importlib.util.find_spec(
        "google.cloud.dataproc_v1"
    )
    is None
), "Dataproc SDK must not be installed in the relay image"


class FakeStore:
    @property
    def bucket_name(
        self,
    ) -> str:
        return "riverwatch-contract-lake"

    def read_bytes(
        self,
        *,
        key: str,
    ) -> bytes:
        raise AssertionError(
            f"health/app contract must not read {key}"
        )


class FakePublisher:
    def publish(
        self,
        event: object,
    ) -> None:
        raise AssertionError(
            f"health/app contract must not publish {event}"
        )


app = create_app(
    store=FakeStore(),
    publisher=FakePublisher(),
)

routes = {
    route.path: getattr(
        route,
        "methods",
        set(),
    )
    for route in app.routes
}

assert "/health/live" in routes
assert "GET" in routes["/health/live"]

assert "/events/pubsub" in routes
assert "POST" in routes["/events/pubsub"]

print("Relay Python dependency contract OK")
print("Relay FastAPI route contract OK")
PY

if command -v java >/dev/null 2>&1; then
  echo "ERROR: Java must not be installed in the relay image" >&2
  exit 1
fi

python -m compileall \
  -q \
  /app/src/riverwatch

echo "Relay source compilation OK"
'


echo "==> Checking Terraform relay service contract"

require_text \
  "$CLOUD_RUN_EVENTS" \
  'module "processing_completion_relay_service"'

require_text \
  "$CLOUD_RUN_EVENTS" \
  'processing_completion_relay_image'

require_text \
  "$CLOUD_RUN_EVENTS" \
  'RIVERWATCH_LAKE_BUCKET'

require_text \
  "$CLOUD_RUN_EVENTS" \
  'RIVERWATCH_PROCESSING_COMPLETED_TOPIC'

require_text \
  "$CLOUD_RUN_EVENTS" \
  '"processing_relay"'


echo "==> Checking processing gateway does not own completion topic"

processing_gateway_block="$(
  sed -n \
    '/module "processing_event_service"/,/module "processing_completion_relay_service"/p' \
    "$CLOUD_RUN_EVENTS"
)"

if grep \
  -Fq \
  'RIVERWATCH_PROCESSING_COMPLETED_TOPIC' \
  <<<"$processing_gateway_block"
then
  fail "processing gateway must not receive RIVERWATCH_PROCESSING_COMPLETED_TOPIC"
fi


echo "==> Checking dedicated storage-notification topic"

require_text \
  "$PUBSUB" \
  'resource "google_pubsub_topic" "processing_completion_receipt_notifications"'

require_text \
  "$PUBSUB" \
  'processing_completion_relay_service.uri}/events/pubsub'

require_text \
  "$PUBSUB" \
  'processing_completion_receipt_dead_letter'

require_text \
  "$PUBSUB" \
  'processing_completion_receipt_gcs_publisher'


echo "==> Checking processing.completed publisher ownership"

publisher_block="$(
  sed -n \
    '/resource "google_pubsub_topic_iam_member" "processing_completion_publisher"/,/^}/p' \
    "$PUBSUB"
)"

if ! grep \
  -Fq \
  '"processing_relay"' \
  <<<"$publisher_block"
then
  fail "processing.completed publisher must be processing_relay"
fi

if grep \
  -Fq \
  '"processing"' \
  <<<"$publisher_block"
then
  fail "Spark processing identity must not publish processing.completed"
fi


echo "==> Checking GCS OBJECT_FINALIZE notification"

require_text \
  "$STORAGE" \
  'resource "google_storage_notification" "processing_completion_receipts"'

require_text \
  "$STORAGE" \
  '"OBJECT_FINALIZE"'

require_text \
  "$STORAGE" \
  '"quality/"'

require_text \
  "$STORAGE" \
  '"processing_relay"'


echo "==> Checking dedicated relay runtime identity"

require_text \
  "$SERVICE_ACCOUNTS" \
  'processing_relay = {'


echo "==> Validating Terraform"

terraform \
  -chdir=infra/terraform/stacks/dev \
  validate


echo "==> Running RiverWatch Terraform policy checks"

./scripts/check_terraform.sh


echo "==> Checking repository diff whitespace"

git diff --check


echo "==> Processing completion relay contract passed"
