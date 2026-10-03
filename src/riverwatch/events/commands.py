from __future__ import annotations

import argparse
import importlib
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

from riverwatch.config import (
    get_settings,
)
from riverwatch.events.idempotency import (
    LocalIngestionExecutionStore,
)
from riverwatch.events.ingestion import (
    IngestionRequestPublisher,
)
from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.retry import (
    LocalIngestionRetryStore,
    RetryPolicy,
)
from riverwatch.events.runtime import (
    LocalWorkerHost,
)
from riverwatch.events.worker import (
    IngestionRunner,
    IngestionWorker,
)
from riverwatch.observability.logging import (
    configure_logging,
)

DEFAULT_EVENTS_ROOT = Path(
    "data/events"
)

DEFAULT_LAKE_ROOT = Path(
    "data/lake"
)

DEFAULT_PROVIDER = "bipad"

DEFAULT_ENDPOINT = (
    "river-stations"
)

DEFAULT_POLL_INTERVAL_SECONDS = 5.0

DEFAULT_MAX_ATTEMPTS = 3


RunnerFactory = Callable[
    [Path],
    IngestionRunner,
]


def request_ingestion_main(
    argv: Sequence[str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Publish a RiverWatch "
            "ingestion request."
        )
    )

    parser.add_argument(
        "--events-root",
        type=Path,
        default=DEFAULT_EVENTS_ROOT,
    )

    parser.add_argument(
        "--provider",
        default=DEFAULT_PROVIDER,
    )

    parser.add_argument(
        "--endpoint",
        default=DEFAULT_ENDPOINT,
    )

    args = parser.parse_args(
        argv
    )

    queue = LocalEventBus(
        events_root=args.events_root,
    )

    publisher = (
        IngestionRequestPublisher(
            publisher=queue
        )
    )

    event = publisher.request(
        provider=args.provider,
        endpoint=args.endpoint,
    )

    print(
        "Published ingestion request"
    )

    print(
        f"event_id={event.event_id}"
    )

    print(
        "correlation_id="
        f"{event.correlation_id}"
    )

    print(
        "provider="
        f"{event.data['provider']}"
    )

    print(
        "endpoint="
        f"{event.data['endpoint']}"
    )

    return 0


def worker_main(
    argv: Sequence[str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the RiverWatch local "
            "ingestion event worker."
        )
    )

    parser.add_argument(
        "--events-root",
        type=Path,
        default=DEFAULT_EVENTS_ROOT,
    )

    parser.add_argument(
        "--lake-root",
        type=Path,
        default=DEFAULT_LAKE_ROOT,
    )

    parser.add_argument(
        "--runner-factory",
        required=True,
        help=(
            "Runner factory in "
            "'module:function' format."
        ),
    )

    parser.add_argument(
        "--once",
        action="store_true",
    )

    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=(
            DEFAULT_POLL_INTERVAL_SECONDS
        ),
    )

    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
    )

    args = parser.parse_args(
        argv
    )

    settings = get_settings()

    configure_logging(
        log_level=settings.log_level
    )

    runner_factory = (
        load_runner_factory(
            args.runner_factory
        )
    )

    runner = runner_factory(
        args.lake_root
    )

    worker = build_local_worker(
        events_root=args.events_root,
        runner=runner,
        max_attempts=(
            args.max_attempts
        ),
    )

    host = LocalWorkerHost(
        worker=worker,
        poll_interval_seconds=(
            args
            .poll_interval_seconds
        ),
    )

    if args.once:
        processed_count = (
            host.run_once()
        )

        print(
            "Worker cycle complete"
        )

        print(
            "processed="
            f"{processed_count}"
        )

        return 0

    print(
        "RiverWatch ingestion worker "
        "started"
    )

    print(
        "poll_interval_seconds="
        f"{args.poll_interval_seconds}"
    )

    try:
        host.run_forever()

    except KeyboardInterrupt:
        print(
            "RiverWatch ingestion "
            "worker stopped"
        )

    return 0


def build_local_worker(
    *,
    events_root: Path,
    runner: IngestionRunner,
    max_attempts: int = (
        DEFAULT_MAX_ATTEMPTS
    ),
) -> IngestionWorker:
    queue = LocalEventBus(
        events_root=events_root,
    )

    return IngestionWorker(
        queue=queue,
        runner=runner,
        execution_store=(
            LocalIngestionExecutionStore(
                events_root=events_root,
            )
        ),
        retry_store=(
            LocalIngestionRetryStore(
                events_root=events_root,
            )
        ),
        retry_policy=RetryPolicy(
            max_attempts=max_attempts,
        ),
    )


def load_runner_factory(
    specification: str,
) -> RunnerFactory:
    module_name, separator, name = (
        specification.partition(":")
    )

    if (
        not separator
        or not module_name.strip()
        or not name.strip()
    ):
        raise ValueError(
            "runner factory must use "
            "'module:function' format"
        )

    module = importlib.import_module(
        module_name
    )

    candidate = getattr(
        module,
        name,
        None,
    )

    if not callable(
        candidate
    ):
        raise ValueError(
            "runner factory is not "
            "callable: "
            f"{specification}"
        )

    return cast(
        RunnerFactory,
        candidate,
    )