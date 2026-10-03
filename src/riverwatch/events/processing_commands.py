from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from riverwatch.config import (
    get_settings,
)
from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.processing import (
    ProcessingWorker,
)
from riverwatch.events.processing_idempotency import (
    LocalProcessingExecutionStore,
)
from riverwatch.events.processing_retry import (
    LocalProcessingRetryStore,
)
from riverwatch.events.retry import (
    RetryPolicy,
)
from riverwatch.events.runtime import (
    LocalWorkerHost,
)
from riverwatch.observability.logging import (
    configure_logging,
)
from riverwatch.processing.spark.event_runner import (
    SparkProcessingEventRunner,
)
from riverwatch.processing.spark.session import (
    create_local_spark_session,
)

DEFAULT_EVENTS_ROOT = Path(
    "data/events"
)

DEFAULT_LAKE_ROOT = Path(
    "data/lake"
)

DEFAULT_POLL_INTERVAL_SECONDS = 5.0
DEFAULT_MAX_ATTEMPTS = 3



def processing_worker_main(
    argv: Sequence[str] | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run the RiverWatch local "
            "processing event worker."
        )
    )
    parser.add_argument(
        "--events-root",
        type=Path,
        default=DEFAULT_EVENTS_ROOT,
    )

    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
    )

    parser.add_argument(
        "--lake-root",
        type=Path,
        default=DEFAULT_LAKE_ROOT,
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

    args = parser.parse_args(
        argv
    )
    settings = get_settings()

    configure_logging(
        log_level=settings.log_level
    )

    queue = LocalEventBus(
        events_root=args.events_root,
    )

    spark = (
        create_local_spark_session(
            app_name=(
                "riverwatch-processing-worker"
            )
        )
    )

    try:
        runner = (
            SparkProcessingEventRunner(
                spark=spark,
                lake_root=args.lake_root,
            )
        )

        worker = ProcessingWorker(
            queue=queue,
            runner=runner,
            execution_store=(
                LocalProcessingExecutionStore(
                    events_root=args.events_root,
                )
            ),
            retry_store=(
                LocalProcessingRetryStore(
                    events_root=args.events_root,
                )
            ),
            retry_policy=RetryPolicy(
                max_attempts=args.max_attempts,
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
                "Processing worker "
                "cycle complete"
            )

            print(
                "processed="
                f"{processed_count}"
            )

            return 0

        print(
            "RiverWatch processing "
            "worker started"
        )

        print(
            "poll_interval_seconds="
            f"{args.poll_interval_seconds}"
        )

        try:
            host.run_forever()

        except KeyboardInterrupt:
            print(
                "RiverWatch processing "
                "worker stopped"
            )

        return 0

    finally:
        spark.stop()