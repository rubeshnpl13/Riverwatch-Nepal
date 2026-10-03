from pathlib import Path

from riverwatch.events.commands import (
    request_ingestion_main,
)
from riverwatch.events.ingestion import (
    IngestionRequestPublisher,
)
from riverwatch.events.local import (
    LocalEventBus,
)

if __name__ == "__main__":
    raise SystemExit(
        request_ingestion_main()
    )

EVENT_ROOT = Path(
    "data/events"
)


def main() -> None:
    bus = LocalEventBus(
        events_root=EVENT_ROOT
    )

    requests = (
        IngestionRequestPublisher(
            publisher=bus
        )
    )

    event = requests.request(
        provider="bipad",
        endpoint="river-stations",
    )

    print(
        "Published ingestion request"
    )

    print(
        "event_id:",
        event.event_id,
    )

    print(
        "correlation_id:",
        event.correlation_id,
    )

    print(
        "event_type:",
        event.event_type.value,
    )

    print(
        "endpoint:",
        event.data[
            "endpoint"
        ],
    )


if __name__ == "__main__":
    main()