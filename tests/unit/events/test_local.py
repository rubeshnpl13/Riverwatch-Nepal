from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path
from uuid import UUID, uuid4

from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.model import (
    EventEnvelope,
    EventType,
)


def make_event() -> EventEnvelope:
    return EventEnvelope(
        event_id=uuid4(),
        event_type=(
            EventType.INGESTION_REQUESTED
        ),
        occurred_at=datetime.now(
            UTC
        ),
        correlation_id=uuid4(),
        data={
            "endpoint":
                "river-stations",
        },
    )


def test_publish_creates_pending_json(
    tmp_path,
) -> None:
    bus = LocalEventBus(
        events_root=(
            tmp_path / "events"
        )
    )

    event = make_event()

    bus.publish(event)

    path = (
        tmp_path
        / "events"
        / "pending"
        / f"{event.event_id}.json"
    )

    assert path.exists()


def test_publish_serializes_event(
    tmp_path,
) -> None:
    bus = LocalEventBus(
        events_root=(
            tmp_path / "events"
        )
    )

    event = make_event()

    bus.publish(event)

    pending = bus.list_pending()

    assert len(pending) == 1

    restored = pending[0]

    assert (
        restored.event_id
        == event.event_id
    )

    assert (
        restored.event_type
        == event.event_type
    )

    assert (
        restored.correlation_id
        == event.correlation_id
    )

    assert (
        restored.occurred_at
        == event.occurred_at
    )

    assert (
        dict(restored.data)
        == dict(event.data)
    )


def test_duplicate_publish_does_not_overwrite(
    tmp_path,
) -> None:
    bus = LocalEventBus(
        events_root=(
            tmp_path / "events"
        )
    )

    event = make_event()

    bus.publish(event)

    path = (
        tmp_path
        / "events"
        / "pending"
        / f"{event.event_id}.json"
    )

    original = path.read_text(
        encoding="utf-8"
    )

    bus.publish(event)

    current = path.read_text(
        encoding="utf-8"
    )

    assert current == original


def test_list_pending_returns_all_events(
    tmp_path,
) -> None:
    bus = LocalEventBus(
        events_root=(
            tmp_path / "events"
        )
    )

    first = make_event()
    second = make_event()

    bus.publish(first)
    bus.publish(second)

    events = bus.list_pending()

    assert len(events) == 2

    event_ids = {
        event.event_id
        for event in events
    }

    assert first.event_id in event_ids
    assert second.event_id in event_ids


def test_mark_processed_moves_event(
    tmp_path,
) -> None:
    bus = LocalEventBus(
        events_root=(
            tmp_path / "events"
        )
    )

    event = make_event()

    bus.publish(event)

    pending_path = (
        tmp_path
        / "events"
        / "pending"
        / f"{event.event_id}.json"
    )

    processed_path = (
        tmp_path
        / "events"
        / "processed"
        / f"{event.event_id}.json"
    )

    assert pending_path.exists()
    assert not processed_path.exists()

    bus.mark_processed(
        event.event_id
    )

    assert not pending_path.exists()
    assert processed_path.exists()

#test mark failed
def test_mark_failed_moves_event(
    tmp_path: Path,
) -> None:
    bus = LocalEventBus(
        events_root=tmp_path,
    )

    event = make_event()

    bus.publish(
        event
    )

    bus.mark_failed(
        event.event_id
    )

    assert not (
        tmp_path
        / "pending"
        / f"{event.event_id}.json"
    ).exists()

    assert (
        tmp_path
        / "failed"
        / f"{event.event_id}.json"
    ).is_file()

#retry-store tests


REQUEST_EVENT_ID = UUID(
    "11111111-1111-1111-1111-111111111111"
)

FIRST_FAILURE = datetime(
    2026,
    10,
    3,
    0,
    0,
    tzinfo=UTC,
)

SECOND_FAILURE = (
    FIRST_FAILURE
    + timedelta(
        minutes=1
    )
)