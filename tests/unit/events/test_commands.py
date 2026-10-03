from pathlib import Path
from uuid import UUID

import pytest

from riverwatch.events.commands import (
    build_local_worker,
    load_runner_factory,
    request_ingestion_main,
)
from riverwatch.events.local import (
    LocalEventBus,
)
from riverwatch.events.model import (
    EventType,
)
from riverwatch.events.worker import (
    IngestionRunReference,
)


class FakeRunner:
    def run(
        self,
        *,
        provider: str,
        endpoint: str,
        idempotency_key: UUID,
    ) -> IngestionRunReference:
        return IngestionRunReference(
            run_id="run-123",
            manifest_path=(
                "manifest.json"
            ),
        )


def test_request_command_publishes_event(
    tmp_path: Path,
) -> None:
    exit_code = (
        request_ingestion_main(
            [
                "--events-root",
                str(tmp_path),
                "--provider",
                "bipad",
                "--endpoint",
                "river-stations",
            ]
        )
    )

    assert exit_code == 0

    queue = LocalEventBus(
        events_root=tmp_path,
    )

    pending = queue.list_pending()

    assert len(pending) == 1

    event = pending[0]

    assert (
        event.event_type
        == EventType
        .INGESTION_REQUESTED
    )

    assert (
        event.data["provider"]
        == "bipad"
    )

    assert (
        event.data["endpoint"]
        == "river-stations"
    )


def test_build_local_worker(
    tmp_path: Path,
) -> None:
    worker = build_local_worker(
        events_root=tmp_path,
        runner=FakeRunner(),
        max_attempts=4,
    )

    assert worker.run_once() == 0


@pytest.mark.parametrize(
    "specification",
    [
        "",
        "module",
        ":factory",
        "module:",
    ],
)
def test_runner_factory_rejects_invalid_specification(
    specification: str,
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            "runner factory must use"
        ),
    ):
        load_runner_factory(
            specification
        )


def test_runner_factory_rejects_missing_callable() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "runner factory is not callable"
        ),
    ):
        load_runner_factory(
            "pathlib:missing_factory"
        )


def test_runner_factory_loads_callable() -> None:
    factory = load_runner_factory(
        "pathlib:Path"
    )

    value = factory(
        Path("data/lake")
    )

    assert (
        value
        == Path("data/lake")
    )