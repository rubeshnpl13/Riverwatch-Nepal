import pytest

from riverwatch.events.runtime import (
    LocalWorkerHost,
)


class RecordingWorker:
    def __init__(
        self,
        results: list[int],
    ) -> None:
        self._results = iter(
            results
        )

        self.calls = 0

    def run_once(self) -> int:
        self.calls += 1

        return next(
            self._results
        )


def test_run_once_delegates_to_worker() -> None:
    worker = RecordingWorker(
        [2]
    )

    host = LocalWorkerHost(
        worker=worker,
    )

    assert host.run_once() == 2

    assert worker.calls == 1


def test_run_cycles_accumulates_processed_count() -> None:
    worker = RecordingWorker(
        [
            1,
            0,
            2,
        ]
    )

    sleeps: list[float] = []

    host = LocalWorkerHost(
        worker=worker,
        poll_interval_seconds=2.5,
        sleeper=sleeps.append,
    )

    assert (
        host.run_cycles(
            max_cycles=3
        )
        == 3
    )

    assert worker.calls == 3

    assert sleeps == [
        2.5,
        2.5,
    ]


def test_run_cycles_rejects_invalid_cycle_count() -> None:
    host = LocalWorkerHost(
        worker=RecordingWorker(
            [0]
        )
    )

    with pytest.raises(
        ValueError,
        match=(
            "max_cycles must be "
            "at least 1"
        ),
    ):
        host.run_cycles(
            max_cycles=0
        )


def test_host_rejects_negative_poll_interval() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "poll_interval_seconds "
            "must not be negative"
        ),
    ):
        LocalWorkerHost(
            worker=RecordingWorker(
                [0]
            ),
            poll_interval_seconds=-1,
        )