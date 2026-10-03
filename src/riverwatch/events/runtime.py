from __future__ import annotations

from collections.abc import Callable
from time import sleep
from typing import Protocol

Sleeper = Callable[[float], None]


class Worker(Protocol):
    def run_once(self) -> int:
        """Run one worker cycle."""
        ...


class LocalWorkerHost:
    def __init__(
        self,
        *,
        worker: Worker,
        poll_interval_seconds: float = 5.0,
        sleeper: Sleeper = sleep,
    ) -> None:
        if poll_interval_seconds < 0:
            raise ValueError(
                "poll_interval_seconds must "
                "not be negative"
            )

        self._worker = worker
        self._poll_interval_seconds = (
            poll_interval_seconds
        )
        self._sleeper = sleeper

    def run_once(self) -> int:
        return self._worker.run_once()

    def run_cycles(
        self,
        *,
        max_cycles: int,
    ) -> int:
        if max_cycles < 1:
            raise ValueError(
                "max_cycles must be "
                "at least 1"
            )

        processed_total = 0

        for cycle_index in range(
            max_cycles
        ):
            processed_total += (
                self._worker.run_once()
            )

            if (
                cycle_index
                < max_cycles - 1
            ):
                self._sleeper(
                    self
                    ._poll_interval_seconds
                )

        return processed_total

    def run_forever(self) -> None:
        while True:
            self._worker.run_once()

            self._sleeper(
                self._poll_interval_seconds
            )