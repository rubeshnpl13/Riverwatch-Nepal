from __future__ import annotations

from datetime import datetime
from typing import (
    Protocol,
    runtime_checkable,
)

from riverwatch.analytics.model import (
    BasinCurrentSummary,
    CurrentNetworkSummary,
    CurrentRiverStation,
    StationObservation,
)


@runtime_checkable
class AnalyticsReader(
    Protocol,
):
    def get_current_snapshot(
        self,
    ) -> tuple[
        CurrentRiverStation,
        ...,
    ]:
        ...

    def get_station(
        self,
        station_id: str,
    ) -> CurrentRiverStation | None:
        ...

    def get_station_history(
        self,
        station_id: str,
        *,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> tuple[
        StationObservation,
        ...,
    ]:
        ...

    def get_basin_summaries(
        self,
    ) -> tuple[
        BasinCurrentSummary,
        ...,
    ]:
        ...

    def get_network_summary(
        self,
    ) -> CurrentNetworkSummary:
        ...