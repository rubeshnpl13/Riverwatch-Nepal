from enum import StrEnum


class BipadEndpoint(StrEnum):
    RIVER = "river"
    RIVER_STATIONS = "river-stations"
    FLOOD_STATION = "flood-station"
    STREAMFLOW = "streamflow"