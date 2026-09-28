from itertools import islice

from riverwatch.config import get_settings
from riverwatch.ingestion.bipad.client import (
    create_bipad_client,
)


def main() -> None:
    settings = get_settings()

    with create_bipad_client(
        settings
    ) as client:
        stations = list(
            islice(
                client.iter_river_stations(
                    params={
                        "limit": 5,
                    },
                    max_pages=2,
                ),
                5,
            )
        )

    print(
        f"Received {len(stations)} stations"
    )

    for station in stations:
        print(
            station.id,
            station.title,
            station.water_level,
            station.water_level_on,
        )


if __name__ == "__main__":
    main()