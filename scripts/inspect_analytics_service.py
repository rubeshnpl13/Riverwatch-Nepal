from pathlib import Path

from riverwatch.analytics.catalog import (
    create_analytics_connection,
)
from riverwatch.analytics.service import (
    AnalyticsService,
)


def main() -> None:
    connection = (
        create_analytics_connection(
            lake_root=Path(
                "data/lake"
            )
        )
    )

    try:
        analytics = AnalyticsService(
            connection=connection
        )

        print()
        print("=" * 70)
        print(
            "TYPED ANALYTICS SERVICE"
        )
        print()

        network = (
            analytics
            .get_network_summary()
        )

        print(
            "Network:",
            network,
        )

        print()
        print(
            "CURRENT SNAPSHOT"
        )

        snapshot = (
            analytics
            .get_current_snapshot()
        )

        print(
            "Stations:",
            len(
                snapshot
            ),
        )

        with_observation = sum(
            station.has_observation
            for station in snapshot
        )

        print(
            "With observation:",
            with_observation,
        )

        print(
            "Without observation:",
            (
                len(snapshot)
                - with_observation
            ),
        )

        print()
        print(
            "BASINS"
        )

        basins = (
            analytics
            .get_basin_summaries()
        )

        print(
            "Basins:",
            len(
                basins
            ),
        )

        for basin in basins[:5]:
            print(
                basin
            )

        station = next(
            (
                item
                for item in snapshot
                if item.has_observation
            ),
            None,
        )

        if station is None:
            return

        if station.station_id is None:
            return

        print()
        print(
            "STATION LOOKUP"
        )

        print(
            analytics.get_station(
                station.station_id
            )
        )

        print()
        print(
            "STATION HISTORY"
        )

        history = (
            analytics
            .get_station_history(
                station.station_id
            )
        )

        print(
            "Station:",
            station.station_id,
        )

        print(
            "History rows:",
            len(
                history
            ),
        )

        for observation in (
            history[-5:]
        ):
            print(
                observation
            )

    finally:
        connection.close()


if __name__ == "__main__":
    main()