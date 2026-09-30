from pathlib import Path

from riverwatch.analytics.catalog import (
    create_analytics_connection,
)


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    connection = (
        create_analytics_connection(
            lake_root=lake_root
        )
    )

    try:
        print()
        print("=" * 70)
        print(
            "STATION OBSERVATION HISTORY"
        )
        print()

        total = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM station_observation_history
                """
            ).fetchone()[0]
        )

        unique_stations = (
            connection.execute(
                """
                SELECT
                    COUNT(
                        DISTINCT station_id
                    )
                FROM station_observation_history
                """
            ).fetchone()[0]
        )

        print(
            "History rows:",
            total,
        )

        print(
            "Stations represented:",
            unique_stations,
        )

        print()
        print(
            "STATION WITH MOST OBSERVATIONS"
        )

        top_station = (
            connection.execute(
                """
                SELECT
                    station_id,
                    COUNT(*) AS observations,
                    MIN(observed_at),
                    MAX(observed_at)
                FROM station_observation_history
                GROUP BY station_id
                ORDER BY
                    observations DESC,
                    station_id
                LIMIT 1
                """
            ).fetchone()
        )

        if top_station is None:
            print(
                "No observations found."
            )
            return

        station_id = (
            top_station[0]
        )

        print(
            "Station:",
            station_id,
        )

        print(
            "Observations:",
            top_station[1],
        )

        print(
            "Oldest:",
            top_station[2],
        )

        print(
            "Latest:",
            top_station[3],
        )

        print()
        print(
            "LATEST 20 OBSERVATIONS"
        )

        rows = (
            connection.execute(
                """
                SELECT
                    observed_at,
                    water_level_m,
                    endpoint,
                    source_record_id
                FROM station_observation_history
                WHERE station_id = ?
                ORDER BY
                    observed_at DESC
                LIMIT 20
                """,
                [
                    station_id,
                ],
            ).fetchall()
        )

        for row in rows:
            print(
                row
            )

        print()
        print(
            "DUPLICATE TIMESTAMP CHECK"
        )

        duplicate_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT
                        station_id,
                        observed_at
                    FROM
                        station_observation_history
                    GROUP BY
                        station_id,
                        observed_at
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )

        print(
            "Duplicate station/timestamps:",
            duplicate_count,
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()