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
        print("RIVERWATCH ANALYTICS")
        print()

        station_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM stations
                """
            ).fetchone()[0]
        )

        observation_count = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM observations
                """
            ).fetchone()[0]
        )

        print(
            "Stations:",
            station_count,
        )

        print(
            "Observations:",
            observation_count,
        )

        print()
        print(
            "OBSERVATIONS BY ENDPOINT"
        )

        rows = connection.execute(
            """
            SELECT
                endpoint,
                COUNT(*) AS observation_count
            FROM observations
            GROUP BY endpoint
            ORDER BY endpoint
            """
        ).fetchall()

        for (
            endpoint,
            count,
        ) in rows:
            print(
                f"{endpoint}: {count}"
            )

        print()
        print(
            "OBSERVATION TIME RANGE"
        )

        time_range = (
            connection.execute(
                """
                SELECT
                    MIN(observed_at),
                    MAX(observed_at)
                FROM observations
                """
            ).fetchone()
        )

        print(
            "Oldest:",
            time_range[0],
        )

        print(
            "Latest:",
            time_range[1],
        )

        print()
        print(
            "CANONICAL VIEW COUNTS"
        )

        view_counts = (
            connection.execute(
                """
                SELECT
                    (
                        SELECT COUNT(*)
                        FROM current_stations
                    ),
                    (
                        SELECT COUNT(*)
                        FROM current_observations
                    ),
                    (
                        SELECT COUNT(*)
                        FROM historical_observations
                    ),
                    (
                        SELECT COUNT(*)
                        FROM latest_observation_per_station
                    )
                """
            ).fetchone()
        )

        print(
            "Current stations:",
            view_counts[0],
        )

        print(
            "Current observations:",
            view_counts[1],
        )

        print(
            "Historical observations:",
            view_counts[2],
        )

        print(
            "Stations with latest observation:",
            view_counts[3],
        )

        print()
        print(
            "LATEST CURRENT RUN"
        )

        latest_run = (
            connection.execute(
                """
                SELECT
                    run_id,
                    ingested_at
                FROM latest_current_run
                """
            ).fetchone()
        )

        if latest_run is None:
            print(
                "No current river-stations run found."
            )
        else:
            print(
                "Run:",
                latest_run[0],
            )

            print(
                "Ingested at:",
                latest_run[1],
            )

    finally:
        connection.close()


if __name__ == "__main__":
    main()