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
            "CURRENT RIVER SNAPSHOT"
        )
        print()

        summary = (
            connection.execute(
                """
                SELECT
                    COUNT(*)
                        AS total_stations,

                    COUNT(
                        DISTINCT station_id
                    )
                        AS unique_stations,

                    COUNT(*) FILTER (
                        WHERE has_observation
                    )
                        AS with_observation,

                    COUNT(*) FILTER (
                        WHERE NOT has_observation
                    )
                        AS without_observation

                FROM current_river_snapshot
                """
            ).fetchone()
        )

        print(
            "Snapshot rows:",
            summary[0],
        )

        print(
            "Unique stations:",
            summary[1],
        )

        print(
            "With observation:",
            summary[2],
        )

        print(
            "Without observation:",
            summary[3],
        )

        print()
        print(
            "OBSERVATION AGE"
        )

        age_summary = (
            connection.execute(
                """
                SELECT
                    MIN(
                        observation_age_hours
                    ),
                    AVG(
                        observation_age_hours
                    ),
                    MAX(
                        observation_age_hours
                    )
                FROM current_river_snapshot
                WHERE has_observation
                """
            ).fetchone()
        )

        print(
            "Minimum hours:",
            age_summary[0],
        )

        print(
            "Average hours:",
            age_summary[1],
        )

        print(
            "Maximum hours:",
            age_summary[2],
        )

        print()
        print(
            "STATIONS WITHOUT OBSERVATIONS"
        )

        rows = (
            connection.execute(
                """
                SELECT
                    station_id,
                    station_name
                FROM current_river_snapshot
                WHERE NOT has_observation
                ORDER BY station_id
                """
            ).fetchall()
        )

        if rows:
            for (
                station_id,
                station_name,
            ) in rows:
                print(
                    f"{station_id}: "
                    f"{station_name}"
                )
        else:
            print("None")

        print()
        print(
            "LATEST OBSERVATIONS"
        )

        rows = (
            connection.execute(
                """
                SELECT
                    station_id,
                    station_name,
                    observed_at,
                    water_level_m,
                    observation_age_hours
                FROM current_river_snapshot
                WHERE has_observation
                ORDER BY
                    observed_at DESC
                        NULLS LAST
                LIMIT 10
                """
            ).fetchall()
        )

        for row in rows:
            print(
                row
            )

    finally:
        connection.close()


if __name__ == "__main__":
    main()