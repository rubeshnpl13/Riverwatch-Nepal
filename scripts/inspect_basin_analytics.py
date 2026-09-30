from pathlib import Path

from riverwatch.analytics.catalog import (
    create_analytics_connection,
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
        print()
        print("=" * 70)
        print(
            "CURRENT NETWORK SUMMARY"
        )
        print()

        network = (
            connection.execute(
                """
                SELECT *
                FROM current_network_summary
                """
            ).fetchone()
        )

        columns = [
            item[0]
            for item in (
                connection.description
            )
        ]

        for (
            name,
            value,
        ) in zip(
            columns,
            network,
            strict=True,
        ):
            print(
                f"{name}: {value}"
            )

        print()
        print("=" * 70)
        print(
            "BASIN CURRENT SUMMARY"
        )
        print()

        rows = (
            connection.execute(
                """
                SELECT
                    basin_name,
                    total_stations,
                    stations_with_observation,
                    stations_without_observation,
                    fresh_observations,
                    stale_observations,
                    future_observations,
                    observations_without_water_level,
                    coverage_ratio,
                    freshness_ratio,
                    latest_observed_at
                FROM basin_current_summary
                ORDER BY
                    total_stations DESC,
                    basin_name
                """
            ).fetchall()
        )

        for row in rows:
            print(
                row
            )

        print()
        print(
            "BASIN TOTAL CHECK"
        )

        basin_total = (
            connection.execute(
                """
                SELECT
                    SUM(total_stations)
                FROM basin_current_summary
                """
            ).fetchone()[0]
        )

        snapshot_total = (
            connection.execute(
                """
                SELECT COUNT(*)
                FROM current_river_snapshot
                """
            ).fetchone()[0]
        )

        print(
            "Basin station total:",
            basin_total,
        )

        print(
            "Snapshot station total:",
            snapshot_total,
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()