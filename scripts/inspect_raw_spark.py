from pathlib import Path

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.discovery import (
    find_latest_manifest,
)
from riverwatch.processing.manifest_loader import (
    load_processing_input,
)
from riverwatch.processing.spark.reader import (
    read_raw_pages,
)
from riverwatch.processing.spark.session import (
    create_local_spark_session,
)


def inspect_endpoint(
    *,
    endpoint: BipadEndpoint,
) -> None:
    lake_root = Path(
        "data/lake"
    )

    manifest_path = (
        find_latest_manifest(
            lake_root=lake_root,
            endpoint=endpoint,
        )
    )

    processing_input = (
        load_processing_input(
            lake_root=lake_root,
            manifest_path=manifest_path,
        )
    )

    spark = create_local_spark_session(
        app_name=(
            "riverwatch-raw-inspection"
        )
    )

    try:
        dataframe = read_raw_pages(
            spark=spark,
            processing_input=(
                processing_input
            ),
        )

        print()
        print(
            "=" * 70
        )

        print(
            "Endpoint:",
            endpoint.value,
        )

        print(
            "Run:",
            processing_input.run_id,
        )

        print(
            "Raw page rows:",
            dataframe.count(),
        )

        dataframe.printSchema()

        dataframe.select(
            "count",
            "next",
            "previous",
        ).show(
            truncate=False
        )

        dataframe.selectExpr(
            "size(results) AS record_count"
        ).show()

    finally:
        spark.stop()


def main() -> None:
    inspect_endpoint(
        endpoint=(
            BipadEndpoint.RIVER_STATIONS
        )
    )

    inspect_endpoint(
        endpoint=BipadEndpoint.RIVER
    )


if __name__ == "__main__":
    main()