from pathlib import Path

from riverwatch.ingestion.bipad.endpoints import (
    BipadEndpoint,
)
from riverwatch.processing.discovery import (
    find_latest_manifest,
)
from riverwatch.processing.spark.recovery import (
    process_manifest_retry_safe,
)
from riverwatch.processing.spark.session import (
    create_local_spark_session,
)


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    spark = create_local_spark_session(
        app_name="riverwatch-processing"
    )

    try:
        for endpoint in (
            BipadEndpoint.RIVER_STATIONS,
            BipadEndpoint.RIVER,
        ):
            manifest_path = (
                find_latest_manifest(
                    lake_root=lake_root,
                    endpoint=endpoint,
                )
            )

            result = process_manifest_retry_safe(
                spark=spark,
                lake_root=lake_root,
                manifest_path=manifest_path,
            )

            print()
            print(
                "Endpoint:",
                result.endpoint.value,
            )

            print(
                "Run:",
                result.run_id,
            )

            for output in result.outputs:
                print(
                    f"{output.dataset.value}:",
                    output.row_count,
                )

                print(
                    "Output:",
                    output.path,
                )
            print(
                "Quality report:",
                result.quality_report_path,
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()