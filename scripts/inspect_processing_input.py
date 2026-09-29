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


def main() -> None:
    lake_root = Path(
        "data/lake"
    )

    for endpoint in (
        BipadEndpoint.RIVER_STATIONS,
        BipadEndpoint.RIVER,
    ):
        print()
        print(
            "Endpoint:",
            endpoint.value,
        )

        manifest = find_latest_manifest(
            lake_root=lake_root,
            endpoint=endpoint,
        )

        print(
            "Manifest:",
            manifest,
        )

        processing_input = (
            load_processing_input(
                lake_root=lake_root,
                manifest_path=manifest,
            )
        )

        print(
            "Run ID:",
            processing_input.run_id,
        )

        print(
            "Pages:",
            len(
                processing_input.pages
            ),
        )

        for page in (
            processing_input.pages
        ):
            print(
                " ",
                f"page={page.page_index}",
                f"records={page.record_count}",
                f"path={page.payload_path}",
            )


if __name__ == "__main__":
    main()