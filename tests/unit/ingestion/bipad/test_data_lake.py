import json
import logging
from pathlib import Path

import httpx
import respx

from riverwatch.ingestion.bipad.client import (
    BipadClient,
)
from riverwatch.ingestion.bipad.historical_service import (
    HistoricalRiverBackfillService,
)
from riverwatch.ingestion.bipad.manifest_writer import (
    BipadRunManifestWriter,
)
from riverwatch.ingestion.bipad.quarantine_writer import (
    BipadQuarantineWriter,
)
from riverwatch.ingestion.bipad.raw_writer import (
    BipadRawPageWriter,
)
from riverwatch.ingestion.models import (
    HistoricalRiverIngestionBatch,
)
from riverwatch.storage.local import (
    LocalObjectStore,
)

BASE_URL = "https://bipadportal.gov.np"

FIXTURE_PATH = Path(
    "tests/fixtures/bipad/river_record.json"
)


def load_river_fixture() -> dict[str, object]:
    return json.loads(
        FIXTURE_PATH.read_text(
            encoding="utf-8",
        )
    )


def create_client() -> BipadClient:
    return BipadClient(
        base_url=BASE_URL,
        api_version="v1",
        timeout_seconds=10,
        max_retries=0,
        sleep=lambda _: None,
    )


@respx.mock
def test_historical_workflow_persists_complete_data_lake(
    tmp_path: Path,
) -> None:
    url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
    )

    second_url = (
        "https://bipadportal.gov.np/"
        "api/v1/river/"
        "?limit=2&offset=2"
    )

    first = load_river_fixture()

    second = load_river_fixture()
    second["id"] = 20000001

    invalid = load_river_fixture()
    invalid["id"] = 20000002
    invalid.pop(
        "waterLevelOn"
    )

    fourth = load_river_fixture()
    fourth["id"] = 20000003

    def responder(
        request: httpx.Request,
    ) -> httpx.Response:
        offset = request.url.params.get(
            "offset"
        )

        if offset in {
            None,
            "0",
        }:
            return httpx.Response(
                200,
                json={
                    "next": second_url,
                    "results": [
                        first,
                        second,
                    ],
                },
                request=request,
            )

        if offset == "2":
            return httpx.Response(
                200,
                json={
                    "next": None,
                    "results": [
                        invalid,
                        fourth,
                    ],
                },
                request=request,
            )

        raise AssertionError(
            f"Unexpected URL: {request.url}"
        )

    respx.get(
        url
    ).mock(
        side_effect=responder
    )

    store = LocalObjectStore(
        root=tmp_path
    )

    raw_writer = BipadRawPageWriter(
        store=store
    )

    quarantine_writer = (
        BipadQuarantineWriter(
            store=store
        )
    )

    manifest_writer = (
        BipadRunManifestWriter(
            store=store
        )
    )

    batches: list[
        HistoricalRiverIngestionBatch
    ] = []

    logger = logging.getLogger(
        "riverwatch.integration.lake"
    )

    with create_client() as client:
        service = (
            HistoricalRiverBackfillService(
                client=client,
                logger=logger,
                batch_handler=batches.append,
                raw_writer=raw_writer,
                quarantine_writer=(
                    quarantine_writer
                ),
                manifest_writer=(
                    manifest_writer
                ),
            )
        )

        result = service.run(
            page_limit=2,
            page_size=2,
        )

    assert (
        result.metrics.records_received
        == 4
    )

    assert (
        result.metrics.records_valid
        == 3
    )

    assert (
        result.metrics.records_invalid
        == 1
    )

    assert (
        result.metrics.observations_emitted
        == 3
    )

    #
    # RAW
    #

    payload_files = sorted(
        tmp_path.rglob(
            "raw/**/payload.json"
        )
    )

    metadata_files = sorted(
        tmp_path.rglob(
            "raw/**/metadata.json"
        )
    )

    assert len(payload_files) == 2
    assert len(metadata_files) == 2

    #
    # QUARANTINE
    #

    quarantine_files = list(
        tmp_path.rglob(
            "quarantine/**/*.json"
        )
    )

    assert len(
        quarantine_files
    ) == 1

    quarantine_document = json.loads(
        quarantine_files[0].read_text(
            encoding="utf-8"
        )
    )

    # Records 0 and 1 were on page 0.
    # The invalid record is the first
    # record on page 1.
    assert (
        quarantine_document[
            "record_index"
        ]
        == 2
    )

    assert (
        quarantine_document[
            "raw_record"
        ]["id"]
        == 20000002
    )

    #
    # MANIFEST
    #

    manifest_files = list(
        tmp_path.rglob(
            "manifests/**/manifest.json"
        )
    )

    assert len(
        manifest_files
    ) == 1

    manifest = json.loads(
        manifest_files[0].read_text(
            encoding="utf-8"
        )
    )

    assert (
        manifest["run_id"]
        == result.run_id
    )

    assert (
        manifest["status"]
        == "completed"
    )

    assert (
        manifest["endpoint"]
        == "river"
    )

    assert (
        manifest["metrics"][
            "records_received"
        ]
        == 4
    )

    assert (
        manifest["metrics"][
            "records_invalid"
        ]
        == 1
    )

    assert len(
        manifest["raw_pages"]
    ) == 2

    assert len(
        manifest[
            "quarantine_objects"
        ]
    ) == 1

    #
    # LINEAGE
    #

    manifest_payload_keys = {
        page["payload_key"]
        for page in manifest[
            "raw_pages"
        ]
    }

    actual_payload_keys = {
        str(
            path.relative_to(
                tmp_path
            )
        )
        for path in payload_files
    }

    assert (
        manifest_payload_keys
        == actual_payload_keys
    )

    quarantine_key = str(
        quarantine_files[0].relative_to(
            tmp_path
        )
    )

    assert (
        manifest[
            "quarantine_objects"
        ][0]["object_key"]
        == quarantine_key
    )