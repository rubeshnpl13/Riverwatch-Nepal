from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from riverwatch.config import get_settings

ENDPOINTS = {
    "river": "river",
    "river-stations": "river-stations",
    "flood-station": "flood-station",
    "streamflow": "streamflow",
}

OUTPUT_DIR = Path("data/inspection")


def build_endpoint_url(endpoint: str) -> str:
    settings = get_settings()

    base_url = settings.bipad_base_url.rstrip("/")

    return (
        f"{base_url}/api/"
        f"{settings.bipad_api_version}/"
        f"{ENDPOINTS[endpoint]}/"
    )


def fetch_json(endpoint: str) -> tuple[Any, httpx.Response]:
    settings = get_settings()
    url = build_endpoint_url(endpoint)

    print(f"\nRequesting: {url}")

    with httpx.Client(
        timeout=settings.http_timeout_seconds,
        follow_redirects=True,
        headers={
            "Accept": "application/json",
            "User-Agent": "RiverWatch-Nepal/0.1",
        },
    ) as client:
        response = client.get(url)

    print(f"HTTP status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('content-type')}")

    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            "The API returned a successful response, "
            "but the body was not valid JSON."
        ) from exc

    return payload, response


def describe_value(
    value: Any,
    path: str = "$",
    depth: int = 0,
    max_depth: int = 4,
) -> None:
    indent = "    " * depth

    if depth > max_depth:
        print(f"{indent}{path}: ...")
        return

    if isinstance(value, dict):
        print(f"{indent}{path}: object ({len(value)} keys)")

        for key, child in value.items():
            describe_value(
                child,
                path=key,
                depth=depth + 1,
                max_depth=max_depth,
            )

    elif isinstance(value, list):
        print(f"{indent}{path}: array ({len(value)} items)")

        if value:
            describe_value(
                value[0],
                path="[0]",
                depth=depth + 1,
                max_depth=max_depth,
            )

    else:
        value_type = type(value).__name__

        preview = repr(value)

        if len(preview) > 100:
            preview = preview[:97] + "..."

        print(
            f"{indent}{path}: "
            f"{value_type} = {preview}"
        )


def find_example_record(payload: Any) -> Any | None:
    if isinstance(payload, list):
        return payload[0] if payload else None

    if not isinstance(payload, dict):
        return None

    likely_collection_keys = (
        "results",
        "data",
        "items",
        "records",
    )

    for key in likely_collection_keys:
        if key not in payload:
            continue

        value = payload[key]

        if isinstance(value, list):
            return value[0] if value else None

    for value in payload.values():
        if isinstance(value, list):
            return value[0] if value else None

    return payload


def save_response(
    endpoint: str,
    payload: Any,
    response: httpx.Response,
) -> Path:
    timestamp = datetime.now(UTC)

    run_directory = (
        OUTPUT_DIR
        / endpoint
        / timestamp.strftime("%Y-%m-%d")
        / timestamp.strftime("%H%M%S")
    )

    run_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload_path = run_directory / "response.json"
    metadata_path = run_directory / "metadata.json"

    payload_path.write_text(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    metadata = {
        "endpoint": endpoint,
        "url": str(response.url),
        "http_status": response.status_code,
        "content_type": response.headers.get("content-type"),
        "retrieved_at": timestamp.isoformat(),
    }

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
        ),
        encoding="utf-8",
    )

    return run_directory


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect live BIPAD API responses."
    )

    parser.add_argument(
        "endpoint",
        choices=ENDPOINTS.keys(),
        help="BIPAD endpoint to inspect.",
    )

    args = parser.parse_args()

    try:
        payload, response = fetch_json(args.endpoint)

    except httpx.TimeoutException as exc:
        print("\nERROR: Request timed out.")
        raise SystemExit(1) from exc

    except httpx.HTTPStatusError as exc:
        print(
            "\nERROR: BIPAD returned HTTP "
            f"{exc.response.status_code}."
        )
        raise SystemExit(1) from exc

    except httpx.RequestError as exc:
        print(f"\nERROR: Could not reach BIPAD: {exc}")
        raise SystemExit(1) from exc

    print("\n--- RESPONSE STRUCTURE ---\n")

    describe_value(payload)

    example = find_example_record(payload)

    print("\n--- EXAMPLE RECORD ---\n")

    if example is None:
        print("No example record found.")
    else:
        print(
            json.dumps(
                example,
                indent=2,
                ensure_ascii=False,
            )
        )

    output_directory = save_response(
        args.endpoint,
        payload,
        response,
    )

    print(
        f"\nSaved inspection data to: "
        f"{output_directory}"
    )


if __name__ == "__main__":
    main()