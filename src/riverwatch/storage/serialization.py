import hashlib
import json
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel


def _json_default(
    value: Any,
) -> Any:
    if isinstance(
        value,
        datetime,
    ):
        return value.isoformat()

    if isinstance(
        value,
        Enum,
    ):
        return value.value

    if isinstance(
        value,
        BaseModel,
    ):
        return value.model_dump(
            mode="json"
        )

    raise TypeError(
        "Object is not JSON serializable: "
        f"{type(value).__name__}"
    )


def serialize_json(
    value: Any,
) -> bytes:
    text = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(
            ",",
            ":",
        ),
        default=_json_default,
    )

    return (
        text + "\n"
    ).encode(
        "utf-8"
    )


def sha256_hex(
    data: bytes,
) -> str:
    return hashlib.sha256(
        data
    ).hexdigest()