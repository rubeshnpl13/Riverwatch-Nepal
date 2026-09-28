from typing import Any

from pydantic import BaseModel, ConfigDict


class BipadRawPage(BaseModel):
    model_config = ConfigDict(
        extra="allow",
    )

    count: int | None = None
    next: str | None = None
    previous: str | None = None
    results: list[Any]