from typing import Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    service: Literal["riverwatch"]
    status: Literal["ok"]