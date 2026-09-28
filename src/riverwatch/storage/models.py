from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class StoredObject(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )

    key: str = Field(
        min_length=1,
    )

    size_bytes: int = Field(
        ge=0,
    )

    content_type: str = Field(
        min_length=1,
    )

    sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$",
    )