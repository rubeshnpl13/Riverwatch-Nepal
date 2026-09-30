from pydantic import (
    BaseModel,
    ConfigDict,
)


class APIResponseModel(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        frozen=True,
    )