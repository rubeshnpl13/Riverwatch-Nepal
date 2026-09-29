from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from riverwatch.processing.models import (
    ProcessingInputRun,
)
from riverwatch.processing.spark.schemas import (
    schema_for_endpoint,
)


def read_raw_pages(
    *,
    spark: SparkSession,
    processing_input: ProcessingInputRun,
) -> DataFrame:
    if not processing_input.pages:
        raise ValueError(
            "Processing input contains no raw pages"
        )

    paths = [
        str(
            page.payload_path
        )
        for page in processing_input.pages
    ]

    schema = schema_for_endpoint(
        processing_input.endpoint
    )

    return (
        spark.read
        .schema(schema)
        .json(paths)
    )