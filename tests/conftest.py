from collections.abc import (
    Iterator,
)

import pytest
from pyspark.sql import SparkSession

from riverwatch.processing.spark.session import (
    create_local_spark_session,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = create_local_spark_session(
        app_name="riverwatch-tests",
    )

    yield session

    session.stop()