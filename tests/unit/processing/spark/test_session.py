from pyspark.sql import SparkSession


def test_creates_local_spark_session(
    spark: SparkSession,
) -> None:
    assert (
        spark.sparkContext.appName
        == "riverwatch-tests"
    )

    assert (
        spark.conf.get(
            "spark.sql.session.timeZone"
        )
        == "UTC"
    )

    assert (
        spark.conf.get(
            "spark.sql.shuffle.partitions"
        )
        == "4"
    )


def test_spark_can_process_dataframe(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("44", 1.2),
            ("282", 2.4),
            ("100", 3.6),
        ],
        [
            "station_id",
            "water_level_m",
        ],
    )

    result = (
        dataframe
        .filter(
            dataframe.water_level_m
            > 2.0
        )
        .orderBy(
            "station_id"
        )
        .collect()
    )

    assert len(result) == 2

    assert {
        row["station_id"]
        for row in result
    } == {
        "100",
        "282",
    }