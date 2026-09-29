from riverwatch.processing.spark.session import (
    create_local_spark_session,
)


def main() -> None:
    spark = create_local_spark_session(
        app_name="riverwatch-smoke-test",
    )

    try:
        print(
            "Spark version:",
            spark.version,
        )

        print(
            "Spark master:",
            spark.sparkContext.master,
        )

        print(
            "Spark application:",
            spark.sparkContext.appName,
        )

        print(
            "Spark timezone:",
            spark.conf.get(
                "spark.sql.session.timeZone"
            ),
        )

        dataframe = spark.createDataFrame(
            [
                (
                    "44",
                    "Kali Gandaki",
                    1.2,
                ),
                (
                    "282",
                    "Banara River",
                    2.4,
                ),
            ],
            [
                "station_id",
                "river_name",
                "water_level_m",
            ],
        )

        dataframe.printSchema()

        dataframe.show(
            truncate=False
        )

        print(
            "Row count:",
            dataframe.count(),
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()