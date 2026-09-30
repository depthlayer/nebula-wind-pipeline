from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("Nebula Wind Pipeline")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()