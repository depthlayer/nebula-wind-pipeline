from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nebula.schemas import TURBINE_SCHEMA


def read_turbine_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """Read one or many turbine CSV files using an explicit schema."""

    return (
        spark.read.option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(TURBINE_SCHEMA)
        .csv(input_path)
        .withColumn("source_file", F.input_file_name())
        .withColumn(
            "source_group",
            F.regexp_extract(F.col("source_file"), r"data_group_(\d+)\.csv", 1).cast("int"),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )
