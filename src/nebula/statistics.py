from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_daily_statistics(df: DataFrame) -> DataFrame:
    """Calculate daily output statistics and data-quality completeness by turbine."""

    return (
        df.groupBy("measurement_date", "turbine_id")
        .agg(
            F.min("power_output").alias("min_power_mw"),
            F.max("power_output").alias("max_power_mw"),
            F.avg("power_output").alias("avg_power_mw"),
            F.stddev_pop("power_output").alias("stddev_power_mw"),
            F.count("power_output").alias("measurement_count"),
            F.sum(F.col("measurement_missing").cast("int")).alias(
                "missing_measurement_count"
            ),
            F.sum(F.col("power_output_imputed").cast("int")).alias(
                "imputed_power_count"
            ),
            F.sum(F.col("data_quality_issue").cast("int")).alias(
                "data_quality_issue_count"
            ),
        )
        .withColumn("expected_measurement_count", F.lit(24))
        .withColumn(
            "completeness_pct",
            F.round(
                (
                    F.col("expected_measurement_count")
                    - F.col("missing_measurement_count")
                )
                / F.col("expected_measurement_count")
                * F.lit(100.0),
                2,
            ),
        )
    )
