from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def identify_daily_anomalies(daily_stats: DataFrame) -> DataFrame:
    """Flag turbine daily averages outside fleet daily mean ± 2 population stddevs."""

    fleet_window = Window.partitionBy("measurement_date")

    return (
        daily_stats.withColumn(
            "fleet_mean_power_mw", F.avg("avg_power_mw").over(fleet_window)
        )
        .withColumn(
            "fleet_stddev_power_mw",
            F.stddev_pop("avg_power_mw").over(fleet_window),
        )
        .withColumn(
            "lower_anomaly_bound_mw",
            F.col("fleet_mean_power_mw")
            - F.lit(2.0) * F.col("fleet_stddev_power_mw"),
        )
        .withColumn(
            "upper_anomaly_bound_mw",
            F.col("fleet_mean_power_mw")
            + F.lit(2.0) * F.col("fleet_stddev_power_mw"),
        )
        .withColumn(
            "is_anomaly",
            F.when(F.col("fleet_stddev_power_mw").isNull(), F.lit(False))
            .when(F.col("fleet_stddev_power_mw") == 0, F.lit(False))
            .otherwise(
                (F.col("avg_power_mw") < F.col("lower_anomaly_bound_mw"))
                | (F.col("avg_power_mw") > F.col("upper_anomaly_bound_mw"))
            ),
        )
        .withColumn(
            "anomaly_direction",
            F.when(~F.col("is_anomaly"), F.lit(None).cast("string"))
            .when(F.col("avg_power_mw") < F.col("lower_anomaly_bound_mw"), "LOW")
            .otherwise("HIGH"),
        )
    )
