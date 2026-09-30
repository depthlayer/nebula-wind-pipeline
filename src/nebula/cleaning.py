from __future__ import annotations

import math
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PI = math.pi


def _timestamp_literal(value: datetime | None, fallback_column: str) -> F.Column:
    if value is None:
        return F.col(fallback_column)
    return F.lit(value).cast("timestamp")


def complete_hourly_grid(
    df: DataFrame,
    expected_start: datetime | None = None,
    expected_end: datetime | None = None,
) -> DataFrame:
    """Insert rows for missing hourly measurements for every observed turbine.

    If explicit boundaries are not provided, the observed min/max timestamps define
    the expected window. In a scheduled production job, pass the exact processing
    window so a sensor outage at the beginning or end of a day can also be detected.
    """

    spark = df.sparkSession
    bounds = df.agg(
        F.min("timestamp").alias("observed_start"),
        F.max("timestamp").alias("observed_end"),
    )

    window = bounds.select(
        _timestamp_literal(expected_start, "observed_start").alias("start_ts"),
        _timestamp_literal(expected_end, "observed_end").alias("end_ts"),
    ).first()

    if window.start_ts is None or window.end_ts is None:
        return df.withColumn("measurement_missing", F.lit(False))

    hours = spark.range(1).select(
        F.explode(
            F.sequence(
                F.lit(window.start_ts).cast("timestamp"),
                F.lit(window.end_ts).cast("timestamp"),
                F.expr("INTERVAL 1 HOUR"),
            )
        ).alias("timestamp")
    )

    turbines = df.select("turbine_id").where(F.col("turbine_id").isNotNull()).distinct()
    expected = turbines.crossJoin(hours)

    observed = (
        df.dropDuplicates(["turbine_id", "timestamp"])
        .withColumn("_observed_row", F.lit(True))
    )

    return (
        expected.join(observed, ["turbine_id", "timestamp"], "left")
        .withColumn("measurement_missing", F.col("_observed_row").isNull())
        .drop("_observed_row")
    )


def _apply_physical_validation(df: DataFrame) -> DataFrame:
    """Null values that are physically invalid while preserving quality flags."""

    return (
        df.withColumn(
            "invalid_wind_speed",
            F.col("wind_speed").isNotNull() & (F.col("wind_speed") < 0),
        )
        .withColumn(
            "invalid_wind_direction",
            F.col("wind_direction").isNotNull()
            & ~F.col("wind_direction").between(0, 359),
        )
        .withColumn(
            "invalid_power_output",
            F.col("power_output").isNotNull() & (F.col("power_output") < 0),
        )
        .withColumn(
            "wind_speed",
            F.when(~F.col("invalid_wind_speed"), F.col("wind_speed")),
        )
        .withColumn(
            "wind_direction",
            F.when(~F.col("invalid_wind_direction"), F.col("wind_direction")),
        )
        .withColumn(
            "power_output",
            F.when(~F.col("invalid_power_output"), F.col("power_output")),
        )
    )


def _per_turbine_cleaning_stats(df: DataFrame) -> DataFrame:
    """Calculate robust imputation and IQR statistics per turbine."""

    direction_radians = F.col("wind_direction") * F.lit(PI) / F.lit(180.0)

    return (
        df.groupBy("turbine_id")
        .agg(
            F.percentile_approx(
                "wind_speed", F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75)), 10000
            ).alias("wind_speed_q"),
            F.percentile_approx(
                "power_output", F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75)), 10000
            ).alias("power_output_q"),
            F.avg(F.sin(direction_radians)).alias("direction_sin_mean"),
            F.avg(F.cos(direction_radians)).alias("direction_cos_mean"),
        )
        .select(
            "turbine_id",
            F.col("wind_speed_q")[0].alias("wind_speed_q1"),
            F.col("wind_speed_q")[1].alias("wind_speed_median"),
            F.col("wind_speed_q")[2].alias("wind_speed_q3"),
            F.col("power_output_q")[0].alias("power_output_q1"),
            F.col("power_output_q")[1].alias("power_output_median"),
            F.col("power_output_q")[2].alias("power_output_q3"),
            "direction_sin_mean",
            "direction_cos_mean",
        )
        .withColumn(
            "direction_mean_rad",
            F.atan2(F.col("direction_sin_mean"), F.col("direction_cos_mean")),
        )
        .withColumn(
            "direction_mean_rad",
            F.when(
                F.col("direction_mean_rad") < 0,
                F.col("direction_mean_rad") + F.lit(2.0 * PI),
            ).otherwise(F.col("direction_mean_rad")),
        )
        .withColumn(
            "wind_direction_circular_mean",
            F.col("direction_mean_rad") * F.lit(180.0) / F.lit(PI),
        )
    )


def clean_measurements(
    df: DataFrame,
    expected_start: datetime | None = None,
    expected_end: datetime | None = None,
    iqr_multiplier: float = 1.5,
) -> DataFrame:
    """Clean and impute turbine telemetry.

    Data-quality outliers are measurement-level IQR outliers for wind speed and
    power output. They are distinct from the daily operational anomalies detected
    later with the requested fleet mean ±2 standard deviations rule.
    """

    complete = complete_hourly_grid(df, expected_start, expected_end)
    validated = _apply_physical_validation(complete)
    stats = _per_turbine_cleaning_stats(validated)
    enriched = validated.join(stats, "turbine_id", "left")

    wind_iqr = F.col("wind_speed_q3") - F.col("wind_speed_q1")
    power_iqr = F.col("power_output_q3") - F.col("power_output_q1")

    wind_outlier = F.col("wind_speed").isNotNull() & (
        (F.col("wind_speed") < F.col("wind_speed_q1") - F.lit(iqr_multiplier) * wind_iqr)
        | (F.col("wind_speed") > F.col("wind_speed_q3") + F.lit(iqr_multiplier) * wind_iqr)
    )
    power_outlier = F.col("power_output").isNotNull() & (
        (
            F.col("power_output")
            < F.col("power_output_q1") - F.lit(iqr_multiplier) * power_iqr
        )
        | (
            F.col("power_output")
            > F.col("power_output_q3") + F.lit(iqr_multiplier) * power_iqr
        )
    )

    cleaned = (
        enriched.withColumn("wind_speed_outlier", F.coalesce(wind_outlier, F.lit(False)))
        .withColumn(
            "power_output_outlier", F.coalesce(power_outlier, F.lit(False))
        )
        .withColumn(
            "wind_speed_imputed",
            F.col("wind_speed").isNull() | F.col("wind_speed_outlier"),
        )
        .withColumn(
            "wind_direction_imputed", F.col("wind_direction").isNull()
        )
        .withColumn(
            "power_output_imputed",
            F.col("power_output").isNull() | F.col("power_output_outlier"),
        )
        .withColumn(
            "wind_speed",
            F.when(
                F.col("wind_speed_imputed"), F.col("wind_speed_median")
            ).otherwise(F.col("wind_speed")),
        )
        .withColumn(
            "power_output",
            F.when(
                F.col("power_output_imputed"), F.col("power_output_median")
            ).otherwise(F.col("power_output")),
        )
        .withColumn(
            "wind_direction",
            F.when(
                F.col("wind_direction_imputed"),
                F.pmod(
                    F.round(F.col("wind_direction_circular_mean")),
                    F.lit(360),
                ).cast("int"),
            ).otherwise(F.col("wind_direction")),
        )
        .withColumn(
            "wind_direction_rad",
            F.col("wind_direction") * F.lit(PI) / F.lit(180.0),
        )
        .withColumn("measurement_date", F.to_date("timestamp"))
        .withColumn(
            "data_quality_issue",
            F.col("measurement_missing")
            | F.col("invalid_wind_speed")
            | F.col("invalid_wind_direction")
            | F.col("invalid_power_output")
            | F.col("wind_speed_outlier")
            | F.col("power_output_outlier")
            | F.col("wind_speed_imputed")
            | F.col("wind_direction_imputed")
            | F.col("power_output_imputed"),
        )
    )

    helper_columns = [
        "wind_speed_q1",
        "wind_speed_median",
        "wind_speed_q3",
        "power_output_q1",
        "power_output_median",
        "power_output_q3",
        "direction_sin_mean",
        "direction_cos_mean",
        "direction_mean_rad",
        "wind_direction_circular_mean",
    ]

    return cleaned.drop(*helper_columns)
