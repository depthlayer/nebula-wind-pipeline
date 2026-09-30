from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from nebula.cleaning import clean_measurements, complete_hourly_grid


SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), False),
        StructField("turbine_id", IntegerType(), False),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("power_output", DoubleType(), True),
    ]
)


def test_missing_hour_is_created(spark):
    rows = [
        (datetime(2022, 3, 1, 0), 1, 10.0, 90, 2.0),
        (datetime(2022, 3, 1, 2), 1, 12.0, 100, 2.4),
    ]
    df = spark.createDataFrame(rows, SCHEMA)

    result = complete_hourly_grid(df).orderBy("timestamp").collect()

    assert len(result) == 3
    assert result[1].timestamp == datetime(2022, 3, 1, 1)
    assert result[1].measurement_missing is True


def test_invalid_and_sensor_outlier_values_are_imputed(spark):
    rows = [
        (datetime(2022, 3, 1, 0), 1, 10.0, 350, 2.0),
        (datetime(2022, 3, 1, 1), 1, 11.0, 355, 2.1),
        (datetime(2022, 3, 1, 2), 1, 12.0, 0, 2.2),
        (datetime(2022, 3, 1, 3), 1, 13.0, 5, 2.3),
        (datetime(2022, 3, 1, 4), 1, 1000.0, 10, 99.0),
        (datetime(2022, 3, 1, 5), 1, -1.0, 400, -5.0),
    ]
    df = spark.createDataFrame(rows, SCHEMA)

    result = clean_measurements(df).orderBy("timestamp").collect()
    outlier_row = result[4]
    invalid_row = result[5]

    assert outlier_row.wind_speed_outlier is True
    assert outlier_row.power_output_outlier is True
    assert outlier_row.wind_speed != 1000.0
    assert outlier_row.power_output != 99.0

    assert invalid_row.invalid_wind_speed is True
    assert invalid_row.invalid_wind_direction is True
    assert invalid_row.invalid_power_output is True
    assert invalid_row.wind_speed is not None
    assert 0 <= invalid_row.wind_direction <= 359
    assert invalid_row.power_output is not None
