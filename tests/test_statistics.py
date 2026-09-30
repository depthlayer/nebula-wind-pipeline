from datetime import date, datetime

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from nebula.statistics import calculate_daily_statistics


SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), False),
        StructField("measurement_date", DateType(), False),
        StructField("turbine_id", IntegerType(), False),
        StructField("power_output", DoubleType(), True),
        StructField("measurement_missing", BooleanType(), False),
        StructField("power_output_imputed", BooleanType(), False),
        StructField("data_quality_issue", BooleanType(), False),
    ]
)


def test_daily_statistics(spark):
    rows = [
        (datetime(2022, 3, 1, 0), date(2022, 3, 1), 1, 1.0, False, False, False),
        (datetime(2022, 3, 1, 1), date(2022, 3, 1), 1, 2.0, False, False, False),
        (datetime(2022, 3, 1, 2), date(2022, 3, 1), 1, 3.0, True, True, True),
    ]
    df = spark.createDataFrame(rows, SCHEMA)

    result = calculate_daily_statistics(df).first()

    assert result.min_power_mw == 1.0
    assert result.max_power_mw == 3.0
    assert result.avg_power_mw == 2.0
    assert result.measurement_count == 3
    assert result.missing_measurement_count == 1
    assert result.imputed_power_count == 1
    assert result.data_quality_issue_count == 1
