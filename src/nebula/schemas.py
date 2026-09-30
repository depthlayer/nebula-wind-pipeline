from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)


TURBINE_SCHEMA = StructType(
    [
        StructField("timestamp", TimestampType(), False),
        StructField("turbine_id", IntegerType(), False),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("power_output", DoubleType(), True),
    ]
)
