from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def upsert_delta(
    df: DataFrame,
    path: str,
    keys: list[str],
    partition_columns: list[str] | None = None,
) -> None:
    """Idempotently upsert a DataFrame into a Delta table at ``path``."""

    spark = df.sparkSession

    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.save(path)
        return

    target = DeltaTable.forPath(spark, path)
    condition = " AND ".join([f"target.`{key}` = source.`{key}`" for key in keys])

    (
        target.alias("target")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
