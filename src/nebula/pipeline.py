from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from nebula.anomalies import identify_daily_anomalies
from nebula.cleaning import clean_measurements
from nebula.config import PipelineConfig
from nebula.ingestion import read_turbine_csv
from nebula.spark import build_spark
from nebula.statistics import calculate_daily_statistics
from nebula.storage import upsert_delta


def run_pipeline(config: PipelineConfig) -> None:
    spark = build_spark()
    spark.conf.set("spark.sql.session.timeZone", config.timezone)

    try:
        raw = read_turbine_csv(spark, config.input_path)

        # Bronze is lightly normalized raw input. Drop ingestion time from the merge
        # key so reruns update rather than duplicate the same sensor measurement.
        upsert_delta(
            raw,
            config.bronze_path,
            keys=["turbine_id", "timestamp"],
            partition_columns=None,
        )

        cleaned = clean_measurements(
            raw,
            expected_start=config.expected_start,
            expected_end=config.expected_end,
            iqr_multiplier=config.iqr_multiplier,
        ).cache()

        daily_stats = calculate_daily_statistics(cleaned).cache()
        anomaly_results = identify_daily_anomalies(daily_stats)

        upsert_delta(
            cleaned,
            config.silver_path,
            keys=["turbine_id", "timestamp"],
            partition_columns=["measurement_date"],
        )
        upsert_delta(
            daily_stats,
            config.statistics_path,
            keys=["turbine_id", "measurement_date"],
            partition_columns=["measurement_date"],
        )
        upsert_delta(
            anomaly_results,
            config.anomalies_path,
            keys=["turbine_id", "measurement_date"],
            partition_columns=["measurement_date"],
        )

        anomaly_count = anomaly_results.filter(F.col("is_anomaly")).count()
        print(
            f"Nebula pipeline complete:\n"
            f"  Cleaned measurements: {cleaned_count}\n"
            f"  Turbine-day summaries: {summary_count}\n"
            f"  Anomalies detected: {anomaly_count}"
        )
    finally:
        spark.stop()


def _parse_datetime(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the Nebula Wind Pipeline")
    parser.add_argument(
        "--input",
        default="data/raw/data_group_*.csv",
        help="Input CSV glob or directory",
    )
    parser.add_argument("--output", default="output", help="Delta output root")
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument(
        "--expected-start",
        help="Optional ISO timestamp for the expected processing-window start",
    )
    parser.add_argument(
        "--expected-end",
        help="Optional ISO timestamp for the expected processing-window end",
    )
    parser.add_argument("--iqr-multiplier", type=float, default=1.5)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_pipeline(
        PipelineConfig(
            input_path=args.input,
            output_path=args.output,
            timezone=args.timezone,
            expected_start=_parse_datetime(args.expected_start),
            expected_end=_parse_datetime(args.expected_end),
            iqr_multiplier=args.iqr_multiplier,
        )
    )


if __name__ == "__main__":
    main()
