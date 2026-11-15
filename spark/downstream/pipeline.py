"""Handoff scaffold for feature engineering / delay modeling (NOT implemented).

Read the cleaned Parquet datasets from HDFS. Implement build_features before
running this script. This file is not called by bootstrap or batch cleaning.
"""

import os
from pyspark.sql import SparkSession

HDFS = os.getenv("HDFS_BASE", "hdfs://namenode:8020").rstrip("/")


def read_cleaned(spark: SparkSession) -> dict:
    datasets = (
        "gps_observations", "weather_observations", "air_quality_observations",
        "gtfs_routes", "gtfs_stops", "gtfs_trips", "gtfs_stop_times",
        "gtfs_calendar", "gtfs_calendar_dates",
    )
    # calendar.txt and calendar_dates.txt are optional in some GTFS feeds.
    required = [name for name in datasets if name not in {"gtfs_calendar", "gtfs_calendar_dates"}]
    return {name: spark.read.parquet(f"{HDFS}/data/cleaned/{name}") for name in required}


def build_features(cleaned: dict):
    """TODO: link positions to scheduled stop visits + environmental features."""
    raise NotImplementedError(
        "Downstream spatiotemporal joins, feature engineering, and model training "
        "are assigned to the next developer; see spark/downstream/README.md."
    )


def main():
    spark = (SparkSession.builder.appName("big-data-ztm-downstream-scaffold")
             .master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.hadoop.fs.defaultFS", HDFS)
             .getOrCreate())
    try:
        build_features(read_cleaned(spark))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
