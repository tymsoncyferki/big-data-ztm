"""Build the GTFS trip/service-date reference from cleaned timetable data.

For every active service date, combine a GTFS trip with its route metadata.
The result is a static reference dataset; it does not calculate delays.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession, functions as F

HDFS = os.getenv("HDFS_BASE", "hdfs://namenode:8020").rstrip("/")
LOG = logging.getLogger("ztm-enriched")


def available_versions(fs, jvm):
    root = jvm.org.apache.hadoop.fs.Path(f"{HDFS}/data/cleaned/gtfs_trips")
    if not fs.exists(root):
        return []
    return sorted(stat.getPath().getName().split("=", 1)[1]
                  for stat in fs.listStatus(root)
                  if stat.isDirectory() and stat.getPath().getName().startswith("feed_version="))


def build_schedule(spark, version):
    """Combine GTFS trips, their active service dates and route descriptions."""
    root = f"{HDFS}/data/cleaned"
    trips = spark.read.parquet(f"{root}/gtfs_trips/feed_version={version}")
    routes = spark.read.parquet(f"{root}/gtfs_routes/feed_version={version}")
    dates = spark.read.parquet(f"{root}/gtfs_calendar_dates/feed_version={version}")

    # calendar_dates defines service dates. Removed dates (exception_type=2)
    # do not generate trip-service records.
    active = (dates.filter(F.col("exception_type") == 1)
              .select("service_id", F.col("date").alias("service_date"))
              .dropDuplicates(["service_id", "service_date"]))
    routes_ref = F.broadcast(routes.select(
        "route_id", "route_short_name", "route_long_name", "route_type"
    ).dropDuplicates(["route_id"]))

    return (trips.join(active, "service_id", "inner")
            .join(routes_ref, "route_id", "left")
            .select(
                "trip_id", "service_date", "service_id", "route_id",
                "route_short_name", "route_long_name", "route_type",
                "trip_short_name", "trip_headsign", "direction_id",
                "shape_id", "exceptional", "wheelchair_accessible",
                "block_id", "block_short_name", "variant_code", "fleet_type",
            ))


def enrich_gtfs(spark, force=False):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    versions = available_versions(fs, jvm)
    if not versions:
        LOG.info("Stage 2: no cleaned GTFS feeds found; skipping reference enrichment")
    markers = jvm.org.apache.hadoop.fs.Path(f"{HDFS}/data/cleaned/_pipeline_state/enriched")
    fs.mkdirs(markers)
    for version in versions:
        marker = jvm.org.apache.hadoop.fs.Path(f"{markers}/{version}.done")
        calendar = jvm.org.apache.hadoop.fs.Path(
            f"{HDFS}/data/cleaned/gtfs_calendar_dates/feed_version={version}"
        )
        if not fs.exists(calendar):
            LOG.info("Stage 2: %s has no calendar_dates; service-date expansion skipped", version)
            continue
        destination = f"{HDFS}/data/enriched/gtfs_trip_services/feed_version={version}"
        if fs.exists(marker) and not force:
            continue
        LOG.info("Stage 2: expanding GTFS service dates, feed_version=%s", version)
        frame = build_schedule(spark, version)
        frame.write.mode("overwrite").option("compression", "snappy").parquet(destination)
        if not fs.exists(marker) and not fs.createNewFile(marker):
            raise IOError(f"Could not record enrichment completion marker: {marker}")
        LOG.info("Stage 2: written %s", destination)


def main():
    spark = (SparkSession.builder.appName("big-data-ztm-enrichment")
             .master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.hadoop.fs.defaultFS", HDFS)
             .getOrCreate())
    try:
        enrich_gtfs(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
