"""Daily timetable lookup for Spark Structured Streaming.

WHAT IT BUILDS
--------------
/data/enriched/schedule_stops/service_date=YYYY-MM-DD/   (Snappy Parquet)

One row per (service date, trip, stop): where the trip stops and when it should be
there. A streaming job loads the partitions it needs once (for example at start-up and
after midnight) and joins live GPS pings to it on `trip_id`, with the same delay
definition as the batch job:

    scheduled_epoch = sched_epoch                  (already includes the service-day start)
    delay_s         = ping_epoch - scheduled_epoch (positive = late)

Columns: feed_version, trip_id, route_id, route_short_name, route_type, direction_id,
trip_headsign, stop_sequence, stop_id, stop_lat, stop_lon, cell_x, cell_y,
sched_seconds, sched_epoch, trip_last_sequence; partition column: service_date.
`cell_x`/`cell_y` are the same grid cells as in delay_estimation (grid_x/grid_y).

WHEN IT IS BUILT
----------------
Once per service date AND feed version: a date is rebuilt only when it has no partition
yet or the stored `feed_version` differs from the feed that now covers that date
(pick_feed_versions, the same rule the ML build uses). A new GTFS feed (downloaded
daily at 03:00 Warsaw time) therefore refreshes the table without any extra schedule.
Dates built: yesterday, today and tomorrow (Warsaw), because GTFS times may exceed
24:00:00 - at 01:00 on the 17th the vehicles still run the trips of the 16th.

INPUT
-----
/data/enriched/gtfs_trip_services (Stage 2: which trip runs on which date, route
description) and cleaned gtfs_stop_times / gtfs_stops of the same feed version.

Run by hand (all three dates, or one):
  spark-submit spark/ml/schedule_stops.py [--date 2026-11-16] [--force]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date, timedelta

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from delay_estimation import build_trip_stops, pick_feed_versions, read_feed_coverage
from ml_common import (
    HDFS,
    WARSAW_TZ,
    BatchLock,
    build_spark,
    exists,
    grid_x,
    grid_y,
    service_day_start_epoch,
)

LOG = logging.getLogger("ztm-ml")

OUTPUT = "/data/enriched/schedule_stops"
TRIP_SERVICES = "/data/enriched/gtfs_trip_services"

COLUMNS = [
    "feed_version", "service_date", "trip_id", "route_id", "route_short_name",
    "route_type", "direction_id", "trip_headsign", "stop_sequence", "stop_id",
    "stop_lat", "stop_lon", "cell_x", "cell_y", "sched_seconds", "sched_epoch",
    "trip_last_sequence",
]


# --------------------------------------------------------------------------- #
def warsaw_today(spark: SparkSession) -> str:
    """Today's date in Warsaw as 'YYYY-MM-DD' (the session time zone is UTC)."""
    row = spark.sql(
        f"SELECT CAST(from_utc_timestamp(current_timestamp(), '{WARSAW_TZ}') AS DATE) AS d"
    ).first()
    return row["d"].isoformat()


def schedule_dates(today: str, behind: int = 1, ahead: int = 1) -> list[str]:
    """today-behind .. today+ahead as ISO dates."""
    d = date.fromisoformat(today)
    return [(d + timedelta(days=i)).isoformat() for i in range(-behind, ahead + 1)]


def stored_feed_version(spark: SparkSession, base: str, day: str) -> str | None:
    """feed_version already stored for `day`, or None (missing, empty or mixed)."""
    path = f"{base}{OUTPUT}/service_date={day}"
    if not exists(spark, path):
        return None
    try:
        rows = spark.read.parquet(path).select("feed_version").distinct().collect()
    except Exception as exc:  # half-written folder: rebuild it
        LOG.warning("Cannot read %s (%s); it will be rebuilt", path, str(exc).splitlines()[0])
        return None
    return rows[0]["feed_version"] if len(rows) == 1 else None


def _feed_table(spark: SparkSession, base: str, table: str, version: str) -> DataFrame:
    return spark.read.parquet(f"{base}/data/cleaned/{table}").filter(
        F.col("feed_version") == version
    )


def build_day(spark: SparkSession, base: str, day: str, version: str) -> DataFrame:
    """The schedule rows of one service date, from one feed version (lazy DataFrame)."""
    services = (
        spark.read.parquet(f"{base}{TRIP_SERVICES}")
        .filter(
            (F.col("feed_version") == version)
            & (F.col("service_date") == F.lit(day).cast("date"))
        )
        .select(
            "feed_version", "trip_id", "service_date", "route_id", "route_short_name",
            "route_type", "direction_id", "trip_headsign",
        )
        .dropDuplicates(["feed_version", "trip_id", "service_date"])
    )
    # Only the stop times of trips that run on this date (same idea as in build()).
    trips_today = F.broadcast(services.select("feed_version", "trip_id").distinct())
    stop_times = _feed_table(spark, base, "gtfs_stop_times", version).join(
        trips_today, ["feed_version", "trip_id"], "left_semi"
    )
    stops = _feed_table(spark, base, "gtfs_stops", version)
    trip_stops = build_trip_stops(stop_times, stops).withColumn(
        "trip_last_sequence",
        F.max("stop_sequence").over(Window.partitionBy("feed_version", "trip_id")),
    )
    return (
        services.join(trip_stops, ["feed_version", "trip_id"], "inner")
        .withColumn("sched_seconds", F.col("sched_seconds").cast("int"))
        .withColumn(
            "sched_epoch",
            service_day_start_epoch(F.col("service_date")) + F.col("sched_seconds"),
        )
        .withColumn("cell_x", grid_x(F.col("stop_lon")))
        .withColumn("cell_y", grid_y(F.col("stop_lat")))
        .select(*COLUMNS)
    )


def build_schedule_stops(
    spark: SparkSession, base: str, days: list[str], force: bool = False
) -> dict[str, dict]:
    """Build (or refresh) the partitions for `days`. Returns a status per day.

    status: built | unchanged | no-feed | empty | no-trip-services
    `spark` must be a session on which configure_session() was called (dynamic partition
    overwrite); otherwise overwriting one date would delete every other date.
    """
    base = base.rstrip("/")
    if spark.conf.get("spark.sql.sources.partitionOverwriteMode", "static") != "dynamic":
        raise RuntimeError(
            "partitionOverwriteMode is not 'dynamic': call configure_session(spark.newSession())"
        )
    if not exists(spark, f"{base}{TRIP_SERVICES}"):
        LOG.warning("No %s yet (Stage 2 has not run); schedule_stops not built", TRIP_SERVICES)
        return {d: {"status": "no-trip-services"} for d in days}

    mapping = pick_feed_versions(read_feed_coverage(spark, base), days)
    result: dict[str, dict] = {}
    for day in days:
        version = mapping.get(day)
        if version is None:
            LOG.warning("No GTFS feed covers service date %s; schedule_stops skipped", day)
            result[day] = {"status": "no-feed"}
            continue
        if not force and stored_feed_version(spark, base, day) == version:
            result[day] = {"status": "unchanged", "feed_version": version}
            continue

        frame = build_day(spark, base, day, version).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            rows = frame.count()
            if rows == 0:
                # e.g. the feed's trip_services are not written yet; retried next cycle
                LOG.warning("schedule_stops for %s (%s) has no rows; not written", day, version)
                result[day] = {"status": "empty", "feed_version": version}
                continue
            (frame.coalesce(4).write.mode("overwrite").partitionBy("service_date")
             .parquet(f"{base}{OUTPUT}"))
            LOG.info("schedule_stops %s: %d rows from feed %s", day, rows, version)
            result[day] = {"status": "built", "feed_version": version, "rows": rows}
        finally:
            frame.unpersist()
    return result


def refresh_schedule_stops(
    spark: SparkSession, base: str = HDFS, today: str | None = None, force: bool = False
) -> dict[str, dict]:
    """Entry point for the pipeline: yesterday, today and tomorrow (configurable)."""
    behind = max(0, int(os.getenv("SCHEDULE_DAYS_BEHIND", "1")))
    ahead = max(0, int(os.getenv("SCHEDULE_DAYS_AHEAD", "1")))
    days = schedule_dates(today or warsaw_today(spark), behind, ahead)
    return build_schedule_stops(spark, base, days, force)


# --------------------------------------------------------------------------- #
def main() -> None:
    parser = argparse.ArgumentParser(description="Build the daily timetable lookup for streaming")
    parser.add_argument("--base", default=HDFS)
    parser.add_argument("--date", action="append", help="service date; repeatable (default: yesterday..tomorrow)")
    parser.add_argument("--force", action="store_true", help="rebuild even if the feed version is unchanged")
    args = parser.parse_args()
    with BatchLock():
        spark = build_spark("ztm-schedule-stops", args.base)
        try:
            if args.date:
                result = build_schedule_stops(spark, args.base, sorted(args.date), args.force)
            else:
                result = refresh_schedule_stops(spark, args.base, force=args.force)
        finally:
            spark.stop()
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()