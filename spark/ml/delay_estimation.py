"""Delay estimation: GPS pings + GTFS timetable -> stop visits with a delay.

DEFINITION (the single source of truth for "delay" in this project)
------------------------------------------------------------------
For a vehicle on trip T (service date S), a *stop visit* is found from the GPS pings
within `radius_m` metres of a stop of T: the closest approach, or - if the vehicle
stands at the stop - the LAST ping of the stay (its departure).
For that visit

    scheduled_time = service_day_start(S) + departure_seconds(stop)
    delay_s        = visit_time - scheduled_time          (positive = late)

* `service_day_start` follows the GTFS convention (noon minus 12 h), so DST days
  and times such as 25:10:00 are handled correctly.
* One visit per (trip, service date, vehicle, stop_sequence).
* If the same physical stop occurs twice in a trip (loop routes) or two stops are
  close together, each ping is assigned to the *nearest* stop, and ties between
  stop_sequences of the same stop are broken by the smallest |delay|.
* Visits with |delay| > max_abs_delay_s are discarded as mismatches.

Nothing here is batch specific; the same expressions can run on a stream.
"""

from __future__ import annotations

import logging
import re

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from ml_common import (
    DEFAULT_DWELL_TOLERANCE_M,
    DEFAULT_MAX_ABS_DELAY_S,
    DEFAULT_STOP_RADIUS_M,
    MAX_RADIUS_FOR_GRID_M,
    grid_x,
    grid_y,
    haversine_m,
    service_day_start_epoch,
)

LOG = logging.getLogger("ztm-ml")

# One physical vehicle driving one trip on one service date.
TRIP_KEY = ["trip_id", "service_date", "side_number"]

_FEED_DATE = re.compile(r"^(\d{4}-\d{2}-\d{2})-")


# --------------------------------------------------------------------------- #
# Choosing the GTFS feed that belongs to each service date
# --------------------------------------------------------------------------- #
def feed_download_date(feed_version: str) -> str | None:
    """'2026-11-15-ab12cd34ef56' -> '2026-11-15' (None for 'undated-...')."""
    match = _FEED_DATE.match(feed_version)
    return match.group(1) if match else None


def pick_feed_versions(
    coverage: dict[str, set[str]],
    service_dates: list[str],
    forced: str | None = None,
) -> dict[str, str]:
    """Map every service date to one GTFS feed version.

    coverage: feed_version -> set of 'YYYY-MM-DD' dates that feed operates.
    Rule: among the feeds that contain the service date, prefer the most recent
    feed downloaded on or before that date (the timetable that was valid on the
    day). If there is none, fall back to the earliest feed downloaded later.
    Dates covered by no feed are omitted (their pings cannot be matched).
    `forced` maps every date to one feed version (useful if trip_ids are stable).
    """
    if forced:
        return {day: forced for day in service_dates}
    mapping: dict[str, str] = {}
    for day in service_dates:
        candidates = [v for v, days in coverage.items() if day in days]
        if not candidates:
            continue
        # Feeds without a download date sort as "oldest".
        key = lambda v: (feed_download_date(v) or "0000-00-00", v)
        before = [v for v in candidates if (feed_download_date(v) or "0000-00-00") <= day]
        mapping[day] = max(before, key=key) if before else min(candidates, key=key)
    return mapping


def read_feed_coverage(spark: SparkSession, base: str) -> dict[str, set[str]]:
    """feed_version -> dates with active service, from cleaned gtfs_calendar_dates."""
    root = f"{base}/data/cleaned/gtfs_calendar_dates"
    try:
        rows = (
            spark.read.parquet(root)
            .filter(F.col("exception_type") == 1)
            .select("feed_version", F.date_format("date", "yyyy-MM-dd").alias("day"))
            .distinct()
            .collect()
        )
    except Exception as exc:  # directory missing / empty
        LOG.warning("No GTFS calendar_dates found under %s (%s)", root, str(exc).splitlines()[0])
        return {}
    coverage: dict[str, set[str]] = {}
    for row in rows:
        coverage.setdefault(row["feed_version"], set()).add(row["day"])
    return coverage


# --------------------------------------------------------------------------- #
# Stops of each trip, with coordinates and scheduled seconds
# --------------------------------------------------------------------------- #
def build_trip_stops(stop_times: DataFrame, stops: DataFrame) -> DataFrame:
    """Join stop_times with stop coordinates.

    Both inputs must carry a `feed_version` column (the Parquet partition).
    Output: feed_version, trip_id, stop_sequence, stop_id, stop_lat, stop_lon,
            sched_seconds (departure_seconds, falling back to arrival_seconds).
    """
    coords = stops.select(
        "feed_version", "stop_id", "stop_lat", "stop_lon"
    ).dropDuplicates(["feed_version", "stop_id"])
    return (
        stop_times.select(
            "feed_version", "trip_id", "stop_sequence", "stop_id",
            F.coalesce("departure_seconds", "arrival_seconds").alias("sched_seconds"),
        )
        .join(coords, ["feed_version", "stop_id"], "inner")
        .filter(
            F.col("sched_seconds").isNotNull()
            & F.col("stop_lat").isNotNull()
            & F.col("stop_lon").isNotNull()
        )
    )


# --------------------------------------------------------------------------- #
# Stop visit detection
# --------------------------------------------------------------------------- #
def detect_stop_visits(
    pings: DataFrame,
    trip_stops: DataFrame,
    radius_m: float = DEFAULT_STOP_RADIUS_M,
    max_abs_delay_s: int = DEFAULT_MAX_ABS_DELAY_S,
    dwell_tolerance_m: float = DEFAULT_DWELL_TOLERANCE_M,
) -> DataFrame:
    """Find stop visits and their delay.

    pings must contain: feed_version, trip_id, service_date (date), side_number,
    event_epoch (long, UTC seconds), lat, lon.
    trip_stops comes from build_trip_stops() (ideally restricted to the trips that
    appear in `pings`, otherwise the join becomes huge).

    Efficiency: instead of pairing every ping with every stop of its trip, stops
    are copied into the 3x3 neighbouring grid cells and joined on
    (feed_version, trip_id, cell_x, cell_y); only genuinely nearby stops meet.
    """
    if radius_m > MAX_RADIUS_FOR_GRID_M:
        raise ValueError(
            f"radius_m={radius_m} exceeds the grid cell size; max is {MAX_RADIUS_FOR_GRID_M} m"
        )

    offsets = F.array(*[
        F.struct(F.lit(dx).alias("dx"), F.lit(dy).alias("dy"))
        for dx in (-1, 0, 1) for dy in (-1, 0, 1)
    ])
    stop_cells = (
        trip_stops.withColumn("_o", F.explode(offsets))
        .withColumn("cell_x", grid_x(F.col("stop_lon")) + F.col("_o.dx"))
        .withColumn("cell_y", grid_y(F.col("stop_lat")) + F.col("_o.dy"))
        .drop("_o")
    )
    pings_c = (
        pings.select(
            "feed_version", "trip_id", "service_date", "side_number",
            "event_epoch", "lat", "lon",
        )
        .withColumn("cell_x", grid_x(F.col("lon")))
        .withColumn("cell_y", grid_y(F.col("lat")))
    )

    candidates = (
        pings_c.join(stop_cells, ["feed_version", "trip_id", "cell_x", "cell_y"], "inner")
        .withColumn(
            "distance_m",
            haversine_m(F.col("lat"), F.col("lon"), F.col("stop_lat"), F.col("stop_lon")),
        )
        .filter(F.col("distance_m") <= radius_m)
        .withColumn(
            "scheduled_epoch",
            service_day_start_epoch(F.col("service_date")) + F.col("sched_seconds"),
        )
        .withColumn("delay_s", F.col("event_epoch") - F.col("scheduled_epoch"))
    )

    # Step 1 - every ping belongs to ONE stop: the nearest one (rounded to 0.1 m so
    # that the same physical stop appearing twice ties), then smallest |delay|.
    ping_key = [*TRIP_KEY, "event_epoch"]
    per_ping = candidates.groupBy(*ping_key).agg(
        F.min(F.struct(
            F.round("distance_m", 1).alias("distance_rounded"),
            F.abs("delay_s").alias("abs_delay_s"),
            F.col("stop_sequence"),
            F.col("stop_id"),
            F.col("delay_s"),
            F.col("scheduled_epoch"),
            F.col("distance_m"),
            F.col("feed_version"),
        )).alias("best")
    ).select(*ping_key, "best.*")

    # Step 2 - every (trip, vehicle, stop_sequence) is visited once. Find the closest
    # approach, then take the LAST ping that is still within `dwell_tolerance_m` of it:
    #  * a bus driving past has one ping near the stop -> that ping;
    #  * a bus standing at the stop (terminus, timing point) sends many pings from the
    #    same spot -> the last one is the DEPARTURE, which is what GTFS schedules.
    # Taking the first ping instead would stamp the arrival and make early arrivals at
    # terminals look like huge negative delays.
    by_stop = Window.partitionBy(*TRIP_KEY, "stop_sequence")
    near = (
        per_ping.withColumn("_dmin", F.min("distance_m").over(by_stop))
        .filter(F.col("distance_m") <= F.col("_dmin") + dwell_tolerance_m)
    )
    visits = near.groupBy(*TRIP_KEY, "stop_sequence").agg(
        F.max(F.struct(
            F.col("event_epoch"),
            F.col("stop_id"),
            F.col("delay_s"),
            F.col("scheduled_epoch"),
            F.col("distance_m"),
            F.col("feed_version"),
        )).alias("v")
    )
    return (
        visits.select(
            *TRIP_KEY,
            "stop_sequence",
            F.col("v.stop_id").alias("stop_id"),
            F.col("v.feed_version").alias("feed_version"),
            F.col("v.event_epoch").alias("visit_epoch"),
            F.col("v.scheduled_epoch").alias("scheduled_epoch"),
            F.col("v.delay_s").cast("int").alias("delay_s"),
            F.col("v.distance_m").alias("distance_m"),
        )
        .filter(F.abs("delay_s") <= max_abs_delay_s)
        .withColumn("visit_time_utc", F.col("visit_epoch").cast("timestamp"))
        .withColumn("scheduled_time_utc", F.col("scheduled_epoch").cast("timestamp"))
        .withColumn("date", F.to_date("visit_time_utc"))
    )
