"""Shared fixtures: a local Spark session and a synthetic 'cleaned' data lake.

The synthetic world (service date 2026-11-16, Warsaw = UTC+1):
  * route 175, trip T1, 12 stops, 0.01 deg (~1.1 km) apart going north along lon 21.0
  * timetable: stop k is scheduled at 08:00:00 local + 120 s * k
  * bus 1001 runs T1 with a KNOWN delay of 60 + 10*k seconds at stop k,
    GPS ping every 10 s, linear movement between stops
  * junk: bus 2002 on a trip_id that is not in GTFS, and one ping at (0, 0)
  * one weather station and one PM10 air station near the route
"""
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

# PySpark converts timestamps to Python datetimes in the *local* time zone of the
# test process. Pin it to UTC so results do not depend on the developer's laptop
# (Warsaw) or on CI.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "spark" / "ml"), str(ROOT / "spark" / "batch")]
os.environ.setdefault("ML_SPARK_MASTER", "local[2]")
os.environ.setdefault("ML_SHUFFLE_PARTITIONS", "4")
os.environ.setdefault("HDFS_BASE", "file:///tmp/unused")

FEED = "2026-11-15-abcdef123456"
SERVICE_DATE = date(2026, 11, 16)
N_STOPS = 12
STOP_STEP_DEG = 0.01
BASE_LAT, LON = 52.10, 21.00
SCHED_START_UTC = datetime(2026, 11, 16, 7, 0, 0)  # 08:00 Warsaw, naive UTC


def known_delay(k: int) -> int:
    return 60 + 10 * k


@pytest.fixture(scope="session")
def spark():
    from ml_common import build_spark
    s = build_spark("pytest-ml", "file:///tmp")
    yield s
    s.stop()


def _write(spark, rows, schema, path, partition=None):
    writer = spark.createDataFrame(rows, schema).write.mode("overwrite")
    (writer.partitionBy(partition) if partition else writer).parquet(path)


def _bus_pings():
    arrive = [SCHED_START_UTC + timedelta(seconds=120 * k + known_delay(k)) for k in range(N_STOPS)]
    speed = STOP_STEP_DEG / 130.0  # deg per second outside the timetable
    t, end = arrive[0] - timedelta(seconds=60), arrive[-1] + timedelta(seconds=60)
    pings, n = [], 0
    while t <= end:
        if t <= arrive[0]:
            lat = BASE_LAT - (arrive[0] - t).total_seconds() * speed
        elif t >= arrive[-1]:
            lat = BASE_LAT + (N_STOPS - 1) * STOP_STEP_DEG + (t - arrive[-1]).total_seconds() * speed
        else:
            k = max(i for i in range(N_STOPS) if arrive[i] <= t)
            frac = (t - arrive[k]).total_seconds() / (arrive[k + 1] - arrive[k]).total_seconds()
            lat = BASE_LAT + (k + frac) * STOP_STEP_DEG
        pings.append((f"r{n}", "T1", "1001", SERVICE_DATE, t, lat, LON, 0.0))
        n += 1
        t += timedelta(seconds=10)
    return pings, end


@pytest.fixture(scope="session")
def lake(spark, tmp_path_factory):
    """Write the synthetic cleaned datasets; return the base URI (file://...)."""
    from pyspark.sql import functions as F

    base = f"file://{tmp_path_factory.mktemp('lake')}"
    cl = f"{base}/data/cleaned"

    # ---- GTFS (partitioned by feed_version, as clean_gtfs.py writes it) ----
    _write(spark, [(FEED, "S1", SERVICE_DATE, 1)],
           "feed_version string, service_id string, date date, exception_type int",
           f"{cl}/gtfs_calendar_dates", "feed_version")
    _write(spark, [(FEED, "T1", "175", "S1", 0)],
           "feed_version string, trip_id string, route_id string, service_id string, direction_id int",
           f"{cl}/gtfs_trips", "feed_version")
    _write(spark, [(FEED, "175", "175", 3)],
           "feed_version string, route_id string, route_short_name string, route_type int",
           f"{cl}/gtfs_routes", "feed_version")
    _write(spark, [(FEED, f"stop{k}", BASE_LAT + k * STOP_STEP_DEG, LON) for k in range(N_STOPS)],
           "feed_version string, stop_id string, stop_lat double, stop_lon double",
           f"{cl}/gtfs_stops", "feed_version")
    _write(spark, [(FEED, "T1", k, f"stop{k}", 8 * 3600 + 120 * k, 8 * 3600 + 120 * k)
                   for k in range(N_STOPS)],
           "feed_version string, trip_id string, stop_sequence int, stop_id string, "
           "departure_seconds int, arrival_seconds int",
           f"{cl}/gtfs_stop_times", "feed_version")

    # ---- GPS ----
    pings, end = _bus_pings()
    pings.append(("x1", "NOPE", "2002", SERVICE_DATE, datetime(2026, 11, 16, 7, 30, 0), 52.2, 21.0, 0.0))
    pings.append(("x2", "T1", "1001", SERVICE_DATE, datetime(2026, 11, 16, 7, 31, 0), 0.0, 0.0, 0.0))
    (spark.createDataFrame(
        pings, "record_id string, trip_id string, side_number string, service_date date, "
               "event_time_utc timestamp, lat double, lon double, bearing double")
     .withColumn("date", F.to_date("event_time_utc"))
     .write.mode("overwrite").partitionBy("date").parquet(f"{cl}/gps_observations"))

    # ---- weather every 30 s, air (PM10) every 60 s ----
    start_env, end_env = SCHED_START_UTC - timedelta(minutes=5), end + timedelta(minutes=30)
    weather, t = [], start_env
    while t <= end_env:
        weather.append(("W1", "Station A", t, 52.15, 21.0, 5.0, 4.0, 80.0, 3.0, 90.0,
                        0.2 if t.minute % 2 == 0 else 0.0))
        t += timedelta(seconds=30)
    (spark.createDataFrame(
        weather, "station_id string, station_name string, event_time_utc timestamp, lat double, "
                 "lon double, temp_pow double, temp_naw double, wilg_pow double, pred_wiatru double, "
                 "kier_wiatru double, int_opadu double")
     .withColumn("date", F.to_date("event_time_utc"))
     .write.mode("overwrite").partitionBy("date").parquet(f"{cl}/weather_observations"))

    air, t = [], start_env
    while t <= end_env:
        air.append(("A1", "Warszawa-X", "PM10", t, 52.14, 21.01, 25.0, "ug/m3"))
        t += timedelta(seconds=60)
    (spark.createDataFrame(
        air, "station_id string, station_name string, param_code string, event_time_utc timestamp, "
             "lat double, lon double, value double, unit string")
     .withColumn("date", F.to_date("event_time_utc"))
     .write.mode("overwrite").partitionBy("date").parquet(f"{cl}/air_quality_observations"))
    return base
