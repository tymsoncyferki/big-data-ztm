"""schedule_stops: the daily timetable lookup for streaming (tiny synthetic lake)."""
from datetime import date

import pytest
from pyspark.sql import functions as F

from schedule_stops import (
    OUTPUT,
    build_schedule_stops,
    schedule_dates,
    stored_feed_version,
)

FEED = "2026-11-15-abcdef123456"
FEED2 = "2026-11-16-fedcba654321"
DAY = date(2026, 11, 16)


def _write(spark, rows, schema, path, partition=None):
    writer = spark.createDataFrame(rows, schema).write.mode("overwrite")
    (writer.partitionBy(partition) if partition else writer).parquet(path)


def _mini_lake(spark, tmp_path, feed=FEED):
    """Two trips on 2026-11-16: T1 (08:00, 3 stops) and T2 (25:00:00, 2 stops)."""
    base = f"file://{tmp_path}"
    cl, en = f"{base}/data/cleaned", f"{base}/data/enriched"
    _write(spark, [(feed, "S1", DAY, 1)],
           "feed_version string, service_id string, date date, exception_type int",
           f"{cl}/gtfs_calendar_dates", "feed_version")
    _write(spark, [(feed, f"stop{k}", 52.10 + 0.01 * k, 21.0) for k in range(3)],
           "feed_version string, stop_id string, stop_lat double, stop_lon double",
           f"{cl}/gtfs_stops", "feed_version")
    stop_times = [(feed, "T1", k, f"stop{k}", 8 * 3600 + 120 * k, 8 * 3600 + 120 * k) for k in range(3)]
    stop_times += [(feed, "T2", 1, "stop0", 25 * 3600, 25 * 3600),
                   (feed, "T2", 2, "stop1", 25 * 3600 + 300, 25 * 3600 + 300)]
    _write(spark, stop_times,
           "feed_version string, trip_id string, stop_sequence int, stop_id string, "
           "departure_seconds int, arrival_seconds int",
           f"{cl}/gtfs_stop_times", "feed_version")
    _write(spark, [(feed, t, DAY, "S1", "175", "175", 3, 0, "Centrum") for t in ("T1", "T2")],
           "feed_version string, trip_id string, service_date date, service_id string, "
           "route_id string, route_short_name string, route_type int, direction_id int, "
           "trip_headsign string",
           f"{en}/gtfs_trip_services", "feed_version")
    return base


def _read(spark, base, day="2026-11-16"):
    return spark.read.parquet(f"{base}{OUTPUT}/service_date={day}")


def test_schedule_dates():
    assert schedule_dates("2026-11-16") == ["2026-11-15", "2026-11-16", "2026-11-17"]
    assert schedule_dates("2026-11-16", 0, 0) == ["2026-11-16"]


def test_build_content_and_after_midnight_times(spark, tmp_path):
    base = _mini_lake(spark, tmp_path)
    result = build_schedule_stops(spark, base, ["2026-11-16"])
    assert result["2026-11-16"] == {"status": "built", "feed_version": FEED, "rows": 5}

    rows = {(r["trip_id"], r["stop_sequence"]): r for r in _read(spark, base).collect()}
    assert len(rows) == 5
    # 08:00 Warsaw (UTC+1 in November) = 07:00 UTC
    t1 = rows[("T1", 0)]
    first_utc = spark.range(1).select(F.lit("2026-11-16 07:00:00").cast("timestamp").cast("long")).first()[0]
    assert t1["sched_epoch"] == first_utc
    assert rows[("T1", 2)]["sched_epoch"] == first_utc + 240
    assert t1["trip_last_sequence"] == 2 and t1["route_short_name"] == "175"
    assert t1["trip_headsign"] == "Centrum" and t1["feed_version"] == FEED
    # 25:00:00 = 01:00 Warsaw on the 17th = 00:00 UTC on the 17th (07:00 UTC + 17 h)
    t2 = rows[("T2", 1)]
    assert t2["sched_seconds"] == 25 * 3600
    assert t2["sched_epoch"] == first_utc + 17 * 3600
    assert t2["cell_x"] is not None and t2["cell_y"] is not None


def test_second_run_is_unchanged_and_force_rebuilds(spark, tmp_path):
    base = _mini_lake(spark, tmp_path)
    build_schedule_stops(spark, base, ["2026-11-16"])
    assert stored_feed_version(spark, base, "2026-11-16") == FEED
    again = build_schedule_stops(spark, base, ["2026-11-16"])
    assert again["2026-11-16"]["status"] == "unchanged"
    forced = build_schedule_stops(spark, base, ["2026-11-16"], force=True)
    assert forced["2026-11-16"]["status"] == "built"


def test_other_dates_are_not_touched(spark, tmp_path):
    base = _mini_lake(spark, tmp_path)
    build_schedule_stops(spark, base, ["2026-11-16"])
    # a date without any feed does not delete or create anything
    result = build_schedule_stops(spark, base, ["2026-11-16", "2026-12-24"], force=True)
    assert result["2026-12-24"]["status"] == "no-feed"
    assert _read(spark, base).count() == 5


def test_new_feed_version_replaces_the_partition(spark, tmp_path):
    base = _mini_lake(spark, tmp_path)
    build_schedule_stops(spark, base, ["2026-11-16"])
    # a newer feed (downloaded on the 16th) also covers the 16th
    cl, en = f"{base}/data/cleaned", f"{base}/data/enriched"
    _write(spark, [(FEED2, "S1", DAY, 1)],
           "feed_version string, service_id string, date date, exception_type int",
           f"{cl}/gtfs_calendar_dates", "feed_version")
    for table, df_rows, schema in [
        ("gtfs_stops", [(FEED2, "stop0", 52.10, 21.0)],
         "feed_version string, stop_id string, stop_lat double, stop_lon double"),
        ("gtfs_stop_times", [(FEED2, "T9", 0, "stop0", 9 * 3600, 9 * 3600)],
         "feed_version string, trip_id string, stop_sequence int, stop_id string, "
         "departure_seconds int, arrival_seconds int"),
    ]:
        spark.createDataFrame(df_rows, schema).write.mode("append").partitionBy("feed_version").parquet(f"{cl}/{table}")
    spark.createDataFrame(
        [(FEED2, "T9", DAY, "S1", "175", "175", 3, 0, "Centrum")],
        "feed_version string, trip_id string, service_date date, service_id string, "
        "route_id string, route_short_name string, route_type int, direction_id int, "
        "trip_headsign string",
    ).write.mode("append").partitionBy("feed_version").parquet(f"{en}/gtfs_trip_services")

    result = build_schedule_stops(spark, base, ["2026-11-16"])
    assert result["2026-11-16"] == {"status": "built", "feed_version": FEED2, "rows": 1}
    assert [r["trip_id"] for r in _read(spark, base).collect()] == ["T9"]


def test_missing_trip_services_is_not_an_error(spark, tmp_path):
    base = f"file://{tmp_path}"
    result = build_schedule_stops(spark, base, ["2026-11-16"])
    assert result == {"2026-11-16": {"status": "no-trip-services"}}


def test_refuses_static_overwrite_mode(spark, tmp_path):
    fresh = spark.newSession()
    fresh.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    with pytest.raises(RuntimeError):
        build_schedule_stops(fresh, f"file://{tmp_path}", ["2026-11-16"])