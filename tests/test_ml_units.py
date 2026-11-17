"""Small unit tests of the reusable definitions."""
import pytest
from pyspark.sql import functions as F

from delay_estimation import feed_download_date, pick_feed_versions
from feature_engineering import air_column_name
from ml_common import haversine_m, service_day_start_epoch


def test_haversine_known_distance(spark):
    # 0.01 deg of latitude is 1111.95 m
    r = spark.range(1).select(haversine_m(F.lit(52.0), F.lit(21.0), F.lit(52.01), F.lit(21.0)).alias("d")).first()
    assert r["d"] == pytest.approx(1111.95, abs=1.0)


def test_service_day_start_normal_and_dst_days(spark):
    rows = spark.createDataFrame([("2026-11-16",), ("2026-10-25",), ("2026-03-29",)], "d string") \
        .select("d", service_day_start_epoch(F.to_date("d")).alias("e")) \
        .select("d", F.col("e").cast("timestamp").alias("utc")).collect()
    got = {r["d"]: r["utc"].strftime("%Y-%m-%d %H:%M") for r in rows}
    assert got["2026-11-16"] == "2026-11-15 23:00"   # midnight Warsaw (UTC+1)
    # GTFS: service day starts at "noon minus 12 h", which differs from local
    # midnight on the two DST days of the year.
    assert got["2026-10-25"] == "2026-10-24 23:00"   # fall-back day (25 h): local midnight would be 22:00 UTC
    assert got["2026-03-29"] == "2026-03-28 22:00"   # spring-forward day (23 h): local midnight would be 23:00 UTC


def test_feed_selection():
    cov = {"2026-11-10-aaa": {"2026-11-12", "2026-11-16"}, "2026-11-15-bbb": {"2026-11-16", "2026-11-17"}}
    m = pick_feed_versions(cov, ["2026-11-12", "2026-11-16", "2026-11-17", "2026-12-01"])
    assert m["2026-11-12"] == "2026-11-10-aaa"
    assert m["2026-11-16"] == "2026-11-15-bbb"     # newest feed downloaded on/before the day
    assert m["2026-11-17"] == "2026-11-15-bbb"
    assert "2026-12-01" not in m                    # no feed covers it
    assert pick_feed_versions(cov, ["2026-11-12"], forced="X") == {"2026-11-12": "X"}
    assert feed_download_date("undated-abc") is None


def test_air_column_name():
    assert air_column_name("PM2.5") == "air_pm2_5"
    assert air_column_name("NO2") == "air_no2"
