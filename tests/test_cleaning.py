"""Tests of the existing Raw->Cleaned transformations (spark/batch/clean_raw.py, clean_gtfs.py)."""
import json

import pytest
from pyspark.sql import functions as F

import clean_raw
from clean_gtfs import seconds_since_start


@pytest.fixture()
def gps_json(spark, tmp_path):
    snap = {"time": "2026-11-16T08:00:00+01:00", "positions": [
        {"id": "a", "trip_id": "T1", "side_number": "1", "service_date": "20261116",
         "timestamp": "2026-11-16T07:00:00Z", "lat": 52.2, "lon": 21.0, "bearing": -90.0},
        {"id": "a", "trip_id": "T1", "side_number": "1", "service_date": "20261116",   # duplicate key
         "timestamp": "2026-11-16T07:00:00Z", "lat": 52.2, "lon": 21.0, "bearing": -90.0},
        {"id": "b", "trip_id": "T1", "side_number": "1", "service_date": "20261116",   # lat out of range
         "timestamp": "2026-11-16T07:00:10Z", "lat": 200.0, "lon": 21.0, "bearing": 0.0},
        {"id": "c", "trip_id": "", "side_number": "1", "service_date": "20261116",     # empty trip
         "timestamp": "2026-11-16T07:00:20Z", "lat": 52.2, "lon": 21.0, "bearing": 0.0},
    ]}
    (tmp_path / "s.json").write_text(json.dumps(snap))
    return spark.read.schema(clean_raw.GPS_SNAPSHOT).option("multiLine", "true").json(str(tmp_path / "s.json"))


def test_gps_cleaning_drops_bad_and_duplicate_rows(gps_json):
    out = clean_raw.clean_gps(gps_json).collect()
    assert len(out) == 1
    assert out[0]["bearing"] == pytest.approx(270.0)   # -90 -> 270


def test_gtfs_seconds_after_midnight(spark):
    r = spark.createDataFrame([("25:10:00",), ("08:00:00",), ("bad",)], "t string") \
        .select("t", seconds_since_start("t").alias("s")).collect()
    got = {x["t"]: x["s"] for x in r}
    assert got == {"25:10:00": 90600, "08:00:00": 28800, "bad": None}


def test_warsaw_local_time_converted_to_utc(spark):
    r = spark.createDataFrame([("2026-11-16 08:00:00",), ("2026-07-16 08:00:00",)], "t string") \
        .select("t", clean_raw.local_warsaw_to_utc(F.col("t")).alias("u")).collect()
    got = {x["t"]: x["u"].strftime("%H:%M") for x in r}
    assert got["2026-11-16 08:00:00"] == "07:00"   # winter, UTC+1
    assert got["2026-07-16 08:00:00"] == "06:00"   # summer, UTC+2
