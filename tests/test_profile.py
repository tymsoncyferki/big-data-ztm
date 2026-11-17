"""profile_data.py must detect whether GPS trip_ids match the GTFS trip_ids."""
from pyspark.sql import functions as F

from delay_estimation import read_feed_coverage
from profile_data import profile_gtfs, profile_gtfs_stop_times, profile_gps_quality, profile_trip_id_match


def test_matching_trip_ids_are_reported_ok(spark, lake):
    _, coverage = profile_gtfs(spark, lake)
    out = profile_trip_id_match(spark, lake, ["2026-11-16"], coverage)
    day = out["per_service_date"][0]
    # synthetic GPS has two trip_ids: T1 (in GTFS) and NOPE (not in GTFS)
    assert day["gps_trip_ids"] == 2 and day["matched"] == 1
    assert day["sample_unmatched_gps_trip_ids"] == ["NOPE"]
    assert out["verdict"].startswith("PARTIAL")


def test_completely_different_trip_id_format_is_a_failure(spark, lake, tmp_path):
    """Simulate GPS ids like '1_T1' vs GTFS 'T1' : the verdict must be FAIL."""
    base = f"file://{tmp_path}"
    src = f"{lake}/data/cleaned"
    for name in ("gtfs_calendar_dates", "gtfs_trips"):
        spark.read.parquet(f"{src}/{name}").write.partitionBy("feed_version").parquet(f"{base}/data/cleaned/{name}")
    (spark.read.parquet(f"{src}/gps_observations")
     .withColumn("trip_id", F.concat(F.lit("1_"), "trip_id"))
     .write.partitionBy("date").parquet(f"{base}/data/cleaned/gps_observations"))
    coverage = read_feed_coverage(spark, base)
    out = profile_trip_id_match(spark, base, ["2026-11-16"], coverage)
    assert out["verdict"].startswith("FAIL")


def test_gtfs_and_quality_summaries(spark, lake):
    quality = profile_gps_quality(spark, lake, ["2026-11-16"])
    assert quality["outside_warsaw_bbox"] == 1          # the (0, 0) ping
    st = profile_gtfs_stop_times(spark, lake, ["2026-11-15-abcdef123456"])
    assert st["rows"] == 12 and st["null_departure_seconds"] == 0
