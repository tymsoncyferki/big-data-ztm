"""End-to-end tests of delay estimation and the ML dataset on the synthetic lake."""
import pytest
from pyspark.sql import functions as F

from build_ml_dataset import BuildConfig, build
from conftest import N_STOPS, known_delay


@pytest.fixture(scope="module")
def result(spark, lake):
    stats = build(spark, BuildConfig(base=lake, start_date="2026-11-16", end_date="2026-11-16"))
    visits = spark.read.parquet(f"{lake}/data/enriched/stop_visits")
    ml = spark.read.parquet(f"{lake}/data/enriched/ml_dataset")
    return stats, visits, ml


def test_every_stop_visit_found_with_right_delay(result):
    _, visits, _ = result
    rows = {r["stop_sequence"]: r["delay_s"] for r in visits.collect()}
    assert sorted(rows) == list(range(N_STOPS))
    for k, delay in rows.items():
        # pings are 10 s apart, so the closest ping is at most ~5-10 s off
        assert abs(delay - known_delay(k)) <= 10, (k, delay, known_delay(k))


def test_junk_pings_do_not_create_visits_or_rows(result):
    _, visits, ml = result
    assert visits.filter(F.col("side_number") == "2002").count() == 0
    assert ml.filter(F.col("lat") < 50).count() == 0  # the (0,0) ping is gone


def test_scheduled_time_uses_warsaw_time(result):
    _, visits, _ = result
    first = visits.filter(F.col("stop_sequence") == 0).first()
    # 08:00 Warsaw in November = 07:00 UTC
    assert first["scheduled_time_utc"].strftime("%H:%M:%S") == "07:00:00"


def test_no_leakage_target_is_strictly_in_the_future(result):
    _, _, ml = result
    with_target = ml.filter("has_target")
    assert with_target.count() > 0
    assert with_target.filter(F.col("target_horizon_s") < 600).count() == 0
    assert with_target.filter(F.col("target_horizon_s") > 900).count() == 0
    # the feature is the PAST delay; target (future) differs from it on a growing delay
    grown = with_target.filter("last_delay_s is not null").filter(F.col("target_delay_s") > F.col("last_delay_s"))
    assert grown.count() > 0


def test_last_delay_is_never_from_the_future(result):
    _, _, ml = result
    assert ml.filter(F.col("last_delay_age_s") < 30).count() == 0  # confirm lag


def test_environment_features_attached(result):
    _, _, ml = result
    row = ml.filter(~F.col("weather_missing")).first()
    assert row is not None
    assert row["temp_air"] == pytest.approx(5.0)
    assert row["wind_speed"] == pytest.approx(3.0)
    assert row["air_pm10"] == pytest.approx(25.0)
    assert row["weather_station_dist_m"] < 10_000
    assert ml.filter(F.col("air_pm2_5").isNotNull()).count() == 0  # not in the data -> NULL, not 0
    assert ml.filter("air_missing").count() == 0


def test_calendar_features_in_local_time(result):
    _, _, ml = result
    r = ml.first()
    assert r["hour_local"] in (8, 9)        # 07:xx-08:xx UTC = 08:xx-09:xx Warsaw
    assert r["day_of_week"] == 0            # 2026-11-16 is a Monday
    assert r["is_weekend"] is False


def test_rerun_is_idempotent(spark, lake, result):
    stats, visits, ml = result
    before = (visits.count(), ml.count())
    build(spark, BuildConfig(base=lake, start_date="2026-11-16", end_date="2026-11-16"))
    after = (spark.read.parquet(f"{lake}/data/enriched/stop_visits").count(),
             spark.read.parquet(f"{lake}/data/enriched/ml_dataset").count())
    assert before == after


def test_report_numbers(result):
    stats, _, _ = result
    assert stats["pings"]["with_trip_match"] < stats["pings"]["pings"]  # NOPE trip unmatched
    assert stats["stop_visits"]["visits"] == N_STOPS
