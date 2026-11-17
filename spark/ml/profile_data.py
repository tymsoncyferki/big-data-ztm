"""Profile the cleaned datasets and verify the GPS <-> GTFS trip_id join.

Run this FIRST on the real data (before build_ml_dataset.py). It is read-only
(except for a small JSON report) and answers the questions the ML dataset depends on:

  1. How much data is there per day (GPS / weather / air quality)?
  2. Which GTFS feeds exist and which service dates do they cover?
  3. Does `trip_id` in GPS equal `trip_id` in GTFS?   <-- the critical one
  4. How many GPS vehicles / pings can be given a route and a timetable?

    bash scripts/profile-data.sh [--start-date 2026-11-16] [--end-date 2026-11-17]

Writes /data/enriched/_ml_reports/profile_<timestamp>.json
"""

from __future__ import annotations

import argparse
import json
import logging
import time

from pyspark.sql import SparkSession, functions as F

from delay_estimation import pick_feed_versions, read_feed_coverage
from ml_common import BBOX_LAT, BBOX_LON, HDFS, BatchLock, build_spark, list_partition_values, write_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("ztm-ml")


def _read(spark: SparkSession, base: str, name: str, days: list[str] | None = None):
    root = f"{base}/data/cleaned/{name}"
    if days is None:
        return spark.read.parquet(root)
    existing = set(list_partition_values(spark, root, "date"))
    wanted = [d for d in days if d in existing]
    if not wanted:
        return None
    return spark.read.option("basePath", root).parquet(*[f"{root}/date={d}" for d in wanted])


def _rows(frame, n: int = 1000) -> list[dict]:
    return [r.asDict(recursive=True) for r in frame.limit(n).collect()]


def profile_observations(spark, base, days, name, entity_cols):
    """Per-day row counts and distinct entities for a cleaned observation table."""
    frame = _read(spark, base, name, days)
    if frame is None:
        return {"present": False}
    per_day = (
        frame.groupBy("date")
        .agg(F.count("*").alias("rows"),
             *[F.countDistinct(c).alias(f"distinct_{c}") for c in entity_cols],
             F.min("event_time_utc").alias("first_event"),
             F.max("event_time_utc").alias("last_event"))
        .orderBy("date")
    )
    return {"present": True, "per_day": _rows(per_day)}


def profile_gps_quality(spark, base, days):
    frame = _read(spark, base, "gps_observations", days)
    if frame is None:
        return {}
    row = frame.agg(
        F.count("*").alias("rows"),
        F.sum((~(F.col("lat").between(*BBOX_LAT) & F.col("lon").between(*BBOX_LON))).cast("int"))
        .alias("outside_warsaw_bbox"),
        F.sum(F.col("bearing").isNull().cast("int")).alias("null_bearing"),
        F.countDistinct("side_number").alias("vehicles"),
        F.countDistinct("trip_id").alias("trip_ids"),
        # service_date should be within a day of the event date; larger gaps would
        # break the "delay = event - scheduled" assumption.
        F.sum((F.abs(F.datediff(F.col("date"), F.col("service_date"))) > 1).cast("int"))
        .alias("service_date_far_from_event_date"),
    ).first()
    return row.asDict()


def profile_gtfs(spark, base):
    coverage = read_feed_coverage(spark, base)
    out = {"feeds": {}}
    for version in sorted(coverage):
        days = sorted(coverage[version])
        out["feeds"][version] = {"service_days": len(days), "first": days[0], "last": days[-1]}
    return out, coverage


def profile_trip_id_match(spark, base, days, coverage):
    """Compare GPS trip_ids with GTFS trip_ids, per service date and feed."""
    gps = _read(spark, base, "gps_observations", days)
    if gps is None:
        return {"error": "no GPS partitions"}
    service_days = sorted(
        r["d"] for r in gps.select(F.date_format("service_date", "yyyy-MM-dd").alias("d")).distinct().collect()
    )
    mapping = pick_feed_versions(coverage, service_days)
    result = {"service_dates": service_days, "feed_by_service_date": mapping, "per_service_date": []}
    trips_all = spark.read.parquet(f"{base}/data/cleaned/gtfs_trips").select("feed_version", "trip_id")

    gps_ids = gps.select(
        F.date_format("service_date", "yyyy-MM-dd").alias("service_day"), "trip_id"
    ).distinct()
    for day in service_days:
        version = mapping.get(day)
        ids = gps_ids.filter(F.col("service_day") == day).select("trip_id")
        n_ids = ids.count()
        entry = {"service_date": day, "feed_version": version, "gps_trip_ids": n_ids}
        if version is None:
            entry["matched"] = None
            entry["note"] = "no GTFS feed covers this date"
            result["per_service_date"].append(entry)
            continue
        feed_trips = trips_all.filter(F.col("feed_version") == version).select("trip_id").distinct()
        matched = ids.join(feed_trips, "trip_id", "left_semi")
        n_match = matched.count()
        entry["matched"] = n_match
        entry["match_rate"] = round(n_match / n_ids, 4) if n_ids else None
        # If the chosen feed fails, does ANY other feed contain these ids?
        other = ids.join(trips_all.filter(F.col("feed_version") != version).select("trip_id").distinct(),
                         "trip_id", "left_semi").count()
        entry["matched_by_some_other_feed"] = other
        entry["sample_unmatched_gps_trip_ids"] = [
            r["trip_id"] for r in ids.join(feed_trips, "trip_id", "left_anti").limit(5).collect()
        ]
        entry["sample_gtfs_trip_ids"] = [r["trip_id"] for r in feed_trips.limit(5).collect()]
        result["per_service_date"].append(entry)

    rates = [e["match_rate"] for e in result["per_service_date"] if e.get("match_rate") is not None]
    result["verdict"] = (
        "NO feed matched any service date" if not rates
        else "OK (>= 90% of GPS trip_ids exist in GTFS)" if min(rates) >= 0.9
        else "PARTIAL: some GPS trip_ids are missing in GTFS (check sample ids and feed choice)" if max(rates) > 0.1
        else "FAIL: GPS and GTFS trip_ids do not match; see sample ids to find the transformation needed"
    )
    return result


def profile_gtfs_stop_times(spark, base, versions):
    if not versions:
        return {}
    st = spark.read.parquet(f"{base}/data/cleaned/gtfs_stop_times").filter(F.col("feed_version").isin(versions))
    row = st.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("departure_seconds").isNull().cast("int")).alias("null_departure_seconds"),
        F.sum((F.col("departure_seconds") >= 86400).cast("int")).alias("after_midnight_rows"),
        F.max("departure_seconds").alias("max_departure_seconds"),
    ).first()
    return row.asDict()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base", default=HDFS)
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--no-lock", action="store_true")
    a = p.parse_args()
    base = a.base.rstrip("/")

    with BatchLock(enabled=not a.no_lock):
        spark = build_spark("big-data-ztm-profile", base)
        try:
            all_days = list_partition_values(spark, f"{base}/data/cleaned/gps_observations", "date")
            days = [d for d in all_days if (not a.start_date or d >= a.start_date)
                    and (not a.end_date or d <= a.end_date)]
            LOG.info("GPS date partitions available: %s ; profiling: %s", all_days, days)
            report: dict = {"created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "days": days}
            report["gps"] = profile_observations(spark, base, days, "gps_observations", ["side_number", "trip_id"])
            report["gps_quality"] = profile_gps_quality(spark, base, days)
            report["weather"] = profile_observations(spark, base, days, "weather_observations", ["station_id"])
            report["air_quality"] = profile_observations(spark, base, days, "air_quality_observations",
                                                         ["station_id", "param_code"])
            gtfs, coverage = profile_gtfs(spark, base)
            report["gtfs"] = gtfs
            if coverage and days:
                report["trip_id_match"] = profile_trip_id_match(spark, base, days, coverage)
                versions = sorted(set(report["trip_id_match"]["feed_by_service_date"].values()))
                report["gtfs_stop_times"] = profile_gtfs_stop_times(spark, base, versions)
            else:
                report["trip_id_match"] = {"error": "no GTFS calendar_dates or no GPS days"}
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            write_text(spark, f"{base}/data/enriched/_ml_reports/profile_{stamp}.json",
                       json.dumps(report, indent=2, default=str))
        finally:
            spark.stop()
    print(json.dumps(report, indent=2, default=str))
    print("\nTRIP_ID VERDICT:", report["trip_id_match"].get("verdict", report["trip_id_match"]))


if __name__ == "__main__":
    main()
