"""Describe the source datasets for the Milestone 2 report (read-only).

Collects, for GPS, weather, air quality and GTFS:
  * HDFS inventory of the Raw layer: files, bytes, files per day, covered dates;
  * Raw -> Cleaned on one sample day: records per snapshot (by local hour),
    rows removed as invalid or as duplicates (same transforms as clean_raw.py);
  * Cleaned layer over the whole period: rows / entities per day, hourly
    availability and gaps, NULL rates and value distributions;
  * GPS details on the last `--detail-days` days: ping interval per vehicle,
    GPS glitches (speed > 120 km/h), share of trip_ids found in GTFS, buses vs trams;
  * GTFS: feed versions, how often the content really changes, table sizes,
    validity range, routes by type.

    bash scripts/describe-sources.sh [--raw-day 2026-11-20] [--detail-days 2] [--skip-raw]

Prints one JSON document on stdout (logs go to stderr).
"""

from __future__ import annotations

import argparse
import calendar
import json
import logging
import sys
import time
from pathlib import Path

from pyspark.sql import Window, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "batch"))
from clean_raw import SCHEMAS, TRANSFORMS  # noqa: E402

from delay_estimation import read_feed_coverage  # noqa: E402
from feature_engineering import AIR_VALID_RANGE, MAX_SPEED_GAP_S, MAX_SPEED_KMH  # noqa: E402
from ml_common import (  # noqa: E402
    BBOX_LAT, BBOX_LON, HDFS, WARSAW_TZ, BatchLock, _fs, build_spark, haversine_m, list_partition_values,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
LOG = logging.getLogger("ztm-describe")

DYNAMIC = {"gps": "gps_observations", "weather": "weather_observations", "air_quality": "air_quality_observations"}
WEATHER_FIELDS = ["temp_pow", "temp_naw", "wilg_pow", "pred_wiatru", "kier_wiatru", "int_opadu"]
# Same plausibility ranges as feature_engineering.WEATHER_VALID_RANGE, keyed by source names.
WEATHER_RANGE = {"temp_pow": (-50, 60), "temp_naw": (-50, 90), "wilg_pow": (0, 100),
                 "pred_wiatru": (0, 60), "kier_wiatru": (0, 360), "int_opadu": (0, 500)}
GTFS_TABLES = ["agency", "attributions", "feed_info", "calendar_dates", "routes", "stops", "trips",
               "stop_times", "frequencies", "shapes", "fare_attributes", "fare_rules"]


def rows(frame, n=2000):
    return [r.asDict(recursive=True) for r in frame.limit(n).collect()]


def r4(x):
    return None if x is None else round(float(x), 4)


def stats(frame, col, extra=None):
    """count / nulls / min / max / mean / p5 / p50 / p95 of one numeric column."""
    c = F.col(col)
    aggs = [F.count("*").alias("rows"), F.sum(c.isNull().cast("int")).alias("nulls"),
            F.min(c).alias("min"), F.max(c).alias("max"), F.avg(c).alias("mean"),
            F.percentile_approx(c, [0.05, 0.5, 0.95], 1000).alias("p05_p50_p95")]
    row = frame.agg(*aggs, *(extra or [])).first().asDict()
    row["null_rate"] = r4(row["nulls"] / row["rows"]) if row["rows"] else None
    row["mean"] = r4(row["mean"])
    return row


# --------------------------------------------------------------------------- #
# HDFS inventory
# --------------------------------------------------------------------------- #
def inventory(spark, uri):
    """Files and bytes under `uri`, grouped by the date=YYYY-MM-DD path component."""
    jvm, fs, root = _fs(spark, uri)
    if not fs.exists(root):
        return {"present": False}
    per_day, files, size = {}, 0, 0
    it = fs.listFiles(root, True)
    while it.hasNext():
        st = it.next()
        name = st.getPath().getName()
        if name.startswith((".", "_")):
            continue
        day = next((p[5:] for p in st.getPath().toString().split("/") if p.startswith("date=")), "?")
        d = per_day.setdefault(day, {"files": 0, "bytes": 0})
        d["files"] += 1
        d["bytes"] += st.getLen()
        files += 1
        size += st.getLen()
    return {"present": True, "files": files, "bytes": size,
            "avg_file_bytes": round(size / files) if files else None,
            "first_day": min(per_day) if per_day else None, "last_day": max(per_day) if per_day else None,
            "per_day": dict(sorted(per_day.items()))}


def dir_size(spark, uri):
    _, fs, p = _fs(spark, uri)
    if not fs.exists(p):
        return None
    s = fs.getContentSummary(p)
    return {"bytes": s.getLength(), "files": s.getFileCount()}


# --------------------------------------------------------------------------- #
# Raw sample day
# --------------------------------------------------------------------------- #
def raw_sample(spark, base, name, day):
    root = f"{base}/data/raw/{name}/date={day}"
    _, fs, p = _fs(spark, root)
    if not fs.exists(p):
        return {"present": False, "day": day}
    raw = spark.read.schema(SCHEMAS[name]).option("multiLine", True).json(f"{root}/*/*.json")
    raw = raw.withColumn("source_file", F.input_file_name()).cache()
    if name == "gps":
        per_file = raw.select("source_file", F.size("positions").alias("n"),
                              F.to_timestamp("time").alias("t"))
        exploded = raw.select(F.explode("positions").alias("p"))
    elif name == "weather":
        per_file = raw.select("source_file", F.size("stations").alias("n"), F.lit(None).cast("timestamp").alias("t"))
        exploded = raw.select(F.explode("stations").alias("p"))
    else:
        st = raw.select("source_file", F.explode("stations").alias("s"))
        per_file = (st.groupBy("source_file").agg(F.sum(F.size("s.data")).alias("n"),
                                                  F.count("*").alias("stations"))
                    .withColumn("t", F.lit(None).cast("timestamp")))
        exploded = st.select(F.explode("s.data").alias("p"))

    snap = per_file.agg(F.count("*").alias("snapshots"), F.sum("n").alias("records"),
                        F.avg("n").alias("avg_records_per_snapshot"), F.min("n").alias("min"),
                        F.max("n").alias("max"), F.sum((F.col("n") == 0).cast("int")).alias("empty_snapshots"),
                        ).first().asDict()
    snap["avg_records_per_snapshot"] = r4(snap["avg_records_per_snapshot"])
    out = {"present": True, "day": day, "snapshots": snap}
    if name == "air_quality":
        out["stations_per_snapshot"] = r4(per_file.agg(F.avg("stations")).first()[0])
    if name == "gps":
        # The snapshot 'time' says when the source produced it; by local hour = rush vs night.
        out["records_per_snapshot_by_local_hour"] = {
            r["h"]: r4(r["avg_n"]) for r in per_file.filter(F.col("t").isNotNull())
            .groupBy(F.hour(F.from_utc_timestamp("t", WARSAW_TZ)).alias("h"))
            .agg(F.avg("n").alias("avg_n")).orderBy("h").collect()}
        # Fewer distinct source times than snapshots = the source refreshes slower than we poll.
        out["distinct_source_snapshot_times"] = per_file.select("t").distinct().count()
    cleaned = TRANSFORMS[name](raw.drop("source_file"))
    n_raw = exploded.count()
    n_clean = cleaned.count()
    out["raw_records"] = n_raw
    out["after_cleaning"] = n_clean
    out["removed_invalid_or_duplicate"] = n_raw - n_clean
    out["removed_share"] = r4((n_raw - n_clean) / n_raw) if n_raw else None
    raw.unpersist()
    return out


# --------------------------------------------------------------------------- #
# Cleaned layer
# --------------------------------------------------------------------------- #
def read_cleaned(spark, base, name, days=None):
    root = f"{base}/data/cleaned/{name}"
    avail = list_partition_values(spark, root, "date")
    if days is not None:
        avail = [d for d in avail if d in set(days)]
    if not avail:
        return None
    return spark.read.option("basePath", root).parquet(*[f"{root}/date={d}" for d in avail])


def availability(frame):
    """Rows per UTC hour over the whole period and the hours with no data at all."""
    # Format in Spark (session time zone = UTC); collected datetimes would be shifted to local time.
    hourly = {r["h"]: r["n"] for r in
              frame.groupBy(F.date_format("event_time_utc", "yyyy-MM-dd HH").alias("h")).count()
              .withColumnRenamed("count", "n").collect()}
    if not hourly:
        return {}
    keys = sorted(hourly)
    t0 = calendar.timegm(time.strptime(keys[0], "%Y-%m-%d %H"))
    t1 = calendar.timegm(time.strptime(keys[-1], "%Y-%m-%d %H"))
    expected = [time.strftime("%Y-%m-%d %H", time.gmtime(t0 + 3600 * i))
                for i in range(int((t1 - t0) // 3600) + 1)]
    missing = [h for h in expected if h not in hourly]
    vals = sorted(hourly.values())
    return {"first_hour_utc": keys[0], "last_hour_utc": keys[-1], "hours_spanned": len(expected),
            "hours_with_data": len(keys), "missing_hours_utc": missing[:200],
            "rows_per_hour_min_median_max": [vals[0], vals[len(vals) // 2], vals[-1]]}


def per_day(frame, entities):
    return rows(frame.groupBy("date").agg(
        F.count("*").alias("rows"), *[F.countDistinct(c).alias(f"distinct_{c}") for c in entities],
        F.countDistinct("event_time_utc").alias("distinct_event_times"),
        F.min("event_time_utc").cast("string").alias("first_event_utc"),
        F.max("event_time_utc").cast("string").alias("last_event_utc"),
    ).orderBy("date"))


def describe_gps(spark, base, detail_days):
    gps = read_cleaned(spark, base, DYNAMIC["gps"])
    if gps is None:
        return {"present": False}
    out = {"present": True, "per_day": per_day(gps, ["side_number", "trip_id"]), "availability": availability(gps)}
    out["totals"] = gps.agg(F.count("*").alias("rows"), F.countDistinct("side_number").alias("vehicles"),
                            F.countDistinct("trip_id").alias("trip_ids")).first().asDict()
    out["quality"] = gps.agg(
        F.sum((~(F.col("lat").between(*BBOX_LAT) & F.col("lon").between(*BBOX_LON))).cast("int"))
        .alias("outside_warsaw_bbox"),
        F.sum(F.col("bearing").isNull().cast("int")).alias("null_bearing"),
        F.sum((F.abs(F.datediff("date", "service_date")) > 1).cast("int")).alias("service_date_far_from_event"),
    ).first().asDict()

    d = read_cleaned(spark, base, DYNAMIC["gps"], detail_days)
    if d is None:
        return out
    d = d.select("side_number", "trip_id", "event_time_utc", "lat", "lon").cache()
    # Active vehicles per minute, averaged by Warsaw local hour (rush hour vs night).
    per_min = d.groupBy(F.date_trunc("minute", "event_time_utc").alias("m")).agg(
        F.countDistinct("side_number").alias("v"))
    out["active_vehicles_per_minute_by_local_hour"] = {
        r["h"]: r4(r["v"]) for r in per_min.groupBy(F.hour(F.from_utc_timestamp("m", WARSAW_TZ)).alias("h"))
        .agg(F.avg("v").alias("v")).orderBy("h").collect()}
    # Interval between consecutive pings of one vehicle, and GPS glitches.
    w = Window.partitionBy("side_number").orderBy("event_time_utc")
    g = (d.withColumn("dt", F.col("event_time_utc").cast("long") - F.lag("event_time_utc").over(w).cast("long"))
         .withColumn("dist", haversine_m(F.lag("lat").over(w), F.lag("lon").over(w), F.col("lat"), F.col("lon")))
         .filter(F.col("dt").isNotNull()))
    g = g.withColumn("kmh", F.when(F.col("dt") > 0, F.col("dist") / F.col("dt") * 3.6))
    out["ping_interval_s"] = g.agg(
        F.percentile_approx("dt", [0.1, 0.25, 0.5, 0.75, 0.9, 0.99], 1000).alias("p10_p25_p50_p75_p90_p99"),
        F.avg(F.when(F.col("dt") <= MAX_SPEED_GAP_S, F.col("dt"))).alias("mean_without_gaps"),
        F.sum((F.col("dt") > MAX_SPEED_GAP_S).cast("int")).alias(f"gaps_over_{MAX_SPEED_GAP_S}s"),
        F.count("*").alias("pairs"),
    ).first().asDict()
    out["speed_glitches"] = g.agg(
        F.sum(((F.col("kmh") > MAX_SPEED_KMH) & (F.col("dt") <= MAX_SPEED_GAP_S)).cast("int"))
        .alias(f"pairs_over_{int(MAX_SPEED_KMH)}kmh"),
        F.sum((F.col("dist") < 1).cast("int")).alias("pairs_not_moving_lt_1m"),
    ).first().asDict()
    # trip_id -> GTFS route type (any feed): share matched, buses vs trams.
    try:
        trips = spark.read.parquet(f"{base}/data/cleaned/gtfs_trips").select("trip_id", "route_id").distinct()
        routes = spark.read.parquet(f"{base}/data/cleaned/gtfs_routes").select("route_id", "route_type").distinct()
        tt = trips.join(routes, "route_id").groupBy("trip_id").agg(F.min("route_type").alias("route_type"))
        ids = d.select("trip_id", "side_number").distinct()
        j = ids.join(tt, "trip_id", "left")
        out["gtfs_match"] = {
            "gps_trip_ids": ids.select("trip_id").distinct().count(),
            "matched_trip_ids": j.filter(F.col("route_type").isNotNull()).select("trip_id").distinct().count(),
            "vehicles_by_route_type": {str(r["route_type"]): r["n"] for r in
                                       j.groupBy("route_type").agg(F.countDistinct("side_number").alias("n")).collect()},
        }
    except Exception as exc:  # noqa: BLE001
        out["gtfs_match"] = {"error": str(exc)[:300]}
    out["detail_days"] = detail_days
    d.unpersist()
    return out


def describe_weather(spark, base):
    w = read_cleaned(spark, base, DYNAMIC["weather"])
    if w is None:
        return {"present": False}
    out = {"present": True, "per_day": per_day(w, ["station_id"]), "availability": availability(w),
           "stations": rows(w.groupBy("station_name").agg(F.first("lat").alias("lat"), F.first("lon").alias("lon"),
                                                          F.count("*").alias("rows")).orderBy("station_name"))}
    out["fields"] = {}
    for f in WEATHER_FIELDS:
        lo, hi = WEATHER_RANGE[f]
        out["fields"][f] = stats(w, f, [F.sum((F.col(f).isNotNull() & ~F.col(f).between(lo, hi)).cast("int"))
                                        .alias(f"outside_{lo}_{hi}")])
    out["fields"]["int_opadu"]["share_gt_0"] = r4(w.agg(F.avg((F.col("int_opadu") > 0).cast("int"))).first()[0])
    out["stations_all_fields_null_rows"] = w.filter(
        " AND ".join(f"{f} IS NULL" for f in WEATHER_FIELDS)).count()
    return out


def describe_air(spark, base):
    a = read_cleaned(spark, base, DYNAMIC["air_quality"])
    if a is None:
        return {"present": False}
    out = {"present": True, "per_day": per_day(a, ["station_id", "param_code"]), "availability": availability(a)}
    lo, hi = AIR_VALID_RANGE
    out["params"] = rows(a.groupBy("param_code").agg(
        F.first("param_name").alias("param_name"), F.first("unit").alias("unit"),
        F.countDistinct("station_id").alias("stations"), F.count("*").alias("rows"),
        F.min("value").alias("min"), F.avg("value").alias("mean"), F.max("value").alias("max"),
        F.percentile_approx("value", [0.05, 0.5, 0.95], 1000).alias("p05_p50_p95"),
        F.sum((~F.col("value").between(lo, hi)).cast("int")).alias(f"outside_{lo}_{hi}"),
    ).orderBy("param_code"))
    out["stations"] = rows(a.groupBy("station_name").agg(
        F.first("lat").alias("lat"), F.first("lon").alias("lon"),
        F.sort_array(F.collect_set("param_code")).alias("params")).orderBy("station_name"))
    return out


def describe_gtfs(spark, base):
    root = f"{base}/data/cleaned"
    versions = list_partition_values(spark, f"{root}/gtfs_trips", "feed_version")
    out = {"raw": inventory(spark, f"{base}/data/raw/gtfs"), "feed_versions": versions,
           "distinct_contents_sha12": sorted({v.rsplit("-", 1)[-1] for v in versions})}
    try:
        cov = read_feed_coverage(spark, base)
        out["coverage"] = {v: {"service_days": len(d), "first": min(d), "last": max(d)}
                           for v, d in sorted(cov.items()) if d}
    except Exception as exc:  # noqa: BLE001
        out["coverage"] = {"error": str(exc)[:300]}
    if not versions:
        return out
    latest = versions[-1]
    out["latest"] = {"feed_version": latest, "tables": {}}
    for t in GTFS_TABLES:
        p = f"{root}/gtfs_{t}/feed_version={latest}"
        _, fs, hp = _fs(spark, p)
        if fs.exists(hp):
            df = spark.read.parquet(p)
            out["latest"]["tables"][t] = {"rows": df.count(), "columns": df.columns}
    try:
        r = spark.read.parquet(f"{root}/gtfs_routes/feed_version={latest}")
        out["latest"]["routes_by_type"] = {str(x["route_type"]): x["n"] for x in
                                           r.groupBy("route_type").agg(F.count("*").alias("n")).collect()}
        st = spark.read.parquet(f"{root}/gtfs_stop_times/feed_version={latest}")
        out["latest"]["stop_times"] = st.agg(
            F.sum((F.col("departure_seconds") >= 86400).cast("int")).alias("after_midnight_rows"),
            F.max("departure_time").alias("max_departure_time"),
            F.sum(F.col("departure_seconds").isNull().cast("int")).alias("null_departure_seconds"),
        ).first().asDict()
        fi = spark.read.parquet(f"{root}/gtfs_feed_info/feed_version={latest}")
        out["latest"]["feed_info"] = rows(fi, 5)
    except Exception as exc:  # noqa: BLE001
        out["latest"]["error"] = str(exc)[:300]
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base", default=HDFS)
    p.add_argument("--raw-day", help="Raw date partition to sample (default: last complete day)")
    p.add_argument("--detail-days", type=int, default=2, help="GPS details on the last N complete days")
    p.add_argument("--skip-raw", action="store_true", help="Skip reading raw JSON (the slowest part)")
    p.add_argument("--no-lock", action="store_true")
    a = p.parse_args()
    base = a.base.rstrip("/")

    report = {"created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    with BatchLock(enabled=not a.no_lock):
        spark = build_spark("big-data-ztm-describe-sources", base)
        try:
            LOG.info("HDFS inventory")
            report["hdfs"] = {
                "raw": {n: inventory(spark, f"{base}/data/raw/{n}") for n in DYNAMIC},
                "cleaned": {n: dir_size(spark, f"{base}/data/cleaned/{n}") for n in DYNAMIC.values()},
                "enriched": {n: dir_size(spark, f"{base}/data/enriched/{n}")
                             for n in ["gtfs_trip_services", "stop_visits", "ml_dataset", "schedule_stops"]},
            }
            gps_days = list_partition_values(spark, f"{base}/data/cleaned/gps_observations", "date")
            complete = gps_days[1:-1] if len(gps_days) > 2 else gps_days  # first/last day are partial
            detail = complete[-a.detail_days:]
            raw_days = sorted(report["hdfs"]["raw"]["gps"].get("per_day", {}))
            raw_day = a.raw_day or (raw_days[-2] if len(raw_days) >= 2 else (raw_days or [None])[-1])
            report["params"] = {"raw_day": raw_day, "detail_days": detail, "cleaned_gps_days": gps_days}

            if not a.skip_raw and raw_day:
                report["raw_sample"] = {}
                for n in DYNAMIC:
                    LOG.info("Raw sample %s %s", n, raw_day)
                    report["raw_sample"][n] = raw_sample(spark, base, n, raw_day)
            LOG.info("Cleaned GPS")
            report["gps"] = describe_gps(spark, base, detail)
            LOG.info("Cleaned weather")
            report["weather"] = describe_weather(spark, base)
            LOG.info("Cleaned air quality")
            report["air_quality"] = describe_air(spark, base)
            LOG.info("GTFS")
            report["gtfs"] = describe_gtfs(spark, base)
        finally:
            spark.stop()
    print(json.dumps(report, indent=2, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
