"""Build the delay-prediction dataset from the cleaned Parquet data.

Reads   /data/cleaned/{gps,weather,air_quality}_observations, gtfs_* tables
Writes  /data/enriched/stop_visits/date=...   one row per (trip, vehicle, stop) visit with its delay
        /data/enriched/ml_dataset/date=...    one row per vehicle per `--ping-interval-s`
                                              with features and the future target
        /data/enriched/_ml_reports/build_*.json   counts for the report

Run (inside the Spark container):
    bash scripts/build-ml-dataset.sh --start-date 2026-11-16 --end-date 2026-11-17

Re-running a date range replaces exactly those date partitions (idempotent).
See docs/ml-dataset.md for the definitions of every column.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass, field, replace

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from delay_estimation import (
    build_trip_stops,
    detect_stop_visits,
    pick_feed_versions,
    read_feed_coverage,
)
from feature_engineering import (
    WEATHER_RENAME,
    add_delay_change,
    add_lookup_keys,
    add_motion_features,
    add_time_features,
    attach_visit_context,
    downsample,
    join_air,
    join_weather,
    ml_columns,
)
from ml_common import (
    BBOX_LAT,
    BBOX_LON,
    DEFAULT_AIR_PARAMS,
    DEFAULT_DWELL_TOLERANCE_M,
    DEFAULT_CONFIRM_LAG_S,
    DEFAULT_HORIZON_S,
    DEFAULT_HORIZON_TOLERANCE_S,
    DEFAULT_MAX_ABS_DELAY_S,
    DEFAULT_PING_INTERVAL_S,
    DEFAULT_STOP_RADIUS_M,
    HDFS,
    BatchLock,
    build_spark,
    list_partition_values,
    next_day,
    write_text,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("ztm-ml")


@dataclass
class BuildConfig:
    base: str = HDFS
    start_date: str | None = None
    end_date: str | None = None
    stop_radius_m: float = DEFAULT_STOP_RADIUS_M
    max_abs_delay_s: int = DEFAULT_MAX_ABS_DELAY_S
    dwell_tolerance_m: float = DEFAULT_DWELL_TOLERANCE_M
    ping_interval_s: int = DEFAULT_PING_INTERVAL_S
    horizon_s: int = DEFAULT_HORIZON_S
    tolerance_s: int = DEFAULT_HORIZON_TOLERANCE_S
    confirm_lag_s: int = DEFAULT_CONFIRM_LAG_S
    air_params: tuple = field(default_factory=lambda: DEFAULT_AIR_PARAMS)
    feed_version: str | None = None


# --------------------------------------------------------------------------- #
def _read_days(spark: SparkSession, root: str, days: list[str]) -> DataFrame | None:
    """Read the given date=... partitions that exist; None if there are none."""
    existing = set(list_partition_values(spark, root, "date"))
    wanted = [d for d in days if d in existing]
    if not wanted:
        return None
    return spark.read.option("basePath", root).parquet(*[f"{root}/date={d}" for d in wanted])


def _empty_weather(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("station_id", T.StringType()),
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
            T.StructField("event_time_utc", T.TimestampType()),
            *[T.StructField(c, T.DoubleType()) for c in WEATHER_RENAME],
        ]
    )
    return spark.createDataFrame([], schema)


def _empty_air(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("station_id", T.StringType()),
            T.StructField("param_code", T.StringType()),
            T.StructField("lat", T.DoubleType()),
            T.StructField("lon", T.DoubleType()),
            T.StructField("value", T.DoubleType()),
            T.StructField("event_time_utc", T.TimestampType()),
        ]
    )
    return spark.createDataFrame([], schema)


def _feed_table(spark: SparkSession, base: str, table: str, versions: list[str]) -> DataFrame:
    """A cleaned GTFS table restricted to some feed versions (partition pruning)."""
    return spark.read.parquet(f"{base}/data/cleaned/{table}").filter(
        F.col("feed_version").isin(versions)
    )


# --------------------------------------------------------------------------- #
def build(spark: SparkSession, cfg: BuildConfig) -> dict:
    started = time.monotonic()
    base = cfg.base
    gps_root = f"{base}/data/cleaned/gps_observations"
    available = list_partition_values(spark, gps_root, "date")
    if not available:
        raise SystemExit(f"No cleaned GPS partitions under {gps_root}; run the cleaning batch first.")
    start = cfg.start_date or available[0]
    end = cfg.end_date or available[-1]
    target_days = [d for d in available if start <= d <= end]
    if not target_days:
        raise SystemExit(f"No cleaned GPS partitions between {start} and {end}. Available: {available}")
    # One extra day is read so targets near midnight can look into the future.
    read_days = [d for d in available if start <= d <= next_day(end)]
    LOG.info("Target dates %s..%s (%d partitions), reading %d", start, end, len(target_days), len(read_days))

    stats: dict = {"config": {**asdict(cfg), "air_params": list(cfg.air_params)},
                   "start_date": start, "end_date": end}

    # ---- 1. GPS pings --------------------------------------------------------
    gps = (
        spark.read.option("basePath", gps_root)
        .parquet(*[f"{gps_root}/date={d}" for d in read_days])
        .filter(
            F.col("lat").between(*BBOX_LAT)
            & F.col("lon").between(*BBOX_LON)
        )
        .select(
            "trip_id", "side_number", "service_date", "event_time_utc", "lat", "lon",
            F.col("event_time_utc").cast("long").alias("event_epoch"),
        )
        .withColumn("date", F.to_date("event_time_utc"))
    )

    # ---- 2. Which GTFS feed belongs to each service date --------------------
    service_days = sorted(
        r["d"] for r in gps.select(F.date_format("service_date", "yyyy-MM-dd").alias("d")).distinct().collect()
    )
    coverage = read_feed_coverage(spark, base)
    mapping = pick_feed_versions(coverage, service_days, cfg.feed_version)
    stats["service_dates"] = service_days
    stats["feed_by_service_date"] = mapping
    unmapped = [d for d in service_days if d not in mapping]
    if unmapped:
        LOG.warning("No GTFS feed covers service dates %s; those pings get no delay", unmapped)
    versions = sorted(set(mapping.values()))
    if not versions:
        raise SystemExit("No GTFS feed covers any GPS service date; check /data/cleaned/gtfs_calendar_dates.")

    mapping_df = spark.createDataFrame(
        [(d, v) for d, v in mapping.items()], "service_day string, feed_version string"
    ).withColumn("service_date", F.to_date("service_day")).drop("service_day")

    trips = _feed_table(spark, base, "gtfs_trips", versions).select(
        "feed_version", "trip_id", "route_id", "direction_id"
    )
    routes = _feed_table(spark, base, "gtfs_routes", versions).select(
        "feed_version", "route_id", "route_short_name", "route_type"
    ).dropDuplicates(["feed_version", "route_id"])
    trip_ref = (
        trips.dropDuplicates(["feed_version", "trip_id"])
        .join(routes, ["feed_version", "route_id"], "left")
    )
    pings = (
        gps.join(F.broadcast(mapping_df), "service_date", "left")
        .join(trip_ref, ["feed_version", "trip_id"], "left")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    row = pings.agg(
        F.count("*").alias("pings"),
        F.sum(F.col("feed_version").isNotNull().cast("int")).alias("with_feed"),
        F.sum(F.col("route_id").isNotNull().cast("int")).alias("with_trip_match"),
    ).first()
    stats["pings"] = {k: int(row[k] or 0) for k in ("pings", "with_feed", "with_trip_match")}
    LOG.info("Pings: %s", stats["pings"])
    if stats["pings"]["with_trip_match"] == 0:
        raise SystemExit(
            "0 GPS pings match a GTFS trip_id. Run profile_data.py and compare the trip_id formats."
        )

    matched = add_motion_features(pings.filter(F.col("route_id").isNotNull())).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    # ---- 3. Stop visits and delays ------------------------------------------
    seen_trips = matched.select("feed_version", "trip_id").distinct()
    stop_times = _feed_table(spark, base, "gtfs_stop_times", versions).join(
        F.broadcast(seen_trips), ["feed_version", "trip_id"], "left_semi"
    )
    stops = _feed_table(spark, base, "gtfs_stops", versions)
    trip_stops = build_trip_stops(stop_times, stops)
    trip_info = trip_stops.groupBy("feed_version", "trip_id").agg(
        F.max("stop_sequence").alias("trip_last_sequence")
    )

    visits = add_delay_change(
        detect_stop_visits(matched, trip_stops, cfg.stop_radius_m, cfg.max_abs_delay_s,
                           cfg.dwell_tolerance_m)
    ).join(
        trip_ref.select("feed_version", "trip_id", "route_id", "route_short_name", "route_type"),
        ["feed_version", "trip_id"], "left",
    ).persist(StorageLevel.MEMORY_AND_DISK)

    in_range = F.col("date").between(F.lit(start).cast("date"), F.lit(end).cast("date"))
    visits_out = visits.filter(in_range).select(
        "date", "trip_id", "service_date", "side_number", "stop_sequence", "stop_id",
        "route_id", "route_short_name", "route_type", "feed_version",
        "visit_time_utc", "scheduled_time_utc", "delay_s", "delay_change_s", "distance_m",
    )
    vrow = visits_out.agg(
        F.count("*").alias("visits"),
        F.avg("delay_s").alias("mean_delay_s"),
        F.expr("percentile_approx(delay_s, 0.5)").alias("median_delay_s"),
        F.expr("percentile_approx(abs(delay_s), 0.95)").alias("p95_abs_delay_s"),
    ).first()
    stats["stop_visits"] = {k: (None if vrow[k] is None else float(vrow[k])) for k in vrow.asDict()}
    LOG.info("Stop visits: %s", stats["stop_visits"])
    (visits_out.write.mode("overwrite").partitionBy("date")
     .parquet(f"{base}/data/enriched/stop_visits"))

    # ---- 4. ML rows: motion + delay context + target (full-rate), then sample
    annotated = attach_visit_context(
        matched.join(F.broadcast(trip_info), ["feed_version", "trip_id"], "left"),
        visits, cfg.horizon_s, cfg.tolerance_s, cfg.confirm_lag_s,
    ).filter(in_range)
    sampled = add_lookup_keys(downsample(annotated, cfg.ping_interval_s))

    weather = _read_days(spark, f"{base}/data/cleaned/weather_observations", target_days)
    air = _read_days(spark, f"{base}/data/cleaned/air_quality_observations", target_days)
    if weather is None:
        LOG.warning("No cleaned weather data for %s; weather features will be NULL", target_days)
    if air is None:
        LOG.warning("No cleaned air-quality data for %s; air features will be NULL", target_days)
    enriched = join_weather(spark, sampled, weather if weather is not None else _empty_weather(spark))
    enriched = join_air(spark, enriched, air if air is not None else _empty_air(spark), cfg.air_params)
    final = add_time_features(enriched).select(*ml_columns(cfg.air_params)).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    air_nulls = [F.avg(F.col(c).isNull().cast("double")).alias(f"null_{c}")
                 for c in ml_columns(cfg.air_params) if c.startswith("air_") and not c.endswith(("_dist_m", "missing"))]
    mrow = final.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("has_target").cast("int")).alias("rows_with_target"),
        F.sum(F.col("last_delay_s").isNotNull().cast("int")).alias("rows_with_last_delay"),
        F.avg(F.col("weather_missing").cast("double")).alias("share_weather_missing"),
        F.avg(F.col("air_missing").cast("double")).alias("share_air_missing"),
        # Baseline "the delay will not change": the error the ML model has to beat.
        F.avg(F.abs(F.col("target_delay_s") - F.col("last_delay_s"))).alias("baseline_mae_no_change_s"),
        F.avg(F.abs(F.col("target_delay_s"))).alias("baseline_mae_predict_zero_s"),
        *air_nulls,
    ).first()
    stats["ml_dataset"] = {k: (None if v is None else float(v)) for k, v in mrow.asDict().items()}
    LOG.info("ML dataset: %s", stats["ml_dataset"])
    (final.write.mode("overwrite").partitionBy("date")
     .parquet(f"{base}/data/enriched/ml_dataset"))

    for frame in (final, visits, matched, pings):
        frame.unpersist()

    stats["seconds"] = round(time.monotonic() - started, 1)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    write_text(spark, f"{base}/data/enriched/_ml_reports/build_{stamp}.json",
               json.dumps(stats, indent=2, default=str))
    return stats


# --------------------------------------------------------------------------- #
def parse_args(argv=None) -> tuple[BuildConfig, argparse.Namespace]:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base", default=HDFS, help="Filesystem root (default hdfs://namenode:8020)")
    p.add_argument("--start-date", help="first UTC event date YYYY-MM-DD (default: earliest available)")
    p.add_argument("--end-date", help="last UTC event date YYYY-MM-DD (default: latest available)")
    p.add_argument("--stop-radius-m", type=float, default=DEFAULT_STOP_RADIUS_M)
    p.add_argument("--max-abs-delay-s", type=int, default=DEFAULT_MAX_ABS_DELAY_S)
    p.add_argument("--dwell-tolerance-m", type=float, default=DEFAULT_DWELL_TOLERANCE_M,
                   help="pings this close to the closest approach count as standing at the stop")
    p.add_argument("--ping-interval-s", type=int, default=DEFAULT_PING_INTERVAL_S,
                   help="keep one ML row per vehicle per this many seconds")
    p.add_argument("--horizon-min", type=float, default=DEFAULT_HORIZON_S / 60,
                   help="prediction horizon in minutes")
    p.add_argument("--horizon-tolerance-s", type=int, default=DEFAULT_HORIZON_TOLERANCE_S)
    p.add_argument("--confirm-lag-s", type=int, default=DEFAULT_CONFIRM_LAG_S)
    p.add_argument("--air-params", default=",".join(DEFAULT_AIR_PARAMS),
                   help="comma separated pollutant codes, e.g. PM10,PM2.5,NO2")
    p.add_argument("--feed-version", help="force one GTFS feed_version for all dates")
    p.add_argument("--no-lock", action="store_true", help="do not wait for the cleaning batch lock")
    p.add_argument("--lock-timeout-min", type=int, default=45)
    a = p.parse_args(argv)
    cfg = BuildConfig(
        base=a.base.rstrip("/"), start_date=a.start_date, end_date=a.end_date,
        stop_radius_m=a.stop_radius_m, max_abs_delay_s=a.max_abs_delay_s,
        dwell_tolerance_m=a.dwell_tolerance_m,
        ping_interval_s=a.ping_interval_s, horizon_s=int(a.horizon_min * 60),
        tolerance_s=a.horizon_tolerance_s, confirm_lag_s=a.confirm_lag_s,
        air_params=tuple(x.strip() for x in a.air_params.split(",") if x.strip()),
        feed_version=a.feed_version,
    )
    return cfg, a


def main() -> None:
    cfg, args = parse_args()
    with BatchLock(timeout_s=args.lock_timeout_min * 60, enabled=not args.no_lock):
        spark = build_spark("big-data-ztm-ml-dataset", cfg.base)
        try:
            available = list_partition_values(spark, f"{cfg.base}/data/cleaned/gps_observations", "date")
            start = cfg.start_date or (available[0] if available else "")
            end = cfg.end_date or (available[-1] if available else "")
            days = [d for d in available if start <= d <= end]
            if not days:
                raise SystemExit(f"No cleaned GPS partitions between {start} and {end}. Available: {available}")
            # One day at a time, so the job fits in the container memory limit
            all_stats = {}
            for day in days:
                LOG.info("Building day %s (%d/%d)", day, days.index(day) + 1, len(days))
                try:
                    all_stats[day] = build(spark, replace(cfg, start_date=day, end_date=day))
                except SystemExit as exc:
                    # e.g. no GTFS feed for that day; skip it, the other days are still useful
                    LOG.warning("Day %s not built: %s", day, exc)
                    all_stats[day] = {"error": str(exc)}
        finally:
            spark.stop()
    print(json.dumps(all_stats, indent=2, default=str))


if __name__ == "__main__":
    main()
