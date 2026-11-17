"""Feature and target engineering for the delay-prediction dataset.

LEAKAGE RULES (a row describes the situation at its own event time `t`)
-----------------------------------------------------------------------
* motion features use pings with time <= t
* `last_delay_*` use only stop visits with time <= t - confirm_lag_s
  (a visit is only recognised a few seconds after the closest approach)
* weather / air values come from the observation of the minute that contains t
* the TARGET uses the first stop visit with time >= t + horizon_s of the same
  vehicle and trip; it is never used as a feature
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from delay_estimation import TRIP_KEY
from ml_common import (
    BBOX_LAT,
    BBOX_LON,
    LOOKUP_CELL_DEG,
    WARSAW_TZ,
    haversine_m,
)

MAX_SPEED_KMH = 120.0        # faster than this between two pings = GPS glitch
MAX_SPEED_GAP_S = 180        # pings further apart than this give no speed

# cleaned column -> ML column
WEATHER_RENAME = {
    "temp_pow": "temp_air",
    "temp_naw": "temp_road",
    "wilg_pow": "humidity",
    "pred_wiatru": "wind_speed",
    "kier_wiatru": "wind_dir",
    "int_opadu": "precip_intensity",
}
# Physically plausible ranges; anything outside becomes NULL (sensor glitch).
WEATHER_VALID_RANGE = {
    "temp_air": (-50.0, 60.0),
    "temp_road": (-50.0, 90.0),
    "humidity": (0.0, 100.0),
    "wind_speed": (0.0, 60.0),
    "wind_dir": (0.0, 360.0),
    "precip_intensity": (0.0, 500.0),
}
WEATHER_FEATURES = list(WEATHER_RENAME.values())
AIR_VALID_RANGE = (0.0, 5000.0)


# --------------------------------------------------------------------------- #
# Motion
# --------------------------------------------------------------------------- #
def add_motion_features(pings: DataFrame) -> DataFrame:
    """speed_kmh (from the previous ping) and speed_avg_5min_kmh (mean of the last 5 min)."""
    ordered = Window.partitionBy(*TRIP_KEY).orderBy("event_epoch")
    dt = F.col("event_epoch") - F.lag("event_epoch").over(ordered)
    distance = haversine_m(
        F.lag("lat").over(ordered), F.lag("lon").over(ordered), F.col("lat"), F.col("lon")
    )
    raw_speed = distance / dt * 3.6
    speed = F.when(
        (dt >= 1) & (dt <= MAX_SPEED_GAP_S) & (raw_speed <= MAX_SPEED_KMH), raw_speed
    )
    return pings.withColumn("speed_kmh", speed).withColumn(
        "speed_avg_5min_kmh", F.avg("speed_kmh").over(ordered.rangeBetween(-300, 0))
    )


# --------------------------------------------------------------------------- #
# Delay context (past information) and target (future information)
# --------------------------------------------------------------------------- #
def add_delay_change(visits: DataFrame) -> DataFrame:
    """delay_change_s = delay at this visit minus delay at the previous visit of the trip."""
    ordered = Window.partitionBy(*TRIP_KEY).orderBy("visit_epoch")
    return visits.withColumn(
        "delay_change_s", F.col("delay_s") - F.lag("delay_s").over(ordered)
    )


def attach_visit_context(
    pings: DataFrame,
    visits: DataFrame,
    horizon_s: int,
    tolerance_s: int,
    confirm_lag_s: int,
) -> DataFrame:
    """Add last_delay_* (past) and target_* (future) columns to full-rate pings.

    Must run on the FULL-RATE pings (before down-sampling) because the visit is
    marked on the exact ping where the closest approach happened.
    """
    marker = visits.select(
        *TRIP_KEY,
        F.col("visit_epoch").alias("event_epoch"),
        F.col("stop_sequence").alias("v_seq"),
        F.col("delay_s").alias("v_delay"),
        F.col("delay_change_s").alias("v_change"),
    )
    joined = pings.join(marker, [*TRIP_KEY, "event_epoch"], "left").withColumn(
        "_visit",
        F.when(
            F.col("v_seq").isNotNull(),
            F.struct(
                F.col("event_epoch").alias("t"),
                F.col("v_delay").alias("delay_s"),
                F.col("v_change").alias("change_s"),
                F.col("v_seq").alias("seq"),
            ),
        ),
    )
    ordered = Window.partitionBy(*TRIP_KEY).orderBy("event_epoch")
    past = F.last("_visit", ignorenulls=True).over(
        ordered.rangeBetween(Window.unboundedPreceding, -confirm_lag_s)
    )
    future = F.first("_visit", ignorenulls=True).over(
        ordered.rangeBetween(horizon_s, Window.unboundedFollowing)
    )
    target_ok = (F.col("_next.t") - F.col("event_epoch")) <= (horizon_s + tolerance_s)
    return (
        joined.withColumn("_last", past)
        .withColumn("_next", future)
        .withColumn("last_delay_s", F.col("_last.delay_s"))
        .withColumn("last_delay_age_s", F.col("event_epoch") - F.col("_last.t"))
        .withColumn("delay_change_s", F.col("_last.change_s"))
        .withColumn("last_stop_sequence", F.col("_last.seq"))
        .withColumn("target_delay_s", F.when(target_ok, F.col("_next.delay_s")))
        .withColumn(
            "target_horizon_s", F.when(target_ok, F.col("_next.t") - F.col("event_epoch"))
        )
        .withColumn("has_target", F.col("target_delay_s").isNotNull())
        .withColumn(
            "trip_progress",
            F.when(
                F.col("trip_last_sequence") > 0,
                F.col("last_stop_sequence") / F.col("trip_last_sequence"),
            ),
        )
        .drop("_visit", "_last", "_next", "v_seq", "v_delay", "v_change")
    )


def downsample(pings: DataFrame, interval_s: int) -> DataFrame:
    """Keep the first ping of every `interval_s` window per vehicle trip."""
    if interval_s <= 1:
        return pings
    bucket = F.floor(F.col("event_epoch") / interval_s).cast("long")
    first = Window.partitionBy(*TRIP_KEY, "_bucket").orderBy("event_epoch")
    return (
        pings.withColumn("_bucket", bucket)
        .withColumn("_rn", F.row_number().over(first))
        .filter(F.col("_rn") == 1)
        .drop("_bucket", "_rn")
    )


# --------------------------------------------------------------------------- #
# Calendar features (Warsaw local time)
# --------------------------------------------------------------------------- #
def add_time_features(df: DataFrame) -> DataFrame:
    local = F.from_utc_timestamp(F.col("event_time_utc"), WARSAW_TZ)
    hour = F.hour(local)
    dow = (F.dayofweek(local) + 5) % 7  # Monday = 0 ... Sunday = 6
    weekend = dow >= 5
    rush = (~weekend) & (hour.between(7, 9) | hour.between(15, 18))
    return (
        df.withColumn("hour_local", hour)
        .withColumn("day_of_week", dow)
        .withColumn("is_weekend", weekend)
        .withColumn("is_rush_hour", rush)
    )


# --------------------------------------------------------------------------- #
# Environment joins: nearest station in space, observation of the same minute
# --------------------------------------------------------------------------- #
def air_column_name(param_code: str) -> str:
    """'PM2.5' -> 'air_pm2_5'"""
    return "air_" + re.sub(r"[^a-z0-9]+", "_", param_code.lower()).strip("_")


def add_lookup_keys(df: DataFrame) -> DataFrame:
    """lk_y / lk_x (spatial cell) and minute (time bucket) used by the lookups."""
    return (
        df.withColumn("lk_y", F.floor(F.col("lat") / LOOKUP_CELL_DEG).cast("int"))
        .withColumn("lk_x", F.floor(F.col("lon") / LOOKUP_CELL_DEG).cast("int"))
        .withColumn("minute", F.floor(F.col("event_epoch") / 60).cast("long") * 60)
    )


def _lookup_grid(spark: SparkSession) -> DataFrame:
    """Every lookup cell inside the Warsaw bounding box, with its centre point."""
    import math

    y0 = math.floor(BBOX_LAT[0] / LOOKUP_CELL_DEG) - 1
    y1 = math.floor(BBOX_LAT[1] / LOOKUP_CELL_DEG) + 1
    x0 = math.floor(BBOX_LON[0] / LOOKUP_CELL_DEG) - 1
    x1 = math.floor(BBOX_LON[1] / LOOKUP_CELL_DEG) + 1
    ys = spark.range(y0, y1 + 1).select(F.col("id").cast("int").alias("lk_y"))
    xs = spark.range(x0, x1 + 1).select(F.col("id").cast("int").alias("lk_x"))
    return ys.crossJoin(xs).select(
        "lk_y", "lk_x",
        ((F.col("lk_y") + 0.5) * LOOKUP_CELL_DEG).alias("cell_lat"),
        ((F.col("lk_x") + 0.5) * LOOKUP_CELL_DEG).alias("cell_lon"),
    )


def nearest_station_lookup(
    spark: SparkSession, stations: DataFrame, id_column: str, dist_column: str
) -> DataFrame:
    """For every lookup cell, the nearest station and its distance.

    stations: station_id, s_lat, s_lon. Output columns: lk_y, lk_x, <id_column>,
    <dist_column>.
    """
    paired = _lookup_grid(spark).crossJoin(F.broadcast(stations)).withColumn(
        "d", haversine_m(F.col("cell_lat"), F.col("cell_lon"), F.col("s_lat"), F.col("s_lon"))
    )
    return (
        paired.groupBy("lk_y", "lk_x")
        .agg(F.min(F.struct("d", "station_id")).alias("best"))
        .select(
            "lk_y", "lk_x",
            F.col("best.station_id").alias(id_column),
            F.col("best.d").alias(dist_column),
        )
    )


def sanitize_weather(weather: DataFrame) -> DataFrame:
    """Rename cleaned weather columns and null out implausible values."""
    out = weather.select(
        "station_id", "lat", "lon",
        F.col("event_time_utc").cast("long").alias("event_epoch"),
        *[F.col(src).alias(dst) for src, dst in WEATHER_RENAME.items()],
    )
    for name, (low, high) in WEATHER_VALID_RANGE.items():
        out = out.withColumn(name, F.when(F.col(name).between(low, high), F.col(name)))
    return out


def join_weather(spark: SparkSession, pings: DataFrame, weather: DataFrame) -> DataFrame:
    """Attach weather of the nearest station for the minute of each ping.

    `pings` must already carry lk_y, lk_x, minute (see add_lookup_keys).
    """
    clean = sanitize_weather(weather)
    stations = clean.groupBy("station_id").agg(
        F.avg("lat").alias("s_lat"), F.avg("lon").alias("s_lon")
    )
    nearest = nearest_station_lookup(
        spark, stations, "weather_station_id", "weather_station_dist_m"
    )
    # First observation inside each minute: never later than the ping itself
    # when the source is sampled on a 30 s grid.
    minutes = (
        clean.withColumn("minute", F.floor(F.col("event_epoch") / 60).cast("long") * 60)
        .groupBy("station_id", "minute")
        .agg(*[F.min_by(c, "event_epoch").alias(c) for c in WEATHER_FEATURES])
        .withColumnRenamed("station_id", "weather_station_id")
    )
    joined = pings.join(F.broadcast(nearest), ["lk_y", "lk_x"], "left").join(
        F.broadcast(minutes), ["weather_station_id", "minute"], "left"
    )
    all_null = F.lit(True)
    for column in WEATHER_FEATURES:
        all_null = all_null & F.col(column).isNull()
    wind_rad = F.radians(F.col("wind_dir"))
    return (
        joined.withColumn("weather_missing", all_null)
        .withColumn("wind_dir_sin", F.sin(wind_rad))
        .withColumn("wind_dir_cos", F.cos(wind_rad))
        .withColumn(
            "is_raining",
            F.when(F.col("precip_intensity").isNull(), F.lit(None).cast("boolean"))
            .otherwise(F.col("precip_intensity") > 0),
        )
    )


def join_air(
    spark: SparkSession, pings: DataFrame, air: DataFrame, params: tuple[str, ...] | list[str]
) -> DataFrame:
    """For each pollutant: value from the nearest station that measures it."""
    out = pings
    low, high = AIR_VALID_RANGE
    value_columns = []
    for code in params:
        name = air_column_name(code)
        value_columns.append(name)
        rows = (
            air.filter(F.upper(F.trim("param_code")) == code.upper())
            .withColumn("event_epoch", F.col("event_time_utc").cast("long"))
            # temperature ("T") may be negative; concentrations may not.
            .filter(
                F.col("value").between(low, high)
                | ((F.upper(F.lit(code)) == "T") & F.col("value").isNotNull())
            )
        )
        stations = rows.groupBy("station_id").agg(
            F.avg("lat").alias("s_lat"), F.avg("lon").alias("s_lon")
        )
        nearest = nearest_station_lookup(
            spark, stations, f"{name}_station_id", f"{name}_dist_m"
        )
        minutes = (
            rows.withColumn("minute", F.floor(F.col("event_epoch") / 60).cast("long") * 60)
            .groupBy("station_id", "minute")
            .agg(F.min_by("value", "event_epoch").alias(name))
            .withColumnRenamed("station_id", f"{name}_station_id")
        )
        out = out.join(F.broadcast(nearest), ["lk_y", "lk_x"], "left").join(
            F.broadcast(minutes), [f"{name}_station_id", "minute"], "left"
        )
    missing = F.lit(True)
    for column in value_columns:
        missing = missing & F.col(column).isNull()
    return out.withColumn("air_missing", missing if value_columns else F.lit(True))


# --------------------------------------------------------------------------- #
# Final column layout
# --------------------------------------------------------------------------- #
def ml_columns(air_params: tuple[str, ...] | list[str]) -> list[str]:
    identity = [
        "date", "event_time_utc", "service_date", "trip_id", "side_number",
        "route_id", "route_short_name", "route_type", "direction_id", "lat", "lon",
    ]
    calendar = ["hour_local", "day_of_week", "is_weekend", "is_rush_hour"]
    motion = ["speed_kmh", "speed_avg_5min_kmh"]
    delay = [
        "last_delay_s", "last_delay_age_s", "delay_change_s",
        "last_stop_sequence", "trip_last_sequence", "trip_progress",
    ]
    weather = [
        *WEATHER_FEATURES, "wind_dir_sin", "wind_dir_cos", "is_raining",
        "weather_station_dist_m", "weather_missing",
    ]
    air = []
    for code in air_params:
        name = air_column_name(code)
        air += [name, f"{name}_dist_m"]
    air.append("air_missing")
    target = ["target_delay_s", "target_horizon_s", "has_target"]
    return identity + calendar + motion + delay + weather + air + target
