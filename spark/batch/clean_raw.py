"""Incrementally clean archived GPS, weather and air-quality JSON from HDFS.

Pipeline contract
-----------------
* NiFi archives source JSON in HDFS. This script does not ingest live sources.
* A manifest remembers immutable raw JSON files already processed.
* Only new raw files are read and only affected UTC event-date partitions change.
* New observations are merged with existing cleaned observations by business key.
* Each replacement is staged separately, then installed using HDFS renames and backups.
* The manifest is committed only after all affected partitions were installed.

Run with: bash scripts/clean-batch.sh [all|gps|weather|air_quality]
"""

from __future__ import annotations

import argparse
import logging
import os
import time
import uuid

from incremental_hdfs import (
    STATE,
    hadoop_fs,
    list_archived_json,
    path,
    read_manifest,
    recover_backups,
    replace_partition,
    save_manifest,
)

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("ztm-spark-batch")

HDFS = os.getenv("HDFS_BASE", "hdfs://namenode:8020").rstrip("/")
WEATHER_FIELDS = (
    "temp_pow",
    "temp_naw",
    "wilg_pow",
    "pred_wiatru",
    "kier_wiatru",
    "int_opadu",
)


def structure(fields):
    return T.StructType([T.StructField(name, typ, True) for name, typ in fields])


GPS_RECORD = structure([
    ("id", T.StringType()),
    ("trip_id", T.StringType()),
    ("side_number", T.StringType()),
    ("service_date", T.StringType()),
    ("timestamp", T.StringType()),
    ("lat", T.DoubleType()),
    ("lon", T.DoubleType()),
    ("bearing", T.DoubleType()),
])
GPS_SNAPSHOT = structure([
    ("time", T.StringType()),
    ("positions", T.ArrayType(GPS_RECORD)),
])

WEATHER_RECORD = structure([
    ("lokalizacja", T.StringType()),
    ("czas", T.StringType()),
    ("lat", T.DoubleType()),
    ("lon", T.DoubleType()),
    *((field, T.DoubleType()) for field in WEATHER_FIELDS),
])
WEATHER_SNAPSHOT = structure([("stations", T.ArrayType(WEATHER_RECORD))])

AIR_READING = structure([
    ("param_code", T.StringType()),
    ("param_name", T.StringType()),
    ("value", T.DoubleType()),
    ("time", T.StringType()),
    ("unit", T.StringType()),
])
AIR_STATION = structure([
    ("name", T.StringType()),
    ("lat", T.DoubleType()),
    ("lon", T.DoubleType()),
    ("data", T.ArrayType(AIR_READING)),
])
AIR_SNAPSHOT = structure([("stations", T.ArrayType(AIR_STATION))])

SCHEMAS = {
    "gps": GPS_SNAPSHOT,
    "weather": WEATHER_SNAPSHOT,
    "air_quality": AIR_SNAPSHOT,
}
OUTPUTS = {
    "gps": "gps_observations",
    "weather": "weather_observations",
    "air_quality": "air_quality_observations",
}
DEDUP_KEYS = {
    "gps": ["trip_id", "side_number", "event_time_utc"],
    "weather": ["station_id", "event_time_utc"],
    "air_quality": ["station_id", "param_code", "event_time_utc"],
}


def nonempty(value):
    return value.isNotNull() & (F.length(F.trim(value)) > 0)


def valid_coordinates(frame: DataFrame) -> DataFrame:
    return frame.filter(
        F.col("lat").between(-90, 90)
        & F.col("lon").between(-180, 180)
        & ~F.isnan("lat")
        & ~F.isnan("lon")
    )


def local_warsaw_to_utc(value):
    """Assume naive source timestamps use Europe/Warsaw; Spark runs in UTC.

    Local timestamps near DST changes remain ambiguous without a source offset.
    """
    return F.to_utc_timestamp(
        F.to_timestamp(value, "yyyy-MM-dd HH:mm:ss"), "Europe/Warsaw"
    )


def normalized_station(name):
    # Provisional identifier: a station master is needed for renames/collisions.
    return F.sha2(F.lower(F.trim(name)), 256)


def deterministic_dedup(frame: DataFrame, keys: list[str]) -> DataFrame:
    """Pick one record per business key, preferring later raw filenames.

    For equal filenames, a hash of the full row breaks ties consistently.
    Keep this behavior identical for incoming and merged data.
    """
    tie = F.sha2(
        F.to_json(F.struct(*[F.col(column) for column in frame.columns])), 256
    )
    window = Window.partitionBy(*keys).orderBy(
        F.col("source_file").desc(), tie.desc()
    )
    return (
        frame.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def clean_gps(frame: DataFrame) -> DataFrame:
    records = frame.select(
        F.input_file_name().alias("source_file"),
        F.explode("positions").alias("p"),
    )
    result = records.select(
        F.trim("p.id").alias("record_id"),
        F.trim("p.trip_id").alias("trip_id"),
        F.trim("p.side_number").alias("side_number"),
        F.coalesce(
            F.to_date("p.service_date", "yyyyMMdd"),
            F.to_date("p.service_date", "yyyy-MM-dd"),
        ).alias("service_date"),
        F.to_timestamp("p.timestamp").alias("event_time_utc"),
        F.lit(None).cast("timestamp").alias("ingested_at_utc"),
        "source_file",
        F.col("p.lat").alias("lat"),
        F.col("p.lon").alias("lon"),
        F.col("p.bearing").alias("bearing"),
    )
    # The raw GPS snapshots do not contain ingestion timestamps: do not invent one.
    result = valid_coordinates(result).filter(
        nonempty(F.col("record_id"))
        & nonempty(F.col("trip_id"))
        & nonempty(F.col("side_number"))
        & F.col("service_date").isNotNull()
        & F.col("event_time_utc").isNotNull()
    )
    result = result.withColumn(
        "bearing",
        F.when(
            F.col("bearing").between(0, 360) & ~F.isnan("bearing"),
            F.col("bearing"),
        ).otherwise(F.lit(None).cast("double")),
    )
    return deterministic_dedup(
        result, DEDUP_KEYS["gps"]
    ).withColumn("date", F.to_date("event_time_utc"))


def clean_weather(frame: DataFrame) -> DataFrame:
    records = frame.select(
        F.input_file_name().alias("source_file"),
        F.explode("stations").alias("s"),
    )
    name = F.trim(F.col("s.lokalizacja"))
    result = records.select(
        normalized_station(name).alias("station_id"),
        name.alias("station_name"),
        local_warsaw_to_utc(F.col("s.czas")).alias("event_time_utc"),
        F.lit(None).cast("timestamp").alias("ingested_at_utc"),
        "source_file",
        F.col("s.lat").alias("lat"),
        F.col("s.lon").alias("lon"),
        *(F.col(f"s.{field}").alias(field) for field in WEATHER_FIELDS),
    )
    result = valid_coordinates(result).filter(
        nonempty(F.col("station_name")) & F.col("event_time_utc").isNotNull()
    )
    for field in WEATHER_FIELDS:
        result = result.withColumn(
            field,
            F.when(
                F.isnan(F.col(field)), F.lit(None).cast("double")
            ).otherwise(F.col(field)),
        )
    return deterministic_dedup(
        result, DEDUP_KEYS["weather"]
    ).withColumn("date", F.to_date("event_time_utc"))


def clean_air_quality(frame: DataFrame) -> DataFrame:
    records = (
        frame.select(
            F.input_file_name().alias("source_file"),
            F.explode("stations").alias("s"),
        )
        .select(
            "source_file", "s.name", "s.lat", "s.lon",
            F.explode("s.data").alias("p"),
        )
    )
    name = F.trim(F.col("name"))
    result = records.select(
        normalized_station(name).alias("station_id"),
        name.alias("station_name"),
        F.trim(F.col("p.param_code")).alias("param_code"),
        F.col("p.param_name").alias("param_name"),
        local_warsaw_to_utc(F.col("p.time")).alias("event_time_utc"),
        F.lit(None).cast("timestamp").alias("ingested_at_utc"),
        "source_file", "lat", "lon",
        F.col("p.value").alias("value"),
        F.col("p.unit").alias("unit"),
    )
    result = valid_coordinates(result).filter(
        nonempty(F.col("station_name"))
        & nonempty(F.col("param_code"))
        & F.col("event_time_utc").isNotNull()
        & F.col("value").isNotNull()
        & ~F.isnan("value")
        & (F.col("value") >= 0)
    )
    return deterministic_dedup(
        result, DEDUP_KEYS["air_quality"]
    ).withColumn("date", F.to_date("event_time_utc"))


TRANSFORMS = {
    "gps": clean_gps,
    "weather": clean_weather,
    "air_quality": clean_air_quality,
}


def run_dataset(spark: SparkSession, name: str) -> bool:
    started = time.monotonic()
    raw = f"{HDFS}/data/raw/{name}"
    target = f"{HDFS}/data/cleaned/{OUTPUTS[name]}"
    jvm, fs = hadoop_fs(spark)

    # Restore a previous target if a process stopped during partition replacement.
    recover_backups(fs, jvm, name, target)

    current = list_archived_json(fs, jvm, raw)
    previous = read_manifest(fs, jvm, name)

    altered = [
        filename for filename, signature in previous.items()
        if filename in current and current[filename] != signature
    ]
    if altered:
        raise RuntimeError(
            f"Previously processed raw {name} JSON changed in place: "
            f"{altered[:3]}. Raw inputs must be append-only."
        )

    missing = [filename for filename in previous if filename not in current]
    if missing:
        raise RuntimeError(
            f"Previously processed raw {name} JSON disappeared: {missing[:3]}. "
            "Refusing to preserve stale derived records."
        )

    new_files = [filename for filename in current if filename not in previous]
    if not new_files:
        LOG.info("%s: no new raw JSON files; skipping transformation", name)
        return False

    LOG.info(
        "%s: processing %d new raw JSON files (of %d total)",
        name, len(new_files), len(current),
    )

    snapshots = (
        spark.read.schema(SCHEMAS[name])
        .option("multiLine", "true")
        .json(new_files)
    )
    fresh = TRANSFORMS[name](snapshots).persist(StorageLevel.MEMORY_AND_DISK)
    stage_root = f"{HDFS}{STATE}/staging/{name}/{uuid.uuid4().hex}"

    try:
        count = fresh.count()
        dates = [
            str(row["date"])
            for row in (
                fresh.select("date")
                .where(F.col("date").isNotNull())
                .distinct()
                .orderBy("date")
                .collect()
            )
        ]
        LOG.info("%s: %d valid new records, affected dates: %s", name, count, dates)

        for day in dates:
            started_day = time.monotonic()
            target_part = f"{target}/date={day}"
            staged_part = f"{stage_root}/date={day}"
            incoming = fresh.where(F.col("date") == F.lit(day)).drop("date")

            LOG.info("%s: checking existing partition date=%s", name, day)
            if fs.exists(path(jvm, target_part)):
                LOG.info("%s: reading existing Parquet metadata date=%s", name, day)
                existing = spark.read.parquet(target_part).drop("date")
                combined = existing.unionByName(incoming, allowMissingColumns=True)
                result = deterministic_dedup(combined, DEDUP_KEYS[name])
                LOG.info("%s: prepared merge plan date=%s", name, day)
            else:
                # Fresh data were deduplicated by TRANSFORMS[name].
                result = incoming
                LOG.info("%s: creating new partition date=%s", name, day)

            # This is the action that computes the union/dedup and writes Parquet.
            # Never overwrite the target while a DataFrame is reading that target.
            LOG.info("%s: starting staged Parquet write date=%s", name, day)
            (
                result.write.mode("overwrite")
                .option("compression", "snappy")
                .parquet(staged_part)
            )
            LOG.info(
                "%s: staged Parquet write completed date=%s (%.1fs)",
                name, day, time.monotonic() - started_day,
            )

            LOG.info("%s: replacing HDFS partition date=%s", name, day)
            replace_partition(
                fs, jvm,
                path(jvm, staged_part),
                path(jvm, target_part),
                path(jvm, f"{HDFS}{STATE}/backups/{name}/date={day}"),
            )
            LOG.info(
                "%s: refreshed cleaned partition date=%s (%.1fs)",
                name, day, time.monotonic() - started_day,
            )

        # Commit only after every affected partition has been installed.
        # Reprocessing new files after interruption is safe thanks to key-based merge.
        LOG.info("%s: saving raw-file manifest", name)
        save_manifest(fs, jvm, name, current)
        LOG.info("%s: completed successfully in %.1fs", name, time.monotonic() - started)
        return True
    finally:
        fresh.unpersist()
        # A unique staging root makes interrupted attempts non-conflicting.
        fs.delete(path(jvm, stage_root), True)


def main(dataset: str) -> None:
    # An experimental workaround for stalls with pending partitions and no
    # active tasks. Setting locality wait to zero does not change the data.
    # It is configurable so the scheduling setting can be adjusted.
    locality_wait = os.getenv("SPARK_LOCALITY_WAIT", "0s")

    spark = (
        SparkSession.builder
        .appName("big-data-ztm-raw-batch-cleaning")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.locality.wait", locality_wait)
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    LOG.info("Spark started: master=local[2], locality.wait=%s", locality_wait)

    if os.getenv("SPARK_SCHEDULER_DEBUG", "0") == "1":
        # Only use this when investigating stalled Spark scheduling.
        jvm = spark._jvm
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            "org.apache.spark.scheduler",
            jvm.org.apache.logging.log4j.Level.DEBUG,
        )

    try:
        names = list(TRANSFORMS) if dataset == "all" else [dataset]
        for name in names:
            try:
                run_dataset(spark, name)
            except Exception:
                LOG.exception("%s: batch cleaning failed", name)
                raise
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["all", *TRANSFORMS], default="all")
    args = parser.parse_args()
    main(args.dataset)
