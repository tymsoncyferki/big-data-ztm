"""Clean Warsaw GTFS ZIP files from HDFS into versioned Snappy Parquet.

Each supported GTFS source file is written to
/data/cleaned/gtfs_<name>/feed_version=<date>-<sha12>.
The archive is copied to temporary local disk and CSV files are processed
one at a time. Spark runs in local mode.
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path
import re
import shutil
import sys
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from pyspark.sql import SparkSession, functions as F

LOG = logging.getLogger("gtfs-batch")
HDFS = os.getenv("HDFS_BASE", "hdfs://namenode:8020").rstrip("/")

# Field order is explicit so raw feed-specific extras cannot silently change
# the interface consumed by downstream jobs. Text identifiers remain strings:
# e.g. service_id='2026-11-15:NdS', trip_id contains ':', stop_id may have zeroes.
TABLES = {
    "agency": [
        ("agency_id", "string"), ("agency_name", "string"),
        ("agency_url", "string"), ("agency_timezone", "string"),
        ("agency_lang", "string"), ("agency_phone", "string"),
    ],
    "attributions": [
        ("attribution_id", "string"), ("organization_name", "string"),
        ("attribution_url", "string"), ("is_producer", "int"),
        ("is_operator", "int"), ("is_authority", "int"),
        ("is_data_source", "int"),
    ],
    "feed_info": [
        ("feed_publisher_name", "string"), ("feed_publisher_url", "string"),
        ("feed_lang", "string"), ("feed_start_date", "date"),
        ("feed_end_date", "date"), ("feed_version", "string"),
    ],
    "routes": [
        ("route_id", "string"), ("agency_id", "string"),
        ("route_short_name", "string"), ("route_long_name", "string"),
        ("route_type", "int"), ("route_color", "string"),
        ("route_text_color", "string"),
    ],
    "stops": [
        ("stop_id", "string"), ("stop_name", "string"),
        ("stop_code", "string"), ("platform_code", "string"),
        ("stop_lat", "double"), ("stop_lon", "double"),
        ("location_type", "int"), ("parent_station", "string"),
        ("wheelchair_boarding", "int"), ("zone_id", "string"),
        ("stop_name_stem", "string"), ("town_name", "string"),
        ("street_name", "string"),
    ],
    "trips": [
        ("trip_id", "string"), ("route_id", "string"),
        ("service_id", "string"), ("shape_id", "string"),
        ("trip_short_name", "string"), ("trip_headsign", "string"),
        ("direction_id", "int"), ("exceptional", "int"),
        ("wheelchair_accessible", "int"), ("block_id", "string"),
        ("block_short_name", "string"), ("variant_code", "string"),
        ("fleet_type", "string"),
    ],
    "stop_times": [
        ("trip_id", "string"), ("stop_sequence", "int"),
        ("stop_id", "string"), ("arrival_time", "string"),
        ("departure_time", "string"), ("pickup_type", "int"),
        ("drop_off_type", "int"), ("shape_dist_traveled", "double"),
    ],
    "calendar_dates": [
        ("service_id", "string"), ("date", "date"),
        ("exception_type", "int"),
    ],
    "frequencies": [
        ("trip_id", "string"), ("start_time", "string"),
        ("end_time", "string"), ("headway_secs", "int"),
        ("exact_times", "int"),
    ],
    "shapes": [
        ("shape_id", "string"), ("shape_pt_sequence", "int"),
        ("shape_pt_lat", "double"), ("shape_pt_lon", "double"),
        ("shape_dist_traveled", "double"),
    ],
    "fare_attributes": [
        ("fare_id", "string"), ("price", "double"),
        ("currency_type", "string"), ("payment_method", "int"),
        ("transfers", "int"), ("agency_id", "string"),
        ("transfer_duration", "int"),
    ],
    "fare_rules": [
        ("fare_id", "string"), ("route_id", "string"),
        ("origin_id", "string"), ("destination_id", "string"),
        ("contains_id", "string"),
    ],
}

# Required inputs for cleaning and the trip/service enrichment stage.
REQUIRED = {"routes", "stops", "trips", "stop_times", "calendar_dates"}
KEYS = {
    "routes": ("route_id",),
    "stops": ("stop_id",),
    "trips": ("trip_id", "route_id", "service_id"),
    "stop_times": ("trip_id", "stop_sequence", "stop_id"),
    "calendar_dates": ("service_id", "date"),
    "frequencies": ("trip_id", "start_time", "end_time"),
    "shapes": ("shape_id", "shape_pt_sequence"),
    "fare_attributes": ("fare_id",),
    "fare_rules": ("fare_id",),
    "agency": ("agency_name",),
    "attributions": ("organization_name",),
    "feed_info": ("feed_publisher_name",),
}


def seconds_since_start(column):
    """GTFS service-day seconds; hours 24+ are valid and never UTC times."""
    value = F.col(column)
    chunks = F.split(value, ":")
    return F.when(
        value.rlike(r"^\d{1,3}:[0-5]\d:[0-5]\d$"),
        chunks.getItem(0).cast("int") * 3600
        + chunks.getItem(1).cast("int") * 60
        + chunks.getItem(2).cast("int"),
    ).otherwise(F.lit(None).cast("int"))


def to_typed_frame(spark, csv_path, table):
    """Read a GTFS CSV as text, retain documented fields, cast and tidy it."""
    raw = (spark.read.option("header", "true")
           .option("encoding", "UTF-8")
           .option("mode", "PERMISSIVE")
           .option("ignoreLeadingWhiteSpace", "false")
           .option("ignoreTrailingWhiteSpace", "false")
           .csv(csv_path.as_uri()))
    columns = []
    for name, kind in TABLES[table]:
        if name in raw.columns:
            field = F.trim(F.col(f"`{name}`"))
            field = F.when(F.length(field) > 0, field)
        else:
            field = F.lit(None).cast("string")
        if kind == "date":
            field = F.to_date(field, "yyyyMMdd")
        else:
            field = field.cast(kind)
        columns.append(field.alias(name))
    frame = raw.select(*columns)

    if table == "stop_times":
        frame = (frame.withColumn("arrival_seconds", seconds_since_start("arrival_time"))
                 .withColumn("departure_seconds", seconds_since_start("departure_time")))
    elif table == "frequencies":
        frame = (frame.withColumn("start_seconds", seconds_since_start("start_time"))
                 .withColumn("end_seconds", seconds_since_start("end_time")))

    for key in KEYS.get(table, ()):
        frame = frame.filter(F.col(key).isNotNull())

    # Large timetable/shape files are already keyed by the feed publisher.
    # Avoid a global shuffle of hundreds of MB just to remove hypothetical
    # duplicates; reference tables below are small enough to deduplicate.
    if table not in {"stop_times", "shapes"}:
        frame = frame.dropDuplicates()
    return frame


def download_hdfs_zip(spark, hdfs_uri, local_path):
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    source = jvm.org.apache.hadoop.fs.Path(hdfs_uri)
    dest = jvm.org.apache.hadoop.fs.Path(str(local_path))
    fs = source.getFileSystem(conf)
    if not fs.exists(source):
        raise FileNotFoundError(f"GTFS ZIP not found in HDFS: {hdfs_uri}")
    fs.copyToLocalFile(source, dest)


def sha256_file(filename):
    hasher = hashlib.sha256()
    with filename.open("rb") as src:
        for block in iter(lambda: src.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()[:12]


def process_archive(spark, archive):
    if not archive.startswith("hdfs://") or not archive.lower().endswith(".zip"):
        raise ValueError("Expected an archived HDFS GTFS .zip URI")
    with TemporaryDirectory(prefix="gtfs-") as directory:
        temp = Path(directory)
        local_zip = temp / "schedule.zip"
        download_hdfs_zip(spark, archive, local_zip)
        content_id = sha256_file(local_zip)
        match = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", archive)
        archive_date = match.group(1) if match else "undated"
        feed_version = f"{archive_date}-{content_id}"
        LOG.info("GTFS version %s; archive size %d", feed_version, local_zip.stat().st_size)

        with ZipFile(local_zip) as zipped:
            members = {}
            for item in zipped.infolist():
                name = Path(item.filename).name.lower()
                if name.endswith(".txt") and not item.is_dir():
                    if name in members:
                        raise ValueError(f"Duplicate GTFS member name: {name}")
                    members[name] = item
            for name in REQUIRED:
                if f"{name}.txt" not in members:
                    raise ValueError(f"Missing required GTFS file: {name}.txt")

            for name in TABLES:
                item = members.get(f"{name}.txt")
                if item is None:
                    continue
                # Extract one CSV at a time to limit local disk requirements.
                csv = temp / f"{name}.csv"
                with zipped.open(item) as source, csv.open("wb") as target:
                    shutil.copyfileobj(source, target, length=1024 * 1024)
                frame = to_typed_frame(spark, csv, name)
                destination = f"{HDFS}/data/cleaned/gtfs_{name}/feed_version={feed_version}"
                (frame.write.mode("overwrite")
                 .option("compression", "snappy")
                 .parquet(destination))
                LOG.info("Written GTFS %s -> %s", name, destination)
                csv.unlink()
        return feed_version


def main(archive):
    spark = (SparkSession.builder.appName("big-data-ztm-gtfs-cleaning")
             .master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.hadoop.fs.defaultFS", HDFS)
             .getOrCreate())
    try:
        process_archive(spark, archive)
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: spark-submit clean_gtfs.py <hdfs://.../gtfs-archive.zip>")
    main(sys.argv[1])
