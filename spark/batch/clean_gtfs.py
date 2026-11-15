"""Batch clean one archived GTFS ZIP into versioned HDFS Parquet tables.

Usage (inside Spark container): spark-submit clean_gtfs.py hdfs://namenode:8020/data/raw/gtfs/date=.../archive.zip
All Spark work runs in local mode in this project, so temp extraction is shared
between the driver and Spark workers. The ZIP is copied to a temporary file
using Hadoop FileSystem instead of being collected into the driver heap.
"""

import hashlib
import logging
from pathlib import Path
import re
import shutil
import sys
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger("gtfs-batch")

# Column name, Spark SQL type; order must match the Hive external table DDL.
TABLES = {
    "routes": [
        ("route_id", "string"), ("route_short_name", "string"),
        ("route_long_name", "string"), ("route_type", "int"),
    ],
    "stops": [
        ("stop_id", "string"), ("stop_name", "string"),
        ("stop_lat", "double"), ("stop_lon", "double"),
    ],
    "trips": [
        ("trip_id", "string"), ("route_id", "string"),
        ("service_id", "string"), ("trip_headsign", "string"),
        ("direction_id", "int"),
    ],
    "stop_times": [
        ("trip_id", "string"), ("stop_sequence", "int"),
        ("stop_id", "string"), ("arrival_time", "string"),
        ("departure_time", "string"),
    ],
    "calendar": [
        ("service_id", "string"),
        *[(day, "boolean") for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")],
        ("start_date", "date"), ("end_date", "date"),
    ],
    "calendar_dates": [
        ("service_id", "string"), ("date", "date"),
        ("exception_type", "int"),
    ],
}
REQUIRED = {"routes", "stops", "trips", "stop_times"}


def seconds_since_start(column):
    value = F.col(column)
    chunks = F.split(value, ":")
    return F.when(
        value.rlike(r"^\d{1,3}:[0-5]\d:[0-5]\d$"),
        chunks.getItem(0).cast("int") * 3600
        + chunks.getItem(1).cast("int") * 60
        + chunks.getItem(2).cast("int"),
    ).otherwise(F.lit(None).cast("int"))


def to_typed_frame(spark, csv_path, table):
    raw = spark.read.option("header", "true").option("encoding", "UTF-8").csv(csv_path.as_uri())
    columns = []
    for name, kind in TABLES[table]:
        field = F.col(name) if name in raw.columns else F.lit(None)
        if kind == "date":
            field = F.to_date(field, "yyyyMMdd")
        else:
            field = field.cast(kind)
        columns.append(field.alias(name))
    result = raw.select(*columns)
    if table == "stop_times":
        result = (result
                  .withColumn("arrival_seconds", seconds_since_start("arrival_time"))
                  .withColumn("departure_seconds", seconds_since_start("departure_time")))
    # Do not create rows that cannot be joined to a stable GTFS identifier.
    identifiers = {
        "routes": ["route_id"], "stops": ["stop_id"],
        "trips": ["trip_id", "route_id", "service_id"],
        "stop_times": ["trip_id", "stop_id", "stop_sequence"],
        "calendar": ["service_id"], "calendar_dates": ["service_id", "date"],
    }
    for name in identifiers[table]:
        result = result.filter(F.col(name).isNotNull())
    return result.dropDuplicates(identifiers[table])


def download_hdfs_zip(spark, hdfs_uri, local_path):
    """Copy the archive with Hadoop FileSystem, without materializing it in Spark/JVM heap.

    This job uses Spark local mode, so the driver and executors see the same
    container filesystem. The local ZIP is removed with TemporaryDirectory.
    """
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    source = jvm.org.apache.hadoop.fs.Path(hdfs_uri)
    destination = jvm.org.apache.hadoop.fs.Path(str(local_path))
    filesystem = source.getFileSystem(conf)
    if not filesystem.exists(source):
        raise FileNotFoundError(f"GTFS archive missing from HDFS: {hdfs_uri}")
    filesystem.copyToLocalFile(source, destination)
    if not local_path.is_file():
        raise RuntimeError(f"Failed to copy GTFS archive to {local_path}")


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()[:12]


def process_archive(spark, archive):
    """Process one archived ZIP using an existing Spark session."""
    if not archive.startswith("hdfs://") or not archive.endswith(".zip"):
        raise ValueError("Provide a fully qualified HDFS ZIP URI")
    with TemporaryDirectory(prefix="gtfs-") as temp:
        tmp = Path(temp)
        local_archive = tmp / "schedule.zip"
        download_hdfs_zip(spark, archive, local_archive)
        sha = sha256_file(local_archive)
        matched = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", archive)
        downloaded = matched.group(1) if matched else "undated"
        feed_version = f"{downloaded}-{sha}"
        LOG.info("GTFS feed version %s, ZIP size %d bytes", feed_version, local_archive.stat().st_size)
        locations = {}
        with ZipFile(local_archive) as zipped:
            for table in TABLES:
                members = [member for member in zipped.namelist()
                           if Path(member).name.lower() == f"{table}.txt"]
                if len(members) > 1:
                    raise ValueError(f"Multiple {table}.txt files in GTFS archive")
                if not members:
                    if table in REQUIRED:
                        raise ValueError(f"Required {table}.txt missing in GTFS archive")
                    continue
                dest = tmp / f"{table}.csv"
                with zipped.open(members[0]) as source, dest.open("wb") as target:
                    shutil.copyfileobj(source, target, length=1024 * 1024)
                locations[table] = dest
        for table, local_path in locations.items():
            frame = to_typed_frame(spark, local_path, table)
            dest = f"hdfs://namenode:8020/data/cleaned/gtfs_{table}/feed_version={feed_version}"
            frame.write.mode("overwrite").option("compression", "snappy").parquet(dest)
            LOG.info("Wrote %s to %s", table, dest)


def main(archive):
    spark = (
        SparkSession.builder.appName("big-data-ztm-gtfs-cleaning")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .getOrCreate()
    )
    try:
        process_archive(spark, archive)
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: spark-submit clean_gtfs.py <hdfs://.../gtfs-archive.zip>")
    main(sys.argv[1])
