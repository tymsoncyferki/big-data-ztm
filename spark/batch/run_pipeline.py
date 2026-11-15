"""HDFS Raw -> Snappy Parquet batch cycle (Spark only).

One normal cycle always checks both inputs in sequence:
  1. GPS/weather/air-quality JSON snapshots
  2. GTFS ZIP timetable feeds
  3. Cleaned -> Enriched (GTFS trip/service reference)
Individual dataset switches are maintenance/debug operations only.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os

from pyspark.sql import SparkSession

from clean_raw import TRANSFORMS, run_dataset
from clean_gtfs import process_archive
from incremental_hdfs import STATE, hadoop_fs, path

# Stage 2 runs in the same Spark application after Stage 1 has finished.
import sys
from pathlib import Path as LocalPath
sys.path.insert(0, str(LocalPath(__file__).resolve().parents[1] / "downstream"))
from pipeline import enrich_gtfs

LOG = logging.getLogger("ztm-pipeline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
HDFS = os.getenv("HDFS_BASE", "hdfs://namenode:8020").rstrip("/")


def gtfs_archives(fs, jvm):
    root = path(jvm, f"{HDFS}/data/raw/gtfs")
    if not fs.exists(root):
        return []
    iterator = fs.listFiles(root, True)
    archives = []
    while iterator.hasNext():
        status = iterator.next()
        if status.getPath().getName().lower().endswith(".zip"):
            archives.append((str(status.getPath().toString()), int(status.getLen()),
                             int(status.getModificationTime())))
    return sorted(archives)


def run_gtfs(spark, archive=None, force=False):
    jvm, fs = hadoop_fs(spark)
    state = path(jvm, f"{HDFS}{STATE}/gtfs")
    fs.mkdirs(state)
    files = gtfs_archives(fs, jvm)
    if archive:
        if not archive.startswith(f"{HDFS}/data/raw/gtfs/") or not archive.lower().endswith(".zip"):
            raise ValueError("GTFS archive must be in HDFS /data/raw/gtfs and end with .zip")
        files = [item for item in files if item[0] == archive]
        if not files:
            raise FileNotFoundError(f"GTFS ZIP not found: {archive}")
    if not files:
        LOG.info("No archived GTFS ZIP files: no timetable changes")
    for uri, size, modified in files:
        fingerprint = hashlib.sha256(f"{uri}\n{size}:{modified}\n".encode()).hexdigest()
        marker = path(jvm, f"{HDFS}{STATE}/gtfs/{fingerprint}.done")
        if fs.exists(marker) and not force:
            LOG.info("Skipping previously processed GTFS archive: %s", uri)
            continue
        LOG.info("Processing GTFS %s", uri)
        version = process_archive(spark, uri)
        if not fs.exists(marker) and not fs.createNewFile(marker):
            raise IOError(f"Unable to store GTFS marker: {marker}")
        LOG.info("Committed GTFS feed_version=%s", version)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["all", "gps", "weather", "air_quality", "gtfs"], default="all")
    parser.add_argument("--archive")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("big-data-ztm-batch-pipeline")
             .master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.locality.wait", os.getenv("SPARK_LOCALITY_WAIT", "0s"))
             .config("spark.hadoop.fs.defaultFS", HDFS)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.dataset != "gtfs":
            for name in (TRANSFORMS if args.dataset == "all" else [args.dataset]):
                run_dataset(spark, name)
        if args.dataset in ("all", "gtfs"):
            run_gtfs(spark, args.archive, args.force)
            # Mandatory second stage of the normal full batch cycle.
            enrich_gtfs(spark, force=args.force)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
