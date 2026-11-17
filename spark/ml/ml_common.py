"""Shared constants and helpers for the ML data-preparation jobs.

Everything in this package works on the *cleaned* Parquet datasets produced by
spark/batch (Raw -> Cleaned) and never touches /data/raw or /data/cleaned for
writing. Outputs go to /data/enriched/{stop_visits,ml_dataset}.

The functions that define "what a delay is" and "what a feature is"
(haversine_m, service_day_start_epoch, ...) are plain Column expressions with no
batch-only assumptions, so the Spark Structured Streaming job can import and
reuse exactly the same definitions.
"""

from __future__ import annotations

import fcntl
import logging
import os
import time
from datetime import date, timedelta

from pyspark.sql import Column, SparkSession, functions as F

LOG = logging.getLogger("ztm-ml")

HDFS = os.getenv("HDFS_BASE", "hdfs://namenode:8020").rstrip("/")
WARSAW_TZ = "Europe/Warsaw"
EARTH_RADIUS_M = 6_371_000.0

# Generous bounding box around the Warsaw agglomeration. It only removes
# obviously broken GPS points such as (0, 0); it is not a precise city border.
BBOX_LAT = (51.8, 52.6)
BBOX_LON = (20.4, 21.6)

# Grid used for fast "which stops are near this GPS point" joins.
# Cells must be larger than the maximum stop radius (see MAX_RADIUS_FOR_GRID_M).
CELL_LAT_DEG = 0.0015  # ~167 m
CELL_LON_DEG = 0.0025  # ~171 m at 52.2 N
MAX_RADIUS_FOR_GRID_M = 150.0

# Coarser grid used to pick the nearest weather / air-quality station.
LOOKUP_CELL_DEG = 0.005  # ~550 m x ~340 m

# Warsaw stops are NOT always unique per direction, so keep these defaults
# overridable from the command line.
DEFAULT_STOP_RADIUS_M = 75.0
DEFAULT_MAX_ABS_DELAY_S = 7200
# Pings this close (metres) to the closest approach count as 'standing at the stop';
# the LAST of them is the departure time (GTFS schedules departures).
DEFAULT_DWELL_TOLERANCE_M = 10.0
DEFAULT_PING_INTERVAL_S = 60
DEFAULT_HORIZON_S = 600
DEFAULT_HORIZON_TOLERANCE_S = 300
DEFAULT_CONFIRM_LAG_S = 30
DEFAULT_AIR_PARAMS = ("PM10", "PM2.5", "NO2")

LOCK_FILE = os.getenv("ML_LOCK_FILE", "/var/lock/ztm/batch.lock")


# --------------------------------------------------------------------------- #
# Spark session
# --------------------------------------------------------------------------- #
def configure_session(spark: SparkSession) -> SparkSession:
    """Apply the SQL settings every ML job depends on (all are runtime settings).

    IMPORTANT: when the ML build runs inside another Spark application (the 15-minute
    pipeline), call this on a *new session* of it (``spark.newSession()``), otherwise
    ``mode("overwrite")`` would replace the WHOLE ml_dataset directory instead of only
    the re-computed ``date=`` partitions.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    # Only replace the partitions that are actually written (re-runs are safe).
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    # Keep feed_version="2026-11-15-ab12..." and date="2026-11-15" as strings.
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("ML_SHUFFLE_PARTITIONS", "16"))
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    return spark


def build_spark(app_name: str, base: str = HDFS) -> SparkSession:
    """Create a SparkSession with the settings every ML job needs.

    The master is normally supplied by spark-submit (--master local[2]); set
    ML_SPARK_MASTER only when running with plain `python`.
    """
    builder = SparkSession.builder.appName(app_name)
    if base.startswith("hdfs://"):
        builder = builder.config("spark.hadoop.fs.defaultFS", base)
    master = os.getenv("ML_SPARK_MASTER")
    if master:
        builder = builder.master(master)
    spark = configure_session(builder.getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------- #
# Column expressions shared by batch and (future) streaming code
# --------------------------------------------------------------------------- #
def haversine_m(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in metres between two WGS84 points."""
    phi1, phi2 = F.radians(lat1), F.radians(lat2)
    d_phi = F.radians(lat2 - lat1)
    d_lambda = F.radians(lon2 - lon1)
    a = F.pow(F.sin(d_phi / 2), 2) + F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(d_lambda / 2), 2)
    # least(1, a) protects asin/sqrt from tiny floating point overshoots.
    return 2 * EARTH_RADIUS_M * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def grid_x(lon: Column) -> Column:
    return F.floor(lon / CELL_LON_DEG).cast("int")


def grid_y(lat: Column) -> Column:
    return F.floor(lat / CELL_LAT_DEG).cast("int")


def service_day_start_epoch(service_date: Column) -> Column:
    """Epoch seconds (UTC) of the start of a GTFS service day.

    GTFS measures times such as "25:10:00" from *noon minus 12 hours* on the
    service date, which equals local midnight except on the two daylight-saving
    days per year (23 h / 25 h days). Using plain local midnight would be off by
    one hour on those days.
    """
    noon_wall_clock = (service_date.cast("timestamp").cast("long") + 12 * 3600).cast("timestamp")
    noon_utc = F.to_utc_timestamp(noon_wall_clock, WARSAW_TZ)
    return noon_utc.cast("long") - 12 * 3600


# --------------------------------------------------------------------------- #
# HDFS helpers (work on any Hadoop FileSystem, including file:// for tests)
# --------------------------------------------------------------------------- #
def _fs(spark: SparkSession, uri: str):
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(uri)
    return jvm, p.getFileSystem(conf), p


def exists(spark: SparkSession, uri: str) -> bool:
    _, fs, p = _fs(spark, uri)
    return bool(fs.exists(p))


def list_partition_values(spark: SparkSession, root_uri: str, key: str) -> list[str]:
    """Return sorted values of `key=<value>` sub-directories (e.g. date=2026-11-15)."""
    _, fs, root = _fs(spark, root_uri)
    if not fs.exists(root):
        return []
    prefix = f"{key}="
    values = []
    for status in fs.listStatus(root):
        name = status.getPath().getName()
        if status.isDirectory() and name.startswith(prefix):
            values.append(name[len(prefix):])
    return sorted(values)


def write_text(spark: SparkSession, uri: str, text: str) -> None:
    jvm, fs, p = _fs(spark, uri)
    fs.mkdirs(p.getParent())
    writer = jvm.java.io.OutputStreamWriter(fs.create(p, True), "UTF-8")
    try:
        writer.write(text)
    finally:
        writer.close()


def date_range(start: str, end: str) -> list[str]:
    d0, d1 = date.fromisoformat(start), date.fromisoformat(end)
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def next_day(day: str) -> str:
    return (date.fromisoformat(day) + timedelta(days=1)).isoformat()


# --------------------------------------------------------------------------- #
# Lock shared with the 15-minute cleaning scheduler
# --------------------------------------------------------------------------- #
class BatchLock:
    """Hold the same file lock as scripts/container_scheduler.py.

    While the ML job reads /data/cleaned, the cleaning job could swap a partition
    directory under it (rename). Holding the lock prevents that. The scheduler
    simply skips its cycle while we hold the lock and catches up afterwards,
    because the cleaning is incremental.
    """

    def __init__(self, path: str = LOCK_FILE, timeout_s: int = 45 * 60, enabled: bool = True):
        self.path, self.timeout_s, self.enabled = path, timeout_s, enabled
        self._handle = None

    def __enter__(self):
        if not self.enabled:
            return self
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self._handle = open(self.path, "a+")
        except OSError as exc:
            LOG.warning("Cannot open lock file %s (%s); running WITHOUT the batch lock", self.path, exc)
            return self
        deadline = time.monotonic() + self.timeout_s
        while True:
            try:
                fcntl.flock(self._handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                LOG.info("Acquired batch lock %s", self.path)
                return self
            except BlockingIOError:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Could not get {self.path} within {self.timeout_s}s")
                LOG.info("Cleaning batch is running; waiting for the lock...")
                time.sleep(15)

    def __exit__(self, *exc):
        if self._handle is not None:
            try:
                fcntl.flock(self._handle, fcntl.LOCK_UN)
            finally:
                self._handle.close()
        return False
