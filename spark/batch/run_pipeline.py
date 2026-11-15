"""One Spark application for JSON raw cleaning, GTFS archives, and Hive repair.

The script runs only in the batch Docker image. The outer container scheduler
serializes executions with a shared Docker-volume lock, without a Docker socket.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
from pathlib import Path
import subprocess

from pyspark.sql import SparkSession

from clean_raw import TRANSFORMS, run_dataset
from clean_gtfs import process_archive
from incremental_hdfs import STATE, hadoop_fs, path

LOG = logging.getLogger('ztm-pipeline')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
HDFS = os.getenv('HDFS_BASE', 'hdfs://namenode:8020').rstrip('/')
JDBC = os.getenv('HIVE_JDBC_URL', 'jdbc:hive2://hive-server2:10000/default')
REFRESH = '/opt/project/hive/sql/refresh_partitions.sql'


def refresh_hive():
    LOG.info('Refreshing all Hive external table partitions via Beeline')
    subprocess.run(['/opt/hive/bin/beeline', '-u', JDBC, '-f', REFRESH], check=True)


def gtfs_archives(fs, jvm):
    root = path(jvm, f'{HDFS}/data/raw/gtfs')
    if not fs.exists(root):
        return []
    entries = fs.listFiles(root, True)
    archives = []
    while entries.hasNext():
        stat = entries.next()
        if str(stat.getPath().getName()).lower().endswith('.zip'):
            archives.append((str(stat.getPath().toString()), int(stat.getLen()),
                             int(stat.getModificationTime())))
    return sorted(archives)


def run_gtfs(spark, archive=None, force=False):
    jvm, fs = hadoop_fs(spark)
    marker_dir = path(jvm, f'{HDFS}{STATE}/gtfs')
    fs.mkdirs(marker_dir)
    files = gtfs_archives(fs, jvm)
    if archive:
        if not archive.startswith(f'{HDFS}/data/raw/gtfs/') or not archive.endswith('.zip'):
            raise ValueError('GTFS archive must be under HDFS /data/raw/gtfs and end in .zip')
        matches = [entry for entry in files if entry[0] == archive]
        if not matches:
            raise FileNotFoundError(f'GTFS ZIP not found: {archive}')
        files = matches
    if not files:
        LOG.info('No GTFS ZIP archives available; skipping GTFS cleaning')
    for uri, size, modified in files:
        # Content-independent marker; the same immutable ZIP is not reprocessed.
        fingerprint = hashlib.sha256(f'{uri}\n{size}:{modified}\n'.encode()).hexdigest()
        marker = path(jvm, f'{HDFS}{STATE}/gtfs/{fingerprint}.done')
        if not force and fs.exists(marker):
            LOG.info('GTFS archive already processed: %s', uri)
            continue
        LOG.info('Processing GTFS ZIP: %s', uri)
        process_archive(spark, uri)
        # Keep per-ZIP metadata semantics: a marker follows successful SQL repair.
        refresh_hive()
        if not fs.createNewFile(marker):
            if not fs.exists(marker):
                raise IOError(f'Unable to record GTFS completion marker: {marker}')
        LOG.info('GTFS ZIP committed: %s', uri)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', choices=['all', 'gps', 'weather', 'air_quality', 'gtfs'], default='all')
    parser.add_argument('--archive')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    spark = (SparkSession.builder
             .appName('big-data-ztm-batch-pipeline')
             .master('local[2]')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.ansi.enabled', 'false')
             .config('spark.sql.shuffle.partitions', '4')
             .config('spark.locality.wait', os.getenv('SPARK_LOCALITY_WAIT', '0s'))
             .config('spark.hadoop.fs.defaultFS', HDFS)
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        if args.dataset != 'gtfs':
            names = TRANSFORMS if args.dataset == 'all' else [args.dataset]
            for name in names:
                run_dataset(spark, name)
            # Repair even when there were no new JSONs; a previous Hive sync
            # may have failed after the cleaned partition was committed.
            refresh_hive()
        if args.dataset in ('all', 'gtfs'):
            run_gtfs(spark, args.archive, args.force)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
