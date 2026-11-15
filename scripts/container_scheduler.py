#!/usr/bin/env python3
"""Docker-contained batch scheduler and single-run entry point.

Uses Python's standard library *inside the Docker image*, not on the host.
The kernel file lock is on a shared Docker volume: one-off and scheduled runs
cannot rewrite the same HDFS partition concurrently. No Docker socket or cron.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import time

LOCK = Path('/var/lock/ztm/batch.lock')
INTERVAL = 15 * 60
SPARK_SUBMIT = '/opt/spark/bin/spark-submit'
PIPELINE = '/opt/project/spark/batch/run_pipeline.py'


def log(message: str) -> None:
    print(f'[{datetime.now().astimezone().isoformat(timespec="seconds")}] {message}', flush=True)


def run_once(dataset: str, archive: str | None = None, force: bool = False) -> bool:
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    with LOCK.open('a+') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log('Previous batch iteration still running; skipping overlap.')
            return False
        command = [SPARK_SUBMIT, '--master', 'local[2]', '--driver-memory', '2g', PIPELINE,
                   '--dataset', dataset]
        if archive:
            command.extend(['--archive', archive])
        if force:
            command.append('--force')
        log(f'Starting batch: dataset={dataset}' + (f', archive={archive}' if archive else ''))
        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as exc:
            log(f'Batch failed (exit status {exc.returncode}).')
            raise
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
        log('Batch finished successfully.')
        return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--loop', action='store_true', help='Run at quarter-hour boundaries')
    parser.add_argument('--once', action='store_true', help='Run one iteration (default)')
    parser.add_argument('--dataset', choices=['all', 'gps', 'weather', 'air_quality', 'gtfs'], default='all')
    parser.add_argument('--archive', help='Explicit archived GTFS HDFS URI')
    parser.add_argument('--force', action='store_true', help='Rebuild a GTFS archive')
    args = parser.parse_args()
    if args.loop and (args.once or args.dataset != 'all' or args.archive or args.force):
        parser.error('--loop cannot be combined with manual batch options')
    if args.archive and args.dataset != 'gtfs':
        parser.error('--archive requires --dataset gtfs')
    if args.force and args.dataset != 'gtfs':
        parser.error('--force requires --dataset gtfs')
    if not args.loop:
        try:
            run_once(args.dataset, args.archive, args.force)
            return 0
        except subprocess.CalledProcessError as exc:
            return exc.returncode or 1

    log('Docker batch scheduler started (every 15 minutes).')
    while True:
        # UTC quarter-hours are also Warsaw quarter-hours (whole-hour UTC offset).
        delay = INTERVAL - (time.time() % INTERVAL)
        time.sleep(delay)
        try:
            run_once('all')
        except Exception as exc:
            log(f'Scheduled iteration failed: {exc}; next cycle will retry.')


if __name__ == '__main__':
    sys.exit(main())
