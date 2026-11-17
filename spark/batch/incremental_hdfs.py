"""HDFS helpers for incremental raw JSON processing.

Each archive file is immutable. A completed manifest is stored in HDFS, not
on the Spark container's ephemeral filesystem. Changes to an already processed
raw file fail loudly, because merging cannot retract removed source records.
"""

from __future__ import annotations

import json
import logging
import uuid

LOG = logging.getLogger("ztm-spark-batch")
STATE = "/data/cleaned/_pipeline_state"


def hadoop_fs(spark):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    return jvm, jvm.org.apache.hadoop.fs.FileSystem.get(conf)


def path(jvm, value: str):
    return jvm.org.apache.hadoop.fs.Path(value)


def list_archived_json(fs, jvm, source_dir):
    root = path(jvm, source_dir)
    if not fs.exists(root):
        return {}
    files = fs.listFiles(root, True)
    result = {}
    while files.hasNext():
        stat = files.next()
        name = stat.getPath().getName()
        # skip hidden files
        if name.endswith(".json") and not name.startswith((".", "_")):
            result[str(stat.getPath().toString())] = [
                int(stat.getLen()), int(stat.getModificationTime())
            ]
    return dict(sorted(result.items()))


def read_manifest(fs, jvm, name):
    location = path(jvm, f"{STATE}/raw/{name}.json")
    if not fs.exists(location):
        return {}
    stream = fs.open(location)
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
    lines = []
    try:
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(line)
    finally:
        reader.close()
    loaded = json.loads("\n".join(lines))
    if not isinstance(loaded, dict) or loaded.get("version") != 1:
        raise ValueError(f"Invalid raw manifest for {name}; refusing to guess processed files")
    return loaded["files"]


def save_manifest(fs, jvm, name, signatures):
    location = path(jvm, f"{STATE}/raw/{name}.json")
    temporary = path(jvm, f"{STATE}/raw/.{name}.{uuid.uuid4().hex}.tmp")
    fs.mkdirs(location.getParent())
    writer = jvm.java.io.OutputStreamWriter(fs.create(temporary, False), "UTF-8")
    try:
        writer.write(json.dumps({"version": 1, "files": signatures}, sort_keys=True))
    finally:
        writer.close()
    # Missing manifest after an interrupted replacement is safe: the next job
    # reprocesses the archives and merges them by business key.
    if fs.exists(location):
        fs.delete(location, False)
    if not fs.rename(temporary, location):
        raise IOError(f"Failed to commit manifest {location}")


def recover_backups(fs, jvm, name, target_root):
    """Restore a partition if an earlier run stopped between HDFS renames."""
    backup_root = path(jvm, f"{STATE}/backups/{name}")
    if not fs.exists(backup_root):
        return
    for entry in fs.listStatus(backup_root):
        backup = entry.getPath()
        target = path(jvm, f"{target_root}/{backup.getName()}")
        if fs.exists(target):
            # Target is already installed; this is a leftover previous version.
            fs.delete(backup, True)
        elif not fs.rename(backup, target):
            raise IOError(f"Could not restore interrupted partition: {target}")


def replace_partition(fs, jvm, staged, target, backup):
    """Stage before swapping, never write while reading the target folder."""
    fs.mkdirs(backup.getParent())
    if fs.exists(backup):
        raise RuntimeError(f"Unrecovered backup exists: {backup}")
    had_previous = fs.exists(target)
    if had_previous and not fs.rename(target, backup):
        raise IOError(f"Failed to back up partition {target}")
    if not fs.rename(staged, target):
        if had_previous and not fs.exists(target):
            fs.rename(backup, target)
        raise IOError(f"Failed to install staged partition {target}")
    if had_previous:
        fs.delete(backup, True)
