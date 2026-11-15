# Downstream processing — team handoff

This is the **starting point for the next developer**. It is deliberately not a
completed prediction pipeline. The ingestion and cleaned-data layers should
remain independent of feature engineering and model training.

## Available inputs

Run `bash scripts/bootstrap.sh` (Linux/WSL) or `.\scripts\bootstrap.ps1`
(Windows PowerShell) first. A full incremental batch is executed during bootstrap. Then read the following Spark Parquet datasets
from HDFS (or query the matching Hive external tables under database `cleaned`):

| Dataset | HDFS path |
|---|---|
| GPS observations | `hdfs://namenode:8020/data/cleaned/gps_observations` |
| Weather observations | `hdfs://namenode:8020/data/cleaned/weather_observations` |
| Air-quality observations | `hdfs://namenode:8020/data/cleaned/air_quality_observations` |
| GTFS trips | `hdfs://namenode:8020/data/cleaned/gtfs_trips` |
| GTFS stop times | `hdfs://namenode:8020/data/cleaned/gtfs_stop_times` |
| GTFS routes, stops and service calendars | `hdfs://namenode:8020/data/cleaned/gtfs_*` |

The schemas, event keys, UTC timestamp conventions, and partitioning rules are
documented in [`docs/data-contracts.md`](../../docs/data-contracts.md).

## Tasks for the next developer

1. Verify how GPS `trip_id` maps to GTFS `trips.txt`, including `service_date`
   and the correct `feed_version`.
2. Match GPS points to scheduled stop visits and calculate observed versus
   scheduled arrival/departure times. GTFS times can exceed `24:00:00`.
3. Add weather and pollution features using explicit geographical and temporal
   matching rules (avoid using future measurements when predicting delays).
4. Design the feature/target table for training, define time-based validation,
   then implement and evaluate models.
5. Write enriched/feature outputs to a **separate** HDFS namespace, e.g.
   `/data/enriched/` and `/data/features/`. Do not overwrite `/data/cleaned/`.

## Starter code

[`pipeline.py`](pipeline.py) contains the Spark session and typed-data readers,
plus a clearly marked `NotImplementedError` where enrichment and modeling work
should begin. It is a scaffold, **not a working predictor**.

## Separate online pathway

Streaming from Kafka belongs to the planned online/speed layer. See
[`spark/streaming/README.md`](../streaming/README.md). Do not change the batch
cleaner to consume Kafka; the two paths have different purposes.
