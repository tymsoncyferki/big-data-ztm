# Big Data ZTM — Warsaw Public Transport

## Project overview

Big Data ZTM is a data engineering project focused on Warsaw's public transport network. It combines vehicle GPS observations, weather and air-quality measurements, and GTFS timetables to build a consistent historical view of transport operations and the conditions in which they take place. The resulting datasets provide a foundation for exploring vehicle movement, service reliability, and potential relationships between traffic conditions and the surrounding environment.

The project implements a containerized pipeline for ingesting, storing, preparing, and querying these datasets. Apache NiFi handles ingestion and raw archival, Kafka receives individual observation records, Hadoop HDFS stores the historical data, Apache Spark performs incremental batch cleaning and deduplication, and Apache Hive exposes the resulting Parquet tables through SQL. The current milestone delivers this data foundation; delay modeling, advanced enrichment, and Spark Structured Streaming remain future work.

## Architecture

```text
GPS / weather / air-quality APIs
      ↓
Source simulator (Python INSIDE Docker; optional)
      ↓ HTTP snapshots
NiFi ─────→ HDFS /data/raw/{gps,weather,air_quality} (full JSON snapshots)
   └──────→ Kafka topics gps / weather / air_quality (individual records)

GTFS source ──→ NiFi (enabled by --with-data; initial fetch + daily 03:00)
            ──→ HDFS /data/raw/gtfs/date=YYYY-MM-DD/*.zip

HDFS Raw ──→ Docker batch runner (Spark 3.5 / HDFS business-key cleaning)
          ──→ HDFS /data/cleaned/* (Snappy Parquet; date / feed_version)
          ──→ Hive Metastore + HiveServer2 (external tables in cleaned.*)

Optional batch-scheduler container ──→ identical batch runner every 15 minutes
```

The source simulator buffers GPS snapshots and interpolates environmental readings; it does **not** perform NiFi's ingestion, duplicate filtering, Kafka forwarding, or HDFS storage. Spark batch reads **HDFS Raw**, not Kafka. Both Spark and the Hive JDBC client run inside Docker. `batch-scheduler` runs its own Python standard-library clock (not host cron) and uses a shared Docker-volume file lock (not host `flock` or a Docker socket).

## Requirements

- Docker Desktop (with Linux containers) on **Windows 11**, or Docker Engine on **Linux**; Docker Compose v2.
- For Linux / WSL: Bash. For Windows PowerShell: PowerShell 5.1+ or PowerShell 7.
- Internet access for pulling/building Docker images and NiFi/API ingestion.
- Enough memory and disk space to run NiFi, Kafka, Hadoop, Hive and Spark simultaneously.

**Not required on the host:** Python, pip, `requests`, Java, Spark, Hive, cron, or `flock`. The optional scheduler does not need WSL to run independently: Docker Desktop itself must remain running.

### Configuration

On Linux / WSL:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Edit `.env` and configure `NIFI_USERNAME`, `NIFI_PASSWORD`, `NIFI_SENSITIVE_PROPS_KEY`, and `HIVE_DB_PASSWORD`. `WEATHER_API_TOKEN` is required only for `--with-data` / `-WithData` (which enables both the simulator and GTFS acquisition). Never commit `.env` or overwrite it while upgrading. The development Compose project uses the fixed project name `big-data-ztm`; do not start a second copy using the same project name against the same Docker daemon.

## First startup — choose the optional components

All startup modes launch HDFS, Kafka, NiFi, PostgreSQL/Hive Metastore/HiveServer2, initialize the nine Hive tables and NiFi flows, and run **one** incremental Spark/Hive/GTFS batch immediately. The scheduler starts only when selected; it is not needed for that first batch.

| Data acquisition (simulator + GTFS) | Batch scheduler | Linux / WSL Bash | Windows PowerShell |
|---|---|---|---|
| Off | Off | `bash scripts/bootstrap.sh` | `.\scripts\bootstrap.ps1` |
| On | Off | `bash scripts/bootstrap.sh --with-data` | `.\scripts\bootstrap.ps1 -WithData` |
| Off | On | `bash scripts/bootstrap.sh --with-schedule` | `.\scripts\bootstrap.ps1 -WithSchedule` |
| On | On | `bash scripts/bootstrap.sh --with-data --with-schedule` | `.\scripts\bootstrap.ps1 -WithData -WithSchedule` |

`--no-data` and `--no-schedule` (`-NoData` and `-NoSchedule`) are explicit alternatives to the off-by-default settings. Re-running bootstrap applies your choices to already-running optional services. It **does not remove HDFS/Kafka/Hive volumes**. GTFS downloads are **disabled** unless data acquisition is enabled. The NiFi GTFS process group is still imported in both modes, but its processors do not run when data acquisition is off.

When `--with-data` is selected, `start-data` enables the NiFi GTFS flow and its daily 03:00 (Europe/Warsaw) trigger. If HDFS has no nonempty `.zip` under `/data/raw/gtfs/`, the NiFi `Generate GTFS Trigger` is run once immediately; startup waits up to 300 seconds for the archive to appear before the initial Spark batch. An existing archived ZIP is reused, and no additional initial download is requested. Source outages or NiFi flow errors cause a clear startup error rather than silently claiming a successful initial fetch. Starting data acquisition later uses exactly the same behavior.

**PowerShell execution policy:** If Windows blocks `.ps1` files, use a session-scoped policy or launch PowerShell with an appropriate policy permitted by your organization. No WSL or local Bash is required for the PowerShell path.

## Toggle data ingestion and scheduling later

Without restarting the core infrastructure:

| Operation | Linux / WSL Bash | Windows PowerShell |
|---|---|---|
| Start data acquisition (simulator + GTFS) | `bash scripts/start-data.sh` | `.\scripts\start-data.ps1` |
| Stop data acquisition (simulator + GTFS) | `bash scripts/stop-data.sh` | `.\scripts\stop-data.ps1` |
| Start 15-minute batch scheduler | `bash scripts/start-schedule.sh` | `.\scripts\start-schedule.ps1` |
| Stop batch scheduler | `bash scripts/stop-schedule.sh` | `.\scripts\stop-schedule.ps1` |
| Run one complete batch now | `bash scripts/run-scheduled.sh` | `.\scripts\run-batch.ps1` |
| Run GPS batch only | `bash scripts/clean-batch.sh gps` | `.\scripts\run-batch.ps1 -Dataset gps` |
| Run GTFS batch only | `bash scripts/clean-gtfs.sh` | `.\scripts\run-batch.ps1 -Dataset gtfs` |
| Repair Hive partitions | `bash scripts/refresh-hive.sh` | `.\scripts\refresh-hive.ps1` |

For a targeted GTFS toggle test, start from the existing (initialized) stack with data acquisition off, run `bash scripts/start-data.sh`, inspect HDFS for `/data/raw/gtfs/date=.../*.zip`, and run `bash scripts/stop-data.sh`. Neither command affects the 15-minute batch scheduler. To process the newly archived ZIP immediately without waiting for that scheduler, use `bash scripts/run-scheduled.sh` (or `./scripts/run-batch.ps1` in PowerShell).

`install-schedule.sh` and `remove-schedule.sh` are compatibility aliases for the new Docker `start-schedule.sh` and `stop-schedule.sh`. They do **not** install or remove a crontab entry.

The Docker scheduler aligns to `:00`, `:15`, `:30`, and `:45` minutes and attempts one complete cycle. A cycle that overlaps an existing manual/scheduled batch is skipped via the shared container lock. Docker Compose writes scheduler logs to the container log stream:

```bash
docker compose --profile analytics --profile scheduler logs -f batch-scheduler
```

The same command works in PowerShell. Stopping Docker Desktop/Engine prevents future runs; missed cycles are not replayed individually, but the next run processes the accumulated unprocessed Raw files.

**Migrating from the old WSL cron scheduler:** Remove any legacy `# BIG_DATA_ZTM_BATCH_SCHEDULER` crontab entry to prevent double executions. `bootstrap.sh` removes that tagged entry automatically **if** `crontab` happens to be installed. PowerShell cannot manage WSL crontab; remove it in WSL once if you previously installed it.

## How a batch works

The `batch-runner` and `batch-scheduler` services use **the same image**. Each iteration starts a single Spark application, in which:

1. New immutable archived JSON files are identified through persistent HDFS manifests.
2. GPS, weather and air-quality records are validated, deduplicated by business key and merged with any existing cleaned date partitions using staged HDFS renames.
3. The Hive metadata for cleaned partitions is refreshed via Beeline in the same Docker image.
4. Any unprocessed GTFS ZIP is downloaded into the Spark container, parsed, and written to `feed_version=...` Parquet partitions. Each archive receives a persistent HDFS completion marker **only after Hive refresh succeeds**.

The schedule is independent of acquisition: with `--with-schedule` but without `--with-data`, only previously archived Raw files are processed, and no new GTFS download is initiated. A run with no new files is a safe no-op apart from Hive metadata repair. The GTFS ZIP archive marker is now derived from the HDFS path, length and modification timestamp as returned by the Hadoop API; when migrating from the old host script, existing ZIP archives may be processed **once more**, writing the same content-addressed `feed_version` path. This should not create duplicate versions.

Manual Bash usage (from an already initialized project):

```bash
bash scripts/clean-batch.sh all
bash scripts/clean-gtfs.sh --force hdfs://namenode:8020/data/raw/gtfs/date=YYYY-MM-DD/archive.zip
bash scripts/refresh-hive.sh
```

## Querying Hive

```bash
docker compose --profile analytics exec -T hive-server2 \
  /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/default' \
  -e 'SHOW PARTITIONS cleaned.gps_observations; SELECT COUNT(*) FROM cleaned.gps_observations;'
```

Nine Hive external tables exist: `cleaned.gps_observations`, `cleaned.weather_observations`, `cleaned.air_quality_observations`, `cleaned.gtfs_routes`, `cleaned.gtfs_stops`, `cleaned.gtfs_trips`, `cleaned.gtfs_stop_times`, `cleaned.gtfs_calendar`, and `cleaned.gtfs_calendar_dates`. GPS/weather/air-quality tables use partition `date` (UTC observation date); GTFS tables use `feed_version`. Optional GTFS calendar files may legitimately be absent.

Hive 4 in this demonstration uses local MapReduce for some queries. Warnings about deprecated Hive-on-MR or multiple SLF4J bindings can appear even when SQL succeeds. Success must be determined from the exit status and results.

## Services and troubleshooting

| Service | Host endpoint / command |
|---|---|
| NiFi | https://localhost:8443/nifi |
| NameNode UI | http://localhost:9870 |
| Kafka | `localhost:9092` |
| HiveServer2 | `jdbc:hive2://localhost:10000/default` |
| Batch scheduler | `docker compose --profile analytics --profile scheduler logs batch-scheduler` |
| One-off Spark | `docker compose --profile analytics run --rm --no-deps batch-runner` |

Host ports are bound to `127.0.0.1` in the supplied Compose configuration. Use `docker compose --profile analytics --profile scheduler --profile ingestion ps -a` to inspect all services.

To shut down optional ingestion or scheduling without losing data, use the corresponding `stop-*.sh` / `stop-*.ps1` script. **Do not run `docker compose down -v` unless you intentionally want to delete persistent data.** Raw archive files are append-only: editing or removing a processed file causes the Spark manifest check to fail instead of silently accepting inconsistent derived data. Do not delete `/data/cleaned/_pipeline_state/` without a recovery plan.

## Repository structure

```text
acquisition/            Optional source simulator (Python inside Docker)
nifi/flows/             Versioned NiFi flow definition
spark/batch/            JSON and GTFS cleaners + combined Spark batch pipeline
spark/downstream/       Future enrichment and delay-modeling scaffold
spark/streaming/        Future Kafka streaming integration
docker/batch/           Spark + Hive client image for one-off/scheduled runs
docker/nifi-init/       One-off NiFi bootstrap with bundled Python requests
hive/sql/               Hive DDL and partition metadata repair
scripts/                Bash and PowerShell entry points, Docker scheduler
docs/data-contracts.md  Detailed implemented and planned data contracts
```

See [data contracts](docs/data-contracts.md) for fields, keys, timestamps, partition schemas, guarantees, and limitations. See [review notes](docs/REVIEW-NOTES.md) for the refactor decisions and deployment caveats.
