# Big Data ZTM — Warsaw Public Transport

## Project overview

This project builds a Docker-based data platform for Warsaw public transport.
The Python source simulator buffers GPS observations and interpolates or carries
forward weather and air-quality measurements to emulate live data sources.
Apache NiFi receives these observations, stores JSON snapshots in HDFS Raw and
publishes individual events to Kafka. NiFi also downloads Warsaw GTFS archives.

Apache Spark processes stored data in three batch stages:

1. **Raw → Cleaned:** clean and convert incoming JSON and GTFS data into
   Snappy-compressed Parquet in HDFS.
2. **Cleaned → Enriched:** combine GTFS trip, route and service-date data into
   the `gtfs_trip_services` reference dataset.
3. **Cleaned → Enriched (ML dataset):** match GPS pings to GTFS stop times to
   estimate the delay at every stop (`stop_visits`), then build one row per
   vehicle per minute with motion, time, weather and air-quality features and
   the delay 10 minutes ahead as the target (`ml_dataset`). Code is in
   `spark/ml/`; definitions are in [docs/ml-dataset.md](docs/ml-dataset.md).

All three stages run in every full batch cycle, and the optional Docker
scheduler runs subsequent cycles every 15 minutes. Stage 3 rebuilds only the
last 2 UTC days, one day at a time, at most every 12 hours (`ML_STAGE_DAYS`,
`ML_STAGE_MIN_INTERVAL_MIN`); a failure there is logged and never fails the
cleaning stages. Use `bash scripts/build-ml-dataset.sh` for a full or
historical rebuild. Historical statistics, model training, Spark Structured
Streaming and Cassandra are planned, not implemented.

Stage 3 also prepares the timetable lookup for the future streaming job:
`/data/enriched/schedule_stops/` holds one row per scheduled stop of every trip
(stop position, scheduled time as UTC epoch seconds, route and headsign), built
from `gtfs_trip_services` and the cleaned `gtfs_stop_times` and `gtfs_stops` of
the same feed. It covers yesterday, today and tomorrow in Warsaw time, because
GTFS times can pass midnight. It is checked in every 15-minute cycle, independently
of the 12-hour ML throttle, and a date is rebuilt only when it has no partition
yet or a new GTFS feed version now covers it. The columns and the details are
described in [docs/ml-dataset.md](docs/ml-dataset.md).

## Architecture

```text
GPS / weather / air-quality APIs
                |
       Python source simulator
                |
          NiFi ingestion ------------------ GTFS source ZIP
           |           |                           |
           v           v                           v
       HDFS Raw      Kafka                   HDFS Raw GTFS
           |                                       |
           +-------------------+-------------------+
                               |
                  Spark Batch: Raw → Cleaned
                               |
                      HDFS /data/cleaned
                               |
                Spark Batch: Cleaned → Enriched
        (GTFS reference; stop visits, delays, ML features)
                               |
                      HDFS /data/enriched

Planned online pipeline:
Kafka + GTFS reference data from HDFS → Spark Structured Streaming → Cassandra
```

The simulator only prepares upstream observations; NiFi handles ingestion,
schema validation, duplicate detection, Kafka forwarding and Raw archiving.
The complete pipeline and field-level contracts are described in
[docs/data-contracts.md](docs/data-contracts.md).

## Data sources

| Data | Source | Ingestion output |
|---|---|---|
| GPS | `https://mkuran.pl/gtfs/warsaw/positions.json` | HDFS Raw JSON and Kafka `gps` |
| Weather | `https://dane.um.warszawa.pl/api/action/get_zom_pogoda` | HDFS Raw JSON and Kafka `weather` |
| Air quality | `https://dane.um.warszawa.pl/api/action/get_bopipk_monitoring_jakosci_powietrza` | HDFS Raw JSON and Kafka `air_quality` |
| GTFS | `https://mkuran.pl/gtfs/warsaw.zip` | ZIP archived in HDFS Raw; cleaned Parquet in HDFS |

The weather and air-quality APIs require a municipal API token. Refer to the
GTFS provider's licensing terms and `attributions.txt` before republishing data.

## Requirements and configuration

- Docker Engine with Docker Compose v2 (Docker Desktop + WSL2 is supported).
- Sufficient RAM and free Docker disk space for NiFi, Kafka, HDFS and Spark.
  GTFS processing requires temporary disk space for extracted CSV files.
- Network access to source APIs when data acquisition is enabled.
- No host Python, Java, Spark or cron installation is required to run the
  pipeline (only to run the optional unit tests, see [Tests](#tests)).

Create the environment file:

```bash
cp .env.example .env
```

Set `NIFI_USERNAME`, `NIFI_PASSWORD` and `NIFI_SENSITIVE_PROPS_KEY` to
non-placeholder values. The password and sensitive properties key must each
have at least 12 characters. Set `WEATHER_API_TOKEN` to enable data acquisition.
Do not commit `.env`.

## Start the project

Run commands from the project root (`~/projects/big-data-ztm` on WSL).

```bash
# Infrastructure + one full Spark batch
bash scripts/bootstrap.sh

# Infrastructure + simulator + GTFS acquisition + one full batch
bash scripts/bootstrap.sh --with-data

# Full startup with recurring batches every 15 minutes
bash scripts/bootstrap.sh --with-data --with-schedule
```

Acquisition and scheduling are independent. You can also enable recurring
batches without the simulator using `--with-schedule`. Bootstrap always
executes one full batch cycle. `--with-data` requires `WEATHER_API_TOKEN` and
requests the initial GTFS download if it is not yet present in HDFS. The NiFi
GTFS download is then scheduled daily at 03:00 (Europe/Warsaw).

Windows PowerShell alternatives:

```powershell
.\scripts\bootstrap.ps1
.\scripts\bootstrap.ps1 -WithData -WithSchedule
```

## Operations: monitoring and control

### Container status

```bash
# All main services, including optional simulator and scheduler
docker compose --profile ingestion --profile scheduler ps -a

# Simulator status
docker compose --profile ingestion ps -a simulator

# Scheduler status
docker compose --profile scheduler ps -a batch-scheduler
```

### Live logs

```bash
# Follow the data simulator (API polling, buffering and sends to NiFi)
docker compose --profile ingestion logs -f --tail=100 simulator

# Follow the batch scheduler and successive Spark batch runs
docker compose --profile scheduler logs -f --tail=100 batch-scheduler

# NiFi application logs
docker compose logs -f --tail=100 nifi

# Last 100 scheduler log lines, without live following
docker compose --profile scheduler logs --tail=100 batch-scheduler
```

Use `Ctrl+C` to stop following logs; the containers keep running. The simulator
buffers observations for approximately 15 minutes, so newly started acquisition
may not immediately produce GPS messages in NiFi, Kafka or HDFS.

### Start and stop data acquisition and scheduling

```bash
bash scripts/start-data.sh       # start simulator and NiFi GTFS acquisition
bash scripts/stop-data.sh        # stop simulator and NiFi GTFS acquisition
bash scripts/start-schedule.sh   # enable 15-minute batch scheduler
bash scripts/stop-schedule.sh    # disable scheduler, keep core services running
```

### Run batches manually

```bash
bash scripts/run-scheduled.sh     # one full Raw → Cleaned → Enriched cycle
bash scripts/clean-gtfs.sh        # process newly available GTFS archives
bash scripts/clean-gtfs.sh --force # reprocess GTFS archives
bash scripts/clean-batch.sh gps    # only GPS cleaning
bash scripts/clean-batch.sh weather
bash scripts/clean-batch.sh air_quality
```

Targeted cleaning commands are for maintenance; the scheduled cycle runs all
batch stages. A shared lock prevents overlapping scheduled and manual runs.

### ML dataset: delays and features

```bash
bash scripts/analysis/profile-data.sh                 # read-only data check, incl. GPS vs GTFS trip_id match
bash scripts/build-ml-dataset.sh                      # build stop_visits and ml_dataset for all available days
bash scripts/build-ml-dataset.sh --start-date 2026-11-16 --end-date 2026-11-17
bash scripts/analysis/diagnose-delays.sh              # quality of the estimated delays and baselines
bash scripts/analysis/show-sample.sh ml_dataset 5     # print a few rows (also: stop_visits, schedule_stops)
```

The build also runs automatically as the last stage of every full batch cycle
(`run-scheduled.sh`, `bootstrap.sh`, the scheduler).
The commands above are for full rebuilds and checks.

Run `profile-data.sh` first on new data: the build stops if no GPS `trip_id`
exists in the GTFS timetable. Re-running a date range replaces exactly those
`date=` partitions. The jobs wait for the shared batch lock, so they do not run
while a cleaning cycle is rewriting partitions. Delay definition, target,
leakage rules and every column are described in
[docs/ml-dataset.md](docs/ml-dataset.md).

### Tests

```bash
pip install pyspark==3.5.7 pytest      # needs Python 3.11/3.12, Java 17 or 21 on the host
python -m pytest tests -q
```

The tests use a small synthetic city with known delays and need neither Docker
nor HDFS.

### Inspect data in HDFS

```bash
# List datasets
docker compose exec -T namenode hdfs dfs -ls /data/raw
docker compose exec -T namenode hdfs dfs -ls /data/cleaned
docker compose exec -T namenode hdfs dfs -ls /data/enriched

# Total sizes by dataset (not the zero directory size shown by -ls)
docker compose exec -T namenode hdfs dfs -du -s -h '/data/raw/*' '/data/cleaned/*' '/data/enriched/*'

# See all GTFS source archives and cleaned trip partitions
docker compose exec -T namenode hdfs dfs -ls -R /data/raw/gtfs
docker compose exec -T namenode hdfs dfs -ls -R /data/cleaned/gtfs_trips
```

To display Parquet rows, use a Spark session connected to HDFS:

```python
trips = spark.read.parquet('hdfs://namenode:8020/data/cleaned/gtfs_trips/')
trips.show(20, truncate=False)
```

### Web interfaces

- NiFi: <https://localhost:8443/nifi> (credentials from `.env`).
- HDFS NameNode and file browser: <http://localhost:9870>.

### Stop the project

```bash
# Stop acquisition and the recurring scheduler independently
bash scripts/stop-data.sh
bash scripts/stop-schedule.sh

# Stop all Compose services, preserving stored data volumes
docker compose --profile ingestion --profile analytics --profile scheduler down --remove-orphans
```

Do not use `down -v` unless you intend to delete persistent NiFi, Kafka and
HDFS data. For an existing NiFi instance, updating the exported flow JSON file
does not automatically replace a flow already imported into NiFi.

## HDFS datasets

```text
/data/raw/gps|weather|air_quality/       JSON snapshots archived by NiFi
/data/raw/gtfs/date=YYYY-MM-DD/*.zip     GTFS source archives
/data/cleaned/gps_observations/date=YYYY-MM-DD/
/data/cleaned/weather_observations/date=YYYY-MM-DD/
/data/cleaned/air_quality_observations/date=YYYY-MM-DD/
/data/cleaned/gtfs_<table>/feed_version=<version>/
/data/enriched/gtfs_trip_services/feed_version=<version>/
/data/enriched/stop_visits/date=YYYY-MM-DD/   Delay at each stop visit (estimated from GPS)
/data/enriched/ml_dataset/date=YYYY-MM-DD/    Features + 10-minute-ahead target per vehicle-minute
/data/enriched/schedule_stops/service_date=YYYY-MM-DD/   Timetable lookup (stop, scheduled time) for streaming
/data/enriched/_ml_reports/                   JSON reports: counts and baselines per build
/data/cleaned/_pipeline_state/          Batch state and processing markers
```

GTFS partitions must be read consistently by `feed_version`. For the complete
list of supported GTFS source files, schemas, transformations and the Enriched
join rules, see [docs/data-contracts.md](docs/data-contracts.md).

## Healthcheck alerts

It is possible to set up healthcheck alerts. One has to create jobs on healthchecks.io
and add corresponding links to the env. The `Heartbeat` service will ping the URLs every 15 minutes.
There are two types of jobs:
- `HC_DOCKER_URL` - always pinged, shows that docker and the stack are up
- `HC_DATA_URL` - pinged only when a new GPS snapshot landed in HDFS since the last check (confirms that NIFI is working)
Alerts and grace time are configured on the healthcheck.io website.

## Recovery after a hard restart

If WSL or the PC was killed (crash, out of memory, Windows update), the stack
may come back up but no new data reaches HDFS. Check these in order:

1. All containers are running (none `Exited`). Docker does not restart a
   container that fails right at startup, so start it manually:

   ```bash
   docker compose --profile ingestion --profile scheduler ps -a
   docker compose up -d nifi   # example for a container that did not start
   ```

2. HDFS is not stuck in safe mode (reads work, but all writes are rejected):

   ```bash
   docker compose exec namenode hdfs dfsadmin -safemode get
   docker compose exec namenode hdfs fsck / | tail -20   # Missing/Corrupt blocks should be 0
   docker compose exec namenode hdfs dfsadmin -safemode leave
   ```

   If `fsck` shows corrupt blocks, list them with `hdfs fsck / -list-corruptfileblocks`
   before deleting anything.

3. NiFi has no errors on processors (e.g. `ContentNotFoundException` or
   `FlowFile ... is not known`). These come from broken queues. Empty all queues
   in the NiFi UI, and if errors come back, reset the queues with:

   ```bash
   bash scripts/reset-nifi.sh
   ```

   Do not stop the simulator during this, it keeps unsent GPS snapshots in memory
   (up to `GPS_BUFFER_MAX_HOURS`) and resends them when NiFi is back.

4. New GPS files appear in HDFS:

   ```bash
   docker compose exec namenode hdfs dfs -ls -R /data/raw/gps | tail -3
   ```

## Implementation status

**Implemented:** source simulation, NiFi ingestion and GTFS download, Kafka
topics, HDFS Raw, Spark cleaning to Parquet, GTFS trip/service enrichment,
the Docker batch scheduler, GPS-to-GTFS matching with estimated delays at stops,
the ML dataset (motion, time, weather and air-quality features and the target),
data-quality tooling and unit tests.

**Planned:** historical statistics and the weather-sensitivity ranking, model
training, Spark Structured Streaming and Cassandra storage. Streaming will
consume live events from Kafka and GTFS reference data from HDFS, and will reuse
the delay and feature definitions of `spark/ml`.

## Repository structure

```text
acquisition/             Upstream source simulator
docker/                  Docker images
docker-compose.yml       Infrastructure and optional services
nifi/flows/              Exported NiFi ingestion and GTFS flow
spark/batch/             Raw → Cleaned and batch orchestration
spark/downstream/        Cleaned → Enriched reference data
spark/ml/                Delay estimation, features and ML dataset (Cleaned → Enriched)
spark/streaming/         Planned streaming implementation
scripts/                 Bootstrap, pipeline jobs and service controls
scripts/analysis/        Read-only data checks and diagnostics
tests/                   Unit and integration tests on a synthetic city
docs/data-contracts.md   Sources, schemas and transformation contracts
docs/ml-dataset.md       Delay definition, ML columns, leakage rules, tests
docs/milestone 1/        Milestone 1 report and diagrams
```