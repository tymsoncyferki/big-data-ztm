# Big Data ZTM — Warsaw Public Transport

## Project overview

This project builds a Docker-based data platform for Warsaw public transport.
The Python source simulator buffers GPS observations and interpolates or carries
forward weather and air-quality measurements to emulate live data sources.
Apache NiFi receives these observations, stores JSON snapshots in HDFS Raw and
publishes individual events to Kafka. NiFi also downloads Warsaw GTFS archives.

Apache Spark processes stored data in two batch stages:

1. **Raw → Cleaned:** clean and convert incoming JSON and GTFS data into
   Snappy-compressed Parquet in HDFS.
2. **Cleaned → Enriched:** combine GTFS trip, route and service-date data into
   the `gtfs_trip_services` reference dataset.

Both stages run in every full batch cycle. The optional Docker scheduler runs
subsequent cycles every 15 minutes. Spark Structured Streaming, Cassandra,
and delay prediction are planned, not implemented.

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
- No host Python, Java, Spark or cron installation is required.

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

Targeted cleaning commands are for maintenance; the scheduled cycle runs both
batch stages. A shared lock prevents overlapping scheduled and manual runs.

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

Downloading files from the HDFS file browser redirects the browser to the DataNode (`http://datanode:9864/...`). For this to work, port 9864 has to be
exposed in `docker-compose.yml` and the `datanode` name has to point to
localhost. Add this line to the hosts file (on Windows
`C:\Windows\System32\drivers\etc\hosts`, on Linux `/etc/hosts`):

```text
127.0.0.1 datanode
```

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
/data/cleaned/_pipeline_state/          Batch state and processing markers
```

GTFS partitions must be read consistently by `feed_version`. For the complete
list of supported GTFS source files, schemas, transformations and the Enriched
join rules, see [docs/data-contracts.md](docs/data-contracts.md).

## Implementation status

**Implemented:** source simulation, NiFi ingestion and GTFS download, Kafka
topics, HDFS Raw, Spark cleaning to Parquet, GTFS trip/service enrichment,
and the Docker batch scheduler.

**Planned:** GPS-to-GTFS trip/stop matching, delay metrics, environmental
features, predictions, Spark Structured Streaming and Cassandra storage.
Streaming will consume live events from Kafka and GTFS reference data from
HDFS.

## Repository structure

```text
acquisition/             Upstream source simulator
docker/                  Docker images
docker-compose.yml       Infrastructure and optional services
nifi/flows/              Exported NiFi ingestion and GTFS flow
spark/batch/             Raw → Cleaned and batch orchestration
spark/downstream/        Cleaned → Enriched reference data
spark/streaming/         Planned streaming implementation
scripts/                 Bootstrap, manual jobs and service controls
docs/data-contracts.md   Sources, schemas and transformation contracts
docs/milestone 1/        Milestone 1 report and diagrams
```
