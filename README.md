# Big Data ZTM — Warsaw Public Transport

A Big Data project for collecting and processing Warsaw public transport data. It uses GPS positions, weather, air-quality measurements and GTFS schedules as data sources.

## Technology

- **Python** — simulates incoming GPS, weather and air-quality data.
- **Apache NiFi** — ingests and processes incoming data.
- **Apache Kafka** — transports data streams.
- **Apache Hadoop HDFS** — stores raw data.
- **Docker Compose** — runs the project services.

## Getting started

**Requirements:** Docker with Docker Compose, Bash and an internet connection.

1. Configure the environment:

   ```bash
   cp .env.example .env
   ```

   Edit `.env` and provide the required credentials. Set `WEATHER_API_TOKEN` to enable weather and air-quality acquisition.

2. Start the infrastructure:

   ```bash
   bash scripts/bootstrap.sh --no-data
   ```

3. Start the data simulator:

   ```bash
   bash scripts/start-data.sh
   ```

## Useful commands

```bash
# Follow simulator logs
docker compose logs -f simulator

# Stop only the simulator
bash scripts/stop-data.sh

# See running services
docker compose ps

# Stop the infrastructure without deleting data
docker compose stop
```

## Web interfaces

- **NiFi:** https://localhost:8443/nifi
- **HDFS NameNode:** http://localhost:9870

## Project layout

```text
acquisition/       Python data simulator
docker/            Docker images and configuration
nifi/flows/        Exported NiFi flow definition
scripts/           Startup and initialization scripts
docker-compose.yml Infrastructure definition
```

The NiFi flow definition is stored in the repository and imported during bootstrap. Python acquisition is optional; the NiFi GTFS schedule operates independently.
