-- External tables: Hive reads Spark's Parquet files in HDFS directly.
-- Partition metadata is refreshed after batch jobs using scripts/refresh-hive.sh.
CREATE DATABASE IF NOT EXISTS cleaned;

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.gps_observations (
  record_id STRING,
  trip_id STRING,
  side_number STRING,
  service_date DATE,
  event_time_utc TIMESTAMP,
  ingested_at_utc TIMESTAMP,
  source_file STRING,
  lat DOUBLE,
  lon DOUBLE,
  bearing DOUBLE
) PARTITIONED BY (`date` DATE)
  STORED AS PARQUET
  LOCATION 'hdfs://namenode:8020/data/cleaned/gps_observations';

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.weather_observations (
  station_id STRING,
  station_name STRING,
  event_time_utc TIMESTAMP,
  ingested_at_utc TIMESTAMP,
  source_file STRING,
  lat DOUBLE,
  lon DOUBLE,
  temp_pow DOUBLE,
  temp_naw DOUBLE,
  wilg_pow DOUBLE,
  pred_wiatru DOUBLE,
  kier_wiatru DOUBLE,
  int_opadu DOUBLE
) PARTITIONED BY (`date` DATE)
  STORED AS PARQUET
  LOCATION 'hdfs://namenode:8020/data/cleaned/weather_observations';

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.air_quality_observations (
  station_id STRING,
  station_name STRING,
  param_code STRING,
  param_name STRING,
  event_time_utc TIMESTAMP,
  ingested_at_utc TIMESTAMP,
  source_file STRING,
  lat DOUBLE,
  lon DOUBLE,
  value DOUBLE,
  unit STRING
) PARTITIONED BY (`date` DATE)
  STORED AS PARQUET
  LOCATION 'hdfs://namenode:8020/data/cleaned/air_quality_observations';

-- GTFS reference tables, partitioned by immutable archive version.
CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.gtfs_routes (
  route_id STRING, route_short_name STRING, route_long_name STRING, route_type INT
) PARTITIONED BY (feed_version STRING)
  STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/cleaned/gtfs_routes';

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.gtfs_stops (
  stop_id STRING, stop_name STRING, stop_lat DOUBLE, stop_lon DOUBLE
) PARTITIONED BY (feed_version STRING)
  STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/cleaned/gtfs_stops';

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.gtfs_trips (
  trip_id STRING, route_id STRING, service_id STRING,
  trip_headsign STRING, direction_id INT
) PARTITIONED BY (feed_version STRING)
  STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/cleaned/gtfs_trips';

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.gtfs_stop_times (
  trip_id STRING, stop_sequence INT, stop_id STRING,
  arrival_time STRING, departure_time STRING,
  arrival_seconds INT, departure_seconds INT
) PARTITIONED BY (feed_version STRING)
  STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/cleaned/gtfs_stop_times';

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.gtfs_calendar (
  service_id STRING,
  monday BOOLEAN, tuesday BOOLEAN, wednesday BOOLEAN, thursday BOOLEAN,
  friday BOOLEAN, saturday BOOLEAN, sunday BOOLEAN,
  start_date DATE, end_date DATE
) PARTITIONED BY (feed_version STRING)
  STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/cleaned/gtfs_calendar';

CREATE EXTERNAL TABLE IF NOT EXISTS cleaned.gtfs_calendar_dates (
  service_id STRING, `date` DATE, exception_type INT
) PARTITIONED BY (feed_version STRING)
  STORED AS PARQUET LOCATION 'hdfs://namenode:8020/data/cleaned/gtfs_calendar_dates';
