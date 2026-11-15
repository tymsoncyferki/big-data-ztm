# Planned online path — Spark Structured Streaming

**Not implemented** in this version.

- Read live GPS, weather and air-quality records from their Kafka topics.
- Read **only timetable/reference data** from HDFS as static Parquet DataFrames
  (e.g. `gtfs_trips`, `gtfs_stop_times`, `gtfs_stops`,
  `gtfs_calendar_dates`, and stage-2 `gtfs_trip_services`).
- Match GPS observations to service, trip and scheduled stop with explicit
  route/day/time rules. GTFS clock times can exceed 24:00.
- Write suitable results to Cassandra with replay-safe keys and separate
  streaming checkpoints.

This path does not stream GPS/weather/air quality from HDFS, nor does it use
batch processing state. Updates of timetable `feed_version` must be handled
without mixing different GTFS releases. Cassandra, connector code and the
streaming job are future scope.
