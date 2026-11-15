# Batch Stage 2 — GTFS enrichment

This stage builds a reference dataset of scheduled trips by service date. It
uses only GTFS files already cleaned from the Warsaw source archive.

## Input datasets

All inputs must come from the same `feed_version`:

| HDFS dataset | Source file | Used fields |
|---|---|---|
| `gtfs_trips` | `trips.txt` | Trip identifiers, line, service, direction and vehicle information |
| `gtfs_routes` | `routes.txt` | Line number, description and transport type |
| `gtfs_calendar_dates` | `calendar_dates.txt` | Service date and `exception_type` |

## Transformation

1. Keep `calendar_dates` records with `exception_type = 1` (active service).
   Records with `exception_type = 2` do not produce output records.
2. Join `trips` to active dates by `service_id`. A trip may appear on several
   service dates.
3. Join routes by `route_id` (left join, preserving trips even if route
   metadata is unavailable).
4. Select the trip, service-date and route fields defined in
   [`docs/data-contracts.md`](../../docs/data-contracts.md#8-enriched-data-model-implemented).

## Output

`/data/enriched/gtfs_trip_services/feed_version=<version>/` contains one
record per `(trip_id, service_date)` for each GTFS feed version, assuming the
GTFS source uses unique trip identifiers. It is stored as Snappy Parquet.

The output makes it possible to find a trip's line and scheduled operating
date without repeatedly joining the GTFS reference files. Stop-by-stop times
remain in `gtfs_stop_times`, and no GPS positions, weather observations,
actual delays or predictions are joined here.

## Execution

`spark/downstream/pipeline.py` runs after Raw -> Cleaned in the full batch
cycle. Previously completed feed versions are skipped using the enrichment
marker under `/data/cleaned/_pipeline_state/enriched/`. Reprocessing with
`--force` rebuilds the output for the same feed version.
