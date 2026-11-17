# ML dataset: delays, features and target

This layer turns the **cleaned** Parquet data into tables a model can learn from.
It reads `/data/cleaned/*`, writes only under `/data/enriched/`.

```text
/data/cleaned/gps_observations, weather_observations, air_quality_observations, gtfs_*
        │  spark/ml/build_ml_dataset.py   (automatic stage of the 15-minute batch, or manual)
        ▼
/data/enriched/stop_visits/date=YYYY-MM-DD/          one row per (trip, vehicle, stop) visit, with its delay
/data/enriched/ml_dataset/date=YYYY-MM-DD/           one row per vehicle per minute: features + target
/data/enriched/_ml_reports/                          JSON reports (counts, baselines) for the project report
```

## Programs and scripts

| Script (`scripts/`) | Python file (`spark/ml/`) | Purpose |
|---|---|---|
| `profile-data.sh` | `profile_data.py` | Read-only data check: rows per day, GTFS feeds, **GPS `trip_id` vs GTFS match rate** |
| `build-ml-dataset.sh` | `build_ml_dataset.py` | Build `stop_visits` and `ml_dataset` (full or date-limited rebuild) |
| `diagnose-delays.sh` | `diagnose_delays.py` | Quality of the estimated delays: distributions, first/last stops, trams vs buses, baselines |
| `show-sample.sh` | `show_sample.py` | Print a few readable rows of `stop_visits` or `ml_dataset` |
| (called by the pipeline) | `ml_stage.py` | Automatic, throttled refresh inside the 15-minute batch |

Shared code: `ml_common.py` (Spark settings, distance, grid, service-day start, HDFS helpers, batch lock),
`delay_estimation.py` (delay definition), `feature_engineering.py` (features, target, weather/air joins).

## How to run

```bash
bash scripts/profile-data.sh                 # 1. check the data (run first on new data)
bash scripts/build-ml-dataset.sh             # 2. build everything that is available
bash scripts/build-ml-dataset.sh --start-date 2026-11-16 --end-date 2026-11-17
bash scripts/diagnose-delays.sh              # 3. check the quality of the delays
bash scripts/show-sample.sh ml_dataset 5     # 4. look at a few rows (or: stop_visits; --route 175)
```

Options of the build (all optional): `--stop-radius-m 75`, `--max-abs-delay-s 7200`,
`--dwell-tolerance-m 10`, `--ping-interval-s 60`, `--horizon-min 10`, `--horizon-tolerance-s 300`,
`--confirm-lag-s 30`, `--air-params PM10,PM2.5,NO2`, `--feed-version <v>` (force one GTFS feed), `--no-lock`.

Re-running a date range replaces exactly those `date=` partitions (idempotent). All scripts take the
same lock as the 15-minute cleaning scheduler, so they never read partitions while cleaning rewrites
them: if cleaning is running, the job waits (up to 45 min); if the job is running, the scheduler skips
that cycle and catches up on the next one.

## Automatic refresh (part of the normal 15-minute batch)

`spark/batch/run_pipeline.py` calls `spark/ml/ml_stage.py` as its last stage (after cleaning and GTFS
enrichment), inside the same Spark application and the same batch lock. So `bootstrap.sh`,
`run-scheduled.sh` and the Docker scheduler refresh the ML tables without any extra step.

* Only the last `ML_STAGE_DAYS` (default 2) UTC days are rebuilt, and at most every
  `ML_STAGE_MIN_INTERVAL_MIN` (default 60) minutes (a marker file
  `/data/enriched/_ml_reports/last_stage_run.json` records the last success); older partitions are left alone.
  To change the defaults, pass the variables to the container, e.g. add
  `ML_STAGE_MIN_INTERVAL_MIN: "${ML_STAGE_MIN_INTERVAL_MIN:-60}"` to the `batch_environment` block of
  `docker-compose.yml` and set the value in `.env`.
* Because of the throttle, the ML tables change about once an hour even though cleaning runs every
  15 minutes. The newest rows have no target yet (it needs a stop visit 10+ minutes later) and get it
  on the next rebuild.
* The first run after installation only covers the last 2 days: run `bash scripts/build-ml-dataset.sh`
  once to build the full history.
* A failure in this stage is logged ("ML dataset stage failed") and does **not** fail the cleaning cycle.

## Definition of delay (single source of truth: `spark/ml/delay_estimation.py`)

The feed contains no "delay" field, so delay is **estimated** from where the vehicle actually was:

1. A *stop visit* is found from the GPS pings of a vehicle on trip T that are within
   `stop_radius_m` (default 75 m) of a stop of T. Take the closest approach; if the vehicle
   *stands* at the stop (pings within `dwell_tolerance_m`, default 10 m, of the closest one),
   take the **last** ping of the stay, i.e. its departure. One visit per
   (trip, service date, vehicle, stop_sequence).
   Why: GTFS schedules departures. On the first real run, taking the first ping stamped the
   *arrival* of vehicles waiting at terminals.
2. `scheduled_time = service_day_start + departure_seconds` (falls back to `arrival_seconds`).
   `service_day_start` follows the GTFS rule *noon minus 12 h* on the service date, which equals
   Warsaw midnight except on the two daylight-saving days; times beyond 24 h such as `25:10:00` work.
3. `delay_s = visit_time − scheduled_time` in seconds. **Positive = late, negative = early.**
4. Visits with `|delay_s| > max_abs_delay_s` (2 h) are dropped as mismatches (wrong trip, wrong feed).
5. A ping is assigned to one stop only (the nearest); this handles loop routes and stops that
   occur twice in one trip.

Precision: a visit is the nearest *ping*, so the error is at most about half the interval between
two pings of that vehicle (check the real interval from the per-day counts of `profile-data.sh`).

The GTFS feed for a service date is chosen by `pick_feed_versions`: among feeds whose
`calendar_dates` contain that date, the newest feed downloaded on or before that date.

## Target and leakage rules

* **Target** `target_delay_s`: the delay at the **first stop visit at least `horizon` (10 min)
  after the row's time**, accepted only if it happens within `horizon + tolerance` (15 min).
  `target_horizon_s` is the real horizon of that row (600–900 s); `has_target` marks usable rows.
  Train only on `has_target = true`.
* `last_delay_s` (the main feature) uses only visits **at least 30 s before** the row
  (`--confirm-lag-s`): a vehicle that has just reached a stop is only recognised as having left it
  once the next pings show it moving away, and a live (streaming) job has the same lag. If the real
  ping interval is larger than 30 s, raise the lag.
* Weather and air quality come from the **same minute** as the ping.

## Columns of `ml_dataset`

| Column | Meaning |
|---|---|
| `event_time_utc`, `service_date`, `trip_id`, `side_number` | row key: vehicle `side_number` on trip `trip_id` at that minute |
| `route_id`, `route_short_name`, `route_type`, `direction_id` | from GTFS (`route_type` 0 = tram, 3 = bus) |
| `lat`, `lon` | GPS position |
| `hour_local`, `day_of_week` (Mon=0), `is_weekend`, `is_rush_hour` | Warsaw local time; rush = weekdays 7–9 and 15–18 |
| `speed_kmh`, `speed_avg_5min_kmh` | from consecutive pings; NULL if the gap is >180 s or the speed is >120 km/h (GPS glitch) |
| `last_delay_s`, `last_delay_age_s`, `delay_change_s` | delay at the last confirmed stop, seconds since then, change vs the stop before |
| `last_stop_sequence`, `trip_last_sequence`, `trip_progress` | position along the trip, 0..1 |
| `temp_air`, `temp_road`, `humidity`, `wind_speed`, `wind_dir`, `precip_intensity` | nearest weather station, same minute; out-of-range values become NULL |
| `wind_dir_sin`, `wind_dir_cos`, `is_raining` | derived from wind direction and precipitation |
| `weather_station_dist_m`, `weather_missing` | distance to that station; true if all weather values are NULL |
| `air_pm10`, `air_pm2_5`, `air_no2` (+ `_dist_m`), `air_missing` | nearest station that measures that pollutant; NULL (not 0) if unavailable |
| `target_delay_s`, `target_horizon_s`, `has_target` | see above |
| `date` | partition column (UTC event date) |

## Columns of `stop_visits`

`trip_id, service_date, side_number, stop_sequence, stop_id, route_id, route_short_name, route_type,
feed_version, visit_time_utc, scheduled_time_utc, delay_s, delay_change_s, distance_m, date`

One row per stop visit; `distance_m` is how close the chosen ping was to the stop, `delay_change_s` is
the change of delay since the vehicle's previous visit.

## Inspecting the data

* `bash scripts/show-sample.sh stop_visits 5` / `ml_dataset 5` prints the schema, the row count and a few rows.
* HDFS browser: <http://localhost:9870> → Utilities → Browse the file system → `/data/enriched`.
* Command line: `docker compose exec -T namenode hdfs dfs -ls -R /data/enriched`.
* Parquet files are binary; use Spark (the scripts above) to read them.

## Tests

Run on a laptop with Java 17 or 21: `pip install pyspark==3.5.7 pytest && python -m pytest tests -q`.
The tests build a tiny synthetic city (one trip, 12 stops, a bus with a *known* delay of `60 + 10·k` s at
stop k, plus junk pings, one weather and one air station) and check that the pipeline recovers it.

| # | Goal | Steps | Expected |
|---|---|---|---|
| 1 | Delay is recovered | run build on the synthetic lake | 12 visits, each delay within 10 s of the planted value |
| 2 | Junk is rejected | unknown trip_id + (0,0) ping in the input | no visit for the unknown vehicle; no row at lat≈0 |
| 3 | Time zone handling | scheduled time of stop 0 | 07:00:00 UTC for a 08:00 Warsaw timetable in November |
| 4 | Service-day start on DST days | `service_day_start_epoch` for 2026-03-29 / 10-25 / normal day | 22:00 / 23:00 / 23:00 UTC of the previous day |
| 5 | Target never leaks | check `target_horizon_s` | all targets 600–900 s ahead; target ≠ last delay when delay grows |
| 6 | Past feature never uses the future | check `last_delay_age_s` | ≥ 30 s everywhere |
| 7 | Weather/air attached | compare with planted values | 5 °C, 3 m/s, PM10 = 25; PM2.5/NO2 NULL (not 0) |
| 8 | Calendar features in local time | first row | hour 8–9, Monday, not weekend |
| 9 | Re-run is idempotent | build twice | identical row counts |
| 10 | Feed choice | `pick_feed_versions` with 2 feeds | newest feed ≤ date; uncovered date omitted; `forced` wins |
| 11 | Haversine | 0.01° of latitude | 1111.95 m ± 1 m |
| 12 | trip_id check | matching / prefixed ids | verdict PARTIAL / FAIL |
| 13 | Profiler summaries | GPS bbox check, GTFS stop-time summary | 1 ping outside the box; 12 stop times, 0 NULL departures |
| 14 | Cleaning: bad rows | duplicate, lat=200, empty trip_id | exactly 1 row; bearing −90 → 270 |
| 15 | GTFS times after midnight | `25:10:00`, `08:00:00`, `bad` | 90600, 28800, NULL |
| 16 | Naive Warsaw time → UTC | winter / summer timestamp | −1 h / −2 h |
| 17 | Early bus waiting at a stop | arrives 5 min early, waits, leaves on time | delay 0 (departure), not −300 |
| 18 | Passing bus | pings 20 m before / 63 m after the stop | closest ping chosen |
| 19 | Dwell tolerance 0 | identical pings, tolerance 0 | latest ping still wins |
| 20 | Automatic stage keeps other days | pipeline-like session without dynamic overwrite, extra day present | only the target day is replaced; the other day survives |
| 21 | Automatic stage is throttled | run it twice in a row | second run is skipped; caller's session settings unchanged |
| 22 | Automatic stage without data | empty lake | returns "no-data", does not raise |
