"""A bus that arrives early and waits must be timed by its DEPARTURE (regression for
the terminal-stop artifact found on real data: first stops had median delay -145 s)."""
from datetime import date, datetime, timezone

from delay_estimation import detect_stop_visits

FEED, TRIP, BUS = "2026-11-16-aaaaaaaaaaaa", "T1", "1001"
STOP_LAT, STOP_LON = 52.2000, 21.0000
SCHEDULED = datetime(2026, 11, 16, 7, 0, 0, tzinfo=timezone.utc)   # 08:00 Warsaw
SCHED_SECONDS = 8 * 3600


def _epoch(h, m, s):
    return int(datetime(2026, 11, 16, h, m, s, tzinfo=timezone.utc).timestamp())


def _run(spark, pings, **kw):
    ping_rows = [(FEED, TRIP, date(2026, 11, 16), BUS, t, lat, lon) for t, lat, lon in pings]
    pdf = spark.createDataFrame(
        ping_rows, "feed_version string, trip_id string, service_date date, side_number string, "
                   "event_epoch long, lat double, lon double")
    stops = spark.createDataFrame(
        [(FEED, TRIP, 0, "s0", STOP_LAT, STOP_LON, SCHED_SECONDS)],
        "feed_version string, trip_id string, stop_sequence int, stop_id string, "
        "stop_lat double, stop_lon double, sched_seconds int")
    return detect_stop_visits(pdf, stops, **kw).collect()


def test_waiting_bus_is_timed_by_departure_not_arrival(spark):
    # arrives 06:55:00 (5 min early), stands there with 1-3 m GPS jitter until 07:00:00,
    # drives off: 40 m away at 07:00:30 (still inside the radius, but not "standing").
    waiting = [(_epoch(6, 55, 0) + 30 * i, STOP_LAT + (i % 3) * 0.00001, STOP_LON) for i in range(11)]
    leaving = [(_epoch(7, 0, 30), STOP_LAT + 0.00036, STOP_LON), (_epoch(7, 1, 0), STOP_LAT + 0.003, STOP_LON)]
    (visit,) = _run(spark, waiting + leaving)
    assert visit["visit_epoch"] == _epoch(7, 0, 0)
    assert visit["delay_s"] == 0          # on time at departure; arrival-based logic gave -300


def test_passing_bus_still_uses_closest_ping(spark):
    # 30 km/h, a ping every 10 s (83 m): pings are 20 m before and 63 m after the stop;
    # the 63 m one is not within 10 m of the closest approach, so it must not win
    pings = [(_epoch(7, 1, 0) + 10 * i, STOP_LAT - 0.00018 + i * 0.00075, STOP_LON) for i in range(4)]
    (visit,) = _run(spark, pings)
    assert visit["visit_epoch"] == _epoch(7, 1, 0)
    assert visit["delay_s"] == 60


def test_dwell_tolerance_can_be_disabled(spark):
    waiting = [(_epoch(6, 55, 0) + 30 * i, STOP_LAT, STOP_LON) for i in range(11)]
    (visit,) = _run(spark, waiting, dwell_tolerance_m=0.0)
    assert visit["visit_epoch"] == _epoch(7, 0, 0)    # identical pings: tolerance 0 keeps ties, latest wins
