"""Synchronized *source simulator* for Warsaw transit data.

GPS snapshots are fetched every 10 seconds and sent unchanged 15 minutes
later (measured from receipt). Weather and Warsaw-only air quality are polled
every minute, then emitted every 30 seconds for event time 15 minutes ago.
Environmental values are interpolated if a later source observation is known;
otherwise the latest reading at/before the target is carried forward.

This script only simulates upstream inputs. NiFi remains responsible for
validation, duplicate detection, archiving to HDFS and publishing to Kafka.

Commands:
  python acquisition/simulator.py --check   # one request to each API
  python acquisition/simulator.py --demo    # entirely offline example
  python acquisition/simulator.py --run     # buffer/replay, print counts only
  python acquisition/simulator.py --run --send  # POST to configured NiFi paths
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
import threading

LOG = logging.getLogger("source_simulator")
WARSAW = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc

GPS_URL = "https://mkuran.pl/gtfs/warsaw/positions.json"
WEATHER_URL = "https://dane.um.warszawa.pl/api/action/get_zom_pogoda"
AIR_URL = "https://dane.um.warszawa.pl/api/action/get_bopipk_monitoring_jakosci_powietrza"

GPS_POLL_SECONDS = 10
WEATHER_POLL_SECONDS = 60
AIR_POLL_SECONDS = 60
BUFFER_SECONDS = 15 * 60
ENVIRONMENT_SECONDS = 30
RETRY_SECONDS = 5
HISTORY_HOURS = 8  # Retain a previous reading for carry-forward

WEATHER_FIELDS = (
    "temp_pow", "temp_naw", "wilg_pow", "pred_wiatru", "kier_wiatru", "int_opadu"
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_time(value: str, *, naive_zone: ZoneInfo = WARSAW) -> datetime:
    """Convert ISO timestamps or 'YYYY-MM-DD HH:MM:SS' to UTC.

    Assumes *naive* Warsaw API timestamps denote Europe/Warsaw local time.
    Verify this assumption with the data provider before production use.
    """
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=naive_zone)
    return dt.astimezone(UTC)


def local_time_text(moment: datetime) -> str:
    return moment.astimezone(WARSAW).strftime("%Y-%m-%d %H:%M:%S")


def floor_seconds(moment: datetime, seconds: int) -> datetime:
    timestamp = int(moment.timestamp())
    return datetime.fromtimestamp(timestamp - timestamp % seconds, tz=UTC)


def ceil_seconds(moment: datetime, seconds: int) -> datetime:
    timestamp = moment.timestamp()
    rounded = math.ceil(timestamp / seconds) * seconds
    return datetime.fromtimestamp(rounded, tz=UTC)


def numeric(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def lerp(a: float, b: float, fraction: float) -> float:
    return a + (b - a) * fraction


def lerp_degrees(a: float, b: float, fraction: float) -> float:
    # Interpolate across 0/360 instead of crossing 180 (e.g., 350 -> 10).
    delta = (b - a + 180) % 360 - 180
    return (a + delta * fraction) % 360


def previous_and_next(history: dict[datetime, dict], target: datetime):
    """Last reading <= target; optionally first later reading (> target).

    Future-only readings must not be used as a fallback for an earlier target.
    """
    before = max((t for t in history if t <= target), default=None)
    if before is None:
        return None
    after = min((t for t in history if t > target), default=None)
    return before, history[before], after, history[after] if after is not None else None


class Sources:
    def __init__(self, token: str | None):
        self.token = token
        # Separate HTTP session per worker: requests.Session is not thread-safe.
        self._thread_local = threading.local()

    def _session(self) -> requests.Session:
        if not hasattr(self._thread_local, "session"):
            self._thread_local.session = requests.Session()
        return self._thread_local.session

    def _post(self, url: str):
        if not self.token:
            raise RuntimeError("Set WEATHER_API_TOKEN before using Warsaw APIs")
        response = self._session().post(
            url, headers={"Authorization": self.token}, timeout=25
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError(f"Expected list from {url}, got {type(data).__name__}")
        return data

    def gps(self) -> dict:
        response = self._session().get(GPS_URL, timeout=25)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not isinstance(data.get("positions"), list):
            raise ValueError("GPS API response has no positions list")
        if "time" not in data:
            raise ValueError("GPS API response has no snapshot time")
        return data

    def weather(self) -> list[dict]:
        return self._post(WEATHER_URL)

    def air(self) -> list[dict]:
        # The API includes stations outside Warsaw. Keep only those whose
        # declared city is Warsaw; station names alone are not reliable.
        stations = self._post(AIR_URL)
        return [
            station for station in stations
            if isinstance(station, dict)
            and isinstance(station.get("address"), dict)
            and str(station["address"].get("city") or "").strip().casefold() == "warszawa"
        ]


class SourceHistory:
    """Keeps raw source observations in memory for interpolation."""

    def __init__(self):
        self.weather: dict[str, dict[datetime, dict]] = defaultdict(dict)
        self.air: dict[tuple[str, str], dict[datetime, dict]] = defaultdict(dict)
        self.air_station_metadata: dict[str, dict] = {}
        # monotonic receipt time, NOT source timestamp: delay exactly 15 min
        # after the snapshot arrives, retaining its original payload.
        self.gps: deque[tuple[float, dict]] = deque()

    def add_gps(self, snapshot: dict, *, received_at: float | None = None) -> None:
        self.gps.append((time.monotonic() if received_at is None else received_at, snapshot))

    def add_weather(self, records: list[dict]) -> None:
        for record in records:
            try:
                station = str(record["lokalizacja"])
                t = parse_time(record["czas"])
            except (KeyError, TypeError, ValueError) as exc:
                LOG.warning("Skipping malformed weather station: %s", exc)
                continue
            self.weather[station][t] = record

    def add_air(self, stations: list[dict]) -> None:
        for station in stations:
            name = station.get("name")
            if not name:
                continue
            # 'ijp' is an observed categorical index, not interpolatable.
            self.air_station_metadata[name] = {
                k: v for k, v in station.items() if k not in {"data", "ijp"}
            }
            for reading in station.get("data") or []:
                if not isinstance(reading, dict):
                    continue
                code = reading.get("param_code")
                if not code:
                    continue
                if numeric(reading.get("value")) is None:
                    continue
                try:
                    t = parse_time(reading["time"])
                except (KeyError, TypeError, ValueError):
                    continue
                self.air[(name, str(code))][t] = reading

    def interpolated_weather(self, at: datetime) -> list[dict]:
        """30-s snapshot: interpolate if possible, else carry forward last known."""
        stations = []
        for history in self.weather.values():
            pair = previous_and_next(history, at)
            if pair is None:
                continue
            _, earlier, _, _ = pair
            out = {
                "lokalizacja": earlier["lokalizacja"],
                "lat": earlier.get("lat"),
                "lon": earlier.get("lon"),
                "czas": local_time_text(at),
            }
            for key in WEATHER_FIELDS:
                # Each sensor field may be null independently. Locate its own
                # last *valid* observation rather than propagating a newer null.
                numeric_times_before = [t for t, obs in history.items()
                                        if t <= at and numeric(obs.get(key)) is not None]
                if not numeric_times_before:
                    out[key] = None
                    continue
                field_before = max(numeric_times_before)
                a = numeric(history[field_before][key])
                numeric_times_after = [t for t, obs in history.items()
                                       if t > at and numeric(obs.get(key)) is not None]
                if not numeric_times_after:
                    out[key] = a
                    continue
                field_after = min(numeric_times_after)
                b = numeric(history[field_after][key])
                fraction = (at - field_before).total_seconds() / (field_after - field_before).total_seconds()
                if key == "kier_wiatru":
                    out[key] = round(lerp_degrees(a, b, fraction), 4)
                else:
                    out[key] = round(lerp(a, b, fraction), 5)
            stations.append(out)
        return stations

    def interpolated_air(self, at: datetime) -> list[dict]:
        """Independently interpolate/carry forward each station/pollutant."""
        by_station: dict[str, list[dict]] = defaultdict(list)
        for (name, code), history in self.air.items():
            pair = previous_and_next(history, at)
            if pair is None:
                continue
            t0, earlier, t1, later = pair
            a = numeric(earlier.get("value"))
            if a is None:
                continue
            b = numeric(later.get("value")) if later is not None else None
            if t1 is not None and b is not None and earlier.get("unit") == later.get("unit"):
                fraction = (at - t0).total_seconds() / (t1 - t0).total_seconds()
                value = round(lerp(a, b, fraction), 6)
            else:
                value = a  # no later sample / unit changed: last observed value
            by_station[name].append({
                "param_code": code,
                "param_name": earlier.get("param_name"),
                "value": value,
                "time": local_time_text(at),
                "unit": earlier.get("unit"),
            })
        result = []
        for name, readings in by_station.items():
            out = dict(self.air_station_metadata.get(name, {}))
            out["name"] = name
            out["data"] = readings
            result.append(out)
        return result

    def forget_older_than(self, cutoff: datetime) -> None:
        # Never discard waiting GPS snapshots here: it would silently lose input.
        for collection in (self.weather, self.air):
            for history in collection.values():
                # Retain the latest observation before cutoff for bracketing.
                previous = max((t for t in history if t < cutoff), default=None)
                for t in list(history):
                    if t < cutoff and t != previous:
                        del history[t]


class Simulator:
    def __init__(self, *, send: bool):
        token = os.getenv("WEATHER_API_TOKEN") or os.getenv("WARSAW_API_TOKEN")
        self.sources = Sources(token)
        self.history = SourceHistory()
        self.send = send
        self.nifi_session = requests.Session()
        self.urls = {
            "gps": os.getenv("NIFI_GPS_URL", "http://localhost:8081/gps"),
            "weather": os.getenv("NIFI_WEATHER_URL", "http://localhost:8082/weather"),
            "air_quality": os.getenv("NIFI_AIR_URL", "http://localhost:8083/air-quality"),
        }
        # Every environmental output is scheduled for a wall-clock boundary,
        # but its measurement time is precisely 15 minutes in the past.
        self.started_at = utc_now()
        first_send = self.started_at + timedelta(seconds=BUFFER_SECONDS)
        self.next_env_send = {
            source: ceil_seconds(first_send, ENVIRONMENT_SECONDS)
            for source in ("weather", "air_quality")
        }
        self.retry_after = {"gps": 0.0, "weather": 0.0, "air_quality": 0.0}

    def publish(self, source: str, payload: dict) -> bool:
        count = len(payload.get("positions", payload.get("stations", [])))
        if not self.send:
            LOG.info("PREVIEW %-11s records=%d", source, count)
            return True
        try:
            response = self.nifi_session.post(self.urls[source], json=payload, timeout=30)
            response.raise_for_status()
            LOG.info("SENT %-11s status=%s records=%d", source, response.status_code, count)
            return True
        except requests.RequestException as exc:
            LOG.error("NiFi %s failed; will retry: %s", source, exc)
            return False

    def emit_due_gps(self, now_mono: float) -> None:
        """Send each unchanged GPS snapshot >= 900 s after its receipt."""
        if now_mono < self.retry_after["gps"]:
            return
        while self.history.gps and self.history.gps[0][0] + BUFFER_SECONDS <= now_mono:
            _, snapshot = self.history.gps[0]
            if self.publish("gps", snapshot):
                # Remove only the successfully posted snapshot. No duplicates on retry.
                self.history.gps.popleft()
            else:
                self.retry_after["gps"] = now_mono + RETRY_SECONDS
                return

    def emit_due_environment(self, now_wall: datetime, now_mono: float) -> None:
        """Emit each 30-s grid tick independently for weather and air quality."""
        for source in ("weather", "air_quality"):
            if now_mono < self.retry_after[source]:
                continue
            # On temporary NiFi failure, retain this stream's tick for retry.
            # Do not stall GPS or the other environment stream.
            while self.next_env_send[source] <= now_wall:
                target = self.next_env_send[source] - timedelta(seconds=BUFFER_SECONDS)
                stations = (self.history.interpolated_weather(target)
                            if source == "weather" else self.history.interpolated_air(target))
                # If there is not even a previous observation, send an empty
                # snapshot instead of inventing values or blocking indefinitely.
                if not stations:
                    LOG.warning("No historical %s readings at %s; sending empty snapshot",
                                source, target.isoformat())
                if self.publish(source, {"stations": stations}):
                    self.next_env_send[source] += timedelta(seconds=ENVIRONMENT_SECONDS)
                else:
                    self.retry_after[source] = now_mono + RETRY_SECONDS
                    break

    def run(self) -> None:
        LOG.info("Polling GPS / weather / air every %s / %s / %s seconds",
                 GPS_POLL_SECONDS, WEATHER_POLL_SECONDS, AIR_POLL_SECONDS)
        LOG.info("Buffer: %s min; GPS replay after receipt; weather + air every %ss; mode=%s",
                 BUFFER_SECONDS // 60, ENVIRONMENT_SECONDS,
                 "SEND" if self.send else "PREVIEW")
        # Fetching is concurrent so a slow environmental API does not prevent
        # 10-second GPS polling or the independent replay/publishing loop.
        schedules = {
            "gps": {"next": 0.0, "interval": GPS_POLL_SECONDS,
                    "fetcher": self.sources.gps, "future": None},
            "weather": {"next": 0.0, "interval": WEATHER_POLL_SECONDS,
                        "fetcher": self.sources.weather, "future": None},
            "air": {"next": 0.0, "interval": AIR_POLL_SECONDS,
                    "fetcher": self.sources.air, "future": None},
        }
        next_cleanup = time.monotonic() + 60
        with ThreadPoolExecutor(max_workers=3) as pool:
            while True:
                now = time.monotonic()
                for name, schedule in schedules.items():
                    future = schedule["future"]
                    if future is not None and future.done():
                        try:
                            payload = future.result()
                            if name == "gps":
                                self.history.add_gps(payload, received_at=time.monotonic())
                                count = len(payload["positions"])
                            elif name == "weather":
                                self.history.add_weather(payload)
                                count = len(payload)
                            else:
                                self.history.add_air(payload)
                                count = len(payload)
                            LOG.info("Fetched %-7s count=%d", name, count)
                        except Exception as exc:
                            # External APIs can return unexpected shapes; a
                            # failed poll must not terminate the replay loop.
                            LOG.warning("Failed to fetch %s: %s", name, exc)
                        schedule["future"] = None
                    if schedule["future"] is None and now >= schedule["next"]:
                        schedule["future"] = pool.submit(schedule["fetcher"])
                        schedule["next"] = now + schedule["interval"]
                self.emit_due_gps(time.monotonic())
                self.emit_due_environment(utc_now(), time.monotonic())
                if time.monotonic() >= next_cleanup:
                    self.history.forget_older_than(utc_now() - timedelta(hours=HISTORY_HOURS))
                    next_cleanup = time.monotonic() + 60
                time.sleep(0.2)


def check_once() -> None:
    sources = Sources(os.getenv("WEATHER_API_TOKEN") or os.getenv("WARSAW_API_TOKEN"))
    for label, fetcher in (("gps", sources.gps), ("weather", sources.weather), ("air_quality", sources.air)):
        try:
            result = fetcher()
            if label == "gps":
                print(f"{label}: {len(result['positions'])} positions; snapshot_time={result['time']}")
            else:
                print(f"{label}: {len(result)} stations")
                if label == "air_quality" and result:
                    print("  first station keys:", sorted(result[0]))
                    print("  first station data entries:", len(result[0].get("data") or []))
        except Exception as exc:
            print(f"{label}: ERROR {exc}")


def demo() -> None:
    # Small, entirely offline example of the interpolation logic.
    history = SourceHistory()
    history.add_weather([
        {"lokalizacja": "Stacja Bielany", "lat": 52.29, "lon": 20.97,
         "czas": "2026-11-15 00:00:00", "temp_pow": 2, "temp_naw": 1,
         "wilg_pow": 90, "pred_wiatru": 1, "kier_wiatru": 350, "int_opadu": 0},
        {"lokalizacja": "Stacja Bielany", "lat": 52.29, "lon": 20.97,
         "czas": "2026-11-15 00:10:00", "temp_pow": 4, "temp_naw": 3,
         "wilg_pow": 94, "pred_wiatru": 2, "kier_wiatru": 10, "int_opadu": 2},
    ])
    history.add_air([
        {"name": "Warszawa-Grochowska", "lat": 52.24, "lon": 21.08,
         "data": [{"param_code": "PM10", "param_name": "pył PM10", "value": 8,
                   "time": "2026-11-15 00:00:00", "unit": "µg/m3"}]},
        {"name": "Warszawa-Grochowska", "lat": 52.24, "lon": 21.08,
         "data": [{"param_code": "PM10", "param_name": "pył PM10", "value": 12,
                   "time": "2026-11-15 00:10:00", "unit": "µg/m3"}]},
    ])
    t = parse_time("2026-11-15 00:05:00")
    import json
    print(json.dumps({"weather": history.interpolated_weather(t),
                      "air_quality": history.interpolated_air(t)},
                     indent=2, ensure_ascii=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Fetch each real API once")
    mode.add_argument("--demo", action="store_true", help="Demonstrate interpolation offline")
    mode.add_argument("--run", action="store_true", help="Run source simulator continuously")
    parser.add_argument("--send", action="store_true", help="Actually send to NiFi (default: preview)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.check:
        check_once()
    elif args.demo:
        demo()
    else:
        Simulator(send=args.send).run()


if __name__ == "__main__":
    main()
