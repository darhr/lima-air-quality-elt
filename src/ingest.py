"""Source-aligned ingestion for Lima: OpenAQ (PM2.5/PM10/O3) + NASA POWER.

The raw layer keeps the original API response in the ``payload`` column.
Stations are not hardcoded: they are discovered in OpenAQ by bounding box
(``LIMA_BBOX``). Weather comes from NASA POWER for the single city point
(``LIMA_CITY_POINT``): its grid cell (radiation at 1 degree, meteorology at
about 0.5 degree) covers the whole city, so the meteorological exposure is
city-level. Only the requested parameters and variables are selected; every
sensor of a requested parameter is kept, and rows are partitioned by UTC day.
Fill values (-999.0) are kept as the source sends them. Normalization, range
filtering and flattening happen in dbt.

Exit codes: 0 ok | 2 validation | 3 rate limit exhausted | 4 source failed.
Audit: data/_state/ingest_log.jsonl + data/_state/logs/ingest-<run_id>.log.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from itertools import chain
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from exceptions import RateLimitExhausted, SourceError, die

ROOT = Path(__file__).resolve().parent.parent  # src/ -> project root


def env_value(name: str, default: str | None = None) -> str | None:
    """Read an environment variable or the ``.env`` file; ``default`` if unset."""
    value = os.environ.get(name)
    if value:
        return value
    # No dotenv dependency: the local .env is read by hand, one KEY=value line
    # per line (a leading "export " is also accepted).
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.removeprefix("export ").partition("=")
            if sep and key.strip() == name:
                return value.strip().strip("'\"")
    return default


# RAW_ROOT may be absolute (anywhere on disk) or relative to the project root.
_raw_root = Path(env_value("RAW_ROOT", "data/raw"))
RAW = _raw_root if _raw_root.is_absolute() else ROOT / _raw_root

# Audit state lives beside the raw data
STATE = ROOT / "data" / "_state"
INGEST_LOG = STATE / "ingest_log.jsonl"
LOGS = STATE / "logs"

# One id per process: every line written by this run traces back to it.
RUN_ID = uuid.uuid4().hex[:12]
RUN_LOG = LOGS / f"ingest-{RUN_ID}.log"
AQ_URL = "https://api.openaq.org/v3"
POWER_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"

# Bounding box of Metropolitan Lima: minLon,minLat,maxLon,maxLat (WGS84).
LIMA_BBOX = "-77.30,-12.35,-76.80,-11.90"
PARAMETERS = {"pm25", "pm10", "o3"}

# City point for NASA POWER: a single grid cell covers Metropolitan Lima, so
# the meteorological exposure is city-level and the key of every weather row
# is ``CITY_ID``.
CITY_ID = "lima"
LIMA_CITY_LAT = -12.05
LIMA_CITY_LON = -77.03

# Hourly variables requested from NASA POWER, with the source-native units:
# radiation W/m2, temperature C, humidity %, wind m/s and degrees,
# precipitation mm/h, pressure kPa.
POWER_VARS = (
    "ALLSKY_SFC_SW_DWN", "CLRSKY_SFC_SW_DWN", "T2M", "T2MDEW", "RH2M",
    "WS10M", "WD10M", "PRECTOTCORR", "PS",
)
# Missing values arrive as this fill and are preserved in the raw layer.
POWER_FILL = -999.0


class Station(NamedTuple):
    """Station discovered in OpenAQ inside the bbox (dynamic catalog)."""

    location_id: int
    name: str
    latitude: float
    longitude: float
    sensors: dict[str, list[int]]  # parameter (pm25/pm10/o3) -> sensor ids


# The schemas are the raw contract: they fix the types written to parquet, so
# the validation never has to check types.
AQ_SCHEMA = pa.schema([
    ("location_id", pa.int64()),
    ("location_name", pa.string()),
    ("sensor_id", pa.int64()),
    ("payload", pa.string()),
])
POWER_SCHEMA = pa.schema([
    ("city_id", pa.string()),
    ("requested_lat", pa.float64()),
    ("requested_lon", pa.float64()),
    ("payload", pa.string()),
])

# Locations snapshot: no daily partition, it is replaced once per run.
LOCATIONS_SCHEMA = pa.schema([
    ("location_id", pa.int64()),
    ("name", pa.string()),
    ("locality", pa.string()),
    ("country_code", pa.string()),
    ("timezone", pa.string()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("provider_id", pa.int64()),
    ("provider_name", pa.string()),
    ("is_mobile", pa.bool_()),
    ("is_monitor", pa.bool_()),
    ("licenses", pa.string()),  # JSON: list of licenses of the location
])


# Attempts per request: the first try plus two retries.
RETRIES = 3


def utc_now() -> str:
    """Current UTC time as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    """Run message: console + data/_state/logs/ingest-<run_id>.log."""
    line = f"{utc_now()} {message}"
    print(line)
    _write_run_log(line)


def log_detail(message: str) -> None:
    """Repetitive detail: run log only, silent on the console.

    Non-actionable findings (for example a sensor without data, which may be
    permanently retired or just paused) stay out of the console so the signals
    that need a reaction remain visible; the run log keeps the full trail.
    """
    _write_run_log(f"{utc_now()} {message}")


def _write_run_log(line: str) -> None:
    """Append one timestamped line to the run log file."""
    LOGS.mkdir(parents=True, exist_ok=True)
    with RUN_LOG.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def ingest_record(
    source: str,
    entity: str,
    day: date,
    status: str,
    rows: int,
    started_at: str,
    error: str | None = None,
) -> None:
    """Append one line per source/entity/day to ingest_log.jsonl (append-only)."""
    STATE.mkdir(parents=True, exist_ok=True)
    entry = {
        "run_id": RUN_ID,
        "source": source,
        "entity": entity,
        "dt": day.isoformat(),
        "status": status,  # ok | partial | failed
        "rows": rows,
        "started_at": started_at,
        "finished_at": utc_now(),
        "error": error,
    }
    with INGEST_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def env(name: str) -> str:
    """Required variable; aborts with exit 2 when it is missing."""
    value = env_value(name)
    if value is None:
        die(2, f"Missing {name} in .env")
    return value


def iso_dt(value: str | None) -> datetime | None:
    """ISO-8601 UTC string to naive UTC ``datetime``; ``None`` when empty."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
        timezone.utc
    ).replace(tzinfo=None)


def stamp(value: datetime) -> str:
    """Naive UTC ``datetime`` as ``YYYY-MM-DDTHH:MM:SSZ``."""
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def power_hour(key: str) -> datetime | None:
    """``YYYYMMDDHH`` hour key of a POWER payload to naive UTC ``datetime``.

    ``None`` when the key is not a valid 10-digit timestamp hour.
    """
    try:
        return datetime.strptime(key, "%Y%m%d%H")
    except (TypeError, ValueError):
        return None


def request_with_retry(
    url: str,
    *,
    headers: dict | None = None,
    params: dict | None = None,
    pacing: float = 0.0,
) -> requests.Response:
    """Unified retries for both sources.

    3 attempts on 429, 5xx and network errors, with 1 s/2 s backoff. On 429 the
    ``x-ratelimit-reset`` header is honored when present. A 4xx is returned as
    is so the caller can report the reason (it is never retried). ``pacing`` is
    the sleep after each success (OpenAQ 60 req/min; NASA POWER a moderate 1 s,
    courtesy limit with no published quota).

    When exhausted: ``RateLimitExhausted`` if any 429 was seen, else
    ``SourceError``.
    """
    delay = 1.0
    last_error = ""
    saw_429 = False
    for attempt in range(RETRIES):
        # Backoff happens only between attempts: the first try goes out now.
        if attempt:
            time.sleep(delay)
            delay *= 2
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
        except requests.RequestException as error:
            last_error = str(error)
            continue
        if response.status_code == 429:
            saw_429 = True
            last_error = "HTTP 429"
            # The header says how long the block lasts: honor it, capped at
            # 60 s plus one second of margin, or fall back to plain backoff.
            reset = response.headers.get("x-ratelimit-reset")
            try:
                delay = min(float(reset), 60) + 1 if reset else 6.0
            except ValueError:
                delay = 6.0
            continue
        if response.status_code >= 500:
            last_error = f"HTTP {response.status_code}"
            continue
        if response.status_code >= 400:
            return response  # 4xx is not transient: the caller reports it
        if pacing:
            time.sleep(pacing)  # proactive rate limit, only after a success
        return response
    # An exhausted 429 is a rate limit problem (exit 3); anything else means
    # the source never answered (exit 4).
    if saw_429:
        raise RateLimitExhausted(
            f"rate limit exhausted after {RETRIES} attempts: {url}"
        )
    raise SourceError(f"no response after {RETRIES} attempts ({last_error}): {url}")


def api(path: str, key: str, **params: str | int) -> dict:
    """GET an OpenAQ endpoint as JSON, with unified retries and 60/min pacing."""
    # 1.05 s between calls keeps the run under the 60 requests/min quota.
    response = request_with_retry(
        AQ_URL + path, headers={"X-API-Key": key}, params=params, pacing=1.05
    )
    response.raise_for_status()  # 4xx: the caller decides (404/422 = retired)
    return response.json()


def discover_stations(key: str) -> tuple[list[Station], list[dict]]:
    """Discover stations with pm25/pm10/o3 sensors inside ``LIMA_BBOX``.

    The bbox is limited to Peru (``iso=PE``) in case the box is widened outside
    the country. OpenAQ returns the official id, name and coordinates of each
    station.

    Returns ``(catalog, raw locations)``: the filtered catalog sorted by
    ``location_id``, and **all** the unfiltered bbox locations (the ones dumped
    to ``snapshot.parquet``) so that dbt can check, for example, that no mobile
    station slipped in. Both collections are deduplicated by location id; a
    location repeated across pages keeps its last occurrence. Every sensor of an
    enabled parameter is kept, so dbt can choose one per station, pollutant and
    hour.
    """
    stations_by_id: dict[int, Station] = {}
    locations_by_id: dict[int, dict] = {}
    page = 1
    while True:
        payload = api(
            "/locations", key, bbox=LIMA_BBOX, iso="PE", limit=1000, page=page
        )
        results = payload.get("results") or []
        for location in results:
            location_id = location["id"]
            # Last occurrence wins: a location repeated across pages is stored
            # once in the snapshot and once in the catalog.
            locations_by_id[location_id] = location
            if location.get("isMobile"):
                continue  # fixed stations only
            # Keep every sensor of the enabled parameters: dbt selects one per
            # station, pollutant and hour. Sorting the ids keeps the catalog
            # deterministic between runs.
            sensors: dict[str, list[int]] = {}
            for sensor in location.get("sensors") or []:
                parameter = (sensor.get("parameter") or {}).get("name")
                if parameter in PARAMETERS:
                    sensors.setdefault(parameter, []).append(sensor["id"])
            if not sensors:
                continue  # nothing in scope to query for this station
            # Coordinates are mandatory: they are part of the station identity
            # exposed by dim_station.
            coordinates = location.get("coordinates") or {}
            latitude = coordinates.get("latitude")
            longitude = coordinates.get("longitude")
            if latitude is None or longitude is None:
                continue
            stations_by_id[location_id] = Station(
                location_id=location_id,
                name=location.get("name") or f"location {location_id}",
                latitude=float(latitude),
                longitude=float(longitude),
                sensors={
                    parameter: sorted(set(sensor_ids))
                    for parameter, sensor_ids in sensors.items()
                },
            )
        if len(results) < 1000:
            break  # short page: there are no more results
        page += 1
    # Sorted by natural key so the catalog and the snapshot are deterministic
    # between runs.
    stations = sorted(stations_by_id.values(), key=lambda station: station.location_id)
    if not stations:
        die(2, f"OpenAQ: no pm25/pm10/o3 stations in bbox {LIMA_BBOX}")
    locations = sorted(locations_by_id.values(), key=lambda location: location["id"])
    return stations, locations


def write_snapshot(locations: list[dict]) -> int:
    """Rewrite ``raw/openaq/locations/snapshot.parquet`` (atomic write).

    One row per location returned in the bbox, with the metadata the catalog
    discards (``locality``, ``timezone``, ``provider``, ``licenses``): it is
    the source of ``dim_station``. No ingestion timestamps: its content
    depends only on the source.
    """
    rows: list[dict] = []
    for location in locations:
        coordinates = location.get("coordinates") or {}
        provider = location.get("provider") or {}
        country = location.get("country") or {}
        licenses = location.get("licenses")
        # Flatten the nested location object into the snapshot contract; keys
        # the API does not send stay null and the license list becomes JSON.
        rows.append({
            "location_id": location.get("id"),
            "name": location.get("name"),
            "locality": location.get("locality"),
            "country_code": country.get("code"),
            "timezone": location.get("timezone"),
            "latitude": coordinates.get("latitude"),
            "longitude": coordinates.get("longitude"),
            "provider_id": provider.get("id"),
            "provider_name": provider.get("name"),
            "is_mobile": location.get("isMobile"),
            "is_monitor": location.get("isMonitor"),
            "licenses": (
                json.dumps(licenses, ensure_ascii=False, separators=(",", ":"))
                if licenses is not None
                else None
            ),
        })
    # A null id would break the sort of integers, so it is pushed to the front.
    rows.sort(key=lambda row: row["location_id"] or -1)
    path = RAW / "openaq" / "locations" / "snapshot.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write a temporary file and rename it: a reader can only see the previous
    # snapshot or the new one, never half of it.
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(
        pa.Table.from_pylist(rows, schema=LOCATIONS_SCHEMA), tmp, compression="zstd"
    )
    os.replace(tmp, path)
    return len(rows)


def get_openaq(
    key: str, start: datetime, end: datetime, stations: list[Station]
) -> tuple[list[dict], list[str], list[str]]:
    """Return (raw sensor-hour rows, 404/422 skips, no-data messages)."""
    rows: list[dict] = []
    skipped: list[str] = []
    no_data: list[str] = []
    for station in stations:
        for sensor_id in chain.from_iterable(station.sensors.values()):
            page = 1
            # A 404/422 marks the sensor as skipped so it is not reported again
            # as "no data": it is gone, not empty.
            skipped_sensor = False
            sensor_rows = 0
            while True:
                try:
                    payload = api(
                        f"/sensors/{sensor_id}/hours",
                        key,
                        datetime_from=stamp(start),
                        datetime_to=stamp(end),
                        limit=1000,
                        page=page,
                    )
                except requests.HTTPError as error:
                    response = error.response
                    status = response.status_code if response is not None else None
                    if status in (404, 422):
                        # retired sensor or no hours: record it and continue
                        skipped.append(
                            f"{station.location_id}/sensor {sensor_id}: HTTP {status}"
                        )
                        skipped_sensor = True
                        break
                    raise
                results = payload.get("results") or []
                for item in results:
                    when = iso_dt(
                        ((item.get("period") or {}).get("datetimeFrom") or {}).get("utc")
                    )
                    # The API may return rows outside the requested window:
                    # keep the half-open interval that defines the partition.
                    if when is None or not (start <= when < end):
                        continue
                    # _ts and _day are helpers for sorting, grouping and
                    # validation; the schema does not store them.
                    rows.append({
                        "location_id": station.location_id,
                        "location_name": station.name,
                        "sensor_id": sensor_id,
                        "payload": json.dumps(
                            item, ensure_ascii=False, separators=(",", ":")
                        ),
                        "_ts": when,
                        "_day": when.date(),
                    })
                    sensor_rows += 1
                if len(results) < 1000:
                    break  # short page: the window is fully paginated
                page += 1
            if not skipped_sensor and sensor_rows == 0:
                # No rows in the window: nothing to impute, but warn about it.
                no_data.append(
                    f"sensor {sensor_id} ({station.name}) has no data in "
                    f"{start.date()}..{(end - timedelta(days=1)).date()}"
                )
    # Natural key order (sensor, timestamp) so the partition is deterministic.
    rows.sort(key=lambda row: (row["sensor_id"], row["_ts"]))
    return rows, skipped, no_data


def get_nasapower(start: date, end: date) -> list[dict]:
    """Return one raw payload per city-day, without replacing fill values.

    One request per day (``start = end`` in ``YYYYMMDD``): the response stays
    inside the UTC day and the daily partitions remain independent, so a range
    can be retried day by day. The payload keeps the source structure
    (``geometry`` + ``parameter``) with the original parameter names, units and
    fill values; the transient ``header``/``messages`` fields are dropped to
    keep the file idempotent.
    """
    rows: list[dict] = []
    for i in range((end - start).days + 1):
        day = start + timedelta(days=i)
        stamp_day = day.strftime("%Y%m%d")
        response = request_with_retry(
            POWER_URL,
            params={
                "parameters": ",".join(POWER_VARS),
                "community": "RE",
                "latitude": LIMA_CITY_LAT,
                "longitude": LIMA_CITY_LON,
                "start": stamp_day,
                "end": stamp_day,
                "time-standard": "UTC",
            },
            pacing=1.0,
        )
        # An error body is usually a JSON with "messages": parse defensively
        # and report it below instead of crashing here.
        try:
            data = response.json()
        except ValueError:
            data = {}
        parameter = (data.get("properties") or {}).get("parameter") or {}
        if response.status_code != 200 or not parameter:
            messages = data.get("messages")
            reason = "; ".join(messages) if messages else response.text[:200]
            raise SourceError(f"NASA POWER: {reason}")
        rows.append({
            "city_id": CITY_ID,
            "requested_lat": LIMA_CITY_LAT,
            "requested_lon": LIMA_CITY_LON,
            "payload": json.dumps(
                {
                    "geometry": data.get("geometry"),
                    "parameter": parameter,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "_day": day,
        })
    # Natural key order (city, day) so the partition is deterministic.
    rows.sort(key=lambda row: row["_day"])
    return rows


def write_day(
    source: str, entity: str, day: date, rows: list[dict], schema: pa.Schema
) -> int:
    """Atomically write one daily partition; an empty day keeps the full schema."""
    part = [row for row in rows if row["_day"] == day]
    # Project onto the schema: the helper keys (_ts, _day) are used for
    # grouping and validation but never stored.
    clean = [{name: row.get(name) for name in schema.names} for row in part]
    path = RAW / source / entity / f"dt={day.isoformat()}" / "part-000.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic replace: a crash mid-write leaves the previous partition (or none)
    # in place, and an empty day still carries the full schema.
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(
        pa.Table.from_pylist(clean, schema=schema), tmp, compression="zstd"
    )
    os.replace(tmp, path)
    return len(part)


def summarize(problems: list[str]) -> str:
    """Log detail: up to 10 problems, the rest as a count."""
    head = problems[:10]
    text = "\n  - ".join(head)
    if len(problems) > len(head):
        text += f"\n  - ... and {len(problems) - len(head)} more"
    return text


def validate_day(
    source: str,
    day: date,
    rows: list[dict],
    station_ids: set[int],
) -> tuple[list[str], list[str]]:
    """(errors, warnings) of the partition before it is written.

    ERROR => the partition is not written (exit 2 and a ``failed`` record).
    WARN  => it is written anyway and noted in the run log.

    Validation **never drops or modifies rows**: it only decides whether to
    write. Units, fill values and physical ranges are not validated (they live
    in dbt).
    """
    errors: list[str] = []
    warnings: list[str] = []

    def payload_of(row: dict, index: int) -> dict | None:
        """Parsed payload of a row, or ``None`` after recording an error."""
        raw = row.get("payload")
        if not raw:
            errors.append(f"row {index}: payload is null or empty")
            return None
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            errors.append(f"row {index}: payload is not valid JSON")
            return None
        if not isinstance(data, dict):
            errors.append(f"row {index}: payload is not a JSON object")
            return None
        return data

    if source == "openaq":
        # The station key comes from our discovery, not from the source.
        for index, row in enumerate(rows):
            location_id = row.get("location_id")
            if location_id is None:
                errors.append(f"row {index}: location_id is null")
            elif location_id not in station_ids:
                errors.append(
                    f"row {index}: location_id {location_id} is outside the catalog"
                )
            if not row.get("location_name"):
                warnings.append(f"row {index}: location_name is empty")

        # (sensor_id, utc) must be unique inside the partition.
        seen: set[tuple] = set()
        for index, row in enumerate(rows):
            sensor_id = row.get("sensor_id")
            if sensor_id is None:
                errors.append(f"row {index}: sensor_id is null")
            data = payload_of(row, index)
            if data is None:
                continue
            utc = ((data.get("period") or {}).get("datetimeFrom") or {}).get("utc")
            if not utc:
                errors.append(f"row {index}: payload has no period.datetimeFrom.utc")
                continue
            if not str(utc).startswith(day.isoformat()):
                errors.append(
                    f"row {index}: {utc} is outside partition {day.isoformat()}"
                )
            key = (sensor_id, utc)
            if key in seen:
                errors.append(f"row {index}: duplicate natural key {key}")
            seen.add(key)

    if source == "nasapower":
        # One weather row per city per day; the city key is ours.
        seen_ids: set = set()
        for index, row in enumerate(rows):
            city_id = row.get("city_id")
            if city_id is None:
                errors.append(f"row {index}: city_id is null")
            elif city_id != CITY_ID:
                errors.append(
                    f"row {index}: city_id {city_id!r} is outside the catalog"
                )
            if city_id in seen_ids:
                errors.append(
                    f"row {index}: city_id {city_id} duplicated within the day"
                )
            seen_ids.add(city_id)
            if row.get("requested_lat") is None or row.get("requested_lon") is None:
                warnings.append(f"row {index}: requested_lat/lon are null")
            data = payload_of(row, index)
            if data is None:
                continue
            parameter = data.get("parameter")
            if not isinstance(parameter, dict):
                errors.append(f"row {index}: payload.parameter is not a JSON object")
                continue
            # Every variable must be a mapping of valid hour keys of this day
            # to numbers, and all of them must cover the same hours.
            keys_by_var: dict[str, set[str]] = {}
            for variable in POWER_VARS:
                values = parameter.get(variable)
                if not isinstance(values, dict):
                    errors.append(f"row {index}: {variable} is not a JSON object")
                    keys_by_var[variable] = set()
                    continue
                hours: set[str] = set()
                fills = 0
                for key, value in values.items():
                    when = power_hour(key)
                    if when is None:
                        errors.append(
                            f"row {index}: {variable} has an invalid hour key {key!r}"
                        )
                        continue
                    if when.date() != day:
                        errors.append(
                            f"row {index}: {variable} hour {key} is outside day "
                            f"{day.isoformat()}"
                        )
                        continue
                    if isinstance(value, bool) or not isinstance(value, (int, float)):
                        errors.append(
                            f"row {index}: {variable}[{key}] is not a number"
                        )
                        continue
                    if value == POWER_FILL:
                        fills += 1
                    hours.add(key)
                if hours and fills * 2 > len(hours):
                    warnings.append(
                        f"row {index}: {variable} has {fills}/{len(hours)} fill values"
                    )
                keys_by_var[variable] = hours
            if len({frozenset(hours) for hours in keys_by_var.values()}) > 1:
                errors.append(f"row {index}: variables cover different hour keys")

    return errors, warnings
