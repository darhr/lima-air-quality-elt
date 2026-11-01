"""Tests for OpenAQ discovery: location deduplication and sensor retention."""

from __future__ import annotations

from datetime import datetime

import pytest

import ingest


def location(location_id: int, name: str, sensors: list[dict]) -> dict:
    """Build the minimal OpenAQ location payload used by discovery."""
    return {
        "id": location_id,
        "name": name,
        "isMobile": False,
        "coordinates": {"latitude": -12.0, "longitude": -77.0},
        "sensors": sensors,
    }


def sensor(sensor_id: int, parameter: str) -> dict:
    """Build the minimal OpenAQ sensor payload used by discovery."""
    return {"id": sensor_id, "parameter": {"name": parameter}}


def test_discovery_keeps_all_sensors_of_a_parameter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every sensor of an enabled parameter must reach the catalog."""
    payload = {
        "results": [
            location(
                1,
                "one",
                [
                    sensor(101, "pm25"),
                    sensor(103, "pm25"),
                    sensor(201, "pm10"),
                ],
            )
        ]
    }
    monkeypatch.setattr(ingest, "api", lambda *args, **kwargs: payload)

    stations, locations = ingest.discover_stations("key")

    assert len(stations) == 1
    assert stations[0].sensors == {"pm25": [101, 103], "pm10": [201]}
    assert len(locations) == 1


def test_discovery_deduplicates_locations_by_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A location repeated within a page is kept once, with the last occurrence."""
    payload = {
        "results": [
            location(1, "first", [sensor(101, "pm25")]),
            location(2, "other", [sensor(102, "pm25")]),
            location(1, "last", [sensor(101, "pm25")]),
        ]
    }
    monkeypatch.setattr(ingest, "api", lambda *args, **kwargs: payload)

    stations, locations = ingest.discover_stations("key")

    assert [station.location_id for station in stations] == [1, 2]
    assert [station.name for station in stations] == ["last", "other"]
    assert [location["id"] for location in locations] == [1, 2]
    assert locations[0]["name"] == "last"


def test_openaq_queries_every_sensor(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ingestion must request every sensor kept by discovery."""
    requested: list[int] = []

    def fake_api(path: str, key: str, **kwargs: object) -> dict:
        """Return one hourly result for the sensor in the path."""
        sensor_id = int(path.split("/")[2])
        requested.append(sensor_id)
        return {
            "results": [{
                "period": {"datetimeFrom": {"utc": "2025-01-15T00:00:00Z"}},
                "value": sensor_id,
            }]
        }

    monkeypatch.setattr(ingest, "api", fake_api)
    station = ingest.Station(
        location_id=1,
        name="one",
        latitude=-12.0,
        longitude=-77.0,
        sensors={"pm25": [101, 102]},
    )

    rows, skipped, no_data = ingest.get_openaq(
        "key",
        datetime(2025, 1, 15),
        datetime(2025, 1, 16),
        [station],
    )

    assert requested == [101, 102]
    assert [row["sensor_id"] for row in rows] == [101, 102]
    assert skipped == []
    assert no_data == []
