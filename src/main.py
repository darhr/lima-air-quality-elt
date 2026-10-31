"""CLI entrypoint of the ingestion: arguments, run orchestration, exit codes.

Usage (from the project root):

    python src/main.py --date 2024-05-15
    python src/main.py --start 2024-05-01 --end 2024-05-31

``--date D`` is the same as ``--start D --end D``; ``--source`` limits the run
to one of the sources. Exit codes: 0 ok, 2 validation, 3 rate limit exhausted,
4 one source failed. Audit: data/_state/ingest_log.jsonl +
data/_state/logs/ingest-<run_id>.log.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone

from exceptions import RateLimitExhausted, die
from ingest import (
    AQ_SCHEMA,
    LIMA_BBOX,
    POWER_SCHEMA,
    discover_stations,
    env,
    env_value,
    get_nasapower,
    get_openaq,
    ingest_record,
    log,
    log_detail,
    summarize,
    utc_now,
    validate_day,
    write_day,
    write_snapshot,
)


def main() -> int:
    """Parse the arguments, run the requested sources and return the exit code."""
    parser = argparse.ArgumentParser(description="Source-aligned ingestion for Lima")
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--source", choices=("all", "openaq", "nasapower"), default="all")
    args = parser.parse_args()

    if args.date:
        start = end = args.date
    elif args.start and args.end:
        start, end = args.start, args.end
    else:
        parser.error("provide --date D or --start S --end E")
    if start > end:
        parser.error("--start cannot be later than --end")

    span = (end - start).days + 1
    if span > 31:
        log(
            f"warning: range of {span} days ({start}..{end}) exceeds 31 days; "
            "requested in one call and written as daily partitions"
        )

    # Sources publish with different delays: NASA POWER meteorology (MERRA-2)
    # is ready in ~3 days but its hourly radiation (CERES) only after ~2
    # months, so the lag follows the slowest variable: a partition is only
    # ingested once every variable of the day is final.
    if args.source in ("all", "nasapower"):
        lag_raw = env_value("INGEST_LAG_DAYS", "70")
        try:
            lag = int(lag_raw or "70")
        except ValueError:
            die(2, f"INGEST_LAG_DAYS is not an integer: {lag_raw!r}")
            raise  # unreachable: die() never returns
        latest = datetime.now(timezone.utc).date() - timedelta(days=lag)
        if end > latest:
            die(
                2,
                f"NASA POWER publishes with ~{lag} days of delay "
                f"(INGEST_LAG_DAYS): --end {end} is after {latest} "
                f"(today_utc - {lag}). Use an earlier date or lower INGEST_LAG_DAYS.",
            )

    # OpenAQ takes datetimes (end exclusive) and NASA POWER takes dates; both
    # windows cover the same inclusive range of days.
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    aq_start = datetime.combine(start, datetime.min.time())
    aq_end = datetime.combine(end + timedelta(days=1), datetime.min.time())

    # The discovery runs on every run, even with --source nasapower: the
    # snapshot is refreshed from the same call.
    key = env("OPENAQ_API_KEY")
    try:
        stations, locations = discover_stations(key)
    except RateLimitExhausted as error:
        die(3, str(error))
    except Exception as error:
        die(4, f"station discovery failed: {error}")
    log(f"stations in bbox {LIMA_BBOX} (Metropolitan Lima): {len(stations)}")
    try:
        snapshot_rows = write_snapshot(locations)
    except Exception as error:
        die(4, f"locations snapshot failed: {error}")
    log(f"snapshot openaq/locations: {snapshot_rows} locations")
    # The catalog is the allowlist that every partition is validated against.
    station_ids = {station.location_id for station in stations}

    # Sources are independent: a failure is recorded here and reported at the
    # end instead of aborting the other source.
    failures: dict[str, Exception] = {}

    if args.source in ("all", "openaq"):
        started = utc_now()
        # Fetch first and write only in the else branch: when the source fails,
        # no partition of this source is touched.
        try:
            rows, skipped, no_data = get_openaq(key, aq_start, aq_end, stations)
        except Exception as error:
            failures["openaq"] = error
            for day in days:
                ingest_record(
                    "openaq", "measurements", day, "failed", 0, started, str(error)
                )
        else:
            # Per-sensor detail goes to the run log only; sensors are still
            # queried every run and one that resumes reporting shows up in the
            # rows again.
            for entry in skipped:
                log_detail(f"sensor skipped ({entry})")
            for entry in no_data:
                log_detail(f"warning openaq: {entry}")
            if skipped or no_data:
                log(
                    f"openaq: {len(skipped)} sensor(s) skipped, "
                    f"{len(no_data)} with no data (detail in the run log)"
                )
            # Skipped sensors degrade the day to partial, never to failed.
            note = (
                f"{len(skipped)} sensor(s) skipped with 404/422"
                if skipped
                else None
            )
            # Each day is independent: validate first, so a bad day aborts with
            # exit 2 leaving its previous partition untouched.
            for day in days:
                part = [row for row in rows if row["_day"] == day]
                errors, warnings = validate_day("openaq", day, part, station_ids)
                for warning in warnings:
                    log(f"warning openaq {day}: {warning}")
                if errors:
                    detail = summarize(errors)
                    ingest_record(
                        "openaq", "measurements", day, "failed", 0, started,
                        f"{len(errors)} validation error(s): {detail}",
                    )
                    die(
                        2,
                        f"validation openaq {day}: partition not written "
                        f"({len(errors)} error(s))\n  - {detail}",
                    )
                written = write_day("openaq", "measurements", day, rows, AQ_SCHEMA)
                ingest_record(
                    "openaq", "measurements", day,
                    "partial" if skipped else "ok",
                    written, started, note,
                )
                log(f"openaq {day}: {written} rows")

    if args.source in ("all", "nasapower"):
        started = utc_now()
        # Same shape as the source above: fetch first, write in the else branch.
        try:
            rows = get_nasapower(start, end)
        except Exception as error:
            failures["nasapower"] = error
            for day in days:
                ingest_record(
                    "nasapower", "weather", day, "failed", 0, started, str(error)
                )
        else:
            for day in days:
                part = [row for row in rows if row["_day"] == day]
                errors, warnings = validate_day("nasapower", day, part, station_ids)
                for warning in warnings:
                    log(f"warning nasapower {day}: {warning}")
                if errors:
                    detail = summarize(errors)
                    ingest_record(
                        "nasapower", "weather", day, "failed", 0, started,
                        f"{len(errors)} validation error(s): {detail}",
                    )
                    die(
                        2,
                        f"validation nasapower {day}: partition not written "
                        f"({len(errors)} error(s))\n  - {detail}",
                    )
                written = write_day("nasapower", "weather", day, rows, POWER_SCHEMA)
                ingest_record("nasapower", "weather", day, "ok", written, started)
                log(f"nasapower {day}: {written} rows")

    if failures:
        for source, error in failures.items():
            log(f"{source}: failed ({error})")
        # An exhausted rate limit (3) outranks a plain source failure (4).
        if any(
            isinstance(error, RateLimitExhausted) for error in failures.values()
        ):
            return 3
        return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
