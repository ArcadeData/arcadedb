#!/usr/bin/env python3
"""Example 17: Time Series End-to-End.

This example is intentionally more realistic than a simple smoke test.
It models synthetic building telemetry for multiple sensors across several hours.

Workflow covered:
- create a TimeSeries type with multiple tags and numeric fields
- generate deterministic telemetry for several sensors
- bulk insert samples transactionally
- run raw window queries with multiple tag filters
- aggregate by hour with ts.timeBucket() at sensor and building scopes
- derive alert-style views from SQL aggregates using Python post-processing
- inspect the latest sample per sensor

Requirements:
- A packaged ArcadeDB runtime that supports CREATE TIMESERIES TYPE

Note:
- This repo intentionally treats time series as SQL-first from Python.
- If the packaged runtime does not include TimeSeries support, the example
  exits with a short explanation instead of pretending there is a Python-native API.
"""

from __future__ import annotations

import argparse
import math
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone

import arcadedb_embedded as arcadedb

START_TS_MS = 1_710_000_000_000


@dataclass(frozen=True)
class SensorProfile:
    sensor_id: str
    region: str
    building: str
    zone: str
    base_temperature: float
    base_humidity: float
    base_power_kw: float
    base_co2_ppm: float
    base_occupancy: int
    phase: float


SENSORS = [
    SensorProfile(
        "north-alpha-lab-01",
        "north",
        "alpha",
        "lab",
        21.4,
        41.0,
        1.8,
        640.0,
        12,
        0.2,
    ),
    SensorProfile(
        "north-alpha-office-02",
        "north",
        "alpha",
        "office",
        22.6,
        39.5,
        2.4,
        690.0,
        20,
        1.1,
    ),
    SensorProfile(
        "north-beta-warehouse-01",
        "north",
        "beta",
        "warehouse",
        19.8,
        48.0,
        3.2,
        590.0,
        8,
        2.0,
    ),
    SensorProfile(
        "south-gamma-lab-01",
        "south",
        "gamma",
        "lab",
        23.1,
        43.0,
        2.1,
        720.0,
        18,
        2.7,
    ),
    SensorProfile(
        "south-gamma-office-02",
        "south",
        "gamma",
        "office",
        24.2,
        38.5,
        2.9,
        770.0,
        24,
        3.4,
    ),
    SensorProfile(
        "south-delta-warehouse-01",
        "south",
        "delta",
        "warehouse",
        20.7,
        46.5,
        3.6,
        610.0,
        10,
        4.0,
    ),
]


def epoch_ms_to_iso(value) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat()
        return value.astimezone(timezone.utc).isoformat()
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Example 17: Complex SQL-first time-series workflow"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=6,
        help="Number of synthetic hours to generate (default: 6)",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=5,
        help="Sampling interval in minutes (default: 5)",
    )
    parser.add_argument(
        "--db-path",
        default="./my_test_databases/timeseries_demo_db",
        help="Database path (default: ./my_test_databases/timeseries_demo_db)",
    )
    return parser.parse_args()


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def generate_samples(hours: int, interval_minutes: int) -> list[tuple]:
    samples_per_hour = max(1, 60 // interval_minutes)
    total_steps = hours * samples_per_hour
    generated = []

    for step in range(total_steps):
        ts = START_TS_MS + step * interval_minutes * 60 * 1000
        hour_index = step / samples_per_hour
        daily_wave = math.sin((step / samples_per_hour) * math.pi / 3.0)

        for sensor in SENSORS:
            local_wave = math.sin(hour_index * 1.35 + sensor.phase)
            temp = sensor.base_temperature + 1.8 * daily_wave + 0.9 * local_wave
            humidity = sensor.base_humidity - 3.2 * daily_wave + 1.4 * local_wave
            occupancy = sensor.base_occupancy + int(
                round(
                    max(
                        0.0,
                        10.0 * daily_wave + 4.0 * math.cos(hour_index + sensor.phase),
                    )
                )
            )
            power_kw = (
                sensor.base_power_kw
                + occupancy * 0.05
                + max(0.0, temp - 22.0) * 0.18
                + abs(local_wave) * 0.25
            )
            co2_ppm = (
                sensor.base_co2_ppm
                + occupancy * 16.0
                + max(0.0, temp - 22.0) * 32.0
                + abs(local_wave) * 28.0
            )

            generated.append(
                (
                    ts,
                    sensor.sensor_id,
                    sensor.region,
                    sensor.building,
                    sensor.zone,
                    round(temp, 3),
                    round(clamp(humidity, 25.0, 70.0), 3),
                    round(power_kw, 3),
                    round(co2_ppm, 3),
                    occupancy,
                )
            )

    return generated


def print_rows(title: str, rows: list[dict], *, limit: int | None = None) -> None:
    print(title)
    if not rows:
        print("  (no rows)")
        print()
        return

    display_rows = rows if limit is None else rows[:limit]
    for row in display_rows:
        print(f"  {row}")
    if limit is not None and len(rows) > limit:
        print(f"  ... {len(rows) - limit} more rows")
    print()


def insert_samples(db, samples: list[tuple]) -> None:
    with db.transaction():
        for sample in samples:
            db.command(
                "sql",
                "INSERT INTO SensorReading SET ts = ?, sensor_id = ?, region = ?, "
                "building = ?, zone = ?, temperature = ?, humidity = ?, "
                "power_kw = ?, co2_ppm = ?, occupancy = ?",
                *sample,
            )


def latest_per_sensor(db) -> list[dict]:
    latest: dict[str, dict] = {}
    for row in db.query(
        "sql",
        "SELECT FROM SensorReading ORDER BY sensor_id, ts DESC",
    ):
        sensor_id = row.get("sensor_id")
        if sensor_id in latest:
            continue
        latest[sensor_id] = {
            "sensor_id": sensor_id,
            "building": row.get("building"),
            "zone": row.get("zone"),
            "ts": epoch_ms_to_iso(row.get("ts")),
            "temperature": round(float(row.get("temperature")), 2),
            "co2_ppm": round(float(row.get("co2_ppm")), 2),
            "occupancy": int(row.get("occupancy")),
        }
    return list(latest.values())


def main() -> int:
    args = parse_args()

    print("=" * 72)
    print("ArcadeDB Python - Example 17: Time Series End-to-End")
    print("=" * 72)
    print()

    db_path = args.db_path
    db_dir = os.path.dirname(db_path)

    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    if os.path.exists("./log"):
        shutil.rmtree("./log")

    with arcadedb.create_database(db_path) as db:
        print(f"Database created at: {db_path}")
        print("Approach: SQL-first time-series access from Python bindings")
        print(
            f"Synthetic workload: {len(SENSORS)} sensors, {args.hours} hours, "
            f"{args.interval_minutes}-minute interval"
        )
        print()

        try:
            db.command(
                "sql",
                "CREATE TIMESERIES TYPE SensorReading "
                "TIMESTAMP ts "
                "TAGS (sensor_id STRING, region STRING, building STRING, zone STRING) "
                "FIELDS (temperature DOUBLE, humidity DOUBLE, power_kw DOUBLE, "
                "co2_ppm DOUBLE, occupancy LONG)",
            )
        except arcadedb.ArcadeDBError as exc:
            print("TimeSeries SQL is not available in this packaged runtime.")
            print("This example is SQL-first and depends on CREATE TIMESERIES TYPE.")
            print(f"Original error: {exc}")
            return 0

        print("Created TimeSeries type: SensorReading")
        print()

        samples = generate_samples(args.hours, args.interval_minutes)
        insert_samples(db, samples)

        start_ts = samples[0][0]
        end_ts = samples[-1][0]

        print(f"Inserted {len(samples)} tagged samples")
        print(f"Time range: {epoch_ms_to_iso(start_ts)} -> {epoch_ms_to_iso(end_ts)}")
        print()

        focus_sensor = SENSORS[1]
        raw_window_start = start_ts + 60 * 60 * 1000
        raw_window_end = raw_window_start + 90 * 60 * 1000

        range_rows = [
            {
                "ts": epoch_ms_to_iso(row.get("ts")),
                "sensor_id": row.get("sensor_id"),
                "region": row.get("region"),
                "building": row.get("building"),
                "zone": row.get("zone"),
                "temperature": round(float(row.get("temperature")), 2),
                "humidity": round(float(row.get("humidity")), 2),
                "co2_ppm": round(float(row.get("co2_ppm")), 1),
            }
            for row in db.query(
                "sql",
                "SELECT FROM SensorReading "
                f"WHERE ts BETWEEN {raw_window_start} AND {raw_window_end} "
                f"AND sensor_id = '{focus_sensor.sensor_id}' "
                f"AND building = '{focus_sensor.building}' "
                "ORDER BY ts",
            )
        ]
        print_rows(
            f"Raw window for {focus_sensor.sensor_id}:",
            range_rows,
            limit=8,
        )

        sensor_hourly_rows = [
            {
                "hour": epoch_ms_to_iso(row.get("hour")),
                "sensor_id": row.get("sensor_id"),
                "avg_temperature": round(float(row.get("avg_temperature")), 2),
                "max_co2_ppm": round(float(row.get("max_co2_ppm")), 1),
                "avg_power_kw": round(float(row.get("avg_power_kw")), 2),
                "samples": int(row.get("samples")),
            }
            for row in db.query(
                "sql",
                "SELECT ts.timeBucket('1h', ts) AS hour, sensor_id, "
                "avg(temperature) AS avg_temperature, "
                "max(co2_ppm) AS max_co2_ppm, "
                "avg(power_kw) AS avg_power_kw, "
                "count(*) AS samples "
                "FROM SensorReading "
                "GROUP BY hour, sensor_id "
                "ORDER BY hour, sensor_id",
            )
        ]
        print_rows("Hourly aggregation by sensor:", sensor_hourly_rows, limit=12)

        building_hourly_rows = [
            {
                "hour": epoch_ms_to_iso(row.get("hour")),
                "region": row.get("region"),
                "building": row.get("building"),
                "avg_temperature": round(float(row.get("avg_temperature")), 2),
                "avg_humidity": round(float(row.get("avg_humidity")), 2),
                "avg_co2_ppm": round(float(row.get("avg_co2_ppm")), 1),
                "avg_power_kw": round(float(row.get("avg_power_kw")), 2),
            }
            for row in db.query(
                "sql",
                "SELECT ts.timeBucket('1h', ts) AS hour, region, building, "
                "avg(temperature) AS avg_temperature, "
                "avg(humidity) AS avg_humidity, "
                "avg(co2_ppm) AS avg_co2_ppm, "
                "avg(power_kw) AS avg_power_kw "
                "FROM SensorReading "
                "GROUP BY hour, region, building "
                "ORDER BY hour, region, building",
            )
        ]

        hot_busy_buildings = [
            row
            for row in building_hourly_rows
            if row["avg_temperature"] >= 24.0 and row["avg_co2_ppm"] >= 950.0
        ]
        print_rows(
            "Hourly building rollups:",
            building_hourly_rows,
            limit=10,
        )
        print_rows(
            "Derived alerts: hot and busy building-hours:",
            hot_busy_buildings,
        )

        region_rollup_rows = [
            {
                "hour": epoch_ms_to_iso(row.get("hour")),
                "region": row.get("region"),
                "avg_temperature": round(float(row.get("avg_temperature")), 2),
                "avg_power_kw": round(float(row.get("avg_power_kw")), 2),
                "peak_co2_ppm": round(float(row.get("peak_co2_ppm")), 1),
            }
            for row in db.query(
                "sql",
                "SELECT ts.timeBucket('1h', ts) AS hour, region, "
                "avg(temperature) AS avg_temperature, "
                "avg(power_kw) AS avg_power_kw, "
                "max(co2_ppm) AS peak_co2_ppm "
                "FROM SensorReading "
                "GROUP BY hour, region "
                "ORDER BY hour, region",
            )
        ]
        print_rows("Regional hourly rollups:", region_rollup_rows, limit=12)

        print_rows("Latest sample per sensor:", latest_per_sensor(db))

    print("Example complete. Database files were kept for inspection.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
