"""Timeseries SQL coverage for Python bindings."""

from datetime import datetime, timezone

import arcadedb_embedded as arcadedb


def _create_timeseries(db):
    db.command(
        "sql",
        "CREATE TIMESERIES TYPE TempData TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (value DOUBLE)",
    )


def _to_epoch_millis(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            epoch = datetime(1970, 1, 1)
            return int((value - epoch).total_seconds() * 1000)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        return int((value.astimezone(timezone.utc) - epoch).total_seconds() * 1000)
    return int(value)


def _assert_epoch_set(actual_values, expected_values):
    actual_set = {int(v) for v in actual_values}
    expected_set = {int(v) for v in expected_values}
    if actual_set == expected_set:
        return

    actual_min = min(actual_set)
    expected_min = min(expected_set)
    offset = actual_min - expected_min
    normalized = {v - offset for v in actual_set}
    assert normalized == expected_set


def test_timeseries_sql_insert_between_and_bucket(temp_db_path):
    """Create timeseries type, insert records, query by range, and aggregate by bucket."""
    with arcadedb.create_database(temp_db_path) as db:
        _create_timeseries(db)

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 3600000, sensor_id = 'A', value = 10.0",
            )
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 3601000, sensor_id = 'A', value = 20.0",
            )
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 3602000, sensor_id = 'B', value = 30.0",
            )
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 7200000, sensor_id = 'A', value = 40.0",
            )
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 7201000, sensor_id = 'B', value = 50.0",
            )

        between_rows = list(
            db.query("sql", "SELECT FROM TempData WHERE ts BETWEEN 3600000 AND 3602000")
        )
        assert len(between_rows) == 3
        _assert_epoch_set(
            {_to_epoch_millis(row.get("ts")) for row in between_rows},
            {3600000, 3601000, 3602000},
        )

        buckets = list(
            db.query(
                "sql",
                "SELECT ts.timeBucket('1h', ts) AS hour, avg(value) AS avg_val FROM TempData GROUP BY hour ORDER BY hour",
            )
        )

        assert len(buckets) == 2
        assert abs(float(buckets[0].get("avg_val")) - 20.0) < 1e-9
        assert abs(float(buckets[1].get("avg_val")) - 45.0) < 1e-9


def test_timeseries_sql_tag_filter_and_empty_range(temp_db_path):
    """Timeseries supports tag filtering and returns no rows for non-overlapping ranges."""
    with arcadedb.create_database(temp_db_path) as db:
        _create_timeseries(db)

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 1000, sensor_id = 'A', value = 11.0",
            )
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 2000, sensor_id = 'B', value = 22.0",
            )
            db.command(
                "sql",
                "INSERT INTO TempData SET ts = 3000, sensor_id = 'A', value = 33.0",
            )

        tag_rows = list(db.query("sql", "SELECT FROM TempData WHERE sensor_id = 'A'"))
        assert len(tag_rows) == 2
        _assert_epoch_set(
            {_to_epoch_millis(row.get("ts")) for row in tag_rows},
            {1000, 3000},
        )

        empty_rows = list(
            db.query("sql", "SELECT FROM TempData WHERE ts BETWEEN 9000 AND 10000")
        )
        assert empty_rows == []


def test_compact_timeseries_type_seals_the_tail(temp_db_path):
    # 26.10.1 (ArcadeData/arcadedb#8574): after a bulk load, COMPACT TIMESERIES
    # TYPE seals what is still in the mutable tail and says how much is left,
    # instead of waiting for the 60-second background pass.
    import numpy as np

    with arcadedb.create_database(temp_db_path) as db:
        _create_timeseries(db)
        n = 5_000
        ex = db.async_executor()
        ex.append_samples(
            "TempData",
            1_700_000_000_000 + np.arange(n, dtype=np.int64) * 1_000,
            [f"s{i % 10}" for i in range(n)],
            np.arange(n, dtype=np.float64),
        )
        ex.wait_completion()

        row = db.command("sql", "COMPACT TIMESERIES TYPE TempData").first()
        assert row.get("typeName") == "TempData"
        assert row.get("mutableSamples") == 0
        # The background pass may already have sealed part of it.
        assert 0 <= row.get("mutableSamplesBefore") <= n
        assert (
            db.query("sql", "SELECT count(*) AS n FROM TempData").first().get("n") == n
        )
