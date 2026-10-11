"""
The java.util.Date and java.util.Calendar temporal implementations (#9630).

``arcadedb.dateTimeImplementation`` and ``arcadedb.dateImplementation`` both
document ``java.util.Date`` and ``java.util.Calendar`` as supported. The engine
stores DATETIME as a UTC wall clock, so the Date (or Calendar) it hands back
holds that wall clock's UTC instant, and every reader of the binding must give
back the wall clock that was written:

* the per-row converter read a ``java.util.Date`` with
  ``datetime.fromtimestamp()``, which applies the HOST's time zone, so the value
  moved by the host's UTC offset and a DATE landed on the previous day west of
  UTC;
* a ``java.util.Calendar`` had no converter at all and reached Python as the
  raw JVM object;
* the columnar readers had no Calendar branch, and ``to_json_list`` wrote a
  Calendar as its ``toString()``.

The host zone is switched with ``time.tzset()`` where the platform has it, west
and east of UTC, because CI runs in UTC and the shift is invisible there.

@author Luca Garulli (l.garulli@arcadedata.com)
"""

import os
import time
from contextlib import contextmanager
from datetime import date, datetime

import arcadedb_embedded as arcadedb
import pytest

WRITTEN = datetime(2024, 3, 15, 14, 30, 45, 123000)
WRITTEN_DAY = date(2024, 3, 15)
EPOCH_MILLIS = 1710513045123  # 2024-03-15T14:30:45.123Z
DAY_MILLIS = 1710460800000  # 2024-03-15T00:00:00Z

IMPLEMENTATIONS = ["java.util.Date", "java.util.Calendar"]
HOST_ZONES = ["America/New_York", "Asia/Tokyo", "UTC"]


@contextmanager
def host_zone(zone):
    """Run the block with the process's local time zone set to ``zone``."""
    if not hasattr(
        time, "tzset"
    ):  # Windows: the assertions still hold, in the host zone
        yield
        return
    previous = os.environ.get("TZ")
    os.environ["TZ"] = zone
    time.tzset()
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


@pytest.fixture(params=IMPLEMENTATIONS)
def legacy_db(request, temp_db_path):
    """A database whose DATE and DATETIME read back as ``request.param``."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command(
            "sql", f"ALTER DATABASE `arcadedb.dateTimeImplementation` '{request.param}'"
        )
        db.command(
            "sql", f"ALTER DATABASE `arcadedb.dateImplementation` '{request.param}'"
        )
        db.command("sql", "CREATE DOCUMENT TYPE E")
        db.command("sql", "CREATE PROPERTY E.ts DATETIME")
        db.command("sql", "CREATE PROPERTY E.d DATE")
        with db.transaction():
            doc = db.new_document("E")
            doc.set("ts", WRITTEN)
            doc.set("d", WRITTEN_DAY)
            doc.save()
        yield db, request.param
        db.drop()


def _raw(db):
    return db._java_db.query("sql", "SELECT ts, d FROM E").next()


def test_the_engine_hands_back_the_configured_class_holding_the_utc_instant(legacy_db):
    """The premise: what the binding receives, and what it means."""
    db, implementation = legacy_db
    raw = _raw(db)
    for name, millis in (("ts", EPOCH_MILLIS), ("d", DAY_MILLIS)):
        value = raw.getProperty(name)
        assert value.getClass().getName() in (
            implementation,
            "java.util.GregorianCalendar",
        )
        held = (
            value.getTime()
            if implementation == "java.util.Date"
            else value.getTimeInMillis()
        )
        assert int(held) == millis


@pytest.mark.parametrize("zone", HOST_ZONES)
def test_per_row_readers_give_back_the_written_wall_clock(legacy_db, zone):
    db, _ = legacy_db
    with host_zone(zone):
        first = db.query("sql", "SELECT ts, d FROM E").first()
        assert first.get("ts") == WRITTEN
        # A DATE carried by a Date or Calendar is its UTC midnight: the same day
        # in every host zone, never the previous one.
        assert first.get("d") == datetime(2024, 3, 15)

        assert db.query("sql", "SELECT ts, d FROM E").first().to_dict() == {
            "ts": WRITTEN,
            "d": datetime(2024, 3, 15),
        }
        for rows in (
            db.query("sql", "SELECT ts, d FROM E").to_list(),
            list(db.query("sql", "SELECT ts, d FROM E").iter_dicts()),
        ):
            assert rows == [{"ts": WRITTEN, "d": datetime(2024, 3, 15)}]

        # the document API goes through the same converter
        doc = db.query("sql", "SELECT FROM E").first().get_element()
        assert doc.get("ts") == WRITTEN


def test_a_calendar_converts_to_a_python_datetime_not_a_jvm_object(legacy_db):
    db, _ = legacy_db
    value = db.query("sql", "SELECT ts FROM E").first().get("ts")
    assert type(value) is datetime


@pytest.mark.parametrize("zone", HOST_ZONES)
def test_every_reader_agrees_with_the_columnar_readers(legacy_db, zone):
    import numpy as np

    db, _ = legacy_db
    with host_zone(zone):
        per_row = db.query("sql", "SELECT ts FROM E").first().get("ts")
        columns = db.query("sql", "SELECT ts FROM E").to_columns()
        assert columns["ts"][0] == np.datetime64("2024-03-15T14:30:45.123")
        assert np.datetime64(per_row, "ms") == columns["ts"][0]

        frame = db.query("sql", "SELECT ts FROM E").to_dataframe()
        assert frame["ts"][0].to_pydatetime() == per_row

        # epoch milliseconds, never Calendar.toString()
        assert db.query("sql", "SELECT ts, d FROM E").to_json_list() == [
            {"ts": EPOCH_MILLIS, "d": DAY_MILLIS}
        ]


def test_the_written_value_finds_the_record_again(legacy_db):
    """What is read back is a value the caller can query by."""
    db, _ = legacy_db
    value = db.query("sql", "SELECT ts FROM E").first().get("ts")
    assert (
        db.query("sql", "SELECT count(*) AS c FROM E WHERE ts = ?", value)
        .first()
        .get("c")
        == 1
    )
