"""The bridge's bulk writers have one temporal encoding, and it is the engine's (#10522).

``to_json_list()`` (``RowBatcher``) and ``to_columns()`` (``ColumnBatcher``) used to carry an encoder of their
own for a date, written when the engine had no type for a ``LocalDate``. The engine has since fixed what that
encoder worked around (a ``LocalDate`` is UTC midnight whatever the JVM zone, a ``Calendar`` is its milliseconds,
a date past the range of a long is text), so the private copy only differed from the engine's generic JSON writer
in the one place it was worse: a date past year 292 million threw and failed the whole batch.

What these tests hold is the rule the issue asks for, in a form that survives the next engine-side type mapping:
for every temporal type the JSON writer, the columnar writer and ``JSONObject.put`` write the same number, at the
top level and inside a list or a map, in a record row and in a projection row. The one number that differs, a
DATE column at the top level of ``Result.to_json()`` of a record row (days), is the engine's round-trip form of
the record and is pinned separately, so its difference is a decision written down and not a drift.

Author: Luca Garulli (l.garulli@arcadedata.com)
"""

import datetime as dt
import json

import arcadedb_embedded as arcadedb  # noqa: F401  (starts the JVM through the fixtures)
import jpype
import numpy as np
import pytest
from arcadedb_embedded import results
from arcadedb_embedded.exceptions import ArcadeDBError

MIDNIGHT_UTC_2024_01_02 = 1704153600000
DAY_MS = 86_400_000


def _in_memory(rows):
    """A result set over rows built in Java, for values SQL cannot produce."""
    internal = jpype.JClass("com.arcadedb.query.sql.executor.InternalResultSet")()
    result_internal = jpype.JClass("com.arcadedb.query.sql.executor.ResultInternal")
    for row in rows:
        java_row = result_internal()
        for key, value in row.items():
            java_row.setProperty(key, value)
        internal.add(java_row)
    return internal


def _java_temporals():
    """One value of every temporal class a row can hold, with sub-millisecond digits and a pre-epoch instant."""
    java = jpype.JClass
    instant = java("java.time.Instant").parse("2024-01-02T03:04:05.123456789Z")
    calendar = java("java.util.Calendar").getInstance(
        java("java.util.TimeZone").getTimeZone("UTC")
    )
    calendar.setTimeInMillis(jpype.JLong(1704164645123))
    return {
        "local_date": java("java.time.LocalDate").of(2024, 1, 2),
        "pre_epoch_date": java("java.time.LocalDate").of(1969, 12, 31),
        "local_date_time": java("java.time.LocalDateTime").of(
            2024, 1, 2, 3, 4, 5, 123_456_789
        ),
        "pre_epoch_date_time": java("java.time.LocalDateTime").of(
            1969, 12, 31, 23, 59, 59, 999_500_000
        ),
        "instant": instant,
        "zoned": java("java.time.ZonedDateTime").ofInstant(
            instant, java("java.time.ZoneId").of("Asia/Seoul")
        ),
        "offset": java("java.time.OffsetDateTime").parse("2024-01-02T03:04:05.5+09:00"),
        "util_date": java("java.util.Date")(jpype.JLong(1704164645123)),
        "calendar": calendar,
    }


def _engine_json(name, value):
    """What the engine's generic JSON writer makes of ``value``: the parsed text, or the exception's class name."""
    json_object = jpype.JClass("com.arcadedb.serializer.json.JSONObject")()
    try:
        json_object.put(name, value)
    except jpype.JException as exc:
        return {"raises": str(exc.getClass().getName())}
    return json.loads(str(json_object.toString()))


def test_the_json_and_columnar_writers_write_the_same_number_for_every_temporal(
    temp_db,
):
    values = _java_temporals()

    as_json = results.ResultSet(_in_memory([values]), temp_db).to_json_list()[0]
    columns = results.ResultSet(_in_memory([values]), temp_db).to_columns()

    assert set(columns) == set(values)
    for name in values:
        column = columns[name]
        assert column.dtype == np.dtype("datetime64[ms]"), name
        assert int(column[0].astype("int64")) == as_json[name], name


def test_the_json_writer_is_the_engines_generic_writer_for_every_temporal(temp_db):
    """No encoder of its own: if the engine's JSONObject changes how it writes a type, this fails and a decision is made."""
    values = _java_temporals()
    # a date past the range of a long of milliseconds: text for an engine that has #10483, a failure for one that has not,
    # and the bridge follows either way
    values["far_date"] = jpype.JClass("java.time.LocalDate").of(999_999_999, 12, 31)
    dates = jpype.JClass("java.util.ArrayList")()
    dates.add(values["local_date"])
    dates.add(values["pre_epoch_date"])
    values["dates"] = dates
    members = jpype.JClass("java.util.LinkedHashMap")()
    members.put("on", values["local_date"])
    values["meta"] = members

    for name, value in values.items():
        expected = _engine_json(name, value)
        if "raises" in expected:
            # the bridge follows the engine it bundles, and the bindings raise a Java failure of a read as ArcadeDBError
            with pytest.raises(ArcadeDBError):
                results.ResultSet(_in_memory([{name: value}]), temp_db).to_json_list()
        else:
            row = results.ResultSet(_in_memory([{name: value}]), temp_db).to_json_list()
            assert row == [expected], name


def test_a_date_is_one_number_in_a_record_row_a_projection_row_a_list_and_a_map(
    temp_db,
):
    temp_db.command("sql", "CREATE DOCUMENT TYPE Event")
    temp_db.command("sql", "CREATE PROPERTY Event.day DATE")
    with temp_db.transaction():
        temp_db.command(
            "sql",
            "INSERT INTO Event SET day = :day, days = :days, meta = :meta",
            {
                "day": dt.date(2024, 1, 2),
                "days": [dt.date(2024, 1, 2)],
                "meta": {"on": dt.date(2024, 1, 2)},
            },
        )

    record_row = temp_db.query("sql", "SELECT FROM Event").to_json_list()[0]
    projection_row = temp_db.query(
        "sql", "SELECT day, days, meta FROM Event"
    ).to_json_list()[0]

    expected = {
        "day": MIDNIGHT_UTC_2024_01_02,
        "days": [MIDNIGHT_UTC_2024_01_02],
        "meta": {"on": MIDNIGHT_UTC_2024_01_02},
    }
    assert {key: record_row[key] for key in expected} == expected
    assert projection_row == expected


def test_result_to_json_writes_the_top_level_date_of_a_record_row_as_days(temp_db):
    """The one place the engine writes days: the round-trip JSON of the record, pinned so the difference is a decision.

    A DATE column read back by ``fromJSON`` is a count of days, which is why ``Result.to_json()`` of a record row
    writes it so. ``to_json_list()`` is a read-only bulk transport and writes milliseconds everywhere, so a caller
    decodes every temporal the same way; the two differ by exactly the milliseconds in a day, and by nothing else.
    """
    temp_db.command("sql", "CREATE DOCUMENT TYPE Event")
    temp_db.command("sql", "CREATE PROPERTY Event.day DATE")
    with temp_db.transaction():
        temp_db.command(
            "sql",
            "INSERT INTO Event SET day = :day",
            {"day": dt.date(2024, 1, 2)},
        )

    single = json.loads(temp_db.query("sql", "SELECT FROM Event").first().to_json())
    batch = temp_db.query("sql", "SELECT FROM Event").to_json_list()[0]

    assert single["day"] == MIDNIGHT_UTC_2024_01_02 // DAY_MS
    assert batch["day"] == single["day"] * DAY_MS
    assert "@rid" in single and "@rid" not in batch
