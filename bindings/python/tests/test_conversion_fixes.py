"""Conversions and small API gaps found by the bindings round-trip review (2026-10-03).

Each section names its issue in this repository.
"""

from datetime import date, datetime

import arcadedb_embedded as arcadedb
import pytest
from arcadedb_embedded import type_conversion
from arcadedb_embedded.schema import PropertyType
from arcadedb_embedded.vector import to_java_int_array

np = pytest.importorskip("numpy")


# --- #119: a numpy bool is a bool, not the float 1.0 -------------------------------------------


def test_numpy_bool_is_stored_as_a_boolean_on_every_write_path(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE B")
        with db.transaction():
            doc = db.new_document("B").set("path", "set").set("ok", np.True_)
            doc.save()
            db.command(
                "sql", "INSERT INTO B SET path = ?, ok = ?", "positional", np.True_
            )
            db.command(
                "sql",
                "INSERT INTO B SET path = :p, ok = :ok",
                {"p": "named", "ok": np.True_},
            )
            db.command(
                "sql",
                "INSERT INTO B SET path = ?, ok = ?",
                "list",
                [np.True_, np.False_],
            )
        rows = {
            r.get("path"): r.get("ok")
            for r in db.query("sql", "SELECT path, ok FROM B").to_list()
        }
        assert (
            rows["set"] is True and rows["positional"] is True and rows["named"] is True
        )
        assert rows["list"] == [True, False]
        assert (
            db.query("sql", "SELECT count(*) AS n FROM B WHERE ok = true")
            .first()
            .get("n")
            == 3
        )


def test_a_false_numpy_bool_is_a_false_boolean(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE B")
        with db.transaction():
            db.new_document("B").set("ok", np.False_).save()
        assert db.query("sql", "SELECT ok FROM B").first().get("ok") is False


def test_numpy_bool_detection_does_not_touch_other_numpy_scalars():
    assert type_conversion._is_numpy_bool(np.True_)
    assert not type_conversion._is_numpy_bool(np.int64(1))
    assert not type_conversion._is_numpy_bool(True)
    assert not type_conversion._is_numpy_bool(1.0)


# --- #120: an int64 array past 32 bits raises, as the same list does -----------------------------


def test_to_java_int_array_refuses_values_beyond_32_bits_in_an_array():
    for bad in (2**31, -(2**31) - 1, 2**33):
        with pytest.raises(OverflowError):
            to_java_int_array(np.array([1, bad], dtype=np.int64))
        with pytest.raises(OverflowError):
            to_java_int_array([1, bad])
    with pytest.raises(OverflowError):
        to_java_int_array(np.array([2**31], dtype=np.uint32))


def test_to_java_int_array_keeps_values_that_fit():
    array = to_java_int_array(np.array([-(2**31), 0, 2**31 - 1], dtype=np.int64))
    assert list(array) == [-(2**31), 0, 2**31 - 1]
    assert len(to_java_int_array(np.array([], dtype=np.int64))) == 0


# --- #123: lookup_by_key with a datetime or date key ----------------------------------------------


def test_lookup_by_key_accepts_datetime_and_date_keys(temp_db_path):
    when = datetime(2026, 10, 3, 12, 34, 56, 123456)
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Ev")
        db.command("sql", "CREATE PROPERTY Ev.at DATETIME_MICROS")
        db.command("sql", "CREATE PROPERTY Ev.day DATE")
        db.command("sql", "CREATE INDEX ON Ev (at) UNIQUE")
        db.command("sql", "CREATE INDEX ON Ev (day) UNIQUE")
        with db.transaction():
            db.new_document("Ev").set("at", when).set("day", date(2026, 10, 3)).set(
                "tag", "x"
            ).save()
        by_datetime = db.lookup_by_key("Ev", ["at"], [when])
        by_date = db.lookup_by_key("Ev", ["day"], [date(2026, 10, 3)])
        assert by_datetime is not None and by_datetime.get("tag") == "x"
        assert by_date is not None and by_date.get("tag") == "x"


# --- #124: append_samples(primitive=True) takes the columns the default path takes ---------------


def _timeseries(db, name):
    db.command(
        "sql",
        f"CREATE TIMESERIES TYPE {name} TIMESTAMP ts TAGS (host STRING) FIELDS (val DOUBLE) SHARDS 1",
    )


@pytest.mark.parametrize(
    "primitive", [False, True], ids=["default_path", "primitive_path"]
)
def test_append_samples_accepts_numpy_tag_and_bool_columns(temp_db, primitive):
    """A numpy str tag array used to raise on the primitive path ("truth value of an array is
    ambiguous"). A numpy bool array is a 0/1 numeric column on both paths: the default path stored it
    as 1.0 and 0.0 only because a numpy bool was read as a number, which a numpy bool no longer is.
    """
    ts = np.array([1000, 2000, 3000], dtype=np.int64)
    for name, hosts, field, expected in (
        (
            "TsStr",
            np.array(["a", "b", "c"]),
            np.array([1.5, 2.5, 3.5]),
            [1.5, 2.5, 3.5],
        ),
        ("TsBool", ["a", "b", "c"], np.array([True, False, True]), [1.0, 0.0, 1.0]),
    ):
        name = f"{name}{int(primitive)}"
        _timeseries(temp_db, name)
        executor = temp_db.async_executor()
        executor.append_samples(name, ts, hosts, field, primitive=primitive)
        executor.wait_completion()
        query = (
            f"SELECT host, val FROM {name} ORDER BY ts"  # nosec B608 - test-owned name
        )
        rows = temp_db.query("sql", query).to_list()
        assert [r.get("host") for r in rows] == ["a", "b", "c"]
        assert [r.get("val") for r in rows] == expected


@pytest.mark.parametrize("short", ["host", "val"])
def test_primitive_append_samples_rejects_a_column_of_the_wrong_length(temp_db, short):
    _timeseries(temp_db, "TsShort")
    ts = np.array([1000, 2000, 3000], dtype=np.int64)
    hosts = ["a", "b", "c"][: 2 if short == "host" else 3]
    vals = np.array([1.5, 2.5, 3.5])[: 2 if short == "val" else 3]
    executor = temp_db.async_executor()
    with pytest.raises(ValueError, match="column"):
        executor.append_samples("TsShort", ts, hosts, vals, primitive=True)
    executor.wait_completion()
    assert (
        temp_db.query("sql", "SELECT count(*) AS n FROM TsShort").first().get("n") == 0
    )


# --- #125: get_or_create_property takes a PropertyType for of_type, as create_property does ---------


def test_get_or_create_property_accepts_a_property_type_for_of_type(temp_db_path):
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE P")
        db.schema.create_property("P", "created", "LIST", of_type=PropertyType.STRING)
        db.schema.get_or_create_property(
            "P", "got", "LIST", of_type=PropertyType.STRING
        )
        again = db.schema.get_or_create_property(
            "P", "got", "LIST", of_type=PropertyType.STRING
        )
        assert again is not None
        with db.transaction():
            db.new_document("P").set("created", ["a"]).set("got", ["b"]).save()


# --- #127: Instant, ZonedDateTime, and OffsetDateTime keep their microseconds -----------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("2026-10-03T12:34:56.123456Z", datetime(2026, 10, 3, 12, 34, 56, 123456)),
        ("2300-01-01T00:00:00.123456Z", datetime(2300, 1, 1, 0, 0, 0, 123456)),
        ("3000-06-15T01:02:03.654321Z", datetime(3000, 6, 15, 1, 2, 3, 654321)),
        ("9999-12-31T23:59:59.999999Z", datetime(9999, 12, 31, 23, 59, 59, 999999)),
        ("1969-12-31T23:59:59.999999Z", datetime(1969, 12, 31, 23, 59, 59, 999999)),
        ("1900-01-01T00:00:00.000001Z", datetime(1900, 1, 1, 0, 0, 0, 1)),
    ],
)
def test_java_instants_convert_without_losing_microseconds(
    temp_db_path, text, expected
):
    with arcadedb.create_database(temp_db_path):
        import jpype

        instant = jpype.JClass("java.time.Instant").parse(text)
        zoned = jpype.JClass("java.time.ZonedDateTime").ofInstant(
            instant, jpype.JClass("java.time.ZoneId").of("Asia/Seoul")
        )
        offset = jpype.JClass("java.time.OffsetDateTime").ofInstant(
            instant, jpype.JClass("java.time.ZoneOffset").ofHours(9)
        )
        for value in (instant, zoned, offset):
            converted = type_conversion.convert_java_to_python(value)
            assert converted.replace(tzinfo=None) == expected
            assert converted.utcoffset().total_seconds() == 0
