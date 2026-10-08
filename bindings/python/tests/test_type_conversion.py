"""
Tests for type conversion between Java and Python types.
"""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import arcadedb_embedded as arcadedb


def test_basic_type_conversion(temp_db_path):
    """Test basic type conversion for common data types."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE TypeTest")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO TypeTest SET
                    string_val = 'hello',
                    int_val = 42,
                    long_val = 9223372036854775807,
                    float_val = 3.14,
                    double_val = 2.71828,
                    bool_val = true,
                    null_val = null
            """,
            )

        result = db.query("sql", "SELECT FROM TypeTest")
        record = result.first()

        # Test string conversion
        assert record.get("string_val") == "hello"
        assert isinstance(record.get("string_val"), str)

        # Test integer conversion
        assert record.get("int_val") == 42
        assert isinstance(record.get("int_val"), int)

        # Test long conversion
        assert record.get("long_val") == 9223372036854775807
        assert isinstance(record.get("long_val"), int)

        # Test float conversion
        float_val = record.get("float_val")
        assert abs(float_val - 3.14) < 0.01
        assert isinstance(float_val, float)

        # Test double conversion
        double_val = record.get("double_val")
        assert abs(double_val - 2.71828) < 0.0001
        assert isinstance(double_val, float)

        # Test boolean conversion
        assert record.get("bool_val") is True
        assert isinstance(record.get("bool_val"), bool)

        # Test null conversion
        assert record.get("null_val") is None


def test_decimal_conversion(temp_db_path):
    """Test BigDecimal to Python Decimal conversion."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE DecimalTest")
        db.command("sql", "CREATE PROPERTY DecimalTest.price DECIMAL")

        with db.transaction():
            db.command("sql", "INSERT INTO DecimalTest SET price = 99.95")

        result = db.query("sql", "SELECT FROM DecimalTest")
        record = result.first()

        price = record.get("price")
        # Should be converted to Python Decimal for precision
        assert isinstance(price, Decimal)
        assert price == Decimal("99.95")


def test_date_conversion(temp_db_path):
    """Test Java Date/LocalDate to Python datetime/date conversion."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE DateTest")
        db.command("sql", "CREATE PROPERTY DateTest.created_date DATE")
        db.command("sql", "CREATE PROPERTY DateTest.created_datetime DATETIME")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO DateTest SET
                    created_date = date('2024-01-15'),
                    created_datetime = sysdate()
            """,
            )

        result = db.query("sql", "SELECT FROM DateTest")
        record = result.first()

        # Test date conversion
        created_date = record.get("created_date")
        assert created_date is not None
        # Should be converted to Python date/datetime
        assert isinstance(created_date, (date, datetime))

        # Test datetime conversion
        created_datetime = record.get("created_datetime")
        assert created_datetime is not None
        assert isinstance(created_datetime, datetime)


def test_offset_datetime_conversion(temp_db_path):
    """OffsetDateTime is a storable DATETIME since engine 26.7.2 (#4922).

    Storing one no longer silently drops the property (the original bug, hit
    by every Bolt/Neo4j client write). The engine normalizes it to UTC and
    hands reads back as LocalDateTime -> naive Python datetime; a raw
    OffsetDateTime (e.g. from an expression, not storage) converts directly.
    """
    import jpype
    from arcadedb_embedded import convert_java_to_python

    with arcadedb.create_database(temp_db_path) as db:  # starts the JVM
        db.command("sql", "CREATE DOCUMENT TYPE OffsetTest")

        OffsetDateTime = jpype.JClass("java.time.OffsetDateTime")
        odt = OffsetDateTime.parse("2026-07-05T10:30:00.250+02:00")

        # direct converter path: offset applied, tz-aware UTC result
        converted = convert_java_to_python(odt)
        assert isinstance(converted, datetime)
        assert converted == datetime(2026, 7, 5, 8, 30, 0, 250000, tzinfo=timezone.utc)

        with db.transaction():
            doc = db.new_document("OffsetTest")
            doc.set("stamp", odt)
            doc.save()

        # storage round-trip: engine normalizes to UTC wall-clock (naive)
        record = db.query("sql", "SELECT FROM OffsetTest").first()
        stamp = record.get("stamp")
        assert isinstance(stamp, datetime)
        assert stamp.replace(tzinfo=None) == datetime(2026, 7, 5, 8, 30, 0, 250000)

        # columnar bulk path (ColumnBatcher datetime lane)
        cols = db.query("sql", "SELECT stamp FROM OffsetTest").to_columns()
        if cols is not None:  # numpy + bridge jar available
            value = cols["stamp"][0]
            assert str(value.astype("datetime64[ms]")) == "2026-07-05T08:30:00.250"


def test_collection_conversion(temp_db_path):
    """Test Java collections (List, Set, Map) to Python conversion."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE CollectionTest")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO CollectionTest SET
                    tags = ['python', 'database', 'graph'],
                    metadata = {
                        'version': 1,
                        'active': true,
                        'name': 'test'
                    }
            """,
            )

        result = db.query("sql", "SELECT FROM CollectionTest")
        record = result.first()

        # Test list conversion
        tags = record.get("tags")
        assert isinstance(tags, list)
        assert len(tags) == 3
        assert "python" in tags
        assert "database" in tags
        assert "graph" in tags

        # Test map/dict conversion
        metadata = record.get("metadata")
        assert isinstance(metadata, dict)
        assert metadata["version"] == 1
        assert metadata["active"] is True
        assert metadata["name"] == "test"


def test_a_python_set_is_a_set_only_until_the_commit(temp_db_path):
    """The engine has no set type, so a HashSet is serialized as a list when the
    transaction commits (#122). The documented behavior: a set inside the
    transaction, a list from every read after it. If the engine ever keeps sets,
    this fails and api/type_conversion.md needs to change with it."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE VERTEX TYPE User")

        with db.transaction():
            vertex = db.new_vertex("User")
            vertex.set("roles", {"admin", "user", "admin"})
            vertex.save()
            assert vertex.get("roles") == {"admin", "user"}
            rid = vertex.get_rid()

        by_rid = db.lookup_by_rid(rid).get("roles")
        by_query = db.query("sql", "SELECT roles FROM User").first().get("roles")
        rows = db.query("sql", "SELECT roles FROM User").to_list()
        for value in (by_rid, by_query, rows[0]["roles"]):
            assert isinstance(value, list)
            assert sorted(value) == ["admin", "user"]


def test_nested_collection_conversion(temp_db_path):
    """Test conversion of nested collections."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE NestedTest")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO NestedTest SET
                    nested_data = {
                        'users': [
                            {'name': 'Alice', 'age': 30},
                            {'name': 'Bob', 'age': 25}
                        ],
                        'settings': {
                            'theme': 'dark',
                            'notifications': true
                        }
                    }
            """,
            )

        result = db.query("sql", "SELECT FROM NestedTest")
        record = result.first()

        nested_data = record.get("nested_data")
        assert isinstance(nested_data, dict)

        # Test nested list of dicts
        users = nested_data["users"]
        assert isinstance(users, list)
        assert len(users) == 2
        assert isinstance(users[0], dict)
        assert users[0]["name"] == "Alice"
        assert users[0]["age"] == 30

        # Test nested dict
        settings = nested_data["settings"]
        assert isinstance(settings, dict)
        assert settings["theme"] == "dark"
        assert settings["notifications"] is True


def test_property_names(temp_db_path):
    """Test the property_names property."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE PropsTest")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO PropsTest SET
                    name = 'test',
                    age = 30,
                    active = true,
                    score = 95.5
            """,
            )

        result = db.query("sql", "SELECT FROM PropsTest")
        record = result.first()

        # Test property_names property
        prop_names = record.property_names
        assert isinstance(prop_names, list)
        assert "name" in prop_names
        assert "age" in prop_names
        assert "active" in prop_names
        assert "score" in prop_names


def test_to_dict_conversion(temp_db_path):
    """Test Result.to_dict() method."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE DictTest")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO DictTest SET
                    name = 'test',
                    count = 42,
                    active = true,
                    price = 99.95,
                    tags = ['a', 'b', 'c']
            """,
            )

        result = db.query("sql", "SELECT FROM DictTest")
        record = result.first()

        # Test to_dict with type conversion
        data = record.to_dict(convert_types=True)
        assert isinstance(data, dict)
        assert data["name"] == "test"
        assert data["count"] == 42
        assert data["active"] is True
        assert isinstance(data["tags"], list)
        assert len(data["tags"]) == 3

        # Test to_dict without type conversion
        data_raw = record.to_dict(convert_types=False)
        assert isinstance(data_raw, dict)
        # Values may be Java objects without conversion


def test_to_json_conversion(temp_db_path):
    """Test Result.to_json() method."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE JsonTest")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO JsonTest SET
                    name = 'test',
                    count = 42,
                    active = true
            """,
            )

        result = db.query("sql", "SELECT FROM JsonTest")
        record = result.first()

        # Test to_json
        json_str = record.to_json()
        assert isinstance(json_str, str)
        assert "test" in json_str
        assert "42" in json_str
        assert "true" in json_str.lower() or "True" in json_str


def test_python_to_java_conversion(temp_db_path):
    """Test converting Python types to Java when setting properties."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE PyToJavaTest")

        with db.transaction():
            doc = db.new_document("PyToJavaTest")

            # Test setting various Python types
            doc.set("name", "test")  # str
            doc.set("count", 42)  # int
            doc.set("price", Decimal("99.95"))  # Decimal -> BigDecimal
            doc.set("active", True)  # bool

            # Convert list to Java ArrayList for compatibility
            from arcadedb_embedded.type_conversion import convert_python_to_java

            doc.set("tags", convert_python_to_java(["a", "b", "c"]))  # list
            doc.set("metadata", convert_python_to_java({"key": "value"}))  # dict
            doc.set("unique_items", convert_python_to_java({"x", "y", "z"}))  # set

            doc.save()

        # Query back and verify conversions
        result = db.query("sql", "SELECT FROM PyToJavaTest")
        record = result.first()

        assert record.get("name") == "test"
        assert record.get("count") == 42
        # BigDecimal may be converted to float or Decimal depending on Java handling
        price = record.get("price")
        assert isinstance(price, (Decimal, float))
        assert abs(float(price) - 99.95) < 0.01
        assert record.get("active") is True
        assert isinstance(record.get("tags"), list)
        assert len(record.get("tags")) == 3
        assert isinstance(record.get("metadata"), dict)
        # Set may be converted to list or remain as set/collection
        unique_items = record.get("unique_items")
        assert unique_items is not None


def test_decimal_parameter_keeps_every_digit(temp_db_path):
    """A Decimal bound to a SQL parameter is stored and matched exactly (#58).

    Left to JPype it reached the engine as a Double: 38 digits were stored as
    1.2345678901234567E+19, and a lookup by the same Decimal missed the rows
    that held it exactly.
    """
    value = Decimal("12345678901234567890.123456789012345678")
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Money")
        db.command("sql", "CREATE PROPERTY Money.amount DECIMAL")
        with db.transaction():
            db.command("sql", "INSERT INTO Money SET k = 'param', amount = ?", value)
            db.new_document("Money").set("k", "set").set("amount", value).save()

        for key in ("param", "set"):
            got = db.query("sql", "SELECT amount FROM Money WHERE k = ?", key).first()
            assert got.get("amount") == value, key
        found = db.query("sql", "SELECT k FROM Money WHERE amount = ?", value).to_list()
        assert sorted(r["k"] for r in found) == ["param", "set"]


def test_datetime_and_date_parameters(temp_db_path):
    """datetime and date bind to SQL parameters, and keep microseconds (#58).

    Both used to be refused ("No matching overloads"), and a datetime crossed as
    a java.util.Date, which keeps milliseconds: DATETIME_MICROS stored
    ...56.789000 for ...56.789123 and a lookup by the same value found nothing.
    The engine stores DATETIME as a UTC wall clock: a naive value is that wall
    clock as it stands and reads back unchanged on any host, and an aware one
    is converted to UTC, so 21:34+09:00 is the same value as a naive 12:34.
    """
    naive = datetime(2026, 10, 1, 12, 34, 56, 789123)
    aware = datetime(
        2026, 10, 1, 21, 34, 56, 789123, tzinfo=timezone(timedelta(hours=9))
    )
    epoch_ms = 1790858096789  # 2026-10-01T12:34:56.789Z
    on_day = date(2026, 10, 1)
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Event")
        db.command("sql", "CREATE PROPERTY Event.at DATETIME_MICROS")
        db.command("sql", "CREATE PROPERTY Event.on_day DATE")
        with db.transaction():
            for label, value in (("naive", naive), ("aware", aware)):
                db.command(
                    "sql",
                    "INSERT INTO Event SET k = ?, at = ?, on_day = ?",
                    label + " param",
                    value,
                    on_day,
                )
                db.new_document("Event").set("k", label + " set").set(
                    "at", value
                ).save()

        every = ["aware param", "aware set", "naive param", "naive set"]
        for key in every:
            got = db.query(
                "sql", "SELECT at, at.asLong() AS ms FROM Event WHERE k = ?", key
            ).first()
            assert got.get("at") == naive, key
            assert got.get("ms") == epoch_ms, key
        for value in (naive, aware):
            found = db.query("sql", "SELECT k FROM Event WHERE at = ?", value).to_list()
            assert sorted(r["k"] for r in found) == every

        found = db.query(
            "sql", "SELECT k FROM Event WHERE on_day = ?", on_day
        ).to_list()
        assert sorted(r["k"] for r in found) == ["aware param", "naive param"]


def test_bytes_keep_every_byte(temp_db_path):
    """Python bytes are stored as byte[], through set() and a bound parameter.

    They used to reach Java as a String: b"Hello" came back as "Hello" and
    non-UTF-8 bytes as "", with no error. A byte[] reads back as signed ints.
    """
    payload = b"\xff\x00\xfe\x80Hello"
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Blob")
        with db.transaction():
            doc = db.new_document("Blob")
            doc.set("k", "set").set("data", payload).save()
            db.command(
                "sql", "INSERT INTO Blob SET k = 'param', data = ?", bytearray(payload)
            )

        for key in ("set", "param"):
            got = db.query("sql", "SELECT data FROM Blob WHERE k = ?", key).first()
            data = got.get("data")
            assert isinstance(data, list), (key, data)
            assert bytes(b & 0xFF for b in data) == payload, key


def test_scalar_list_crosses_as_one_array_with_the_same_types(temp_db_path):
    """A list of plain scalars converts in one JVM call, element types unchanged.

    The per-element path boxes int as Long, float as Double, bool as Boolean;
    the one-array path must give exactly those, raise the same OverflowError
    past 64 bits, and leave anything else (here a nested list) to the loop.
    """
    import jpype
    import pytest
    from arcadedb_embedded.type_conversion import convert_python_to_java

    with arcadedb.create_database(temp_db_path) as db:
        values = [1, -(2**63), 2**63 - 1, 1.5, float("inf"), "x", "", True, None]
        for seq in (values, tuple(values)):
            got = convert_python_to_java(seq)
            assert str(got.getClass().getName()) == "java.util.ArrayList"
            classes = [
                (
                    None
                    if got.get(i) is None
                    else str(got.get(i).getClass().getSimpleName())
                )
                for i in range(got.size())
            ]
            assert classes == [
                "Long",
                "Long",
                "Long",
                "Double",
                "Double",
                "String",
                "String",
                "Boolean",
                None,
            ]
            got.add(jpype.JObject(0))  # still a growable ArrayList
        with pytest.raises(OverflowError):
            convert_python_to_java([1, 2**63])

        nested = convert_python_to_java([1, [2, 3], {"k": 4}])
        assert str(nested.get(1).getClass().getName()) == "java.util.ArrayList"
        assert str(nested.get(2).getClass().getName()) == "java.util.HashMap"

        db.command("sql", "CREATE DOCUMENT TYPE Item")
        db.command("sql", "CREATE PROPERTY Item.k LONG")
        db.command("sql", "CREATE INDEX ON Item (k) UNIQUE")
        with db.transaction():
            for k in range(100):
                db.command("sql", "INSERT INTO Item SET k = ?", k)
        ids = list(range(0, 100, 7))
        rows = db.query(
            "sql", "SELECT k FROM Item WHERE k IN :ids ORDER BY k", {"ids": ids}
        ).to_list()
        assert [r["k"] for r in rows] == ids


def test_array_conversion(temp_db_path):
    """Test Java list to Python list conversion."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE ArrayTest")
        db.command("sql", "CREATE PROPERTY ArrayTest.numbers LIST")
        db.command("sql", "CREATE PROPERTY ArrayTest.names LIST")

        with db.transaction():
            doc = db.new_document("ArrayTest")
            # Use Java collections via conversion
            from arcadedb_embedded.type_conversion import convert_python_to_java

            doc.set("numbers", convert_python_to_java([1, 2, 3, 4, 5]))
            doc.set("names", convert_python_to_java(["Alice", "Bob", "Charlie"]))
            doc.save()

        result = db.query("sql", "SELECT FROM ArrayTest")
        record = result.first()

        # Test array conversion
        numbers = record.get("numbers")
        assert isinstance(numbers, list)
        assert len(numbers) == 5
        assert numbers[0] == 1
        assert numbers[4] == 5

        names = record.get("names")
        assert isinstance(names, list)
        assert len(names) == 3
        assert "Alice" in names
        assert "Charlie" in names


class TestPrimitiveArrayFormats:
    """Regression tests for issue #4: int[]/long[] buffer formats ('=i'/'=q')
    crashed memoryview.tolist() and poisoned the converter cache."""

    def test_int_array(self):
        import jpype
        from arcadedb_embedded.type_conversion import convert_java_to_python

        assert convert_java_to_python(jpype.JArray(jpype.JInt)([1, 2, 3])) == [1, 2, 3]
        # second conversion exercises the cached converter
        assert convert_java_to_python(jpype.JArray(jpype.JInt)([4, 5])) == [4, 5]

    def test_long_array(self):
        import jpype
        from arcadedb_embedded.type_conversion import convert_java_to_python

        big = 2**40
        assert convert_java_to_python(jpype.JArray(jpype.JLong)([big, -big])) == [
            big,
            -big,
        ]

    def test_cache_not_poisoned_by_failure(self):
        from arcadedb_embedded import type_conversion as tc

        class Boom:
            pass

        def bad_converter(v):
            raise RuntimeError("first call fails")

        b = Boom()
        try:
            tc._register(b, bad_converter)
        except RuntimeError:
            pass
        assert type(b) not in tc._CONVERTER_CACHE
