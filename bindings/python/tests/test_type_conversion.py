"""
Tests for type conversion between Java and Python types.
"""

from datetime import date, datetime
from decimal import Decimal

import arcadedb_embedded as arcadedb


def test_basic_type_conversion(temp_db_path):
    """Test basic type conversion for common data types."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("TypeTest")

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
        # Schema operations are auto-transactional
        db.schema.create_document_type("DecimalTest")
        db.schema.create_property("DecimalTest", "price", "DECIMAL")

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
        # Schema operations are auto-transactional
        db.schema.create_document_type("DateTest")
        db.schema.create_property("DateTest", "created_date", "DATE")
        db.schema.create_property("DateTest", "created_datetime", "DATETIME")

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


def test_collection_conversion(temp_db_path):
    """Test Java collections (List, Set, Map) to Python conversion."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("CollectionTest")

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


def test_nested_collection_conversion(temp_db_path):
    """Test conversion of nested collections."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("NestedTest")

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
        # Schema operations are auto-transactional
        db.schema.create_document_type("PropsTest")

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
        # Schema operations are auto-transactional
        db.schema.create_document_type("DictTest")

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
        # Schema operations are auto-transactional
        db.schema.create_document_type("JsonTest")

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
        # Schema operations are auto-transactional
        db.schema.create_document_type("PyToJavaTest")

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


def test_array_conversion(temp_db_path):
    """Test Java list to Python list conversion."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("ArrayTest")
        db.schema.create_property("ArrayTest", "numbers", "LIST")
        db.schema.create_property("ArrayTest", "names", "LIST")

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
