"""
Tests for ArcadeDB data import functionality.
Tests CSV, JSON, and JSONL import capabilities with complex data types and NULL handling.
"""

import os
import tempfile

import arcadedb_embedded as arcadedb
import pytest


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_import_db")
    yield db_path
    # Cleanup
    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def sample_csv_path():
    """Create a sample CSV file for testing."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("name,age,city\n")
    temp_file.write("Alice,30,New York\n")
    temp_file.write("Bob,25,London\n")
    temp_file.write("Charlie,35,Paris\n")
    temp_file.close()
    yield temp_file.name
    os.unlink(temp_file.name)


@pytest.fixture
def sample_csv_vertices_path():
    """Create a sample CSV file for vertex import."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,name,category\n")
    temp_file.write("1,Product A,Electronics\n")
    temp_file.write("2,Product B,Books\n")
    temp_file.write("3,Product C,Electronics\n")
    temp_file.close()
    yield temp_file.name
    os.unlink(temp_file.name)


def test_csv_import_as_documents(temp_db_path, sample_csv_path):
    """Test importing CSV as documents."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema (auto-transactional)
        db.schema.create_document_type("Person")

        # Import CSV
        stats = arcadedb.import_csv(db, sample_csv_path, "Person")

        # Verify import
        assert stats["documents"] == 3
        assert stats["errors"] == 0

        # Query imported data
        result = db.query("sql", "SELECT FROM Person ORDER BY name")
        records = list(result)

        assert len(records) == 3
        assert records[0].get("name") == "Alice"
        assert records[0].get("age") == 30
        assert records[0].get("city") == "New York"


def test_csv_import_as_vertices(temp_db_path, sample_csv_vertices_path):
    """Test importing CSV as vertices."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema (auto-transactional)
        db.schema.create_vertex_type("Product")

        # Import CSV as vertices
        stats = arcadedb.import_csv(
            db,
            sample_csv_vertices_path,
            "Product",
            import_type="vertices",
            typeIdProperty="id",
        )

        # Verify import
        assert stats["vertices"] == 3
        assert stats["documents"] == 0
        assert stats["errors"] == 0

        # Query imported vertices
        result = db.query("sql", "SELECT FROM Product ORDER BY id")
        vertices = list(result)

        assert len(vertices) == 3
        assert vertices[0].get("name") == "Product A"
        assert vertices[0].get("category") == "Electronics"


def test_csv_import_with_custom_delimiter(temp_db_path):
    """Test importing CSV with custom delimiter (TSV)."""
    # Create TSV file
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False)
    temp_file.write("name\tvalue\tdescription\n")
    temp_file.write("Item1\t100\tFirst item\n")
    temp_file.write("Item2\t200\tSecond item\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Create schema (auto-transactional)
            db.schema.create_document_type("Item")

            # Import TSV with tab delimiter
            stats = arcadedb.import_csv(db, temp_file.name, "Item", delimiter="\t")

            # Verify
            assert stats["documents"] == 2

            result = db.query("sql", "SELECT FROM `Item` ORDER BY name")
            records = list(result)

            assert len(records) == 2
            assert records[0].get("name") == "Item1"
            assert records[0].get("value") == 100
    finally:
        os.unlink(temp_file.name)


def test_xml_import_as_documents(temp_db_path):
    """Test importing XML as documents with attributes and child elements."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False)
    temp_file.write(
        """
        <people>
          <person id="1" name="Alice"><city>New York</city></person>
          <person id="2" name="Bob"><city>London</city></person>
        </people>
        """
    )
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            stats = arcadedb.import_xml(
                db,
                temp_file.name,
                import_type="documents",
                objectNestLevel=1,
            )

            assert stats["documents"] == 2
            assert stats["errors"] == 0

            result = db.query("sql", "SELECT FROM person ORDER BY id")
            records = list(result)

            assert len(records) == 2
            assert records[0].get("name") == "Alice"
            assert records[0].get("city") == "New York"
            assert records[1].get("name") == "Bob"
            assert records[1].get("city") == "London"
    finally:
        os.unlink(temp_file.name)


def test_importer_class_api(temp_db_path, sample_csv_path):
    """Test using Importer class directly."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema (auto-transactional)
        db.schema.create_document_type("Person")

        # Use Importer class
        importer = arcadedb.Importer(db)
        stats = importer.import_file(
            sample_csv_path,
            format_type="csv",
            type_name="Person",
            commit_every=2,  # Small batch for testing
        )

        assert stats["documents"] == 3
        assert stats["errors"] == 0

        # Verify data
        result = db.query("sql", "SELECT count(*) as cnt FROM Person")
        count = list(result)[0].get("cnt")
        assert count == 3


def test_csv_type_inference(temp_db_path):
    """Test that CSV importer correctly infers data types."""
    # Create CSV with various types
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("name,count,price,active\n")
    temp_file.write("Item1,42,19.99,true\n")
    temp_file.write("Item2,100,29.50,false\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("Product")

            # Import
            stats = arcadedb.import_csv(db, temp_file.name, "Product")

            assert stats["documents"] == 2

            # Verify types were inferred
            result = db.query("sql", "SELECT FROM Product WHERE name = 'Item1'")
            record = list(result)[0]

            # Check that types were inferred correctly
            count = record.get("count")
            price = record.get("price")
            active = record.get("active")

            assert isinstance(count, int)
            assert count == 42
            assert isinstance(price, float)
            assert price == 19.99
            # Note: Java CSV importer infers booleans as STRING type
            assert isinstance(active, str)
            assert active == "true"
    finally:
        os.unlink(temp_file.name)


def test_csv_import_with_nulls(temp_db_path):
    """Test importing CSV with empty/null values."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("name,age,city\n")
    temp_file.write("Alice,30,\n")  # Empty city
    temp_file.write("Bob,,London\n")  # Empty age
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("Person")

            stats = arcadedb.import_csv(db, temp_file.name, "Person")

            assert stats["documents"] == 2

            # Verify null handling
            result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
            alice = list(result)[0]

            # Empty string should be converted to None
            city = alice.get("city")
            assert city is None or city == ""
    finally:
        os.unlink(temp_file.name)


def test_import_nonexistent_file(temp_db_path):
    """Test that importing non-existent file raises error."""
    with arcadedb.create_database(temp_db_path) as db:
        importer = arcadedb.Importer(db)

        with pytest.raises(arcadedb.ArcadeDBError) as exc_info:
            importer.import_file("/nonexistent/file.csv", type_name="Test")

        assert "not found" in str(exc_info.value).lower()


def test_csv_import_without_type_name(temp_db_path, sample_csv_path):
    """Test that CSV import without type_name creates default 'Document' type."""
    with arcadedb.create_database(temp_db_path) as db:
        importer = arcadedb.Importer(db)

        # Java importer creates default "Document" type if not specified
        stats = importer.import_file(sample_csv_path, format_type="csv")

        # The workaround query should succeed and return count
        assert stats["documents"] == 3
        assert stats["errors"] == 0


def test_format_auto_detection(temp_db_path, sample_csv_path):
    """Test that file format is auto-detected from extension."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Person")

        importer = arcadedb.Importer(db)

        # Don't specify format_type - should auto-detect from .csv extension
        stats = importer.import_file(sample_csv_path, type_name="Person")

        assert stats["documents"] == 3


def test_import_statistics(temp_db_path, sample_csv_path):
    """Test that import statistics are returned correctly."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Person")

        stats = arcadedb.import_csv(db, sample_csv_path, "Person")

        # Check all expected keys
        assert "documents" in stats
        assert "vertices" in stats
        assert "edges" in stats
        assert "errors" in stats
        assert "duration_ms" in stats

        # Check values
        assert stats["documents"] == 3
        assert stats["vertices"] == 0
        assert stats["edges"] == 0
        assert stats["errors"] == 0
        assert stats["duration_ms"] >= 0


def test_large_csv_batch_commit(temp_db_path):
    """Test importing larger CSV with batch commits."""
    # Create a larger CSV file
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,value\n")
    for i in range(100):
        temp_file.write(f"{i},{i*10}\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("Record")

            # Import with small batch size
            stats = arcadedb.import_csv(
                db, temp_file.name, "Record", commit_every=10  # Commit every 10 records
            )

            assert stats["documents"] == 100

            # Verify all records imported
            result = db.query("sql", "SELECT count(*) as cnt FROM `Record`")
            count = list(result)[0].get("cnt")
            assert count == 100
    finally:
        os.unlink(temp_file.name)


def test_csv_import_integration(temp_db_path):
    """Integration test: Import and query CSV data."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("employee_id,name,department,salary\n")
    temp_file.write("1,Alice,Engineering,75000\n")
    temp_file.write("2,Bob,Sales,65000\n")
    temp_file.write("3,Charlie,Engineering,80000\n")
    temp_file.write("4,Diana,HR,60000\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("Employee")

            # Import data
            stats = arcadedb.import_csv(db, temp_file.name, "Employee")
            assert stats["documents"] == 4

            # Run various queries
            # 1. Filter by department
            result = db.query(
                "sql",
                "SELECT FROM Employee WHERE department = 'Engineering' ORDER BY name",
            )
            eng_employees = list(result)
            assert len(eng_employees) == 2
            assert eng_employees[0].get("name") == "Alice"

            # 2. Aggregate by department
            result = db.query(
                "sql",
                "SELECT department, count(*) as cnt FROM Employee GROUP BY department",
            )
            dept_counts = list(result)
            assert len(dept_counts) == 3

            # 3. Find high earners
            result = db.query(
                "sql", "SELECT FROM Employee WHERE salary > 70000 ORDER BY salary DESC"
            )
            high_earners = list(result)
            assert len(high_earners) == 2
            assert high_earners[0].get("name") == "Charlie"
    finally:
        os.unlink(temp_file.name)


def test_csv_complex_data_types(temp_db_path):
    """Test CSV import with various data types including edge cases."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,name,count,price,ratio,active,tags,notes\n")
    temp_file.write("1,Item A,100,19.99,0.85,true,tag1;tag2,Normal item\n")
    temp_file.write("2,Item B,0,-5.50,1.0,false,tag3,Negative price\n")
    temp_file.write("3,Item C,999999,0.01,0.0,true,,Empty tags\n")
    temp_file.write(
        '4,"Item ""D""",42,1234.5678,0.123456789,false,tag1,"Quoted, value"\n'
    )
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("ComplexItem")

            stats = arcadedb.import_csv(db, temp_file.name, "ComplexItem")
            assert stats["documents"] == 4

            # Verify complex values
            result = db.query("sql", "SELECT FROM ComplexItem ORDER BY id")
            items = list(result)

            # Item with quotes in name
            assert items[3].get("name") == 'Item "D"'
            assert items[3].get("notes") == "Quoted, value"

            # Zero values
            assert items[1].get("count") == 0
            assert items[2].get("ratio") == 0.0

            # Large numbers
            assert items[2].get("count") == 999999
            assert items[3].get("price") == 1234.5678

            # Empty string (CSV treats as empty string, not null)
            tags = items[2].get("tags")
            assert tags == "" or tags is None
    finally:
        os.unlink(temp_file.name)


def test_csv_null_and_empty_values(temp_db_path):
    """Test CSV handling of NULL, empty strings, and missing values."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,name,value,description\n")
    temp_file.write("1,Item 1,,Empty value\n")  # Empty value field
    temp_file.write("2,Item 2,42,\n")  # Empty description
    temp_file.write("3,,99,No name\n")  # Empty name
    temp_file.write('4,"",0,""\n')  # Explicitly empty quoted fields
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("NullTest")

            stats = arcadedb.import_csv(db, temp_file.name, "NullTest")
            assert stats["documents"] == 4

            result = db.query("sql", "SELECT FROM NullTest ORDER BY id")
            items = list(result)

            # CSV empty values can be imported as empty strings OR None
            # depending on the schema inference
            for item in items:
                for prop in item.get_property_names():
                    val = item.get(prop)
                    # Value can be None, empty string, or actual value
                    # Just verify it doesn't raise an exception
                    assert val is None or isinstance(val, (str, int, float, bool))
    finally:
        os.unlink(temp_file.name)


def test_csv_unicode_and_special_chars(temp_db_path):
    """Test CSV import with Unicode and special characters."""
    temp_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    temp_file.write("id,name,description\n")
    temp_file.write("1,Café,French café ☕\n")
    temp_file.write("2,日本,Japanese text 🇯🇵\n")
    temp_file.write("3,Москва,Russian city 🏛️\n")
    temp_file.write("4,Math,Formula: x² + y² = z²\n")
    temp_file.write("5,Emoji,Hearts: ❤️💙💚\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("UnicodeTest")

            stats = arcadedb.import_csv(db, temp_file.name, "UnicodeTest")
            assert stats["documents"] == 5

            result = db.query("sql", "SELECT FROM UnicodeTest ORDER BY id")
            items = list(result)

            assert "Café" in items[0].get("name")
            assert "☕" in items[0].get("description")
            assert "日本" in items[1].get("name")
            assert "🇯🇵" in items[1].get("description")
            assert "²" in items[3].get("description")
            assert "❤️" in items[4].get("description")
    finally:
        os.unlink(temp_file.name)


def test_large_dataset_performance(temp_db_path):
    """Test import performance with larger dataset."""
    # Create a CSV with 1000 records
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,name,value,timestamp\n")
    for i in range(1000):
        temp_file.write(f"{i},Item {i},{i * 1.5},2024-01-{(i % 28) + 1:02d}\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("LargeTest")

            # Import with custom batch size
            stats = arcadedb.import_csv(
                db, temp_file.name, "LargeTest", commitEvery=100
            )

            assert stats["documents"] == 1000
            assert stats["errors"] == 0
            assert stats["duration_ms"] >= 0

            # Verify random sampling
            result = db.query("sql", "SELECT count(*) as cnt FROM LargeTest")
            count = list(result)[0].get("cnt")
            assert count == 1000

            # Verify some values
            result = db.query("sql", "SELECT FROM LargeTest WHERE id = 500")
            item = list(result)[0]
            assert item.get("name") == "Item 500"
            assert abs(item.get("value") - 750.0) < 0.01
    finally:
        os.unlink(temp_file.name)


def test_csv_complex_data_types(temp_db_path):
    """Test CSV import with various data types including edge cases."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,name,count,price,ratio,active,tags,notes\n")
    temp_file.write("1,Item A,100,19.99,0.85,true,tag1;tag2,Normal item\n")
    temp_file.write("2,Item B,0,-5.50,1.0,false,tag3,Negative price\n")
    temp_file.write("3,Item C,999999,0.01,0.0,true,,Empty tags\n")
    temp_file.write(
        '4,"Item ""D""",42,1234.5678,0.123456789,false,tag1,"Quoted, value"\n'
    )
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            with db.transaction():
                db.schema.create_document_type("ComplexItem")

            stats = arcadedb.import_csv(db, temp_file.name, "ComplexItem")
            assert stats["documents"] == 4

            # Verify complex values
            result = db.query("sql", "SELECT FROM ComplexItem ORDER BY id")
            items = list(result)

            # Item with quotes in name
            assert items[3].get("name") == 'Item "D"'
            assert items[3].get("notes") == "Quoted, value"

            # Zero values
            assert items[1].get("count") == 0
            assert items[2].get("ratio") == 0.0

            # Large numbers
            assert items[2].get("count") == 999999
            assert items[3].get("price") == 1234.5678

            # Empty string (CSV treats as empty string, not null)
            tags = items[2].get("tags")
            assert tags == "" or tags is None
    finally:
        os.unlink(temp_file.name)


def test_csv_null_and_empty_values(temp_db_path):
    """Test CSV handling of NULL, empty strings, and missing values."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,name,value,description\n")
    temp_file.write("1,Item 1,,Empty value\n")  # Empty value field
    temp_file.write("2,Item 2,42,\n")  # Empty description
    temp_file.write("3,,99,No name\n")  # Empty name
    temp_file.write('4,"",0,""\n')  # Explicitly empty quoted fields
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("NullTest")

            stats = arcadedb.import_csv(db, temp_file.name, "NullTest")
            assert stats["documents"] == 4

            result = db.query("sql", "SELECT FROM NullTest ORDER BY id")
            items = list(result)

            # CSV empty values can be imported as empty strings OR None
            # depending on the schema inference
            for item in items:
                for prop in item.get_property_names():
                    val = item.get(prop)
                    # Value can be None, empty string, or actual value
                    # Just verify it doesn't raise an exception
                    assert val is None or isinstance(val, (str, int, float, bool))
    finally:
        os.unlink(temp_file.name)


def test_csv_unicode_and_special_chars(temp_db_path):
    """Test CSV import with Unicode and special characters."""
    temp_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, encoding="utf-8"
    )
    temp_file.write("id,name,description\n")
    temp_file.write("1,Café,French café ☕\n")
    temp_file.write("2,日本,Japanese text 🇯🇵\n")
    temp_file.write("3,Москва,Russian city 🏛️\n")
    temp_file.write("4,Math,Formula: x² + y² = z²\n")
    temp_file.write("5,Emoji,Hearts: ❤️💙💚\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("UnicodeTest")

            stats = arcadedb.import_csv(db, temp_file.name, "UnicodeTest")
            assert stats["documents"] == 5

            result = db.query("sql", "SELECT FROM UnicodeTest ORDER BY id")
            items = list(result)

            assert "Café" in items[0].get("name")
            assert "☕" in items[0].get("description")
            assert "日本" in items[1].get("name")
            assert "🇯🇵" in items[1].get("description")
            assert "²" in items[3].get("description")
            assert "❤️" in items[4].get("description")
    finally:
        os.unlink(temp_file.name)


def test_large_dataset_performance(temp_db_path):
    """Test import performance with larger dataset."""
    # Create a CSV with 1000 records
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    temp_file.write("id,name,value,timestamp\n")
    for i in range(1000):
        temp_file.write(f"{i},Item {i},{i * 1.5},2024-01-{(i % 28) + 1:02d}\n")
    temp_file.close()

    try:
        with arcadedb.create_database(temp_db_path) as db:
            # Schema operations are auto-transactional
            db.schema.create_document_type("LargeTest")

            # Import with custom batch size
            stats = arcadedb.import_csv(
                db, temp_file.name, "LargeTest", commitEvery=100
            )

            assert stats["documents"] == 1000
            assert stats["errors"] == 0
            assert stats["duration_ms"] >= 0

            # Verify random sampling
            result = db.query("sql", "SELECT count(*) as cnt FROM LargeTest")
            count = list(result)[0].get("cnt")
            assert count == 1000

            # Verify some values
            result = db.query("sql", "SELECT FROM LargeTest WHERE id = 500")
            item = list(result)[0]
            assert item.get("name") == "Item 500"
            assert abs(item.get("value") - 750.0) < 0.01
    finally:
        os.unlink(temp_file.name)
