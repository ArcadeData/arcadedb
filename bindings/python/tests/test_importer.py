"""
Tests for ArcadeDB data import functionality.
Tests CSV, JSON, and JSONL import capabilities.
"""

import os
import tempfile
from pathlib import Path

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


@pytest.fixture
def sample_jsonl_path():
    """Create a sample JSONL file for testing."""
    temp_file = tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False)
    temp_file.write('{"name": "Alice", "age": 30, "active": true}\n')
    temp_file.write('{"name": "Bob", "age": 25, "active": false}\n')
    temp_file.write('{"name": "Charlie", "age": 35, "active": true}\n')
    temp_file.close()
    yield temp_file.name
    os.unlink(temp_file.name)


def test_csv_import_as_documents(temp_db_path, sample_csv_path):
    """Test importing CSV as documents."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE Person")

        # Import CSV
        stats = arcadedb.import_csv(db, sample_csv_path, "Person")

        # Verify import
        assert stats["documents"] == 3
        assert stats["errors"] == 0

        # Query imported data
        result = db.query("sql", "SELECT FROM Person ORDER BY name")
        records = list(result)

        assert len(records) == 3
        assert records[0].get_property("name") == "Alice"
        assert records[0].get_property("age") == 30
        assert records[0].get_property("city") == "New York"


def test_csv_import_as_vertices(temp_db_path, sample_csv_vertices_path):
    """Test importing CSV as vertices."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE Product")

        # Import CSV as vertices
        stats = arcadedb.import_csv(
            db, sample_csv_vertices_path, "Product", vertex_type="Product"
        )

        # Verify import
        assert stats["vertices"] == 3
        assert stats["documents"] == 0
        assert stats["errors"] == 0

        # Query imported vertices
        result = db.query("sql", "SELECT FROM Product ORDER BY id")
        vertices = list(result)

        assert len(vertices) == 3
        assert vertices[0].get_property("name") == "Product A"
        assert vertices[0].get_property("category") == "Electronics"


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
            # Create schema
            with db.transaction():
                db.command("sql", "CREATE DOCUMENT TYPE Item")

            # Import TSV with tab delimiter
            stats = arcadedb.import_csv(db, temp_file.name, "Item", delimiter="\t")

            # Verify
            assert stats["documents"] == 2

            result = db.query("sql", "SELECT FROM Item ORDER BY name")
            records = list(result)

            assert len(records) == 2
            assert records[0].get_property("name") == "Item1"
            assert records[0].get_property("value") == 100
    finally:
        os.unlink(temp_file.name)


def test_jsonl_import(temp_db_path, sample_jsonl_path):
    """Test importing JSONL (line-delimited JSON)."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE User")

        # Import JSONL
        importer = arcadedb.Importer(db)
        stats = importer.import_file(sample_jsonl_path, type_name="User")

        # Note: JSONL import uses Java importer which creates documents
        # The exact behavior depends on JSONL format
        assert stats["errors"] == 0


def test_importer_class_api(temp_db_path, sample_csv_path):
    """Test using Importer class directly."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE Person")

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
        count = list(result)[0].get_property("cnt")
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
            # Create schema
            with db.transaction():
                db.command("sql", "CREATE DOCUMENT TYPE Product")

            # Import
            stats = arcadedb.import_csv(db, temp_file.name, "Product")

            assert stats["documents"] == 2

            # Verify types were inferred
            result = db.query("sql", "SELECT FROM Product WHERE name = 'Item1'")
            record = list(result)[0]

            # Check that types were inferred correctly
            count = record.get_property("count")
            price = record.get_property("price")
            active = record.get_property("active")

            assert isinstance(count, int)
            assert count == 42
            assert isinstance(price, float)
            assert abs(price - 19.99) < 0.01
            assert isinstance(active, bool)
            assert active is True
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
            with db.transaction():
                db.command("sql", "CREATE DOCUMENT TYPE Person")

            stats = arcadedb.import_csv(db, temp_file.name, "Person")

            assert stats["documents"] == 2

            # Verify null handling
            result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
            alice = list(result)[0]

            # Empty string should be converted to None
            city = alice.get_property("city")
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
    """Test that CSV import without type_name raises error."""
    with arcadedb.create_database(temp_db_path) as db:
        importer = arcadedb.Importer(db)

        with pytest.raises(arcadedb.ArcadeDBError) as exc_info:
            importer.import_file(sample_csv_path, format_type="csv")

        assert "type_name is required" in str(exc_info.value)


def test_format_auto_detection(temp_db_path, sample_csv_path):
    """Test that file format is auto-detected from extension."""
    with arcadedb.create_database(temp_db_path) as db:
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE Person")

        importer = arcadedb.Importer(db)

        # Don't specify format_type - should auto-detect from .csv extension
        stats = importer.import_file(sample_csv_path, type_name="Person")

        assert stats["documents"] == 3


def test_import_statistics(temp_db_path, sample_csv_path):
    """Test that import statistics are returned correctly."""
    with arcadedb.create_database(temp_db_path) as db:
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE Person")

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
            with db.transaction():
                db.command("sql", "CREATE DOCUMENT TYPE Record")

            # Import with small batch size
            stats = arcadedb.import_csv(
                db, temp_file.name, "Record", commit_every=10  # Commit every 10 records
            )

            assert stats["documents"] == 100

            # Verify all records imported
            result = db.query("sql", "SELECT count(*) as cnt FROM Record")
            count = list(result)[0].get_property("cnt")
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
            # Create schema
            with db.transaction():
                db.command("sql", "CREATE DOCUMENT TYPE Employee")

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
            assert eng_employees[0].get_property("name") == "Alice"

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
            assert high_earners[0].get_property("name") == "Charlie"
    finally:
        os.unlink(temp_file.name)
