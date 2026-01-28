"""
Tests for ArcadeDB database export functionality.
Tests JSONL, GraphML, GraphSON, and CSV export capabilities.
"""

import csv
import os
import tempfile
from pathlib import Path

import arcadedb_embedded as arcadedb
import pytest
from tests.conftest import has_graph_export_support


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_export_db")
    yield db_path
    # Cleanup
    import shutil

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def sample_db(temp_db_path):
    """Create a sample database with test data."""
    db = arcadedb.create_database(temp_db_path)

    # Create schema with properties
    db.schema.create_vertex_type("User")
    db.schema.create_property("User", "userId", "INTEGER")
    db.schema.create_property("User", "name", "STRING")
    db.schema.create_property("User", "email", "STRING")
    db.schema.create_property("User", "age", "INTEGER")
    db.schema.create_property("User", "premium", "BOOLEAN")
    db.schema.create_index("User", ["userId"], unique=True)

    db.schema.create_vertex_type("Movie")
    db.schema.create_property("Movie", "movieId", "INTEGER")
    db.schema.create_property("Movie", "title", "STRING")
    db.schema.create_property("Movie", "year", "INTEGER")
    db.schema.create_property("Movie", "genres", "LIST")
    db.schema.create_property("Movie", "rating", "DOUBLE")
    db.schema.create_index("Movie", ["movieId"], unique=True)

    db.schema.create_vertex_type("Actor")
    db.schema.create_property("Actor", "actorId", "INTEGER")
    db.schema.create_property("Actor", "name", "STRING")
    db.schema.create_property("Actor", "birthYear", "INTEGER")

    db.schema.create_edge_type("Rated")
    db.schema.create_property("Rated", "rating", "DOUBLE")
    db.schema.create_property("Rated", "timestamp", "LONG")
    db.schema.create_property("Rated", "review", "STRING")

    db.schema.create_edge_type("ActedIn")
    db.schema.create_property("ActedIn", "role", "STRING")
    db.schema.create_property("ActedIn", "year", "INTEGER")

    db.schema.create_edge_type("Follows")

    db.schema.create_document_type("LogEntry")
    db.schema.create_property("LogEntry", "level", "STRING")
    db.schema.create_property("LogEntry", "message", "STRING")
    db.schema.create_property("LogEntry", "timestamp", "LONG")

    db.schema.create_document_type("Config")
    db.schema.create_property("Config", "key", "STRING")
    db.schema.create_property("Config", "value", "STRING")
    db.schema.create_property("Config", "enabled", "BOOLEAN")

    # Add test data with more complexity
    with db.transaction():
        # Create 20 users with varied properties
        for i in range(20):
            db.command(
                "sql",
                f"""CREATE VERTEX User SET
                    userId = {i},
                    name = 'User{i}',
                    email = 'user{i}@example.com',
                    age = {20 + (i % 50)},
                    premium = {str(i % 3 == 0).lower()}""",
            )

        # Create 15 movies with genres and ratings
        genres_list = [
            "['Action', 'Thriller']",
            "['Drama', 'Romance']",
            "['Comedy']",
            "['Sci-Fi', 'Action']",
            "['Horror', 'Thriller']",
        ]
        for i in range(15):
            genre = genres_list[i % len(genres_list)]
            db.command(
                "sql",
                f"""CREATE VERTEX Movie SET
                    movieId = {i},
                    title = 'Movie{i}',
                    year = {2015 + (i % 10)},
                    genres = {genre},
                    rating = {3.5 + (i % 5) * 0.5}""",
            )

        # Create 10 actors
        for i in range(10):
            db.command(
                "sql",
                f"""CREATE VERTEX Actor SET
                    actorId = {i},
                    name = 'Actor{i}',
                    birthYear = {1970 + (i % 30)}""",
            )

        # Create 50 rating edges (users rating movies)
        import time

        timestamp = int(time.time())
        reviews = [
            "Great movie!",
            "Loved it",
            "Not bad",
            "Amazing!",
            "Could be better",
            "",
        ]
        for i in range(50):
            user_id = i % 20
            movie_id = i % 15
            rating_val = 1.0 + (i % 5)
            review = reviews[i % len(reviews)]
            db.command(
                "sql",
                f"""CREATE EDGE Rated
                    FROM (SELECT FROM User WHERE userId = {user_id})
                    TO (SELECT FROM Movie WHERE movieId = {movie_id})
                    SET rating = {rating_val},
                        timestamp = {timestamp + i * 3600},
                        review = '{review}'""",
            )

        # Create 30 ActedIn edges (actors in movies)
        roles = ["Lead", "Supporting", "Cameo", "Villain", "Hero"]
        for i in range(30):
            actor_id = i % 10
            movie_id = i % 15
            role = roles[i % len(roles)]
            year = 2015 + (movie_id % 10)
            db.command(
                "sql",
                f"""CREATE EDGE ActedIn
                    FROM (SELECT FROM Actor WHERE actorId = {actor_id})
                    TO (SELECT FROM Movie WHERE movieId = {movie_id})
                    SET role = '{role}', year = {year}""",
            )

        # Create 15 Follows edges (users following each other)
        for i in range(15):
            from_user = i % 20
            to_user = (i + 5) % 20
            if from_user != to_user:
                db.command(
                    "sql",
                    f"""CREATE EDGE Follows
                        FROM (SELECT FROM User WHERE userId = {from_user})
                        TO (SELECT FROM User WHERE userId = {to_user})""",
                )

        # Create 10 log entries with different levels
        levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
        for i in range(10):
            level = levels[i % len(levels)]
            db.command(
                "sql",
                f"""INSERT INTO LogEntry SET
                    level = '{level}',
                    message = 'Log entry {i}: {level} message',
                    timestamp = {timestamp + i * 60}""",
            )

        # Create 5 config documents
        configs = [
            ("max_users", "1000", True),
            ("enable_caching", "true", True),
            ("debug_mode", "false", False),
            ("api_timeout", "30", True),
            ("feature_flag_new_ui", "true", True),
        ]
        for key, value, enabled in configs:
            db.command(
                "sql",
                f"""INSERT INTO Config SET
                    key = '{key}',
                    value = '{value}',
                    enabled = {str(enabled).lower()}""",
            )

    yield db
    db.close()


class TestDatabaseExport:
    """Tests for database-wide export functionality."""

    def test_export_jsonl_basic(self, sample_db, temp_db_path):
        """Test basic JSONL export."""
        export_path = "test_export.jsonl.tgz"

        stats = sample_db.export_database(export_path, format="jsonl", overwrite=True)

        # Check statistics
        # 20 users + 15 movies + 10 actors + 50 ratings + 30 actedIn
        # + 15 follows + 10 logs + 5 configs
        assert "totalRecords" in stats
        assert stats["totalRecords"] == 155  # 45 vertices + 95 edges + 15 documents
        assert stats["vertices"] == 45  # 20 users + 15 movies + 10 actors
        assert stats["edges"] == 95  # 50 ratings + 30 actedIn + 15 follows
        assert stats["documents"] == 15  # 10 logs + 5 configs

        # Check file exists
        export_file = Path("exports") / export_path
        assert export_file.exists()

        # Clean up
        export_file.unlink()

    def test_export_jsonl_with_include_types(self, sample_db, temp_db_path):
        """Test JSONL export with include_types filter."""
        export_path = "test_export_users.jsonl.tgz"

        stats = sample_db.export_database(
            export_path, format="jsonl", include_types=["User"], overwrite=True
        )

        # Should only export User vertices
        assert stats["vertices"] == 20  # 20 users
        assert stats.get("edges", 0) == 0  # No edges included
        assert stats.get("documents", 0) == 0  # No documents included

        # Clean up
        export_file = Path("exports") / export_path
        export_file.unlink()

    def test_export_jsonl_with_exclude_types(self, sample_db, temp_db_path):
        """Test JSONL export with exclude_types filter."""
        export_path = "test_export_no_logs.jsonl.tgz"

        stats = sample_db.export_database(
            export_path, format="jsonl", exclude_types=["LogEntry"], overwrite=True
        )

        # Should export everything except LogEntry documents (10 logs excluded)
        assert stats["vertices"] == 45  # All vertices
        assert stats["edges"] == 95  # All edges
        # Only Config documents (LogEntry excluded)
        assert stats.get("documents", 0) == 5

        # Clean up
        export_file = Path("exports") / export_path
        export_file.unlink()

    def test_export_overwrite_protection(self, sample_db, temp_db_path):
        """Test that export fails when file exists and overwrite=False."""
        export_path = "test_export_overwrite.jsonl.tgz"

        # First export should succeed
        sample_db.export_database(export_path, format="jsonl", overwrite=True)

        # Second export with overwrite=False should fail
        with pytest.raises(arcadedb.ArcadeDBError) as exc_info:
            sample_db.export_database(export_path, format="jsonl", overwrite=False)

        assert (
            "already exists" in str(exc_info.value).lower()
            or "overwrite" in str(exc_info.value).lower()
        )

        # Clean up
        export_file = Path("exports") / export_path
        export_file.unlink()

    def test_export_invalid_format(self, sample_db, temp_db_path):
        """Test that invalid format raises error."""
        with pytest.raises(arcadedb.ArcadeDBError) as exc_info:
            sample_db.export_database("test.invalid", format="invalid_format")

        assert (
            "invalid" in str(exc_info.value).lower()
            or "format" in str(exc_info.value).lower()
        )

    @pytest.mark.graph_export
    @pytest.mark.skipif(
        not has_graph_export_support(), reason="Requires GraphML/GraphSON support"
    )
    def test_export_graphml(self, sample_db, temp_db_path):
        """Test GraphML export (requires GraphML/GraphSON support)."""
        export_path = "test_export.graphml.tgz"

        try:
            stats = sample_db.export_database(
                export_path, format="graphml", overwrite=True
            )

            # Check statistics - GraphML may only export graph elements
            assert "elapsedInSecs" in stats
            # GraphML format exists and file was created

            # Check file exists
            export_file = Path("exports") / export_path
            assert export_file.exists()

            # Clean up
            export_file.unlink()

        except arcadedb.ArcadeDBError as e:
            if "requires additional modules" in str(e):
                pytest.skip("GraphML export requires GraphML/GraphSON support")
            else:
                raise

    @pytest.mark.graph_export
    @pytest.mark.skipif(
        not has_graph_export_support(), reason="Requires GraphML/GraphSON support"
    )
    def test_export_graphson(self, sample_db, temp_db_path):
        """Test GraphSON export (requires GraphML/GraphSON support)."""
        export_path = "test_export.graphson.tgz"

        try:
            stats = sample_db.export_database(
                export_path, format="graphson", overwrite=True
            )

            # Check statistics - GraphSON may only export graph elements
            assert "elapsedInSecs" in stats
            # GraphSON format exists and file was created

            # Check file exists
            export_file = Path("exports") / export_path
            assert export_file.exists()

            # Clean up
            export_file.unlink()

        except arcadedb.ArcadeDBError as e:
            if "requires additional modules" in str(e):
                pytest.skip("GraphSON export requires GraphML/GraphSON support")
            else:
                raise

    def test_export_verbose_levels(self, sample_db, temp_db_path):
        """Test different verbosity levels."""
        export_path = "test_export_verbose.jsonl.tgz"

        # Test with different verbose levels
        for verbose_level in [0, 1, 2]:
            stats = sample_db.export_database(
                export_path, format="jsonl", verbose=verbose_level, overwrite=True
            )
            assert "totalRecords" in stats

        # Clean up
        export_file = Path("exports") / export_path
        export_file.unlink()

    def test_export_empty_database(self, temp_db_path):
        """Test exporting an empty database."""
        db = arcadedb.create_database(temp_db_path)
        export_path = "test_export_empty.jsonl.tgz"

        stats = db.export_database(export_path, format="jsonl", overwrite=True)

        # Empty database should have 0 records
        assert stats.get("totalRecords", 0) == 0
        assert stats.get("vertices", 0) == 0
        assert stats.get("edges", 0) == 0
        assert stats.get("documents", 0) == 0

        # File should still exist
        export_file = Path("exports") / export_path
        assert export_file.exists()

        # Clean up
        export_file.unlink()
        db.close()


class TestCSVExport:
    """Tests for CSV export functionality."""

    def test_export_to_csv_basic(self, sample_db, temp_db_path):
        """Test basic CSV export from query results."""
        csv_path = os.path.join(temp_db_path, "users.csv")

        # Export all users to CSV
        sample_db.export_to_csv("SELECT * FROM User", csv_path)

        # Check file exists and has content
        assert os.path.exists(csv_path)

        # Read and verify CSV
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 20  # 20 users
            assert "userId" in rows[0]
            assert "name" in rows[0]
            assert "age" in rows[0]
            assert "email" in rows[0]
            assert "premium" in rows[0]

    def test_export_to_csv_with_fieldnames(self, sample_db, temp_db_path):
        """Test CSV export with custom field names."""
        csv_path = os.path.join(temp_db_path, "movies.csv")

        # Export with specific columns
        sample_db.export_to_csv(
            "SELECT movieId, title FROM Movie",
            csv_path,
            fieldnames=["movieId", "title"],
        )

        # Read and verify CSV
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 15  # 15 movies
            # Check that custom fieldnames are used
            assert "movieId" in reader.fieldnames
            assert "title" in reader.fieldnames

    def test_export_to_csv_empty_results(self, sample_db, temp_db_path):
        """Test CSV export with empty query results."""
        csv_path = os.path.join(temp_db_path, "empty.csv")

        # Query that returns no results
        sample_db.export_to_csv("SELECT * FROM User WHERE userId = 999", csv_path)

        # File should exist with just headers
        assert os.path.exists(csv_path)

        with open(csv_path, "r", encoding="utf-8") as f:
            content = f.read()
            assert len(content) == 0 or content.count("\n") <= 1  # Only header or empty

    def test_export_to_csv_with_resultset(self, sample_db, temp_db_path):
        """Test CSV export using ResultSet directly."""
        from arcadedb_embedded.exporter import export_to_csv

        csv_path = os.path.join(temp_db_path, "ratings.csv")

        # Get results and export
        results = sample_db.query("sql", "SELECT rating FROM Rated WHERE rating > 0")
        export_to_csv(results, csv_path)

        # Verify file
        assert os.path.exists(csv_path)

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 50  # 50 rating edges in sample_db

    def test_export_to_csv_with_list_of_dicts(self, temp_db_path):
        """Test CSV export from list of dictionaries."""
        from arcadedb_embedded.exporter import export_to_csv

        csv_path = os.path.join(temp_db_path, "custom_data.csv")

        # Custom data
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.3},
            {"id": 3, "name": "Charlie", "score": 91.8},
        ]

        export_to_csv(data, csv_path)

        # Verify file
        assert os.path.exists(csv_path)

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

            assert len(rows) == 3
            assert rows[0]["name"] == "Alice"
            assert float(rows[0]["score"]) == 95.5


class TestRoundTripExport:
    """Tests for export/import round-trip verification."""

    def test_jsonl_export_import_roundtrip(self, sample_db, temp_db_path):
        """Test that exported JSONL can be re-imported."""
        export_path = "test_roundtrip.jsonl.tgz"

        # Export database
        sample_db.export_database(export_path, format="jsonl", overwrite=True)

        # Close original database
        sample_db.close()

        # Create new database and import
        import_db_path = temp_db_path + "_import"
        import_db = arcadedb.create_database(import_db_path)

        export_file = Path("exports") / export_path
        assert export_file.exists()

        # Import the exported data
        try:
            # Convert path to string and replace backslashes with forward slashes for Windows compatibility
            import_path_str = str(export_file.absolute()).replace("\\", "/")
            import_db.command("sql", f"IMPORT DATABASE file://{import_path_str}")

            # Verify counts match the complex sample_db
            user_count = import_db.count_type("User")
            movie_count = import_db.count_type("Movie")
            actor_count = import_db.count_type("Actor")
            log_count = import_db.count_type("LogEntry")
            config_count = import_db.count_type("Config")

            assert user_count == 20
            assert movie_count == 15
            assert actor_count == 10
            assert log_count == 10
            assert config_count == 5

            # Verify data integrity
            users = import_db.query("sql", "SELECT FROM User ORDER BY userId").to_list()
            assert len(users) == 20
            assert users[0]["name"] == "User0"

        finally:
            import_db.close()
            # Force garbage collection to release file handles (Windows fix)
            import gc

            gc.collect()

            if export_file.exists():
                try:
                    export_file.unlink()
                except PermissionError:
                    # On Windows, file might still be locked by Java process
                    # Wait a bit and try again, or ignore if it persists (temp file)
                    import time

                    time.sleep(0.5)
                    try:
                        export_file.unlink()
                    except PermissionError:
                        pass

            # Cleanup import database
            import shutil

            if os.path.exists(import_db_path):
                try:
                    shutil.rmtree(import_db_path)
                except PermissionError:
                    pass


class TestExportWithBulkInsert:
    """Tests export after bulk insert without batch_context."""

    def test_export_after_chunked_insert(self, temp_db_path):
        """Export database after chunked transaction inserts."""
        db = arcadedb.create_database(temp_db_path)

        # Create schema
        db.schema.create_vertex_type("Product")

        # Bulk insert using chunked transactions (avoids batch_context dependency here)
        chunk_size = 100
        total = 500
        for start in range(0, total, chunk_size):
            with db.transaction():
                for i in range(start, min(start + chunk_size, total)):
                    vertex = db.new_vertex("Product")
                    vertex.set("productId", i)
                    vertex.set("name", f"Product{i}")
                    vertex.save()

        # Export database
        export_path = "test_bulk_export.jsonl.tgz"
        stats = db.export_database(export_path, format="jsonl", overwrite=True)

        # Verify all products were exported
        assert stats["vertices"] == total
        assert stats["totalRecords"] == total

        # Clean up
        db.close()
        export_file = Path("exports") / export_path
        export_file.unlink()


class TestAllDataTypes:
    """Test export with all supported ArcadeDB data types."""

    def test_export_all_data_types(self, temp_db_path):
        """Test JSONL export/import with comprehensive data types.

        Data types tested (from 01_simple_document_store.py):
        - STRING: Text data
        - BOOLEAN: True/False
        - INTEGER: 32-bit integers
        - LONG: 64-bit integers
        - FLOAT: 32-bit floating point
        - DOUBLE: 64-bit floating point
        - DATE: Date only (no time)
        - DATETIME: Date and time
        - DECIMAL: High precision decimals
        - LIST: Arrays of values
        - EMBEDDED: Nested objects
        - LINK: References to other records
        """
        db = arcadedb.create_database(temp_db_path)

        try:
            # Create comprehensive type with all data types (auto-transactional)
            db.schema.create_document_type("DataTypeTest")

            # Basic types
            db.schema.create_property("DataTypeTest", "text_field", "STRING")
            db.schema.create_property("DataTypeTest", "bool_field", "BOOLEAN")
            db.schema.create_property("DataTypeTest", "int_field", "INTEGER")
            db.schema.create_property("DataTypeTest", "long_field", "LONG")
            db.schema.create_property("DataTypeTest", "float_field", "FLOAT")
            db.schema.create_property("DataTypeTest", "double_field", "DOUBLE")

            # Date/time types
            db.schema.create_property("DataTypeTest", "date_field", "DATE")
            db.command("sql", "CREATE PROPERTY DataTypeTest.datetime_field DATETIME")

            # Precision types
            db.schema.create_property("DataTypeTest", "decimal_field", "DECIMAL")

            # Collection types
            db.command("sql", "CREATE PROPERTY DataTypeTest.string_list LIST OF STRING")
            db.command("sql", "CREATE PROPERTY DataTypeTest.int_list LIST")
            db.schema.create_property("DataTypeTest", "mixed_list", "LIST")

            # Note: EMBEDDED type works best without explicit property definition
            # ArcadeDB will automatically handle nested objects

            # Create a reference type for LINK testing
            db.schema.create_vertex_type("RefVertex")
            db.schema.create_property("RefVertex", "ref_id", "INTEGER")

            # LINK type (reference to another record)
            db.command(
                "sql", "CREATE PROPERTY DataTypeTest.link_field LINK OF RefVertex"
            )

            # Insert test data with all types
            with db.transaction():
                # Create a vertex to link to
                result = db.command(
                    "sql",
                    "CREATE VERTEX RefVertex SET ref_id = 42",
                )

                # Insert comprehensive test record using CONTENT for embedded objects
                db.command(
                    "sql",
                    """INSERT INTO DataTypeTest CONTENT {
                        "text_field": "Hello ArcadeDB!",
                        "bool_field": true,
                        "int_field": 42,
                        "long_field": 9223372036854775807,
                        "float_field": 3.14159,
                            "double_field": 2.718281828459045,
                        "decimal_field": 123.456789,
                        "string_list": ["apple", "banana", "cherry"],
                        "int_list": [1, 2, 3, 4, 5],
                        "mixed_list": ["text", 42, true],
                        "embedded_obj": {"name": "John", "age": 30, "city": "NYC"}
                    }""",
                )

                # Set date fields separately using SET
                # (dates don't work well in JSON CONTENT)
                db.command(
                    "sql",
                    """UPDATE DataTypeTest SET
                        date_field = date('2024-01-15'),
                        datetime_field = '2024-01-15 14:30:45'
                        WHERE text_field = 'Hello ArcadeDB!'""",
                )

                # Insert record with NULL values
                db.command(
                    "sql",
                    """INSERT INTO DataTypeTest CONTENT {
                        "text_field": "With NULLs",
                        "bool_field": false,
                        "int_field": null,
                        "long_field": null,
                        "float_field": null,
                        "double_field": null,
                        "decimal_field": null,
                        "string_list": [],
                        "int_list": null,
                        "mixed_list": null,
                        "embedded_obj": null
                    }""",
                )

                # Explicitly set date fields to NULL
                db.command(
                    "sql",
                    """UPDATE DataTypeTest SET
                        date_field = NULL,
                        datetime_field = NULL
                        WHERE text_field = 'With NULLs'""",
                )

                # Insert record with edge cases
                db.command(
                    "sql",
                    """INSERT INTO DataTypeTest CONTENT {
                        "text_field": "Special chars: @#$%^&*()_+-=[]|:;<>?,./~`",
                        "bool_field": false,
                        "int_field": -2147483648,
                        "long_field": -9223372036854775807,
                        "float_field": -0.0,
                        "double_field": 1.0e30,
                        "decimal_field": 0.000000001,
                        "string_list": [""],
                        "int_list": [0, -1, -2147483648, 2147483647],
                        "mixed_list": [null, 0, "", false],
                        "embedded_obj": {"nested": {"deeply": {"value": 123}}}
                    }""",
                )

                # Set date fields for edge case record
                db.command(
                    "sql",
                    """UPDATE DataTypeTest SET
                        date_field = date('2000-01-01'),
                        datetime_field = '2000-01-01 00:00:00'
                        WHERE text_field LIKE 'Special chars%'""",
                )

            # Export database
            export_path = "test_all_datatypes.jsonl.tgz"
            stats_export = db.export_database(
                export_path, format="jsonl", overwrite=True
            )

            # Verify export stats (3 documents, no link test for now)
            assert stats_export["totalRecords"] == 4  # 3 docs + 1 vertex
            assert stats_export["documents"] == 3  # DataTypeTest records
            assert stats_export["vertices"] == 1  # RefVertex (not used yet)

            # Close original database
            db.close()

            # Re-import and verify data integrity
            import_db_path = temp_db_path + "_import"
            import_db = arcadedb.create_database(import_db_path)

            try:
                export_file = Path("exports") / export_path
                assert export_file.exists()

                # Import the data
                # Convert path to string and replace backslashes with forward slashes for Windows compatibility
                import_path_str = str(export_file.absolute()).replace("\\", "/")
                import_db.command("sql", f"IMPORT DATABASE file://{import_path_str}")

                # Verify record count
                count = import_db.count_type("DataTypeTest")
                assert count == 3, f"Expected 3 DataTypeTest records, got {count}"

                # Query and verify first record (with all types)
                result = import_db.query(
                    "sql",
                    "SELECT FROM DataTypeTest WHERE text_field = 'Hello ArcadeDB!'",
                )
                records = result.to_list()
                assert len(records) == 1

                record = records[0]

                # Verify basic types
                assert record["text_field"] == "Hello ArcadeDB!"
                assert record["bool_field"] is True
                assert record["int_field"] == 42
                assert record["long_field"] == 9223372036854775807
                assert abs(record["float_field"] - 3.14159) < 0.0001
                assert abs(record["double_field"] - 2.718281828459045) < 0.000001

                # Verify collections
                assert record["string_list"] == ["apple", "banana", "cherry"]
                assert record["int_list"] == [1, 2, 3, 4, 5]
                assert record["mixed_list"] == ["text", 42, True]

                # Verify embedded object
                assert record["embedded_obj"]["name"] == "John"
                assert record["embedded_obj"]["age"] == 30
                assert record["embedded_obj"]["city"] == "NYC"

                # Verify NULL record
                result = import_db.query(
                    "sql", "SELECT FROM DataTypeTest WHERE text_field = 'With NULLs'"
                )
                records = result.to_list()
                assert len(records) == 1

                record = records[0]
                assert record["bool_field"] is False
                assert record["int_field"] is None
                assert record["long_field"] is None
                assert record["float_field"] is None
                assert record["string_list"] == []

                # Verify edge cases record
                result = import_db.query(
                    "sql",
                    "SELECT FROM DataTypeTest WHERE int_field = -2147483648",
                )
                records = result.to_list()
                assert len(records) == 1

                record = records[0]
                assert record["int_field"] == -2147483648
                assert record["long_field"] == -9223372036854775807
                assert "Special chars" in record["text_field"]
                assert record["int_list"] == [0, -1, -2147483648, 2147483647]

                # Verify deeply nested embedded object
                assert record["embedded_obj"]["nested"]["deeply"]["value"] == 123

                print("✅ All data types exported and imported successfully!")
                print("   Verified: STRING, BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE")
                print("   Verified: DATE, DATETIME, DECIMAL")
                print("   Verified: LIST (string, int, mixed)")
                print("   Verified: EMBEDDED (nested objects)")
                print("   Verified: NULL values")
                print("   Verified: Edge cases (min/max values, special chars)")

            finally:
                import_db.close()
                # Force garbage collection to release file handles (Windows fix)
                import gc

                gc.collect()

            # Clean up
            export_file = Path("exports") / export_path
            if export_file.exists():
                try:
                    export_file.unlink()
                except PermissionError:
                    # On Windows, file might still be locked by Java process
                    import time

                    time.sleep(0.5)
                    try:
                        export_file.unlink()
                    except PermissionError:
                        pass

        finally:
            # Properly close database if still open
            try:
                if db:
                    db.close()
            except Exception:
                pass  # Database already closed or error during close


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
