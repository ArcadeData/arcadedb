"""
Core functionality tests for ArcadeDB Python bindings.
These tests work with our base package (includes SQL, OpenCypher, Studio).
"""

import json

import arcadedb_embedded as arcadedb
import pytest


def test_database_creation(temp_db_path):
    """Test creating a new database."""
    db = arcadedb.create_database(temp_db_path)
    assert db.is_open()
    db.close()
    assert not db.is_open()


def test_database_operations(temp_db_path):
    """Test basic database operations."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create a document type (schema ops auto-transactional)
        db.command("sql", "CREATE DOCUMENT TYPE TestDoc")

        # Insert data
        with db.transaction():
            db.command("sql", "INSERT INTO TestDoc SET name = 'test', value = 42")

        # Query data
        result = db.query("sql", "SELECT FROM TestDoc WHERE name = 'test'")
        records = list(result)

        assert len(records) == 1
        record = records[0]
        assert record.get("name") == "test"
        assert record.get("value") == 42


def test_sql_count_on_empty_type_returns_zero(temp_db_path):
    """Test SQL count(*) returns a zero row for an empty type."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE EmptyDoc")

        result = db.query("sql", "SELECT count(*) as total FROM EmptyDoc")
        row = result.one()

        assert row.get("total") == 0


def test_rich_data_types(temp_db_path):
    """Test ArcadeDB's rich data type support with comprehensive CRUD operations.

    This test validates:
    - Schema creation with multiple data types (STRING, BOOLEAN, INTEGER, FLOAT,
      DECIMAL, DATE, DATETIME)
    - Built-in SQL functions (uuid(), date(), sysdate())
    - CRUD operations with type validation
    - Aggregation queries and filtering
    """
    with arcadedb.create_database(temp_db_path) as db:
        # Create document type with rich data types (schema ops auto-transactional)
        db.command("sql", "CREATE DOCUMENT TYPE Task")

        # Define properties with various ArcadeDB data types
        db.command("sql", "CREATE PROPERTY Task.title STRING")
        db.command("sql", "CREATE PROPERTY Task.priority STRING")
        db.command("sql", "CREATE PROPERTY Task.completed BOOLEAN")
        db.command("sql", "CREATE PROPERTY Task.created_date DATE")
        db.command("sql", "CREATE PROPERTY Task.due_datetime DATETIME")
        db.command("sql", "CREATE PROPERTY Task.estimated_hours FLOAT")
        db.command("sql", "CREATE PROPERTY Task.priority_score INTEGER")
        db.command("sql", "CREATE PROPERTY Task.cost DECIMAL")
        db.command("sql", "CREATE PROPERTY Task.task_id STRING")  # UUID as string

        # Insert sample data showcasing various data types
        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO Task SET
                    title = 'Setup Development Environment',
                    priority = 'high',
                    completed = false,
                    created_date = date('2024-01-15'),
                    due_datetime = sysdate(),
                    estimated_hours = 4.5,
                    priority_score = 8,
                    cost = 150.75,
                    task_id = uuid()
            """,
            )

            db.command(
                "sql",
                """
                INSERT INTO Task SET
                    title = 'Write Documentation',
                    priority = 'medium',
                    completed = true,
                    created_date = date(),
                    due_datetime = sysdate(),
                    estimated_hours = 2.0,
                    priority_score = 5,
                    cost = 75.00,
                    task_id = uuid()
            """,
            )  # Test data types and values
        result = db.query(
            "sql", "SELECT FROM Task WHERE title = " "'Setup Development Environment'"
        )
        records = list(result)

        assert len(records) == 1
        record = records[0]

        # Verify data types and values
        assert record.get("title") == "Setup Development Environment"
        assert record.get("priority") == "high"
        assert record.get("completed") is False
        assert record.get("estimated_hours") == 4.5
        assert record.get("priority_score") == 8
        assert record.get("cost") is not None  # DECIMAL type
        assert record.get("task_id") is not None  # UUID as string
        assert record.get("created_date") is not None  # DATE type
        assert record.get("due_datetime") is not None  # DATETIME type

        # Test aggregation queries
        result = db.query("sql", "SELECT count(*) as total FROM Task")
        total = list(result)[0].get("total")
        assert total == 2

        # Test filtering by boolean
        result = db.query("sql", "SELECT FROM Task WHERE completed = true")
        completed_tasks = list(result)
        assert len(completed_tasks) == 1
        assert completed_tasks[0].get("title") == "Write Documentation"

        # Test UPDATE operations
        with db.transaction():
            db.command(
                "sql",
                "UPDATE Task SET completed = true "
                "WHERE title = 'Setup Development Environment'",
            )

        # Verify update
        result = db.query(
            "sql",
            "SELECT count(*) as completed_count FROM Task " "WHERE completed = true",
        )
        completed_count = list(result)[0].get("completed_count")
        assert completed_count == 2

        # Test DELETE operations
        with db.transaction():
            db.command("sql", "DELETE FROM Task WHERE completed = true")

        # Verify deletion
        result = db.query("sql", "SELECT count(*) as remaining FROM Task")
        remaining = list(result)[0].get("remaining")
        assert remaining == 0


def test_arcadedb_sql_features(temp_db_path):
    """Test ArcadeDB SQL dialect features and built-in functions.

    This test validates:
    - Built-in SQL functions (uuid(), date(), sysdate())
    - JSON-like document operations with embedded objects
    - ArcadeDB-specific SQL extensions
    - Data type handling in queries
    """
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE TestEntity")

        # Test built-in SQL functions
        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO TestEntity SET
                    id = uuid(),
                    created_at = sysdate(),
                    custom_date = date('2024-01-01'),
                    custom_datetime = sysdate()
            """,
            )

        # Test querying with functions
        result = db.query("sql", "SELECT FROM TestEntity WHERE created_at IS NOT NULL")
        records = list(result)

        assert len(records) == 1
        record = records[0]

        # Verify function results
        assert record.get("id") is not None  # UUID function worked
        assert record.get("created_at") is not None  # sysDate() worked
        assert record.get("custom_date") is not None  # date() worked
        assert record.get("custom_datetime") is not None  # datetime() worked

        # Test JSON-like document operations
        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO TestEntity SET
                    name = 'Test Document',
                    metadata = {
                        'tags': ['test', 'demo'],
                        'priority': 5,
                        'active': true
                    }
            """,
            )

        # Query embedded document properties
        result = db.query("sql", "SELECT FROM TestEntity WHERE name = 'Test Document'")
        doc_record = list(result)[0]

        assert doc_record.get("name") == "Test Document"
        metadata = doc_record.get("metadata")
        assert metadata is not None
        # Metadata is a Java LinkedHashMap, not a Python dict
        assert hasattr(metadata, "get")  # Check it's a map-like object


def test_fulltext_search_with_score(temp_db_path):
    """Full-text search returns results with $score."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE Article")
        db.command("sql", "CREATE PROPERTY Article.content STRING")
        db.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT")

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO Article SET content = 'Database search with Lucene'",
            )
            db.command(
                "sql",
                "INSERT INTO Article SET content = 'Graph database analytics'",
            )

        result = db.query(
            "sql",
            "SELECT content, $score FROM Article "
            "WHERE SEARCH_INDEX('Article[content]', 'database') = true "
            "ORDER BY $score DESC",
        )
        first = result.first()
        assert first is not None
        assert first.get("$score") is not None


def test_sqlscript_returns_last_command_result(temp_db_path):
    """SQLScript returns the last command result when no explicit RETURN is used."""
    with arcadedb.create_database(temp_db_path) as db:
        script = """
            CREATE VERTEX TYPE SqlScriptVertex;
            INSERT INTO SqlScriptVertex SET name = 'test';
            ALTER TYPE SqlScriptVertex ALIASES ss;
        """

        with db.transaction():
            result = db.command("sqlscript", script)

        assert result is not None
        last = result.first()
        assert last is not None
        assert last.get("operation").lower() == "alter type"
        assert last.get("typeName") == "SqlScriptVertex"


def test_update_with_json_array_content(temp_db_path):
    """UPDATE ... CONTENT supports JSON arrays for multi-document updates."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE JsonArrayDoc")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO JsonArrayDoc CONTENT
                [{"name":"tim"},{"name":"tom"}]
                """,
            )

        inserted = db.query("sql", "SELECT @rid, name FROM JsonArrayDoc").to_list()
        assert len(inserted) == 2

        updates = []
        for row in inserted:
            rid = row.get("@rid")
            assert rid is not None
            updates.append(
                {
                    "@rid": str(rid),
                    "name": row.get("name"),
                    "status": "updated",
                }
            )

        update_content = ", ".join(
            f"{{@rid:'{row['@rid']}',name:'{row['name']}',status:'updated'}}"
            for row in updates
        )
        with db.transaction():
            update_result = db.command(
                "sql",
                f"UPDATE JsonArrayDoc CONTENT [{update_content}] RETURN AFTER",
            )

        rows = update_result.to_list()
        assert {row["status"] for row in rows} == {"updated"}


def test_truncate_bucket(temp_db_path):
    """TRUNCATE BUCKET removes all records in a bucket."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE BucketDoc BUCKETS 1")
        type_row = db.query(
            "sql", "SELECT buckets FROM schema:types WHERE name = 'BucketDoc'"
        ).first()
        assert type_row is not None
        buckets = type_row.get("buckets")
        assert buckets is not None and len(buckets) > 0
        bucket_name = buckets[0]

        with db.transaction():
            db.command("sql", "INSERT INTO BucketDoc SET name = 'one'")
            db.command("sql", "INSERT INTO BucketDoc SET name = 'two'")

        assert db.count_type("BucketDoc") == 2

        with db.transaction():
            db.command("sql", f"TRUNCATE BUCKET {bucket_name}")

        assert db.count_type("BucketDoc") == 0


def test_transactions(temp_db_path):
    """Test transaction support."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE TransactionTest")

        # Test successful transaction
        with db.transaction():
            db.command("sql", "INSERT INTO TransactionTest SET id = 1")
            db.command("sql", "INSERT INTO TransactionTest SET id = 2")

        # Verify data was committed
        result = db.query("sql", "SELECT count(*) as count FROM TransactionTest")
        count = list(result)[0].get("count")
        assert count == 2

        # Test transaction rollback
        try:
            with db.transaction():
                db.command("sql", "INSERT INTO TransactionTest SET id = 3")
                raise Exception("Intentional error")
        except Exception:
            pass  # Expected

        # Verify rollback worked
        result = db.query("sql", "SELECT count(*) as count FROM TransactionTest")
        count = list(result)[0].get("count")
        assert count == 2  # Should still be 2


def test_graph_operations(temp_db_path):
    """Test graph operations."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create graph schema
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE EDGE TYPE Knows UNIDIRECTIONAL")

        # Create vertices using SQL
        with db.transaction():
            db.command("sql", "INSERT INTO Person SET name = 'Alice'")
            db.command("sql", "INSERT INTO Person SET name = 'Bob'")

        # Create edge using SQL
        with db.transaction():
            db.command(
                "sql",
                "CREATE EDGE Knows "
                "FROM (SELECT FROM Person WHERE name = 'Alice') "
                "TO (SELECT FROM Person WHERE name = 'Bob')",
            )

        # Test graph traversal
        result = db.query(
            "sql",
            """
            SELECT expand(out('Knows').name)
            FROM Person
            WHERE name = 'Alice'
        """,
        )

        names = [record.get("value") for record in result]
        assert "Bob" in names


def test_error_handling():
    """Test error handling."""
    # Test with invalid path
    with pytest.raises(arcadedb.ArcadeDBError):
        arcadedb.open_database("/invalid/path/that/does/not/exist")


def test_result_methods(temp_db_path):
    """Test Result object methods."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE ResultTest")
        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO ResultTest SET
                name = 'test',
                number = 42,
                flag = true,
                nested = {'key': 'value'}
            """,
            )

        result = db.query("sql", "SELECT FROM ResultTest")
        record = list(result)[0]

        # Test property access
        assert record.has_property("name")
        assert record.get("name") == "test"
        assert not record.has_property("nonexistent")

        # Test property names
        prop_names = record.get_property_names()
        assert "name" in prop_names
        assert "number" in prop_names

        # Test to_dict
        data = record.to_dict()
        assert isinstance(data, dict)
        assert data["name"] == "test"
        assert data["number"] == 42

        # Test to_json
        json_str = record.to_json()
        assert isinstance(json_str, str)
        assert "test" in json_str


def test_opencypher_queries(temp_db_path):
    """Test OpenCypher query language support."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create graph schema
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE EDGE TYPE FRIEND_OF UNIDIRECTIONAL")

        # Insert data using OpenCypher (if available)
        try:
            with db.transaction():
                db.command("opencypher", "CREATE (p:Person {name: 'Alice', age: 30})")
                db.command("opencypher", "CREATE (p:Person {name: 'Bob', age: 25})")

            # Query using OpenCypher
            result = db.query(
                "opencypher",
                "MATCH (p:Person) WHERE p.age > 20 RETURN p.name as name",
            )
            names = [record.get("name") for record in result]

            assert len(names) == 2
            assert "Alice" in names
            assert "Bob" in names
        except arcadedb.ArcadeDBError as e:
            if "Query engine 'opencypher' was not found" in str(e):
                pytest.skip("OpenCypher not available (unexpected in base package)")
            raise


def test_unicode_support(temp_db_path):
    """Test Unicode and international character support."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE User")
        with db.transaction():
            # Test various Unicode: Spanish, Chinese, Japanese, Arabic, Emoji
            db.command(
                "sql",
                "INSERT INTO User SET name = 'José García', " "city = 'São Paulo'",
            )
            db.command("sql", "INSERT INTO User SET name = '王小明', city = '北京'")
            db.command(
                "sql", "INSERT INTO User SET name = '田中太郎', " "city = '東京'"
            )
            db.command("sql", "INSERT INTO User SET name = 'محمد', " "city = 'القاهرة'")
            db.command(
                "sql",
                "INSERT INTO User SET name = 'Test 😀', "
                "description = '🎉 Unicode test'",
            )

        # Query with Unicode in WHERE clause
        result = db.query("sql", "SELECT FROM User WHERE name = 'José García'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get("name") == "José García"
        assert records[0].get("city") == "São Paulo"

        # Query Chinese characters
        result = db.query("sql", "SELECT FROM User WHERE city = '北京'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get("name") == "王小明"

        # Query Japanese characters
        result = db.query("sql", "SELECT FROM User WHERE city = '東京'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get("name") == "田中太郎"

        # Query Arabic characters
        result = db.query("sql", "SELECT FROM User WHERE name = 'محمد'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get("city") == "القاهرة"

        # Query with emoji
        result = db.query("sql", "SELECT FROM User WHERE name = 'Test 😀'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get("description") == "🎉 Unicode test"

        # Count all records
        result = db.query("sql", "SELECT count(*) as count FROM User")
        count = list(result)[0].get("count")
        assert count == 5


def test_schema_queries(temp_db_path):
    """Test querying database schema information."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema with various property types
        db.command("sql", "CREATE DOCUMENT TYPE Person")
        db.command("sql", "CREATE PROPERTY Person.name STRING")
        db.command("sql", "CREATE PROPERTY Person.age INTEGER")
        db.command("sql", "CREATE PROPERTY Person.email STRING")
        db.command("sql", "CREATE INDEX ON Person (email) UNIQUE_HASH")

        db.command("sql", "CREATE VERTEX TYPE Company")
        db.command("sql", "CREATE PROPERTY Company.name STRING")

        db.command("sql", "CREATE EDGE TYPE WorksFor UNIDIRECTIONAL")

        # Query schema:types to get type information
        result = db.query("sql", "SELECT FROM schema:types WHERE name = 'Person'")
        records = list(result)
        assert len(records) == 1
        person_type = records[0]
        assert person_type.get("name") == "Person"

        # Query all types
        result = db.query("sql", "SELECT FROM schema:types ORDER BY name")
        types = list(result)
        type_names = [t.get("name") for t in types]
        assert "Person" in type_names
        assert "Company" in type_names
        assert "WorksFor" in type_names

        # Query schema:indexes
        result = db.query("sql", "SELECT FROM schema:indexes")
        indexes = list(result)
        # Should have at least the unique index on Person.email
        assert len(indexes) > 0

        # Query schema:database for database metadata
        result = db.query("sql", "SELECT FROM schema:database")
        records = list(result)
        assert len(records) == 1
        db_info = records[0]
        # Database should have a name property
        assert db_info.has_property("name")


def test_large_result_set_handling(temp_db_path):
    """Test handling large result sets efficiently."""
    with arcadedb.create_database(temp_db_path) as db:
        db.command("sql", "CREATE DOCUMENT TYPE LargeData")
        with db.transaction():
            # Insert 1000 records
            for i in range(1000):
                db.command(
                    "sql",
                    f"INSERT INTO LargeData SET id = {i}, "
                    f"value = {i * 10}, batchNum = {i // 100}",
                )

        # Test iterating over large result set
        result = db.query("sql", "SELECT FROM LargeData ORDER BY id")
        count = 0
        last_id = -1
        for record in result:
            record_id = record.get("id")
            assert record_id > last_id, "Records should be ordered"
            last_id = record_id
            count += 1

        assert count == 1000

        # Test filtered query on large dataset
        result = db.query("sql", "SELECT FROM LargeData WHERE batchNum = 5")
        records = list(result)
        assert len(records) == 100

        # Test aggregation on large dataset
        result = db.query(
            "sql",
            "SELECT batchNum, count(*) as cnt, avg(value) as avg_value "
            "FROM LargeData GROUP BY batchNum ORDER BY batchNum",
        )
        batches = list(result)
        assert len(batches) == 10  # 10 batches (0-9)
        for batch in batches:
            assert batch.get("cnt") == 100


def test_property_type_conversions(temp_db_path):
    """Test that property types are correctly converted between Python/Java."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.command("sql", "CREATE DOCUMENT TYPE TypeTest")

        with db.transaction():
            db.command(
                "sql",
                """
                INSERT INTO TypeTest SET
                str_prop = 'text',
                int_prop = 42,
                long_prop = 9223372036854775807,
                float_prop = 3.14,
                double_prop = 3.14159265359,
                bool_prop = true,
                null_prop = null,
                date_prop = date('2024-01-15', 'yyyy-MM-dd')
            """,
            )

        result = db.query("sql", "SELECT FROM TypeTest")
        record = list(result)[0]

        # Test type conversions
        str_val = record.get("str_prop")
        assert str_val == "text"

        int_val = record.get("int_prop")
        assert int_val == 42
        assert isinstance(int_val, int)

        long_val = record.get("long_prop")
        assert long_val == 9223372036854775807
        assert isinstance(long_val, int)

        float_val = record.get("float_prop")
        assert abs(float_val - 3.14) < 0.01
        assert isinstance(float_val, float)

        double_val = record.get("double_prop")
        assert abs(double_val - 3.14159265359) < 0.0001
        assert isinstance(double_val, float)

        bool_val = record.get("bool_prop")
        assert bool_val is True
        assert isinstance(bool_val, bool)

        null_val = record.get("null_prop")
        assert null_val is None

        # Date should be converted to some Python type
        date_val = record.get("date_prop")
        assert date_val is not None


def test_complex_graph_traversal(temp_db_path):
    """Test complex graph traversal patterns."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create social network graph
        db.command("sql", "CREATE VERTEX TYPE Person")
        db.command("sql", "CREATE EDGE TYPE Follows UNIDIRECTIONAL")
        db.command("sql", "CREATE EDGE TYPE Likes UNIDIRECTIONAL")

        # Create vertices using SQL
        with db.transaction():
            db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")
            db.command("sql", "INSERT INTO Person SET name = 'Bob', age = 25")
            db.command("sql", "INSERT INTO Person SET name = 'Charlie', age = 35")
            db.command("sql", "INSERT INTO Person SET name = 'Diana', age = 28")

        # Create edges using SQL
        with db.transaction():
            db.command(
                "sql",
                "CREATE EDGE Follows "
                "FROM (SELECT FROM Person WHERE name = 'Alice') "
                "TO (SELECT FROM Person WHERE name = 'Bob')",
            )
            db.command(
                "sql",
                "CREATE EDGE Follows "
                "FROM (SELECT FROM Person WHERE name = 'Alice') "
                "TO (SELECT FROM Person WHERE name = 'Charlie')",
            )
            db.command(
                "sql",
                "CREATE EDGE Follows "
                "FROM (SELECT FROM Person WHERE name = 'Bob') "
                "TO (SELECT FROM Person WHERE name = 'Diana')",
            )
            db.command(
                "sql",
                "CREATE EDGE Likes "
                "FROM (SELECT FROM Person WHERE name = 'Charlie') "
                "TO (SELECT FROM Person WHERE name = 'Bob')",
            )

        # Test: Find who Alice follows
        result = db.query(
            "sql",
            "SELECT expand(out('Follows').name) FROM Person " "WHERE name = 'Alice'",
        )
        names = [r.get("value") for r in result]
        assert "Bob" in names
        assert "Charlie" in names

        # Test: Find friends of friends (2-hop traversal)
        result = db.query(
            "sql",
            "SELECT expand(out('Follows').out('Follows').name) "
            "FROM Person WHERE name = 'Alice'",
        )
        names = [r.get("value") for r in result]
        assert "Diana" in names

        # Test: Find who Bob follows
        result = db.query(
            "sql",
            "SELECT expand(out('Follows').name) FROM Person " "WHERE name = 'Bob'",
        )
        names = [r.get("value") for r in result]
        assert "Diana" in names

        # Test: Mixed edge types with direction-aware traversal
        result = db.query(
            "sql",
            "SELECT expand(out('Likes').name) FROM Person " "WHERE name = 'Charlie'",
        )
        names = [r.get("value") for r in result]
        assert "Bob" in names


def test_lookup_by_rid(temp_db_path):
    """Test looking up records by RID."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema
        db.command("sql", "CREATE VERTEX TYPE User")

        # Create a vertex
        with db.transaction():
            user = db.new_vertex("User")
            user.set("name", "John Doe")
            user.save()
            # Get RID as string
            rid = str(user.get_identity())

        # Lookup by RID
        found_user = db.lookup_by_rid(rid)
        assert found_user is not None
        assert found_user.get("name") == "John Doe"
        assert str(found_user.get_identity()) == rid

        # Test lookup with invalid RID format
        with pytest.raises(arcadedb.ArcadeDBError):
            db.lookup_by_rid("invalid_rid")
