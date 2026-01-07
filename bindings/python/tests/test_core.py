"""
Core functionality tests for ArcadeDB Python bindings.
These tests work with our base package (includes SQL, Cypher, Gremlin, Studio).
"""

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
        # Create a document type
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE TestDoc")

        # Insert data
        with db.transaction():
            db.command("sql", "INSERT INTO TestDoc SET name = 'test', value = 42")

        # Query data
        result = db.query("sql", "SELECT FROM TestDoc WHERE name = 'test'")
        records = list(result)

        assert len(records) == 1
        record = records[0]
        assert record.get_property("name") == "test"
        assert record.get_property("value") == 42


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
        # Create document type with rich data types
        with db.transaction():
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
        assert record.get_property("title") == "Setup Development Environment"
        assert record.get_property("priority") == "high"
        assert record.get_property("completed") is False
        assert record.get_property("estimated_hours") == 4.5
        assert record.get_property("priority_score") == 8
        assert record.get_property("cost") is not None  # DECIMAL type
        assert record.get_property("task_id") is not None  # UUID as string
        assert record.get_property("created_date") is not None  # DATE type
        assert record.get_property("due_datetime") is not None  # DATETIME type

        # Test aggregation queries
        result = db.query("sql", "SELECT count(*) as total FROM Task")
        total = list(result)[0].get_property("total")
        assert total == 2

        # Test filtering by boolean
        result = db.query("sql", "SELECT FROM Task WHERE completed = true")
        completed_tasks = list(result)
        assert len(completed_tasks) == 1
        assert completed_tasks[0].get_property("title") == "Write Documentation"

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
        completed_count = list(result)[0].get_property("completed_count")
        assert completed_count == 2

        # Test DELETE operations
        with db.transaction():
            db.command("sql", "DELETE FROM Task WHERE completed = true")

        # Verify deletion
        result = db.query("sql", "SELECT count(*) as remaining FROM Task")
        remaining = list(result)[0].get_property("remaining")
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
        with db.transaction():
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
        assert record.get_property("id") is not None  # UUID function worked
        assert record.get_property("created_at") is not None  # sysDate() worked
        assert record.get_property("custom_date") is not None  # date() worked
        assert record.get_property("custom_datetime") is not None  # datetime() worked

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

        assert doc_record.get_property("name") == "Test Document"
        metadata = doc_record.get_property("metadata")
        assert metadata is not None
        # Metadata is a Java LinkedHashMap, not a Python dict
        assert hasattr(metadata, "get")  # Check it's a map-like object


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
        count = list(result)[0].get_property("count")
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
        count = list(result)[0].get_property("count")
        assert count == 2  # Should still be 2


def test_graph_operations(temp_db_path):
    """Test graph operations."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create graph schema
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE Knows")

        # Create vertices using Java API
        with db.transaction():
            alice = db.new_vertex("Person")
            alice.set("name", "Alice")
            alice.save()

            bob = db.new_vertex("Person")
            bob.set("name", "Bob")
            bob.save()

        # Create edge using Java API
        with db.transaction():
            # Query vertices to get Java objects
            alice_result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
            bob_result = db.query("sql", "SELECT FROM Person WHERE name = 'Bob'")

            alice_wrapper = list(alice_result)[0]
            bob_wrapper = list(bob_result)[0]

            # Extract Java vertices
            alice_vertex = alice_wrapper._java_result.getElement().get().asVertex()
            bob_vertex = bob_wrapper._java_result.getElement().get().asVertex()

            # Create edge using vertex.newEdge()
            edge = alice_vertex.newEdge("Knows", bob_vertex)
            edge.save()

        # Test graph traversal
        result = db.query(
            "sql",
            """
            SELECT expand(out('Knows').name)
            FROM Person
            WHERE name = 'Alice'
        """,
        )

        names = [record.get_property("value") for record in result]
        assert "Bob" in names


def test_error_handling():
    """Test error handling."""
    # Test with invalid path
    with pytest.raises(arcadedb.ArcadeDBError):
        arcadedb.open_database("/invalid/path/that/does/not/exist")


def test_result_methods(temp_db_path):
    """Test Result object methods."""
    with arcadedb.create_database(temp_db_path) as db:
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE ResultTest")
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
        assert record.get_property("name") == "test"
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


def test_cypher_queries(temp_db_path):
    """Test Cypher query language support."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create graph schema
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE FRIEND_OF")

        # Insert data using Cypher (if available)
        try:
            with db.transaction():
                db.command("cypher", "CREATE (p:Person {name: 'Alice', age: 30})")
                db.command("cypher", "CREATE (p:Person {name: 'Bob', age: 25})")

            # Query using Cypher
            result = db.query(
                "cypher", "MATCH (p:Person) WHERE p.age > 20 RETURN p.name as name"
            )
            names = [record.get_property("name") for record in result]

            assert len(names) == 2
            assert "Alice" in names
            assert "Bob" in names
        except arcadedb.ArcadeDBError as e:
            if "Query engine 'cypher' was not found" in str(e):
                pytest.skip("Cypher not available (unexpected in base package)")
            raise


def test_unicode_support(temp_db_path):
    """Test Unicode and international character support."""
    with arcadedb.create_database(temp_db_path) as db:
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE User")
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
        assert records[0].get_property("name") == "José García"
        assert records[0].get_property("city") == "São Paulo"

        # Query Chinese characters
        result = db.query("sql", "SELECT FROM User WHERE city = '北京'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get_property("name") == "王小明"

        # Query Japanese characters
        result = db.query("sql", "SELECT FROM User WHERE city = '東京'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get_property("name") == "田中太郎"

        # Query Arabic characters
        result = db.query("sql", "SELECT FROM User WHERE name = 'محمد'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get_property("city") == "القاهرة"

        # Query with emoji
        result = db.query("sql", "SELECT FROM User WHERE name = 'Test 😀'")
        records = list(result)
        assert len(records) == 1
        assert records[0].get_property("description") == "🎉 Unicode test"

        # Count all records
        result = db.query("sql", "SELECT count(*) as count FROM User")
        count = list(result)[0].get_property("count")
        assert count == 5


def test_schema_queries(temp_db_path):
    """Test querying database schema information."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create schema with various property types
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE Person")
            db.command("sql", "CREATE PROPERTY Person.name STRING")
            db.command("sql", "CREATE PROPERTY Person.age INTEGER")
            db.command("sql", "CREATE PROPERTY Person.email STRING")
            db.command("sql", "CREATE INDEX ON Person (email) UNIQUE")

            db.command("sql", "CREATE VERTEX TYPE Company")
            db.command("sql", "CREATE PROPERTY Company.name STRING")

            db.command("sql", "CREATE EDGE TYPE WorksFor")

        # Query schema:types to get type information
        result = db.query("sql", "SELECT FROM schema:types WHERE name = 'Person'")
        records = list(result)
        assert len(records) == 1
        person_type = records[0]
        assert person_type.get_property("name") == "Person"

        # Query all types
        result = db.query("sql", "SELECT FROM schema:types ORDER BY name")
        types = list(result)
        type_names = [t.get_property("name") for t in types]
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
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE LargeData")
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
            record_id = record.get_property("id")
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
            assert batch.get_property("cnt") == 100


def test_property_type_conversions(temp_db_path):
    """Test that property types are correctly converted between Python/Java."""
    with arcadedb.create_database(temp_db_path) as db:
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE TypeTest")
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
        str_val = record.get_property("str_prop")
        assert str_val == "text"

        int_val = record.get_property("int_prop")
        assert int_val == 42
        assert isinstance(int_val, int)

        long_val = record.get_property("long_prop")
        assert long_val == 9223372036854775807
        assert isinstance(long_val, int)

        float_val = record.get_property("float_prop")
        assert abs(float_val - 3.14) < 0.01
        assert isinstance(float_val, float)

        double_val = record.get_property("double_prop")
        assert abs(double_val - 3.14159265359) < 0.0001
        assert isinstance(double_val, float)

        bool_val = record.get_property("bool_prop")
        assert bool_val is True
        assert isinstance(bool_val, bool)

        null_val = record.get_property("null_prop")
        assert null_val is None

        # Date should be converted to some Python type
        date_val = record.get_property("date_prop")
        assert date_val is not None


def test_complex_graph_traversal(temp_db_path):
    """Test complex graph traversal patterns."""
    with arcadedb.create_database(temp_db_path) as db:
        # Create social network graph
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE Person")
            db.command("sql", "CREATE EDGE TYPE Follows")
            db.command("sql", "CREATE EDGE TYPE Likes")

        # Create vertices using Java API
        with db.transaction():
            alice = db.new_vertex("Person")
            alice.set("name", "Alice")
            alice.set("age", 30)
            alice.save()

            bob = db.new_vertex("Person")
            bob.set("name", "Bob")
            bob.set("age", 25)
            bob.save()

            charlie = db.new_vertex("Person")
            charlie.set("name", "Charlie")
            charlie.set("age", 35)
            charlie.save()

            diana = db.new_vertex("Person")
            diana.set("name", "Diana")
            diana.set("age", 28)
            diana.save()

        # Create edges using Java API
        with db.transaction():
            # Query to get all vertices
            query_result = db.query("sql", "SELECT FROM Person")
            person_cache = {}
            for wrapper in query_result:
                java_vertex = wrapper._java_result.getElement().get().asVertex()
                name = wrapper.get_property("name")
                person_cache[name] = java_vertex

            # Alice follows Bob and Charlie
            edge1 = person_cache["Alice"].newEdge("Follows", person_cache["Bob"])
            edge1.save()

            edge2 = person_cache["Alice"].newEdge("Follows", person_cache["Charlie"])
            edge2.save()

            # Bob follows Diana
            edge3 = person_cache["Bob"].newEdge("Follows", person_cache["Diana"])
            edge3.save()

            # Charlie likes Bob
            edge4 = person_cache["Charlie"].newEdge("Likes", person_cache["Bob"])
            edge4.save()

        # Test: Find who Alice follows
        result = db.query(
            "sql",
            "SELECT expand(out('Follows').name) FROM Person " "WHERE name = 'Alice'",
        )
        names = [r.get_property("value") for r in result]
        assert "Bob" in names
        assert "Charlie" in names

        # Test: Find friends of friends (2-hop traversal)
        result = db.query(
            "sql",
            "SELECT expand(out('Follows').out('Follows').name) "
            "FROM Person WHERE name = 'Alice'",
        )
        names = [r.get_property("value") for r in result]
        assert "Diana" in names

        # Test: Find who follows Bob
        result = db.query(
            "sql", "SELECT expand(in('Follows').name) FROM Person " "WHERE name = 'Bob'"
        )
        names = [r.get_property("value") for r in result]
        assert "Alice" in names

        # Test: Mixed edge types
        result = db.query(
            "sql", "SELECT expand(in('Likes').name) FROM Person " "WHERE name = 'Bob'"
        )
        names = [r.get_property("value") for r in result]
        assert "Charlie" in names
