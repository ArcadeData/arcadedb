"""
Core functionality tests for ArcadeDB Python bindings.
These tests work with ALL distributions (headless, minimal, full).
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

        # Create vertices
        with db.transaction():
            db.command("sql", "CREATE VERTEX Person SET name = 'Alice'")
            db.command("sql", "CREATE VERTEX Person SET name = 'Bob'")

        # Create edge
        with db.transaction():
            db.command(
                "sql",
                """
                CREATE EDGE Knows
                FROM (SELECT FROM Person WHERE name = 'Alice')
                TO (SELECT FROM Person WHERE name = 'Bob')
            """,
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
                pytest.skip("Cypher query engine not available in this distribution")
            raise


def test_vector_search(temp_db_path):
    """Test vector embeddings with HNSW similarity search.

    This test creates an HNSW vector index using the simplified Python API
    to enable nearest-neighbor similarity search on vector embeddings.
    Works with NumPy arrays (preferred) or plain Python lists.
    """
    # Try to use NumPy if available
    try:
        import numpy as np

        use_numpy = True
    except ImportError:
        use_numpy = False

    with arcadedb.create_database(temp_db_path) as db:
        # Create vertex type for vector embeddings
        with db.transaction():
            db.command("sql", "CREATE VERTEX TYPE EmbeddingNode")
            db.command("sql", "CREATE PROPERTY EmbeddingNode.name STRING")
            # Use ARRAY_OF_FLOATS for vector property (required for HNSW)
            db.command("sql", "CREATE PROPERTY EmbeddingNode.vector ARRAY_OF_FLOATS")
            # Create unique index on name for lookups
            db.command("sql", "CREATE INDEX ON EmbeddingNode (name) UNIQUE")

        # Insert sample word embeddings (4-dimensional for simplicity)
        # In reality, embeddings would be 300-1536 dimensions
        if use_numpy:
            embeddings = [
                ("king", np.array([0.5, 0.3, 0.1, 0.2])),
                ("queen", np.array([0.52, 0.32, 0.08, 0.18])),  # Similar to king
                ("man", np.array([0.48, 0.25, 0.15, 0.22])),
                ("woman", np.array([0.50, 0.28, 0.12, 0.20])),
                ("cat", np.array([0.1, 0.8, 0.6, 0.3])),  # Different cluster
                ("dog", np.array([0.12, 0.82, 0.58, 0.32])),  # Similar to cat
            ]
        else:
            embeddings = [
                ("king", [0.5, 0.3, 0.1, 0.2]),
                ("queen", [0.52, 0.32, 0.08, 0.18]),  # Similar to king
                ("man", [0.48, 0.25, 0.15, 0.22]),
                ("woman", [0.50, 0.28, 0.12, 0.20]),
                ("cat", [0.1, 0.8, 0.6, 0.3]),  # Different cluster
                ("dog", [0.12, 0.82, 0.58, 0.32]),  # Similar to cat
            ]

        with db.transaction():
            for name, vector in embeddings:
                # Use the helper function to convert to Java array
                java_vector = arcadedb.to_java_float_array(vector)

                # Create vertex with Java array as vector property
                java_db = db._java_db
                vertex = java_db.newVertex("EmbeddingNode")
                vertex.set("name", name)
                vertex.set("vector", java_vector)
                vertex.save()

        # Test 1: Verify we can query and retrieve stored vectors
        result = db.query("sql", "SELECT FROM EmbeddingNode WHERE name = 'king'")
        records = list(result)
        assert len(records) == 1

        king_node = records[0]
        assert king_node.has_property("vector")
        assert king_node.has_property("name")
        assert king_node.get_property("name") == "king"

        vector = king_node.get_property("vector")
        # Convert to Python/NumPy array
        vector = arcadedb.to_python_array(vector, use_numpy=use_numpy)

        if use_numpy:
            assert isinstance(vector, np.ndarray)
            assert vector.shape == (4,)
        else:
            assert isinstance(vector, list)
            assert len(vector) == 4

        assert abs(vector[0] - 0.5) < 0.01  # Verify first component

        # Test 2: Create HNSW vector index using simplified API
        print("\nCreating HNSW vector index...")

        # Create index with simplified API
        with db.transaction():
            index = db.create_vector_index(
                vertex_type="EmbeddingNode",
                vector_property="vector",
                dimensions=4,
                id_property="name",  # Use name as ID
                distance_function="cosine",
                m=16,
                ef=128,
                max_items=1000,
            )

        print("  ✓ Created vector index")

        # Index existing vertices
        print("  Indexing existing vertices...")
        result = db.query("sql", "SELECT FROM EmbeddingNode")
        indexed_count = 0

        with db.transaction():
            for record in result:
                vertex = record._java_result.getElement().get().asVertex()
                index.add_vertex(vertex)
                indexed_count += 1

        print(f"  ✓ Indexed {indexed_count} vertices")

        # Test 3: Perform similarity search with NumPy/list arrays
        print("\nTesting nearest-neighbor similarity search...")

        # Search for neighbors of "king" - should find "queen", "man", "woman"
        if use_numpy:
            king_vector = np.array([0.5, 0.3, 0.1, 0.2])
        else:
            king_vector = [0.5, 0.3, 0.1, 0.2]

        neighbors = index.find_nearest(king_vector, k=3)

        # Extract names from results
        neighbor_names = [str(vertex.get("name")) for vertex, distance in neighbors]

        print(f"  3 nearest neighbors to 'king': {neighbor_names}")
        assert "queen" in neighbor_names, "Expected 'queen' to be near 'king'"
        assert "man" in neighbor_names or "woman" in neighbor_names
        assert "cat" not in neighbor_names, "'cat' should be in different cluster"
        assert "dog" not in neighbor_names, "'dog' should be in different cluster"
        print("  ✓ Similarity search returns correct neighbors!")

        # Search for neighbors of "cat" - should find "dog"
        if use_numpy:
            cat_vector = np.array([0.1, 0.8, 0.6, 0.3])
        else:
            cat_vector = [0.1, 0.8, 0.6, 0.3]

        neighbors = index.find_nearest(cat_vector, k=2)
        neighbor_names = [str(vertex.get("name")) for vertex, distance in neighbors]

        print(f"  2 nearest neighbors to 'cat': {neighbor_names}")
        assert "dog" in neighbor_names, "Expected 'dog' to be near 'cat'"
        assert "king" not in neighbor_names, "'king' should be in different cluster"
        print("  ✓ Different cluster correctly separated!")

        print("\n✓ HNSW vector index fully functional with NumPy support!")


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

            # Create vertices
            db.command("sql", "CREATE VERTEX Person SET name = 'Alice', age = 30")
            db.command("sql", "CREATE VERTEX Person SET name = 'Bob', age = 25")
            db.command("sql", "CREATE VERTEX Person SET name = 'Charlie', age = 35")
            db.command("sql", "CREATE VERTEX Person SET name = 'Diana', age = 28")

            # Alice follows Bob and Charlie
            db.command(
                "sql",
                """
                CREATE EDGE Follows FROM (SELECT FROM Person WHERE name = 'Alice')
                TO (SELECT FROM Person WHERE name = 'Bob')
            """,
            )
            db.command(
                "sql",
                """
                CREATE EDGE Follows FROM (SELECT FROM Person WHERE name = 'Alice')
                TO (SELECT FROM Person WHERE name = 'Charlie')
            """,
            )

            # Bob follows Diana
            db.command(
                "sql",
                """
                CREATE EDGE Follows FROM (SELECT FROM Person WHERE name = 'Bob')
                TO (SELECT FROM Person WHERE name = 'Diana')
            """,
            )

            # Charlie likes Bob
            db.command(
                "sql",
                """
                CREATE EDGE Likes FROM (SELECT FROM Person WHERE name = 'Charlie')
                TO (SELECT FROM Person WHERE name = 'Bob')
            """,
            )

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
