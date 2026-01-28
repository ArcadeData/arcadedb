"""
Tests for enhanced ResultSet and Result functionality.
"""

import arcadedb_embedded as arcadedb


def test_resultset_to_list(temp_db_path):
    """Test ResultSet.to_list() method."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("User")

        with db.transaction():
            db.command("sql", "INSERT INTO User SET name = 'Alice', age = 30")
            db.command("sql", "INSERT INTO User SET name = 'Bob', age = 25")
            db.command("sql", "INSERT INTO User SET name = 'Charlie', age = 35")

        result = db.query("sql", "SELECT FROM User ORDER BY name")

        # Test to_list with type conversion
        users_list = result.to_list(convert_types=True)
        assert isinstance(users_list, list)
        assert len(users_list) == 3

        # Verify it's a list of dicts
        assert all(isinstance(item, dict) for item in users_list)

        # Check first user
        assert users_list[0]["name"] == "Alice"
        assert users_list[0]["age"] == 30

        # Check last user
        assert users_list[2]["name"] == "Charlie"
        assert users_list[2]["age"] == 35


def test_resultset_to_dataframe(temp_db_path):
    """Test ResultSet.to_dataframe() method."""
    try:
        import pandas as pd
    except ImportError:
        # Skip test if pandas not installed
        return

    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Product")

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO Product SET name = 'Widget', price = 9.99, stock = 100",
            )
            db.command(
                "sql",
                "INSERT INTO Product SET name = 'Gadget', price = 19.99, stock = 50",
            )
            db.command(
                "sql",
                "INSERT INTO Product SET name = 'Doohickey', price = 14.99, stock = 75",
            )

        result = db.query("sql", "SELECT FROM Product ORDER BY name")

        # Test to_dataframe
        df = result.to_dataframe(convert_types=True)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

        # Check column names
        assert "name" in df.columns
        assert "price" in df.columns
        assert "stock" in df.columns

        # Check values
        assert df.iloc[1]["name"] == "Gadget"
        assert df.iloc[1]["stock"] == 50

        # Test DataFrame operations
        total_stock = df["stock"].sum()
        assert total_stock == 225


def test_resultset_iter_chunks(temp_db_path):
    """Test ResultSet.iter_chunks() for memory-efficient iteration."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Item")

        with db.transaction():
            # Insert 250 items
            for i in range(250):
                db.command(
                    "sql",
                    f"INSERT INTO `Item` SET id = {i}, value = {i * 10}, "
                    f"batchNum = {i // 100}",
                )

        result = db.query("sql", "SELECT FROM `Item` ORDER BY id")

        # Test chunked iteration with chunk size 100
        chunks = list(result.iter_chunks(size=100))

        # Should have 3 chunks (100, 100, 50)
        assert len(chunks) == 3
        assert len(chunks[0]) == 100
        assert len(chunks[1]) == 100
        assert len(chunks[2]) == 50

        # Verify chunk data
        first_chunk = chunks[0]
        assert first_chunk[0]["id"] == 0
        assert first_chunk[99]["id"] == 99

        last_chunk = chunks[2]
        assert last_chunk[0]["id"] == 200
        assert last_chunk[49]["id"] == 249


def test_resultset_count(temp_db_path):
    """Test ResultSet.count() method."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Counter")

        with db.transaction():
            for i in range(50):
                db.command("sql", f"INSERT INTO Counter SET num = {i}")

        result = db.query("sql", "SELECT FROM Counter")

        # Test count without loading all results
        count = result.count()
        assert count == 50


def test_resultset_first(temp_db_path):
    """Test ResultSet.first() method."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("FirstTest")

        with db.transaction():
            db.command("sql", "INSERT INTO FirstTest SET value = 'first'")
            db.command("sql", "INSERT INTO FirstTest SET value = 'second'")
            db.command("sql", "INSERT INTO FirstTest SET value = 'third'")

        # Test first() returns first result
        result = db.query("sql", "SELECT FROM FirstTest ORDER BY value")
        first_record = result.first()

        assert first_record is not None
        assert first_record.get("value") == "first"

        # Test first() returns None for empty results
        result_empty = db.query(
            "sql", "SELECT FROM FirstTest WHERE value = 'nonexistent'"
        )
        first_empty = result_empty.first()
        assert first_empty is None


def test_resultset_one(temp_db_path):
    """Test ResultSet.one() method."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("OneTest")

        with db.transaction():
            db.command("sql", "INSERT INTO OneTest SET id = 1, value = 'unique'")
            db.command("sql", "INSERT INTO OneTest SET id = 2, value = 'multiple'")
            db.command("sql", "INSERT INTO OneTest SET id = 3, value = 'multiple'")

        # Test one() returns single result
        result = db.query("sql", "SELECT FROM OneTest WHERE value = 'unique'")
        record = result.one()
        assert record is not None
        assert record.get("value") == "unique"

        # Test one() raises error for empty results
        try:
            result_empty = db.query(
                "sql", "SELECT FROM OneTest WHERE value = 'nonexistent'"
            )
            result_empty.one()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "no results" in str(e).lower()

        # Test one() raises error for multiple results
        try:
            result_multi = db.query(
                "sql", "SELECT FROM OneTest WHERE value = 'multiple'"
            )
            result_multi.one()
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "multiple" in str(e).lower() or "more than one" in str(e).lower()


def test_resultset_iteration_patterns(temp_db_path):
    """Test various iteration patterns with ResultSet."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("IterTest")

        with db.transaction():
            for i in range(10):
                db.command("sql", f"INSERT INTO IterTest SET num = {i}")

        # Test traditional iteration
        result = db.query("sql", "SELECT FROM IterTest ORDER BY num")
        nums_iter = [r.get("num") for r in result]
        assert len(nums_iter) == 10
        assert nums_iter[0] == 0
        assert nums_iter[9] == 9

        # Test list conversion for traditional operations
        result2 = db.query("sql", "SELECT FROM IterTest ORDER BY num")
        results_list = list(result2)
        assert len(results_list) == 10

        # Test first on iterated result
        result3 = db.query("sql", "SELECT FROM IterTest ORDER BY num DESC")
        first = result3.first()
        assert first.get("num") == 9  # Descending order


def test_result_representation(temp_db_path):
    """Test Result.__repr__() for better debugging."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("ReprTest")

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO ReprTest SET name = 'test', value = 42, active = true",
            )

        result = db.query("sql", "SELECT FROM ReprTest")
        record = result.first()

        # Test __repr__
        repr_str = repr(record)
        assert isinstance(repr_str, str)
        assert "Result" in repr_str
        # Should show some properties
        assert "name" in repr_str or "test" in repr_str or "value" in repr_str


def test_resultset_with_complex_queries(temp_db_path):
    """Test ResultSet methods with complex queries."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("Sales")
        db.schema.create_property("Sales", "amount", "DECIMAL")
        db.schema.create_property("Sales", "region", "STRING")

        with db.transaction():
            # Insert sample data
            regions = ["North", "South", "East", "West"]
            for i in range(100):
                region = regions[i % 4]
                amount = 100.0 + (i * 5.5)
                db.command(
                    "sql",
                    f"INSERT INTO Sales SET region = '{region}', amount = {amount}",
                )

        # Test aggregation query with to_list
        result = db.query(
            "sql",
            """
            SELECT region, count(*) as count, sum(amount) as total
            FROM Sales
            GROUP BY region
            ORDER BY region
        """,
        )

        agg_list = result.to_list()
        assert len(agg_list) == 4  # 4 regions

        # Each group should have 25 records
        for item in agg_list:
            assert item["count"] == 25

        # Test filtering with first()
        result2 = db.query(
            "sql",
            "SELECT FROM Sales WHERE region = 'North' ORDER BY amount DESC",
        )
        highest_north = result2.first()
        assert highest_north is not None
        assert highest_north.get("region") == "North"


def test_resultset_empty_handling(temp_db_path):
    """Test ResultSet methods with empty results."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("EmptyTest")

        # Query empty table
        result = db.query("sql", "SELECT FROM EmptyTest")

        # Test to_list on empty result
        empty_list = result.to_list()
        assert empty_list == []

        # Test count on empty result
        result2 = db.query("sql", "SELECT FROM EmptyTest")
        count = result2.count()
        assert count == 0

        # Test first on empty result
        result3 = db.query("sql", "SELECT FROM EmptyTest")
        first = result3.first()
        assert first is None

        # Test iter_chunks on empty result
        result4 = db.query("sql", "SELECT FROM EmptyTest")
        chunks = list(result4.iter_chunks(size=10))
        assert len(chunks) == 0


def test_resultset_reusability(temp_db_path):
    """Test that ResultSet can only be iterated once (Java ResultSet behavior)."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("ReuseTest")

        with db.transaction():
            db.command("sql", "INSERT INTO ReuseTest SET value = 1")
            db.command("sql", "INSERT INTO ReuseTest SET value = 2")

        result = db.query("sql", "SELECT FROM ReuseTest")

        # First iteration works
        first_list = list(result)
        assert len(first_list) == 2

        # Second iteration should be empty (ResultSet is consumed)
        second_list = list(result)
        assert len(second_list) == 0

        # Need new query for fresh ResultSet
        result2 = db.query("sql", "SELECT FROM ReuseTest")
        fresh_list = list(result2)
        assert len(fresh_list) == 2


def test_result_get_rid_and_vertex(temp_db_path):
    """Test get_rid() and get_vertex() methods on Result."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Person")

        with db.transaction():
            db.command("sql", "INSERT INTO Person SET name = 'Alice'")

        result = db.query("sql", "SELECT FROM Person").first()

        # Test get_rid()
        rid = result.get_rid()
        assert rid is not None
        assert isinstance(rid, str)
        assert rid.startswith("#")

        # Test get_vertex()
        vertex = result.get_vertex()
        assert vertex is not None
        # It should be a Java object
        assert "Vertex" in str(vertex) or "Vertex" in vertex.getClass().getName()

        # Verify we can use the vertex object
        assert vertex.get("name") == "Alice"


def test_result_to_json_with_arrays(temp_db_path):
    """Result.to_json() should serialize list properties as JSON arrays."""
    with arcadedb.create_database(temp_db_path) as db:
        db.schema.create_document_type("JsonArrayTest")

        with db.transaction():
            db.command(
                "sql",
                "INSERT INTO JsonArrayTest SET tags = ['a', 'b', 'c']",
            )

        result = db.query("sql", "SELECT FROM JsonArrayTest").first()
        json_str = result.to_json()

        assert '"tags"' in json_str
        assert '["a","b","c"]' in json_str or '["a", "b", "c"]' in json_str
