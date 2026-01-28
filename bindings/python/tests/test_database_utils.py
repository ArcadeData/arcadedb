"""
Tests for new Database utility methods.
"""

import arcadedb_embedded as arcadedb


def test_count_type(temp_db_path):
    """Test Database.count_type() method."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_document_type("User")
        db.schema.create_document_type("Product")

        with db.transaction():
            # Insert users
            for i in range(10):
                db.command("sql", f"INSERT INTO User SET name = 'User{i}'")

            # Insert products
            for i in range(5):
                db.command("sql", f"INSERT INTO Product SET name = 'Product{i}'")

        # Test count_type for User
        user_count = db.count_type("User")
        assert user_count == 10

        # Test count_type for Product
        product_count = db.count_type("Product")
        assert product_count == 5

        # Test count_type for non-existent type
        empty_count = db.count_type("NonExistent")
        assert empty_count == 0


def test_is_transaction_active(temp_db_path):
    """Test Database.is_transaction_active() method."""
    with arcadedb.create_database(temp_db_path) as db:
        # Initially no transaction
        assert not db.is_transaction_active()

        # Inside transaction context
        with db.transaction():
            assert db.is_transaction_active()

        # After transaction
        assert not db.is_transaction_active()


def test_drop_database(temp_db_path):
    """Test Database.drop() method."""
    db = arcadedb.create_database(temp_db_path)

    # Create some data
    # Schema operations are auto-transactional
    db.schema.create_document_type("Test")
    with db.transaction():
        db.command("sql", "INSERT INTO Test SET value = 1")

    # Verify data exists (read-only, no transaction needed)
    result = db.query("sql", "SELECT count(*) as count FROM Test")
    count = result.first().get("count")
    assert count == 1

    # Drop the database
    db.drop()

    # Database should be closed after drop
    assert not db.is_open()


def test_database_methods_integration(temp_db_path):
    """Test using multiple database methods together."""
    with arcadedb.create_database(temp_db_path) as db:
        # Schema operations are auto-transactional
        db.schema.create_vertex_type("Person")
        db.schema.create_edge_type("Knows")

        # Insert data
        with db.transaction():
            alice = db.new_vertex("Person")
            alice.set("name", "Alice")
            alice.save()

            bob = db.new_vertex("Person")
            bob.set("name", "Bob")
            bob.save()

        # Use count_type to verify
        person_count = db.count_type("Person")
        assert person_count == 2

        # Use query with new ResultSet methods
        result = db.query("sql", "SELECT FROM Person ORDER BY name")

        # Test first()
        first_person = result.first()
        assert first_person.get("name") == "Alice"

        # New query for to_list()
        result2 = db.query("sql", "SELECT FROM Person ORDER BY name")
        people_list = result2.to_list()
        assert len(people_list) == 2
        assert people_list[1]["name"] == "Bob"


def test_error_handling_new_methods(temp_db_path):
    """Test error handling for new database methods."""
    db = arcadedb.create_database(temp_db_path)
    db.close()

    # Calling methods on closed database should raise error
    try:
        db.count_type("Test")
        assert False, "Should have raised error on closed database"
    except arcadedb.ArcadeDBError:
        pass  # Expected

    try:
        db.is_transaction_active()
        assert False, "Should have raised error on closed database"
    except arcadedb.ArcadeDBError:
        pass  # Expected
