"""
Tests for transaction configuration methods.

Priority 7: Transaction Configuration
"""

import pytest
from arcadedb_embedded import ArcadeDBError, create_database


def test_set_wal_flush_modes(temp_db):
    """Test all WAL flush modes."""
    # Test 'no' mode (default/maximum performance)
    temp_db.set_wal_flush("no")

    # Test 'yes_nometadata' mode
    temp_db.set_wal_flush("yes_nometadata")

    # Test 'yes_full' mode (maximum durability)
    temp_db.set_wal_flush("yes_full")

    # Reset to default
    temp_db.set_wal_flush("no")


def test_set_wal_flush_invalid_mode(temp_db):
    """Test that invalid WAL flush mode raises ValueError."""
    with pytest.raises(ValueError, match="Invalid WAL flush mode"):
        temp_db.set_wal_flush("invalid_mode")

    with pytest.raises(ValueError, match="Invalid WAL flush mode"):
        temp_db.set_wal_flush("YES_FULL")  # Must be lowercase


def test_set_read_your_writes(temp_db):
    """Test read-your-writes configuration."""
    # Default is True
    temp_db.set_read_your_writes(True)

    # Disable for better concurrency
    temp_db.set_read_your_writes(False)

    # Re-enable
    temp_db.set_read_your_writes(True)


def test_set_auto_transaction(temp_db):
    """Test auto-transaction configuration."""
    # Default is True
    temp_db.set_auto_transaction(True)

    # Disable for manual control
    temp_db.set_auto_transaction(False)

    # Re-enable
    temp_db.set_auto_transaction(True)


def test_transaction_config_with_operations(temp_db):
    """Test transaction config methods work alongside normal operations."""
    # Schema operations are auto-transactional
    temp_db.schema.create_vertex_type("ConfigTest")

    # Configure for maximum durability
    temp_db.set_wal_flush("yes_full")
    temp_db.set_read_your_writes(True)

    # Do some operations
    with temp_db.transaction():
        temp_db.command("sql", "CREATE VERTEX ConfigTest SET name = 'test1'")
        temp_db.command("sql", "CREATE VERTEX ConfigTest SET name = 'test2'")

    # Verify data was written
    result = temp_db.query("sql", "SELECT count(*) as cnt FROM ConfigTest")
    count = next(result).get("cnt")
    assert count == 2

    # Switch to performance mode
    temp_db.set_wal_flush("no")
    temp_db.set_read_your_writes(False)

    # More operations (still need transaction context)
    with temp_db.transaction():
        temp_db.command("sql", "CREATE VERTEX ConfigTest SET name = 'test3'")

    # Verify
    result = temp_db.query("sql", "SELECT count(*) as cnt FROM ConfigTest")
    count = next(result).get("cnt")
    assert count == 3


def test_manual_transaction_mode(temp_db):
    """Test manual transaction control with auto-transaction disabled."""
    # Schema operations are auto-transactional
    temp_db.schema.create_vertex_type("ManualTest")

    # Disable auto-transaction
    temp_db.set_auto_transaction(False)

    try:
        # Must use transaction context manager
        with temp_db.transaction():
            temp_db.command("sql", "CREATE VERTEX ManualTest SET name = 'manual1'")
            temp_db.command("sql", "CREATE VERTEX ManualTest SET name = 'manual2'")

        # Verify data was written
        with temp_db.transaction():
            result = temp_db.query("sql", "SELECT count(*) as cnt FROM ManualTest")
            count = next(result).get("cnt")

        assert count == 2

    finally:
        # Always re-enable auto-transaction
        temp_db.set_auto_transaction(True)


def test_wal_flush_with_bulk_operations(temp_db):
    """Test WAL flush modes with bulk inserts using chunked transactions."""
    # Schema operations are auto-transactional
    temp_db.schema.create_vertex_type("BatchTest")

    # Test with maximum durability
    temp_db.set_wal_flush("yes_full")

    chunk_size = 200
    total = 500
    for start in range(0, total, chunk_size):
        with temp_db.transaction():
            for i in range(start, min(start + chunk_size, total)):
                vertex = temp_db.new_vertex("BatchTest")
                vertex.set("value", i)
                vertex.save()

    # Verify all records were written
    result = temp_db.query("sql", "SELECT count(*) as cnt FROM BatchTest")
    count = next(result).get("cnt")
    assert count == 500

    # Test with maximum performance
    temp_db.set_wal_flush("no")

    for start in range(total, 1000, chunk_size):
        with temp_db.transaction():
            for i in range(start, min(start + chunk_size, 1000)):
                vertex = temp_db.new_vertex("BatchTest")
                vertex.set("value", i)
                vertex.save()

    # Verify all records were written
    result = temp_db.query("sql", "SELECT count(*) as cnt FROM BatchTest")
    count = next(result).get("cnt")
    assert count == 1000


def test_config_methods_on_closed_database(temp_db_path):
    """Test that config methods raise error on closed database."""
    db = create_database(temp_db_path)
    db.close()

    with pytest.raises(ArcadeDBError, match="closed"):
        db.set_wal_flush("no")

    with pytest.raises(ArcadeDBError, match="closed"):
        db.set_read_your_writes(True)

    with pytest.raises(ArcadeDBError, match="closed"):
        db.set_auto_transaction(True)


def test_combined_config_changes(temp_db):
    """Test changing multiple config settings together."""
    # Start with maximum durability settings
    temp_db.set_wal_flush("yes_full")
    temp_db.set_read_your_writes(True)
    temp_db.set_auto_transaction(True)

    # Schema operations are auto-transactional
    temp_db.schema.create_vertex_type("Combined")

    # Create some data (requires transaction)
    with temp_db.transaction():
        temp_db.command("sql", "CREATE VERTEX Combined SET value = 1")

    # Switch to performance settings
    temp_db.set_wal_flush("no")
    temp_db.set_read_your_writes(False)

    # Create more data (still need transaction context)
    with temp_db.transaction():
        temp_db.command("sql", "CREATE VERTEX Combined SET value = 2")

    # Verify everything worked
    result = temp_db.query("sql", "SELECT count(*) as cnt FROM Combined")
    count = next(result).get("cnt")
    assert count == 2

    # Restore durability settings
    temp_db.set_wal_flush("yes_full")
    temp_db.set_read_your_writes(True)
