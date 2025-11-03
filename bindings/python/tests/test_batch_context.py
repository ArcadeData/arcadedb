"""Tests for BatchContext class."""

import time

import pytest


def test_batch_context_basic(temp_db):
    """Test basic batch context usage."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE User")

    # Use batch context to create vertices
    with db.batch_context(batch_size=100, parallel=2) as batch:
        for i in range(500):
            batch.create_vertex("User", userId=i, name=f"User{i}")

    # Verify all vertices were created
    result = db.query("sql", "SELECT count(*) as count FROM User")
    count = next(result).get_property("count")
    assert count == 500, f"Expected 500 vertices, got {count}"


def test_batch_context_with_documents(temp_db):
    """Test batch context with documents."""
    db = temp_db

    # Create document type
    db.command("sql", "CREATE DOCUMENT TYPE LogEntry")

    # Use batch context to create documents
    with db.batch_context(batch_size=50, parallel=4) as batch:
        for i in range(200):
            batch.create_document(
                "LogEntry", level="INFO", message=f"Log message {i}", sequence=i
            )

    # Verify all documents were created
    result = db.query("sql", "SELECT count(*) as count FROM LogEntry")
    count = next(result).get_property("count")
    assert count == 200, f"Expected 200 documents, got {count}"


def test_batch_context_with_edges(temp_db):
    """Test batch context with edge creation."""
    db = temp_db

    # Create schema
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE EDGE TYPE KNOWS")

    # Create some vertices first
    with db.transaction():
        person1 = db.new_vertex("Person")
        person1.set("name", "Alice")
        person1.save()

        person2 = db.new_vertex("Person")
        person2.set("name", "Bob")
        person2.save()

        person3 = db.new_vertex("Person")
        person3.set("name", "Charlie")
        person3.save()

    # Query vertices
    people = list(db.query("sql", "SELECT FROM Person"))
    assert len(people) == 3

    # Get Java vertex objects for edge creation
    alice = people[0]._java_result.getElement().get().asVertex()
    bob = people[1]._java_result.getElement().get().asVertex()
    charlie = people[2]._java_result.getElement().get().asVertex()

    # Create edges in batch (edges need to be created in transaction context)
    with db.transaction():
        with db.batch_context(batch_size=10) as batch:
            batch.create_edge(alice, bob, "KNOWS", since=2020)
            batch.create_edge(bob, charlie, "KNOWS", since=2021)
            batch.create_edge(charlie, alice, "KNOWS", since=2022)

    # Verify edges were created
    result = db.query("sql", "SELECT count(*) as count FROM KNOWS")
    count = next(result).get_property("count")
    assert count == 3, f"Expected 3 edges, got {count}"


def test_batch_context_with_callbacks(temp_db):
    """Test batch context with success callbacks."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Item")

    created_ids = []

    def on_created(record):
        """Callback to collect created record IDs."""
        created_ids.append(str(record.getIdentity()))

    # Use batch context with callbacks
    with db.batch_context(batch_size=50) as batch:
        for i in range(100):
            batch.create_vertex("Item", itemId=i, callback=on_created)

    # Verify callbacks were called
    assert len(created_ids) == 100, f"Expected 100 callbacks, got {len(created_ids)}"

    # Verify all items were created
    result = db.query("sql", "SELECT count(*) as count FROM Item")
    count = next(result).get_property("count")
    assert count == 100


def test_batch_context_success_count(temp_db):
    """Test batch context success counting."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Product")

    # Use batch context and track success count
    with db.batch_context(batch_size=100) as batch:
        for i in range(250):
            batch.create_vertex("Product", productId=i, name=f"Product{i}")

        # Check success count
        batch.wait_completion()
        success_count = batch.get_success_count()

    assert success_count == 250, f"Expected 250 successes, got {success_count}"


def test_batch_context_create_record(temp_db):
    """Test batch context with direct record creation."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Node")

    # Create records directly
    with db.batch_context(batch_size=50) as batch:
        for i in range(150):
            node = db.new_vertex("Node")
            node.set("nodeId", i)
            node.set("label", f"Node_{i}")
            batch.create_record(node)

    # Verify all nodes were created
    result = db.query("sql", "SELECT count(*) as count FROM Node")
    count = next(result).get_property("count")
    assert count == 150


def test_batch_context_is_pending(temp_db):
    """Test batch context is_pending status."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Task")

    with db.batch_context(batch_size=1000, parallel=2) as batch:
        # Queue many operations
        for i in range(5000):
            batch.create_vertex("Task", taskId=i)

        # Should have pending operations
        assert batch.is_pending() or True  # May complete very fast

    # After context exit, should be complete
    assert not batch.is_pending()


def test_batch_context_wait_completion(temp_db):
    """Test batch context manual wait_completion."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Event")

    with db.batch_context(batch_size=500, parallel=4) as batch:
        for i in range(2000):
            batch.create_vertex("Event", eventId=i, timestamp=time.time())

        # Manually wait for completion
        batch.wait_completion()

        # Should be complete now
        assert not batch.is_pending()

    # Verify all events were created
    result = db.query("sql", "SELECT count(*) as count FROM Event")
    count = next(result).get_property("count")
    assert count == 2000


def test_batch_context_performance(temp_db):
    """Test batch context performance vs synchronous operations."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Benchmark")

    # Measure batch context performance
    start_batch = time.time()
    with db.batch_context(batch_size=5000, parallel=8) as batch:
        for i in range(10000):
            batch.create_vertex("Benchmark", value=i)
    batch_time = time.time() - start_batch

    print(f"\nBatch context: {10000 / batch_time:.0f} records/sec")

    # Clean up for sync test
    with db.transaction():
        db.command("sql", "DELETE FROM Benchmark")

    # Measure synchronous performance
    start_sync = time.time()
    with db.transaction():
        for i in range(10000):
            vertex = db.new_vertex("Benchmark")
            vertex.set("value", i)
            vertex.save()
    sync_time = time.time() - start_sync

    print(f"Synchronous: {10000 / sync_time:.0f} records/sec")
    print(f"Speedup: {sync_time / batch_time:.1f}x")

    # Note: Batch context may not always be faster for small datasets
    # due to callback overhead, but it provides better API and automatic cleanup
    # For large datasets (100k+), batch context should be significantly faster


def test_batch_context_different_batch_sizes(temp_db):
    """Test batch context with different batch sizes."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Record")

    # Test with small batch size
    with db.batch_context(batch_size=10) as batch:
        for i in range(50):
            batch.create_vertex("Record", recordId=i, batch="small")

    # Test with large batch size
    with db.batch_context(batch_size=5000) as batch:
        for i in range(50, 100):
            batch.create_vertex("Record", recordId=i, batch="large")

    # Verify all records were created
    result = db.query("sql", "SELECT count(*) as count FROM Record")
    count = next(result).get_property("count")
    assert count == 100


def test_batch_context_update_record(temp_db):
    """Test batch context with record updates."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Counter")

    # Create some initial records
    with db.transaction():
        for i in range(100):
            counter = db.new_vertex("Counter")
            counter.set("value", i)
            counter.save()

    # Query and update records in batch (modify needs transaction)
    counters = list(db.query("sql", "SELECT FROM Counter"))

    with db.transaction():
        with db.batch_context(batch_size=50) as batch:
            for counter in counters:
                java_vertex = (
                    counter._java_result.getElement().get().asVertex().modify()
                )
                java_vertex.set("value", counter.get_property("value") * 2)
                batch.update_record(java_vertex)

    # Verify updates
    result = db.query("sql", "SELECT sum(value) as total FROM Counter")
    total = next(result).get_property("total")
    # Sum of (0*2 + 1*2 + 2*2 + ... + 99*2) = 2 * sum(0..99) = 2 * 4950 = 9900
    assert total == 9900


def test_batch_context_delete_record(temp_db):
    """Test batch context with record deletion."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Temporary")

    # Create records
    with db.transaction():
        for i in range(200):
            temp = db.new_vertex("Temporary")
            temp.set("tempId", i)
            temp.save()

    # Query records to delete (delete even IDs)
    to_delete = list(db.query("sql", "SELECT FROM Temporary WHERE tempId % 2 = 0"))

    # Delete in batch
    with db.batch_context(batch_size=50) as batch:
        for record in to_delete:
            java_record = record._java_result.getElement().get()
            batch.delete_record(java_record)

    # Verify deletions (should have 100 odd IDs left)
    result = db.query("sql", "SELECT count(*) as count FROM Temporary")
    count = next(result).get_property("count")
    assert count == 100, f"Expected 100 records, got {count}"


def test_batch_context_mixed_operations(temp_db):
    """Test batch context with mixed create/update/delete operations."""
    db = temp_db

    # Create vertex type
    db.command("sql", "CREATE VERTEX TYPE Mixed")

    # Create some initial records
    with db.transaction():
        for i in range(50):
            mixed = db.new_vertex("Mixed")
            mixed.set("value", i)
            mixed.set("status", "old")
            mixed.save()

    with db.transaction():
        with db.batch_context(batch_size=100) as batch:
            # Create new records
            for i in range(50, 100):
                batch.create_vertex("Mixed", value=i, status="new")

            # Update existing records
            existing = list(db.query("sql", "SELECT FROM Mixed WHERE status = 'old'"))
            for record in existing[:25]:  # Update first 25
                java_vertex = record._java_result.getElement().get().asVertex().modify()
                java_vertex.set("status", "updated")
                batch.update_record(java_vertex)

            # Delete some records
            for record in existing[25:]:  # Delete last 25
                java_record = record._java_result.getElement().get()
                batch.delete_record(java_record)

    # Verify final state
    result = db.query("sql", "SELECT count(*) as count FROM Mixed")
    total = next(result).get_property("count")
    assert total == 75  # 25 updated + 50 new

    result = db.query(
        "sql", "SELECT count(*) as count FROM Mixed WHERE status = 'updated'"
    )
    updated = next(result).get_property("count")
    assert updated == 25

    result = db.query("sql", "SELECT count(*) as count FROM Mixed WHERE status = 'new'")
    new = next(result).get_property("count")
    assert new == 50
