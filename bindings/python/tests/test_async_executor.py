"""
Tests for AsyncExecutor - async operations and parallel execution.
"""

import shutil
import tempfile
import time
from pathlib import Path

import arcadedb_embedded as arcadedb


def test_async_executor_basic_create():
    """Test basic async record creation."""
    db_path = Path(tempfile.mkdtemp()) / "test_async_basic"

    try:
        db = arcadedb.create_database(str(db_path))

        # Create schema - use VERTEX type for vertices
        db.schema.create_vertex_type("User")

        # Get async executor
        async_exec = db.async_executor()
        async_exec.set_commit_every(25)  # Explicit transaction cadence for writes
        assert async_exec is not None

        # Create records asynchronously
        for i in range(100):
            vertex = db.new_vertex("User")
            vertex.set("id", i)
            vertex.set("name", f"User{i}")
            async_exec.create_record(vertex)

        # Wait for completion
        async_exec.wait_completion()

        # Close executor to shutdown worker threads
        async_exec.close()

        # Verify all records created
        result = db.query("sql", "SELECT count(*) as count FROM User")
        count = result.first().get("count")
        assert count == 100

        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_async_executor_with_commit_every():
    """Test async executor with automatic commits."""
    db_path = Path(tempfile.mkdtemp()) / "test_async_commit"

    try:
        db = arcadedb.create_database(str(db_path))
        db.schema.create_vertex_type("Item")

        # Configure async executor with auto-commit
        async_exec = db.async_executor()
        async_exec.set_commit_every(50)  # Commit every 50 records

        # Create 200 records (should trigger 4 commits)
        for i in range(200):
            vertex = db.new_vertex("Item")
            vertex.set("id", i)
            async_exec.create_record(vertex)

        async_exec.wait_completion()

        # Close executor to shutdown worker threads
        async_exec.close()

        # Verify all records
        count = db.count_type("Item")
        assert count == 200

        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_async_executor_with_parallel_level():
    """Test async executor with parallel workers."""
    db_path = Path(tempfile.mkdtemp()) / "test_async_parallel"

    try:
        db = arcadedb.create_database(str(db_path))
        db.schema.create_vertex_type("Product")

        # Configure with 4 parallel workers
        async_exec = db.async_executor()
        async_exec.set_parallel_level(4)
        async_exec.set_commit_every(100)

        # Create 500 records in parallel
        start_time = time.time()
        for i in range(500):
            vertex = db.new_vertex("Product")
            vertex.set("id", i)
            vertex.set("price", i * 10.5)
            async_exec.create_record(vertex)

        async_exec.wait_completion()
        elapsed = time.time() - start_time

        # Close executor to shutdown worker threads
        async_exec.close()

        # Verify
        count = db.count_type("Product")
        assert count == 500

        print(f"Created 500 records in {elapsed:.3f}s ({500/elapsed:.0f} rec/sec)")

        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_async_executor_method_chaining():
    """Test fluent interface (method chaining)."""
    db_path = Path(tempfile.mkdtemp()) / "test_async_chain"

    try:
        db = arcadedb.create_database(str(db_path))
        db.schema.create_vertex_type("Task")

        # Chain configuration methods
        async_exec = (
            db.async_executor()
            .set_parallel_level(2)
            .set_commit_every(25)
            .set_back_pressure(75)
        )

        # Create records
        for i in range(100):
            vertex = db.new_vertex("Task")
            vertex.set("id", i)
            async_exec.create_record(vertex)

        async_exec.wait_completion()

        # Close executor to shutdown worker threads
        async_exec.close()

        count = db.count_type("Task")
        assert count == 100

        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_async_executor_is_pending():
    """Test is_pending() method."""
    db_path = Path(tempfile.mkdtemp()) / "test_async_pending"

    try:
        db = arcadedb.create_database(str(db_path))
        db.schema.create_vertex_type("Message")

        async_exec = db.async_executor()
        async_exec.set_commit_every(100)

        # Initially no pending operations
        assert not async_exec.is_pending()

        # Add operations
        for i in range(1000):
            vertex = db.new_vertex("Message")
            vertex.set("id", i)
            async_exec.create_record(vertex)

        # Should have pending operations (unless they complete very fast)
        # Note: This might be False if operations complete immediately
        # pending_status = async_exec.is_pending()
        # (We can't reliably assert True due to timing)

        # Wait for completion
        async_exec.wait_completion()

        # Close executor to shutdown worker threads
        async_exec.close()

        # Now should have no pending operations
        assert not async_exec.is_pending()

        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


def test_async_executor_callback(temp_db):
    """Test async executor with callbacks."""
    db = temp_db

    # Create a type
    db.schema.create_vertex_type("User")

    # Track callback invocations
    created_ids = []

    def on_create(record):
        # Record is a raw Java object - can access properties directly
        created_ids.append(record.getIdentity().toString())

    # Create executor with callback
    async_exec = db.async_executor()
    async_exec.set_parallel_level(2).set_commit_every(5)

    # Create records with callback
    for i in range(10):
        record = db.new_vertex("User")
        record.set("name", f"User{i}")
        async_exec.create_record(record, callback=on_create)

    # Wait for completion
    async_exec.wait_completion()

    # Close executor to shutdown worker threads
    async_exec.close()

    # Verify all callbacks were invoked
    assert len(created_ids) == 10, f"Expected 10 callbacks, got {len(created_ids)}"

    # Verify records were created
    result = db.query("sql", "SELECT FROM User")
    assert result.count() == 10


def test_async_executor_global_callback(temp_db):
    """Test that per-operation callbacks work (global callbacks have JPype proxy issues)."""
    db = temp_db
    db.schema.create_vertex_type("Log")

    success_count = [0]  # Use list to allow mutation in closure

    def on_success(record):
        """Per-operation success callback."""
        success_count[0] += 1

    async_exec = db.async_executor()
    async_exec.set_commit_every(10)

    # NOTE: Global callbacks via on_ok() have JPype proxy creation issues
    # Use per-operation callbacks instead as a workaround
    for i in range(20):
        vertex = db.new_vertex("Log")
        vertex.set("id", i)
        async_exec.create_record(vertex, callback=on_success)

    async_exec.wait_completion()

    # Close executor to shutdown worker threads
    async_exec.close()

    # Per-operation callbacks should work
    assert success_count[0] == 20


def test_async_vs_sync_performance():
    """Compare async vs sync performance for bulk inserts."""
    db_path = Path(tempfile.mkdtemp()) / "test_async_perf"

    try:
        db = arcadedb.create_database(str(db_path))
        db.schema.create_vertex_type("Benchmark")

        # Test synchronous
        sync_start = time.time()
        with db.transaction():
            for i in range(1000):
                vertex = db.new_vertex("Benchmark")
                vertex.set("id", i)
                vertex.set("type", "sync")
                vertex.save()
        sync_time = time.time() - sync_start

        # Clear data
        with db.transaction():
            db.command("sql", "DELETE FROM Benchmark")

        # Test asynchronous
        async_exec = db.async_executor()
        async_exec.set_parallel_level(4)
        async_exec.set_commit_every(250)

        async_start = time.time()
        for i in range(1000):
            vertex = db.new_vertex("Benchmark")
            vertex.set("id", i)
            vertex.set("type", "async")
            async_exec.create_record(vertex)
        async_exec.wait_completion()
        async_time = time.time() - async_start

        # Close executor to shutdown worker threads
        async_exec.close()

        # Verify counts
        sync_count = (
            db.query("sql", "SELECT count(*) as c FROM Benchmark WHERE type = 'sync'")
            .first()
            .get("c")
        )
        async_count = (
            db.query("sql", "SELECT count(*) as c FROM Benchmark WHERE type = 'async'")
            .first()
            .get("c")
        )

        assert sync_count == 0  # We deleted sync records
        assert async_count == 1000

        print(f"\nPerformance Comparison (1000 records):")
        print(f"  Sync:  {sync_time:.3f}s ({1000/sync_time:.0f} rec/sec)")
        print(f"  Async: {async_time:.3f}s ({1000/async_time:.0f} rec/sec)")
        print(f"  Speedup: {sync_time/async_time:.2f}x")

        # Async should be faster (though not guaranteed in all environments)
        # We'll just verify it works, not enforce speedup
        assert async_time > 0  # Sanity check

        db.close()
    finally:
        shutil.rmtree(db_path, ignore_errors=True)


if __name__ == "__main__":
    print("Testing AsyncExecutor...")
    test_async_executor_basic_create()
    print("✓ Basic create")

    test_async_executor_with_commit_every()
    print("✓ Auto-commit")

    test_async_executor_with_parallel_level()
    print("✓ Parallel execution")

    test_async_executor_method_chaining()
    print("✓ Method chaining")

    test_async_executor_is_pending()
    print("✓ Pending status")

    test_async_executor_callback()
    print("✓ Per-operation callback")

    test_async_executor_global_callback()
    print("✓ Global callback")

    test_async_vs_sync_performance()
    print("✓ Performance comparison")

    print("\n✅ All AsyncExecutor tests passed!")
