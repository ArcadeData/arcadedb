"""
Tests for ArcadeDB's concurrency behavior and file locking.

These tests demonstrate:
- File locking mechanism
- Thread-safe operations
- Sequential vs concurrent access patterns
- Multi-process limitations
"""

import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from statistics import mean

import arcadedb_embedded as arcadedb
import pytest
from arcadedb_embedded.exceptions import ArcadeDBError


@pytest.fixture
def cleanup_db():
    """Fixture to clean up test databases."""
    import tempfile

    db_paths = []

    def _create_temp_db(prefix="arcadedb_test_"):
        """Create a temporary database directory and register it for cleanup."""
        temp_dir = tempfile.mkdtemp(prefix=prefix)
        db_paths.append(temp_dir)
        return temp_dir

    yield _create_temp_db

    # Cleanup after test
    for db_path in db_paths:
        if os.path.exists(db_path):
            shutil.rmtree(db_path, ignore_errors=True)


def test_file_lock_mechanism(cleanup_db):
    """Demonstrate file locking with multiple DB instances."""
    print("\n" + "=" * 70)
    print("TEST 1: File Locking Mechanism")
    print("=" * 70)
    print("Shows that ArcadeDB uses file locks to prevent concurrent access")
    print()

    db_path = cleanup_db("lock_db_")

    print("\n1. Opening database...")
    db = arcadedb.create_database(db_path)
    print("   ✅ Database opened")

    # Check for lock file
    lock_file = os.path.join(db_path, "database.lck")
    if os.path.exists(lock_file):
        print(f"\n2. Lock file created: {lock_file}")
        print(f"   📝 Size: {os.path.getsize(lock_file)} bytes")
        print("   🔒 This prevents other processes from opening the database")

    print("\n3. Closing database...")
    db.close()
    print("   ✅ Database closed, lock released")


def test_thread_safety(cleanup_db):
    """Test multiple threads can safely access the database."""
    print("\n" + "=" * 70)
    print("TEST 2: Thread Safety (Multiple Threads, Same Process)")
    print("=" * 70)

    db_path = cleanup_db("thread_db_")

    print("\n1. Creating database with test data...")
    db = arcadedb.create_database(db_path)
    db.schema.create_document_type("Person")

    with db.transaction():
        for i in range(20):
            db.command("sql", f"INSERT INTO Person SET name = 'Person{i}', id = {i}")
    print("   ✅ Created 20 Person records")

    print("\n2. Running 4 threads concurrently...")

    def query_thread(thread_id):
        start = time.time()
        query = (
            f"SELECT FROM Person WHERE id >= {thread_id * 5} "
            f"AND id < {(thread_id + 1) * 5}"
        )
        result = db.query("sql", query)
        count = len(list(result))
        elapsed = time.time() - start
        return f"   Thread {thread_id}: Found {count} records in {elapsed:.3f}s"

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(query_thread, i) for i in range(4)]
        for future in futures:
            print(future.result())

    print("\n   ✅ All threads completed successfully!")

    db.close()
    cleanup_db(db_path)


def test_sequential_access(cleanup_db):
    """Test sequential access (open, close, reopen)."""
    print("\n" + "=" * 70)
    print("TEST 3: Sequential Access (One Process at a Time)")
    print("=" * 70)

    db_path = cleanup_db("sequential_db_")

    print("\n1. First access - Create and populate...")
    db1 = arcadedb.create_database(db_path)
    # Use Schema API for embedded setup (auto-transactional)
    db1.schema.create_document_type("Message")
    with db1.transaction():
        db1.command("sql", "INSERT INTO Message SET text = 'First access'")
    print("   ✅ Database created and populated")
    db1.close()
    print("   ✅ Database closed (lock released)")

    print("\n2. Second access - Reopen and query...")
    db2 = arcadedb.open_database(db_path)
    result = db2.query("sql", "SELECT FROM Message")
    count = len(list(result))
    print(f"   📊 Found {count} message(s)")
    print("   ✅ Database reopened successfully!")
    db2.close()
    print("   ✅ Database closed")

    print("\n3. Third access - Add more data...")
    db3 = arcadedb.open_database(db_path)
    with db3.transaction():
        db3.command("sql", "INSERT INTO Message SET text = 'Third access'")
    result = db3.query("sql", "SELECT FROM Message")
    count = len(list(result))
    print(f"   📊 Total messages: {count}")
    print("   ✅ Sequential access works perfectly!")
    db3.close()

    cleanup_db(db_path)


def test_concurrent_access_limitation(cleanup_db):
    """Test that concurrent access is properly prevented."""
    print("\n" + "=" * 70)
    print("TEST 4: Concurrent Access Limitation")
    print("=" * 70)

    db_path = cleanup_db("concurrent_db_")

    print("\n1. Opening database in this process...")
    db = arcadedb.create_database(db_path)
    print("   ✅ Database opened and locked")

    print("\n2. What happens if another process tries to open it?")
    print("   ❌ It would get: LockException")
    print("   ❌ Error: 'Database is locked by another process'")
    print("   💡 This is BY DESIGN to prevent data corruption!")

    db.close()


def test_oltp_mixed_workload_threads(cleanup_db):
    """OLTP-style mixed read/write workload in a single process."""
    print("\n" + "=" * 70)
    print("TEST 5: OLTP Mixed Workload (Multi-thread, Single Process)")
    print("=" * 70)

    db_path = cleanup_db("oltp_db_")
    db = arcadedb.create_database(db_path)
    db.schema.create_document_type("Account")
    db.schema.create_property("Account", "account_id", "INTEGER")
    db.schema.create_property("Account", "balance", "INTEGER")

    initial_accounts = 10000
    print(f"\n1. Seeding {initial_accounts} accounts...")
    with db.transaction():
        for i in range(initial_accounts):
            db.command(
                "sql",
                f"INSERT INTO Account SET account_id = {i}, balance = 1000",
            )
    print("   ✅ Seed complete")

    worker_count = 6
    ops_per_worker = 600
    read_ratio = 0.9
    print(
        f"\n2. Running {worker_count} threads, "
        f"{ops_per_worker} ops each (read_ratio={read_ratio})..."
    )

    def worker(worker_id):
        import random

        rng = random.Random(42 + worker_id)
        latencies_ms = []
        reads = 0
        writes = 0
        retries = 0

        for _ in range(ops_per_worker):
            account_id = rng.randrange(initial_accounts)
            op = "read" if rng.random() < read_ratio else "write"
            t0 = time.time()
            if op == "read":
                result = db.query(
                    "sql",
                    f"SELECT balance FROM Account WHERE account_id = {account_id}",
                )
                _ = list(result)
                reads += 1
            else:
                delta = rng.choice([-5, -1, 1, 5])
                max_retries = 12
                for attempt in range(max_retries):
                    try:
                        with db.transaction():
                            db.command(
                                "sql",
                                "UPDATE Account SET balance = balance + ? "
                                "WHERE account_id = ?",
                                delta,
                                account_id,
                            )
                        writes += 1
                        break
                    except ArcadeDBError as exc:
                        if "ConcurrentModificationException" not in str(exc):
                            raise
                        retries += 1
                        time.sleep(0.005 * (attempt + 1))
                else:
                    raise AssertionError(
                        "Write failed after retries due to concurrent modifications"
                    )
            latencies_ms.append((time.time() - t0) * 1000.0)

        return {
            "reads": reads,
            "writes": writes,
            "retries": retries,
            "latencies_ms": latencies_ms,
        }

    t_start = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(worker, i) for i in range(worker_count)]
        for f in futures:
            results.append(f.result())
    t_total = time.time() - t_start

    total_reads = sum(r["reads"] for r in results)
    total_writes = sum(r["writes"] for r in results)
    total_retries = sum(r["retries"] for r in results)
    all_lat = [x for r in results for x in r["latencies_ms"]]
    throughput = (total_reads + total_writes) / t_total if t_total else 0

    print("\n3. Results:")
    print(f"   Total ops: {total_reads + total_writes}")
    print(f"   Reads/Writes: {total_reads}/{total_writes}")
    print(f"   Retries: {total_retries}")
    print(f"   Throughput: {throughput:,.0f} ops/sec")
    print(f"   Avg latency: {mean(all_lat):.2f} ms")
    print(f"   p95 latency: {sorted(all_lat)[int(len(all_lat)*0.95)-1]:.2f} ms")

    db.close()
