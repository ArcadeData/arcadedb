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
import subprocess  # nosec B404
import sys
import textwrap
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


def _open_in_child_process(db_path):
    """Open ``db_path`` from a separate Python process (its own JVM).

    Returns "OPENED", or "LOCKED: <message>" when the engine refuses because
    another process holds the database's file lock.
    """
    code = textwrap.dedent(f"""
        import arcadedb_embedded as arcadedb
        try:
            arcadedb.open_database({db_path!r}).close()
            print("RESULT OPENED")
        except Exception as exc:
            print("RESULT LOCKED:", exc)
        """)
    proc = subprocess.run(  # nosec B603
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=180,
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT ")]
    assert lines, f"child produced no result:\n{proc.stdout}\n{proc.stderr}"
    return lines[-1][len("RESULT ") :]


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

    lock_file = os.path.join(db_path, "database.lck")
    assert os.path.exists(lock_file), "an open database holds database.lck"
    print(f"\n2. Lock file created: {lock_file}")

    print("\n3. Closing database...")
    db.close()

    # A clean close deletes the file: one left on disk marks an unclean
    # shutdown, and the next open replays the write-ahead log because of it.
    assert not os.path.exists(lock_file)
    # Released, not merely closed: another process can open it now.
    assert _open_in_child_process(db_path) == "OPENED"
    print("   ✅ Database closed, lock released")


def test_thread_safety(cleanup_db):
    """Test multiple threads can safely access the database."""
    print("\n" + "=" * 70)
    print("TEST 2: Thread Safety (Multiple Threads, Same Process)")
    print("=" * 70)

    db_path = cleanup_db("thread_db_")

    print("\n1. Creating database with test data...")
    db = arcadedb.create_database(db_path)
    db.command("sql", "CREATE DOCUMENT TYPE Person")

    with db.transaction():
        for i in range(20):
            db.command(
                "sql", "INSERT INTO Person SET name = ?, id = ?", f"Person{i}", i
            )
    print("   ✅ Created 20 Person records")

    print("\n2. Running 4 threads concurrently...")

    def query_thread(thread_id):
        start = time.time()
        result = db.query(
            "sql",
            "SELECT id FROM Person WHERE id >= :lo AND id < :hi",
            {"lo": thread_id * 5, "hi": (thread_id + 1) * 5},
        )
        ids = sorted(row.get("id") for row in result)
        elapsed = time.time() - start
        print(f"   Thread {thread_id}: Found {len(ids)} records in {elapsed:.3f}s")
        return ids

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(query_thread, i) for i in range(4)]
        found = [future.result() for future in futures]

    # Each thread sees exactly its own five rows.
    assert found == [list(range(i * 5, (i + 1) * 5)) for i in range(4)]
    print("\n   ✅ All threads completed successfully!")

    db.close()


def test_sequential_access(cleanup_db):
    """Test sequential access (open, close, reopen)."""
    print("\n" + "=" * 70)
    print("TEST 3: Sequential Access (One Process at a Time)")
    print("=" * 70)

    db_path = cleanup_db("sequential_db_")

    print("\n1. First access - Create and populate...")
    db1 = arcadedb.create_database(db_path)
    db1.command("sql", "CREATE DOCUMENT TYPE Message")
    with db1.transaction():
        db1.command("sql", "INSERT INTO Message SET text = 'First access'")
    print("   ✅ Database created and populated")
    db1.close()
    print("   ✅ Database closed (lock released)")

    print("\n2. Second access - Reopen and query...")
    db2 = arcadedb.open_database(db_path)
    result = db2.query("sql", "SELECT FROM Message")
    count = len(list(result))
    assert count == 1
    print(f"   📊 Found {count} message(s)")
    print("   ✅ Database reopened successfully!")
    db2.close()
    print("   ✅ Database closed")

    print("\n3. Third access - Add more data...")
    db3 = arcadedb.open_database(db_path)
    with db3.transaction():
        db3.command("sql", "INSERT INTO Message SET text = 'Third access'")
    result = db3.query("sql", "SELECT text FROM Message ORDER BY text")
    texts = [row.get("text") for row in result]
    assert texts == ["First access", "Third access"]
    count = len(texts)
    print(f"   📊 Total messages: {count}")
    print("   ✅ Sequential access works perfectly!")
    db3.close()


def test_concurrent_access_limitation(cleanup_db):
    """Test that concurrent access is properly prevented."""
    print("\n" + "=" * 70)
    print("TEST 4: Concurrent Access Limitation")
    print("=" * 70)

    db_path = cleanup_db("concurrent_db_")

    print("\n1. Opening database in this process...")
    db = arcadedb.create_database(db_path)
    print("   ✅ Database opened and locked")

    print("\n2. Another process tries to open it...")
    try:
        outcome = _open_in_child_process(db_path)
    finally:
        db.close()
    assert outcome.startswith("LOCKED:"), outcome
    assert "is locked by another process" in outcome, outcome
    print(f"   ❌ {outcome}")
    print("   💡 This is BY DESIGN to prevent data corruption!")


def test_oltp_mixed_workload_threads(cleanup_db):
    """OLTP-style mixed read/write workload in a single process."""
    print("\n" + "=" * 70)
    print("TEST 5: OLTP Mixed Workload (Multi-thread, Single Process)")
    print("=" * 70)

    db_path = cleanup_db("oltp_db_")
    db = arcadedb.create_database(db_path)
    db.command("sql", "CREATE DOCUMENT TYPE Account")
    db.command("sql", "CREATE PROPERTY Account.account_id INTEGER")
    db.command("sql", "CREATE PROPERTY Account.balance INTEGER")

    initial_accounts = 1000
    print(f"\n1. Seeding {initial_accounts} accounts...")
    with db.transaction():
        for i in range(initial_accounts):
            db.command(
                "sql",
                "INSERT INTO Account SET account_id = ?, balance = 1000",
                i,
            )
    print("   ✅ Seed complete")

    worker_count = 4
    ops_per_worker = 400
    read_ratio = 0.9
    print(
        f"\n2. Running {worker_count} threads, "
        f"{ops_per_worker} ops each (read_ratio={read_ratio})..."
    )

    def worker(worker_id):
        import random  # nosec B311

        rng = random.Random(42 + worker_id)  # nosec B311
        latencies_ms = []
        reads = 0
        writes = 0
        retries = 0
        delta_sum = 0

        for _ in range(ops_per_worker):
            account_id = rng.randrange(initial_accounts)
            op = "read" if rng.random() < read_ratio else "write"
            t0 = time.time()
            if op == "read":
                result = db.query(
                    "sql",
                    "SELECT balance FROM Account WHERE account_id = ?",
                    account_id,
                )
                assert len(list(result)) == 1
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
                        delta_sum += delta
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
            "delta_sum": delta_sum,
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

    # Every operation completed, and no committed update was lost: the
    # balances sum to the seed plus exactly the deltas that committed.
    assert total_reads + total_writes == worker_count * ops_per_worker
    expected = initial_accounts * 1000 + sum(r["delta_sum"] for r in results)
    row = next(iter(db.query("sql", "SELECT sum(balance) AS total FROM Account")))
    assert row.get("total") == expected

    db.close()
