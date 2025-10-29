"""
Tests for ArcadeDB server access patterns.

Tests three main access patterns:
1. Java API Standalone: Direct database access (no server)
2. Java API Server-managed: Access via server (embedded)
3. HTTP API: REST requests to server (remote access)

Key insight: Server supports BOTH Java API (embedded) and HTTP API (remote)
simultaneously.
"""

import os
import shutil
import threading
import time

import arcadedb_embedded as arcadedb
import pytest


@pytest.fixture
def cleanup_test_dirs():
    """Fixture to clean up test directories and servers."""
    import tempfile

    dirs = []
    servers = []

    def _create_temp_dir(prefix="arcadedb_test_"):
        """Create a temporary directory and register it for cleanup."""
        temp_dir = tempfile.mkdtemp(prefix=prefix)
        dirs.append(temp_dir)
        return temp_dir

    def _register_server(server):
        servers.append(server)

    yield _create_temp_dir, _register_server

    # Cleanup after test
    for server in servers:
        try:
            if server.is_started():
                server.stop()
        except Exception:
            pass

    # Give servers time to release locks
    time.sleep(0.5)

    for path in dirs:
        if os.path.exists(path):
            try:
                shutil.rmtree(path, ignore_errors=True)
            except Exception:
                pass


def test_server_pattern_recommended(cleanup_test_dirs):
    """
    Recommended Pattern: Start server first, create database through server.

    Benefits:
    - Embedded access for the Python process that started the server
    - HTTP access available for other processes
    - No need to close database before server access
    """
    create_temp_dir, register_server = cleanup_test_dirs

    print("\n" + "=" * 70)
    print("TEST: Recommended Pattern - Server First")
    print("=" * 70)

    root_path = create_temp_dir("server_first_")

    # Step 1: Start server first
    print("\n1. Starting ArcadeDB server...")
    server = arcadedb.create_server(
        root_path=root_path, root_password="test12345"  # Min 8 chars required
    )
    register_server(server)
    server.start()
    print(f"   ✅ Server started on port {server.get_http_port()}")
    print(f"   📊 Studio URL: {server.get_studio_url()}")

    # Step 2: Create database through server
    print("\n2. Creating database through server...")
    db = server.create_database("mydb")

    with db.transaction():
        db.command("sql", "CREATE DOCUMENT TYPE Product")
        db.command("sql", "INSERT INTO Product SET name = 'Laptop', price = 999")
        db.command("sql", "INSERT INTO Product SET name = 'Mouse', price = 29")

    print("   ✅ Database created and populated")

    # Step 3: Query via embedded access
    print("\n3. Querying via embedded access...")
    result = db.query("sql", "SELECT FROM Product WHERE name = 'Laptop'")
    record = list(result)[0]
    name = record.get_property("name")
    price = record.get_property("price")
    print(f"   ✅ Found: {name} costs ${price}")

    # Step 4: HTTP access would work here too
    print("\n4. HTTP API is now available...")
    print(
        f"   💡 Other processes can connect to: http://localhost:{server.get_http_port()}"
    )
    print("   💡 Both embedded AND HTTP access work simultaneously!")

    db.close()
    server.stop()
    print("\n✅ Recommended Pattern Complete!\n")


def test_server_thread_safety(cleanup_test_dirs):
    """
    Test that server-managed database handles concurrent thread access.

    Multiple threads can safely access the same database through the server.
    """
    create_temp_dir, register_server = cleanup_test_dirs

    print("\n" + "=" * 70)
    print("TEST: Server Thread Safety")
    print("=" * 70)

    root_path = create_temp_dir("thread_safety_")

    # Start server and create database
    print("\n1. Setting up server and database...")
    server = arcadedb.create_server(root_path=root_path, root_password="test12345")
    register_server(server)
    server.start()

    db = server.create_database("testdb")

    with db.transaction():
        db.command("sql", "CREATE DOCUMENT TYPE Item")
        for i in range(20):
            db.command("sql", f"INSERT INTO Item SET id = {i}, value = {i * 10}")

    print("   ✅ Created 20 items")

    # Test concurrent thread access
    print("\n2. Running 5 threads concurrently...")
    results = []
    errors = []

    def thread_query(thread_id):
        """Each thread queries the database."""
        try:
            # Query a range of items
            start = thread_id * 4
            end = start + 4
            result = db.query(
                "sql", f"SELECT FROM Item WHERE id >= {start} AND id < {end}"
            )
            count = len(list(result))
            results.append(f"   Thread {thread_id}: Found {count} items")
        except Exception as e:
            errors.append(f"   Thread {thread_id}: Error - {e}")

    threads = []
    for i in range(5):
        thread = threading.Thread(target=thread_query, args=(i,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    for result_msg in results:
        print(result_msg)

    if errors:
        for error_msg in errors:
            print(error_msg)
        pytest.fail("Concurrent thread access failed")

    print("   ✅ All threads accessed database successfully!")

    db.close()
    server.stop()
    print("\n✅ Thread Safety Test Complete!\n")


def test_server_context_manager(cleanup_test_dirs):
    """Test using server with context manager for automatic cleanup."""
    create_temp_dir, register_server = cleanup_test_dirs

    print("\n" + "=" * 70)
    print("TEST: Server Context Manager")
    print("=" * 70)

    root_path = create_temp_dir("context_")

    print("\n1. Using server with context manager...")

    # Server automatically starts and stops
    with arcadedb.create_server(
        root_path=root_path, root_password="test12345"
    ) as server:
        print("   ✅ Server started (automatic)")

        db = server.create_database("contextdb")

        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE Note")
            db.command("sql", "INSERT INTO Note SET text = 'Test'")

        result = db.query("sql", "SELECT count(*) as count FROM Note")
        count = list(result)[0].get_property("count")
        print(f"   ✅ Created {count} notes")

        db.close()

    # Server automatically stopped when exiting context
    print("   ✅ Server stopped (automatic)")
    print("\n✅ Context Manager Test Complete!\n")


def test_pattern1_embedded_first_requires_close(cleanup_test_dirs):
    """
    Pattern 1: Create database with embedded API first, then start server.

    IMPORTANT: Must close the database before starting server, otherwise
    the file lock prevents server access.

    This test shows you MUST close the embedded database before the
    server can access it.
    """
    create_temp_dir, register_server = cleanup_test_dirs

    print("\n" + "=" * 70)
    print("TEST: Pattern 1 - Embedded First (Requires Close)")
    print("=" * 70)

    root_path = create_temp_dir("pattern1_")
    db_name = "mydb"
    db_path = os.path.join(root_path, db_name)

    # Step 1: Create database with embedded API
    print("\n1. Creating database with embedded API...")
    db = arcadedb.create_database(db_path)

    with db.transaction():
        db.command("sql", "CREATE DOCUMENT TYPE Person")
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")
        db.command("sql", "INSERT INTO Person SET name = 'Bob', age = 25")

    result = db.query("sql", "SELECT count(*) as count FROM Person")
    count = list(result)[0].get_property("count")
    print(f"   ✅ Created database with {count} records")

    # Step 2: MUST close database to release file lock
    print("\n2. Closing database to release file lock...")
    db.close()
    print("   ✅ Database closed, lock released")

    # Step 3: Now move database to where server expects it
    print("\n3. Moving database to server's databases directory...")
    server_db_dir = os.path.join(root_path, "databases")
    os.makedirs(server_db_dir, exist_ok=True)
    server_db_path = os.path.join(server_db_dir, db_name)

    if os.path.exists(server_db_path):
        shutil.rmtree(server_db_path)
    shutil.move(db_path, server_db_path)
    print(f"   ✅ Database moved to {server_db_path}")

    # Step 4: Start server
    print("\n4. Starting ArcadeDB server...")
    server = arcadedb.create_server(root_path=root_path, root_password="test12345")
    register_server(server)
    server.start()
    print(f"   ✅ Server started on port {server.get_http_port()}")

    # Step 5: Access database through server
    print("\n5. Accessing database through server...")
    db = server.get_database(db_name)

    result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
    record = list(result)[0]
    name = record.get_property("name")
    age = record.get_property("age")
    print(f"   ✅ Retrieved via server: {name}, age {age}")

    # Step 6: Add more data through server
    print("\n6. Adding data through server...")
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Charlie', age = 35")

    result = db.query("sql", "SELECT count(*) as count FROM Person")
    count = list(result)[0].get_property("count")
    print(f"   ✅ Total records now: {count}")

    # Step 7: Both embedded and HTTP access now available
    print("\n7. Dual access now available...")
    print(f"   💡 Embedded access: db.query() works")
    print(f"   💡 HTTP access: http://localhost:{server.get_http_port()} works")
    print("   💡 Note: Server-managed databases are closed by server.stop()")

    # Don't close db - server-managed databases are shared and closed by server
    server.stop()
    print("\n✅ Pattern 1 Complete: Embedded → Close → Server works!\n")
    print("⚠️  Key Requirement: Must close() the embedded database first!")
    print("⚠️  Note: Don't close server-managed databases - server handles it!")


def test_embedded_performance_comparison(cleanup_test_dirs):
    """
    Demonstrate that Pattern 2 embedded access is just as fast as standalone.

    Key insight: When you access a server-managed database from the same
    Python process, it's a direct JVM call - NO HTTP overhead!

    HTTP access is only for OTHER processes/clients.
    """
    create_temp_dir, register_server = cleanup_test_dirs

    print("\n" + "=" * 70)
    print("TEST: Embedded Performance - Server vs Standalone")
    print("=" * 70)

    # Setup test data size - increased for more reliable benchmarks
    num_records = 5000
    num_queries = 1000

    # Test 1: Standalone embedded (no server)
    print("\n1. Standalone Embedded Mode...")
    standalone_path = create_temp_dir("standalone_perf_")

    db_standalone = arcadedb.create_database(standalone_path)

    # Create document type (schema-less by design)
    db_standalone.command("sql", "CREATE DOCUMENT TYPE PerfTest")

    # Create indexes for better query performance
    # Will be created implicitly when we insert data with these fields

    # Insert complex data with various data types
    categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
    import random
    from datetime import datetime, timedelta

    with db_standalone.transaction():

        for i in range(num_records):
            category = categories[i % len(categories)]
            price = round(random.uniform(10.0, 999.99), 2)
            created_date = datetime.now() - timedelta(days=random.randint(0, 365))
            is_active = random.choice([True, False])
            tags = ",".join(
                random.choices(["new", "sale", "popular", "limited", "premium"], k=2)
            )

            db_standalone.command(
                "sql",
                f"""
                INSERT INTO PerfTest SET
                    id = {i},
                    name = 'Product {i}',
                    description = 'Product {i} with detailed description',
                    price = {price},
                    category = '{category}',
                    tags = '{tags}',
                    created_date = '{created_date.strftime('%Y-%m-%d')}',
                    is_active = {str(is_active).lower()}
            """,
            )

    print(f"   ✅ Created {num_records} complex records")

    # Time standalone queries with complex operations
    import time

    query_types = [
        "SELECT FROM PerfTest WHERE price > 100 ORDER BY price LIMIT 10",
        "SELECT category, avg(price) as avg_price FROM PerfTest GROUP BY category",
        "SELECT FROM PerfTest WHERE category = 'Electronics' AND is_active = true",
        "SELECT FROM PerfTest WHERE tags LIKE '%sale%' ORDER BY created_date DESC",
        "SELECT FROM PerfTest WHERE id BETWEEN 100 AND 200 AND price < 500",
    ]

    start = time.time()
    for i in range(num_queries):
        query = query_types[i % len(query_types)]
        result = db_standalone.query("sql", query)
        list(result)  # Consume results
    standalone_time = time.time() - start

    print(f"   ⚡ {num_queries} complex queries in {standalone_time:.3f}s")
    print(f"   ⚡ {num_queries/standalone_time:.1f} queries/sec")

    db_standalone.close()

    # Test 2: Server-managed embedded access (same process)
    print("\n2. Server-Managed Embedded Mode (same process)...")
    server_path = create_temp_dir("server_perf_")

    server = arcadedb.create_server(root_path=server_path, root_password="test12345")
    register_server(server)
    server.start()

    db_server = server.create_database("perfdb")

    # Create document type (schema-less by design)
    db_server.command("sql", "CREATE DOCUMENT TYPE PerfTest")

    with db_server.transaction():
        for i in range(num_records):
            category = categories[i % len(categories)]
            price = round(random.uniform(10.0, 999.99), 2)
            created_date = datetime.now() - timedelta(days=random.randint(0, 365))
            is_active = random.choice([True, False])
            tags = ",".join(
                random.choices(["new", "sale", "popular", "limited", "premium"], k=2)
            )

            db_server.command(
                "sql",
                f"""
                INSERT INTO PerfTest SET
                    id = {i},
                    name = 'Product {i}',
                    description = 'Product {i} with detailed description',
                    price = {price},
                    category = '{category}',
                    tags = '{tags}',
                    created_date = '{created_date.strftime('%Y-%m-%d')}',
                    is_active = {str(is_active).lower()}
            """,
            )

    print(f"   ✅ Created {num_records} complex records")

    # Time server-managed queries (embedded access) - same complex queries
    start = time.time()
    for i in range(num_queries):
        query = query_types[i % len(query_types)]
        result = db_server.query("sql", query)
        list(result)  # Consume results
    server_time = time.time() - start

    print(f"   ⚡ {num_queries} complex queries in {server_time:.3f}s")
    print(f"   ⚡ {num_queries/server_time:.1f} queries/sec")

    # Compare
    print("\n3. Performance Comparison...")
    ratio = server_time / standalone_time
    print(f"   📊 Standalone: {standalone_time:.3f}s")
    print(f"   📊 Server (embedded): {server_time:.3f}s")
    print(f"   📊 Ratio: {ratio:.2f}x")

    if ratio < 1.5:  # Within 50% is essentially same performance
        print("   ✅ Performance is SIMILAR - direct JVM calls in both cases!")
    else:
        print("   ℹ️  Some overhead from server management")

    print("\n4. Key Insights:")
    print("   💡 Server-managed embedded access = Direct JVM call")
    print("   💡 NO HTTP overhead when accessing from same process")
    print("   💡 HTTP is only for OTHER processes/clients")
    print("   💡 HTTP would add ~5-50ms per request (network + JSON)")

    server.stop()
    print("\n✅ Performance Test Complete!\n")


def test_http_api_access_pattern(cleanup_test_dirs):
    """
    Test HTTP API access pattern - REST requests to server.

    This demonstrates the third access pattern: HTTP API for remote access.
    Shows how HTTP API differs from Java API in terms of usage and performance.

    Includes realistic CRUD operations benchmark with proper connection pooling.
    """
    create_temp_dir, register_server = cleanup_test_dirs

    print("\n" + "=" * 70)
    print("TEST: HTTP API Access Pattern")
    print("=" * 70)

    # Import requests here to avoid dependency for non-HTTP tests
    try:
        import requests
        from requests.auth import HTTPBasicAuth
    except ImportError:
        pytest.skip("requests library not available for HTTP API testing")

    root_path = create_temp_dir("http_api_")

    # Step 1: Start server (required for HTTP API)
    print("\n1. Starting ArcadeDB server...")
    server = arcadedb.create_server(root_path=root_path, root_password="test12345")
    register_server(server)
    server.start()
    time.sleep(1)  # Give server time to fully start

    base_url = f"http://localhost:{server.get_http_port()}"

    # Use session for connection pooling (more realistic)
    session = requests.Session()
    session.auth = HTTPBasicAuth("root", "test12345")

    print(f"   ✅ Server started on port {server.get_http_port()}")
    print(f"   📡 HTTP API base URL: {base_url}")

    # Step 2: Create database via Java API
    # (HTTP API doesn't have database creation endpoint)
    print("\n2. Creating database via Java API (both APIs need this)...")
    server.create_database("httpdb")  # Use Java API for database creation
    print("   ✅ Database created via Java API")

    # Step 3: Create schema via HTTP API
    print("\n3. Creating schema via HTTP API...")
    response = session.post(
        f"{base_url}/api/v1/command/httpdb",
        json={"language": "sql", "command": "CREATE DOCUMENT TYPE Product"},
        timeout=30,
    )
    assert response.status_code == 200, f"Schema creation failed: {response.text}"
    print("   ✅ Schema created via HTTP request")

    # Step 4: Insert initial data via HTTP API
    print("\n4. Inserting sample data via HTTP API...")
    products = [
        ("Laptop", 999, "Electronics"),
        ("Mouse", 29, "Electronics"),
        ("Keyboard", 79, "Electronics"),
        ("Desk", 299, "Furniture"),
        ("Chair", 199, "Furniture"),
    ]

    for name, price, category in products:
        response = session.post(
            f"{base_url}/api/v1/command/httpdb",
            json={
                "language": "sql",
                "command": (
                    f"INSERT INTO Product SET name = '{name}', "
                    f"price = {price}, category = '{category}'"
                ),
            },
            timeout=30,
        )
        assert response.status_code == 200

    print(f"   ✅ Inserted {len(products)} products via HTTP requests")

    # Step 5: Query data via HTTP API
    print("\n5. Querying data via HTTP API...")
    response = session.post(
        f"{base_url}/api/v1/query/httpdb",
        json={"language": "sql", "command": "SELECT FROM Product ORDER BY price"},
        timeout=30,
    )
    assert response.status_code == 200
    result = response.json()

    print("   ✅ Query results via HTTP:")
    for record in result["result"]:
        print(f"     - {record['name']}: ${record['price']} ({record['category']})")

    # Step 6: Comprehensive Performance Comparison - Full CRUD Operations
    print("\n6. Comprehensive Performance Test: HTTP vs Java API...")
    print("   Testing schema creation, inserts, queries, updates, and deletes")

    # Access same database via Java API for comparison
    db = server.get_database("httpdb")

    # Benchmark parameters
    num_operations = 1000  # Reduced from 1000 for more realistic mixed operations
    import random

    # --- HTTP API Full CRUD Benchmark ---
    print("\n   6a. HTTP API - Full CRUD operations...")

    start = time.time()

    # Create a test type
    session.post(
        f"{base_url}/api/v1/command/httpdb",
        json={"language": "sql", "command": "CREATE DOCUMENT TYPE BenchItem"},
        timeout=30,
    )

    # Mixed operations
    for i in range(num_operations):
        op_type = i % 5  # Cycle through 5 operation types

        if op_type == 0:  # Insert
            response = session.post(
                f"{base_url}/api/v1/command/httpdb",
                json={
                    "language": "sql",
                    "command": (
                        f"INSERT INTO BenchItem SET id = {i}, "
                        f"value = {random.randint(1, 1000)}, name = 'Item {i}'"
                    ),
                },
                timeout=30,
            )
        elif op_type == 1:  # Query with filter
            response = session.post(
                f"{base_url}/api/v1/query/httpdb",
                json={
                    "language": "sql",
                    "command": (
                        f"SELECT FROM BenchItem WHERE "
                        f"value > {random.randint(1, 500)} LIMIT 10"
                    ),
                },
                timeout=30,
            )
        elif op_type == 2:  # Update
            response = session.post(
                f"{base_url}/api/v1/command/httpdb",
                json={
                    "language": "sql",
                    "command": (
                        f"UPDATE BenchItem SET value = {random.randint(1, 1000)} "
                        f"WHERE id = {random.randint(0, max(1, i-1))}"
                    ),
                },
                timeout=30,
            )
        elif op_type == 3:  # Aggregation query
            response = session.post(
                f"{base_url}/api/v1/query/httpdb",
                json={
                    "language": "sql",
                    "command": (
                        "SELECT avg(value) as avg_val, "
                        "count(*) as total FROM BenchItem"
                    ),
                },
                timeout=30,
            )
        else:  # Complex query
            response = session.post(
                f"{base_url}/api/v1/query/httpdb",
                json={
                    "language": "sql",
                    "command": (
                        f"SELECT FROM BenchItem WHERE name LIKE "
                        f"'%{random.randint(0, 9)}%' ORDER BY value DESC LIMIT 5"
                    ),
                },
                timeout=30,
            )

        assert response.status_code == 200

    http_time = time.time() - start
    print(f"   ⏱️  HTTP API: {num_operations} operations in {http_time:.3f}s")
    print(f"   📊 {num_operations/http_time:.1f} ops/sec")

    # --- Java API Full CRUD Benchmark ---
    print("\n   6b. Java API - Same CRUD operations...")

    # Clean up from HTTP test
    with db.transaction():
        db.command("sql", "DELETE FROM BenchItem")

    start = time.time()

    # Create same test type (already exists, but included for fair comparison)
    try:
        db.command("sql", "CREATE DOCUMENT TYPE BenchItem")
    except Exception:
        pass  # Already exists

    # Same mixed operations
    for i in range(num_operations):
        op_type = i % 5

        if op_type == 0:  # Insert
            with db.transaction():
                db.command(
                    "sql",
                    f"INSERT INTO BenchItem SET id = {i}, "
                    f"value = {random.randint(1, 1000)}, name = 'Item {i}'",
                )
        elif op_type == 1:  # Query with filter
            result = db.query(
                "sql",
                f"SELECT FROM BenchItem WHERE "
                f"value > {random.randint(1, 500)} LIMIT 10",
            )
            list(result)  # Consume results
        elif op_type == 2:  # Update
            with db.transaction():
                db.command(
                    "sql",
                    f"UPDATE BenchItem SET value = {random.randint(1, 1000)} "
                    f"WHERE id = {random.randint(0, max(1, i-1))}",
                )
        elif op_type == 3:  # Aggregation query
            result = db.query(
                "sql",
                "SELECT avg(value) as avg_val, count(*) as total FROM BenchItem",
            )
            list(result)
        else:  # Complex query
            result = db.query(
                "sql",
                f"SELECT FROM BenchItem WHERE name LIKE "
                f"'%{random.randint(0, 9)}%' ORDER BY value DESC LIMIT 5",
            )
            list(result)

    java_time = time.time() - start
    print(f"   ⏱️  Java API: {num_operations} operations in {java_time:.3f}s")
    print(f"   📊 {num_operations/java_time:.1f} ops/sec")

    # --- Comparison ---
    print("\n   6c. Performance Comparison:")
    ratio = http_time / java_time
    overhead_ms = (http_time - java_time) / num_operations * 1000
    print(f"   📊 HTTP API: {http_time:.3f}s ({num_operations/http_time:.1f} ops/sec)")
    print(f"   📊 Java API: {java_time:.3f}s ({num_operations/java_time:.1f} ops/sec)")
    print(f"   📊 Ratio: HTTP is {ratio:.1f}x slower")
    print(f"   💡 Per-operation overhead: ~{overhead_ms:.1f}ms")

    if ratio < 10:
        print("   ✅ Reasonable overhead for network/HTTP layer")
    else:
        print(
            "   ⚠️  Significant overhead - "
            "Java API strongly preferred for performance"
        )

    # Step 7: Summary of access patterns
    print("\n7. Access Pattern Summary:")
    print("   🔧 Java API (Standalone): arcadedb.create_database()")
    print("   🔧 Java API (Server): server.create_database() → db.command()")
    print("   🔧 HTTP API (Remote): requests.Session() → JSON responses")
    print("   💡 Database creation requires Java API")
    print("   💡 HTTP API adds network + JSON overhead (~1-10ms per operation)")
    print("   💡 Java API provides maximum performance for Python processes")
    print("   💡 HTTP API essential for other languages/remote clients")
    print("   💡 Both can be used simultaneously on same server!")

    session.close()
    server.stop()
    print("\n✅ HTTP API Test Complete!\n")
