# Server Pattern Tests

The `test_server_patterns.py` file contains **6 tests** demonstrating different ways to access ArcadeDB via server.

[View source code](https://github.com/humemai/arcadedb/blob/python-embedded/bindings/python/tests/test_server_patterns.py){ .md-button }

## When Do You Need Server Patterns?

**Use pure embedded (no server needed):**

```python
# Simple embedded - fastest, no server overhead
import arcadedb_embedded as arcadedb

with arcadedb.create_database("./mydb") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Person")
    # ... work ...
```

**Use server patterns when you need:**

- **HTTP API access** - Other processes/languages connecting remotely
- **Studio UI** - Web interface for database exploration
- **Multi-process access** - Multiple applications sharing the database
- **Remote access** - Database on different machine

## Overview

These tests demonstrate **three access patterns**:

1. **Java API (Standalone)**: Direct database access (no server) - `arcadedb.create_database()`
2. **Java API (Server-managed)**: Access via server (embedded) - `server.create_database()`
3. **HTTP API (Remote)**: REST requests to server - `requests.post()`

**Important**: If you only need embedded access and don't need HTTP API or Studio UI, you can skip the server entirely and just use `arcadedb.create_database("./mydb")`.

**Key insight**: Server supports BOTH Java API (embedded) and HTTP API (remote) simultaneously.

## Test Cases

### 1. Server Pattern: Create Through Server (Recommended)

**Test:** `test_server_pattern_recommended`

This demonstrates the **recommended pattern when you need server functionality**:

1. Start server first
2. Create database through server
3. Use Java API for embedded access (fastest)
4. HTTP API automatically available for other processes

**Benefits:**

- ✅ No manual lock management
- ✅ Embedded access = direct JVM calls (no HTTP overhead)
- ✅ HTTP access available for remote clients
- ✅ Simpler code, fewer steps

### 2. Thread Safety

**Test:** `test_server_thread_safety`

Demonstrates that multiple threads can safely access server-managed databases simultaneously.

**What it tests:**

- Multiple threads accessing server-managed database
- Concurrent queries
- Server's internal synchronization

### 3. Context Manager Usage

**Test:** `test_server_context_manager`

Shows proper use of context managers for automatic server lifecycle management.

### 4. Advanced Pattern: Embedded First → Server

**Test:** `test_pattern1_embedded_first_requires_close`

**When to use:**

- Pre-populating a database before exposing it
- Migrating existing embedded database to server mode
- Setup scripts that initialize schema/data

**Critical requirement:** Must `close()` database before starting server!

```python
import arcadedb_embedded as arcadedb
import shutil
import os

# Step 1: Create database with embedded API
db = arcadedb.create_database("./temp_db")

# Step 2: Populate schema and data
db.command("sql", "CREATE DOCUMENT TYPE Person")
with db.transaction():
    db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")
    db.command("sql", "INSERT INTO Person SET name = 'Bob', age = 25")

# Step 3: ⚠️ CRITICAL - Must close to release file lock!
db.close()

# Step 4: Move database to server's databases directory
os.makedirs("./databases", exist_ok=True)
shutil.move("./temp_db", "./databases/mydb")

# Step 5: Start server
server = arcadedb.create_server(root_path="./databases")
server.start()

# Step 6: Access through server
db = server.get_database("mydb")

# Verify data is there
result = db.query("sql", "SELECT FROM Person")
people = list(result)
assert len(people) == 2

# Clean up
server.stop()
```

**Why `close()` is required:**

```
Your Python process:
  db = create_database("./temp_db")  🔒 Lock acquired
  # ... work ...
  db.close()                         🔓 Lock released

  server.start()
  server.get_database("mydb")        🔒 Server acquires lock ✅

Without close():
  db = create_database("./temp_db")  🔒 Lock acquired
  # ... work ...
  # Forgot db.close()!              🔒 Still locked

  server.start()
  server.get_database("mydb")        ❌ LockException!
```

!!! warning "Common Mistake"
    Forgetting to call `db.close()` will cause the server to fail when trying to open the database.

    ```python
    db = arcadedb.create_database("./mydb")
    # ... populate ...
    # ❌ Forgot db.close()!

    server = arcadedb.create_server(root_path="./databases")
    server.start()
    db = server.get_database("mydb")  # ❌ LockException!
    ```

### 5. Performance: Embedded Standalone vs Server-Managed

**Test:** `test_embedded_performance_comparison`

Proves that server-managed embedded access has **virtually no performance penalty** compared to standalone embedded mode.

**Test setup:**

- 5,000 complex records (multiple fields, varied data types)
- 1,000 queries (mix of filters, aggregations, complex conditions)
- Same workload for both patterns

**Typical results:**

```text
Standalone: 5.918s (169.0 queries/sec)
Server-managed: 5.830s (171.5 queries/sec)
Ratio: 0.99x
```

**Conclusion:** Server-managed embedded access is just as fast! It's a direct JVM call, not HTTP.

### 6. Performance: HTTP API vs Java API

**Test:** `test_http_api_access_pattern`

Demonstrates HTTP REST API access for remote clients, with comprehensive CRUD performance comparison.

**Test setup:**

- Full CRUD operations: schema creation, inserts, queries, updates, aggregations
- Uses `requests.Session()` for connection pooling (realistic usage)
- 100 mixed operations for fair comparison

**Typical results:**

```text
HTTP API: 0.830s (1,205 ops/sec)
Java API: 0.151s (6,630 ops/sec)
Ratio: 5.5x slower
Per-operation overhead: ~0.7ms
```

**Key findings:**

- **HTTP is 5-6x slower** (reasonable for network + JSON overhead)
- **Per-operation overhead: ~1ms** (very acceptable for remote access)
- **Use Java API** when in same Python process (6x faster)
- **Use HTTP API** for remote clients, other languages, web tools

**Why the difference?**

- HTTP: Network stack + JSON serialization + HTTP parsing + authentication
- Java API: Direct JVM method call (when in same process)

**When to use each:**

| Scenario | Best Choice | Why |
|----------|-------------|-----|
| Python code accessing DB | Java API | 6x faster, direct JVM calls |
| JavaScript/curl/other tools | HTTP API | Only option, ~1ms overhead acceptable |
| Remote/distributed systems | HTTP API | Designed for this use case |
| Same server, Python process | Java API | No reason to use HTTP |

## Running These Tests

```bash
# Run all server pattern tests
pytest tests/test_server_patterns.py -v

# Run specific pattern test
pytest tests/test_server_patterns.py::test_server_pattern_recommended -v

# Run with output to see timing and detailed results
pytest tests/test_server_patterns.py -s
```

## Best Practices Summary

### ✅ BEST: Pure Embedded (No Server Needed)

```python
# Simplest approach - no server overhead
import arcadedb_embedded as arcadedb

with arcadedb.create_database("./mydb") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Person")
    # ... work ...
# Use this unless you need HTTP API or Studio UI
```

### ✅ IF YOU NEED SERVER: Use Server-First Pattern

```python
# When you need HTTP API or Studio UI
server = arcadedb.create_server(root_path="./databases")
server.start()
db = server.create_database("mydb")

# Use embedded access (still fast - 0.99x performance!)
db.query("sql", "SELECT ...")

# HTTP also available for other processes (5-6x slower, ~1ms overhead)
# http://localhost:2480/api/v1/query/mydb
```

### ✅ DO: Use Context Manager

```python
# Automatic start/stop
with arcadedb.create_server(root_path="./databases") as server:
    server.start()
    db = server.create_database("mydb")
    # ... work ...
# Server automatically stopped
```

### ⚠️ DO (if using embedded-first pattern): Remember to Close

```python
# If you must pre-populate before starting server
db = arcadedb.create_database("./mydb")
# ... populate ...
db.close()  # ⚠️ Don't forget this!

# Then start server
server = arcadedb.create_server(...)
server.start()
```

### ❌ DON'T: Mix Patterns Incorrectly

```python
# ❌ This will fail
db = arcadedb.create_database("./databases/mydb")
# Forgot db.close()!

server = arcadedb.create_server(root_path="./databases")
server.start()
db2 = server.get_database("mydb")  # LockException!
```

## Decision Tree

```text
Do you need HTTP API or Studio UI?
├─ No (most cases)
│  └─ Use pure embedded ⭐ (fastest, simplest)
│     └─ arcadedb.create_database("./mydb")
└─ Yes (multi-process, remote access, web UI)
   ├─ Starting fresh?
   │  └─ Use server-first pattern (create through server)
   └─ Have existing embedded DB?
      └─ Use embedded-first pattern (remember to close!)
```

## Related Documentation

- [Server Tests](test-server.md) - Basic server functionality
- [Concurrency Tests](test-concurrency.md) - Multi-process limitations
- [Server API](../../api/server.md) - Server class reference
- [Server Guide](../../guide/server.md) - User guide for server mode
- [Database API](../../api/database.md) - Database class reference

## Frequently Asked Questions

### Why use server mode at all?

**Benefits:**

1. **HTTP API** - Access from any language/tool
2. **Studio UI** - Web interface for exploration
3. **Multi-process** - Multiple applications can access via HTTP
4. **Management** - Server lifecycle, monitoring, logs

### Does server mode slow down embedded access?

**No!** Embedded access through server is a direct JVM call, not HTTP. Same performance as standalone.

### When should I use Pattern 1 vs Pattern 2?

- **Pattern 2**: Starting fresh, want simplicity → Recommended ⭐
- **Pattern 1**: Have existing embedded DB, need to expose it → Use with caution

### Can I have both embedded and HTTP clients?

**Yes!** That's the whole point of Pattern 2:

- Your Python app: Embedded access (fast)
- Other apps: HTTP API access
- Web users: Studio UI

### Do I need to close server-managed databases?

**No!** Server manages the lifecycle. Just stop the server when done:

```python
server = arcadedb.create_server(...)
server.start()
db = server.create_database("mydb")
# ... work ...
# Don't close db!
server.stop()  # Server handles cleanup
```
