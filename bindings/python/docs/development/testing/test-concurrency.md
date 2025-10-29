# Concurrency Tests

The `test_concurrency.py` file contains **4 tests** that explain ArcadeDB's concurrency model and file locking behavior.

[View source code](https://github.com/humemai/arcadedb/blob/python-embedded/bindings/python/tests/test_concurrency.py){ .md-button }

## Overview

!!! question "The Key Question"
    **"Can multiple Python instances access the same database?"**

    **Short Answer:**

    - ❌ Multiple **processes** cannot (file lock prevents it)
    - ✅ Multiple **threads** can (thread-safe within same process)
    - ✅ Use **server mode** for true multi-process access

These tests demonstrate:

- File locking mechanism (OS-level locks)
- Thread safety within a single process
- Sequential access patterns (open → close → reopen)
- Multi-process limitations and solutions

## Why This Matters

Understanding ArcadeDB's concurrency model is critical for:

- **Deployment architecture**: Knowing when to use embedded vs server mode
- **Multi-process applications**: Understanding limitations and workarounds
- **Thread safety**: Confidently using threads with shared database access
- **Performance optimization**: Choosing the right access pattern

## Test Cases

### 1. File Lock Mechanism

**Test:** `test_file_lock_mechanism`

Demonstrates that ArcadeDB uses OS-level file locks to prevent concurrent access from multiple processes.

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded.exceptions import ArcadeDBError

# Process 1: Create and hold lock
db1 = arcadedb.create_database("./test_db")

# Process 2: Try to open same database (this would be in another Python process)
try:
    db2 = arcadedb.open_database("./test_db")
    # This will fail with LockException
except ArcadeDBError as e:
    print(f"Expected error: {e}")
    # Error message contains: "LockException" or "already locked"

db1.close()
```

**What happens:**

1. First database open acquires an OS-level file lock
2. Second attempt to open fails immediately
3. Lock is released when database is closed
4. Java throws `LockException`, wrapped as `ArcadeDBError` in Python

**Why it exists:**

- Prevents database corruption from concurrent writes
- Ensures data consistency
- Standard practice for embedded databases (SQLite does the same)

!!! warning "Multi-Process Access"
    You **cannot** open the same database from multiple Python processes simultaneously.

    ```python
    # ❌ This WILL NOT work
    # process_1.py
    db = arcadedb.create_database("./mydb")  # Locks the database

    # process_2.py (running simultaneously)
    db = arcadedb.open_database("./mydb")    # FAILS with LockException!
    ```

**Solution:** Use [server mode](test-server-patterns.md) for multi-process access.

---

### 2. Thread-Safe Operations

**Test:** `test_thread_safe_operations`

Demonstrates that multiple threads can safely access the same database instance.

```python
import threading
import arcadedb_embedded as arcadedb

# Create database
db = arcadedb.create_database("./test_db")
db.command("sql", "CREATE DOCUMENT TYPE Counter")

# Insert initial record
with db.transaction():
    db.command("sql", "INSERT INTO Counter SET value = 0")

def increment_counter(thread_id, iterations):
    """Each thread increments the counter"""
    for i in range(iterations):
        with db.transaction():
            # Read current value
            result = db.query("sql", "SELECT FROM Counter")
            current = list(result)[0].get_property("value")

            # Increment
            db.command("sql", f"UPDATE Counter SET value = {current + 1}")

# Create multiple threads
threads = []
num_threads = 5
iterations_per_thread = 10

for i in range(num_threads):
    thread = threading.Thread(
        target=increment_counter,
        args=(i, iterations_per_thread)
    )
    threads.append(thread)
    thread.start()

# Wait for all threads to complete
for thread in threads:
    thread.join()

# Verify final count
result = db.query("sql", "SELECT FROM Counter")
final_value = list(result)[0].get_property("value")

# Should be num_threads * iterations_per_thread
assert final_value == 50

db.close()
```

**What it tests:**

- Multiple threads accessing the same `Database` instance
- Concurrent read/write operations
- Transaction isolation between threads
- No race conditions or data corruption

**Key insight:**

✅ **ArcadeDB is thread-safe!** Multiple threads in the same process can safely share a database instance.

!!! success "Thread Safety"
    The Java ArcadeDB engine handles internal synchronization, so you can confidently use the same database object across multiple threads.

    ```python
    # ✅ This WORKS perfectly
    db = arcadedb.create_database("./mydb")

    def worker():
        with db.transaction():
            db.command("sql", "INSERT INTO MyType SET data = 'value'")

    # Multiple threads share the same db instance
    threads = [Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    ```

**Best practice:**

Use a single database instance shared across threads:

```python
# Good: Share one database instance
db = arcadedb.create_database("./mydb")

def thread_worker():
    result = db.query("sql", "SELECT FROM MyType")
    # Process results...

threads = [Thread(target=thread_worker) for _ in range(10)]
```

Don't create separate database instances per thread (unnecessary overhead):

```python
# Avoid: Don't do this
def thread_worker():
    db = arcadedb.open_database("./mydb")  # ❌ Creates separate instance
    result = db.query("sql", "SELECT FROM MyType")
    db.close()
```

---

### 3. Sequential Access Pattern

**Test:** `test_sequential_access`

Demonstrates that a database can be closed and reopened sequentially.

```python
import arcadedb_embedded as arcadedb

# First: Create and populate
db1 = arcadedb.create_database("./test_db")
db1.command("sql", "CREATE DOCUMENT TYPE Person")

with db1.transaction():
    db1.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

# Close to release file lock
db1.close()

# Second: Reopen and verify data persisted
db2 = arcadedb.open_database("./test_db")

result = db2.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
person = list(result)[0]

assert person.get_property("name") == "Alice"
assert person.get_property("age") == 30

db2.close()
```

**What it tests:**

- Database persistence across close/reopen cycles
- File lock release on `close()`
- Data integrity after reopening
- Proper resource cleanup

**Common pattern:**

This is useful for:

1. **Batch processing**: Open → process → close → repeat
2. **Migration to server**: Create & populate → close → move to server directory
3. **Temporary exclusive access**: Open → do work → close (release lock)

**Important:**

```python
# Pattern: Create → Use → Close → Reopen
db = arcadedb.create_database("./mydb")
# ... do work ...
db.close()  # ⚠️ Must close to release lock

# Now someone else (or same process) can open it
db2 = arcadedb.open_database("./mydb")  # ✅ Works!
```

---

### 4. Multi-Process Limitations

**Test:** `test_multiprocess_limitation`

Explicitly demonstrates why multiple processes cannot access the same database directly, and shows the solution.

```python
import arcadedb_embedded as arcadedb
from arcadedb_embedded.exceptions import ArcadeDBError
import subprocess
import sys

# Create test database
db = arcadedb.create_database("./test_db")
db.command("sql", "CREATE DOCUMENT TYPE Data")
with db.transaction():
    db.command("sql", "INSERT INTO Data SET value = 'test'")
db.close()

# Attempt to access from subprocess
script = """
import arcadedb_embedded as arcadedb
try:
    db = arcadedb.open_database("./test_db")
    print("SUCCESS")
    db.close()
except Exception as e:
    print(f"FAILED: {e}")
"""

# This subprocess will fail if main process holds lock
result = subprocess.run(
    [sys.executable, "-c", script],
    capture_output=True,
    text=True
)

print("Subprocess output:", result.stdout)
```

**Why this fails:**

```
Process 1 (main):     db1 = open("./mydb")  🔒 Lock acquired
                      |
                      | (both processes running)
                      |
Process 2 (subprocess): db2 = open("./mydb")  ❌ LockException!
```

**The solution: Server Mode**

```python
# Solution: Use server mode for multi-process access

# Main process: Start server
import arcadedb_embedded as arcadedb

server = arcadedb.create_server(root_path="./databases")
server.start()

# Create database through server
db = server.create_database("mydb")

# Now you have TWO ways to access:

# 1. Embedded access (same process) - Fast, no HTTP
db.query("sql", "SELECT FROM MyType")

# 2. HTTP access (other processes) - Via HTTP API
import requests
response = requests.post(
    'http://localhost:2480/api/v1/query/mydb',
    json={
        'language': 'sql',
        'command': 'SELECT FROM MyType'
    },
    auth=('root', 'your_password')
)
```

**Key insight:**

!!! tip "Multi-Process Architecture"
    For multi-process applications:

    1. **Start ArcadeDB server** in one process
    2. **Access via HTTP** from other processes
    3. **Or**: Use embedded access in server process + HTTP for others

    See [Server Patterns](test-server-patterns.md) for detailed guide.

## Summary Table

| Scenario | Supported? | Notes |
|----------|------------|-------|
| Multiple threads, same process | ✅ Yes | Thread-safe, share database instance |
| Sequential: open → close → reopen | ✅ Yes | Must close to release lock |
| Multiple processes, embedded mode | ❌ No | File lock prevents concurrent access |
| Multiple processes, server mode | ✅ Yes | Use HTTP API for additional processes |
| Server-managed embedded + HTTP | ✅ Yes | Best of both worlds |

## Running These Tests

```bash
# Run all concurrency tests
pytest tests/test_concurrency.py -v

# Run specific test
pytest tests/test_concurrency.py::test_file_lock_mechanism -v
pytest tests/test_concurrency.py::test_thread_safe_operations -v

# Run with output to see details
pytest tests/test_concurrency.py -v -s
```

## Best Practices

### ✅ DO: Use Threads for Parallelism

```python
db = arcadedb.create_database("./mydb")

def worker(worker_id):
    with db.transaction():
        db.command("sql", f"INSERT INTO Data SET worker = {worker_id}")

threads = [Thread(target=worker, args=(i,)) for i in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()

db.close()
```

### ✅ DO: Use Server Mode for Multi-Process

```python
# Process 1: Start server
server = arcadedb.create_server(root_path="./databases")
server.start()
db = server.create_database("mydb")

# Process 2+: Use HTTP API
# (See Server Patterns documentation)
```

### ✅ DO: Close Between Sequential Opens

```python
# Process 1
db = arcadedb.create_database("./mydb")
# ... work ...
db.close()  # Release lock

# Process 2 (later, or different script)
db = arcadedb.open_database("./mydb")  # Works!
```

### ❌ DON'T: Try Concurrent Process Access

```python
# ❌ This will fail
# script1.py
db1 = arcadedb.create_database("./mydb")
# keeps running...

# script2.py (simultaneously)
db2 = arcadedb.open_database("./mydb")  # LockException!
```

### ❌ DON'T: Create Multiple DB Instances in Threads

```python
# ❌ Inefficient - creates unnecessary instances
def worker():
    db = arcadedb.open_database("./mydb")  # Separate instance
    # ... work ...
    db.close()

# ✅ Better - share one instance
db = arcadedb.open_database("./mydb")

def worker():
    # Use shared db instance
    result = db.query("sql", "SELECT ...")
```

## Related Documentation

- [Server Tests](test-server.md) - Server mode basics
- [Server Patterns](test-server-patterns.md) - Combining embedded + HTTP access
- [Concurrency Guide](../../guide/server.md#multi-process-access) - User guide for concurrency
- [Database API](../../api/database.md) - Database class reference
- [Server API](../../api/server.md) - Server class reference

## Troubleshooting

### "Database is locked" Error

```python
# Error: LockException: database ./mydb is already locked
```

**Cause:** Another process (or unclosed instance) holds the file lock.

**Solutions:**

1. Close the other database instance: `db.close()`
2. Check for zombie processes holding locks
3. Use server mode for multi-process access
4. Restart if lock file is orphaned

### Thread Safety Concerns

**Q:** Is it safe to share a database instance across threads?

**A:** Yes! ArcadeDB handles internal synchronization. You can safely share a `Database` instance across multiple threads.

**Q:** Do I need to synchronize access to the database?

**A:** No, unless you need application-level coordination (e.g., ensuring specific order of operations). The database itself is thread-safe.

### Performance with Threads

**Q:** Should I use one database instance or multiple?

**A:** Use **one shared instance**. Creating multiple instances adds overhead with no benefit since they'll all access the same underlying database through Java.

## Further Reading

- **Wikipedia: File Locking** - Background on OS-level locks
- **SQLite Locking** - Similar approach in SQLite
- **Java Synchronization** - How ArcadeDB handles thread safety internally
