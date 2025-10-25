# Testing Best Practices

Summary of best practices learned from the ArcadeDB Python test suite.

## Database Lifecycle

### ✅ Use Context Managers

```python
# Good: Automatic cleanup
with arcadedb.create_database("./mydb") as db:
    db.query("sql", "SELECT ...")
# Database automatically closed
```

```python
# Also good for servers
with arcadedb.create_server(root_path="./databases") as server:
    server.start()
    db = server.create_database("mydb")
    # ... work ...
# Server automatically stopped
```

### ✅ Close When Done

```python
# If not using context manager, explicit close
db = arcadedb.create_database("./mydb")
try:
    # ... work ...
finally:
    db.close()  # Always close to release lock
```

## Transactions

### ✅ Always Use Transactions for Writes

```python
# Good: Wrapped in transaction
with db.transaction():
    db.command("sql", "INSERT INTO Person SET name = 'Alice'")
    db.command("sql", "UPDATE Person SET age = 30 WHERE name = 'Alice'")
# Auto-commit on success, auto-rollback on exception
```

### ❌ Don't Write Without Transactions

```python
# Bad: No transaction
db.command("sql", "INSERT INTO Person SET name = 'Alice'")  # May fail
```

## Concurrency

### ✅ Use Threads for Parallelism

```python
# Good: Share database instance across threads
db = arcadedb.create_database("./mydb")

def worker():
    result = db.query("sql", "SELECT FROM Data")
    # Process...

threads = [Thread(target=worker) for _ in range(10)]
```

### ✅ Use Server Mode for Multi-Process

```python
# Good: Server mode for multiple processes
server = arcadedb.create_server(root_path="./databases")
server.start()

# Python process: embedded access
db = server.get_database("mydb")

# Other processes: HTTP API
# http://localhost:2480/api/v1/query/mydb
```

### ❌ Don't Try Concurrent Process Access

```python
# Bad: Two processes, same database
# process1.py
db1 = arcadedb.create_database("./mydb")  # Locks

# process2.py (simultaneously)
db2 = arcadedb.open_database("./mydb")    # ❌ LockException!
```

## Server Patterns

### ✅ Prefer Pattern 2 (Server First)

```python
# Recommended: Start server first
server = arcadedb.create_server(root_path="./databases")
server.start()
db = server.create_database("mydb")

# Use embedded access (fast!)
# HTTP also available for other processes
```

### ⚠️ Pattern 1 Requires close()

```python
# If using Pattern 1, MUST close
db = arcadedb.create_database("./mydb")
# ... populate ...
db.close()  # ⚠️ Critical!

# Then start server
server = arcadedb.create_server(...)
```

## Data Import

### ✅ Adjust Batch Size for Performance

```python
# Large files: bigger batches
arcadedb.import_csv(
    db,
    "huge.csv",
    type_name="Data",
    commit_every=10000  # Larger batches
)
```

### ✅ Define Schema Before Import

```python
# Good: Schema first for better performance
db.command("sql", "CREATE DOCUMENT TYPE Person")
db.command("sql", "CREATE PROPERTY Person.age INTEGER")
db.command("sql", "CREATE INDEX ON Person(name)")

# Then import
arcadedb.import_csv(db, "people.csv", type_name="Person")
```

## Query Handling

### ✅ Iterate Results Efficiently

```python
# Good: Iterate directly
result = db.query("sql", "SELECT FROM Person")
for person in result:
    process(person.get_property("name"))
```

### ✅ Convert to List When Needed

```python
# Good when you need all results
result = db.query("sql", "SELECT FROM Person")
people = list(result)
print(f"Found {len(people)} people")
```

## Error Handling

### ✅ Catch ArcadeDBError

```python
from arcadedb_embedded.exceptions import ArcadeDBError

try:
    db.command("sql", "INSERT INTO Person SET name = 'Alice'")
except ArcadeDBError as e:
    print(f"Database error: {e}")
    # Handle error
```

### ✅ Transactions Auto-Rollback

```python
# Good: Exception triggers rollback
try:
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Alice'")
        raise Exception("Something went wrong")
except Exception:
    pass

# Transaction was automatically rolled back
```

## Testing

### ✅ Clean Up Test Databases

```python
import tempfile
import shutil

# Good: Use temp directory
temp_dir = tempfile.mkdtemp()
try:
    db = arcadedb.create_database(f"{temp_dir}/test_db")
    # ... tests ...
    db.close()
finally:
    shutil.rmtree(temp_dir)
```

### ✅ Use Fixtures for Setup/Teardown

```python
import pytest

@pytest.fixture
def db():
    temp_dir = tempfile.mkdtemp()
    database = arcadedb.create_database(f"{temp_dir}/test_db")
    yield database
    database.close()
    shutil.rmtree(temp_dir)

def test_something(db):
    # db is ready to use
    db.command("sql", "CREATE DOCUMENT TYPE Test")
```

## Performance

### ✅ Batch Operations in Transactions

```python
# Good: One transaction for many operations
with db.transaction():
    for i in range(1000):
        db.command("sql", f"INSERT INTO Data SET value = {i}")
```

### ❌ Don't Use Transaction Per Operation

```python
# Bad: 1000 separate transactions
for i in range(1000):
    with db.transaction():
        db.command("sql", f"INSERT INTO Data SET value = {i}")
```

### ✅ Server-Managed Embedded = Fast

```python
# Fast: No HTTP overhead, direct JVM call
server = arcadedb.create_server(root_path="./databases")
server.start()
db = server.create_database("mydb")

# This is as fast as standalone embedded!
result = db.query("sql", "SELECT FROM Data")
```

## Summary Checklist

- [ ] Use context managers for automatic cleanup
- [ ] Wrap writes in transactions
- [ ] Use threads (not processes) for parallelism
- [ ] Use server mode for multi-process access
- [ ] Prefer Pattern 2 (server first) for new projects
- [ ] Pre-create schema for better import performance
- [ ] Batch operations in transactions
- [ ] Clean up test databases
- [ ] Catch ArcadeDBError exceptions
- [ ] Close databases to release locks

## Related Documentation

- [Core Tests](test-core.md)
- [Concurrency Tests](test-concurrency.md)
- [Server Patterns](test-server-patterns.md)
- [Import Tests](test-importer.md)
- [User Guide](../../guide/core/database.md)
