# Server Tests

The `test_server.py` file contains **4 tests** covering basic server functionality.

[View source code](https://github.com/humemai/arcadedb/blob/python-embedded/bindings/python/tests/test_server.py){ .md-button }

## Overview

These tests validate:

- ✅ Server creation and startup
- ✅ Database operations through server (Java API)
- ✅ Custom configuration
- ✅ Context manager usage

For advanced server patterns (embedded + HTTP), see [Server Patterns](test-server-patterns.md).

## Quick Example

```python
import arcadedb_embedded as arcadedb

# Create and start server
server = arcadedb.create_server(
    root_path="./databases",
    root_password="mypassword"
)
server.start()

# Create database
db = server.create_database("mydb")

# Use it
db.command("sql", "CREATE DOCUMENT TYPE Person")
with db.transaction():
    db.command("sql", "INSERT INTO Person SET name = 'Alice'")

# Query
result = db.query("sql", "SELECT FROM Person")
for person in result:
    print(person.get_property("name"))

# Stop server
server.stop()
```

## Test Cases

### 1. Server Creation and Startup

**Test:** `test_server_creation`

```python
server = arcadedb.create_server(root_path="./databases")
server.start()

assert server.is_started()

server.stop()
assert not server.is_started()
```

### 2. Database Operations Through Server

**Test:** `test_server_database_operations`

```python
with arcadedb.create_server(root_path="./databases") as server:
    server.start()

    # Create database through server
    db = server.create_database("testdb")

    # Use Java API for operations
    db.command("sql", "CREATE DOCUMENT TYPE Person")
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Alice'")

    # Query
    result = db.query("sql", "SELECT FROM Person")
    for person in result:
        print(person.get_property("name"))
```

### 3. Custom Configuration

**Test:** `test_server_custom_config`

```python
server = arcadedb.create_server(
    root_path="./databases",
    root_password="secure_password",
    config={
        "http_port": 8080,
        "host": "127.0.0.1",
        "mode": "production"
    }
)
server.start()

assert server.get_http_port() == 8080
```

### 4. Context Manager

**Test:** `test_server_context_manager`

```python
with arcadedb.create_server(root_path="./databases") as server:
    server.start()
    # Server automatically stopped on exit
```

## Running These Tests

```bash
# Run all server tests
pytest tests/test_server.py -v

# Run specific test
pytest tests/test_server.py::test_server_creation -v
```

!!! note "Distribution Support"
    Server tests are skipped in the **headless** distribution (no server module).

## Related Documentation

- [Server Patterns](test-server-patterns.md) - Advanced patterns
- [Server API Reference](../../api/server.md)
- [Server Guide](../../guide/server.md)
- [Concurrency Tests](test-concurrency.md) - Multi-process access
