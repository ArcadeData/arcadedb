# Quick Start

Get up and running with ArcadeDB Python bindings in 5 minutes!

## Installation

First, install the headless distribution (recommended for getting started):

```bash
pip install arcadedb-embedded-headless
```

!!! warning "Java Required"
    Make sure you have Java 21+ installed:
    ```bash
    java -version
    ```
    If not installed, see [Installation Guide](installation.md#java-runtime-environment-jre).

## Access Methods

ArcadeDB Python bindings support **two access methods**:

### Java API (Embedded Mode) - Recommended for Getting Started

Direct JVM method calls via JPype - fastest performance:

```python
import arcadedb_embedded as arcadedb

# Direct database access (Java API)
with arcadedb.create_database("/tmp/mydb") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Person")  # Direct JVM call
    result = db.query("sql", "SELECT FROM Person")     # Direct JVM call
```

### HTTP API (Server Mode) - For Remote Access

REST requests when server is running - enables remote access:

```python
import requests
from requests.auth import HTTPBasicAuth

# First start server with Java API
server = arcadedb.create_server("/tmp/server", "password123")
server.start()

# Then use HTTP API for remote access
auth = HTTPBasicAuth("root", "password123")
response = requests.post(
    f"http://localhost:{server.get_http_port()}/api/v1/command/mydb",
    auth=auth,
    json={"language": "sql", "command": "SELECT FROM Person"}
)
result = response.json()
```

!!! tip "Choose Your Method"
    - **Java API**: Use for single-process apps (fastest)
    - **HTTP API**: Use for multi-process/remote access
    - **Both**: Can be used simultaneously on same server!

## Your First Database

### 1. Create a Database

```python
import arcadedb_embedded as arcadedb

# Create database (context manager for automatic cleanup)
with arcadedb.create_database("/tmp/quickstart") as db:
    print(f"Created database at: {db.get_database_path()}")
```

### 2. Create Schema

```python
with arcadedb.create_database("/tmp/quickstart") as db:
    # Create a document type
    db.command("sql", "CREATE DOCUMENT TYPE Person")

    # Create a property
    db.command("sql", "ALTER TYPE Person CREATE PROPERTY name STRING")
    db.command("sql", "ALTER TYPE Person CREATE PROPERTY age INTEGER")

    print("Schema created!")
```

### 3. Insert Data

All writes must be in a transaction:

```python
with arcadedb.create_database("/tmp/quickstart") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Person")

    # Use transaction for writes
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")
        db.command("sql", "INSERT INTO Person SET name = 'Bob', age = 25")
        db.command("sql", "INSERT INTO Person SET name = 'Charlie', age = 35")

    print("Inserted 3 records")
```

!!! tip "Transactions"
    Always use `with db.transaction():` for INSERT, UPDATE, DELETE operations.

### 4. Query Data

```python
with arcadedb.create_database("/tmp/quickstart") as db:
    # Setup (abbreviated)
    db.command("sql", "CREATE DOCUMENT TYPE Person")
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")
        db.command("sql", "INSERT INTO Person SET name = 'Bob', age = 25")

    # Query data
    result = db.query("sql", "SELECT FROM Person WHERE age > 25")

    for record in result:
        name = record.get_property('name')
        age = record.get_property('age')
        print(f"Name: {name}, Age: {age}")
```

Output:
```
Name: Alice, Age: 30
```

## Complete Example

Here's a complete working example:

```python
import arcadedb_embedded as arcadedb

def main():
    # Create database
    with arcadedb.create_database("/tmp/quickstart") as db:
        # Create schema
        db.command("sql", "CREATE DOCUMENT TYPE Person")
        db.command("sql", "ALTER TYPE Person CREATE PROPERTY name STRING")
        db.command("sql", "ALTER TYPE Person CREATE PROPERTY age INTEGER")
        db.command("sql", "CREATE INDEX Person_name ON Person (name) NOTUNIQUE")

        # Insert data (in transaction)
        with db.transaction():
            db.command("sql", """
                INSERT INTO Person SET
                    name = 'Alice',
                    age = 30,
                    email = 'alice@example.com'
            """)
            db.command("sql", """
                INSERT INTO Person SET
                    name = 'Bob',
                    age = 25,
                    email = 'bob@example.com'
            """)
            db.command("sql", """
                INSERT INTO Person SET
                    name = 'Charlie',
                    age = 35,
                    email = 'charlie@example.com'
            """)

        print("✅ Inserted 3 records")

        # Query all
        print("\n📋 All people:")
        result = db.query("sql", "SELECT FROM Person ORDER BY age")
        for record in result:
            print(f"  - {record.get_property('name')}, age {record.get_property('age')}")

        # Query with filter
        print("\n🔍 People over 25:")
        result = db.query("sql", "SELECT FROM Person WHERE age > 25 ORDER BY age")
        for record in result:
            print(f"  - {record.get_property('name')}, age {record.get_property('age')}")

        # Count
        result = db.query("sql", "SELECT count(*) as total FROM Person")
        total = result[0].get_property('total')
        print(f"\n📊 Total people: {total}")

if __name__ == "__main__":
    main()
```

Output:
```
✅ Inserted 3 records

📋 All people:
  - Bob, age 25
  - Alice, age 30
  - Charlie, age 35

🔍 People over 25:
  - Alice, age 30
  - Charlie, age 35

📊 Total people: 3
```

## Key Concepts

### Context Managers

Always use `with` statements for automatic cleanup:

```python
# ✅ Good - automatic cleanup
with arcadedb.create_database("/tmp/mydb") as db:
    # Use database
    pass
# Database automatically closed

# ❌ Avoid - manual cleanup required
db = arcadedb.create_database("/tmp/mydb")
# Use database
db.close()  # Easy to forget!
```

### Transactions

All writes require a transaction:

```python
# ✅ Good - in transaction
with db.transaction():
    db.command("sql", "INSERT INTO Person SET name = 'Alice'")

# ❌ Will fail - no transaction
db.command("sql", "INSERT INTO Person SET name = 'Alice'")
```

!!! info "Read-Only Operations"
    `db.query()` doesn't require a transaction - only `db.command()` for writes.

### Query Languages

ArcadeDB supports multiple query languages:

=== "SQL"

    ```python
    result = db.query("sql", "SELECT FROM Person WHERE age > 25")
    ```

=== "Cypher"

    ```python
    result = db.query("cypher", "MATCH (p:Person) WHERE p.age > 25 RETURN p")
    ```

=== "MongoDB"

    ```python
    result = db.query("mongo", "{ find: 'Person', filter: { age: { $gt: 25 } } }")
    ```

=== "Gremlin (Full only)"

    ```python
    # Only in arcadedb-embedded (full distribution)
    result = db.query("gremlin", "g.V().has('Person', 'age', gt(25))")
    ```

## Next Steps

Now that you've created your first database, explore more features:

<div class="grid cards" markdown>

-   :material-database:{ .lg .middle } [__Core Operations__](../guide/core/database.md)

    Learn about database management, queries, and transactions

-   :material-vector-triangle:{ .lg .middle } [__Vector Search__](../guide/vectors.md)

    Store and query embeddings with HNSW indexing

-   :material-graph:{ .lg .middle } [__Graph Operations__](../guide/graphs.md)

    Work with vertices, edges, and graph traversals

-   :material-upload:{ .lg .middle } [__Import Data__](../guide/import.md)

    Bulk import from CSV, JSON, Neo4j

</div>

## Common Patterns

### Working with Existing Database

```python
# Open existing database
with arcadedb.open_database("/tmp/quickstart") as db:
    result = db.query("sql", "SELECT FROM Person")
    print(f"Found {len(result)} records")
```

### Batch Inserts

```python
with db.transaction():
    for i in range(100):
        db.command("sql", f"INSERT INTO Person SET name = 'User{i}', age = {20 + i}")
```

### Error Handling

```python
from arcadedb_embedded import ArcadeDBError

try:
    with arcadedb.create_database("/tmp/mydb") as db:
        db.command("sql", "INVALID SQL")
except ArcadeDBError as e:
    print(f"Database error: {e}")
```

## Need Help?

- **Examples**: Check [Examples](../examples/basic.md) for more code samples
- **API Reference**: See [Database API](../api/database.md) for all methods
- **Troubleshooting**: Visit [Troubleshooting Guide](../development/troubleshooting.md)
