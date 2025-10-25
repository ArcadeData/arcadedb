# Architecture

Technical documentation for the ArcadeDB Python bindings architecture, JPype integration, and implementation details.

## Overview

The ArcadeDB Python bindings are a **thin wrapper** around the ArcadeDB Java library using JPype for JVM integration. This design provides:

- **Full API Coverage**: Access to all ArcadeDB features
- **Performance**: Minimal Python overhead
- **Maintenance**: Automatic feature parity with Java releases
- **Type Safety**: Python type hints with Java type conversion

## Module Structure

```
arcadedb_embedded/
├── __init__.py          # Package initialization, JPype startup
├── core.py              # Database, DatabaseFactory, Vertex, Edge, Document
├── server.py            # ArcadeDBServer, HTTP API
├── importer.py          # Importer, data import utilities
├── vector.py            # VectorIndex, embedding utilities
├── results.py           # ResultSet, Result (query results)
├── transactions.py      # TransactionContext (ACID transactions)
└── exceptions.py        # ArcadeDBError (unified exceptions)
```

### Module Responsibilities

**`__init__.py`**
- JVM startup and configuration
- Package-level exports
- Version management
- Distribution variant detection (headless/minimal/full)

**`core.py`**
- `DatabaseFactory`: Database creation/opening
- `Database`: Main database interface (query, command, transactions)
- `Vertex`, `Edge`, `Document`: Record types
- Schema management (types, properties, indexes)

**`server.py`**
- `ArcadeDBServer`: HTTP server lifecycle
- Studio UI access
- Multi-database support
- Server configuration

**`importer.py`**
- `Importer`: Data import orchestration
- Format handlers (CSV, JSON, Neo4j)
- Batch processing
- Type inference

**`vector.py`**
- `VectorIndex`: HNSW vector indexing
- NumPy ↔ Java array conversion
- Nearest neighbor search
- Distance metrics

**`results.py`**
- `ResultSet`: Query result iteration
- `Result`: Single record wrapper
- Type conversion (Java → Python)
- JSON serialization

**`transactions.py`**
- `TransactionContext`: Transaction management
- Context manager protocol
- ACID guarantees
- Auto-rollback on exception

**`exceptions.py`**
- `ArcadeDBError`: Base exception
- Java exception wrapping
- Error categorization

## JPype Integration

### JVM Lifecycle

```python
# Simplified from __init__.py

def _start_jvm():
    """Initialize JVM with ArcadeDB JARs."""
    if jpype.isJVMStarted():
        return

    # Find JAR files
    jar_path = find_package_jars()

    # Start JVM
    jpype.startJVM(
        classpath=[jar_path],
        convertStrings=False  # Manual string conversion for safety
    )
```

**JVM Startup:**
1. Package installation places JARs in site-packages
2. First import triggers JVM startup
3. JVM remains active for process lifetime
4. Cannot restart JVM in same process

**Implications:**
- JVM settings must be configured before first import
- Server mode requires careful JVM configuration
- Testing requires separate processes for isolation

---

### Type Conversion

**Python → Java:**

```python
# String
python_str = "hello"
java_str = jpype.JString(python_str)

# Array
python_array = [1.0, 2.0, 3.0]
java_array = jpype.JArray(jpype.JFloat)(python_array)

# NumPy → Java (vectors)
import numpy as np
from arcadedb_embedded import to_java_float_array

numpy_array = np.array([1.0, 2.0, 3.0], dtype=np.float32)
java_array = to_java_float_array(numpy_array)
```

**Java → Python:**

```python
# Automatic for primitives
java_int = some_java_method()  # Returns Java int
python_int = int(java_int)      # Automatic conversion

# Manual for complex types
java_list = some_java_method()
python_list = [item for item in java_list]

# Java array → NumPy
from arcadedb_embedded import to_python_array

java_array = vertex.get("embedding")
numpy_array = to_python_array(java_array)
```

**Type Mapping:**

| Python Type | Java Type | Notes |
|-------------|-----------|-------|
| `str` | `String` | Manual with `JString()` |
| `int` | `Integer`/`Long` | Automatic |
| `float` | `Float`/`Double` | Automatic |
| `bool` | `Boolean` | Automatic |
| `None` | `null` | Automatic |
| `list` | `ArrayList` | Manual conversion |
| `dict` | `HashMap` | Manual conversion |
| `np.ndarray` | `float[]` | via `to_java_float_array()` |

---

### Memory Management

**Garbage Collection:**
- Python GC: Manages Python objects
- Java GC: Manages Java objects
- JPype: Bridges both, uses Java GC for wrapped objects

**Best Practices:**

```python
# Good: Explicit cleanup
db = arcadedb.open_database("./mydb")
try:
    # Use database
    pass
finally:
    db.close()

# Better: Context manager
db = arcadedb.open_database("./mydb")
with db.transaction():
    # Work with database
db.close()

# Long-running processes: Periodic GC
import gc
for batch in large_dataset:
    process_batch(batch)
    gc.collect()  # Trigger Python GC
```

**Memory Leaks:**
- Holding references to Java objects prevents GC
- Large ResultSets should be consumed and released
- Server mode: Monitor JVM heap usage

## Class Hierarchy

```
DatabaseFactory (core.py)
    ├─ create_database() → Database
    └─ open_database() → Database

Database (core.py)
    ├─ query() → ResultSet
    ├─ command() → None
    ├─ transaction() → TransactionContext
    ├─ new_vertex() → Vertex
    ├─ new_document() → Document
    ├─ get_importer() → Importer
    └─ create_vector_index() → VectorIndex

Record (Java: MutableDocument)
    ├─ Vertex
    │   ├─ new_edge() → Edge
    │   ├─ get_edges() → Iterator[Edge]
    │   └─ get_vertices() → Iterator[Vertex]
    ├─ Edge
    │   ├─ get_out() → Vertex
    │   └─ get_in() → Vertex
    └─ Document
        ├─ get() → Any
        ├─ set() → None
        └─ save() → None

ResultSet (results.py)
    ├─ __iter__() → Iterator[Result]
    ├─ has_next() → bool
    └─ next() → Result

Result (results.py)
    ├─ get() → Any
    ├─ get_property() → Any
    ├─ to_dict() → dict
    └─ to_json() → str
```

## Threading Model

### Thread Safety

**Database:**
- `Database` instances are **NOT thread-safe**
- Each thread needs its own `Database` instance
- Transactions are thread-local

**Example:**

```python
import threading
import arcadedb_embedded as arcadedb

def worker(db_path, worker_id):
    """Worker thread with own database instance."""
    db = arcadedb.open_database(db_path)

    try:
        with db.transaction():
            vertex = db.new_vertex("Worker")
            vertex.set("id", worker_id)
            vertex.save()
    finally:
        db.close()

# Spawn workers
threads = []
for i in range(5):
    t = threading.Thread(target=worker, args=("./mydb", i))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
```

**Server Mode:**
- `ArcadeDBServer` is thread-safe
- HTTP requests handled by internal thread pool
- Each request gets isolated transaction

---

### Multiprocessing

**Safe:**
- Separate processes with separate JVMs
- No shared state
- Ideal for parallel imports

**Example:**

```python
import multiprocessing as mp
import arcadedb_embedded as arcadedb

def process_chunk(db_path, chunk):
    """Process chunk in separate process."""
    # Each process has own JVM
    db = arcadedb.open_database(db_path)

    with db.transaction():
        for record in chunk:
            vertex = db.new_vertex("Data")
            vertex.set("data", record)
            vertex.save()

    db.close()

# Split work across processes
if __name__ == "__main__":
    chunks = split_data_into_chunks()

    with mp.Pool(processes=4) as pool:
        pool.starmap(process_chunk,
                     [("./mydb", chunk) for chunk in chunks])
```

## Performance Considerations

### Bottlenecks

1. **JVM Boundary Crossing**
   - Cost: ~1-10 μs per Java method call
   - Impact: High-frequency calls (loops)
   - Solution: Batch operations, use Java bulk APIs

2. **Type Conversion**
   - Cost: Varies by type (arrays expensive)
   - Impact: Large data transfers
   - Solution: Minimize conversions, use efficient formats

3. **Transaction Overhead**
   - Cost: ~100 μs per transaction
   - Impact: Many small transactions
   - Solution: Batch into larger transactions

---

### Optimization Strategies

**Batch Operations:**

```python
# Bad: Many small transactions
for record in records:
    with db.transaction():
        vertex = db.new_vertex("Data")
        vertex.set("data", record)
        vertex.save()
# 1000 records = 1000 transactions

# Good: One large transaction
with db.transaction():
    for record in records:
        vertex = db.new_vertex("Data")
        vertex.set("data", record)
        vertex.save()
# 1000 records = 1 transaction
```

**Query Optimization:**

```python
# Bad: N+1 queries
users = db.query("sql", "SELECT FROM User")
for user in users:
    # Separate query per user!
    orders = db.query("sql", f"SELECT FROM Order WHERE user_id = '{user.get('id')}'")

# Good: Single query with traversal
result = db.query("sql", """
    SELECT
        name,
        out('Placed').name as orders
    FROM User
""")
```

**ResultSet Streaming:**

```python
# Bad: Load all results
result = db.query("sql", "SELECT FROM LargeTable")
all_results = list(result)  # Loads everything into memory

# Good: Stream results
result = db.query("sql", "SELECT FROM LargeTable")
for row in result:
    process(row)
    # Only one row in memory at a time
```

---

### Profiling

**Python Side:**

```python
import cProfile
import pstats

def benchmark():
    db = arcadedb.create_database("./bench")
    with db.transaction():
        for i in range(10000):
            vertex = db.new_vertex("Data")
            vertex.set("id", i)
            vertex.save()
    db.close()

# Profile
cProfile.run('benchmark()', 'stats.prof')
stats = pstats.Stats('stats.prof')
stats.sort_stats('cumulative')
stats.print_stats(20)
```

**Java Side:**

```python
# Enable JVM profiling
import jpype

# Before starting JVM (in package __init__.py):
jpype.startJVM(
    classpath=[jar_path],
    convertStrings=False,
    # JVM profiling options:
    "-XX:+PrintGCDetails",
    "-Xloggc:gc.log"
)
```

## Distribution Variants

### Headless

**Size:** ~94 MB
**Features:** SQL, Cypher, HTTP API, Studio UI
**Excludes:** Gremlin, GraphQL, MongoDB syntax

```python
# pip install arcadedb-embedded-headless
import arcadedb_embedded as arcadedb

db = arcadedb.create_database("./mydb")
db.query("sql", "SELECT FROM User")      # ✓
db.query("cypher", "MATCH (n) RETURN n")  # ✓
db.query("gremlin", "g.V()")              # ✗ Error
```

---

### Minimal

**Size:** ~97 MB
**Features:** SQL, Cypher, HTTP API, Studio UI
**Excludes:** Gremlin, GraphQL, MongoDB syntax

*Same as headless with different packaging.*

---

### Full

**Size:** ~158 MB
**Features:** Everything (SQL, Cypher, Gremlin, GraphQL, MongoDB)

```python
# pip install arcadedb-embedded
import arcadedb_embedded as arcadedb

db = arcadedb.create_database("./mydb")
db.query("sql", "SELECT FROM User")              # ✓
db.query("cypher", "MATCH (n) RETURN n")         # ✓
db.query("gremlin", "g.V()")                     # ✓
db.query("graphql", "{users{name}}")             # ✓
db.query("mongodb", "db.User.find()")            # ✓
```

## Extension Points

### Custom Vertex/Edge Classes

```python
from arcadedb_embedded import Database

class CustomVertex:
    """Custom vertex wrapper with helper methods."""

    def __init__(self, java_vertex):
        self._java_vertex = java_vertex

    def get_friends(self):
        """Get all friends (out edges of type 'Knows')."""
        edges = self._java_vertex.getEdges(
            jpype.JClass('com.arcadedata.engine.api.graph.Vertex$DIRECTION').OUT,
            "Knows"
        )
        return [edge.getVertex() for edge in edges]

# Usage with wrapped database
```

### Custom Importers

```python
class CustomImporter:
    """Custom import format handler."""

    def __init__(self, db):
        self.db = db

    def import_xml(self, file_path, vertex_type):
        """Import XML format."""
        import xml.etree.ElementTree as ET

        tree = ET.parse(file_path)
        root = tree.getroot()

        with self.db.transaction():
            for elem in root.findall('.//record'):
                vertex = self.db.new_vertex(vertex_type)
                for child in elem:
                    vertex.set(child.tag, child.text)
                vertex.save()

# Usage
importer = CustomImporter(db)
importer.import_xml("data.xml", "Data")
```

## Testing

### Unit Tests

```python
import unittest
import arcadedb_embedded as arcadedb
import tempfile
import shutil

class TestDatabase(unittest.TestCase):
    def setUp(self):
        """Create temp database for each test."""
        self.db_path = tempfile.mkdtemp()
        self.db = arcadedb.create_database(self.db_path)

    def tearDown(self):
        """Clean up."""
        self.db.close()
        shutil.rmtree(self.db_path)

    def test_create_vertex(self):
        """Test vertex creation."""
        with self.db.transaction():
            vertex = self.db.new_vertex("User")
            vertex.set("name", "Alice")
            vertex.save()

        result = self.db.query("sql", "SELECT count(*) as count FROM User")
        count = result.next().get("count")
        self.assertEqual(count, 1)
```

### Integration Tests

```python
import pytest
import arcadedb_embedded as arcadedb

@pytest.fixture(scope="module")
def database():
    """Shared database for integration tests."""
    db = arcadedb.create_database("./test_db")
    yield db
    db.close()

def test_graph_traversal(database):
    """Test complex graph operations."""
    # Setup
    with database.transaction():
        alice = database.new_vertex("User")
        alice.set("name", "Alice")
        alice.save()

        bob = database.new_vertex("User")
        bob.set("name", "Bob")
        bob.save()

        edge = alice.new_edge("Knows", bob)
        edge.save()

    # Test
    result = database.query("sql", """
        SELECT expand(out('Knows'))
        FROM User
        WHERE name = 'Alice'
    """)

    friends = list(result)
    assert len(friends) == 1
    assert friends[0].get("name") == "Bob"
```

## Build System

### Package Build

```python
# pyproject.toml configuration
[build-system]
requires = ["setuptools>=45", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "arcadedb-embedded"
version = "0.1.0"
dependencies = ["JPype1>=1.4.0", "numpy>=1.20.0"]
```

### JAR Management

```python
# setup_jars.py - Download and package JARs

import requests
import os

def download_arcadedb_jars(version, variant="full"):
    """Download ArcadeDB distribution JARs."""

    base_url = f"https://repo1.maven.org/maven2/com/arcadedata/"

    # Core JAR
    jar_url = f"{base_url}arcadedb/{version}/arcadedb-{version}.jar"
    download_jar(jar_url, f"arcadedb-{version}.jar")

    # Variant-specific JARs
    if variant == "full":
        # Download Gremlin, GraphQL, etc.
        pass

# Called during package build
```

## See Also

- [Core API Reference](../api/core.md) - Database and record APIs
- [Troubleshooting](troubleshooting.md) - Common issues and solutions
- [JPype Documentation](https://jpype.readthedocs.io/) - JPype library docs
- [ArcadeDB Java API](https://docs.arcadedb.com/) - Underlying Java API
