# Troubleshooting

Common issues, solutions, and debugging techniques for ArcadeDB Python bindings.

## Installation Issues

### Java Not Found

**Symptom:**
```
Error: Java Runtime Environment (JRE) not found
```

**Solution:**

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install default-jre-headless
```

**macOS:**
```bash
brew install openjdk
```

**Windows:**
Download and install from [java.com](https://java.com)

**Verify:**
```bash
java -version
```

---

### Wrong Package Installed

**Symptom:**
```python
import arcadedb_embedded
# Missing features or import errors
```

**Solution:**

Uninstall all variants first:
```bash
pip uninstall -y arcadedb-embedded arcadedb-embedded-headless arcadedb-embedded-minimal
```

Then install the one you need:
```bash
# Smallest (~94MB, no Gremlin/GraphQL)
pip install arcadedb-embedded-headless

# Full features (~158MB, includes everything)
pip install arcadedb-embedded
```

---

### JPype Installation Fails

**Symptom:**
```
ERROR: Failed building wheel for JPype1
```

**Solution:**

Install build dependencies first:

**Ubuntu/Debian:**
```bash
sudo apt-get install python3-dev build-essential
pip install JPype1
```

**macOS:**
```bash
xcode-select --install
pip install JPype1
```

**Windows:**
Install Visual Studio Build Tools, then:
```bash
pip install JPype1
```

## Runtime Errors

### JVM Already Started

**Symptom:**
```python
jpype._core.JVMNotRunning: Unable to start JVM - already started
```

**Cause:**
Attempting to configure JVM after it's already started.

**Solution:**

Configure JVM options **before** first import:
```python
import jpype

# Configure BEFORE importing arcadedb_embedded
jpype.addClassPath("/path/to/extra.jar")

# Now import
import arcadedb_embedded as arcadedb
```

**Alternative:**
Use separate processes for different JVM configurations:
```python
import multiprocessing as mp

def with_custom_jvm(jar_path):
    import jpype
    jpype.addClassPath(jar_path)

    import arcadedb_embedded as arcadedb
    # Use arcadedb here

if __name__ == "__main__":
    p = mp.Process(target=with_custom_jvm, args=("/path/to/jar",))
    p.start()
    p.join()
```

---

### Database Already Exists

**Symptom:**
```python
arcadedb.create_database("./mydb")
# ArcadeDBError: Database already exists
```

**Solution:**

Use `open_database()` instead:
```python
import os
import arcadedb_embedded as arcadedb

if os.path.exists("./mydb"):
    db = arcadedb.open_database("./mydb")
else:
    db = arcadedb.create_database("./mydb")
```

Or delete existing database:
```python
import shutil

# Remove existing database
if os.path.exists("./mydb"):
    shutil.rmtree("./mydb")

# Create fresh database
db = arcadedb.create_database("./mydb")
```

---

### Database Locked

**Symptom:**
```
ArcadeDBError: Database is locked by another process
```

**Cause:**
Another process has the database open.

**Solution:**

1. **Close other connections:**
```python
# Ensure previous database is closed
db.close()
```

2. **Check for orphaned processes:**
```bash
# Linux/macOS
ps aux | grep python
kill <PID>

# Windows
tasklist | findstr python
taskkill /PID <PID>
```

3. **Remove lock file (last resort):**
```bash
# Only if you're sure no process is using the database
rm ./mydb/.lock
```

---

### Transaction Already Active

**Symptom:**
```python
with db.transaction():
    with db.transaction():  # Nested!
        pass
# ArcadeDBError: Transaction already active
```

**Cause:**
Nested transactions not supported.

**Solution:**

Don't nest transactions:
```python
# Bad
with db.transaction():
    some_operation()
    with db.transaction():  # ✗ Error
        another_operation()

# Good
with db.transaction():
    some_operation()
    another_operation()
```

Or use separate transaction blocks:
```python
with db.transaction():
    some_operation()

# First transaction committed

with db.transaction():
    another_operation()
```

---

### Query Syntax Error

**Symptom:**
```python
db.query("sql", "SELECT * FROM User WHERE name = Alice")
# ArcadeDBError: Syntax error near 'Alice'
```

**Cause:**
String not properly quoted.

**Solution:**

Use parameters (RECOMMENDED):
```python
db.query("sql",
    "SELECT FROM User WHERE name = :name",
    {"name": "Alice"}
)
```

Or quote strings in SQL:
```python
db.query("sql", "SELECT FROM User WHERE name = 'Alice'")
#                                              ↑    ↑ quotes
```

---

### Type Conversion Error

**Symptom:**
```python
vertex.set("embedding", numpy_array)
# TypeError: Cannot convert numpy.ndarray to Java type
```

**Cause:**
NumPy arrays need explicit conversion.

**Solution:**

Use conversion utilities:
```python
from arcadedb_embedded import to_java_float_array
import numpy as np

embedding = np.array([1.0, 2.0, 3.0], dtype=np.float32)
vertex.set("embedding", to_java_float_array(embedding))
```

## Performance Issues

### Slow Queries

**Symptom:**
Queries take seconds or minutes.

**Diagnosis:**

Use EXPLAIN to analyze:
```python
result = db.query("sql", "EXPLAIN SELECT FROM User WHERE email = 'alice@example.com'")
for row in result:
    print(row.to_dict())
```

**Solutions:**

1. **Create indexes:**
```python
with db.transaction():
    db.command("sql", "CREATE INDEX ON User (email) UNIQUE")
```

2. **Use LIMIT:**
```python
# Bad: Load everything
result = db.query("sql", "SELECT FROM User")

# Good: Limit results
result = db.query("sql", "SELECT FROM User LIMIT 100")
```

3. **Project only needed fields:**
```python
# Bad: Load all properties
result = db.query("sql", "SELECT FROM User")

# Good: Only needed fields
result = db.query("sql", "SELECT name, email FROM User")
```

---

### Slow Imports

**Symptom:**
Importing data is very slow.

**Solutions:**

1. **Increase batch size:**
```python
importer = db.get_importer()
importer.batch_size = 10000  # Default is 1000
```

2. **Drop indexes during import:**
```python
# Drop indexes
with db.transaction():
    db.command("sql", "DROP INDEX User.email")

# Import data
importer.import_csv("users.csv", "User")

# Recreate indexes
with db.transaction():
    db.command("sql", "CREATE INDEX ON User (email) UNIQUE")
```

3. **Use transactions efficiently:**
```python
# Bad: Many small transactions
for record in records:
    with db.transaction():
        vertex = db.new_vertex("Data")
        vertex.set("data", record)
        vertex.save()

# Good: Batch in larger transactions
batch_size = 10000
for i in range(0, len(records), batch_size):
    with db.transaction():
        for record in records[i:i+batch_size]:
            vertex = db.new_vertex("Data")
            vertex.set("data", record)
            vertex.save()
```

---

### High Memory Usage

**Symptom:**
Process memory grows continuously.

**Diagnosis:**

Monitor memory:
```python
import psutil
import os

process = psutil.Process(os.getpid())
print(f"Memory: {process.memory_info().rss / 1024 / 1024:.1f} MB")
```

**Solutions:**

1. **Stream large ResultSets:**
```python
# Bad: Load all results
result = db.query("sql", "SELECT FROM LargeTable")
all_results = list(result)  # Loads everything!

# Good: Process streaming
result = db.query("sql", "SELECT FROM LargeTable")
for row in result:
    process(row)
    # Only one row in memory
```

2. **Close ResultSets:**
```python
result = db.query("sql", "SELECT FROM User")
for row in result:
    if some_condition(row):
        break
# ResultSet automatically closed when iterator exhausted
```

3. **Force garbage collection:**
```python
import gc

for batch in large_dataset:
    process_batch(batch)
    gc.collect()  # Trigger GC
```

4. **Smaller transactions:**
```python
# Bad: Huge transaction
with db.transaction():
    for i in range(1000000):
        vertex = db.new_vertex("Data")
        vertex.save()

# Good: Batch transactions
batch_size = 10000
for i in range(0, 1000000, batch_size):
    with db.transaction():
        for j in range(batch_size):
            vertex = db.new_vertex("Data")
            vertex.save()
```

## Server Mode Issues

### Server Won't Start

**Symptom:**
```python
server = arcadedb.create_server("./databases")
server.start()
# ArcadeDBError: Unable to start server
```

**Solutions:**

1. **Check port availability:**
```bash
# Linux/macOS
lsof -i :2480

# Windows
netstat -ano | findstr :2480
```

Use different port:
```python
server = arcadedb.create_server(
    root_path="./databases",
    http_port=8080  # Different port
)
```

2. **Check permissions:**
```bash
ls -la ./databases
# Ensure write permissions
chmod -R 755 ./databases
```

3. **Check logs:**
```python
# Enable logging
import logging
logging.basicConfig(level=logging.DEBUG)

server = arcadedb.create_server("./databases")
server.start()
# Check log output
```

---

### Can't Connect to Server

**Symptom:**
Server running but can't connect via HTTP.

**Solutions:**

1. **Verify server is running:**
```python
if server.is_started():
    print("Server is running")
    print(f"URL: http://localhost:{server.http_port}")
```

2. **Check firewall:**
```bash
# Linux
sudo ufw allow 2480

# macOS
# System Preferences > Security & Privacy > Firewall
```

3. **Test with curl:**
```bash
curl http://localhost:2480/api/v1/server
```

## Vector Search Issues

### Vector Dimension Mismatch

**Symptom:**
```python
index.add_vertex(vertex)
# ArcadeDBError: Vector dimension mismatch
```

**Cause:**
Embedding dimension doesn't match index dimension.

**Solution:**

Verify dimensions match:
```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')

# Check model dimension
test_embedding = model.encode("test")
print(f"Model dimension: {len(test_embedding)}")  # 384

# Create index with matching dimension
index = db.create_vector_index(
    vertex_type="Document",
    vector_property="embedding",
    dimensions=384  # Must match!
)
```

---

### Poor Search Results

**Symptom:**
Vector search returns irrelevant results.

**Solutions:**

1. **Try different distance function:**
```python
# Cosine (default, usually best for text)
index = db.create_vector_index(
    vertex_type="Doc",
    vector_property="embedding",
    dimensions=384,
    distance_function="cosine"
)

# Euclidean (sometimes better for images)
index = db.create_vector_index(
    vertex_type="Image",
    vector_property="features",
    dimensions=512,
    distance_function="euclidean"
)
```

2. **Tune HNSW parameters:**
```python
# Better recall, slower
index = db.create_vector_index(
    vertex_type="Doc",
    vector_property="embedding",
    dimensions=384,
    m=32,              # More connections
    ef=256,            # Larger search candidates
    ef_construction=256
)
```

3. **Improve embeddings:**
```python
# Combine title and content
text = f"{doc['title']}. {doc['content']}"
embedding = model.encode(text)

# vs. just content
embedding = model.encode(doc['content'])  # May be less effective
```

## Debugging

### Enable Logging

**Python logging:**
```python
import logging

# Basic logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# File logging
logging.basicConfig(
    level=logging.DEBUG,
    filename='arcadedb.log',
    filemode='w',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

import arcadedb_embedded as arcadedb
# Now all operations will be logged
```

**Java logging:**
```python
import jpype

# Enable Java logging before importing arcadedb
jpype.startJVM(
    classpath=[...],
    "-Djava.util.logging.config.file=logging.properties"
)
```

logging.properties:
```properties
.level=INFO
handlers=java.util.logging.ConsoleHandler
java.util.logging.ConsoleHandler.level=ALL
com.arcadedata.level=DEBUG
```

---

### Inspect Java Objects

```python
# Get Java class name
java_obj = vertex._java_vertex
print(java_obj.getClass().getName())

# List methods
for method in java_obj.getClass().getMethods():
    print(method.getName())

# Get property value (raw Java)
value = java_obj.get("property_name")
print(f"Type: {type(value)}, Value: {value}")
```

---

### Transaction Debugging

```python
class DebugTransaction:
    """Debug wrapper for transactions."""

    def __init__(self, db):
        self.db = db
        self.transaction = None

    def __enter__(self):
        print("Starting transaction")
        self.transaction = self.db.transaction()
        return self.transaction.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"Transaction failed: {exc_type.__name__}: {exc_val}")
        else:
            print("Transaction committed")
        return self.transaction.__exit__(exc_type, exc_val, exc_tb)

# Usage
with DebugTransaction(db):
    vertex = db.new_vertex("User")
    vertex.set("name", "Alice")
    vertex.save()
```

---

### Query Debugging

```python
def debug_query(db, language, query, params=None):
    """Execute query with debugging."""
    print(f"Query: {query}")
    if params:
        print(f"Params: {params}")

    try:
        result = db.query(language, query, params)
        rows = list(result)
        print(f"Results: {len(rows)} rows")
        return rows
    except Exception as e:
        print(f"Error: {e}")
        raise

# Usage
results = debug_query(db, "sql", "SELECT FROM User WHERE name = :name", {"name": "Alice"})
```

## Common Error Messages

### "Property not found"

**Meaning:** Trying to get property that doesn't exist.

**Solution:**
```python
# Check if property exists
if vertex.has_property("name"):
    name = vertex.get("name")
else:
    name = "Unknown"

# Or use default
name = vertex.get("name") or "Unknown"
```

---

### "Type not found"

**Meaning:** Vertex/Edge type doesn't exist.

**Solution:**
```python
# Create type first
with db.transaction():
    db.command("sql", "CREATE VERTEX TYPE User")

# Then create vertex
with db.transaction():
    vertex = db.new_vertex("User")
```

---

### "Index already exists"

**Meaning:** Trying to create duplicate index.

**Solution:**
```python
# Drop existing index
with db.transaction():
    try:
        db.command("sql", "DROP INDEX User.email")
    except:
        pass  # Index doesn't exist

    # Create new index
    db.command("sql", "CREATE INDEX ON User (email) UNIQUE")
```

---

### "Unique constraint violation"

**Meaning:** Trying to insert duplicate value for unique property.

**Solution:**
```python
# Check if exists first
result = db.query("sql", "SELECT FROM User WHERE email = :email", {"email": "alice@example.com"})

if result.has_next():
    vertex = result.next()
    # Update existing
    vertex.set("name", "Alice")
    vertex.save()
else:
    # Create new
    with db.transaction():
        vertex = db.new_vertex("User")
        vertex.set("email", "alice@example.com")
        vertex.set("name", "Alice")
        vertex.save()
```

## Getting Help

1. **Check Documentation:**
   - [API Reference](../api/database.md)
   - [Guides](../guide/import.md)
   - [Examples](../examples/import.md)

2. **Search Issues:**
   - [GitHub Issues](https://github.com/ArcadeData/arcadedb/issues)
   - [ArcadeDB Documentation](https://docs.arcadedb.com/)

3. **Ask Community:**
   - [Discord](https://discord.gg/arcadedb)
   - [GitHub Discussions](https://github.com/ArcadeData/arcadedb/discussions)

4. **Report Bug:**
   Include:
   - Python version (`python --version`)
   - Package version (`pip show arcadedb-embedded`)
   - Minimal reproducible example
   - Full error message with stack trace
   - Operating system

## See Also

- [Architecture](architecture.md) - System architecture and design
- [Database API](../api/database.md) - Core database operations
- [Exceptions API](../api/exceptions.md) - Error handling reference
