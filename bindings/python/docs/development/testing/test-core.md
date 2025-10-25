# Core Database Tests

The `test_core.py` file contains **13 tests** covering fundamental database operations.

[View source code](https://github.com/humemai/arcadedb/blob/python-embedded/bindings/python/tests/test_core.py){ .md-button }

## Overview

These tests validate:

- ✅ Database creation and opening
- ✅ CRUD operations (Create, Read, Update, Delete)
- ✅ Transaction management
- ✅ Graph operations (vertices, edges, traversals)
- ✅ Query result handling
- ✅ Error handling
- ✅ Cypher queries (when available)
- ✅ Vector search with HNSW indexes
- ✅ Unicode support (international characters, emoji)
- ✅ Schema introspection
- ✅ Large result sets (1000+ records)
- ✅ Type conversions (Python ↔ Java)
- ✅ Complex graph traversals

## Test Cases

### 1. Database Creation

**Test:** `test_database_creation`

Validates that databases can be created with proper initialization.

```python
import arcadedb_embedded as arcadedb

# Create a new database
db = arcadedb.create_database("./test_db")

# Database should be accessible
assert db is not None

# Clean up
db.close()
```

**What it tests:**
- Database creation with default settings
- Database object initialization
- Proper cleanup

---

### 2. Database Opening

**Test:** `test_database_open`

Tests opening existing databases.

```python
# Create database first
db1 = arcadedb.create_database("./test_db")
db1.close()

# Open existing database
db2 = arcadedb.open_database("./test_db")
assert db2 is not None
db2.close()
```

**What it tests:**
- Opening pre-existing databases
- Database persistence
- Multiple open/close cycles

---

### 3. Context Manager Usage

**Test:** `test_context_manager`

Validates the `with` statement for automatic cleanup.

```python
# Database automatically closed when exiting context
with arcadedb.create_database("./test_db") as db:
    result = db.query("sql", "SELECT 1 as num")
    assert list(result)[0].get_property("num") == 1

# Database is automatically closed here
```

**What it tests:**
- Context manager protocol (`__enter__`, `__exit__`)
- Automatic resource cleanup
- Exception handling in context

---

### 4. CRUD Operations

**Test:** `test_crud_operations`

Comprehensive test of Create, Read, Update, Delete operations.

```python
with arcadedb.create_database("./test_db") as db:
    # Create schema
    db.command("sql", "CREATE DOCUMENT TYPE Person")

    # Create (Insert)
    with db.transaction():
        db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

    # Read (Query)
    result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
    person = list(result)[0]
    assert person.get_property("name") == "Alice"
    assert person.get_property("age") == 30

    # Update
    with db.transaction():
        db.command("sql", "UPDATE Person SET age = 31 WHERE name = 'Alice'")

    result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
    person = list(result)[0]
    assert person.get_property("age") == 31

    # Delete
    with db.transaction():
        db.command("sql", "DELETE FROM Person WHERE name = 'Alice'")

    result = db.query("sql", "SELECT FROM Person")
    assert len(list(result)) == 0
```

**What it tests:**
- Document type creation
- Insert operations
- Query and result iteration
- Update operations
- Delete operations
- Transaction boundaries

---

### 5. Transaction Management

**Test:** `test_transactions`

Tests ACID transaction behavior.

```python
with arcadedb.create_database("./test_db") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Product")

    # Successful transaction
    with db.transaction():
        db.command("sql", "INSERT INTO Product SET name = 'Widget', price = 10")

    # Changes are committed
    result = db.query("sql", "SELECT FROM Product")
    assert len(list(result)) == 1

    # Failed transaction (exception causes rollback)
    try:
        with db.transaction():
            db.command("sql", "INSERT INTO Product SET name = 'Gadget', price = 20")
            raise Exception("Simulated error")
    except Exception:
        pass

    # Second insert was rolled back
    result = db.query("sql", "SELECT FROM Product")
    assert len(list(result)) == 1  # Still only 1 record
```

**What it tests:**
- Transaction context manager
- Automatic commit on success
- Automatic rollback on exception
- Data consistency

---

### 6. Graph Operations

**Test:** `test_graph_operations`

Tests vertex and edge creation.

```python
with arcadedb.create_database("./test_db") as db:
    # Create vertex types
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE EDGE TYPE Knows")

    with db.transaction():
        # Create vertices
        db.command("sql", "CREATE VERTEX Person SET name = 'Alice'")
        db.command("sql", "CREATE VERTEX Person SET name = 'Bob'")

        # Create edge between them
        db.command("sql", """
            CREATE EDGE Knows
            FROM (SELECT FROM Person WHERE name = 'Alice')
            TO (SELECT FROM Person WHERE name = 'Bob')
            SET since = 2020
        """)

    # Query graph
    result = db.query("sql", """
        SELECT name, out('Knows').name as friends
        FROM Person WHERE name = 'Alice'
    """)

    alice = list(result)[0]
    assert alice.get_property("name") == "Alice"
    assert "Bob" in str(alice.get_property("friends"))
```

**What it tests:**
- Vertex type creation
- Edge type creation
- Vertex creation
- Edge creation with properties
- Graph traversal queries

---

### 7. Query Result Handling

**Test:** `test_result_set`

Tests ResultSet and Result wrapper classes.

```python
with arcadedb.create_database("./test_db") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Item")

    with db.transaction():
        db.command("sql", "INSERT INTO Item SET id = 1, value = 'first'")
        db.command("sql", "INSERT INTO Item SET id = 2, value = 'second'")
        db.command("sql", "INSERT INTO Item SET id = 3, value = 'third'")

    # Query returns ResultSet
    result_set = db.query("sql", "SELECT FROM Item ORDER BY id")

    # Iterate over results
    items = []
    for result in result_set:
        items.append({
            'id': result.get_property('id'),
            'value': result.get_property('value')
        })

    assert len(items) == 3
    assert items[0]['value'] == 'first'
    assert items[1]['value'] == 'second'
    assert items[2]['value'] == 'third'
```

**What it tests:**
- ResultSet iteration
- Result property access
- Multiple result handling
- Order preservation

---

### 8. Cypher Queries

**Test:** `test_cypher_queries`

Tests Neo4j Cypher query language support (when available).

```python
with arcadedb.create_database("./test_db") as db:
    db.command("sql", "CREATE VERTEX TYPE Person")
    db.command("sql", "CREATE EDGE TYPE KNOWS")

    with db.transaction():
        # Use Cypher to create nodes and relationships
        db.command("cypher", """
            CREATE (a:Person {name: 'Alice', age: 30})
            CREATE (b:Person {name: 'Bob', age: 25})
            CREATE (a)-[:KNOWS {since: 2020}]->(b)
        """)

    # Query with Cypher
    result = db.query("cypher", "MATCH (p:Person) RETURN p.name as name, p.age as age")

    people = [(r.get_property("name"), r.get_property("age")) for r in result]
    assert len(people) == 2
```

!!! note "Distribution Support"
    Cypher support requires specific Java modules. This test is skipped in minimal distribution.

**What it tests:**
- Cypher CREATE statements
- Cypher MATCH queries
- Property access in Cypher results
- Cypher syntax compatibility

---

### 9. Vector Search

**Test:** `test_vector_search`

Tests HNSW vector indexing and similarity search.

```python
import arcadedb_embedded as arcadedb

with arcadedb.create_database("./test_db") as db:
    # Create document type with vector property
    db.command("sql", "CREATE VERTEX TYPE Document")
    db.command("sql", "CREATE PROPERTY Document.name STRING")
    db.command("sql", "CREATE PROPERTY Document.embedding ARRAY_OF_FLOATS")

    # Create HNSW index
    index = db.create_vector_index(
        vertex_type="Document",
        vector_property="embedding",
        dimensions=3,
        id_property="name",
        distance_function="cosine",
        m=16,
        ef=128,
        ef_construction=128,
        max_items=100
    )

    # Insert documents with vectors
    with db.transaction():
        for i, vec in enumerate([[1.0, 0.0, 0.0], [0.9, 0.1, 0.0],
                                  [0.0, 1.0, 0.0]]):
            vertex = db.new_vertex("Document")
            vertex.set("name", f"doc{i+1}")
            vertex.set("embedding", arcadedb.to_java_float_array(vec))
            vertex.save()
            index.add_vertex(vertex)

    # Search for nearest neighbors
    query_vec = [1.0, 0.0, 0.0]
    results = list(index.find_nearest(query_vec, k=2))

    assert len(results) == 2
    # Verify results ordered by distance
    assert results[0][0].get_property("name") == "doc1"  # Exact match
    assert results[1][0].get_property("name") == "doc2"  # Second closest
```

!!! note "Implementation Status"
    Vector search is **experimental**. Current implementation uses jelmerk/hnswlib.
    Future migration to datastax/jvector planned for better performance.

**What it tests:**

- EMBEDDING property type creation
- HNSW index creation with parameters
- Vector insertion (NumPy arrays or Python lists)
- Cosine similarity search
- Result ranking by similarity

**Key findings:**

- ✅ Index creation fast (~0.16s) - creates metadata only
- ⚠️ Index population expensive (~13ms/doc) - builds HNSW graph + disk writes
- ✅ Search efficient (logarithmic) - visits ~1,500-2,000 vertices, not all
- ✅ Works with NumPy arrays and plain Python lists
- ✅ Distance values correct (cosine distance = 1 - similarity, range [0,2])
- ⚠️ No native filtered search - requires oversampling or multiple indexes

**Performance characteristics** (10K documents, 384D):

- Total storage: ~115 MB (24MB vertices + 91MB edges)
- Index file: 4 KB (metadata only, graph stored as edges)
- Per-document cost: ~13ms (HNSW algorithm + edge writes)
- Search working set: ~4 MB (visited vertices, not entire dataset)

See [Vector Search Example](../../examples/03_vector_search.md) for detailed documentation.

---
        CREATE HNSW INDEX Document.embedding
        ON Document(embedding)
        WITH m=16, ef=128, efConstruction=128
    """)

    # Insert vectors
    with db.transaction():
        db.command("sql", "INSERT INTO Document SET name = 'doc1', embedding = [1.0, 0.0, 0.0]")
        db.command("sql", "INSERT INTO Document SET name = 'doc2', embedding = [0.9, 0.1, 0.0]")
        db.command("sql", "INSERT INTO Document SET name = 'doc3', embedding = [0.0, 1.0, 0.0]")

    # Similarity search
    result = db.query("sql", """
        SELECT name, cosine_similarity(embedding, [1.0, 0.0, 0.0]) as similarity
        FROM Document
        ORDER BY similarity DESC
        LIMIT 2
    """)

    docs = list(result)
    assert docs[0].get_property("name") == "doc1"  # Closest match
    assert docs[1].get_property("name") == "doc2"  # Second closest
```

**What it tests:**
- EMBEDDING property type creation
- HNSW index creation with parameters
- Vector insertion
- Cosine similarity search
- Result ranking by similarity

---

### 10. Unicode Support

**Test:** `test_unicode_support`

Tests international characters and emoji.

```python
with arcadedb.create_database("./test_db") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Message")

    with db.transaction():
        # Spanish
        db.command("sql", "INSERT INTO Message SET text = 'Hola, ¿cómo estás?'")

        # Chinese
        db.command("sql", "INSERT INTO Message SET text = '你好世界'")

        # Japanese
        db.command("sql", "INSERT INTO Message SET text = 'こんにちは'")

        # Arabic
        db.command("sql", "INSERT INTO Message SET text = 'مرحبا بالعالم'")

        # Emoji
        db.command("sql", "INSERT INTO Message SET text = '🎮 ArcadeDB 🚀'")

    result = db.query("sql", "SELECT FROM Message")
    texts = [r.get_property("text") for r in result]

    assert "¿cómo estás?" in texts[0]
    assert "你好世界" in texts[1]
    assert "こんにちは" in texts[2]
    assert "مرحبا" in texts[3]
    assert "🎮" in texts[4] and "🚀" in texts[4]
```

**What it tests:**
- UTF-8 encoding/decoding
- Non-ASCII character storage
- Emoji support
- International character sets

---

### 11. Schema Introspection

**Test:** `test_schema_queries`

Tests querying database metadata.

```python
with arcadedb.create_database("./test_db") as db:
    # Create schema
    db.command("sql", "CREATE DOCUMENT TYPE Person")
    db.command("sql", "CREATE VERTEX TYPE Company")
    db.command("sql", "CREATE EDGE TYPE WorksAt")

    # Query schema
    types_result = db.query("sql", "SELECT FROM schema:types")
    type_names = [r.get_property("name") for r in types_result]

    assert "Person" in type_names
    assert "Company" in type_names
    assert "WorksAt" in type_names

    # Get properties of a type
    props_result = db.query("sql", "SELECT FROM schema:type:Person")
    person_type = list(props_result)[0]

    assert person_type.get_property("name") == "Person"
```

**What it tests:**
- Schema metadata queries
- Type listing
- Type introspection
- Schema system tables

---

### 12. Large Result Sets

**Test:** `test_large_result_sets`

Tests handling 1000+ records efficiently.

```python
with arcadedb.create_database("./test_db") as db:
    db.command("sql", "CREATE DOCUMENT TYPE Record")

    # Insert 1000 records
    with db.transaction():
        for i in range(1000):
            db.command("sql", f"INSERT INTO Record SET id = {i}, value = 'record_{i}'")

    # Query all records
    result = db.query("sql", "SELECT FROM Record ORDER BY id")

    # Iterate efficiently
    count = 0
    for record in result:
        assert record.get_property("id") == count
        count += 1

    assert count == 1000
```

**What it tests:**
- Bulk inserts in transactions
- Large result set iteration
- Memory efficiency
- Result ordering at scale

---

### 13. Type Conversions

**Test:** `test_type_conversions`

Tests Python ↔ Java type mapping.

```python
from datetime import datetime

with arcadedb.create_database("./test_db") as db:
    db.command("sql", "CREATE DOCUMENT TYPE TypeTest")

    with db.transaction():
        db.command("sql", """
            INSERT INTO TypeTest SET
                str_val = 'text',
                int_val = 42,
                float_val = 3.14,
                bool_val = true,
                null_val = null,
                date_val = date('2025-10-21')
        """)

    result = db.query("sql", "SELECT FROM TypeTest")
    record = list(result)[0]

    # Verify types
    assert isinstance(record.get_property("str_val"), str)
    assert isinstance(record.get_property("int_val"), int)
    assert isinstance(record.get_property("float_val"), float)
    assert isinstance(record.get_property("bool_val"), bool)
    assert record.get_property("null_val") is None

    # Values match
    assert record.get_property("str_val") == "text"
    assert record.get_property("int_val") == 42
    assert record.get_property("float_val") == 3.14
    assert record.get_property("bool_val") is True
```

**What it tests:**
- String conversion
- Integer conversion
- Float conversion
- Boolean conversion
- Null handling
- Date/time handling
- Type preservation across JVM boundary

## Running These Tests

```bash
# Run all core tests
pytest tests/test_core.py -v

# Run specific test
pytest tests/test_core.py::test_database_creation -v

# Run with output
pytest tests/test_core.py -v -s

# Run only graph-related tests
pytest tests/test_core.py -k "graph" -v

# Run only vector-related tests
pytest tests/test_core.py -k "vector" -v
```

## Key Concepts

### Database Lifecycle

1. **Create**: `arcadedb.create_database(path)`
2. **Use**: Run queries and commands
3. **Close**: `db.close()` or use context manager

### Transaction Pattern

Always use transactions for write operations:

```python
with db.transaction():
    db.command("sql", "INSERT INTO ...")
    db.command("sql", "UPDATE ...")
    # Automatically commits on success
    # Automatically rolls back on exception
```

### Result Handling

```python
# Query returns ResultSet (iterable)
result_set = db.query("sql", "SELECT FROM MyType")

# Iterate to get Result objects
for result in result_set:
    value = result.get_property("field_name")
    # Process value...

# Or convert to list
results = list(result_set)
```

## Related Documentation

- [Database API Reference](../../api/database.md)
- [Transactions API Reference](../../api/transactions.md)
- [Results API Reference](../../api/results.md)
- [Database Guide](../../guide/core/database.md)
- [Query Guide](../../guide/core/queries.md)
- [Transaction Guide](../../guide/core/transactions.md)
