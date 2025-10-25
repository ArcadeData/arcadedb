# Data Import Tests

The `test_importer.py` file contains **12 tests** covering CSV, JSON, and Neo4j data import.

[View source code](https://github.com/humemai/arcadedb/blob/python-embedded/bindings/python/tests/test_importer.py){ .md-button }

## Overview

ArcadeDB Python bindings provide built-in importers for:

- ✅ **CSV** - Import as documents, vertices, or edges
- ✅ **JSON** - Import as documents (uses Java importer)
- ✅ **Neo4j** - Import APOC export format (nodes and relationships)

All importers support:

- Batch transaction commits (default: 1000 records)
- Type inference (CSV: string → int/float/bool/None)
- Error handling and statistics

## Import Capabilities Matrix

| Format | Documents | Vertices | Edges | Type Inference |
|--------|-----------|----------|-------|----------------|
| CSV | ✅ | ✅ | ✅ | ✅ |
| JSON | ✅ | ❌ | ❌ | ✅ (JSON types) |
| Neo4j | ✅ | ✅ (nodes) | ✅ (rels) | ✅ |

## Quick Start Examples

### CSV Import

```python
import arcadedb_embedded as arcadedb

db = arcadedb.create_database("./mydb")

# Import as documents
arcadedb.import_csv(
    db,
    "data.csv",
    type_name="Person",
    import_as="document"  # or "vertex" or "edge"
)
```

### JSON Import

```python
# Import JSON array
arcadedb.import_json(
    db,
    "data.json",
    type_name="Person"
)
```

### Neo4j Import

```python
# Import Neo4j APOC export
arcadedb.import_neo4j(
    db,
    "neo4j_export.jsonl"
)
```

## Test Cases

### CSV Import Tests (5 tests)

#### 1. CSV as Documents

```python
# Create CSV file
csv_content = """name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago"""

# Import
stats = arcadedb.import_csv(
    db,
    "people.csv",
    type_name="Person",
    import_as="document"
)

# Verify
result = db.query("sql", "SELECT FROM Person")
assert len(list(result)) == 3
```

#### 2. CSV as Vertices

```python
csv_content = """name,age
Alice,30
Bob,25"""

stats = arcadedb.import_csv(
    db,
    "people.csv",
    type_name="Person",
    import_as="vertex"
)

# Verify vertices created
result = db.query("sql", "SELECT FROM Person")
for person in result:
    assert hasattr(person, 'out')  # Vertices have out() method
```

#### 3. CSV as Edges

```python
# Requires pre-existing vertices with RIDs
csv_content = """from_rid,to_rid,since
#1:0,#1:1,2020
#1:1,#1:2,2021"""

stats = arcadedb.import_csv(
    db,
    "relationships.csv",
    type_name="Knows",
    import_as="edge"
)
```

#### 4. CSV with Custom Delimiter

```python
csv_content = """name|age|city
Alice|30|NYC"""

stats = arcadedb.import_csv(
    db,
    "data.csv",
    type_name="Person",
    delimiter="|"
)
```

#### 5. CSV Type Inference

```python
csv_content = """name,age,active,score,notes
Alice,30,true,98.5,
Bob,25,false,87.3,Some text"""

stats = arcadedb.import_csv(db, "data.csv", type_name="Person")

result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
alice = list(result)[0]

# Types are inferred
assert isinstance(alice.get_property("age"), int)
assert isinstance(alice.get_property("active"), bool)
assert isinstance(alice.get_property("score"), float)
assert alice.get_property("notes") is None
```

**Type inference rules:**

- `"123"` → `int`
- `"3.14"` → `float`
- `"true"/"false"` → `bool`
- `""` (empty) → `None`
- Everything else → `str`

### JSON Import Tests (2 tests)

#### 1. JSON Array Import

```python
# JSON array format
json_content = '''[
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25},
  {"name": "Charlie", "age": 35}
]'''

stats = arcadedb.import_json(
    db,
    "people.json",
    type_name="Person"
)

# Only creates documents (not vertices)
result = db.query("sql", "SELECT FROM Person")
assert len(list(result)) == 3
```

#### 2. JSON with Nested Objects

```python
json_content = '''[
  {
    "name": "Alice",
    "address": {
      "city": "NYC",
      "zip": "10001"
    },
    "hobbies": ["reading", "gaming"]
  }
]'''

stats = arcadedb.import_json(db, "people.json", type_name="Person")

result = db.query("sql", "SELECT FROM Person WHERE name = 'Alice'")
alice = list(result)[0]

# Nested structures preserved
address = alice.get_property("address")
assert address["city"] == "NYC"
```

### Neo4j Import Tests (3 tests)

#### 1. Neo4j Nodes (Vertices)

```python
# Neo4j APOC export format
neo4j_content = '''{"type":"node","id":"0","labels":["Person"],"properties":{"name":"Alice","age":30}}
{"type":"node","id":"1","labels":["Person"],"properties":{"name":"Bob","age":25}}'''

stats = arcadedb.import_neo4j(db, "neo4j_export.jsonl")

# Creates vertices with Person type
result = db.query("sql", "SELECT FROM Person")
assert len(list(result)) == 2
```

#### 2. Neo4j Relationships (Edges)

```python
# Nodes first
neo4j_content = '''{"type":"node","id":"0","labels":["Person"],"properties":{"name":"Alice"}}
{"type":"node","id":"1","labels":["Person"],"properties":{"name":"Bob"}}
{"type":"relationship","id":"0","label":"KNOWS","start":{"id":"0"},"end":{"id":"1"},"properties":{"since":2020}}'''

stats = arcadedb.import_neo4j(db, "neo4j_export.jsonl")

# Verify edge created
result = db.query("sql", "SELECT FROM KNOWS")
assert len(list(result)) == 1

edge = list(result)[0]
assert edge.get_property("since") == 2020
```

#### 3. Neo4j Full Graph

```python
# Complete graph export
neo4j_content = '''{"type":"node","id":"0","labels":["Person"],"properties":{"name":"Alice"}}
{"type":"node","id":"1","labels":["Person"],"properties":{"name":"Bob"}}
{"type":"node","id":"2","labels":["Person"],"properties":{"name":"Charlie"}}
{"type":"relationship","id":"0","label":"KNOWS","start":{"id":"0"},"end":{"id":"1"},"properties":{}}
{"type":"relationship","id":"1","label":"KNOWS","start":{"id":"1"},"end":{"id":"2"},"properties":{}}'''

stats = arcadedb.import_neo4j(db, "graph.jsonl")

print(f"Nodes imported: {stats['nodes_imported']}")
print(f"Relationships imported: {stats['relationships_imported']}")

# Query graph
result = db.query("sql", """
    SELECT name, out('KNOWS').name as friends
    FROM Person WHERE name = 'Bob'
""")
```

## Import Options

### Common Options (All Importers)

```python
stats = arcadedb.import_xxx(
    db,
    file_path,
    type_name="MyType",
    commit_every=1000,  # Commit every N records (default: 1000)
    **options
)
```

### CSV Options

```python
stats = arcadedb.import_csv(
    db,
    "data.csv",
    type_name="Person",
    import_as="document",  # "document", "vertex", or "edge"
    delimiter=",",          # Field delimiter (default: ",")
    quote_char='"',         # Quote character (default: '"')
    header=True,            # Has header row (default: True)
    commit_every=1000
)
```

### Neo4j Options

```python
stats = arcadedb.import_neo4j(
    db,
    "neo4j_export.jsonl",
    commit_every=1000
)
```

## Import Statistics

All importers return statistics:

```python
stats = arcadedb.import_csv(db, "data.csv", type_name="Person")

print(stats)
# {
#     'records_imported': 1000,
#     'duration_seconds': 1.23,
#     'records_per_second': 813.0,
#     'errors': 0
# }
```

For Neo4j imports:

```python
stats = arcadedb.import_neo4j(db, "graph.jsonl")

print(stats)
# {
#     'nodes_imported': 100,
#     'relationships_imported': 250,
#     'duration_seconds': 0.45,
#     'errors': 0
# }
```

## Running These Tests

```bash
# Run all import tests
pytest tests/test_importer.py -v

# Run specific format tests
pytest tests/test_importer.py -k "csv" -v
pytest tests/test_importer.py -k "neo4j" -v

# Run with output
pytest tests/test_importer.py -v -s
```

## Best Practices

### ✅ DO: Use Appropriate Batch Size

```python
# Large files: increase batch size
arcadedb.import_csv(
    db,
    "huge_file.csv",
    type_name="Data",
    commit_every=10000  # Fewer, larger transactions
)

# Small files: default is fine
arcadedb.import_csv(
    db,
    "small_file.csv",
    type_name="Data"
    # commit_every=1000 (default)
)
```

### ✅ DO: Create Types Before Importing

```python
# Define schema first for better performance
db.command("sql", "CREATE DOCUMENT TYPE Person")
db.command("sql", "CREATE PROPERTY Person.age INTEGER")
db.command("sql", "CREATE INDEX ON Person(name) UNIQUE")

# Then import
arcadedb.import_csv(db, "people.csv", type_name="Person")
```

### ✅ DO: Handle Import Errors

```python
try:
    stats = arcadedb.import_csv(db, "data.csv", type_name="Person")
    print(f"Imported {stats['records_imported']} records")
except Exception as e:
    print(f"Import failed: {e}")
    # Handle error, rollback, etc.
```

## Related Documentation

- [Importer API Reference](../../api/importer.md)
- [Data Import Guide](../../guide/import.md)
- [CSV Import Examples](../../examples/import-csv/)
- [Neo4j Migration Guide](../../examples/neo4j-migration/)
