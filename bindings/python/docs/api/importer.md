# Importer API

The `Importer` class and convenience functions provide high-performance data import capabilities for ArcadeDB, supporting multiple formats including JSON, JSONL, CSV, and Neo4j exports.

## Overview

The importer uses streaming parsers for memory efficiency and performs batch transactions (default 1000 records per commit) for optimal performance. It can import data as documents, vertices, or edges depending on your schema needs.

**Supported Formats:**
- **JSON**: Single or multiple objects
- **JSONL**: Line-delimited JSON (one object per line)
- **CSV/TSV**: Comma or tab-separated values
- **Neo4j**: JSONL export format from Neo4j

## Module Functions

Convenience functions for common import tasks without creating an `Importer` instance:

### `import_json(database, file_path, **options)`

Import a JSON file containing one or more objects.

**Parameters:**
- `database` (Database): Database instance to import into
- `file_path` (str): Path to JSON file
- `**options`: Additional options
  - `commit_every` (int): Records per transaction (default: 1000)
  - `mapping` (Dict): JSON path to database type mappings (optional)

**Returns:**
- `Dict[str, Any]`: Import statistics with keys:
  - `documents`: Number of documents imported
  - `vertices`: Number of vertices imported
  - `edges`: Number of edges imported
  - `errors`: Number of errors encountered
  - `duration_ms`: Import duration in milliseconds

**Example:**
```python
import arcadedb_embedded as arcadedb

db = arcadedb.open_database("./mydb")
stats = arcadedb.import_json(db, "data.json")
print(f"Imported {stats['documents']} documents in {stats['duration_ms']}ms")
```

---

### `import_jsonl(database, file_path, type_name, **options)`

Import a JSONL (line-delimited JSON) file where each line is a separate JSON object.

**Parameters:**
- `database` (Database): Database instance
- `file_path` (str): Path to JSONL file
- `type_name` (str): Target document/vertex type name
- `**options`: Additional options
  - `commit_every` (int): Records per transaction (default: 1000)
  - `vertex_type` (str): Import as vertices instead of documents (optional)
  - `parse_rids` (bool): Parse RID references (default: True)

**Returns:**
- `Dict[str, Any]`: Import statistics

**Example:**
```python
# Import as documents
stats = arcadedb.import_jsonl(db, "users.jsonl", "User")

# Import as vertices
stats = arcadedb.import_jsonl(
    db, "people.jsonl", "Person",
    vertex_type="Person"
)
```

---

### `import_csv(database, file_path, type_name, **options)`

Import CSV or TSV files as documents, vertices, or edges.

**Parameters:**
- `database` (Database): Database instance
- `file_path` (str): Path to CSV file
- `type_name` (str): Target type name
- `**options`: Format-specific options
  - `delimiter` (str): Field delimiter (default: ',', use '\t' for TSV)
  - `header` (bool): File has header row (default: True)
  - `commit_every` (int): Records per transaction (default: 1000)
  - `vertex_type` (str): Import as vertices (optional)
  - `edge_type` (str): Import as edges (optional)
  - `from_property` (str): Source column for edges (default: 'from')
  - `to_property` (str): Target column for edges (default: 'to')
  - `verbose` (bool): Print errors during import (default: False)

**Returns:**
- `Dict[str, Any]`: Import statistics

**Examples:**

**Import as Documents:**
```python
stats = arcadedb.import_csv(db, "people.csv", "Person")
```

**Import as Vertices:**
```python
stats = arcadedb.import_csv(
    db, "users.csv", "User",
    vertex_type="User",
    commit_every=500
)
```

**Import as Edges:**
```python
# CSV must have columns for source and target RIDs
stats = arcadedb.import_csv(
    db, "follows.csv", "Follows",
    edge_type="Follows",
    from_property="user_rid",  # Column containing source RID
    to_property="follows_rid",  # Column containing target RID
    header=True
)
```

**Import TSV:**
```python
stats = arcadedb.import_csv(
    db, "data.tsv", "Data",
    delimiter='\t'
)
```

---

### `import_neo4j(database, file_path, **options)`

Import Neo4j JSONL export files (created with `neo4j-admin export`).

**Parameters:**
- `database` (Database): Database instance
- `file_path` (str): Path to Neo4j export file
- `**options`: Additional options
  - `commit_every` (int): Records per transaction (default: 1000)
  - `verbose` (bool): Show detailed progress (default: False)

**Returns:**
- `Dict[str, Any]`: Import statistics

**Example:**
```python
stats = arcadedb.import_neo4j(db, "neo4j_export.jsonl", verbose=True)
print(f"Imported {stats['vertices']} vertices and {stats['edges']} edges")
```

**Note:** Neo4j imports use Java's `Neo4jImporter` which performs a 3-pass import:
1. Schema analysis
2. Vertex import
3. Edge import

---

## Importer Class

For more control over the import process, use the `Importer` class directly.

### `Importer(database)`

**Constructor:**

**Parameters:**
- `database` (Database): Database instance to import into

**Example:**
```python
from arcadedb_embedded import Importer

db = arcadedb.open_database("./mydb")
importer = Importer(db)
```

---

### `import_file(file_path, format_type=None, type_name=None, commit_every=1000, **options)`

Import data from a file with auto-detection or explicit format specification.

**Parameters:**
- `file_path` (str): Path to file to import
- `format_type` (Optional[str]): Format type ('json', 'jsonl', 'csv', 'neo4j')
  - If None, auto-detects from file extension:
    - `.json` → 'json'
    - `.jsonl`, `.txt` → 'jsonl'
    - `.csv`, `.tsv` → 'csv'
- `type_name` (Optional[str]): Target type name (required for CSV/JSONL)
- `commit_every` (int): Records per transaction (default: 1000)
- `**options`: Format-specific options (see individual format documentation)

**Returns:**
- `Dict[str, Any]`: Import statistics

**Raises:**
- `ArcadeDBError`: If file not found, format unsupported, or import fails

**Example:**
```python
importer = Importer(db)

# Auto-detect format from extension
stats = importer.import_file("data.json")
stats = importer.import_file("users.csv", type_name="User")

# Explicit format
stats = importer.import_file(
    "data.txt",
    format_type='jsonl',
    type_name="Record"
)
```

---

## Format-Specific Details

### JSON Format

**File Structure:**
- Single JSON object: `{...}`
- Array of objects: `[{...}, {...}]`
- Multiple root objects: `{...}\n{...}`

**Options:**
- `mapping` (Dict): Map JSON paths to database types (advanced)

**Type Inference:** The importer uses Java's `JSONImporterFormat` which automatically creates schema based on JSON structure.

**Example:**
```python
# data.json:
# [
#   {"name": "Alice", "age": 30, "city": "NYC"},
#   {"name": "Bob", "age": 25, "city": "LA"}
# ]

stats = arcadedb.import_json(db, "data.json", commit_every=1000)
```

---

### JSONL Format

**File Structure:**
Each line is a complete JSON object:
```
{"id": 1, "name": "Alice"}
{"id": 2, "name": "Bob"}
```

**Options:**
- `vertex_type` (str): Import as vertices instead of documents
- `parse_rids` (bool): Parse RID string references (default: True)

**Type Inference:** Values are converted to Python types:
- Strings remain strings
- Numbers remain numbers
- Booleans remain booleans
- Objects/arrays remain as-is

**Example:**
```python
# Import as documents
stats = arcadedb.import_jsonl(db, "events.jsonl", "Event")

# Import as vertices for graph data
stats = arcadedb.import_jsonl(
    db, "nodes.jsonl", "Node",
    vertex_type="Node",
    commit_every=500
)
```

---

### CSV Format

**File Structure:**
```csv
name,age,city
Alice,30,NYC
Bob,25,LA
```

**Options:**
- `delimiter` (str): Field separator (default: ',')
  - Use `'\t'` for tab-separated (TSV)
- `header` (bool): First row contains column names (default: True)
  - If False, columns named `col_0`, `col_1`, etc.
- `vertex_type` (str): Import as vertices
- `edge_type` (str): Import as edges (requires `from_property` and `to_property`)
- `from_property` (str): Column name for edge source RID (default: 'from')
- `to_property` (str): Column name for edge target RID (default: 'to')

**Type Inference:** String values are automatically converted:
- `"true"`, `"false"` → boolean
- Valid integers → int
- Valid floats → float
- Everything else → string
- Empty strings → None

**Documents Example:**
```python
# people.csv:
# name,age,email
# Alice,30,alice@example.com
# Bob,25,bob@example.com

stats = arcadedb.import_csv(db, "people.csv", "Person")
```

**Vertices Example:**
```python
stats = arcadedb.import_csv(
    db, "users.csv", "User",
    vertex_type="User",
    delimiter=','
)
```

**Edges Example:**
```python
# relationships.csv:
# from_rid,to_rid,type,since
# #1:0,#1:1,FRIEND,2020
# #1:1,#1:2,COLLEAGUE,2021

# First create the schema
db.command("sql", "CREATE EDGE TYPE Relationship")

# Then import
stats = arcadedb.import_csv(
    db, "relationships.csv", "Relationship",
    edge_type="Relationship",
    from_property="from_rid",
    to_property="to_rid"
)
```

**Important for Edge Imports:**
- CSV must have header row (`header=True`)
- Source and target columns must contain valid RIDs (e.g., `#1:0`)
- Edge type must exist in schema before import
- Additional columns become edge properties

---

### Neo4j Format

Imports Neo4j JSONL exports created with:
```bash
neo4j-admin export --to=export.jsonl
```

**Process:**
1. **Schema Pass**: Analyzes structure, creates types
2. **Vertex Pass**: Imports all nodes as vertices
3. **Edge Pass**: Imports all relationships as edges

**Options:**
- `verbose` (bool): Show detailed progress (default: False)

**Example:**
```python
# Export from Neo4j:
# neo4j-admin export --to=/tmp/mydb_export.jsonl

# Import to ArcadeDB:
stats = arcadedb.import_neo4j(db, "/tmp/mydb_export.jsonl", verbose=True)

print(f"Vertices: {stats['vertices']}")
print(f"Edges: {stats['edges']}")
print(f"Time: {stats['duration_ms']}ms")
```

---

## Performance Tips

### Batch Size

The `commit_every` parameter controls transaction size:

```python
# Smaller batches (safer, slower)
stats = arcadedb.import_csv(db, "data.csv", "Data", commit_every=100)

# Larger batches (faster, more memory)
stats = arcadedb.import_csv(db, "data.csv", "Data", commit_every=5000)
```

**Guidelines:**
- **Small files (<10K records)**: 1000-2000
- **Medium files (10K-1M records)**: 2000-5000
- **Large files (>1M records)**: 5000-10000

### Memory Efficiency

The importer uses streaming parsers:
- **JSONL/CSV**: Line-by-line processing (very efficient)
- **JSON**: Uses Java's streaming parser
- **Neo4j**: Multi-pass streaming

### Schema Pre-Creation

Create types before import for better performance:

```python
# Create schema first
db.command("sql", """
    CREATE DOCUMENT TYPE Person
    IF NOT EXISTS
""")
db.command("sql", """
    CREATE PROPERTY Person.email STRING
""")
db.command("sql", """
    CREATE INDEX ON Person (email) UNIQUE
""")

# Then import (type already exists)
stats = arcadedb.import_csv(db, "people.csv", "Person")
```

### Indexing Strategy

Create indexes AFTER import for faster loading:

```python
# Import without indexes
stats = arcadedb.import_csv(db, "users.csv", "User", vertex_type="User")

# Create indexes after import
db.command("sql", "CREATE INDEX ON User (email) UNIQUE")
db.command("sql", "CREATE INDEX ON User (username) UNIQUE")
```

---

## Error Handling

The importer continues on row-level errors and reports them in statistics:

```python
stats = arcadedb.import_csv(
    db, "data.csv", "Data",
    verbose=True  # Print errors as they occur
)

if stats['errors'] > 0:
    print(f"Warning: {stats['errors']} records failed to import")
    print(f"Successfully imported: {stats['documents'] + stats['vertices']}")
```

**Common Errors:**
- **Type mismatch**: Value doesn't match schema constraint
- **Missing required fields**: Schema requires field not in data
- **Invalid RIDs** (edges): Referenced vertex doesn't exist
- **JSON parse errors** (JSONL): Malformed JSON line
- **Encoding issues**: Non-UTF-8 characters

**Error Recovery:**
```python
try:
    stats = importer.import_file("data.csv", type_name="Data")
except arcadedb.ArcadeDBError as e:
    print(f"Import failed: {e}")
    # File not found, format error, or critical failure
```

---

## Complete Examples

### Multi-Format Import Pipeline

```python
import arcadedb_embedded as arcadedb

# Open or create database
db = arcadedb.create_database("./import_demo")

# Create schema
db.command("sql", "CREATE DOCUMENT TYPE User")
db.command("sql", "CREATE VERTEX TYPE Person")
db.command("sql", "CREATE EDGE TYPE Knows")

# Import documents from JSON
stats1 = arcadedb.import_json(db, "users.json")
print(f"Users: {stats1['documents']}")

# Import vertices from CSV
stats2 = arcadedb.import_csv(
    db, "people.csv", "Person",
    vertex_type="Person"
)
print(f"People: {stats2['vertices']}")

# Import edges from CSV
stats3 = arcadedb.import_csv(
    db, "relationships.csv", "Knows",
    edge_type="Knows",
    from_property="person1_rid",
    to_property="person2_rid"
)
print(f"Relationships: {stats3['edges']}")

db.close()
```

### Large-Scale Import with Progress Tracking

```python
import arcadedb_embedded as arcadedb
import time

db = arcadedb.create_database("./large_import")

# Create schema
db.command("sql", "CREATE VERTEX TYPE Product")

# Import with progress monitoring
print("Starting import...")
start = time.time()

stats = arcadedb.import_csv(
    db, "products.csv", "Product",
    vertex_type="Product",
    commit_every=10000,  # Large batches for performance
    verbose=True  # Show errors
)

elapsed = time.time() - start

print(f"\nImport complete!")
print(f"Records: {stats['vertices']:,}")
print(f"Errors: {stats['errors']}")
print(f"Time: {elapsed:.2f}s")
print(f"Rate: {stats['vertices'] / elapsed:.0f} records/sec")

# Create indexes after import
print("\nCreating indexes...")
db.command("sql", "CREATE INDEX ON Product (sku) UNIQUE")
db.command("sql", "CREATE INDEX ON Product (category) NOTUNIQUE")

db.close()
```

### Neo4j Migration

```python
import arcadedb_embedded as arcadedb

# Export from Neo4j (run in Neo4j):
# neo4j-admin export --to=/exports/mydb.jsonl

# Import to ArcadeDB
db = arcadedb.create_database("./migrated_db")

stats = arcadedb.import_neo4j(
    db, "/exports/mydb.jsonl",
    commit_every=5000,
    verbose=True
)

print(f"Migration complete!")
print(f"Vertices: {stats['vertices']:,}")
print(f"Edges: {stats['edges']:,}")
print(f"Time: {stats['duration_ms'] / 1000:.2f}s")

db.close()
```

---

## See Also

- [Data Import Guide](../guide/import.md) - Comprehensive import strategies
- [Database API](database.md) - Database operations
- [Graph Operations Guide](../guide/graphs.md) - Working with graph data
- [Import Examples](../examples/import.md) - More practical examples
