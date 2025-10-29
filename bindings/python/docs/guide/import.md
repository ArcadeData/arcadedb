# Data Import Guide

This guide covers strategies, best practices, and patterns for importing data into ArcadeDB efficiently and reliably.

## Overview

ArcadeDB's Importer supports multiple data formats:

- **CSV**: Tabular data with headers
- **JSON**: Single JSON documents or arrays
- **Neo4j**: Direct migration from Neo4j exports

**Key Features:**

- Automatic type inference
- Batch processing for performance
- Relationship/edge mapping
- Schema validation
- Error handling and recovery

## Quick Start

### CSV Import

```python
import arcadedb_embedded as arcadedb

db = arcadedb.create_database("./mydb")
importer = db.get_importer()

# Import CSV file
importer.import_csv(
    file_path="products.csv",
    vertex_type="Product",
    delimiter=",",
    header=True
)

print("Import complete!")
```

### JSON Import

```python
# Import JSON array
importer.import_json(
    file_path="data.json",
    vertex_type="User"
)
```

### Neo4j Import

```python
# Migrate from Neo4j export
importer.import_neo4j(
    file_path="neo4j_export.json",
    vertex_types=["User", "Product"],
    edge_types=["PURCHASED", "REVIEWED"]
)
```

## Format Selection

### CSV - Tabular Data

**Best For:**
- Spreadsheet data
- Relational database exports
- Time-series data
- Simple structured data

**Advantages:**
- Simple format
- Excel/LibreOffice compatible
- Wide tool support
- Human readable

**Disadvantages:**
- No nested structures
- Limited type information
- Relationships require separate files

**Example:**
```csv
id,name,email,age
1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
```

---

### JSON - Complex Documents

**Best For:**
- API responses
- Document databases
- Nested data structures
- Complex objects

**Advantages:**
- Native type support
- Nested structures
- Arrays and objects
- Standard format

**Disadvantages:**
- Larger file sizes
- Must load entire file
- Harder to edit manually

**Example:**
```json
[
  {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com",
    "addresses": [
      {"street": "123 Main St", "city": "NYC"}
    ]
  }
]
```

---

### Neo4j - Graph Migration

**Best For:**
- Neo4j to ArcadeDB migration
- Graph data with relationships
- Existing Neo4j exports

**Advantages:**
- Direct migration
- Preserves graph structure
- Handles relationships automatically
- 3-pass process (nodes, edges, properties)

**Disadvantages:**
- Neo4j-specific format
- More complex
- Requires understanding of both databases

## Schema Design

### Pre-create Schema

**Recommended:** Define schema before importing for better control and validation.

```python
with db.transaction():
    # Define vertex types
    db.command("sql", "CREATE VERTEX TYPE User")
    db.command("sql", "CREATE PROPERTY User.id STRING")
    db.command("sql", "CREATE PROPERTY User.name STRING")
    db.command("sql", "CREATE PROPERTY User.email STRING")
    db.command("sql", "CREATE PROPERTY User.age INTEGER")

    # Create indexes
    db.command("sql", "CREATE INDEX ON User (id) UNIQUE")
    db.command("sql", "CREATE INDEX ON User (email) UNIQUE")

# Then import
importer.import_csv("users.csv", "User")
```

**Benefits:**
- Type safety
- Validation
- Better performance
- Prevents errors

---

### Let Importer Infer

**Quick Start:** Let importer create schema automatically.

```python
# No schema definition needed
importer.import_csv("users.csv", "User")
```

**Auto-inference:**
- Creates vertex type if missing
- Infers property types from data
- Creates properties as needed

**Trade-offs:**
- Quick to start
- Less control
- Types may be wrong
- No validation

---

### Hybrid Approach

**Best of Both:** Define critical fields, allow others to be inferred.

```python
with db.transaction():
    # Define critical fields only
    db.command("sql", "CREATE VERTEX TYPE User")
    db.command("sql", "CREATE PROPERTY User.id STRING")
    db.command("sql", "CREATE INDEX ON User (id) UNIQUE")

    # Let importer add other properties

# Import with partial schema
importer.import_csv("users.csv", "User")
```

## Performance Optimization

### Batch Size

Control transaction batch size for memory vs. speed trade-off:

```python
# Small batches: Lower memory, more transactions
importer.batch_size = 1000

# Medium batches: Balanced (default)
importer.batch_size = 10000

# Large batches: Higher memory, fewer transactions
importer.batch_size = 100000

# Apply
importer.import_csv("large_file.csv", "Data")
```

**Guidelines:**

| Dataset Size | Recommended Batch Size |
|--------------|------------------------|
| < 100K rows  | 1,000 - 10,000        |
| 100K - 1M    | 10,000 - 50,000       |
| > 1M rows    | 50,000 - 100,000      |

**Consider:**
- Available memory
- Record size
- Concurrent operations
- Disk I/O

---

### Parallel Processing

Split large files and process in parallel:

```python
import concurrent.futures
import os

def split_csv(input_file, chunk_size=100000):
    """Split large CSV into chunks."""
    chunks = []
    chunk_num = 0

    with open(input_file, 'r') as f:
        header = f.readline()

        chunk_file = f"chunk_{chunk_num}.csv"
        chunk_writer = open(chunk_file, 'w')
        chunk_writer.write(header)
        chunks.append(chunk_file)

        line_count = 0
        for line in f:
            chunk_writer.write(line)
            line_count += 1

            if line_count >= chunk_size:
                chunk_writer.close()
                chunk_num += 1
                chunk_file = f"chunk_{chunk_num}.csv"
                chunk_writer = open(chunk_file, 'w')
                chunk_writer.write(header)
                chunks.append(chunk_file)
                line_count = 0

        chunk_writer.close()

    return chunks

def import_chunk(db_path, chunk_file, vertex_type):
    """Import single chunk."""
    db = arcadedb.open_database(db_path)
    importer = db.get_importer()
    importer.batch_size = 10000
    importer.import_csv(chunk_file, vertex_type)
    db.close()
    os.remove(chunk_file)

# Split file
chunks = split_csv("large_data.csv", chunk_size=100000)

# Import in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(import_chunk, "./mydb", chunk, "Data")
        for chunk in chunks
    ]

    for future in concurrent.futures.as_completed(futures):
        future.result()

print(f"Imported {len(chunks)} chunks")
```

---

### Disable Indexes During Import

For massive imports, temporarily disable indexes:

```python
# 1. Drop indexes
with db.transaction():
    db.command("sql", "DROP INDEX User.email")
    db.command("sql", "DROP INDEX User.id")

# 2. Import data
importer.batch_size = 100000
importer.import_csv("huge_file.csv", "User")

# 3. Recreate indexes
with db.transaction():
    db.command("sql", "CREATE INDEX ON User (id) UNIQUE")
    db.command("sql", "CREATE INDEX ON User (email) UNIQUE")
```

**Speed Improvement:** 2-5x faster for large imports

---

### Memory Management

Monitor and control memory usage:

```python
import psutil
import gc

def import_with_memory_monitoring(importer, file_path, vertex_type):
    """Import with memory monitoring."""
    process = psutil.Process()

    # Configure for memory efficiency
    importer.batch_size = 5000  # Smaller batches

    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    print(f"Initial memory: {initial_memory:.1f} MB")

    # Import
    importer.import_csv(file_path, vertex_type)

    # Force garbage collection
    gc.collect()

    final_memory = process.memory_info().rss / 1024 / 1024
    print(f"Final memory: {final_memory:.1f} MB")
    print(f"Memory increase: {final_memory - initial_memory:.1f} MB")

# Usage
import_with_memory_monitoring(importer, "data.csv", "Data")
```

## Error Handling

### Validation Before Import

```python
import csv

def validate_csv(file_path, required_columns):
    """Validate CSV before importing."""
    errors = []

    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)

            # Check headers
            missing = set(required_columns) - set(reader.fieldnames)
            if missing:
                errors.append(f"Missing columns: {missing}")
                return False, errors

            # Check data
            for i, row in enumerate(reader, start=2):
                # Validate required fields
                for col in required_columns:
                    if not row.get(col):
                        errors.append(f"Line {i}: Missing {col}")

                # Validate types (example)
                if row.get('age') and not row['age'].isdigit():
                    errors.append(f"Line {i}: Invalid age '{row['age']}'")

                # Stop after 100 errors
                if len(errors) >= 100:
                    errors.append("... more errors found")
                    return False, errors

        return len(errors) == 0, errors

    except Exception as e:
        return False, [f"File error: {e}"]

# Validate before import
valid, errors = validate_csv("users.csv", ["id", "name", "email"])
if valid:
    importer.import_csv("users.csv", "User")
else:
    print("Validation errors:")
    for error in errors:
        print(f"  - {error}")
```

---

### Try-Catch with Logging

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_import(importer, file_path, vertex_type):
    """Import with error handling."""
    try:
        logger.info(f"Starting import: {file_path} -> {vertex_type}")

        importer.import_csv(file_path, vertex_type, header=True)

        logger.info(f"Import successful: {file_path}")
        return True

    except arcadedb.ArcadeDBError as e:
        logger.error(f"Import failed: {e}")

        # Try to recover
        if "duplicate" in str(e).lower():
            logger.warning("Duplicate key error - trying without unique constraint")
            # Could drop index and retry

        return False

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return False

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

# Usage
success = safe_import(importer, "data.csv", "Data")
```

---

### Partial Import Recovery

```python
def import_with_recovery(db, file_path, vertex_type, checkpoint_interval=10000):
    """Import with periodic checkpoints."""
    importer = db.get_importer()

    # Track progress
    checkpoint_file = f"{file_path}.checkpoint"
    start_line = 0

    # Load checkpoint
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            start_line = int(f.read())
        logger.info(f"Resuming from line {start_line}")

    # Import with checkpoints
    try:
        imported = 0
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)

            # Skip to start line
            for _ in range(start_line):
                next(reader)

            # Import in batches
            batch = []
            for i, row in enumerate(reader, start=start_line):
                batch.append(row)

                if len(batch) >= checkpoint_interval:
                    # Import batch
                    with db.transaction():
                        for record in batch:
                            vertex = db.new_vertex(vertex_type)
                            for key, value in record.items():
                                vertex.set(key, value)
                            vertex.save()

                    imported += len(batch)
                    batch = []

                    # Save checkpoint
                    with open(checkpoint_file, 'w') as cf:
                        cf.write(str(i + 1))

                    logger.info(f"Checkpoint: {i + 1} lines imported")

            # Import remaining
            if batch:
                with db.transaction():
                    for record in batch:
                        vertex = db.new_vertex(vertex_type)
                        for key, value in record.items():
                            vertex.set(key, value)
                        vertex.save()
                imported += len(batch)

        # Remove checkpoint file on success
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)

        logger.info(f"Import complete: {imported} records")
        return True

    except Exception as e:
        logger.error(f"Import failed at checkpoint: {e}")
        return False

# Usage - can resume if interrupted
import_with_recovery(db, "large_file.csv", "Data")
```

## Relationship Mapping

### CSV with Relationships

Import entities and relationships from separate CSV files:

```python
# Step 1: Import users
importer.import_csv("users.csv", "User")

# Step 2: Import products
importer.import_csv("products.csv", "Product")

# Step 3: Import relationships from CSV
# purchases.csv:
# user_id,product_id,date,amount
# 1,101,2024-01-15,29.99

import csv

with open("purchases.csv", 'r') as f:
    reader = csv.DictReader(f)

    with db.transaction():
        for row in reader:
            # Find vertices
            user_result = db.query("sql",
                f"SELECT FROM User WHERE id = '{row['user_id']}'")
            product_result = db.query("sql",
                f"SELECT FROM Product WHERE id = '{row['product_id']}'")

            if user_result.has_next() and product_result.has_next():
                user = user_result.next()
                product = product_result.next()

                # Create edge
                edge = user.new_edge("Purchased", product)
                edge.set("date", row['date'])
                edge.set("amount", float(row['amount']))
                edge.save()
```

---

### JSON with Embedded Relationships

```python
# orders.json structure:
# [
#   {
#     "order_id": "ORD123",
#     "customer": {"id": "CUST1", "name": "Alice"},
#     "items": [
#       {"product_id": "PROD1", "qty": 2},
#       {"product_id": "PROD2", "qty": 1}
#     ]
#   }
# ]

import json

with open("orders.json", 'r') as f:
    orders = json.load(f)

with db.transaction():
    for order in orders:
        # Create or find customer
        customer_id = order['customer']['id']
        customer_result = db.query("sql",
            f"SELECT FROM Customer WHERE id = '{customer_id}'")

        if customer_result.has_next():
            customer = customer_result.next()
        else:
            customer = db.new_vertex("Customer")
            customer.set("id", customer_id)
            customer.set("name", order['customer']['name'])
            customer.save()

        # Create order vertex
        order_vertex = db.new_vertex("Order")
        order_vertex.set("order_id", order['order_id'])
        order_vertex.save()

        # Link customer to order
        edge = customer.new_edge("Placed", order_vertex)
        edge.save()

        # Link order to products
        for item in order['items']:
            product_result = db.query("sql",
                f"SELECT FROM Product WHERE id = '{item['product_id']}'")

            if product_result.has_next():
                product = product_result.next()

                contains_edge = order_vertex.new_edge("Contains", product)
                contains_edge.set("quantity", item['qty'])
                contains_edge.save()
```

## Production Patterns

### Configuration File

```python
import yaml
from dataclasses import dataclass

@dataclass
class ImportConfig:
    file_path: str
    vertex_type: str
    batch_size: int = 10000
    delimiter: str = ","
    header: bool = True
    validate: bool = True

def load_import_config(config_file):
    """Load import configuration from YAML."""
    with open(config_file, 'r') as f:
        config_data = yaml.safe_load(f)

    return ImportConfig(**config_data)

# config.yml:
# file_path: "data/users.csv"
# vertex_type: "User"
# batch_size: 10000
# delimiter: ","
# header: true
# validate: true

# Usage
config = load_import_config("import_config.yml")
importer.batch_size = config.batch_size
importer.import_csv(
    config.file_path,
    config.vertex_type,
    delimiter=config.delimiter,
    header=config.header
)
```

---

### Import Pipeline

```python
class ImportPipeline:
    def __init__(self, db):
        self.db = db
        self.importer = db.get_importer()
        self.stats = {
            'files_processed': 0,
            'records_imported': 0,
            'errors': []
        }

    def add_csv_step(self, file_path, vertex_type, **kwargs):
        """Add CSV import step."""
        try:
            self.importer.import_csv(file_path, vertex_type, **kwargs)
            self.stats['files_processed'] += 1
            logger.info(f"Imported {file_path}")
        except Exception as e:
            self.stats['errors'].append(f"{file_path}: {e}")
            logger.error(f"Failed to import {file_path}: {e}")

    def add_json_step(self, file_path, vertex_type):
        """Add JSON import step."""
        try:
            self.importer.import_json(file_path, vertex_type)
            self.stats['files_processed'] += 1
            logger.info(f"Imported {file_path}")
        except Exception as e:
            self.stats['errors'].append(f"{file_path}: {e}")
            logger.error(f"Failed to import {file_path}: {e}")

    def run(self):
        """Execute pipeline."""
        logger.info("Starting import pipeline")
        # Steps added via add_*_step methods
        logger.info(f"Pipeline complete: {self.stats}")
        return self.stats

# Usage
pipeline = ImportPipeline(db)
pipeline.add_csv_step("users.csv", "User", batch_size=10000)
pipeline.add_csv_step("products.csv", "Product", batch_size=10000)
pipeline.add_json_step("orders.json", "Order")
stats = pipeline.run()
```

---

### Monitoring and Progress

```python
import time
from tqdm import tqdm

def import_with_progress(file_path, vertex_type, db):
    """Import with progress bar."""
    # Count lines
    with open(file_path, 'r') as f:
        total_lines = sum(1 for _ in f) - 1  # Exclude header

    importer = db.get_importer()

    # Import with progress tracking
    start_time = time.time()

    with tqdm(total=total_lines, desc=f"Importing {vertex_type}") as pbar:
        # Monkey-patch to update progress
        original_import = importer.import_csv

        def import_with_callback(*args, **kwargs):
            result = original_import(*args, **kwargs)
            pbar.update(total_lines)
            return result

        importer.import_csv = import_with_callback
        importer.import_csv(file_path, vertex_type, header=True)

    elapsed = time.time() - start_time
    rate = total_lines / elapsed if elapsed > 0 else 0

    print(f"Imported {total_lines} records in {elapsed:.1f}s ({rate:.0f} records/sec)")

# Usage
import_with_progress("data.csv", "Data", db)
```

## Common Use Cases

### Migrate from Relational Database

```python
import sqlite3
import arcadedb_embedded as arcadedb

# Export from SQLite
conn = sqlite3.connect('old_database.db')
cursor = conn.cursor()

# Create ArcadeDB
db = arcadedb.create_database("./new_db")

# Migrate users table
cursor.execute("SELECT id, name, email, created_at FROM users")
users = cursor.fetchall()

with db.transaction():
    for user_id, name, email, created_at in users:
        vertex = db.new_vertex("User")
        vertex.set("id", str(user_id))
        vertex.set("name", name)
        vertex.set("email", email)
        vertex.set("created_at", created_at)
        vertex.save()

# Migrate relationships
cursor.execute("""
    SELECT user_id, friend_id
    FROM friendships
""")

with db.transaction():
    for user_id, friend_id in cursor.fetchall():
        user = db.query("sql", f"SELECT FROM User WHERE id = '{user_id}'").next()
        friend = db.query("sql", f"SELECT FROM User WHERE id = '{friend_id}'").next()

        edge = user.new_edge("FriendOf", friend)
        edge.save()

conn.close()
print("Migration complete")
```

---

### Import from API

```python
import requests

def import_from_api(db, api_url, vertex_type):
    """Import data from REST API."""
    response = requests.get(api_url)
    response.raise_for_status()

    data = response.json()

    with db.transaction():
        for item in data:
            vertex = db.new_vertex(vertex_type)
            for key, value in item.items():
                vertex.set(key, value)
            vertex.save()

    print(f"Imported {len(data)} records from API")

# Usage
import_from_api(
    db,
    "https://api.example.com/users",
    "User"
)
```

---

### Incremental Updates

```python
from datetime import datetime

def incremental_import(db, file_path, vertex_type, id_field):
    """Import only new records based on timestamp."""
    # Get last import time
    last_import_key = f"last_import_{vertex_type}"
    last_import = db.get_metadata(last_import_key) or "1970-01-01"

    importer = db.get_importer()

    # Import with filter
    import csv
    new_records = 0

    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)

        with db.transaction():
            for row in reader:
                # Check if record is new
                if row.get('updated_at', '') > last_import:
                    # Check if exists
                    result = db.query("sql",
                        f"SELECT FROM {vertex_type} WHERE {id_field} = '{row[id_field]}'")

                    if result.has_next():
                        # Update existing
                        vertex = result.next()
                        for key, value in row.items():
                            vertex.set(key, value)
                        vertex.save()
                    else:
                        # Create new
                        vertex = db.new_vertex(vertex_type)
                        for key, value in row.items():
                            vertex.set(key, value)
                        vertex.save()

                    new_records += 1

    # Update last import time
    current_time = datetime.now().isoformat()
    db.set_metadata(last_import_key, current_time)

    print(f"Imported/updated {new_records} records")

# Usage
incremental_import(db, "users.csv", "User", "id")
```

## See Also

- [Importer API Reference](../api/importer.md) - Complete API documentation
- [Import Examples](../examples/import.md) - Practical code examples
- [Database API](../api/database.md) - Database operations
- [Transactions](../api/transactions.md) - Transaction management
