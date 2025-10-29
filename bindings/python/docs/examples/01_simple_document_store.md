# Simple Document Store Example

This comprehensive example demonstrates ArcadeDB's document capabilities using a task management system. You'll learn about data types, NULL handling, SQL operations, and the differences between document and graph storage models.

## Overview

The example creates a task management system showcasing:

- **Rich Data Types** - STRING, BOOLEAN, INTEGER, FLOAT, DECIMAL, DATE, DATETIME, LIST OF STRING, and Arrays
- **NULL Handling** - INSERT with NULL, UPDATE to NULL, queries with IS NULL/IS NOT NULL
- **SQL Operations** - Complete CRUD workflow with ArcadeDB SQL
- **Built-in Functions** - date() for date literals, uuid() for unique IDs, sysdate() for dynamic timestamps
- **Record Types** - Understanding Documents vs Vertices vs Edges
- **Schema Flexibility** - Typed properties for performance with schema-optional flexibility
- **Type Safety** - LIST OF STRING for validated array data

## Source Code

The complete example is available at: [`examples/01_simple_document_store.py`](../../examples/01_simple_document_store.py)

## Key Learning Points

### 1. Data Type Support

ArcadeDB provides comprehensive data type support with NULL handling:

```python
# Schema with typed properties for performance and validation
with db.transaction():
    db.command("sql", "CREATE DOCUMENT TYPE Task")
    db.command("sql", "CREATE PROPERTY Task.title STRING")
    db.command("sql", "CREATE PROPERTY Task.priority STRING")
    db.command("sql", "CREATE PROPERTY Task.completed BOOLEAN")
    db.command("sql", "CREATE PROPERTY Task.tags LIST OF STRING")  # Type-safe arrays
    db.command("sql", "CREATE PROPERTY Task.created_date DATE")
    db.command("sql", "CREATE PROPERTY Task.due_datetime DATETIME")
    db.command("sql", "CREATE PROPERTY Task.estimated_hours FLOAT")
    db.command("sql", "CREATE PROPERTY Task.priority_score INTEGER")
    db.command("sql", "CREATE PROPERTY Task.cost DECIMAL")
    db.command("sql", "CREATE PROPERTY Task.task_id STRING")

# Insert with NULL values for optional fields and uuid() for unique ID
db.command("sql", """
    INSERT INTO Task SET
        title = 'Write documentation',
        priority = 'medium',
        completed = false,
        tags = ['work', 'writing'],
        created_date = date('2024-01-16'),
        due_datetime = NULL,
        estimated_hours = 8.0,
        priority_score = 70,
        cost = NULL,
        task_id = uuid()
""")
```

### 2. SQL Functions and NULL Queries

Learn about built-in functions and NULL handling:

```python
# Built-in functions: date() for DATE type, uuid() for unique IDs
db.command("sql", """
    INSERT INTO Task SET
        title = 'Buy groceries',
        task_id = uuid(),
        created_date = date('2024-01-15'),
        due_datetime = '2024-01-20 18:00:00',
        cost = 150.00
""")

# Query for NULL values
result = db.query("sql", "SELECT FROM Task WHERE due_datetime IS NULL")
result = db.query("sql", "SELECT FROM Task WHERE cost IS NULL")

# UPDATE to set NULL (clear optional values)
db.command("sql", """
    UPDATE Task SET
        cost = NULL,
        estimated_hours = NULL
    WHERE title = 'Call dentist'
""")
```

### 3. Record Types Explained

Understanding when to use different record types:

- **Document** - Like database tables, for simple data storage
- **Vertex** - Graph nodes representing entities
- **Edge** - Graph connections representing relationships

### 4. Advanced Features

The example demonstrates:

- **NULL Values** - Optional fields with IS NULL/IS NOT NULL queries
- **Type-Safe Arrays** - LIST OF STRING for validated collections
- **DECIMAL Handling** - Java BigDecimal conversion via float(str(value))
- **DATETIME Literals** - String literals automatically parsed to DATETIME type
- **Schema-Optional Flexibility** - Define properties for performance, add ad-hoc fields when needed
- **Query Optimization** - Using typed properties and indexes

## Running the Example

```bash
cd bindings/python/examples/
python 01_simple_document_store.py
```

Expected output includes:

- Database creation and schema setup
- Sample tasks with various data types and NULL values
- Query demonstrations including NULL checks
- UPDATE operations setting values to NULL
- File structure explanation

## Database Structure

After running, examine the created files:

```text
my_test_databases/task_db/
├── configuration.json     # Database configuration
├── schema.json           # Type definitions with LIST OF STRING
├── Task_*.bucket         # Data storage files with tasks
├── dictionary.*.dict     # String compression dictionary
└── statistics.json       # Database statistics
```

## Next Steps

After mastering this example:

1. **Explore Graph Operations** - Learn about vertices and edges
2. **Try Vector Search** - Modern AI/ML integration
3. **Review API Documentation** - Deep dive into advanced features

## Common Questions

**Q: How does ArcadeDB handle NULL values?**
A: All ArcadeDB types support NULL by default. You can INSERT NULL, UPDATE to NULL, and query with IS NULL/IS NOT NULL operators.

**Q: What's the difference between LIST and LIST OF STRING?**
A: LIST is a generic untyped list. LIST OF STRING provides type validation ensuring all elements are strings, giving better performance and data integrity.

**Q: Why use typed properties?**
A: They provide better performance, validation, and enable advanced features like indexes. But ArcadeDB is schema-optional - you can still add properties dynamically.

**Q: When should I use Documents vs Vertices?**
A: Use Documents for simple data storage (like SQL tables). Use Vertices when you need to model relationships between entities with Edges.

**Q: How do I handle Java BigDecimal in Python?**
A: Convert via string first: `float(str(decimal_value))`. Direct conversion from Java BigDecimal to Python float requires string intermediary.

**Q: Can I mix data types?**
A: Yes! ArcadeDB is schema-flexible. You can add properties dynamically while benefiting from typed properties where defined.

---

*Need help? Check our [troubleshooting guide](../troubleshooting.md) or [open an issue](https://github.com/humemai/arcadedb-embedded-python/issues).*
