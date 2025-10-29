# Troubleshooting Guide

Common issues and solutions when working with ArcadeDB Python bindings.

## Installation Issues

### Java Runtime Not Found

**Problem**: `JAVA_HOME not set` or `Java runtime not found`

**Solution**:
```bash
# Check Java installation
java -version

# Set JAVA_HOME if needed
export JAVA_HOME=/path/to/java
```

### Package Import Errors

**Problem**: `ModuleNotFoundError: No module named 'arcadedb'`

**Solution**:
```bash
# Install the embedded distribution
pip install arcadedb-embedded
```

## Runtime Issues

### Database Connection Errors

**Problem**: Cannot connect to database or server

**Solutions**:

1. **Embedded Mode** - Check file permissions:
   ```python
   import os
   os.makedirs("databases/mydb", exist_ok=True)
   ```

2. **Server Mode** - Verify server is running:
   ```bash
   # Check if server is listening
   curl http://localhost:2480/api/v1/ready
   ```

### Memory Issues

**Problem**: `OutOfMemoryError` or slow performance

**Solutions**:

1. **Increase JVM Memory**:
   ```python
   import os
   os.environ["JAVA_OPTS"] = "-Xmx4g -Xms1g"
   ```

2. **Use Transactions for Large Operations**:
   ```python
   db.transaction(lambda: bulk_operations())
   ```

### Data Type Issues

**Problem**: Unexpected data types in query results

**Solutions**:

1. **Java Objects in Results**:
   ```python
   # Convert Java objects to Python
   if hasattr(result, 'entrySet'):
       result = dict(result)
   ```

2. **UUID Storage**:
   ```python
   # Store UUIDs as strings
   db.command("sql", "INSERT INTO Product SET id = uuid()")
   ```

## SQL Query Issues

### Function Name Errors

**Problem**: `datetime()` function not found

**Solution**: Use correct function names:
```python
# ❌ Wrong
db.command("sql", "SELECT datetime() as now")

# ✅ Correct
db.command("sql", "SELECT sysdate() as now")
```

### Multi-line Query Issues

**Problem**: SQL parser errors with complex queries

**Solution**: Use single-line queries or proper escaping:
```python
# ✅ Single line
query = "INSERT INTO Product SET name = 'test', created_at = sysdate()"

# ✅ Multi-line with proper formatting
query = """
INSERT INTO Product SET
    name = 'test',
    created_at = sysdate()
""".strip()
```

## Performance Issues

### Slow Queries

**Solutions**:

1. **Add Indexes**:
   ```python
   db.command("sql", "CREATE INDEX Product.name ON Product (name) NOTUNIQUE")
   ```

2. **Use Typed Properties**:
   ```python
   db.command("sql", "CREATE DOCUMENT TYPE Product (name STRING)")
   ```

### Large Result Sets

**Solutions**:

1. **Use Pagination**:
   ```python
   db.command("sql", "SELECT * FROM Product LIMIT 100 SKIP 0")
   ```

2. **Stream Results**:
   ```python
   for record in db.command("sql", "SELECT * FROM Product"):
       process_record(record)
   ```

## Development Issues

### Test Failures

**Problem**: Unit tests failing unexpectedly

**Solutions**:

1. **Clean Test Environment**:
   ```python
   import shutil
   shutil.rmtree("databases/test_db", ignore_errors=True)
   ```

2. **Proper Test Isolation**:
   ```python
   def setUp(self):
       self.db = database.create("databases/test_" + uuid4().hex[:8])
   ```

### Documentation Build Issues

**Problem**: mkdocs build failures

**Solutions**:

1. **Install Dependencies**:
   ```bash
   pip install mkdocs mkdocs-material
   ```

2. **Check Markdown Syntax**:
   ```bash
   # Lint markdown files
   markdownlint docs/
   ```

## Getting Help

### Community Support

- **[GitHub Issues](https://github.com/humemai/arcadedb-embedded-python/issues)** - Bug reports and feature requests
- **[Discussions](https://github.com/humemai/arcadedb-embedded-python/discussions)** - Community Q&A
- **[Documentation](https://docs.arcadedb.com/)** - Official ArcadeDB docs

### Debugging Tips

1. **Enable Logging**:
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Check Database Files**:
   ```python
   # Examine database structure
   import os
   print(os.listdir("databases/mydb/"))
   ```

3. **Validate Schema**:
   ```python
   # Check current schema
   result = db.command("sql", "SELECT FROM schema:types")
   ```

### Reporting Issues

When reporting bugs, include:

- Python version and OS
- ArcadeDB version (`pip show arcadedb-embedded`)
- Minimal code example
- Full error traceback
- Expected vs actual behavior

---

*Still having issues? [Open a GitHub issue](https://github.com/humemai/arcadedb-embedded-python/issues/new) with details.*
