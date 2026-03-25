# ArcadeDB Python Bindings - Tests

Comprehensive test suite for the ArcadeDB Python embedded bindings.

For detailed test documentation, examples, and best practices, see the **[Testing Guide](https://docs.humem.ai/arcadedb/latest/development/testing/)**

## Quick Stats

- **27 test files** in the current suite
- ✅ **Current package**: 279 passed
- Package includes all ArcadeDB features (SQL, OpenCypher, Studio)

## Running Tests

```bash
# Install dependencies
uv pip install pytest pytest-cov

# Run all tests
pytest

# Run specific file
pytest tests/test_core.py -v

# Run with coverage
pytest --cov=arcadedb_embedded --cov-report=html

# Run matching keyword
pytest -k "transaction" -v
```

## Test Files

| File | Tests | Coverage |
|------|-------|----------|
| `test_core.py` | 13 | Core CRUD, transactions, queries, graphs, vectors |
| `test_server.py` | 6 | HTTP API, Studio, configuration |
| `test_concurrency.py` | 4 | File locking, thread safety, multi-process |
| `test_server_patterns.py` | 6 | Embedded, server-managed, HTTP performance |
| `test_import_database.py` | 13 | SQL `IMPORT DATABASE`, CSV/XML/Neo4j and restore flows |
| `test_docs_examples.py` | 7 grouped tests | Validates runnable Python snippets from installation, quickstart, query, and graph docs |
| `test_cypher.py` | 18 | OpenCypher query language, path modes, and planner regressions |

## Documentation Links

- **[Testing Overview](https://docs.humem.ai/arcadedb/latest/development/testing/overview/)** - Quick start guide
- **[Core Tests](https://docs.humem.ai/arcadedb/latest/development/testing/test-core/)** - Database operations
- **[Server Tests](https://docs.humem.ai/arcadedb/latest/development/testing/test-server/)** - HTTP API
- **[Concurrency Tests](https://docs.humem.ai/arcadedb/latest/development/testing/test-concurrency/)** - Multi-process, threads
- **[Server Patterns](https://docs.humem.ai/arcadedb/latest/development/testing/test-server-patterns/)** - Best practices
- **[Data Import Tests](https://docs.humem.ai/arcadedb/latest/development/testing/test-importer/)** - SQL import workflows and format coverage
- **[OpenCypher Tests](https://docs.humem.ai/arcadedb/latest/development/testing/test-opencypher/)** - Graph queries
- **[Best Practices](https://docs.humem.ai/arcadedb/latest/development/testing/best-practices/)** - Summary checklist

## Common Patterns

### Thread Safety ✅
```python
# Multiple threads CAN access same database
import threading

db = arcadedb.create_database("./testdb")

def worker():
    db.command("sql", "INSERT INTO Person SET name = 'Alice'")

threads = [threading.Thread(target=worker) for _ in range(10)]
for t in threads: t.start()
for t in threads: t.join()
```

### Multi-Process ❌ → ✅
```python
# Multiple processes CANNOT access same database file
# Solution: Use server mode

server = arcadedb.create_server("./databases")
server.start()
# "mydb" will be created at ./databases/databases/mydb
db = server.create_database("mydb")

# Now HTTP clients from other processes can connect!
```

### Server Best Practice ⭐
```python
# Pattern 2: Start server first (recommended)
server = arcadedb.create_server("./databases")
server.start()
# "mydb" will be created at ./databases/databases/mydb
db = server.create_database("mydb")

# Both embedded + HTTP work immediately
# No manual lock management needed
```

## Need Help?

- **Questions?** See the [Testing Guide](https://docs.humem.ai/arcadedb/latest/development/testing/)
- **Found a bug?** [Open an issue](https://github.com/humemai/arcadedb-embedded-python/issues)
- **Contributing?** Read [Contributing Guide](https://docs.humem.ai/arcadedb/latest/development/contributing/)
