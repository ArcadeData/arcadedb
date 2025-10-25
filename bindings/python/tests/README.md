# ArcadeDB Python Bindings - Tests

Comprehensive test suite for the ArcadeDB Python embedded bindings.

!!! info "📚 Full Documentation"
    For detailed test documentation, examples, and best practices, see the **[Testing Guide](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/)**

## Quick Stats

- **43 tests** across 6 test files
- ✅ **Headless**: 36 passed, 7 skipped
- ✅ **Minimal**: 40 passed, 3 skipped
- ✅ **Full**: 43 passed, 0 skipped

## Running Tests

```bash
# Install dependencies
pip install pytest pytest-cov

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
| `test_importer.py` | 13 | CSV, JSON, JSONL, Neo4j import |
| `test_gremlin.py` | 1 | Gremlin query language (full only) |

## Documentation Links

- **[Testing Overview](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/overview/)** - Quick start guide
- **[Core Tests](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/test-core/)** - Database operations
- **[Server Tests](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/test-server/)** - HTTP API
- **[Concurrency Tests](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/test-concurrency/)** - Multi-process, threads
- **[Server Patterns](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/test-server-patterns/)** - Best practices
- **[Data Import Tests](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/test-importer/)** - CSV, JSON import
- **[Gremlin Tests](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/test-gremlin/)** - Graph queries
- **[Best Practices](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/best-practices/)** - Summary checklist

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

server = arcadedb.create_server(root_path="./databases")
server.start()
db = server.create_database("mydb")

# Now HTTP clients from other processes can connect!
```

### Server Best Practice ⭐
```python
# Pattern 2: Start server first (recommended)
server = arcadedb.create_server(root_path="./databases")
server.start()
db = server.create_database("mydb")

# Both embedded + HTTP work immediately
# No manual lock management needed
```

## Need Help?

- **Questions?** See the [Testing Guide](https://humemai.github.io/arcadedb-embedded-python/latest/development/testing/)
- **Found a bug?** [Open an issue](https://github.com/humemai/arcadedb/issues)
- **Contributing?** Read [Contributing Guide](https://github.com/humemai/arcadedb/blob/python-embedded/CONTRIBUTING.md)
