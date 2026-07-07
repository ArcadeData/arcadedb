# ArcadeDB Python Bindings - Tests

Comprehensive test suite for the ArcadeDB Python embedded bindings.

For detailed test documentation, examples, and best practices, see the **[Testing Guide](https://docs.humem.ai/arcadedb/latest/development/testing/)**

## Quick Stats

- Current bindings suite
- Package includes the embedded ArcadeDB features (SQL, OpenCypher, vectors, graphs)

## Running Tests

```bash
# Run all tests (dependencies come from the repo-root uv project)
uv run pytest

# Run specific file
uv run pytest tests/test_core.py -v

# Run with coverage
uv run pytest --cov=arcadedb_embedded --cov-report=html

# Run matching keyword
pytest -k "transaction" -v
```

## Test Files

| File | Coverage |
|------|----------|
| `test_core.py` | Core CRUD, transactions, queries, graphs, vectors |
| `test_concurrency.py` | File locking, thread safety, multi-process |
| `test_import_database.py` | SQL `IMPORT DATABASE`, CSV/XML/Neo4j and restore flows |
| `test_docs_examples.py` | Validates runnable Python snippets from installation, quickstart, query, and graph docs |
| `test_cypher.py` | OpenCypher query language, path modes, and planner regressions |

## Documentation Links

- **[Testing Overview](https://docs.humem.ai/arcadedb/latest/development/testing/overview/)** - Quick start guide
- **[Core Tests](https://docs.humem.ai/arcadedb/latest/development/testing/test-core/)** - Database operations
- **[Concurrency Tests](https://docs.humem.ai/arcadedb/latest/development/testing/test-concurrency/)** - Multi-process, threads
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

### Multi-Process ❌
```python
# Multiple processes CANNOT access the same database file (file lock).
# This package is embedded-only: for multi-process/client-server access,
# run the official ArcadeDB server (e.g. the arcadedata/arcadedb Docker
# image) and connect over HTTP.
```

## Need Help?

- **Questions?** See the [Testing Guide](https://docs.humem.ai/arcadedb/latest/development/testing/)
- **Found a bug?** [Open an issue](https://github.com/humemai/arcadedb-embedded-python/issues)
- **Contributing?** Read [Contributing Guide](https://docs.humem.ai/arcadedb/latest/development/contributing/)
