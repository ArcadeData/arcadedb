# ArcadeDB Python Bindings - Tests

Comprehensive test suite for the ArcadeDB Python embedded bindings.

For detailed test documentation, examples, and best practices, see the **[Testing Guide](https://docs.humem.ai/arcadedb/latest/development/testing/)**

## Quick Stats

- Current bindings suite
- Package includes the embedded ArcadeDB features (SQL, OpenCypher, vectors,
  graphs) **and** the optional in-process HTTP server with Studio

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
| `test_server.py` | Server lifecycle, HTTP API, Studio, configuration |
| `test_server_packaging.py` | The server stack is really in the wheel (fails, never skips) |
| `test_server_patterns.py` | Embedded, server-managed, and HTTP access patterns |
| `test_concurrency.py` | File locking, thread safety, multi-process |
| `test_import_database.py` | SQL `IMPORT DATABASE`, CSV/XML/Neo4j and restore flows |
| `test_docs_examples.py` | Validates runnable Python snippets from installation, quickstart, query, graph, and API-access docs |
| `test_cypher.py` | OpenCypher query language, path modes, and planner regressions |

### A note on the server tests

`test_server.py` and `test_server_patterns.py` skip themselves when server
support is absent, which is convenient but has a failure mode: in 26.7.2 the
server JARs were dropped from the wheel and those tests **skipped instead of
failing**, so the suite stayed green while the feature was gone. That is what
`test_server_packaging.py` is for. It never skips. If you deliberately want a
slim wheel, delete that file on purpose rather than letting the suite go quiet.

```bash
uv run pytest -m server -v      # only the server tests
uv run pytest -m "not server"   # skip them (they start a real HTTP listener)
```

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
# Multiple processes CANNOT open the same database file (file lock).
# Server mode is the in-process answer: one process holds the database and
# serves HTTP, everyone else connects over it.

server = arcadedb.create_server("./databases", root_password="...")
server.start()
# "mydb" is created at ./databases/databases/mydb
db = server.create_database("mydb")

# The owning process keeps embedded (in-JVM) access to db, and HTTP clients
# in other processes reach the same data through the server.
```

For a database whose lifetime should outlive your Python process, or for
HA/TLS, run the official ArcadeDB server distribution instead. See
[Server Mode](https://docs.humem.ai/arcadedb/latest/guide/server/).

## Need Help?

- **Questions?** See the [Testing Guide](https://docs.humem.ai/arcadedb/latest/development/testing/)
- **Found a bug?** [Open an issue](https://github.com/humemai/arcadedb-embedded-python/issues)
- **Contributing?** Read [Contributing Guide](https://docs.humem.ai/arcadedb/latest/development/contributing/)
