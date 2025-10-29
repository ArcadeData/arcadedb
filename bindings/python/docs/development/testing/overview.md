# Testing Overview

The ArcadeDB Python bindings have a comprehensive test suite covering all major functionality.

## Quick Statistics

!!! success "Test Results by Distribution"
    - **Headless**: ✅ 36 passed, 7 skipped (Cypher, Gremlin, Server tests)
    - **Minimal**: ✅ 40 passed, 3 skipped (Cypher, Gremlin tests)
    - **Full**: ✅ 43 passed, 0 skipped (all features available)

    **Total: 43 tests** across 6 test files, 100% passing

## What's Tested

The test suite covers:

- ✅ **Core database operations** - CRUD, transactions, queries
- ✅ **Server mode** - HTTP API, multi-client access
- ✅ **Concurrency patterns** - File locking, thread safety, multi-process
- ✅ **Graph operations** - Vertices, edges, traversals
- ✅ **Query languages** - SQL, Cypher, Gremlin
- ✅ **Vector search** - HNSW indexes, similarity search
- ✅ **Data import** - CSV, JSON, Neo4j exports
- ✅ **Unicode support** - International characters, emoji
- ✅ **Schema introspection** - Querying database metadata
- ✅ **Type conversions** - Python/Java type mapping
- ✅ **Large datasets** - Handling 1000+ records efficiently

## Quick Start

### Install Test Dependencies

```bash
pip install pytest pytest-cov
```

### Run All Tests

```bash
# From the bindings/python directory
pytest

# With verbose output
pytest -v

# With coverage report
pytest --cov=arcadedb_embedded --cov-report=html
```

### Run Specific Tests

```bash
# Run a specific test file
pytest tests/test_core.py

# Run a specific test function
pytest tests/test_core.py::test_database_creation

# Run tests matching a keyword
pytest -k "transaction"
pytest -k "server"
pytest -k "concurrency"

# Run with output (see print statements)
pytest -v -s
```

## Test Files Overview

| Test File | Tests | Description |
|-----------|-------|-------------|
| [`test_core.py`](test-core.md) | 13 | Core database operations, CRUD, transactions, queries |
| [`test_server.py`](test-server.md) | 6 | Server mode, HTTP API, configuration |
| [`test_concurrency.py`](test-concurrency.md) | 4 | File locking, thread safety, multi-process behavior |
| [`test_server_patterns.py`](test-server-patterns.md) | 4 | Best practices for embedded + server mode |
| [`test_importer.py`](test-importer.md) | 12 | CSV, JSON, Neo4j import |
| [`test_gremlin.py`](test-gremlin.md) | 1 | Gremlin query language (if available) |

## Common Testing Workflows

### Development Workflow

```bash
# Watch mode - rerun tests on file changes
pytest --watch

# Run only failed tests from last run
pytest --lf

# Run tests in parallel (faster)
pytest -n auto
```

### Debugging Tests

```bash
# Stop on first failure
pytest -x

# Drop into debugger on failure
pytest --pdb

# Show local variables on failure
pytest -l

# Verbose with full output
pytest -vv -s
```

### Distribution-Specific Testing

Some tests are automatically skipped based on your distribution:

```python
@pytest.mark.server
def test_server_feature():
    # Skipped in headless distribution
    pass

@pytest.mark.gremlin
def test_gremlin_query():
    # Skipped in minimal and headless distributions
    pass
```

## Test Markers

Tests are organized with pytest markers:

```bash
# Run only server tests
pytest -m server

# Run only Gremlin tests
pytest -m gremlin

# Run all except slow tests
pytest -m "not slow"
```

## Expected Output

When all tests pass, you should see:

```
======================== 43 passed in 9.67s =========================
```

With some distributions, you may see skipped tests

## Next Steps

- **New to testing?** Start with [Core Tests](test-core.md)
- **Using server mode?** See [Server Tests](test-server.md) and [Server Patterns](test-server-patterns.md)
- **Confused about concurrency?** Read [Concurrency Tests](test-concurrency.md)
- **Importing data?** Check [Data Import Tests](test-importer.md)
- **Using Gremlin?** See [Gremlin Tests](test-gremlin.md)

## Related Documentation

- [API Reference](../../api/database.md) - Database API documentation
- [User Guide](../../guide/core/database.md) - Database usage guide
- [Contributing](contributing.md) - How to contribute tests
