# ArcadeDB Python E2E Tests

This directory contains end-to-end tests for ArcadeDB using Python.

## Setup

This project uses UV as the package manager.

```bash
# Install UV if not already installed
pip install uv

# Install dependencies
uv pip install -e .
```

## Running Tests

```bash
pytest
```

## asyncpg Tests

Tests for PostgreSQL wire protocol compatibility using the Python asyncpg driver.

### Related Issues

- [#1630](https://github.com/ArcadeData/arcadedb/issues/1630) - Bind message parsing with asyncpg
- [#668](https://github.com/ArcadeData/arcadedb/issues/668) - asyncpg compatibility

### Prerequisites

```bash
cd e2e-python
pip install -e .
```

### Running Tests

```bash
# All asyncpg tests
pytest tests/test_asyncpg.py -v

# Specific test
pytest tests/test_asyncpg.py::test_parameterized_select -v

# With logging
pytest tests/test_asyncpg.py -v --log-cli-level=DEBUG
```

### Test Coverage

1. **Basic Connection** - Verify asyncpg can connect to ArcadeDB
2. **Simple Queries** - SELECT queries without parameters
3. **Type Creation** - CREATE DOCUMENT TYPE and INSERT operations
4. **Parameterized Queries** (Issue #1630)
   - Single parameter SELECT
   - Multiple parameter SELECT
   - Parameterized INSERT
5. **Transactions** - Transaction commit and rollback support

### Container Management

Tests use testcontainers to automatically:
- Pull arcadedata/arcadedb:latest image
- Start container with PostgreSQL plugin enabled
- Create asyncpg_testdb database
- Wait for server readiness
- Clean up after tests
