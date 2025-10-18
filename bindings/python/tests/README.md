# ArcadeDB Python Bindings Tests

This directory contains tests organized by feature area.

## Test Files

- **`test_core.py`**: Core functionality that works in ALL distributions (headless, minimal, full)
  - Database creation/operations
  - Transactions
  - Graph operations
  - SQL and Cypher queries
  - Vector search basics
  - Error handling

- **`test_server.py`**: Server and Studio features (requires minimal or full)
  - Server startup/shutdown
  - HTTP API
  - Studio UI availability
  - Database operations through server

- **`test_gremlin.py`**: Gremlin query language (requires full distribution)
  - Gremlin queries
  - Graph traversal with Gremlin

- **`conftest.py`**: Shared fixtures and pytest configuration
  - `temp_db_path`: Temporary database path fixture
  - `temp_server_root`: Temporary server root fixture
  - Feature detection helpers

## Running Tests

### Run all tests (for your installed distribution)
```bash
pytest tests/
```

### Run only core tests (works with any distribution)
```bash
pytest tests/test_core.py
```

### Run server tests (requires minimal or full)
```bash
pytest tests/test_server.py
# OR
pytest -m server
```

### Run Gremlin tests (requires full distribution)
```bash
pytest tests/test_gremlin.py
# OR
pytest -m gremlin
```

### Run with coverage
```bash
pytest tests/ --cov=arcadedb_embedded --cov-report=html
```

## Test Markers

Tests use pytest markers to indicate distribution requirements:

- `@pytest.mark.server`: Requires minimal or full distribution
- `@pytest.mark.gremlin`: Requires full distribution only

Tests automatically skip if the required features aren't available.

## Expected Results by Distribution

### Headless Distribution
- ✅ `test_core.py`: All tests pass
- ⏭️ `test_server.py`: All tests skipped
- ⏭️ `test_gremlin.py`: All tests skipped

### Minimal Distribution
- ✅ `test_core.py`: All tests pass
- ✅ `test_server.py`: All tests pass
- ⏭️ `test_gremlin.py`: All tests skipped

### Full Distribution
- ✅ `test_core.py`: All tests pass
- ✅ `test_server.py`: All tests pass
- ✅ `test_gremlin.py`: All tests pass (if Gremlin is properly configured)
