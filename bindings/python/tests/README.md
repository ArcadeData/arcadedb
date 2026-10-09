# ArcadeDB Python Bindings - Tests

Test suite for the ArcadeDB Python bindings. It runs against the built wheel, so it covers the embedded engine (SQL, OpenCypher, vectors, graphs) and the optional in-process HTTP server with Studio.

The [Testing Guide](https://docs.humem.ai/arcadedb/latest/development/testing/) lists every test file and what it checks, and describes the markers, the zero-skips gate, and how to write a test for this suite.

## Running Tests

```bash
# Run all tests (dependencies come from the repo-root uv project)
uv run pytest

# Run specific file
uv run pytest tests/test_core.py -v

# Run matching keyword
uv run pytest -k "transaction" -v

# Only the server tests, or everything except them (they start a real HTTP listener)
uv run pytest -m server -v
uv run pytest -m "not server"
```

## No skips

The server tests do not skip when the server stack is absent: the wheel always ships it, and a test that skips on a missing feature cannot notice the feature going missing (in 26.7.2 the server JARs were dropped from the wheel and the guarded tests skipped, so the suite stayed green while the feature was gone). `test_server_packaging.py` states the packaging requirement and fails without the JARs. CI also fails any skip that `scripts/check_test_skips.py` does not list, and that list is empty.

## Need Help?

- **Found a bug?** [Open an issue](https://github.com/humemai/arcadedb-embedded-python/issues)
- **Contributing?** Read the [Contributing Guide](https://docs.humem.ai/arcadedb/latest/development/contributing/)
