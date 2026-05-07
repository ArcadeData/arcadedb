# Python Bindings Codacy/Bandit Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the Bandit findings Codacy reports against `bindings/python/` (1192 issues across src/tests/examples) by fixing real defects in production code, parameterizing where practical in tests/examples, configuring Bandit to suppress test/example noise that is not actually unsafe, and wiring Bandit into the existing Python CI workflow as a quality gate.

**Architecture:** Three-PR rollout. PR1 hardens the production library (`src/`) with no `# nosec` allowed except for documented false positives; adds a project-level Bandit config to `pyproject.toml`; and adds a `bandit` job to `.github/workflows/test-python-bindings.yml`. PR2 sweeps the test suite (move shared test password to `conftest.py`, parameterize SQL where reasonable, suppress idiomatic `assert`/`random` noise via configuration). PR3 sweeps `examples/` (one-character SHA1 fix using `usedforsecurity=False`, narrow targeted suppressions for educational SQL/subprocess usage).

**Tech Stack:** Python 3.10+, Bandit 1.9+, pytest, JPype, existing GitHub Actions workflow `test-python-bindings.yml`.

**Verification baseline:**
- Before any change: `python3 -m bandit -r bindings/python/src` reports `Low: 8, Medium: 3, High: 0`.
- After PR1 (src + config + CI): `python3 -m bandit -c bindings/python/pyproject.toml -r bindings/python/src` reports `No issues identified`.
- After PR2 (tests): same command on `bindings/python/tests` reports `No issues identified` at Medium+ confidence and severity.
- After PR3 (examples): same command on `bindings/python/examples` reports `High: 0` (Low/Medium kept at advisory level only).

---

## File Structure

**Created:**
- `docs/superpowers/plans/2026-05-05-python-bindings-codacy-fixes.md` (this file)

**Modified - PR1 (src + config + CI):**
- `bindings/python/pyproject.toml` - add `[tool.bandit]` config, `bandit` to `dev` extras
- `bindings/python/src/arcadedb_embedded/_logging.py` - new helper module (single responsibility: provide module loggers + a `log_swallowed_exception` helper)
- `bindings/python/src/arcadedb_embedded/async_executor.py:207-217` - replace try/except/pass with debug log
- `bindings/python/src/arcadedb_embedded/core.py:903-911` - replace finalizer try/except/pass with narrowed except
- `bindings/python/src/arcadedb_embedded/graph_batch.py:117-125` - replace nested rollback try/except/pass with debug log
- `bindings/python/src/arcadedb_embedded/jvm.py:390-396` - replace shutdown try/except/pass with narrowed except
- `bindings/python/src/arcadedb_embedded/schema.py:684-709, 730-738` - replace probing try/except/pass with debug log
- `bindings/python/src/arcadedb_embedded/server.py:64, 102-109, 162-167` - safer default host + finalizer logging + nosec on equality check
- `bindings/python/src/arcadedb_embedded/vector.py:230-244` - annotate documented-false-positive SQL builder with `# nosec`
- `.github/workflows/test-python-bindings.yml` - new `bandit` job runs on `bindings/python/src` and `bindings/python/tests`

**Modified - PR2 (tests):**
- `bindings/python/tests/conftest.py` - export `TEST_PASSWORD` constant
- `bindings/python/tests/test_server.py` - import `TEST_PASSWORD`
- `bindings/python/tests/test_server_patterns.py` - import `TEST_PASSWORD`; parameterize the bench-loop SQL into `?` placeholders
- `bindings/python/tests/test_vector_sql.py` - parameterize INSERT/DELETE; annotate the `vectorL2Distance({vec})` literal queries (vector literal is not a value parameter in ArcadeDB SQL)
- `bindings/python/tests/test_vector.py` - replace `random.random()` with `random.Random(seed).random()` plus nosec where seeded determinism is desired

**Modified - PR3 (examples):**
- `bindings/python/examples/11_vector_index_build.py:873, 1027` - `hashlib.sha1(..., usedforsecurity=False)`
- `bindings/python/examples/12_vector_search.py:917, 1122, 1258` - same
- `bindings/python/examples/download_data.py:192, 652, 1090, 1621, 1704, 2166` - whitelist `https://` schemes before `urlopen`/`iterparse`, then nosec
- `bindings/python/examples/16_..._graph_ingest.py`, `17_timeseries_end_to_end.py`, `20_graph_algorithms_route_planning.py`, `22_graph_analytical_view_sql.py` - parameterize SQL where ArcadeDB accepts `?`; nosec the rest with a one-line justification
- `bindings/python/examples/21_server_mode_http_access.py:168` - https whitelist + nosec

---

# PR1 - Production source hardening

## Task 1: Add Bandit config and dev dependency

**Files:**
- Modify: `bindings/python/pyproject.toml`

- [ ] **Step 1: Verify current Bandit baseline**

Run: `cd bindings/python && python3 -m bandit -r src 2>&1 | tail -8`

Expected:
```
Run metrics:
        Total issues (by severity):
                Undefined: 0
                Low: 8
                Medium: 3
                High: 0
```

- [ ] **Step 2: Add bandit to dev extras**

Edit `bindings/python/pyproject.toml`. Locate the `[project.optional-dependencies]` block. Inside the `dev = [` list, add a `"bandit>=1.9.0",` entry just before the closing `]`. The block should read:

```toml
dev = [
    "black",
    "isort",
    "mypy",
    "pytest>=7.0.0",
    "pytest-cov",
    "numpy>=1.20.0",
    "bandit>=1.9.0",
]
```

- [ ] **Step 3: Add Bandit config section**

Append the following block to the end of `bindings/python/pyproject.toml`:

```toml
[tool.bandit]
# Skip recursion into test data and build artifacts.
exclude_dirs = ["tests/.cache", "build", "dist"]

[tool.bandit.assert_used]
# Asserts are the standard idiom in pytest; do not flag them in tests/conftest.
skips = ["**/tests/test_*.py", "**/tests/conftest.py"]
```

- [ ] **Step 4: Re-run Bandit with the config to confirm it loads**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r src 2>&1 | tail -8`

Expected: same 8 Low / 3 Medium baseline (config does not affect src yet, just confirms the file parses).

- [ ] **Step 5: Commit**

```bash
git add bindings/python/pyproject.toml
git commit -m "build(python): add bandit dev dependency and project config"
```

---

## Task 2: Add `_logging` helper module

**Files:**
- Create: `bindings/python/src/arcadedb_embedded/_logging.py`
- Test: `bindings/python/tests/test_logging_helper.py`

- [ ] **Step 1: Write the failing test**

Create `bindings/python/tests/test_logging_helper.py`:

```python
"""Tests for the internal _logging helper."""
import logging

from arcadedb_embedded._logging import get_logger, log_swallowed_exception


def test_get_logger_returns_namespaced_logger():
    logger = get_logger("arcadedb_embedded.foo")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "arcadedb_embedded.foo"


def test_log_swallowed_exception_emits_debug(caplog):
    logger = get_logger("arcadedb_embedded.test")
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            log_swallowed_exception(logger, "during shutdown")

    records = [r for r in caplog.records if r.name == logger.name]
    assert len(records) == 1
    assert records[0].levelno == logging.DEBUG
    assert "during shutdown" in records[0].getMessage()
    assert records[0].exc_info is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd bindings/python && python3 -m pytest tests/test_logging_helper.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'arcadedb_embedded._logging'`

- [ ] **Step 3: Implement the helper**

Create `bindings/python/src/arcadedb_embedded/_logging.py`:

```python
"""Internal logging helpers for arcadedb_embedded.

Centralises the pattern for swallowing exceptions in finalizers and
best-effort cleanup paths so the suppression is observable at DEBUG level
instead of being silently dropped.
"""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a namespaced logger for an arcadedb_embedded submodule."""
    return logging.getLogger(name)


def log_swallowed_exception(logger: logging.Logger, context: str) -> None:
    """Log the currently-handled exception at DEBUG with full traceback.

    Use only inside an `except` block where the caller has decided the
    error is non-fatal (e.g. JVM finalizer paths, optional best-effort
    rollback) and continuing is the right behaviour.
    """
    logger.debug("Swallowed exception %s", context, exc_info=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd bindings/python && python3 -m pytest tests/test_logging_helper.py -v`

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/_logging.py bindings/python/tests/test_logging_helper.py
git commit -m "feat(python): add internal _logging helper for swallowed exceptions"
```

---

## Task 3: Replace `try/except/pass` in `async_executor.py`

**Files:**
- Modify: `bindings/python/src/arcadedb_embedded/async_executor.py:207-217`

- [ ] **Step 1: Verify Bandit currently flags this site**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/async_executor.py 2>&1 | grep -A1 "B110"`

Expected: shows the issue at line 211.

- [ ] **Step 2: Apply the fix**

In `bindings/python/src/arcadedb_embedded/async_executor.py`, add the import near the top with other relative imports:

```python
from ._logging import get_logger, log_swallowed_exception
```

Add a module-level logger immediately after the imports, before the first class definition:

```python
_LOGGER = get_logger(__name__)
```

Replace the `is_processing` method body (currently lines 207-217) with:

```python
    def is_processing(self) -> bool:
        try:
            if bool(self._java_async.isProcessing()):
                return True
        except Exception:
            log_swallowed_exception(_LOGGER, "while polling isProcessing()")

        try:
            return not bool(self._java_async.waitCompletion(0))
        except Exception:
            return False
```

- [ ] **Step 3: Run tests for the affected module**

Run: `cd bindings/python && python3 -m pytest tests/test_async_executor.py -v`

Expected: all tests pass (behaviour unchanged).

- [ ] **Step 4: Verify Bandit no longer flags this file**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/async_executor.py`

Expected: `No issues identified.`

- [ ] **Step 5: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/async_executor.py
git commit -m "fix(python): log swallowed exception in AsyncExecutor.is_processing"
```

---

## Task 4: Replace `try/except/pass` in `graph_batch.py`

**Files:**
- Modify: `bindings/python/src/arcadedb_embedded/graph_batch.py:117-125`

- [ ] **Step 1: Apply the fix**

In `bindings/python/src/arcadedb_embedded/graph_batch.py`, add at the top with other relative imports:

```python
from ._logging import get_logger, log_swallowed_exception
```

Add module-level logger after imports:

```python
_LOGGER = get_logger(__name__)
```

Locate the inner rollback `try`/`except` (around line 117-122). Replace:

```python
        except Exception as e:
            if started_transaction:
                try:
                    self._java_db.rollback()
                except Exception:
                    pass
            raise ArcadeDBError(
                f"Failed to create batch vertex of type '{type_name}': {e}"
            ) from e
```

with:

```python
        except Exception as e:
            if started_transaction:
                try:
                    self._java_db.rollback()
                except Exception:
                    log_swallowed_exception(_LOGGER, "during batch vertex rollback")
            raise ArcadeDBError(
                f"Failed to create batch vertex of type '{type_name}': {e}"
            ) from e
```

- [ ] **Step 2: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_graph_batch.py -v`

Expected: all tests pass.

- [ ] **Step 3: Verify Bandit no longer flags this file**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/graph_batch.py`

Expected: `No issues identified.`

- [ ] **Step 4: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/graph_batch.py
git commit -m "fix(python): log swallowed exception in GraphBatch rollback path"
```

---

## Task 5: Narrow `try/except` in `core.py` finalizer

**Files:**
- Modify: `bindings/python/src/arcadedb_embedded/core.py:903-911`

- [ ] **Step 1: Apply the fix**

In `bindings/python/src/arcadedb_embedded/core.py`, replace:

```python
    def __del__(self):
        """Finalizer - ensure database is closed when object is garbage collected."""
        try:
            if not self._closed and self._java_db is not None:
                self._close_async_executors()
                self._java_db.close()
                self._closed = True
        except Exception:
            pass  # Ignore errors during garbage collection
```

with:

```python
    def __del__(self):
        """Finalizer - ensure database is closed when object is garbage collected.

        Errors during garbage collection are intentionally suppressed: the
        interpreter is shutting down and logging may already be unavailable,
        so we narrow the catch to AttributeError/RuntimeError that JPype can
        raise when the JVM has been torn down before this finalizer runs.
        """
        try:
            if not self._closed and self._java_db is not None:
                self._close_async_executors()
                self._java_db.close()
                self._closed = True
        except (AttributeError, RuntimeError):
            # JVM or referenced attributes already gone; nothing to do.
            return
```

- [ ] **Step 2: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_core.py tests/test_concurrency.py -v`

Expected: all tests pass.

- [ ] **Step 3: Verify Bandit no longer flags this file**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/core.py`

Expected: `No issues identified.`

- [ ] **Step 4: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/core.py
git commit -m "fix(python): narrow Database.__del__ finalizer exception handling"
```

---

## Task 6: Narrow `try/except` in `jvm.py` shutdown

**Files:**
- Modify: `bindings/python/src/arcadedb_embedded/jvm.py:390-396`

- [ ] **Step 1: Apply the fix**

In `bindings/python/src/arcadedb_embedded/jvm.py`, replace:

```python
def shutdown_jvm():
    """Shutdown JVM if it was started by this module."""
    if jpype.isJVMStarted():
        try:
            jpype.shutdownJVM()
        except Exception:
            pass  # Ignore errors during shutdown
```

with:

```python
def shutdown_jvm():
    """Shutdown JVM if it was started by this module.

    JPype can raise RuntimeError when the JVM is already mid-shutdown or
    has been detached from the calling thread; in that case there is
    nothing left for us to do.
    """
    if jpype.isJVMStarted():
        try:
            jpype.shutdownJVM()
        except RuntimeError:
            return
```

- [ ] **Step 2: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_jvm_args.py -v`

Expected: all tests pass.

- [ ] **Step 3: Verify Bandit no longer flags this file**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/jvm.py`

Expected: `No issues identified.`

- [ ] **Step 4: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/jvm.py
git commit -m "fix(python): narrow shutdown_jvm exception handling to RuntimeError"
```

---

## Task 7: Replace `try/except/pass` in `schema.py` (3 sites)

**Files:**
- Modify: `bindings/python/src/arcadedb_embedded/schema.py:684-709, 730-738`

- [ ] **Step 1: Apply the fix**

In `bindings/python/src/arcadedb_embedded/schema.py`, add the relative import alongside existing imports:

```python
from ._logging import get_logger, log_swallowed_exception
```

Add module-level logger after imports:

```python
_LOGGER = get_logger(__name__)
```

Replace the three `except Exception: pass` blocks. For lines 684-695 (inner sub-index probe inside `_find_vector_index`):

```python
                        try:
                            sub_indexes = java_index.getSubIndexes()
                            if sub_indexes and not sub_indexes.isEmpty():
                                first_sub = sub_indexes.get(0)
                                if "LSMVectorIndex" in first_sub.getClass().getName():
                                    props = java_index.getPropertyNames()
                                    if len(props) == 1 and props[0] == vector_property:
                                        return VectorIndex(java_index, self._db)
                        except Exception:
                            log_swallowed_exception(_LOGGER, "while inspecting TypeIndex sub-indexes")
```

For lines 700-704 (outer iteration in same method):

```python
            except Exception:
                log_swallowed_exception(_LOGGER, "while iterating schema indexes")
```

For lines 730-738 (`list_vector_indexes` inner sub-index probe):

```python
                    try:
                        sub_indexes = java_index.getSubIndexes()
                        if sub_indexes and not sub_indexes.isEmpty():
                            first_sub = sub_indexes.get(0)
                            if "LSMVectorIndex" in first_sub.getClass().getName():
                                indexes.append(java_index.getName())
                    except Exception:
                        log_swallowed_exception(_LOGGER, "while listing vector indexes")
```

- [ ] **Step 2: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_schema.py tests/test_vector.py tests/test_vector_sql.py -v`

Expected: all tests pass.

- [ ] **Step 3: Verify Bandit no longer flags this file**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/schema.py`

Expected: `No issues identified.`

- [ ] **Step 4: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/schema.py
git commit -m "fix(python): log swallowed exceptions in vector-index discovery"
```

---

## Task 8: Fix `server.py` bind default and finalizer

**Files:**
- Modify: `bindings/python/src/arcadedb_embedded/server.py:64, 102-109, 162-167`

- [ ] **Step 1: Write the failing test for the safer default**

Append to `bindings/python/tests/test_server.py`:

```python
def test_default_host_is_localhost(temp_server_root):
    """Default host should be localhost; binding to all interfaces must be opt-in."""
    from arcadedb_embedded.server import ArcadeDBServer

    server = ArcadeDBServer(
        root_path=temp_server_root,
        root_password="test_password",
    )
    assert server._config.get("host", "localhost") == "localhost"
    assert server.get_studio_url().startswith("http://localhost:")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd bindings/python && python3 -m pytest tests/test_server.py::test_default_host_is_localhost -v`

Expected: FAIL because the current default is `"0.0.0.0"`.

- [ ] **Step 3: Apply the fix**

In `bindings/python/src/arcadedb_embedded/server.py`:

Add the relative import alongside other imports:

```python
from ._logging import get_logger, log_swallowed_exception
```

Add module-level logger after imports:

```python
_LOGGER = get_logger(__name__)
```

Replace line 64:

```python
        host = self._config.get("host", "0.0.0.0")
```

with:

```python
        # Default to loopback; callers must opt in to "0.0.0.0" explicitly to
        # expose the server on all interfaces.
        host = self._config.get("host", "localhost")
```

Replace the `__del__` finalizer (lines 102-109):

```python
    def __del__(self):
        """Finalizer - ensure server is stopped."""
        try:
            if self._started and self._java_server is not None:
                self._java_server.stop()
                self._started = False
        except Exception:
            pass  # Ignore errors during garbage collection
```

with:

```python
    def __del__(self):
        """Finalizer - ensure server is stopped.

        See Database.__del__ for rationale on the narrowed except clause.
        """
        try:
            if self._started and self._java_server is not None:
                self._java_server.stop()
                self._started = False
        except (AttributeError, RuntimeError):
            return
```

For the equality check at line 165 (false positive: it is comparing, not binding), annotate it:

```python
        host = self._config.get("host", "localhost")
        if host == "0.0.0.0":  # nosec B104 - equality comparison, not a bind
            host = "localhost"
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd bindings/python && python3 -m pytest tests/test_server.py -v`

Expected: all tests pass, including the new `test_default_host_is_localhost`.

- [ ] **Step 5: Verify Bandit no longer flags this file**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/server.py`

Expected: `No issues identified.`

- [ ] **Step 6: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/server.py bindings/python/tests/test_server.py
git commit -m "fix(python): default server host to localhost and harden finalizer"
```

---

## Task 9: Annotate `vector.py` SQL builder false positive

**Files:**
- Modify: `bindings/python/src/arcadedb_embedded/vector.py:230-244`

- [ ] **Step 1: Verify the query is in fact parameterized**

Run: `cd bindings/python && grep -n "SELECT.*query_vector" src/arcadedb_embedded/vector.py`

Expected: shows the `key` is bound via `?` and `quoted_vector_property`/`quoted_type_name`/`quoted_id_property` come from `_quote_identifier`.

- [ ] **Step 2: Apply the annotation**

In `bindings/python/src/arcadedb_embedded/vector.py`, replace the `result = self._database.query(...)` block (around lines 237-244):

```python
        if result is None:
            result = self._database.query(
                "sql",
                (
                    f"SELECT {quoted_vector_property} AS `query_vector` FROM {quoted_type_name} "
                    f"WHERE {quoted_id_property} = ? LIMIT 1"
                ),
                key,
            ).first()
```

with:

```python
        if result is None:
            # Identifiers are quoted via _quote_identifier(); the user-supplied
            # `key` is passed as a `?` parameter, so this is not SQL injection.
            result = self._database.query(
                "sql",
                (  # nosec B608
                    f"SELECT {quoted_vector_property} AS `query_vector` FROM {quoted_type_name} "
                    f"WHERE {quoted_id_property} = ? LIMIT 1"
                ),
                key,
            ).first()
```

- [ ] **Step 3: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_vector.py -v -k "lookup_by_key or query_vector"`

Expected: all matching tests pass.

- [ ] **Step 4: Verify Bandit no longer flags the file**

Run: `cd bindings/python && python3 -m bandit src/arcadedb_embedded/vector.py`

Expected: `No issues identified.`

- [ ] **Step 5: Commit**

```bash
git add bindings/python/src/arcadedb_embedded/vector.py
git commit -m "fix(python): annotate vector lookup query as parameterized"
```

---

## Task 10: Verify whole-of-`src/` is clean

**Files:** none (verification only)

- [ ] **Step 1: Run Bandit against the whole src tree**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r src 2>&1 | tail -10`

Expected:
```
Run metrics:
        Total issues (by severity):
                Undefined: 0
                Low: 0
                Medium: 0
                High: 0
```

- [ ] **Step 2: Run the full Python test suite**

Run: `cd bindings/python && python3 -m pytest -x`

Expected: all tests pass.

---

## Task 11: Wire Bandit into CI

**Files:**
- Modify: `.github/workflows/test-python-bindings.yml`

- [ ] **Step 1: Add a `bandit` job**

In `.github/workflows/test-python-bindings.yml`, locate the `jobs:` section and add a new top-level job *before* the existing `download-jars:` job (so it runs in parallel and acts as a fast gate):

```yaml
  bandit:
    name: Bandit security scan (bindings/python)
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6.0.2

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install Bandit
        run: python -m pip install "bandit>=1.9.0"

      - name: Run Bandit on src and tests (must be clean)
        working-directory: bindings/python
        run: python -m bandit -c pyproject.toml -r src tests --severity-level low --confidence-level low

      - name: Run Bandit on examples (informational, must have no High findings)
        working-directory: bindings/python
        run: python -m bandit -c pyproject.toml -r examples --severity-level high --confidence-level high
```

- [ ] **Step 2: Validate workflow syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/test-python-bindings.yml'))"`

Expected: no output (valid YAML).

- [ ] **Step 3: Dry-run the same Bandit invocations locally**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r src tests --severity-level low --confidence-level low`

Expected: at this point in PR1 the `src` portion is clean but `tests` still reports the test-side findings. **This is expected to fail this command in PR1.** Document this in the commit message and do not fail the PR locally; the workflow gate becomes effective once PR2 lands. Adjust the workflow temporarily to scan only `src` for now:

Replace the "Run Bandit on src and tests" step with:

```yaml
      - name: Run Bandit on src (must be clean)
        working-directory: bindings/python
        run: python -m bandit -c pyproject.toml -r src --severity-level low --confidence-level low
```

Note: PR2 will broaden this to include `tests`, and PR3 will drop the `--severity-level high` to medium for examples once they are cleaned up.

- [ ] **Step 4: Re-run the local check matching the workflow**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r src --severity-level low --confidence-level low`

Expected: `No issues identified.`

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/test-python-bindings.yml
git commit -m "ci(python): add Bandit security scan job for bindings/python/src"
```

---

# PR2 - Test suite cleanup

## Task 12: Centralise `TEST_PASSWORD` in `conftest.py`

**Files:**
- Modify: `bindings/python/tests/conftest.py`
- Modify: `bindings/python/tests/test_server.py`
- Modify: `bindings/python/tests/test_server_patterns.py`

- [ ] **Step 1: Add the constant**

Append the following near the top of `bindings/python/tests/conftest.py`, after the existing imports:

```python
# Shared test password used by server-mode tests. ArcadeDB requires >= 8 chars.
# Hardcoded test fixture, not a real credential.
TEST_PASSWORD = "test12345"  # nosec B105 - test fixture
```

- [ ] **Step 2: Update `test_server.py` to use the constant**

In `bindings/python/tests/test_server.py`, add the import (top of file, after existing imports):

```python
from .conftest import TEST_PASSWORD
```

Then replace every occurrence of `root_password="test_password"` with `root_password=TEST_PASSWORD`. (There are 4 occurrences at approximately lines 28, 60, 93, 108.)

- [ ] **Step 3: Update `test_server_patterns.py` to use the constant**

In `bindings/python/tests/test_server_patterns.py`, add the import:

```python
from .conftest import TEST_PASSWORD
```

Then replace every occurrence of `root_password="test12345"` with `root_password=TEST_PASSWORD`. (There are 6 occurrences at approximately lines 80, 135, 206, 280, 408, 503; remove the trailing `# Min 8 chars required` comment from line 80 since the constant doc-string covers it.)

- [ ] **Step 4: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_server.py tests/test_server_patterns.py -v -k "not benchmark"`

Expected: all tests pass (or are skipped on platforms without server support, as before).

- [ ] **Step 5: Verify Bandit no longer flags B106 in tests**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r tests 2>&1 | grep -c B106`

Expected: `0`

- [ ] **Step 6: Commit**

```bash
git add bindings/python/tests/conftest.py bindings/python/tests/test_server.py bindings/python/tests/test_server_patterns.py
git commit -m "test(python): centralise TEST_PASSWORD constant in conftest"
```

---

## Task 13: Parameterize bench-loop SQL in `test_server_patterns.py`

**Files:**
- Modify: `bindings/python/tests/test_server_patterns.py:625-720`

- [ ] **Step 1: Replace string-formatted SQL with `?` placeholders**

In `bindings/python/tests/test_server_patterns.py`, locate the bench loop (approximately lines 625-720). Replace each SQL `f"..."` with parameter binding. Concretely, change the four formatted commands:

From:
```python
                    "command": (
                        f"UPDATE BenchItem SET value = {random.randint(1, 1000)} "
                        f"WHERE id = {random.randint(0, max(1, i-1))}"
                    ),
```

to:
```python
                    "command": "UPDATE BenchItem SET value = ? WHERE id = ?",
                    "params": [random.randint(1, 1000), random.randint(0, max(1, i - 1))],
```

(Adjust the dict construction wherever the test infra reads `command`/`params`. Do the same for the SELECT-LIKE, INSERT, second UPDATE, second SELECT-LIKE, and SELECT-WHERE-value occurrences in the file.)

The `random.randint` calls remain - they are seeded test data generation, not security-relevant. Add a top-of-file marker once:

```python
# random is used to generate synthetic benchmark data; not for security.
import random  # nosec B311
```

- [ ] **Step 2: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_server_patterns.py -v`

Expected: all tests pass; bench loop produces equivalent output.

- [ ] **Step 3: Verify Bandit issues drop**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml tests/test_server_patterns.py 2>&1 | tail -8`

Expected: no B608 entries; B311 is now masked by the `# nosec` on the import line.

- [ ] **Step 4: Commit**

```bash
git add bindings/python/tests/test_server_patterns.py
git commit -m "test(python): parameterize bench SQL and annotate seeded random"
```

---

## Task 14: Parameterize INSERT/DELETE in `test_vector_sql.py`

**Files:**
- Modify: `bindings/python/tests/test_vector_sql.py:580-610, 670-690`

- [ ] **Step 1: Apply the parameterization**

In `bindings/python/tests/test_vector_sql.py`, replace the INSERT and DELETE in the `with test_db.transaction():` block (approximately lines 588-595):

From:
```python
                test_db.command(
                    "sql", f"INSERT INTO DocSql SET id = {i}, embedding = {vec}"
                )
        ...
                test_db.command("sql", f"DELETE FROM DocSql WHERE id = {i}")
```

to:
```python
                test_db.command(
                    "sql",
                    "INSERT INTO DocSql SET id = ?, embedding = ?",
                    i,
                    vec,
                )
        ...
                test_db.command("sql", "DELETE FROM DocSql WHERE id = ?", i)
```

For the two `vectorL2Distance(embedding, {vec})` SELECTs (approximately lines 602-604 and 678-680): ArcadeDB SQL does not bind list literals as `?` parameters in distance functions, so leave the literal but annotate:

```python
            rs = test_db.query(
                "sql",
                # Vector literal is required by vectorL2Distance(); not user input.
                f"SELECT id, vectorL2Distance(embedding, {vec}) as dist FROM DocSql ORDER BY dist ASC LIMIT 1",  # nosec B608
            )
```

Add a single top-of-file annotation for `random` if not already present:

```python
import random  # nosec B311 - synthetic vector data
```

- [ ] **Step 2: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_vector_sql.py -v`

Expected: all tests pass.

- [ ] **Step 3: Verify Bandit results**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml tests/test_vector_sql.py 2>&1 | grep -E "B608|B311" | wc -l`

Expected: `0` (the remaining occurrences are masked by `# nosec`).

- [ ] **Step 4: Commit**

```bash
git add bindings/python/tests/test_vector_sql.py
git commit -m "test(python): parameterize vector INSERT/DELETE; annotate distance literals"
```

---

## Task 15: Annotate `random` usage in `test_vector.py`

**Files:**
- Modify: `bindings/python/tests/test_vector.py:710-715`

- [ ] **Step 1: Apply the annotation**

In `bindings/python/tests/test_vector.py`, find the `import random` statement at the top of the file. Append `# nosec B311 - synthetic vector data, not security` to that line. If `random` is imported as part of a multi-import, split it onto its own line first.

- [ ] **Step 2: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/test_vector.py -v -k "not benchmark"`

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add bindings/python/tests/test_vector.py
git commit -m "test(python): annotate seeded random data generation as nosec B311"
```

---

## Task 15a: Annotate remaining `try/except/pass` and subprocess findings in tests

**Files:**
- Modify: `bindings/python/tests/conftest.py:55`
- Modify: `bindings/python/tests/test_async_executor.py:107`
- Modify: `bindings/python/tests/test_core.py:423`
- Modify: `bindings/python/tests/test_docs_examples.py:4, 67`
- Modify: `bindings/python/tests/test_exporter.py:884`
- Modify: `bindings/python/tests/test_jvm_args.py:112`
- Modify: `bindings/python/tests/test_server_patterns.py:46, 56, 679`
- Modify: `bindings/python/tests/test_vector_params_verification.py:81`
- Modify: `bindings/python/tests/test_vector_sql.py:519`

- [ ] **Step 1: Generate the current location list**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r tests -t B110,B603,B404,B108 2>&1 | grep "Location:"`

Expected: the 12 locations listed in this task's Files section.

- [ ] **Step 2: Annotate or narrow each `try/except/pass`**

For each `B110` site (the lines other than `test_docs_examples.py:4` and `test_jvm_args.py:112`):

- If the swallowed cleanup is intentional (most are best-effort cleanup in fixtures/teardown), narrow the exception class to the specific JPype/OS error expected (e.g. `RuntimeError`, `OSError`, `PermissionError`) and remove the bare `except Exception`.
- If narrowing is impractical (Java exception types may be unavailable when the JVM is down), keep the bare `except Exception` but replace `pass` with a short `# nosec B110` comment explaining the cleanup intent: e.g. `pass  # nosec B110 - best-effort teardown after JVM may be down`.

Concrete example for `tests/conftest.py:55` (cleanup retry loop):

```python
        try:
            shutil.rmtree(temp_dir)
        except PermissionError:
            # On Windows, files might still be locked by Java process
            time.sleep(1)
            try:
                shutil.rmtree(temp_dir)
            except OSError:
                pass  # nosec B110 - best-effort temp cleanup; pytest tmp will be reaped
```

- [ ] **Step 3: Annotate `subprocess` import (B404) in `test_docs_examples.py`**

Edit line 4:

```python
import subprocess  # nosec B404 - launching Python interpreter to run example scripts
```

- [ ] **Step 4: Annotate `subprocess.run` call (B603) in `test_docs_examples.py:67`**

Append `# nosec B603 - argv built from script paths and constants, not user input` to the `subprocess.run(...)` call.

- [ ] **Step 5: Annotate `tempfile.gettempdir()` usage (B108) in `test_jvm_args.py:112`**

Append a short comment justifying the hardcoded temp directory:

```python
        ...,  # nosec B108 - using a stable temp path is required by JPype's argv contract
```

- [ ] **Step 6: Run the affected tests**

Run: `cd bindings/python && python3 -m pytest tests/conftest.py tests/test_async_executor.py tests/test_core.py tests/test_docs_examples.py tests/test_exporter.py tests/test_jvm_args.py tests/test_server_patterns.py tests/test_vector_params_verification.py tests/test_vector_sql.py -v -k "not benchmark"`

Expected: all tests pass (or are appropriately skipped).

- [ ] **Step 7: Verify Bandit on tests/ is clean**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r tests 2>&1 | tail -8`

Expected: `Total issues: 0` across all severities.

- [ ] **Step 8: Commit**

```bash
git add bindings/python/tests/
git commit -m "test(python): annotate teardown try/except and subprocess usage"
```

---

## Task 16: Verify whole-of-`tests/` is clean and broaden CI gate

**Files:**
- Modify: `.github/workflows/test-python-bindings.yml`

- [ ] **Step 1: Confirm tests are clean at medium+ level**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r tests --severity-level medium --confidence-level medium 2>&1 | tail -8`

Expected: `Total issues (by severity): Low: 0, Medium: 0, High: 0`. (Some Low-confidence Low items may remain - acceptable.)

- [ ] **Step 2: Confirm tests are clean at low level too**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r tests 2>&1 | tail -8`

Expected: `Total issues: 0` across all severities (B101 is masked via `assert_used.skips`, B106 via constant, B608 parameterised or annotated, B311 annotated).

- [ ] **Step 3: Broaden the CI step to include tests**

In `.github/workflows/test-python-bindings.yml`, replace the step added in Task 11:

```yaml
      - name: Run Bandit on src (must be clean)
        working-directory: bindings/python
        run: python -m bandit -c pyproject.toml -r src --severity-level low --confidence-level low
```

with:

```yaml
      - name: Run Bandit on src and tests (must be clean)
        working-directory: bindings/python
        run: python -m bandit -c pyproject.toml -r src tests --severity-level low --confidence-level low
```

- [ ] **Step 4: Validate workflow syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/test-python-bindings.yml'))"`

Expected: no output.

- [ ] **Step 5: Run the full test suite**

Run: `cd bindings/python && python3 -m pytest -x`

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/test-python-bindings.yml
git commit -m "ci(python): require Bandit-clean tests in addition to src"
```

---

# PR3 - Examples cleanup

## Task 17: Fix SHA1 high-severity findings

**Files:**
- Modify: `bindings/python/examples/11_vector_index_build.py:873, 1027`
- Modify: `bindings/python/examples/12_vector_search.py:917, 1122, 1258`

- [ ] **Step 1: Apply the `usedforsecurity=False` flag**

For each of the five locations, replace the `hashlib.sha1(<bytes>).hexdigest()` call with `hashlib.sha1(<bytes>, usedforsecurity=False).hexdigest()`.

Concretely, in `bindings/python/examples/11_vector_index_build.py`:

Replace line 873:
```python
    digest = hashlib.sha1(str(db_path).encode("utf-8")).hexdigest()[:10]
```
with:
```python
    digest = hashlib.sha1(str(db_path).encode("utf-8"), usedforsecurity=False).hexdigest()[:10]
```

Apply the same transformation at line 1027 in the same file, and at lines 917, 1122, 1258 in `12_vector_search.py`.

- [ ] **Step 2: Verify no behaviour change**

Run: `cd bindings/python && python3 -c "import hashlib; assert hashlib.sha1(b'x').hexdigest() == hashlib.sha1(b'x', usedforsecurity=False).hexdigest()"`

Expected: no output (assertion passes - the digest is byte-identical, only the security-intent annotation changes).

- [ ] **Step 3: Verify Bandit no longer reports High severity**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r examples --severity-level high 2>&1 | tail -8`

Expected: `Total issues (by severity): Low: 0, Medium: 0, High: 0`.

- [ ] **Step 4: Commit**

```bash
git add bindings/python/examples/11_vector_index_build.py bindings/python/examples/12_vector_search.py
git commit -m "examples(python): mark SHA1 short-digest as usedforsecurity=False"
```

---

## Task 18: Whitelist HTTPS schemes in `download_data.py` and `21_server_mode_http_access.py`

**Files:**
- Modify: `bindings/python/examples/download_data.py:192, 652, 1090, 1621, 1704, 2166`
- Modify: `bindings/python/examples/21_server_mode_http_access.py:168`

- [ ] **Step 1: Add a scheme guard helper near the top of `download_data.py`**

After the imports at the top of `bindings/python/examples/download_data.py`, add:

```python
def _require_https(url: str) -> str:
    """Reject non-HTTPS URLs before opening them.

    Bandit B310 flags urlopen() because it permits file:// and custom schemes.
    Examples download from a fixed list of HTTPS dataset URLs, so we enforce
    that contract explicitly here.
    """
    if not url.startswith("https://"):
        raise ValueError(f"Refusing to open non-HTTPS URL: {url!r}")
    return url
```

For each `urlopen`/`urlretrieve` call site (lines 192, 652, 1090, 1621, 1704, 2166), wrap the URL argument in `_require_https(...)` and append `# nosec B310` to the call:

```python
    request = urllib.request.Request(_require_https(url), headers=headers)
    with urllib.request.urlopen(request, timeout=60) as response:  # nosec B310 - https-only
```

The two `ET.iterparse(xml_path, ...)` lines (1090, 2166) need a different remediation: the file came from a checksum-verified ZIP earlier in the script. Switch the import:

```python
import xml.etree.ElementTree as ET  # nosec B405 - parsing files we just downloaded over HTTPS and verified
```

and annotate each `iterparse` call:

```python
    context = ET.iterparse(xml_path, events=("start", "end"))  # nosec B314 - input is a downloaded, checksum-verified file
```

- [ ] **Step 2: Repeat the HTTPS guard in `21_server_mode_http_access.py`**

In `bindings/python/examples/21_server_mode_http_access.py`, line 168, add an inline check just before the `urlopen` call:

```python
    if not request.full_url.startswith(("http://localhost", "http://127.0.0.1", "https://")):
        raise ValueError(f"Refusing to call unexpected URL: {request.full_url!r}")
    try:
        with urlopen(request, timeout=timeout) as response:  # nosec B310 - localhost or https
```

- [ ] **Step 3: Smoke-test the example imports**

Run: `cd bindings/python && python3 -c "import importlib.util, pathlib; [importlib.util.spec_from_file_location('m', p).loader.exec_module(importlib.util.module_from_spec(importlib.util.spec_from_file_location('m', p))) for p in [pathlib.Path('examples/download_data.py'), pathlib.Path('examples/21_server_mode_http_access.py')]]" 2>&1 | head`

Expected: no syntax errors. (Examples are not part of the unit test suite, so this only checks the file parses and imports.)

- [ ] **Step 4: Verify Bandit no longer flags B310/B314**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml examples/download_data.py examples/21_server_mode_http_access.py 2>&1 | grep -E "B310|B314" | wc -l`

Expected: `0`.

- [ ] **Step 5: Commit**

```bash
git add bindings/python/examples/download_data.py bindings/python/examples/21_server_mode_http_access.py
git commit -m "examples(python): enforce https-only URL fetches and annotate XML parse"
```

---

## Task 19: Parameterize/annotate SQL f-strings in numbered examples

**Files:**
- Modify: `bindings/python/examples/16_import_database_vs_transactional_graph_ingest.py:168, 249`
- Modify: `bindings/python/examples/17_timeseries_end_to_end.py:340`
- Modify: `bindings/python/examples/20_graph_algorithms_route_planning.py:287, 758`
- Modify: `bindings/python/examples/22_graph_analytical_view_sql.py:544, 563, 580`

- [ ] **Step 1: Parameterize where ArcadeDB accepts `?`**

For `16_..._graph_ingest.py:168`, change:

```python
    row = query_one_or_none(
        db.query("sql", f"SELECT FROM {vertex_type} WHERE Id = {vertex_id}")
    )
```

to:

```python
    row = query_one_or_none(
        db.query(
            "sql",
            # Type name is a constant from this script; Id is bound as parameter.
            f"SELECT FROM {vertex_type} WHERE Id = ?",  # nosec B608 - identifier from literal
            vertex_id,
        )
    )
```

For `17_timeseries_end_to_end.py:339-345`, parameterize the value bindings:

```python
                "sql",
                "SELECT FROM SensorReading "
                "WHERE ts BETWEEN ? AND ? "
                "AND sensor_id = ? AND building = ? "
                "ORDER BY ts",
                raw_window_start,
                raw_window_end,
                focus_sensor.sensor_id,
                focus_sensor.building,
            )
```

- [ ] **Step 2: Annotate the rest with one-line justifications**

For each remaining f-string SQL where the interpolated values are constants/types from the script and not user input - `16_..._graph_ingest.py:249`, `20_graph_algorithms_route_planning.py:287, 758`, `22_graph_analytical_view_sql.py:544, 563, 580` - append `# nosec B608` to the offending line and add a one-line comment above it stating the interpolated values are static (e.g. `# vertex_type and aggregate fields are script constants, not user input.`).

Concrete example for `22_graph_analytical_view_sql.py:544`:

```python
    # origin_code is a script-local constant from the demo dataset.
    rs = db.query(
        "sql",
        f"""
        SELECT count(*) AS destination_count FROM (
            MATCH {{type: City, as: src, where: (code = '{origin_code}')}}
                  -ROAD->
                  {{type: City, as: mid}}
                  -ROAD->
                  {{type: City, as: dst}}
            RETURN DISTINCT dst.code AS code
        )
        """,  # nosec B608 - demo-data constants only
    )
```

- [ ] **Step 3: Verify the examples still parse and import**

Run: `cd bindings/python && python3 -m py_compile examples/16_import_database_vs_transactional_graph_ingest.py examples/17_timeseries_end_to_end.py examples/20_graph_algorithms_route_planning.py examples/22_graph_analytical_view_sql.py`

Expected: no output (all four compile).

- [ ] **Step 4: Verify Bandit Medium count drops**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r examples --severity-level medium --confidence-level medium 2>&1 | tail -8`

Expected: `Medium: 0`.

- [ ] **Step 5: Commit**

```bash
git add bindings/python/examples/16_import_database_vs_transactional_graph_ingest.py bindings/python/examples/17_timeseries_end_to_end.py bindings/python/examples/20_graph_algorithms_route_planning.py bindings/python/examples/22_graph_analytical_view_sql.py
git commit -m "examples(python): parameterize timeseries SQL; annotate demo-data SQL"
```

---

## Task 20: Annotate remaining example findings

**Files:**
- Modify: any `bindings/python/examples/*.py` that still has B603/B607/B311/B110/B112/B105/B404/B403 findings

- [ ] **Step 1: Generate a fresh report scoped to remaining items**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r examples 2>&1 | grep "Location:" | sort -u`

Expected: a list of remaining file:line locations.

- [ ] **Step 2: Annotate each remaining site**

For each line in the report:

- B603/B607 (subprocess): add `# nosec B603 B607 - launching <tool name> with literal argv` to the `subprocess.run(...)` call.
- B311 (random): on the `import random` line, append `# nosec B311 - synthetic data`.
- B110/B112 (try/except/pass or continue): replace with a top-of-file `from arcadedb_embedded._logging import get_logger` (examples already depend on the package) plus a `_LOGGER.debug(...)` call - or, if logging is overkill for the example, narrow the exception to a specific class.
- B105 (hardcoded password string): if the constant is named `*PASSWORD*` and is a sample value, append `# nosec B105 - sample password`.
- B404/B403 (subprocess/pickle import): add `# nosec` next to the import with a brief justification.

- [ ] **Step 3: Verify Bandit Low count is now informational only**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r examples --severity-level low --confidence-level high 2>&1 | tail -8`

Expected: `Total issues: 0` at high confidence; some low-confidence Low items may remain.

- [ ] **Step 4: Tighten the CI gate for examples**

In `.github/workflows/test-python-bindings.yml`, the `bandit` job currently has:

```yaml
      - name: Run Bandit on examples (informational, must have no High findings)
        working-directory: bindings/python
        run: python -m bandit -c pyproject.toml -r examples --severity-level high --confidence-level high
```

Change to:

```yaml
      - name: Run Bandit on examples (must be clean at medium+/high-confidence)
        working-directory: bindings/python
        run: python -m bandit -c pyproject.toml -r examples --severity-level medium --confidence-level high
```

- [ ] **Step 5: Validate workflow syntax**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/test-python-bindings.yml'))"`

Expected: no output.

- [ ] **Step 6: Run the local CI equivalent**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r src tests --severity-level low --confidence-level low && python3 -m bandit -c pyproject.toml -r examples --severity-level medium --confidence-level high`

Expected: both commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add bindings/python/examples .github/workflows/test-python-bindings.yml
git commit -m "examples(python): annotate remaining bandit findings; tighten CI gate"
```

---

## Task 21: Final whole-tree verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full Python test suite**

Run: `cd bindings/python && python3 -m pytest -x`

Expected: all tests pass.

- [ ] **Step 2: Run the full CI Bandit equivalent**

Run: `cd bindings/python && python3 -m bandit -c pyproject.toml -r src tests --severity-level low --confidence-level low && python3 -m bandit -c pyproject.toml -r examples --severity-level medium --confidence-level high`

Expected: both invocations exit 0; final report shows `No issues identified.` on the strict run and only Low/low-confidence items on examples.

- [ ] **Step 3: Compare against initial baseline**

Run: `cd bindings/python && python3 -m bandit -r src tests examples 2>&1 | tail -10`

Expected: substantial drop from the original `Low: 1067, Medium: 120, High: 5` to single-digit Low items in examples (informational), with `Medium: 0, High: 0`.

---

## Plan complete

**Saved to:** `docs/superpowers/plans/2026-05-05-python-bindings-codacy-fixes.md`

**Suggested PR boundaries:**
- PR1: Tasks 1-11 (production code + CI gate for src)
- PR2: Tasks 12-16 (test suite cleanup + broaden CI gate)
- PR3: Tasks 17-21 (examples cleanup + final CI gate)
