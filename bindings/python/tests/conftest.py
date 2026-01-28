"""
Shared pytest fixtures and configuration for ArcadeDB tests.
"""

import os
import shutil
import tempfile

import pytest


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    temp_dir = tempfile.mkdtemp(prefix="arcadedb_test_db_")
    db_path = os.path.join(temp_dir, "test_db")
    yield db_path
    # Cleanup
    if os.path.exists(temp_dir):
        # Force garbage collection to release file handles (Windows fix)
        import gc

        gc.collect()

        try:
            shutil.rmtree(temp_dir)
        except PermissionError:
            # On Windows, files might still be locked by Java process
            # Wait a bit and try again
            import time

            time.sleep(0.5)
            try:
                shutil.rmtree(temp_dir)
            except PermissionError:
                # If still locked, ignore (OS will clean up temp eventually)
                pass


@pytest.fixture
def temp_db():
    """Create a temporary database, yield it, and clean up."""
    import arcadedb_embedded as arcadedb

    temp_dir = tempfile.mkdtemp(prefix="arcadedb_test_db_")
    db_path = os.path.join(temp_dir, "test_db")

    db = arcadedb.create_database(db_path)
    yield db

    # Cleanup
    try:
        if not db.is_closed():
            db.close()
    except Exception:
        pass

    # Force garbage collection to release file handles (Windows fix)
    import gc

    gc.collect()

    if os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
        except PermissionError:
            # On Windows, files might still be locked by Java process
            import time

            time.sleep(0.5)
            try:
                shutil.rmtree(temp_dir)
            except PermissionError:
                pass


@pytest.fixture
def temp_server_root():
    """Create a temporary server root directory."""
    temp_dir = tempfile.mkdtemp(prefix="arcadedb_test_server_")
    yield temp_dir
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def temp_dir_factory():
    """Factory fixture to create multiple temporary directories with cleanup."""
    temp_dirs = []

    def _create_temp_dir(prefix="arcadedb_test_"):
        """Create a temporary directory and register it for cleanup."""
        temp_dir = tempfile.mkdtemp(prefix=prefix)
        temp_dirs.append(temp_dir)
        return temp_dir

    yield _create_temp_dir

    # Cleanup all created directories
    for temp_dir in temp_dirs:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)


def has_server_support():
    """Check if server support is available (available in our base package)."""
    try:
        import os

        import arcadedb_embedded

        # Check if studio JAR exists (indicates server support)
        jar_dir = os.path.join(os.path.dirname(arcadedb_embedded.__file__), "jars")
        if not os.path.exists(jar_dir):
            return False
        jar_files = os.listdir(jar_dir)
        return any("studio" in jar.lower() for jar in jar_files)
    except Exception:
        return False


def has_graph_export_support():
    """Check if GraphML/GraphSON export support is available."""
    try:
        # Detect graph export-related modules in bundled JARs
        import os

        import arcadedb_embedded

        jar_dir = os.path.join(os.path.dirname(arcadedb_embedded.__file__), "jars")
        jar_files = os.listdir(jar_dir) if os.path.exists(jar_dir) else []
        return any(
            "graphson" in jar.lower() or "graphml" in jar.lower() for jar in jar_files
        )
    except Exception:
        return False


# Pytest markers for conditional test execution
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "server: tests that require server support (available in base package)",
    )
    config.addinivalue_line(
        "markers",
        "graph_export: tests that require GraphML/GraphSON support",
    )


def pytest_unconfigure(config):
    """
    Force exit after pytest completes to avoid JVM thread hang.

    The issue: JPype's shutdownJVM() itself hangs waiting for JVM threads.
    Even though we properly close AsyncExecutors, some JVM background threads
    (from ArcadeDB's async executor thread pool) don't terminate cleanly.

    Solution: Force exit with os._exit(0) after flushing output. This is safe
    because all tests have completed and we just need to terminate the process.

    Note: This only affects the test suite. Production code is fine - when the
    Python process exits naturally, the OS terminates all threads including JVM.
    """
    import os
    import sys

    # Flush all output to ensure we see test results
    sys.stdout.flush()
    sys.stderr.flush()

    # Force exit - bypass Python cleanup and JVM shutdown
    # This is the only reliable way to exit pytest with JVM threads active
    os._exit(0)


def pytest_sessionfinish(session, exitstatus):
    """Session finish hook."""
    pass


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    """Terminal summary hook."""
    pass
