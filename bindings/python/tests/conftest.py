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
        shutil.rmtree(temp_dir)


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
    """Check if server support is available (minimal or full distribution)."""
    try:
        import os

        import arcadedb_embedded

        # Check if studio JAR exists (indicates minimal or full distribution)
        jar_dir = os.path.join(os.path.dirname(arcadedb_embedded.__file__), "jars")
        if not os.path.exists(jar_dir):
            return False
        jar_files = os.listdir(jar_dir)
        return any("studio" in jar.lower() for jar in jar_files)
    except Exception:
        return False


def has_gremlin_support():
    """Check if Gremlin support is available (full distribution)."""
    try:
        # Try to find Gremlin-related classes
        import os

        import arcadedb_embedded

        jar_dir = os.path.join(os.path.dirname(arcadedb_embedded.__file__), "jars")
        jar_files = os.listdir(jar_dir) if os.path.exists(jar_dir) else []
        return any("gremlin" in jar.lower() for jar in jar_files)
    except Exception:
        return False


# Pytest markers for conditional test execution
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "server: tests that require server support (minimal/full distributions)",
    )
    config.addinivalue_line(
        "markers",
        "gremlin: tests that require Gremlin support (full distribution only)",
    )
