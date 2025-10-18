"""
Shared pytest fixtures and configuration for ArcadeDB tests.
"""

import tempfile
import shutil
import os
import pytest
from pathlib import Path


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_db")
    yield db_path
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def temp_server_root():
    """Create a temporary server root directory."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


def has_server_support():
    """Check if server support is available (minimal or full distribution)."""
    try:
        import arcadedb_embedded
        import os
        # Check if studio JAR exists (indicates minimal or full distribution)
        jar_dir = os.path.join(os.path.dirname(arcadedb_embedded.__file__), 'jars')
        if not os.path.exists(jar_dir):
            return False
        jar_files = os.listdir(jar_dir)
        return any('studio' in jar.lower() for jar in jar_files)
    except Exception:
        return False


def has_gremlin_support():
    """Check if Gremlin support is available (full distribution)."""
    try:
        # Try to find Gremlin-related classes
        import arcadedb_embedded
        import os
        jar_dir = os.path.join(os.path.dirname(arcadedb_embedded.__file__), 'jars')
        jar_files = os.listdir(jar_dir) if os.path.exists(jar_dir) else []
        return any('gremlin' in jar.lower() for jar in jar_files)
    except Exception:
        return False


# Pytest markers for conditional test execution
def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "server: tests that require server support (minimal/full distributions)"
    )
    config.addinivalue_line(
        "markers", "gremlin: tests that require Gremlin support (full distribution only)"
    )
