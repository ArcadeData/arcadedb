"""
Server and Studio tests for ArcadeDB Python bindings.

These tests use the JAVA API for server operations:
- Server creation and management via ArcadeDBServer class
- Database operations via direct JVM method calls (db.command, db.query)
- All operations happen within the same Python process (embedded mode)

Note: These tests do NOT test HTTP API access patterns.
For HTTP API testing, see test_server_patterns.py

These tests require server support (minimal or full distributions).
"""

import time

import pytest
from arcadedb_embedded import ArcadeDBServer
from tests.conftest import has_server_support


@pytest.mark.server
@pytest.mark.skipif(not has_server_support(), reason="Requires server support")
def test_server_creation(temp_server_root):
    """Test creating and starting a server."""
    server = ArcadeDBServer(
        root_path=temp_server_root,
        root_password="test_password",
        config={"http_port": 2480},
    )

    assert not server.is_started()
    server.start()
    assert server.is_started()

    # Give server a moment to start
    time.sleep(1)

    assert server.get_http_port() == 2480
    studio_url = server.get_studio_url()
    assert "http://" in studio_url
    assert "2480" in studio_url

    server.stop()
    assert not server.is_started()


@pytest.mark.server
@pytest.mark.skipif(not has_server_support(), reason="Requires server support")
def test_server_database_operations(temp_server_root):
    """
    Test database operations through server using Java API.

    This test demonstrates:
    - Server-managed database creation via Java API
    - Direct JVM method calls (db.command, db.query)
    - Operations within same Python process (embedded access)
    """
    with ArcadeDBServer(
        root_path=temp_server_root, root_password="test_password"
    ) as server:
        # Server auto-starts in context manager
        time.sleep(1)

        # Create database through server
        db = server.create_database("testdb")
        assert db.is_open()

        # Use database
        with db.transaction():
            db.command("sql", "CREATE DOCUMENT TYPE Person")
            db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

        # Query
        result = db.query("sql", "SELECT FROM Person")
        records = list(result)
        assert len(records) == 1
        assert records[0].get_property("name") == "Alice"

        # Close database
        db.close()


@pytest.mark.server
@pytest.mark.skipif(not has_server_support(), reason="Requires server support")
def test_server_custom_config(temp_server_root):
    """Test server with custom configuration."""
    config = {"http_port": 8080, "host": "127.0.0.1", "mode": "production"}

    server = ArcadeDBServer(
        root_path=temp_server_root, root_password="test_password", config=config
    )
    server.start()
    time.sleep(1)

    assert server.get_http_port() == 8080

    server.stop()


@pytest.mark.server
@pytest.mark.skipif(not has_server_support(), reason="Requires server support")
def test_server_context_manager(temp_server_root):
    """Test server context manager."""
    with ArcadeDBServer(
        root_path=temp_server_root, root_password="test_password"
    ) as server:
        # Server auto-starts in context manager
        time.sleep(1)

        assert server.is_started()

        # Server should auto-stop when exiting context

    # Note: We can't easily test if stopped after context exit
    # because the server object is out of scope
