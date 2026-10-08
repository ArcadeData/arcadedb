"""
Server and Studio tests for ArcadeDB Python bindings.

These tests use the JAVA API for server operations:
- Server creation and management via ArcadeDBServer class
- Database operations via direct JVM method calls (db.command, db.query)
- All operations happen within the same Python process (embedded mode)

Note: These tests do NOT test HTTP API access patterns.
For HTTP API testing, see test_server_patterns.py

These tests require server support (available in our base package).
"""

import time

import pytest
from arcadedb_embedded import ArcadeDBServer
from tests.conftest import TEST_PASSWORD


@pytest.mark.server
def test_server_creation(temp_server_root):
    """Test creating and starting a server."""
    server = ArcadeDBServer(
        root_path=temp_server_root,
        root_password=TEST_PASSWORD,
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
def test_server_database_operations(temp_server_root):
    """
    Test database operations through server using Java API.

    This test demonstrates:
    - Server-managed database creation via Java API
    - Direct JVM method calls (db.command, db.query)
    - Operations within same Python process (embedded access)
    """
    with ArcadeDBServer(
        root_path=temp_server_root, root_password=TEST_PASSWORD
    ) as server:
        # Server auto-starts in context manager
        time.sleep(1)

        # Create database through server
        db = server.create_database("testdb")
        assert db.is_open()

        # Use database
        # Schema statements apply immediately (no transaction needed)
        db.command("sql", "CREATE DOCUMENT TYPE Person")

        with db.transaction():
            db.command("sql", "INSERT INTO Person SET name = 'Alice', age = 30")

        # Query
        result = db.query("sql", "SELECT FROM Person")
        records = list(result)
        assert len(records) == 1
        assert records[0].get("name") == "Alice"

        # Close database
        db.close()


@pytest.mark.server
def test_server_custom_config(temp_server_root):
    """Test server with custom configuration."""
    config = {"http_port": 8080, "host": "127.0.0.1", "mode": "production"}

    server = ArcadeDBServer(
        root_path=temp_server_root, root_password=TEST_PASSWORD, config=config
    )
    server.start()
    time.sleep(1)

    assert server.get_http_port() == 8080

    server.stop()


@pytest.mark.server
def test_server_context_manager(temp_server_root):
    """Test server context manager."""
    with ArcadeDBServer(
        root_path=temp_server_root, root_password=TEST_PASSWORD
    ) as server:
        # Server auto-starts in context manager
        time.sleep(1)

        assert server.is_started()

        # Server should auto-stop when exiting context

    # Note: We can't easily test if stopped after context exit
    # because the server object is out of scope


def test_default_host_is_localhost(temp_server_root):
    """Default host should be localhost; binding to all interfaces must be opt-in.

    Asserts on the publicly-observable Studio URL composition; we do not
    start the server here because that requires a real JVM. The same default
    host feeds both ``get_studio_url()`` and the underlying ContextConfiguration,
    so the URL is a faithful proxy for the configured host.
    """
    from arcadedb_embedded.server import ArcadeDBServer

    server = ArcadeDBServer(
        root_path=temp_server_root,
        root_password=TEST_PASSWORD,
    )
    assert server.get_studio_url().startswith("http://localhost:")


@pytest.mark.server
def test_failed_server_start_does_not_hang_process_exit(tmp_path):
    """A server.start() that fails part-way must not leave the process unable to exit.

    Regression: when a plugin fails to start, the engine has already started
    non-daemon threads (the HTTP idempotency cleaner, the security and session
    timers) and only the Java stop() ends them. The wrapper's stop() returned
    early because the start never completed, so the process hung at exit (a CI
    job ran 28 minutes past its tests on 2026-10-02). The failure is forced with
    a Postgres port out of range, which fails on every OS; a busy port does not
    (macOS lets the engine's listener bind a port another socket holds).
    """
    import subprocess  # nosec B404 - fixed argv, no shell
    import sys

    code = (
        "import socket\n"
        "from arcadedb_embedded import create_server\n"
        "def free():\n"
        "    with socket.socket() as s:\n"
        "        s.bind(('127.0.0.1', 0))\n"
        "        return s.getsockname()[1]\n"
        "server = create_server(\n"
        f"    root_path={str(tmp_path / 'databases')!r},\n"
        f"    root_password={TEST_PASSWORD!r},\n"
        "    config={\n"
        "        'http_port': free(),\n"
        "        'server_plugins': 'Redis:com.arcadedb.redis.RedisProtocolPlugin,'\n"
        "                          'Postgres:com.arcadedb.postgres.PostgresProtocolPlugin',\n"
        "        'redis_port': free(),\n"
        "        'postgres_port': 70000,\n"
        "    },\n"
        ")\n"
        "try:\n"
        "    server.start()\n"
        "    print('started')\n"
        "    server.stop()\n"
        "except Exception:\n"
        "    print('start failed')\n"
    )
    proc = subprocess.run(  # nosec B603 - interpreter + inline snippet, no shell
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=120
    )
    assert "start failed" in proc.stdout, proc.stdout + proc.stderr
    assert proc.returncode == 0
