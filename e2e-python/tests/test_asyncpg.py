import time
import pytest
import pytest_asyncio
import asyncpg
import requests
from testcontainers.core.container import DockerContainer
from time import sleep

arcadedb = (DockerContainer("arcadedata/arcadedb:latest")
            .with_exposed_ports(2480, 5432)
            .with_env("JAVA_OPTS",
                      "-Darcadedb.server.rootPassword=playwithdata "
                      "-Darcadedb.server.defaultDatabases=asyncpg_testdb[root]{} "
                      "-Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin"))


def get_connection_params(container):
    """Extract connection parameters from container."""
    return {
        "host": container.get_container_host_ip(),
        "port": int(container.get_exposed_port(5432)),
        "database": "asyncpg_testdb",
        "user": "root",
        "password": "playwithdata",
        "ssl": None,
        "timeout": 30
    }


def wait_for_http_endpoint(container, path, port, expected_status, timeout=60):
    """Wait for HTTP endpoint to become available."""
    start = time.time()
    host = container.get_container_host_ip()
    mapped_port = int(container.get_exposed_port(port))
    url = f"http://{host}:{mapped_port}{path}"

    while time.time() - start < timeout:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == expected_status:
                return True
        except (requests.ConnectionError, requests.Timeout):
            pass
        sleep(1)

    raise TimeoutError(f"HTTP endpoint {url} did not become ready in {timeout}s")


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    """Start container and wait for readiness."""
    arcadedb.start()
    try:
        # Wait for HTTP API to be ready (returns 204 No Content when ready)
        wait_for_http_endpoint(arcadedb, "/api/v1/ready", 2480, 204)

        # Create the database via HTTP API
        host = arcadedb.get_container_host_ip()
        port = int(arcadedb.get_exposed_port(2480))
        url = f"http://{host}:{port}/api/v1/server"
        response = requests.post(
            url,
            json={"command": "create database asyncpg_testdb"},
            auth=("root", "playwithdata"),
            timeout=10
        )
        if response.status_code not in (200, 204):
            raise RuntimeError(f"Failed to create database: {response.status_code} - {response.text}")

        # Give the PostgreSQL protocol plugin additional time to initialize
        sleep(2)
        yield
    finally:
        arcadedb.stop()


@pytest_asyncio.fixture
async def connection():
    """Provide asyncpg connection to test database."""
    params = get_connection_params(arcadedb)
    conn = await asyncpg.connect(**params)
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture(scope="module")
async def test_type_setup():
    """Create AsyncpgTest type once per module"""
    params = get_connection_params(arcadedb)
    conn = await asyncpg.connect(**params)
    try:
        # Create type (ignore error if already exists)
        try:
            await conn.execute("CREATE DOCUMENT TYPE AsyncpgTest")
        except Exception:
            pass  # Type may already exist from previous run
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_connection_fixture(connection):
    """Verify connection fixture works."""
    # Verify connection is active
    assert not connection.is_closed()
    # Note: Query execution deferred until protocol bug is fixed (Issue #668)


@pytest.mark.asyncio
async def test_basic_connection():
    """Test 1: Basic connection (Issue #668)"""
    params = get_connection_params(arcadedb)
    conn = await asyncpg.connect(**params)
    await conn.close()


@pytest.mark.asyncio
async def test_simple_query(connection):
    """Test 2: Simple query without parameters"""
    # Query schema types
    rows = await connection.fetch("SELECT FROM schema:types")
    assert isinstance(rows, list)
    # Schema should have at least some built-in types
    assert len(rows) > 0


@pytest.mark.asyncio
async def test_create_type_and_insert(connection, test_type_setup):
    """Test 3: Create document type and insert data"""
    # Insert test data
    await connection.execute(
        "INSERT INTO AsyncpgTest SET id = 'test1', name = 'Alice', value = 100"
    )
    await connection.execute(
        "INSERT INTO AsyncpgTest SET id = 'test2', name = 'Bob', value = 200"
    )

    # Verify insertion
    rows = await connection.fetch("SELECT FROM AsyncpgTest WHERE id IN ('test1', 'test2')")
    assert len(rows) == 2
