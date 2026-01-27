import time
import pytest
import pytest_asyncio
import asyncpg
import requests
from testcontainers.core.container import DockerContainer
from time import sleep

# Global container reference
_container = None

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
    global _container
    _container = arcadedb.start()
    try:
        # Wait for HTTP API to be ready (returns 204 No Content when ready)
        wait_for_http_endpoint(_container, "/api/v1/ready", 2480, 204)

        # Create the database via HTTP API
        host = _container.get_container_host_ip()
        port = int(_container.get_exposed_port(2480))
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
        _container.stop()


@pytest_asyncio.fixture
async def connection():
    """Provide asyncpg connection to test database."""
    global _container
    params = get_connection_params(_container)
    conn = await asyncpg.connect(**params)
    try:
        yield conn
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_connection_fixture(connection):
    """Verify connection fixture works."""
    # Execute simple query
    result = await connection.fetchval("SELECT 1")
    assert result == 1
