import time
from time import sleep

import psycopg
import pytest
import requests
from testcontainers.core.container import DockerContainer

arcadedb = (DockerContainer("arcadedata/arcadedb:25.2.1")
            .with_exposed_ports(2480, 2424, 5432)
            .with_env("JAVA_OPTS",
                      "-Darcadedb.server.rootPassword=playwithdata "
                      "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
                      "-Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin"))


def get_connection_params(container):
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    return {
        "host": host,
        "port": port,
        "user": "root",
        "password": "playwithdata",
        "dbname": "beer",
        "sslmode": "disable"
    }


def wait_for_http_endpoint(container, path, port, expected_status, timeout=60):
    """
    Wait for an HTTP endpoint to return the expected status code.

    Args:
        path: The HTTP path to check
        port: The port to connect to
        expected_status: The expected HTTP status code
        timeout: Maximum time to wait in seconds
    """
    host = container.get_container_host_ip()
    url = f"http://{host}:{container.get_exposed_port(port)}{path}"

    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == expected_status:
                return True

        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            pass

        sleep(1)
    raise TimeoutError(f"Container didn't respond with status {expected_status} at {url} within {timeout} seconds")


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    arcadedb.start()
    # wait_for_logs(arcadedb, "started", 60)
    wait_for_http_endpoint(arcadedb, "/api/v1/ready", 2480, 204, 10)


def test_psycopg2_basic_queries():
    """Test basic connectivity and queries using psycopg2."""
    # Get connection parameters
    params = get_connection_params(arcadedb)

    # Connect to the database
    conn = psycopg.connect(**params)
    conn.autocommit = True

    try:
        with conn.cursor() as cursor:

            # Query existing data from Beer dataset
            cursor.execute("SELECT FROM Beer LIMIT 10")

            beers = cursor.fetchall()
            assert len(beers) == 10

            # Create a new document type
            cursor.execute("CREATE DOCUMENT TYPE TestProduct")

            # Insert data
            cursor.execute("INSERT INTO TestProduct (name, price) VALUES ('TestItem', 29.99)")

            # Query the inserted data
            cursor.execute("SELECT * FROM TestProduct")
            products = cursor.fetchall()
            assert len(products) == 1

            # Check if data was correctly inserted
            # The first column is typically the @rid, so we check the subsequent columns
            product = products[0]
            assert 'TestItem' in product
            assert 29.99 in product
    finally:
        conn.close()
