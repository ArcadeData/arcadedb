import time
from time import sleep
import random
import psycopg
import pytest
import requests
from testcontainers.core.container import DockerContainer
import json
import string

import pytest
from pytest_check import check

arcadedb = (DockerContainer("arcadedata/arcadedb:latest")
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


def test_psycopg2_return_array_floats():
    """Check if the driver correctly sends the array of floats as a list of floats to the python driver"""
    # Get connection parameters
    params = get_connection_params(arcadedb)

    # Connect to the database
    conn = psycopg.connect(**params)
    conn.autocommit = True



    try:
        with conn.cursor() as cursor:

            cursor.execute("create vertex type `TEXT_EMBEDDING` if not exists;")

            cursor.execute("create property TEXT_EMBEDDING.str if not exists STRING;")
            cursor.execute("create property TEXT_EMBEDDING.embedding if not exists ARRAY_OF_FLOATS;")

            cursor.execute('INSERT INTO `TEXT_EMBEDDING` SET str = "meow", embedding = [0.1,0.2,0.3] RETURN embedding')
            embeddings = cursor.fetchone()[0]
            assert isinstance(embeddings, list) and all(isinstance(item, float) for item in embeddings), f"Type ARRAY_OF_FLOATS is returned as {type(embeddings)} instead of list of floats"
    finally:
        conn.close()


def random_values(_type, size=64):
    if _type == bool:  # Note: fixed the '=' to '==' for comparison
        return [random.choice([True, False]) for _ in range(size)]
    elif _type == float:
        return [random.uniform(-100, 100) for _ in range(size)]
    elif _type == int:
        return [random.randint(-100, 100) for _ in range(size)]
    elif _type == str:
        # Generate random strings of length between 5 and 15
        return [''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(5, 15)))
                for _ in range(size)]
    else:
        raise ValueError(f"Unsupported type: {_type}")

def test_psycopg2_return_array_common():
    """Check if the driver correctly sends the array of floats as a list of floats to the python driver"""
    # Get connection parameters
    params = get_connection_params(arcadedb)

    # Connect to the database
    conn = psycopg.connect(**params)
    conn.autocommit = True

    types_to_test = [bool, float, int, str]

    try:
        for type_to_test in types_to_test:
            # Extract just the type name from the class representation
            type_name = type_to_test.__name__
            arcade_name = f"TEXT_{type_name}"

            with conn.cursor() as cursor:
                cursor.execute(f"create vertex type `{arcade_name}` if not exists;")
                cursor.execute(f"create property {arcade_name}.str if not exists STRING;")
                cursor.execute(f"create property {arcade_name}.data if not exists LIST;")

                cursor.execute(f'INSERT INTO `{arcade_name}` SET str = "meow", data = {json.dumps(random_values(type_to_test))} RETURN data')
                datas = cursor.fetchone()[0]

                # Use pytest-check to continue even after assertion failures
                with check:
                    assert isinstance(datas, list), f"For {type_name}: Type LIST is returned as {type(datas)} instead of list"

                if isinstance(datas, list):
                    with check:
                        assert all(isinstance(item, type_to_test) for item in datas), f"For {type_name}: Not all items are of type {type_name}"
    finally:
        conn.close()
