#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import time
from time import sleep

import pytest
import requests
from sqlalchemy import create_engine, text
from testcontainers.core.container import DockerContainer

arcadedb = (DockerContainer("arcadedata/arcadedb:latest")
            .with_exposed_ports(2480, 2424, 5432)
            .with_env("JAVA_OPTS",
                      "-Darcadedb.server.rootPassword=playwithdata "
                      "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
                      "-Darcadedb.server.plugins=PostgresProtocolPlugin"))


def get_engine(container):
    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    url = f"postgresql+psycopg://root:playwithdata@{host}:{port}/beer"
    return create_engine(url, connect_args={"sslmode": "disable"})


def wait_for_http_endpoint(container, path, port, expected_status, timeout=60):
    host = container.get_container_host_ip()
    url = f"http://{host}:{container.get_exposed_port(port)}{path}"
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == expected_status:
                return True
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass
        sleep(1)
    raise TimeoutError(f"Container didn't respond with status {expected_status} at {url} within {timeout} seconds")


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    arcadedb.start()
    wait_for_http_endpoint(arcadedb, "/api/v1/ready", 2480, 204, 60)


def test_sqlalchemy_connect():
    """SQLAlchemy engine.connect() must not raise TypeError on server_version."""
    engine = get_engine(arcadedb)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        row = result.fetchone()
        assert row is not None


def test_sqlalchemy_query_existing_data():
    """SQLAlchemy text() query against Beer dataset returns rows."""
    engine = get_engine(arcadedb)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT FROM Beer LIMIT 5"))
        rows = result.fetchall()
        assert len(rows) == 5


def test_sqlalchemy_create_and_insert():
    """SQLAlchemy DDL and DML via text() work end-to-end."""
    engine = get_engine(arcadedb)
    with engine.connect() as conn:
        conn.execute(text("CREATE DOCUMENT TYPE SaProduct IF NOT EXISTS"))
        conn.execute(text("INSERT INTO SaProduct (name, price) VALUES ('Widget', 9)"))
        result = conn.execute(text("SELECT FROM SaProduct"))
        rows = result.fetchall()
        assert len(rows) >= 1
        flat = [str(v) for row in rows for v in row]
        assert any("Widget" in v for v in flat)
