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

"""
Issue #11838: over the Postgres wire protocol a prepared statement described the
columns of schemaless properties as varchar (text[] for a list), so psycopg read
true as 't', 42 as '42', 2.5 as '2.5' and [1, 2, 3] as ['1', '2', '3'] from the
6th execution of the same query on (psycopg prepares a statement on the server
once it has run prepare_threshold=5 times). A uuid.UUID parameter could not be
bound at all: "Type with code 2950 not supported for deserializing".

psycopg 3 goes through libpq's PQprepare (Parse) and PQexecPrepared (Bind,
Describe portal, Execute) for a prepared statement, so every execution from the
6th on is answered by a Describe of the portal of a named statement.
"""

import os
import uuid

import psycopg
import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy

ROOT_PASSWORD = "playwithdata"
DATABASE = "pgprepared11838"
TYPE = "T11838"
QUERY = f"SELECT v FROM {TYPE} WHERE name = %s"
ID = uuid.UUID("0f8fad5b-d9cb-469f-a165-70867728950e")

# psycopg's default: a query text is prepared on the server from its 6th execution
PREPARE_THRESHOLD = 5
EXECUTIONS = PREPARE_THRESHOLD + 3

# name -> (expected Python value, expected Python type, PostgreSQL type OID the column must be described with)
EXPECTED = {
    "bool": (True, bool, 16),
    "int": (42, int, 23),
    "double": (2.5, float, 701),
    "list": ([1, 2, 3], list, 1007),
}

arcadedb = (DockerContainer(os.environ.get("ARCADEDB_DOCKER_IMAGE") or "arcadedata/arcadedb:latest")
            .with_exposed_ports(2480, 5432)
            # the server answers 204 once it is ready: start() returns then, no polling loop of our own
            .waiting_for(HttpWaitStrategy(2480, "/api/v1/ready").for_status_code(204).with_startup_timeout(120))
            .with_env("JAVA_OPTS",
                      f"-Darcadedb.server.rootPassword={ROOT_PASSWORD} "
                      "-Darcadedb.server.plugins=PostgresProtocolPlugin"))


def http_base():
    return f"http://{arcadedb.get_container_host_ip()}:{arcadedb.get_exposed_port(2480)}"


def http_command(path, body):
    response = requests.post(http_base() + path, json=body, auth=("root", ROOT_PASSWORD), timeout=30)
    assert response.status_code < 300, f"POST {path} -> {response.status_code}: {response.text}"


@pytest.fixture(scope="module", autouse=True)
def setup():
    arcadedb.start()
    try:
        http_command("/api/v1/server", {"command": f"create database {DATABASE}"})
        command = f"/api/v1/command/{DATABASE}"
        http_command(command, {"language": "sql", "command": f"CREATE DOCUMENT TYPE {TYPE}"})
        # no property is declared: every column below is schemaless
        http_command(command, {"language": "sql", "command": f"INSERT INTO {TYPE} CONTENT :r", "params": {"r": [
            {"name": "bool", "v": True},
            {"name": "int", "v": 42},
            {"name": "double", "v": 2.5},
            {"name": "list", "v": [1, 2, 3]},
            {"name": "uuid", "u": str(ID)},
        ]}})
        yield
    finally:
        arcadedb.stop()


@pytest.fixture
def conn():
    connection = psycopg.connect(host=arcadedb.get_container_host_ip(), port=arcadedb.get_exposed_port(5432),
                                 dbname=DATABASE, user="root", password=ROOT_PASSWORD, sslmode="disable",
                                 autocommit=True)
    try:
        yield connection
    finally:
        connection.close()


def assert_native(cursor, name, execution):
    expected_value, expected_type, expected_oid = EXPECTED[name]
    rows = cursor.fetchall()
    assert len(rows) == 1, f"{name} execution {execution}: {rows!r}"
    value = rows[0][0]
    assert type(value) is expected_type, \
        f"{name} execution {execution}: {value!r} is a {type(value).__name__}, not a {expected_type.__name__}"
    assert value == expected_value, f"{name} execution {execution}: {value!r}"
    if expected_type is list:
        assert all(type(element) is int for element in value), f"{name} execution {execution}: {value!r}"
    # 1007 is int4[]; ArcadeDB may describe the list as int8[] (1016) as well, but never as text[] (1009)
    type_code = cursor.description[0].type_code
    if expected_type is list:
        assert type_code in (1007, 1016), f"{name} execution {execution}: column described as OID {type_code}"
    else:
        assert type_code == expected_oid, f"{name} execution {execution}: column described as OID {type_code}"


@pytest.mark.parametrize("name", list(EXPECTED))
def test_prepared_statement_keeps_native_types_past_the_prepare_threshold(conn, name):
    """The same query text run 8 times: 1-5 unprepared, 6-8 prepared on the server by psycopg itself."""
    assert conn.prepare_threshold == PREPARE_THRESHOLD
    with conn.cursor() as cursor:
        for execution in range(1, EXECUTIONS + 1):
            cursor.execute(QUERY, (name,))
            assert_native(cursor, name, execution)


def test_prepared_statement_whose_rows_change_type_between_executions(conn):
    """One server-prepared statement serving rows of a different type on each execution."""
    with conn.cursor() as cursor:
        for execution in range(1, EXECUTIONS + 1):
            for name in EXPECTED:
                cursor.execute(QUERY, (name,), prepare=True)
                assert_native(cursor, name, execution)


def test_prepared_statement_with_binary_results(conn):
    """The same, with the rows sent in binary: the described type decides how the client decodes them."""
    with conn.cursor(binary=True) as cursor:
        for execution in range(1, EXECUTIONS + 1):
            for name in EXPECTED:
                cursor.execute(QUERY, (name,), prepare=True)
                assert_native(cursor, name, execution)


@pytest.mark.parametrize("placeholder", ["%s", "%t", "%b"])
def test_uuid_parameter_binds(conn, placeholder):
    """A uuid.UUID is bound as PostgreSQL's uuid (OID 2950): %s and %t in text, %b in binary."""
    with conn.cursor() as cursor:
        for execution in range(1, EXECUTIONS + 1):
            cursor.execute(f"SELECT name, u FROM {TYPE} WHERE u = {placeholder}", (ID,))
            rows = cursor.fetchall()
            assert rows == [("uuid", str(ID))], f"execution {execution} with {placeholder}: {rows!r}"


def test_uuid_parameter_round_trips_through_an_insert(conn):
    """A uuid written through a bound parameter reads back as the canonical text PostgreSQL prints."""
    written = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    with conn.cursor() as cursor:
        cursor.execute(f"INSERT INTO {TYPE} SET name = %s, u = %s", ("uuid-written", written))
        for execution in range(1, EXECUTIONS + 1):
            cursor.execute(f"SELECT u FROM {TYPE} WHERE name = %s AND u = %s", ("uuid-written", written))
            rows = cursor.fetchall()
            assert rows == [(str(written),)], f"execution {execution}: {rows!r}"
            assert uuid.UUID(rows[0][0]) == written
