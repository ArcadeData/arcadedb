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
Bolt protocol conformance suite for issue #4885, implementing every scenario
in bolt/conformance/spec.yaml (issue #4883) against the official `neo4j`
Python driver. Each test function name embeds its spec.yaml scenario id
(test_<AREA>_<NNN>_<slug>) for traceability, per bolt/conformance/README.md's
"Traceability convention".
"""

import datetime
import threading
import time
from pathlib import Path

import pytest
import requests
from neo4j import GraphDatabase, basic_auth
from testcontainers.core.container import DockerContainer

ROOT_PASSWORD = "playwithdata"

REPO_ROOT = Path(__file__).resolve().parents[2]
TYPE_MATRIX_FIXTURE = REPO_ROOT / "bolt" / "conformance" / "fixtures" / "type-matrix.cypher"


def wait_for_http_endpoint(container, path, port, expected_status, timeout=60):
    """Wait for an HTTP endpoint to return the expected status code."""
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
        time.sleep(1)
    raise TimeoutError(f"Container didn't respond with status {expected_status} at {url} within {timeout} seconds")


def bolt_uri(container, scheme="bolt"):
    host = container.get_container_host_ip()
    port = container.get_exposed_port(7687)
    return f"{scheme}://{host}:{port}"


def create_database(container, db_name):
    host = container.get_container_host_ip()
    port = container.get_exposed_port(2480)
    response = requests.post(
        f"http://{host}:{port}/api/v1/server",
        auth=("root", ROOT_PASSWORD),
        json={"command": f"create database {db_name}"},
        timeout=10,
    )
    response.raise_for_status()


def seed_type_matrix(container):
    host = container.get_container_host_ip()
    port = container.get_exposed_port(2480)
    command = TYPE_MATRIX_FIXTURE.read_text()
    response = requests.post(
        f"http://{host}:{port}/api/v1/command/beer",
        auth=("root", ROOT_PASSWORD),
        json={"language": "cypher", "command": command},
        timeout=10,
    )
    response.raise_for_status()


@pytest.fixture(scope="module")
def bolt_container():
    container = (
        DockerContainer("arcadedata/arcadedb:latest")
        .with_exposed_ports(2480, 7687)
        .with_env(
            "JAVA_OPTS",
            "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD + " "
            "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
            "-Darcadedb.server.plugins=BoltProtocolPlugin",
        )
    )
    container.start()
    wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 60)
    create_database(container, "boltscratch")
    seed_type_matrix(container)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def bolt_driver(bolt_container):
    driver = GraphDatabase.driver(bolt_uri(bolt_container), auth=basic_auth("root", ROOT_PASSWORD))
    yield driver
    driver.close()


# --- connection ---------------------------------------------------------


def test_CONN_001_connect_bolt(bolt_driver):
    bolt_driver.verify_connectivity()
