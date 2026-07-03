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
import subprocess
import threading
import time
from pathlib import Path

import pytest
import requests
from neo4j import GraphDatabase, basic_auth
from testcontainers.core.container import DockerContainer

ROOT_PASSWORD = "playwithdata"
TLS_STORE_PASSWORD = "changeit"

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


def generate_tls_certs(cert_dir):
    """Generate a throwaway self-signed PKCS12 keystore + JKS truststore for BOLT TLS
    fixtures, via the JDK `keytool` binary. ArcadeDB does not ship a default TLS
    keystore (arcadedb.ssl.keyStore/.trustStore have no defaults - see
    BoltSslHelper.getRequiredProperty), so enabling arcadedb.bolt.ssl=REQUIRED/OPTIONAL
    without one crashes server startup with a ConfigurationException. This mirrors
    exactly what an operator would set up: a self-signed cert, trusted by the driver
    via bolt+ssc:// instead of a CA-signed one.
    """
    keystore_path = cert_dir / "keystore.p12"
    truststore_path = cert_dir / "truststore.jks"
    cert_path = cert_dir / "bolt.cer"

    subprocess.run(
        [
            "keytool",
            "-genkeypair",
            "-alias", "bolt",
            "-keyalg", "RSA",
            "-keysize", "2048",
            "-validity", "3650",
            "-keystore", str(keystore_path),
            "-storetype", "PKCS12",
            "-storepass", TLS_STORE_PASSWORD,
            "-keypass", TLS_STORE_PASSWORD,
            "-dname", "CN=localhost, OU=ArcadeDB, O=ArcadeDB, L=Test, ST=Test, C=US",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "keytool",
            "-exportcert",
            "-alias", "bolt",
            "-keystore", str(keystore_path),
            "-storetype", "PKCS12",
            "-storepass", TLS_STORE_PASSWORD,
            "-file", str(cert_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            "keytool",
            "-importcert",
            "-alias", "bolt",
            "-keystore", str(truststore_path),
            "-storetype", "JKS",
            "-storepass", TLS_STORE_PASSWORD,
            "-file", str(cert_path),
            "-noprompt",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return keystore_path, truststore_path


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


@pytest.fixture(scope="module")
def tls_certs(tmp_path_factory):
    cert_dir = tmp_path_factory.mktemp("bolt-tls-certs")
    keystore_path, truststore_path = generate_tls_certs(cert_dir)
    return cert_dir, keystore_path.name, truststore_path.name


@pytest.fixture(scope="module")
def bolt_container_tls_required(tls_certs):
    cert_dir, keystore_name, truststore_name = tls_certs
    container = (
        DockerContainer("arcadedata/arcadedb:latest")
        .with_exposed_ports(2480, 7687)
        .with_volume_mapping(str(cert_dir), "/home/arcadedb/tls_certs", "ro")
        .with_env(
            "JAVA_OPTS",
            "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD + " "
            "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
            "-Darcadedb.server.plugins=BoltProtocolPlugin "
            "-Darcadedb.bolt.ssl=REQUIRED "
            "-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/" + keystore_name + " "
            "-Darcadedb.ssl.keyStorePassword=" + TLS_STORE_PASSWORD + " "
            "-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/" + truststore_name + " "
            "-Darcadedb.ssl.trustStorePassword=" + TLS_STORE_PASSWORD,
        )
    )
    container.start()
    wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 60)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def bolt_container_tls_optional(tls_certs):
    cert_dir, keystore_name, truststore_name = tls_certs
    container = (
        DockerContainer("arcadedata/arcadedb:latest")
        .with_exposed_ports(2480, 7687)
        .with_volume_mapping(str(cert_dir), "/home/arcadedb/tls_certs", "ro")
        .with_env(
            "JAVA_OPTS",
            "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD + " "
            "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
            "-Darcadedb.server.plugins=BoltProtocolPlugin "
            "-Darcadedb.bolt.ssl=OPTIONAL "
            "-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/" + keystore_name + " "
            "-Darcadedb.ssl.keyStorePassword=" + TLS_STORE_PASSWORD + " "
            "-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/" + truststore_name + " "
            "-Darcadedb.ssl.trustStorePassword=" + TLS_STORE_PASSWORD,
        )
    )
    container.start()
    wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 60)
    yield container
    container.stop()


def test_CONN_002_tls_required(bolt_container_tls_required):
    driver = GraphDatabase.driver(
        bolt_uri(bolt_container_tls_required, scheme="bolt+ssc"),
        auth=basic_auth("root", ROOT_PASSWORD),
    )
    try:
        driver.verify_connectivity()
        with driver.session(database="beer") as session:
            result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
            assert result.single()["name"] is not None
    finally:
        driver.close()


@pytest.mark.xfail(
    strict=True,
    reason="BoltMessage.parseRoute (bolt/src/main/java/com/arcadedb/bolt/message/"
    "BoltMessage.java:142) casts the ROUTE message's third field straight to "
    "String, but under negotiated Bolt 4.4 it is actually a Map, causing a "
    "server-side ClassCastException; see #4916",
)
def test_CONN_003_neo4j_routing_single_node(bolt_container):
    driver = GraphDatabase.driver(bolt_uri(bolt_container, scheme="neo4j"), auth=basic_auth("root", ROOT_PASSWORD))
    try:
        driver.verify_connectivity()
        with driver.session(database="beer") as session:
            result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
            assert result.single()["name"] is not None
    finally:
        driver.close()


@pytest.mark.skip(
    reason="Requires a 3-node HA cluster; e2e-python's single-node harness "
    "cannot meaningfully exercise this scenario without new multi-node "
    "orchestration infrastructure - see #4890"
)
def test_CONN_004_neo4j_routing_ha_topology():
    pass


def test_CONN_005_tls_optional_plaintext_connects(bolt_container_tls_optional):
    driver = GraphDatabase.driver(
        bolt_uri(bolt_container_tls_optional, scheme="bolt"),
        auth=basic_auth("root", ROOT_PASSWORD),
    )
    try:
        driver.verify_connectivity()
    finally:
        driver.close()


# --- auth ----------------------------------------------------------------


def test_AUTH_001_basic_auth_valid(bolt_container):
    driver = GraphDatabase.driver(bolt_uri(bolt_container), auth=basic_auth("root", ROOT_PASSWORD))
    try:
        driver.verify_connectivity()
        with driver.session(database="beer") as session:
            assert session.run("RETURN 1 AS value").single()["value"] == 1
    finally:
        driver.close()


def test_AUTH_002_basic_auth_invalid(bolt_container):
    from neo4j.exceptions import AuthError

    driver = GraphDatabase.driver(bolt_uri(bolt_container), auth=basic_auth("root", "wrong-password"))
    try:
        with pytest.raises(AuthError) as exc_info:
            driver.verify_connectivity()
        assert exc_info.value.code == "Neo.ClientError.Security.Unauthorized"
    finally:
        driver.close()


def test_AUTH_003_auth_none_rejected(bolt_container):
    from neo4j.exceptions import AuthError, ServiceUnavailable

    driver = GraphDatabase.driver(bolt_uri(bolt_container), auth=None)
    try:
        with pytest.raises((AuthError, ServiceUnavailable)):
            driver.verify_connectivity()
    finally:
        driver.close()


# --- transactions ----------------------------------------------------------


def test_TX_001_autocommit_query(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5")
        records = list(result)
        assert len(records) == 5


def test_TX_002_explicit_commit_persists(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        tx = session.begin_transaction()
        tx.run("CREATE (:TxCommitProbe {marker: 'tx-002'})")
        tx.commit()

        result = session.run("MATCH (n:TxCommitProbe {marker: 'tx-002'}) RETURN count(n) AS c")
        assert result.single()["c"] == 1


def test_TX_003_explicit_rollback_discards(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        tx = session.begin_transaction()
        tx.run("CREATE (:TxRollbackProbe {marker: 'tx-003'})")
        tx.rollback()

        result = session.run("MATCH (n:TxRollbackProbe {marker: 'tx-003'}) RETURN count(n) AS c")
        assert result.single()["c"] == 0


def test_TX_004_managed_write_commits(bolt_driver):
    def create_beer(tx):
        tx.run("CREATE (:Beer {name: $n})", n="TX-004-Beer")

    with bolt_driver.session(database="beer") as session:
        session.execute_write(create_beer)
        result = session.run("MATCH (b:Beer {name: $n}) RETURN count(b) AS c", n="TX-004-Beer")
        assert result.single()["c"] == 1


def _race_two_writers(driver, database, marker):
    """Shared concurrency helper for TX-005 and ERR-004: two sessions race to
    update the same node inside an explicit transaction, one held open past
    the other's commit attempt. Returns the list of exceptions raised."""
    with driver.session(database=database) as setup_session:
        setup_session.run(
            "MERGE (n:RaceProbe {marker: $marker}) SET n.value = 0", marker=marker
        ).consume()

    barrier = threading.Barrier(2)
    errors = []

    def racing_write():
        with driver.session(database=database) as session:
            try:
                barrier.wait(timeout=5)
                tx = session.begin_transaction()
                tx.run(
                    "MATCH (n:RaceProbe {marker: $marker}) SET n.value = n.value + 1 RETURN n",
                    marker=marker,
                ).consume()
                time.sleep(0.5)
                tx.commit()
            except Exception as exc:  # noqa: BLE001 - want to inspect any driver-surfaced error
                errors.append(exc)

    threads = [threading.Thread(target=racing_write) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    return errors


@pytest.mark.xfail(
    strict=True,
    reason="BoltErrorCodes.java defines only 7 codes, all Neo.ClientError.*/ "
    "Neo.DatabaseError.* - no Neo.TransientError.* code exists, so ArcadeDB "
    "never signals a retryable condition and driver-side transient-retry "
    "logic cannot be exercised; see #4890",
)
def test_TX_005_managed_write_retries_on_transient_error(bolt_driver):
    from neo4j.exceptions import TransientError

    errors = _race_two_writers(bolt_driver, "beer", "tx-005")

    assert errors, "expected at least one racing session to fail on the write conflict"
    assert isinstance(errors[0], TransientError), (
        f"expected Neo.TransientError.*, got {type(errors[0]).__name__}: {errors[0]}"
    )


# --- causal-consistency ---------------------------------------------------


def test_CAUSAL_001_bookmark_read_after_write(bolt_driver):
    with bolt_driver.session(database="beer") as session_a:
        session_a.run("CREATE (:CausalProbe {marker: 'causal-001'})").consume()
        bookmarks = session_a.last_bookmarks()

    with bolt_driver.session(database="beer", bookmarks=bookmarks) as session_b:
        result = session_b.run("MATCH (n:CausalProbe {marker: 'causal-001'}) RETURN count(n) AS c")
        assert result.single()["c"] == 1


# --- multi-database --------------------------------------------------------


def test_MDB_001_session_selects_named_database(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
        assert result.single()["name"] is not None


def test_MDB_002_sessions_across_databases_are_isolated(bolt_driver):
    with bolt_driver.session(database="boltscratch") as scratch_session:
        tx = scratch_session.begin_transaction()
        tx.run("CREATE (:ScratchProbe {marker: 'mdb-002'})")

        with bolt_driver.session(database="beer") as beer_session:
            result = beer_session.run("MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c")
            assert result.single()["c"] == 0

        tx.commit()

    with bolt_driver.session(database="boltscratch") as verify_session:
        result = verify_session.run("MATCH (n:ScratchProbe {marker: 'mdb-002'}) RETURN count(n) AS c")
        assert result.single()["c"] == 1
