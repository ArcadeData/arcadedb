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
    stdout, stderr = container.get_logs()
    raise TimeoutError(
        f"Container didn't respond with status {expected_status} at {url} within "
        f"{timeout} seconds\n--- container stdout ---\n{stdout.decode(errors='replace')}\n"
        f"--- container stderr ---\n{stderr.decode(errors='replace')}"
    )


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
    # TLS keystore/truststore loading adds startup latency beyond the default
    # container's margin, especially on shared CI runners - give it more room.
    wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 120)
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
    # See bolt_container_tls_required's comment: TLS startup needs extra margin.
    wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 120)
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


# --- result-handling ---------------------------------------------------


def test_RESULT_001_streaming_pull_incremental(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 10")
        seen = 0
        for record in result:
            assert record["name"] is not None
            seen += 1
        assert seen == 10


def test_RESULT_002_partial_pull_then_continue(bolt_driver):
    with bolt_driver.session(database="beer", fetch_size=2) as session:
        result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5")
        iterator = iter(result)
        first_two = [next(iterator), next(iterator)]
        assert len(first_two) == 2

        remaining = list(result)
        assert len(remaining) == 3


def test_RESULT_003_discard_abandons_remaining(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 5")
        next(iter(result))
        summary = result.consume()
        assert summary is not None


@pytest.mark.xfail(
    strict=True,
    reason="BoltNetworkExecutor.handlePull/handleDiscard never populate a "
    "'stats' key in the SUCCESS message metadata for write queries - the "
    "engine's Cypher CREATE/SET/DELETE steps do not track node/relationship/"
    "property counters anywhere that the Bolt layer could surface, so the "
    "neo4j driver always parses an empty SummaryCounters; see RESULT-004 in "
    "bolt/conformance/spec.yaml",
)
def test_RESULT_004_summary_counters_reflect_writes(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        result = session.run(
            "CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})",
            n="RESULT-004-Beer",
            b="RESULT-004-Brewery",
        )
        counters = result.consume().counters
        assert counters.nodes_created == 2
        assert counters.relationships_created == 1
        assert counters.properties_set >= 2


# --- type-roundtrip ---------------------------------------------------


def test_TYPE_001_node_roundtrip(bolt_driver):
    from neo4j.graph import Node

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (b:Beer) RETURN b LIMIT 1").single()
        node = record["b"]
        assert isinstance(node, Node)
        assert "Beer" in node.labels
        assert node.get("name") is not None


def test_TYPE_002_relationship_roundtrip(bolt_driver):
    from neo4j.graph import Relationship

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH ()-[r]->() RETURN r LIMIT 1").single()
        rel = record["r"]
        assert isinstance(rel, Relationship)
        assert rel.type is not None


@pytest.mark.xfail(
    strict=True,
    reason="structure/BoltPath.java exists but has zero call sites "
    "constructing it anywhere in BoltStructureMapper - query results never "
    "actually produce native Path structures today; see #4890",
)
def test_TYPE_003_path_roundtrip(bolt_driver):
    from neo4j.graph import Path

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1").single()
        path = record["p"]
        assert isinstance(path, Path)
        assert len(path.nodes) >= 2


def test_TYPE_004_bytearray_param_roundtrip(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        payload = bytearray([1, 2, 3, 4])
        record = session.run("RETURN $b AS echo", b=payload).single()
        assert bytes(record["echo"]) == bytes(payload)


def test_TYPE_005_nested_list_map_roundtrip(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        record = session.run(
            "MATCH (t:TypeMatrix) RETURN t.nestedListProp AS l, t.nestedMapProp AS m"
        ).single()
        assert record["l"] == [1, 2, [3, 4]]
        assert record["m"] == {"a": 1, "b": {"c": 2}}


def test_TYPE_006_null_roundtrip(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.nullProp AS n").single()
        assert record["n"] is None

        echo = session.run("RETURN $p AS echo", p=None).single()
        assert echo["echo"] is None


@pytest.mark.xfail(
    strict=True,
    reason="BoltStructureMapper.toPackStreamValue converts LocalDate via "
    ".toString()/ISO string fallback instead of the native Bolt Date "
    "structure (sig 0x44); see #4890",
)
def test_TYPE_007_local_date_roundtrip(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.localDateProp AS d").single()
        assert isinstance(record["d"], datetime.date)

        echo = session.run("RETURN $d AS echo", d=record["d"]).single()
        assert echo["echo"] == record["d"]


@pytest.mark.xfail(
    strict=True,
    reason="Same ISO-string fallback as TYPE-007, for LocalTime (native Bolt "
    "LocalTime structure sig 0x74 not produced); see #4890",
)
def test_TYPE_008_local_time_roundtrip(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.localTimeProp AS t2").single()
        assert isinstance(record["t2"], datetime.time)

        echo = session.run("RETURN $t AS echo", t=record["t2"]).single()
        assert echo["echo"] == record["t2"]


@pytest.mark.xfail(
    strict=True,
    reason="Same ISO-string fallback as TYPE-007, for LocalDateTime (native "
    "Bolt LocalDateTime structure sig 0x64 not produced); see #4890",
)
def test_TYPE_009_local_datetime_roundtrip(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.localDateTimeProp AS dt").single()
        assert isinstance(record["dt"], datetime.datetime)

        echo = session.run("RETURN $dt AS echo", dt=record["dt"]).single()
        assert echo["echo"] == record["dt"]


@pytest.mark.xfail(
    strict=True,
    reason="Same ISO-string fallback as TYPE-007, for OffsetDateTime/"
    "ZonedDateTime (native Bolt DateTime/DateTimeZoneId structures, sig "
    "0x49/0x69, not produced); see #4890",
)
def test_TYPE_010_offset_datetime_roundtrip(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.offsetDateTimeProp AS dt").single()
        dt = record["dt"]
        assert isinstance(dt, datetime.datetime)
        assert dt.utcoffset() == datetime.timedelta(hours=2)

        echo = session.run("RETURN $dt AS echo", dt=dt).single()
        assert echo["echo"] == dt


@pytest.mark.xfail(
    strict=True,
    reason="BoltStructureMapper/PackStreamWriter have no Duration handling "
    "at all - not even a string fallback branch, falls through to generic "
    "value.toString(); see #4890",
)
def test_TYPE_011_duration_roundtrip(bolt_driver):
    from neo4j.time import Duration

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.durationProp AS d").single()
        assert isinstance(record["d"], Duration)

        echo = session.run("RETURN $d AS echo", d=record["d"]).single()
        assert echo["echo"] == record["d"]


@pytest.mark.xfail(
    strict=True,
    reason="No Point/spatial type handling exists anywhere in "
    "BoltStructureMapper or PackStreamWriter (the underlying ArcadeDB "
    "Cypher engine itself does support point() - the gap is Bolt wire "
    "serialization only); see #4890",
)
def test_TYPE_012_point_roundtrip(bolt_driver):
    from neo4j.spatial import Point

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.pointProp AS p").single()
        point = record["p"]
        assert isinstance(point, Point)
        assert point.x == pytest.approx(12.34)
        assert point.y == pytest.approx(56.78)

        echo = session.run("RETURN $p AS echo", p=point).single()
        assert echo["echo"].x == pytest.approx(12.34)
        assert echo["echo"].y == pytest.approx(56.78)


# --- errors ---------------------------------------------------------------


def test_ERR_001_syntax_error(bolt_driver):
    from neo4j.exceptions import ClientError

    with bolt_driver.session(database="beer") as session:
        with pytest.raises(ClientError) as exc_info:
            session.run("MATCH (n RETURN n").consume()
        assert exc_info.value.code == "Neo.ClientError.Statement.SyntaxError"


@pytest.mark.xfail(
    strict=True,
    reason="CypherSemanticValidator correctly detects the undefined "
    "variable but throws it via CommandParsingException, the same "
    "exception class used for genuine ANTLR syntax errors - "
    "BoltNetworkExecutor's RUN handler maps error codes by exception "
    "type, so it cannot distinguish semantic from syntax errors, "
    "making Neo.ClientError.Statement.SemanticError effectively "
    "dead code in the Bolt module; see #4890",
)
def test_ERR_002_semantic_error(bolt_driver):
    from neo4j.exceptions import ClientError

    with bolt_driver.session(database="beer") as session:
        with pytest.raises(ClientError) as exc_info:
            session.run("MATCH (n:Beer) RETURN undeclaredVariable").consume()
        assert exc_info.value.code == "Neo.ClientError.Statement.SemanticError"


@pytest.mark.skip(
    reason="ERR-003 requires sending RUN before completing HELLO/LOGON; the "
    "official neo4j driver's public API always completes the handshake "
    "internally, so this cannot be triggered without a bespoke raw-socket "
    "client, which is out of scope per the epic's 'official drivers only' "
    "principle. spec.yaml's ERR-003 current_status has been updated to "
    "not-applicable to reflect this."
)
def test_ERR_003_unauthenticated_request_rejected():
    pass


@pytest.mark.xfail(
    strict=True,
    reason="BoltErrorCodes.java defines only Neo.ClientError.*/"
    "Neo.DatabaseError.* codes (7 total) - no Neo.TransientError.* code "
    "exists anywhere in the Bolt module; see #4890",
)
def test_ERR_004_transient_condition_error_code(bolt_driver):
    from neo4j.exceptions import TransientError

    errors = _race_two_writers(bolt_driver, "beer", "err-004")

    assert errors, "expected at least one racing session to fail on the write conflict"
    assert isinstance(errors[0], TransientError), (
        f"expected Neo.TransientError.*, got {type(errors[0]).__name__}: {errors[0]}"
    )


# --- protocol ---------------------------------------------------------


def test_PROTO_001_version_negotiation_succeeds(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        assert session.run("RETURN 1 AS value").single()["value"] == 1


@pytest.mark.xfail(
    strict=True,
    reason="BoltNetworkExecutor.SUPPORTED_VERSIONS never advertises any "
    "Bolt 5.x version; drivers that support 5.x only work today by "
    "silently downgrading to 4.4, which is undocumented and untested as a "
    "deliberate compatibility stance; see #4890",
)
def test_PROTO_002_bolt_5x_negotiation_is_documented(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        summary = session.run("RETURN 1 AS value").consume()
        negotiated_version = summary.server.protocol_version
        assert negotiated_version[0] >= 5, (
            f"driver silently downgraded to Bolt {negotiated_version} "
            "instead of negotiating a documented Bolt 5.x version"
        )


def test_PROTO_003_reset_mid_stream(bolt_driver):
    with bolt_driver.session(database="beer", fetch_size=2) as session:
        result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 10")
        next(iter(result))
        result.consume()  # abandon the remaining stream mid-flight

        follow_up = session.run("RETURN 1 AS value")
        assert follow_up.single()["value"] == 1
