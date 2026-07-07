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

"""Bolt protocol conformance suite for issue #4885.

Implements every scenario in bolt/conformance/spec.yaml (issue #4883)
against the official `neo4j` Python driver. Each test function name embeds
its spec.yaml scenario id (test_<AREA>_<NNN>_<slug>) for traceability, per
bolt/conformance/README.md's "Traceability convention".

Note: since #5001 ArcadeDB advertises Bolt 5.0-5.4 (and still 4.4/4.0/3.0),
so the pinned `neo4j` driver negotiates 5.x (see PROTO-002).
"""

import datetime
import platform
import shutil
import subprocess
import threading
import time
from pathlib import Path

import docker
import pytest
import requests
from neo4j import GraphDatabase, basic_auth
from testcontainers.core.container import DockerContainer

ROOT_PASSWORD = "playwithdata"
TLS_STORE_PASSWORD = "changeit"

# Shared by all three container fixtures below (bolt_container,
# bolt_container_tls_required, bolt_container_tls_optional) - the TLS variants
# append their own arcadedb.bolt.ssl/arcadedb.ssl.* sysprops to this.
BASE_JAVA_OPTS = (
    "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD + " "
    "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
    "-Darcadedb.server.plugins=BoltProtocolPlugin"
)

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
    """Generate a throwaway self-signed keystore/truststore pair for BOLT TLS fixtures.

    Uses the JDK `keytool` binary. ArcadeDB does not ship a default TLS
    keystore (arcadedb.ssl.keyStore/.trustStore have no defaults - see
    BoltSslHelper.getRequiredProperty), so enabling arcadedb.bolt.ssl=REQUIRED/OPTIONAL
    without one crashes server startup with a ConfigurationException. This mirrors
    exactly what an operator would set up: a self-signed cert, trusted by the driver
    via bolt+ssc:// instead of a CA-signed one.
    """
    keytool = shutil.which("keytool")
    if keytool is None:
        raise FileNotFoundError(
            "keytool not found on PATH - a JDK is required to run the TLS scenarios "
            "(CONN-002/CONN-005); ensure a Java runtime is available in this environment"
        )

    keystore_path = cert_dir / "keystore.p12"
    truststore_path = cert_dir / "truststore.jks"
    cert_path = cert_dir / "bolt.cer"

    subprocess.run(
        [
            keytool,
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
            keytool,
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
            keytool,
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


def build_tls_image(cert_dir):
    """Build a throwaway image with the TLS keystore/truststore baked in via COPY.

    Bind-mounting the host cert_dir (testcontainers `with_volume_mapping`)
    works locally but is unreliable in some CI environments - GitHub Actions
    runners have been observed to start the container with the mounted
    directory present but empty, so ArcadeDB's BoltSslHelper fails with
    "Could not load resource from path" even though the same fixture passes
    locally. A `docker build` COPY always resolves its own build context
    correctly regardless of host/CI path-translation quirks, so this bakes
    the certs into a derived image instead of mounting them at runtime.
    """
    dockerfile = cert_dir / "Dockerfile"
    dockerfile.write_text(
        "FROM arcadedata/arcadedb:latest\n"
        "COPY --chown=arcadedb:arcadedb keystore.p12 truststore.jks /home/arcadedb/tls_certs/\n"
    )
    client = docker.from_env()
    image, _logs = client.images.build(path=str(cert_dir), tag="arcadedb-bolt-tls-test:latest", rm=True)
    return image.tags[0]


@pytest.fixture(scope="module")
def bolt_container():
    # This tag is loaded from the CI-built branch image (build-and-package's
    # `mvn -Pdocker` output via docker save/load in mvn-test.yml), not pulled
    # from Docker Hub - `docker load` populates the local daemon under this
    # same tag, so testcontainers uses it without a network pull. Running
    # this suite after an explicit `docker pull arcadedata/arcadedb:latest`
    # (overwriting the local tag with the published Hub image) will behave
    # differently, since that image may lag behind this branch's fixes.
    container = (
        DockerContainer("arcadedata/arcadedb:latest")
        .with_exposed_ports(2480, 7687)
        .with_env("JAVA_OPTS", BASE_JAVA_OPTS)
    )
    container.start()
    try:
        wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 60)
        create_database(container, "boltscratch")
        seed_type_matrix(container)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def bolt_driver(bolt_container):
    # Several tests below (TX-002/004, CAUSAL-001, MDB-002, RESULT-004) write
    # into this module-scoped "beer" database and rely on unique marker values
    # rather than isolated databases to avoid cross-test collisions - no test
    # currently asserts an exact row/node count, so this is intentional, not
    # an oversight. A count-based assertion added later should target the
    # dedicated "boltscratch" database instead.
    driver = GraphDatabase.driver(bolt_uri(bolt_container), auth=basic_auth("root", ROOT_PASSWORD))
    yield driver
    driver.close()


# --- connection ---------------------------------------------------------


def test_CONN_001_connect_bolt(bolt_driver):
    bolt_driver.verify_connectivity()


@pytest.fixture(scope="module")
def tls_certs(tmp_path_factory):
    cert_dir = tmp_path_factory.mktemp("bolt-tls-certs")
    generate_tls_certs(cert_dir)
    image_tag = build_tls_image(cert_dir)
    yield image_tag
    # Both bolt_container_tls_required/optional depend on this fixture, so
    # pytest tears them (and their containers) down before this runs.
    docker.from_env().images.remove(image_tag, force=True)


@pytest.fixture(scope="module")
def bolt_container_tls_required(tls_certs):
    container = (
        DockerContainer(tls_certs)
        .with_exposed_ports(2480, 7687)
        .with_env(
            "JAVA_OPTS",
            BASE_JAVA_OPTS + " "
            "-Darcadedb.bolt.ssl=REQUIRED "
            "-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/keystore.p12 "
            "-Darcadedb.ssl.keyStorePassword=" + TLS_STORE_PASSWORD + " "
            "-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/truststore.jks "
            "-Darcadedb.ssl.trustStorePassword=" + TLS_STORE_PASSWORD,
        )
    )
    container.start()
    try:
        # TLS keystore/truststore loading adds startup latency beyond the default
        # container's margin, especially on shared CI runners - give it more room.
        wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 120)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def bolt_container_tls_optional(tls_certs):
    container = (
        DockerContainer(tls_certs)
        .with_exposed_ports(2480, 7687)
        .with_env(
            "JAVA_OPTS",
            BASE_JAVA_OPTS + " "
            "-Darcadedb.bolt.ssl=OPTIONAL "
            "-Darcadedb.ssl.keyStore=/home/arcadedb/tls_certs/keystore.p12 "
            "-Darcadedb.ssl.keyStorePassword=" + TLS_STORE_PASSWORD + " "
            "-Darcadedb.ssl.trustStore=/home/arcadedb/tls_certs/truststore.jks "
            "-Darcadedb.ssl.trustStorePassword=" + TLS_STORE_PASSWORD,
        )
    )
    container.start()
    try:
        # See bolt_container_tls_required's comment: TLS startup needs extra margin.
        wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 120)
        yield container
    finally:
        container.stop()


def test_CONN_002_tls_required(bolt_container_tls_required):
    with GraphDatabase.driver(
        bolt_uri(bolt_container_tls_required, scheme="bolt+ssc"),
        auth=basic_auth("root", ROOT_PASSWORD),
    ) as driver:
        driver.verify_connectivity()
        with driver.session(database="beer") as session:
            result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
            assert result.single()["name"] is not None


@pytest.mark.skipif(
    platform.system() != "Linux",
    reason="neo4j:// routing makes the driver connect to the container IP "
    "handleRoute advertises; that address is host-routable on Linux CI's "
    "native Docker bridge but not from the host on Docker Desktop "
    "(macOS/Windows), where it times out - bolt:// scenarios are unaffected",
)
def test_CONN_003_neo4j_routing_single_node(bolt_container):
    with GraphDatabase.driver(bolt_uri(bolt_container, scheme="neo4j"), auth=basic_auth("root", ROOT_PASSWORD)) as driver:
        driver.verify_connectivity()
        with driver.session(database="beer") as session:
            result = session.run("MATCH (b:Beer) RETURN b.name AS name LIMIT 1")
            assert result.single()["name"] is not None


@pytest.mark.skip(
    reason="Requires a 3-node HA cluster; e2e-python's single-node harness "
    "cannot meaningfully exercise this scenario without new multi-node "
    "orchestration infrastructure - see #4890"
)
def test_CONN_004_neo4j_routing_ha_topology():
    pass


def test_CONN_005_tls_optional_plaintext_connects(bolt_container_tls_optional):
    with GraphDatabase.driver(
        bolt_uri(bolt_container_tls_optional, scheme="bolt"),
        auth=basic_auth("root", ROOT_PASSWORD),
    ) as driver:
        driver.verify_connectivity()


# --- auth ----------------------------------------------------------------


def test_AUTH_001_basic_auth_valid(bolt_container):
    with GraphDatabase.driver(bolt_uri(bolt_container), auth=basic_auth("root", ROOT_PASSWORD)) as driver:
        driver.verify_connectivity()
        with driver.session(database="beer") as session:
            assert session.run("RETURN 1 AS value").single()["value"] == 1


def test_AUTH_002_basic_auth_invalid(bolt_container):
    from neo4j.exceptions import AuthError

    with GraphDatabase.driver(bolt_uri(bolt_container), auth=basic_auth("root", "wrong-password")) as driver:
        with pytest.raises(AuthError) as exc_info:
            driver.verify_connectivity()
        assert exc_info.value.code == "Neo.ClientError.Security.Unauthorized"


def test_AUTH_003_auth_none_rejected(bolt_container):
    from neo4j.exceptions import AuthError, ServiceUnavailable

    with GraphDatabase.driver(bolt_uri(bolt_container), auth=None) as driver:
        with pytest.raises((AuthError, ServiceUnavailable)):
            driver.verify_connectivity()


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
    """Shared concurrency helper for TX-005 and ERR-004.

    Two sessions race to update the same node inside an explicit
    transaction, one held open past the other's commit attempt. Returns the
    list of exceptions raised.
    """
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

    # Diagnostic only (printed unconditionally, not just on failure, so it
    # shows up in captured-stdout even for an unexpected XPASS): this race is
    # inherently timing-sensitive, and an isolated flip to a genuine
    # Neo.TransientError.* would be a real, actionable server-behavior
    # change worth having concrete evidence for rather than a bare XPASS.
    for i, err in enumerate(errors):
        code = getattr(err, "code", None)
        print(f"_race_two_writers[{marker}] errors[{i}]: {type(err).__name__} code={code!r} msg={err}")

    return errors


def test_TX_005_managed_write_retries_on_transient_error(bolt_driver):
    from neo4j.exceptions import ClientError, DatabaseError, TransientError

    errors = _race_two_writers(bolt_driver, "beer", "tx-005")

    assert errors, "expected at least one racing session to fail on the write conflict"
    # Check the whole collection, not just errors[0] - which of the two racing
    # threads appends first is nondeterministic, and this matches the scenario's
    # actual intent: the conflict must be retryable, not merely present.
    assert any(isinstance(e, TransientError) for e in errors), (
        f"expected at least one Neo.TransientError.*, got {[type(e).__name__ for e in errors]}"
    )
    assert not any(isinstance(e, (ClientError, DatabaseError)) for e in errors), (
        f"expected no non-retryable ClientError/DatabaseError among the racing "
        f"errors, got {[type(e).__name__ for e in errors]}"
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


def test_TYPE_007_local_date_roundtrip(bolt_driver):
    from neo4j.time import Date

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.localDateProp AS d").single()
        assert isinstance(record["d"], Date)

        echo = session.run("RETURN $d AS echo", d=record["d"]).single()
        assert echo["echo"] == record["d"]


def test_TYPE_008_local_time_roundtrip(bolt_driver):
    from neo4j.time import Time

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.localTimeProp AS t2").single()
        assert isinstance(record["t2"], Time)

        echo = session.run("RETURN $t AS echo", t=record["t2"]).single()
        assert echo["echo"] == record["t2"]


def test_TYPE_009_local_datetime_roundtrip(bolt_driver):
    from neo4j.time import DateTime

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.localDateTimeProp AS dt").single()
        assert isinstance(record["dt"], DateTime)

        echo = session.run("RETURN $dt AS echo", dt=record["dt"]).single()
        assert echo["echo"] == record["dt"]


def test_TYPE_010_offset_datetime_roundtrip(bolt_driver):
    import datetime as dt_module

    from neo4j.time import DateTime

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.offsetDateTimeProp AS dt").single()
        dt = record["dt"]
        assert isinstance(dt, DateTime)
        assert dt.utc_offset() == dt_module.timedelta(hours=2)

        echo = session.run("RETURN $dt AS echo", dt=dt).single()
        assert echo["echo"] == dt


def test_TYPE_011_duration_roundtrip(bolt_driver):
    from neo4j.time import Duration

    with bolt_driver.session(database="beer") as session:
        record = session.run("MATCH (t:TypeMatrix) RETURN t.durationProp AS d").single()
        assert isinstance(record["d"], Duration)

        echo = session.run("RETURN $d AS echo", d=record["d"]).single()
        assert echo["echo"] == record["d"]


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


def test_ERR_004_transient_condition_error_code(bolt_driver):
    from neo4j.exceptions import ClientError, DatabaseError, TransientError

    errors = _race_two_writers(bolt_driver, "beer", "err-004")

    assert errors, "expected at least one racing session to fail on the write conflict"
    # Check the whole collection, not just errors[0] - which of the two racing
    # threads appends first is nondeterministic, and this matches the scenario's
    # actual intent: the conflict must be retryable, not merely present.
    assert any(isinstance(e, TransientError) for e in errors), (
        f"expected at least one Neo.TransientError.*, got {[type(e).__name__ for e in errors]}"
    )
    assert not any(isinstance(e, (ClientError, DatabaseError)) for e in errors), (
        f"expected no non-retryable ClientError/DatabaseError among the racing "
        f"errors, got {[type(e).__name__ for e in errors]}"
    )


# --- protocol ---------------------------------------------------------


def test_PROTO_001_version_negotiation_succeeds(bolt_driver):
    with bolt_driver.session(database="beer") as session:
        assert session.run("RETURN 1 AS value").single()["value"] == 1


def test_PROTO_002_bolt_5x_negotiation_is_supported(bolt_driver):
    # Since #5001 the server advertises Bolt 5.0-5.4, so a 5.x-capable driver
    # negotiates a 5.x protocol version instead of silently downgrading to 4.4.
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
