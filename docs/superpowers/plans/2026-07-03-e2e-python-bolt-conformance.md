# e2e-python Bolt Conformance Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement all 39 scenarios from `bolt/conformance/spec.yaml` (issue #4883) as pytest tests in a new `e2e-python/tests/test_bolt.py`, certifying the `neo4j` Python driver against ArcadeDB's Bolt protocol implementation, wired into the existing `python-e2e-tests` CI job (issue #4885).

**Architecture:** A single new test file with three module-scoped Testcontainers fixtures (one default TLS-disabled container hosting 37 scenarios, two dedicated TLS-mode containers for the 2 TLS scenarios), mirroring the existing `test_arcadedb.py` container pattern but adding port `7687` and `BoltProtocolPlugin`. Every scenario becomes one test function named `test_<AREA>_<NNN>_<slug>`. The 10 genuinely-known-gap scenarios use `pytest.mark.xfail(strict=True)`; one scenario (CONN-004, needs a 3-node HA cluster) uses `pytest.mark.skip`; the rest are ordinary assertions.

**Tech Stack:** Python 3.13 (CI) / >=3.10 (declared floor), pytest, `neo4j` (official Python driver) >=6.2.0, `testcontainers`, `requests` (all already available or newly added to `e2e-python/pyproject.toml`).

## Global Constraints

- Design spec: `docs/superpowers/specs/2026-07-03-e2e-python-bolt-conformance-design.md` - read it before starting; every task below implements one of its decisions.
- Source of truth for scenario content: `bolt/conformance/spec.yaml` (39 scenarios) - every `known_limitation` string quoted in this plan is copied verbatim from that file for `xfail` reason consistency.
- No protocol/server code changes anywhere in this plan - test-only.
- No `conftest.py` - all fixtures live inline in `test_bolt.py`, matching every other `e2e-python` test file.
- Auth for all containers: user `root`, password `playwithdata` (matches every existing e2e suite in this repo).
- Do not commit until each task's tests are confirmed to run correctly against a real Docker container (Docker must be available locally to execute this plan).
- Do not add Claude as a commit author.
- Remove any stray debug `print()` statements before each commit.

---

### Task 1: Add the `neo4j` driver dependency

**Files:**
- Modify: `e2e-python/pyproject.toml`

**Interfaces:**
- Produces: `neo4j` package importable in `e2e-python`'s environment for all later tasks.

- [ ] **Step 1: Edit `pyproject.toml`**

Change:
```toml
requires-python = ">=3.8"
```
to:
```toml
requires-python = ">=3.10"
```

(The `neo4j` 6.x driver line requires Python >=3.10; CI's `python-e2e-tests` job already pins Python `3.13.0`, so this is a metadata-accuracy fix with no practical CI impact.)

Add `"neo4j>=6.2.0"` to the `dependencies` list, keeping the existing `>=`-floor style and alphabetical-ish grouping used by the other entries:

```toml
dependencies = [
    "testcontainers>=4.14.2",
    "pytest>=9.1.1",
    "pytest-check>=2.8.0",
    "pytest-asyncio>=1.4.0",
    "docker>=7.1.0",
    "psycopg>=3.3.4",
    "psycopg-binary>=3.3.4",
    "asyncpg>=0.31.0",
    "requests>=2.34.2",
    "sqlalchemy>=2.0.51",
    "neo4j>=6.2.0"
]
```

- [ ] **Step 2: Install and verify the import**

Run (from `e2e-python/`):
```bash
cd e2e-python
pip install -e .
python3 -c "import neo4j; print(neo4j.__version__)"
```
Expected: prints a version string `>= 6.2.0`, no `ModuleNotFoundError`.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/pyproject.toml
git commit -m "build(#4885): add neo4j driver dependency to e2e-python"
```

---

### Task 2: Base Bolt container fixture + connectivity smoke test (CONN-001)

**Files:**
- Create: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `neo4j` package (Task 1).
- Produces (used by every later task in this plan):
  - `ROOT_PASSWORD` constant
  - `wait_for_http_endpoint(container, path, port, expected_status, timeout=60)` function
  - `bolt_uri(container, scheme="bolt")` function
  - `create_database(container, db_name)` function
  - `seed_type_matrix(container)` function
  - `bolt_container` module-scoped fixture (yields a started `DockerContainer`, `beer` + `boltscratch` databases present, `type-matrix.cypher` seeded)
  - `bolt_driver` module-scoped fixture (yields a `neo4j.Driver` connected to `bolt_container`)

- [ ] **Step 1: Write the file with imports, helpers, the base fixture, and the first test**

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v
```
Expected: `test_CONN_001_connect_bolt PASSED` (requires Docker running locally; container pull + boot may take ~30-60s).

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add Bolt container fixture and CONN-001 connectivity test"
```

---

### Task 3: Remaining connection scenarios (CONN-002..005) + TLS fixtures

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `ROOT_PASSWORD`, `wait_for_http_endpoint`, `bolt_uri`, `bolt_container`, `bolt_driver` (Task 2).
- Produces: `bolt_container_tls_required`, `bolt_container_tls_optional` fixtures (used only within this task).

- [ ] **Step 1: Append the TLS fixtures and CONN-002/003/004/005 tests**

Append to `e2e-python/tests/test_bolt.py`, directly after `test_CONN_001_connect_bolt`:

```python
@pytest.fixture(scope="module")
def bolt_container_tls_required():
    container = (
        DockerContainer("arcadedata/arcadedb:latest")
        .with_exposed_ports(2480, 7687)
        .with_env(
            "JAVA_OPTS",
            "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD + " "
            "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
            "-Darcadedb.server.plugins=BoltProtocolPlugin "
            "-Darcadedb.bolt.ssl=REQUIRED",
        )
    )
    container.start()
    wait_for_http_endpoint(container, "/api/v1/ready", 2480, 204, 60)
    yield container
    container.stop()


@pytest.fixture(scope="module")
def bolt_container_tls_optional():
    container = (
        DockerContainer("arcadedata/arcadedb:latest")
        .with_exposed_ports(2480, 7687)
        .with_env(
            "JAVA_OPTS",
            "-Darcadedb.server.rootPassword=" + ROOT_PASSWORD + " "
            "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
            "-Darcadedb.server.plugins=BoltProtocolPlugin "
            "-Darcadedb.bolt.ssl=OPTIONAL",
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "CONN"
```
Expected: `test_CONN_001_connect_bolt PASSED`, `test_CONN_002_tls_required PASSED`, `test_CONN_003_neo4j_routing_single_node PASSED`, `test_CONN_004_neo4j_routing_ha_topology SKIPPED`, `test_CONN_005_tls_optional_plaintext_connects PASSED`.

If `test_CONN_002_tls_required` fails to connect (certificate/handshake error), try scheme `bolt+s` instead of `bolt+ssc` and re-run - the difference is whether the driver trusts a self-signed cert (`+ssc`) or requires system-trusted CA (`+s`); ArcadeDB's default TLS keystore is self-signed, so `+ssc` is expected to be correct, but confirm empirically against the real container.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add remaining connection scenarios (CONN-002..005)"
```

---

### Task 4: Auth scenarios (AUTH-001..003)

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `ROOT_PASSWORD`, `bolt_uri`, `bolt_container` (Task 2).

- [ ] **Step 1: Append the auth tests**

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "AUTH"
```
Expected: all 3 `PASSED`. If `test_AUTH_002_basic_auth_invalid` fails because the raised exception isn't `AuthError` or the `.code` differs, inspect the actual exception type/code printed in the failure and adjust the `pytest.raises` type/assertion to match reality - the spec's `known_limitation` fields don't cover AUTH scenarios (they're all `current_status: passing`), so any mismatch here is a real finding, not an expected gap.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add auth scenarios (AUTH-001..003)"
```

---

### Task 5: Transaction scenarios (TX-001..005)

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `ROOT_PASSWORD`, `bolt_driver` (Task 2).

- [ ] **Step 1: Append the transaction tests**

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "TX"
```
Expected: `test_TX_001..004 PASSED`, `test_TX_005_managed_write_retries_on_transient_error XFAIL`. If TX-001..004 fail unexpectedly, inspect and fix the test against real observed behavior (these are `current_status: passing` in spec.yaml). If TX-005 unexpectedly **passes** (no error raised at all, meaning the race didn't produce contention), increase contention by tightening the `barrier.wait`/`time.sleep` timing or by having both threads target the exact same `tx.run` line without a gap; if the underlying error *is* raised but as some other exception type, capture the real type and update the assertion - but keep `xfail(strict=True)` since the root problem (no `TransientError` code path) still holds either way, and flag this in the task's commit message if you had to adjust the concurrency mechanics.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add transaction scenarios (TX-001..005)"
```

---

### Task 6: Causal-consistency and multi-database scenarios (CAUSAL-001, MDB-001..002)

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `bolt_driver` (Task 2), `boltscratch` database (created by `bolt_container` fixture in Task 2).

- [ ] **Step 1: Append the causal-consistency and multi-database tests**

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "CAUSAL or MDB"
```
Expected: all 3 `PASSED`. Both `CAUSAL-001` and `MDB-002` are `current_status: unverified` in spec.yaml - if either fails, this is new information: inspect the failure, and if it looks like a genuine gap (not a test bug), change the decorator to `@pytest.mark.xfail(strict=True, reason="<describe the observed failure>; see #4890")` and update the corresponding scenario's `current_status`/`known_limitation`/`tracking_issue` in `bolt/conformance/spec.yaml` in this same task's commit (reuse `#4890` if the failure matches one of the 4 confirmed gap categories, otherwise flag it explicitly in the commit message for the user to decide whether a new tracking issue is warranted - do not open one automatically).

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add causal-consistency and multi-database scenarios"
```

---

### Task 7: Result-handling scenarios (RESULT-001..004)

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `bolt_driver` (Task 2).

- [ ] **Step 1: Append the result-handling tests**

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "RESULT"
```
Expected: all 4 `PASSED`. `RESULT-002` and `RESULT-004` are `current_status: unverified` - same instruction as Task 6 applies if either fails: investigate, and if it's a real gap, convert to `xfail(strict=True)` with a reason and update `spec.yaml` in this task's commit.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add result-handling scenarios (RESULT-001..004)"
```

---

### Task 8: Type-roundtrip scenarios (TYPE-001..012)

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `bolt_driver` (Task 2), `type_matrix` fixture data seeded into `beer` (Task 2's `seed_type_matrix`).

- [ ] **Step 1: Append the type-roundtrip tests**

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "TYPE"
```
Expected: `TYPE-001, 002, 004, 005, 006 PASSED`; `TYPE-003, 007, 008, 009, 010, 011, 012 XFAIL`. If any of the `expected-fail` scenarios unexpectedly PASS (meaning the gap has already been fixed on `main` since the spec was authored), `xfail(strict=True)` will report it as a hard failure (XPASS) - do not weaken the assertion to force it back to xfail; instead remove the `xfail` decorator for that scenario and update `bolt/conformance/spec.yaml`'s `current_status` for it to `passing` (drop `known_limitation`/`tracking_issue`) in this task's commit, since that would mean issue #4890 has already been partially resolved.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add type-roundtrip scenarios (TYPE-001..012)"
```

---

### Task 9: Error scenarios (ERR-001..004)

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`
- Modify: `bolt/conformance/spec.yaml` (ERR-003 status correction, see Step 1 note)

**Interfaces:**
- Consumes: `bolt_driver` (Task 2), `_race_two_writers` (Task 5).

- [ ] **Step 1: Append the error tests**

**Important finding to apply before writing this step:** ERR-003 ("Unauthenticated request returns Neo.ClientError.Security.Forbidden") requires sending `RUN` before completing `HELLO`/`LOGON`. The official `neo4j` Python driver's public API always completes the Bolt handshake and `HELLO` internally before any session/query method becomes usable - there is no way to reach "RUN before auth" through the driver's public API without a bespoke raw-socket client, which is out of scope per the epic's "test the real official drivers only" governing principle. This is a genuine testability finding, not a code gap: `bolt/conformance/spec.yaml` currently marks `ERR-003` as `current_status: passing`, but for a driver-based suite it should be `not-applicable` (a valid `current_status` value per `bolt/conformance/README.md`'s "Scenario fields" table and `validate_spec.py`'s `VALID_STATUSES`, requiring no `known_limitation`/`tracking_issue`). Update it as part of this task:

In `bolt/conformance/spec.yaml`, find the `ERR-003` entry and change:
```yaml
    applicable_driver_versions: all
    current_status: passing
```
to:
```yaml
    applicable_driver_versions: all
    current_status: not-applicable
    known_limitation: >
      Cannot be triggered through any official driver's public API - every
      official Neo4j driver completes HELLO/LOGON internally before exposing
      session/query methods to user code. Testable only via a bespoke
      raw-socket client, which the epic's governing principles exclude.
```

Run `python3 bolt/conformance/validate_spec.py bolt/conformance/spec.yaml` from the repo root after this edit to confirm it still validates (see Task 10 for the full validator invocation).

Now append the error tests to `test_bolt.py`:

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "ERR"
```
Expected: `ERR-001, 002 PASSED`, `ERR-003 SKIPPED`, `ERR-004 XFAIL`.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py bolt/conformance/spec.yaml
git commit -m "test(#4885): add error scenarios (ERR-001..004), correct ERR-003 spec status"
```

---

### Task 10: Protocol scenarios (PROTO-001..003)

**Files:**
- Modify: `e2e-python/tests/test_bolt.py`

**Interfaces:**
- Consumes: `bolt_driver` (Task 2).

- [ ] **Step 1: Append the protocol tests**

```python
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
```

- [ ] **Step 2: Run it to verify it passes**

```bash
cd e2e-python
pytest tests/test_bolt.py -v -k "PROTO"
```
Expected: `PROTO-001 PASSED`, `PROTO-002 XFAIL`, `PROTO-003 PASSED` (`PROTO-003` is `current_status: unverified` - same instruction as Task 6 if it fails).

If `summary.server.protocol_version` doesn't exist on the installed `neo4j` driver version (API surface can shift between minor versions), check `neo4j.api.ServerInfo`'s actual attributes via `python3 -c "from neo4j.api import ServerInfo; help(ServerInfo)"` and adjust `test_PROTO_002_bolt_5x_negotiation_is_documented` to use the correct attribute name - the assertion's intent (negotiated version's major number is 5 or higher) must be preserved.

- [ ] **Step 3: Commit**

```bash
git add e2e-python/tests/test_bolt.py
git commit -m "test(#4885): add protocol scenarios (PROTO-001..003)"
```

---

### Task 11: Full-suite verification and scenario coverage cross-check

**Files:**
- No new files - verification only, plus a possible final fix-up commit.

**Interfaces:**
- Consumes: everything from Tasks 1-10.

- [ ] **Step 1: Run the full Bolt suite verbosely**

```bash
cd e2e-python
pytest tests/test_bolt.py -v
```
Expected: 39 scenarios accounted for -
- `PASSED`: the scenarios that were `current_status: passing` and any `unverified` ones confirmed correct (should be roughly 22-28 depending on how many `unverified` scenarios resolved as passing).
- `XFAIL`: the 10 `expected-fail` scenarios other than CONN-004, plus any `unverified` scenario that was promoted to `xfail` during Tasks 6/7/10.
- `SKIPPED`: exactly 2 - `CONN-004` and `ERR-003`, each with a clear reason string visible in the `-v` output.
- No unexplained `FAILED` or `SKIPPED` without a `reason=`.

If anything is unaccounted for, go back to the relevant task and fix it before proceeding.

- [ ] **Step 2: Cross-check every spec.yaml scenario has exactly one test**

```bash
cd /Users/frank/projects/arcade/arcadedb/.worktrees/feat/4885-e2e-python-bolt-conformance
python3 - <<'EOF'
import re
import yaml

spec = yaml.safe_load(open("bolt/conformance/spec.yaml"))
spec_ids = {s["id"] for s in spec["scenarios"]}

test_source = open("e2e-python/tests/test_bolt.py").read()
test_ids = set()
for match in re.finditer(r"def test_([A-Z]+)_(\d+)_", test_source):
    area_prefix, number = match.groups()
    test_ids.add(f"{area_prefix}-{number}")

missing = spec_ids - test_ids
extra = test_ids - spec_ids
print(f"spec scenarios: {len(spec_ids)}, test functions: {len(test_ids)}")
print(f"missing (in spec, no test): {sorted(missing) or 'none'}")
print(f"extra (test with no spec id): {sorted(extra) or 'none'}")
assert not missing, f"missing tests for: {sorted(missing)}"
EOF
```
Expected: `spec scenarios: 39, test functions: 39`, `missing (in spec, no test): none`, `extra (test with no spec id): none`. (Requires `pyyaml` - already a dependency of `bolt/conformance/validate_spec.py`; install with `pip install pyyaml` if not present in the active environment.)

If `missing` is non-empty, add the missing test function(s) before proceeding - every scenario in `spec.yaml` must have a corresponding test per issue #4885's acceptance criteria ("`test_bolt.py` implements every A1 scenario").

- [ ] **Step 3: Validate the spec.yaml edit from Task 9 still passes structural validation**

```bash
cd bolt/conformance
pip install -r requirements.txt
python3 validate_spec.py spec.yaml
python3 -m unittest test_validate_spec.py -v
```
Expected: both commands exit 0 with no errors reported.

- [ ] **Step 4: Run the full existing e2e-python suite to confirm no regression**

```bash
cd e2e-python
pytest tests/ -v
```
Expected: all pre-existing Postgres-wire tests (`test_arcadedb.py`, `test_asyncpg.py`, `test_sqlalchemy.py`) still pass, plus all of `test_bolt.py`'s PASSED/XFAIL/SKIPPED results from Step 1.

- [ ] **Step 5: Final commit (if Steps 1-4 required any fix-ups)**

```bash
git add -A
git status   # confirm only expected files are staged
git commit -m "test(#4885): fix up scenario coverage gaps found during verification"
```

Skip this step entirely if Steps 1-4 required no changes.
