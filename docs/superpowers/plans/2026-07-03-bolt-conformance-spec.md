# Bolt Conformance Spec (issue #4883) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Author `bolt/conformance/spec.yaml` - the single, language-neutral scenario matrix that the five per-language Bolt e2e suites (#4885-#4889) will be hand-implemented against - plus its supporting fixture and README, per issue #4883.

**Architecture:** A structured YAML spec (source of truth, human-consumed only - no language parses it at runtime) with one entry per certification scenario, grouped into the epic's 9 feature-matrix areas. A small Python validator script enforces structural integrity (unique IDs, valid areas/status values, full area coverage, the 4 confirmed protocol gaps present as tracked `expected-fail` scenarios) and doubles as the "test suite" for this content-only deliverable. A local Cypher fixture (`type-matrix.cypher`) supplements the existing remote "beer" dataset for type-round-trip scenarios, seeded via HTTP/SQL rather than Bolt to avoid exercising the very serialization path under test.

**Tech Stack:** YAML (spec), Python 3 + PyYAML (validator + its own unit tests, dev-tooling only, not shipped product code), Cypher (fixture script), Markdown (README).

## Global Constraints

- No protocol code (Java) changes in this plan - this is a documentation/spec deliverable only. See design doc `docs/superpowers/specs/2026-07-03-bolt-conformance-spec-design.md`.
- `spec.yaml` is a structured YAML file, not Markdown tables and not Gherkin - decided in the design doc.
- No language's test suite parses `spec.yaml` at runtime; it is a reference document a human implements against by hand, using scenario IDs embedded in test names/comments for traceability.
- Driver versions are named bands (e.g. `latest-6.x`, `lts`), never hardcoded version numbers, in `spec.yaml`.
- The `type_matrix` fixture must be documented as seeded via HTTP/SQL, never via Bolt.
- The 9 valid `area` values are exactly: `connection`, `auth`, `transactions`, `causal-consistency`, `multi-database`, `result-handling`, `type-roundtrip`, `errors`, `protocol`.
- The 4 valid `current_status` values are exactly: `passing`, `expected-fail`, `not-applicable`, `unverified`. (`unverified` is a necessary addition beyond the design doc's original 3-value taxonomy - not every scenario in a ~40-scenario matrix can be manually confirmed during spec authoring without duplicating the B-tasks' own certification work; `unverified` honestly marks "status to be determined by whichever B-task implements this scenario" instead of guessing.)
- Every `expected-fail` scenario requires `known_limitation` and `tracking_issue` fields. Use `tracking_issue: "#4890"` only for the 4 epic-confirmed gaps (single-node-only ROUTE, missing `Neo.TransientError.*`, Bolt 5.x negotiation absent, type-fidelity gaps covering temporal-as-string/missing Duration/missing Point). Do not invent other `expected-fail` scenarios without direct grounding - use `unverified` instead when uncertain.
- Per this repo's root `CLAUDE.md` ("do not commit on git, I will do it after a review"), **no task in this plan runs `git commit`.** Stage changes with `git add` where noted; the user commits after reviewing the whole deliverable.
- New files only live under `bolt/conformance/`; do not modify any existing Bolt protocol source.

---

### Task 1: Spec validator script and its own unit tests

**Files:**
- Create: `bolt/conformance/validate_spec.py`
- Create: `bolt/conformance/test_validate_spec.py`

**Interfaces:**
- Produces: `validate(spec: dict) -> list[str]` (pure function, no file I/O - returns a list of human-readable error strings, empty list means valid) and a `main()` CLI entry point that loads a YAML file path (default `bolt/conformance/spec.yaml`) and exits non-zero with errors printed if invalid. Later tasks run `python3 bolt/conformance/validate_spec.py` after every content change.

- [ ] **Step 1: Write the failing tests**

Create `bolt/conformance/test_validate_spec.py`:

```python
#!/usr/bin/env python3
import unittest

from validate_spec import validate

AREAS = [
    "connection", "auth", "transactions", "causal-consistency",
    "multi-database", "result-handling", "type-roundtrip",
]
GAP_AREAS = ["errors", "protocol", "connection", "type-roundtrip"]


def minimal_valid_spec():
    scenarios = [
        {
            "id": f"{area.upper().replace('-', '')}-001",
            "area": area,
            "title": "t",
            "steps": ["s"],
            "applicable_driver_versions": "all",
            "current_status": "passing",
        }
        for area in AREAS
    ]
    for area in GAP_AREAS:
        scenarios.append(
            {
                "id": f"{area.upper().replace('-', '')}-900",
                "area": area,
                "title": "t",
                "steps": ["s"],
                "applicable_driver_versions": "all",
                "current_status": "expected-fail",
                "known_limitation": "x",
                "tracking_issue": "#4890",
            }
        )
    return {
        "version": 1,
        "driver_version_bands": {
            lang: {"band_names": ["a"], "driver_artifact": "x"}
            for lang in ("java", "javascript", "python", "csharp", "go")
        },
        "fixtures": {"beer": {}, "type_matrix": {}},
        "scenarios": scenarios,
    }


class ValidateSpecTest(unittest.TestCase):
    def test_minimal_valid_spec_passes(self):
        self.assertEqual(validate(minimal_valid_spec()), [])

    def test_missing_area_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"] = [s for s in spec["scenarios"] if s["area"] != "auth"]
        errors = validate(spec)
        self.assertTrue(any("auth" in e for e in errors), errors)

    def test_duplicate_id_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"].append(dict(spec["scenarios"][0]))
        errors = validate(spec)
        self.assertTrue(any("duplicate scenario id" in e for e in errors), errors)

    def test_invalid_status_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["current_status"] = "kinda-works"
        errors = validate(spec)
        self.assertTrue(any("current_status" in e for e in errors), errors)

    def test_expected_fail_without_known_limitation_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["current_status"] = "expected-fail"
        errors = validate(spec)
        self.assertTrue(any("known_limitation" in e for e in errors), errors)

    def test_expected_fail_without_tracking_issue_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"][0]["current_status"] = "expected-fail"
        spec["scenarios"][0]["known_limitation"] = "x"
        errors = validate(spec)
        self.assertTrue(any("tracking_issue" in e for e in errors), errors)

    def test_missing_gap_tracking_area_is_flagged(self):
        spec = minimal_valid_spec()
        spec["scenarios"] = [s for s in spec["scenarios"] if s["id"] != "TYPEROUNDTRIP-900"]
        errors = validate(spec)
        self.assertTrue(
            any("type-roundtrip" in e for e in errors), errors
        )

    def test_missing_driver_language_is_flagged(self):
        spec = minimal_valid_spec()
        del spec["driver_version_bands"]["go"]
        errors = validate(spec)
        self.assertTrue(any("go" in e for e in errors), errors)

    def test_missing_fixture_is_flagged(self):
        spec = minimal_valid_spec()
        del spec["fixtures"]["type_matrix"]
        errors = validate(spec)
        self.assertTrue(any("type_matrix" in e for e in errors), errors)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd bolt/conformance 2>/dev/null || mkdir -p bolt/conformance; cd bolt/conformance && python3 -m unittest test_validate_spec.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'validate_spec'` (the module doesn't exist yet).

- [ ] **Step 3: Write the validator implementation**

Create `bolt/conformance/validate_spec.py`:

```python
#!/usr/bin/env python3
"""Validates bolt/conformance/spec.yaml structural integrity.

This is a reference/authoring aid only - no per-language Bolt e2e suite
imports or executes this script. See README.md for the consumption model.
"""
import sys

import yaml

REQUIRED_AREAS = {
    "connection",
    "auth",
    "transactions",
    "causal-consistency",
    "multi-database",
    "result-handling",
    "type-roundtrip",
    "errors",
    "protocol",
}
VALID_STATUSES = {"passing", "expected-fail", "not-applicable", "unverified"}
REQUIRED_LANGUAGES = {"java", "javascript", "python", "csharp", "go"}
KNOWN_GAP_TRACKING_ISSUE = "#4890"
# Areas that must each carry at least one #4890-tracked expected-fail scenario:
# connection (single-node-only ROUTE), errors (missing Neo.TransientError.*),
# protocol (no Bolt 5.x negotiation), type-roundtrip (temporal-as-string,
# missing Duration, missing Point).
REQUIRED_GAP_AREAS = {"connection", "errors", "protocol", "type-roundtrip"}


def validate(spec: dict) -> list[str]:
    errors = []

    if spec.get("version") != 1:
        errors.append("spec.version must be 1")

    bands = spec.get("driver_version_bands", {})
    missing_langs = REQUIRED_LANGUAGES - bands.keys()
    if missing_langs:
        errors.append(f"driver_version_bands missing languages: {sorted(missing_langs)}")
    for lang, band in bands.items():
        if not band.get("band_names"):
            errors.append(f"driver_version_bands.{lang} missing band_names")
        if not band.get("driver_artifact"):
            errors.append(f"driver_version_bands.{lang} missing driver_artifact")

    fixtures = spec.get("fixtures", {})
    for name in ("beer", "type_matrix"):
        if name not in fixtures:
            errors.append(f"fixtures.{name} is missing")

    scenarios = spec.get("scenarios", [])
    if not scenarios:
        errors.append("scenarios list is empty")

    seen_ids = set()
    areas_seen = set()
    gap_tracking_areas = set()

    for s in scenarios:
        sid = s.get("id")
        if not sid:
            errors.append(f"scenario missing id: {s}")
            continue
        if sid in seen_ids:
            errors.append(f"duplicate scenario id: {sid}")
        seen_ids.add(sid)

        area = s.get("area")
        if area not in REQUIRED_AREAS:
            errors.append(f"{sid}: invalid area '{area}'")
        else:
            areas_seen.add(area)

        status = s.get("current_status")
        if status not in VALID_STATUSES:
            errors.append(f"{sid}: invalid current_status '{status}'")

        if status == "expected-fail":
            if not s.get("known_limitation"):
                errors.append(f"{sid}: expected-fail requires known_limitation")
            if not s.get("tracking_issue"):
                errors.append(f"{sid}: expected-fail requires tracking_issue")
            elif s["tracking_issue"] == KNOWN_GAP_TRACKING_ISSUE:
                gap_tracking_areas.add(area)

        if not s.get("title"):
            errors.append(f"{sid}: missing title")
        if not s.get("steps"):
            errors.append(f"{sid}: missing steps")
        if "applicable_driver_versions" not in s:
            errors.append(f"{sid}: missing applicable_driver_versions")

    missing_area_coverage = REQUIRED_AREAS - areas_seen
    if missing_area_coverage:
        errors.append(f"areas with zero scenarios: {sorted(missing_area_coverage)}")

    missing_gap_areas = REQUIRED_GAP_AREAS - gap_tracking_areas
    if missing_gap_areas:
        errors.append(
            f"areas missing a #4890-tracked expected-fail scenario: {sorted(missing_gap_areas)}"
        )

    return errors


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "spec.yaml"
    with open(path) as f:
        spec = yaml.safe_load(f) or {}
    errors = validate(spec)
    if errors:
        print(f"FAIL: {len(errors)} error(s)")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    areas = {s["area"] for s in spec.get("scenarios", [])}
    print(f"OK: {len(spec.get('scenarios', []))} scenarios across {len(areas)} areas")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd bolt/conformance && python3 -m unittest test_validate_spec.py -v`
Expected: all 9 tests PASS. (Requires PyYAML - already confirmed available locally as 6.0.3; if missing elsewhere, `pip install pyyaml`. This is a dev-tooling script, not a shipped product dependency, so it is not subject to the Maven/Apache-2.0 dependency policy in the root CLAUDE.md.)

---

### Task 2: Scaffold directory, minimal spec.yaml skeleton, README stub

**Files:**
- Create: `bolt/conformance/spec.yaml`
- Create: `bolt/conformance/README.md` (stub, filled in fully by Task 12)
- Create: `bolt/conformance/fixtures/` (empty dir, populated by Task 3)

**Interfaces:**
- Produces: `bolt/conformance/spec.yaml` with `version: 1`, empty `driver_version_bands: {}`, empty `fixtures: {}`, empty `scenarios: []` - the skeleton every later task adds to.

- [ ] **Step 1: Create the minimal skeleton**

Create `bolt/conformance/spec.yaml`:

```yaml
# Shared Bolt conformance spec (issue #4883, part of epic #4882).
# See README.md for how to consume this file - it is a reference for
# hand-writing per-language tests, not something any test suite parses
# at runtime.
version: 1

driver_version_bands: {}

fixtures: {}

scenarios: []
```

Create `bolt/conformance/README.md` (stub):

```markdown
# Bolt Conformance Spec

Full content added in Task 12 of the implementation plan.
```

- [ ] **Step 2: Run the validator and confirm the expected failures**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL, listing:
  - `driver_version_bands missing languages: ['csharp', 'go', 'java', 'javascript', 'python']`
  - `fixtures.beer is missing`
  - `fixtures.type_matrix is missing`
  - `scenarios list is empty`
  - `areas with zero scenarios: ['auth', 'causal-consistency', 'connection', 'errors', 'multi-database', 'protocol', 'result-handling', 'transactions', 'type-roundtrip']`
  - `areas missing a #4890-tracked expected-fail scenario: ['connection', 'errors', 'protocol', 'type-roundtrip']`

This confirms the validator correctly identifies the current gap - each subsequent task closes part of it.

---

### Task 3: Driver version bands and fixtures (including the type-matrix Cypher fixture)

**Files:**
- Modify: `bolt/conformance/spec.yaml`
- Create: `bolt/conformance/fixtures/type-matrix.cypher`

**Interfaces:**
- Consumes: skeleton `spec.yaml` from Task 2.
- Produces: `driver_version_bands` and `fixtures` sections referenced by every scenario added in Tasks 4-11 via `fixture: beer` / `fixture: type_matrix`.

- [ ] **Step 1: Add driver version bands and fixture references to spec.yaml**

Edit `bolt/conformance/spec.yaml`, replacing `driver_version_bands: {}` and `fixtures: {}`:

```yaml
driver_version_bands:
  java:
    band_names: [oldest-supported-4.x, latest-5.x, latest-6.x]
    driver_artifact: "org.neo4j.driver:neo4j-java-driver"
    resolution_note: >
      Resolved to concrete versions by #4889 at implementation time; current
      repo baseline is 6.2.0 (root pom.xml neo4j-driver.version).
  javascript:
    band_names: [oldest-supported-4.x, latest-5.x, latest-6.x]
    driver_artifact: "neo4j-driver (npm)"
    resolution_note: >
      Resolved by #4888; current repo baseline is ^6.0.1 (e2e-js/package.json).
  python:
    band_names: [lts, current, latest]
    driver_artifact: "neo4j (PyPI)"
    resolution_note: "Resolved by #4885 - no existing pin in this repo today."
  csharp:
    band_names: [lts, current, latest]
    driver_artifact: "Neo4j.Driver (NuGet)"
    resolution_note: "Resolved by #4886 - no existing pin in this repo today."
  go:
    band_names: [lts, current, latest]
    driver_artifact: "github.com/neo4j/neo4j-go-driver"
    resolution_note: "Resolved by #4887 - new module, no existing pin."

fixtures:
  beer:
    description: >
      Pre-existing OpenBeer dataset (Beer vertex type, name property
      confirmed via RemoteBoltDatabaseIT); imported at container boot,
      already shared by Java (Bolt) and Python (Postgres-wire) e2e today.
    seeded_by: ArcadeContainerTemplate
    source: "https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz"
    covers_areas: [connection, auth, transactions, causal-consistency, multi-database, result-handling]
  type_matrix:
    description: >
      Supplementary fixture for type-round-trip scenarios not covered by
      beer (temporal, Duration, Point, nested lists/maps, nulls). ByteArray
      is deliberately NOT pre-seeded here: Cypher has no byte-array literal
      syntax (matching real Neo4j), so ByteArray round-trip is tested purely
      via a client-bound parameter, not fixture data.
    seeded_by: >
      HTTP /command (SQL or Cypher), NOT Bolt - avoids exercising the
      under-test serialization path while seeding data.
    path: bolt/conformance/fixtures/type-matrix.cypher
    covers_areas: [type-roundtrip]
```

- [ ] **Step 2: Create the type-matrix fixture**

Create `bolt/conformance/fixtures/type-matrix.cypher`:

```cypher
// Bolt conformance type-round-trip fixture (issue #4883).
// Seed via HTTP /command (language=cypher), NOT via a Bolt session - see
// fixtures.type_matrix.seeded_by in spec.yaml for why.
// Verified to execute successfully against a live ArcadeDB instance
// (2026-07-03, embedded Cypher engine, ArcadeDB MCP tools).
CREATE (:TypeMatrix {
  localDateProp: date('2026-01-15'),
  localTimeProp: localtime('14:30:00'),
  localDateTimeProp: localdatetime('2026-01-15T14:30:00'),
  offsetDateTimeProp: datetime('2026-01-15T14:30:00+02:00'),
  durationProp: duration('P1DT2H30M'),
  pointProp: point({x: 12.34, y: 56.78}),
  nestedListProp: [1, 2, [3, 4]],
  nestedMapProp: {a: 1, b: {c: 2}},
  nullProp: null
});
```

- [ ] **Step 3: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL, but the `driver_version_bands` and `fixtures.*` errors from Task 2 are gone - only `scenarios list is empty`, `areas with zero scenarios: ['auth', 'causal-consistency', 'connection', 'errors', 'multi-database', 'protocol', 'result-handling', 'transactions', 'type-roundtrip']`, and `areas missing a #4890-tracked expected-fail scenario: ['connection', 'errors', 'protocol', 'type-roundtrip']` remain.

---

### Task 4: `connection` area scenarios

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.beer` from Task 3.
- Produces: 5 scenarios under `area: connection`, including the `#4890`-tracked ROUTE gap required by the validator's `REQUIRED_GAP_AREAS` check.

- [ ] **Step 1: Append connection scenarios to spec.yaml's `scenarios` list**

```yaml
  - id: CONN-001
    area: connection
    title: "Connect via bolt:// scheme"
    description: >
      Driver connects to a single ArcadeDB instance using the plain bolt://
      scheme and successfully verifies connectivity.
    fixture: beer
    preconditions: []
    steps:
      - "Given a running ArcadeDB instance with the Bolt listener enabled"
      - "When the driver connects to bolt://<host>:7687 and calls verifyConnectivity()"
      - "Then connectivity succeeds with no error"
    applicable_driver_versions: all
    current_status: passing
  - id: CONN-002
    area: connection
    title: "Connect via bolt+s:// with TLS required"
    description: >
      Driver connects over TLS when the server is configured with
      NETWORK_SSL mode REQUIRED.
    fixture: beer
    preconditions: ["server: NETWORK_SSL_REQUIRED"]
    steps:
      - "Given the server is started with TLS mode REQUIRED (BoltSslHelper)"
      - "When the driver connects to bolt+s://<host>:7687"
      - "Then the TLS handshake succeeds and queries run normally"
    applicable_driver_versions: all
    current_status: passing
  - id: CONN-003
    area: connection
    title: "Connect via neo4j:// routing discovery, single-node deployment"
    description: >
      Driver connects using the routing scheme against a single ArcadeDB
      instance; the routing table trivially contains only that node.
    fixture: beer
    preconditions: []
    steps:
      - "Given a single, non-clustered ArcadeDB instance"
      - "When the driver connects to neo4j://<host>:7687"
      - "Then the driver receives a routing table listing that one node as reader/writer/router and runs queries successfully"
    applicable_driver_versions: all
    current_status: passing
  - id: CONN-004
    area: connection
    title: "neo4j:// routing reflects actual multi-node HA cluster topology"
    description: >
      Against a multi-node HA cluster, the routing table returned by ROUTE
      should list the actual leader as writer and actual followers as
      readers, so neo4j:// driver-side load balancing/failover works.
    fixture: beer
    preconditions: ["deployment: 3-node HA cluster"]
    steps:
      - "Given a 3-node HA cluster with a known leader and two followers"
      - "When the driver connects to neo4j://<any-node>:7687 and requests a routing table"
      - "Then the routing table should list the true leader as writer and the true followers as readers"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      BoltNetworkExecutor.handleRoute (bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:903-939)
      always returns the local node's own address as WRITE, READ, and ROUTE
      roles, regardless of actual cluster topology - single-node only.
    tracking_issue: "#4890"
  - id: CONN-005
    area: connection
    title: "TLS OPTIONAL mode falls back to plaintext bolt://"
    description: >
      When the server's TLS mode is OPTIONAL, a plain (non-TLS) bolt://
      client can still connect successfully alongside TLS clients.
    fixture: beer
    preconditions: ["server: NETWORK_SSL_OPTIONAL"]
    steps:
      - "Given the server is started with TLS mode OPTIONAL"
      - "When a plaintext driver connects to bolt://<host>:7687"
      - "Then the connection succeeds without a TLS handshake"
    applicable_driver_versions: all
    current_status: passing
```

- [ ] **Step 2: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL; `connection` no longer appears in `areas with zero scenarios`, and `connection` no longer appears in `areas missing a #4890-tracked expected-fail scenario` (CONN-004 satisfies it). 8 areas remain with zero scenarios; `errors`, `protocol`, `type-roundtrip` still missing gap coverage.

---

### Task 5: `auth` area scenarios

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.beer` from Task 3.
- Produces: 3 scenarios under `area: auth`.

- [ ] **Step 1: Append auth scenarios**

```yaml
  - id: AUTH-001
    area: auth
    title: "Basic auth succeeds with valid credentials"
    description: "Driver authenticates via HELLO/LOGON with a valid username/password."
    fixture: beer
    preconditions: []
    steps:
      - "Given a server user with a known valid password"
      - "When the driver connects using AuthTokens.basic(user, password)"
      - "Then authentication succeeds and queries run"
    applicable_driver_versions: all
    current_status: passing
  - id: AUTH-002
    area: auth
    title: "Basic auth fails with invalid credentials"
    description: "Driver authentication fails with a structured Neo4j-style error code."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver connects using AuthTokens.basic(user, wrong-password)"
      - "Then authentication fails with error code Neo.ClientError.Security.Unauthorized (BoltErrorCodes.AUTHENTICATION_ERROR)"
    applicable_driver_versions: all
    current_status: passing
  - id: AUTH-003
    area: auth
    title: "Auth scheme 'none' is rejected (intentional, not a bug)"
    description: >
      Certifies that connecting with AuthTokens.none() is deliberately
      rejected by ArcadeDB today - this scenario documents a decision, not
      a gap; it should assert the rejection happens, not that it should
      eventually succeed.
    fixture: beer
    preconditions: []
    steps:
      - "When the driver connects using AuthTokens.none()"
      - "Then the connection is rejected with a structured auth error"
    applicable_driver_versions: all
    current_status: passing
```

- [ ] **Step 2: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL; `auth` no longer in `areas with zero scenarios`. 7 areas remain.

---

### Task 6: `transactions` area scenarios

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.beer` from Task 3.
- Produces: 5 scenarios under `area: transactions`.

- [ ] **Step 1: Append transaction scenarios**

```yaml
  - id: TX-001
    area: transactions
    title: "Autocommit query executes and returns results"
    description: "A single RUN/PULL outside any explicit transaction executes and commits implicitly."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (b:Beer) RETURN b.name LIMIT 5' via session.run() with no explicit transaction"
      - "Then 5 records are returned and the write (if any) is durable after the call returns"
    applicable_driver_versions: all
    current_status: passing
  - id: TX-002
    area: transactions
    title: "Explicit BEGIN/RUN/COMMIT persists changes"
    description: "An explicit transaction's writes are visible after COMMIT."
    fixture: beer
    preconditions: []
    steps:
      - "Given an explicit transaction (session.beginTransaction())"
      - "When the driver creates a node inside it and calls commit()"
      - "Then a subsequent autocommit query sees the created node"
    applicable_driver_versions: all
    current_status: passing
  - id: TX-003
    area: transactions
    title: "Explicit BEGIN/RUN/ROLLBACK discards changes"
    description: "An explicit transaction's writes are not visible after ROLLBACK."
    fixture: beer
    preconditions: []
    steps:
      - "Given an explicit transaction"
      - "When the driver creates a node inside it and calls rollback()"
      - "Then a subsequent autocommit query does not see the created node"
    applicable_driver_versions: all
    current_status: passing
  - id: TX-004
    area: transactions
    title: "Managed transaction function executeWrite commits on success"
    description: >
      The driver's managed-transaction helper (executeWrite/writeTransaction)
      wraps BEGIN/RUN/COMMIT automatically and commits when the callback
      returns normally.
    fixture: beer
    preconditions: []
    steps:
      - "When the driver calls session.executeWrite(tx -> tx.run('CREATE (:Beer {name: $n})', params))"
      - "Then the write is committed and visible afterward"
    applicable_driver_versions: all
    current_status: unverified
  - id: TX-005
    area: transactions
    title: "Managed transaction function retries on Neo.TransientError.*"
    description: >
      Driver-side executeWrite/executeRead retry policies re-run the
      callback automatically when the server signals a transient error
      (e.g. lock timeout, leader change). This requires the server to
      classify such failures as Neo.TransientError.* rather than
      Neo.ClientError.*/Neo.DatabaseError.*.
    fixture: beer
    preconditions: []
    steps:
      - "Given a condition that should be transient (e.g. a concurrent lock conflict)"
      - "When the failure occurs inside an executeWrite callback"
      - "Then the driver should see a Neo.TransientError.* code and retry automatically per its built-in policy"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      BoltErrorCodes.java defines only 7 codes, all Neo.ClientError.*/
      Neo.DatabaseError.* - no Neo.TransientError.* code exists, so
      ArcadeDB never signals a retryable condition and driver-side
      transient-retry logic cannot be exercised.
    tracking_issue: "#4890"
```

- [ ] **Step 2: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL; `transactions` no longer in `areas with zero scenarios`. 6 areas remain.

---

### Task 7: `causal-consistency` and `multi-database` area scenarios

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.beer` from Task 3.
- Produces: 3 scenarios (1 `causal-consistency`, 2 `multi-database`).

- [ ] **Step 1: Append scenarios**

```yaml
  - id: CAUSAL-001
    area: causal-consistency
    title: "Bookmark enforces read-after-write across sessions"
    description: >
      A bookmark obtained after a write-committing session can be passed to
      a new session (session.run with bookmarks=[...]) so the new session's
      reads are guaranteed to see that write.
    fixture: beer
    preconditions: []
    steps:
      - "Given session A commits a write and obtains its last bookmark"
      - "When session B is opened with bookmarks=[that bookmark] and immediately reads the written data"
      - "Then session B's read reflects session A's write, even against a follower"
    applicable_driver_versions: all
    current_status: unverified
  - id: MDB-001
    area: multi-database
    title: "Session selects a specific named database"
    description: >
      A driver session created with database='beer' operates against that
      database, not the server's default.
    fixture: beer
    preconditions: []
    steps:
      - "When the driver opens a session with sessionConfig.database('beer')"
      - "Then queries in that session run against the beer database (confirmed today via RemoteBoltDatabaseIT.queryBeerDatabase)"
    applicable_driver_versions: all
    current_status: passing
  - id: MDB-002
    area: multi-database
    title: "Sessions against different databases on the same driver are isolated"
    description: >
      Two sessions from the same driver instance, opened against two
      different databases, never see each other's data or transaction
      state.
    fixture: beer
    preconditions: ["multiple databases present on the server"]
    steps:
      - "Given two databases, e.g. beer and a second scratch database"
      - "When one session writes to the scratch database inside an open transaction"
      - "Then a concurrent session against beer is unaffected and unaware of that transaction"
    applicable_driver_versions: all
    current_status: unverified
```

- [ ] **Step 2: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL; `causal-consistency` and `multi-database` no longer in `areas with zero scenarios`. 4 areas remain (`result-handling`, `type-roundtrip`, `errors`, `protocol`).

---

### Task 8: `result-handling` area scenarios

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.beer` from Task 3.
- Produces: 4 scenarios under `area: result-handling`.

- [ ] **Step 1: Append result-handling scenarios**

```yaml
  - id: RESULT-001
    area: result-handling
    title: "Streaming PULL returns records incrementally"
    description: "The driver can iterate results as they stream rather than requiring the full result materialized upfront."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs a query returning many Beer records and iterates the Result lazily"
      - "Then records arrive incrementally (PULL/RECORD/RECORD.../SUCCESS), not as one batch"
    applicable_driver_versions: all
    current_status: passing
  - id: RESULT-002
    area: result-handling
    title: "PULL n streams exactly n rows, further PULL continues from where it left off"
    description: >
      When a driver issues PULL with an explicit n less than the total row
      count, exactly n records are returned and a subsequent PULL/DISCARD
      continues correctly rather than restarting or skipping rows.
    fixture: beer
    preconditions: []
    steps:
      - "When the driver issues PULL {n: 2} against a query with 5 results"
      - "Then exactly 2 records are returned, and a follow-up PULL {n: -1} (or DISCARD) returns/discards the remaining 3 without gaps or duplicates"
    applicable_driver_versions: all
    current_status: unverified
  - id: RESULT-003
    area: result-handling
    title: "DISCARD abandons remaining rows without materializing them"
    description: "DISCARD after a partial PULL correctly ends the result stream."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver PULLs 1 row of a 5-row result then calls result.consume() (DISCARD)"
      - "Then the stream ends cleanly and a ResultSummary is returned"
    applicable_driver_versions: all
    current_status: passing
  - id: RESULT-004
    area: result-handling
    title: "ResultSummary counters accurately reflect write operations"
    description: >
      After a write query, summary.counters() (nodesCreated,
      relationshipsCreated, propertiesSet, etc.) matches the actual writes
      performed.
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs 'CREATE (:Beer {name: $n})-[:BREWED_BY]->(:Brewery {name: $b})'"
      - "Then summary.counters().nodesCreated() == 2, relationshipsCreated() == 1, and propertiesSet() matches"
    applicable_driver_versions: all
    current_status: unverified
```

- [ ] **Step 2: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL; `result-handling` no longer in `areas with zero scenarios`. 3 areas remain (`type-roundtrip`, `errors`, `protocol`), all three still missing `#4890` gap coverage.

---

### Task 9: `type-roundtrip` area scenarios, part A - structural types and collections

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.beer` from Task 3 (Node/Relationship come from the existing graph data).
- Produces: 6 scenarios under `area: type-roundtrip` covering Node, Relationship, Path, ByteArray, nested collections, and null.

- [ ] **Step 1: Append structural/collection type scenarios**

```yaml
  - id: TYPE-001
    area: type-roundtrip
    title: "Node round-trips as a native Bolt structure"
    description: "A query returning a graph node yields a native driver Node object (labels + properties), not a flattened map."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (b:Beer) RETURN b LIMIT 1'"
      - "Then the client receives a native Node value with .labels() and .asMap() populated (BoltStructureMapper Node handling, sig 0x4E)"
    applicable_driver_versions: all
    current_status: passing
  - id: TYPE-002
    area: type-roundtrip
    title: "Relationship round-trips as a native Bolt structure"
    description: "A query returning a graph relationship yields a native driver Relationship object."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs 'MATCH ()-[r]->() RETURN r LIMIT 1'"
      - "Then the client receives a native Relationship value (BoltStructureMapper Relationship handling, sig 0x52)"
    applicable_driver_versions: all
    current_status: passing
  - id: TYPE-003
    area: type-roundtrip
    title: "Path round-trips as a native Bolt structure"
    description: "A variable-length or explicit path query yields a native driver Path object, not a list of separately-typed nodes/relationships."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs 'MATCH p=(b:Beer)-[*1..2]-() RETURN p LIMIT 1'"
      - "Then the client receives a native Path value (sig 0x50) with correctly ordered nodes/relationships"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      structure/BoltPath.java exists but has zero call sites constructing
      it anywhere in BoltStructureMapper - query results never actually
      produce native Path structures today.
    tracking_issue: "#4890"
  - id: TYPE-004
    area: type-roundtrip
    title: "ByteArray round-trips as a bound parameter"
    description: >
      Cypher has no byte-array literal syntax, so this is parameter-only:
      the client sends a byte array as $param and reads it back unchanged.
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'RETURN $b AS echo' with params={b: byte[]{1,2,3,4}}"
      - "Then echo is a native byte[] equal to the input (PackStreamWriter byte[] pass-through, BoltStructureMapper.java:111-113)"
    applicable_driver_versions: all
    current_status: passing
  - id: TYPE-005
    area: type-roundtrip
    title: "Nested lists and maps round-trip structurally"
    description: "A property holding nested lists/maps (list of list, map of map) round-trips with structure preserved."
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.nestedListProp AS l, t.nestedMapProp AS m'"
      - "Then l == [1, 2, [3, 4]] and m == {a: 1, b: {c: 2}} with correct nested types"
    applicable_driver_versions: all
    current_status: passing
  - id: TYPE-006
    area: type-roundtrip
    title: "Null values round-trip"
    description: "A null-valued property and a null bound parameter both round-trip as null, not omitted or coerced."
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.nullProp AS n' and separately 'RETURN $p AS echo' with params={p: null}"
      - "Then both n and echo are null"
    applicable_driver_versions: all
    current_status: passing
```

- [ ] **Step 2: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL; `type-roundtrip` no longer in `areas with zero scenarios` and no longer missing gap coverage (TYPE-003 satisfies it). `errors` and `protocol` remain in both lists.

---

### Task 10: `type-roundtrip` area scenarios, part B - temporal and spatial types

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.type_matrix` from Task 3.
- Produces: 6 more `type-roundtrip` scenarios (temporal x4, Duration, Point).

- [ ] **Step 1: Append temporal/spatial type scenarios**

```yaml
  - id: TYPE-007
    area: type-roundtrip
    title: "LocalDate round-trips as a native Bolt Date structure"
    description: "A LocalDate-valued property and bound parameter are native Date values, not ISO strings."
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.localDateProp AS d'"
      - "Then d is a native LocalDate/Date driver type, and 'RETURN $d AS echo' with params={d: <that date>} round-trips it unchanged"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      BoltStructureMapper.toPackStreamValue (lines 116-150) converts
      LocalDate via .toString()/ISO string fallback instead of the native
      Bolt Date structure (sig 0x44).
    tracking_issue: "#4890"
  - id: TYPE-008
    area: type-roundtrip
    title: "LocalTime round-trips as a native Bolt LocalTime structure"
    description: "A LocalTime-valued property and bound parameter are native values, not ISO strings."
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.localTimeProp AS t2'"
      - "Then t2 is a native LocalTime driver type, and round-trips via $-parameter unchanged"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      Same ISO-string fallback as TYPE-007, for LocalTime (native Bolt
      LocalTime structure sig 0x74 not produced).
    tracking_issue: "#4890"
  - id: TYPE-009
    area: type-roundtrip
    title: "LocalDateTime round-trips as a native Bolt LocalDateTime structure"
    description: "A LocalDateTime-valued property and bound parameter are native values, not ISO strings."
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.localDateTimeProp AS dt'"
      - "Then dt is a native LocalDateTime driver type, and round-trips via $-parameter unchanged"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      Same ISO-string fallback as TYPE-007, for LocalDateTime (native Bolt
      LocalDateTime structure sig 0x64 not produced).
    tracking_issue: "#4890"
  - id: TYPE-010
    area: type-roundtrip
    title: "Offset/zoned DateTime round-trips as a native Bolt DateTime structure"
    description: "An offset-aware DateTime-valued property and bound parameter are native values (with correct offset/zone), not ISO strings."
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.offsetDateTimeProp AS dt'"
      - "Then dt is a native OffsetDateTime/ZonedDateTime driver type preserving the +02:00 offset, and round-trips via $-parameter unchanged"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      Same ISO-string fallback as TYPE-007, for OffsetDateTime/ZonedDateTime
      (native Bolt DateTime/DateTimeZoneId structures, sig 0x49/0x69, not
      produced).
    tracking_issue: "#4890"
  - id: TYPE-011
    area: type-roundtrip
    title: "Duration round-trips as a native Bolt Duration structure"
    description: >
      Server returns a java.time.Duration-valued property; client reads it
      back as a native Duration value, then sends it back as a query
      parameter and the server persists/compares it correctly.
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.durationProp AS d'"
      - "Then d is a native Duration/isoduration value, not a string"
      - "When the driver runs 'RETURN $d AS echo' with params={d: <that duration>}"
      - "Then echo round-trips without precision loss"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      BoltStructureMapper/PackStreamWriter have no Duration handling at
      all - not even a string fallback branch, falls through to generic
      value.toString().
    tracking_issue: "#4890"
  - id: TYPE-012
    area: type-roundtrip
    title: "Point round-trips as a native Bolt Point structure"
    description: >
      Server returns a spatial Point-valued property; client reads it back
      as a native Point value with correct coordinates/CRS, then sends it
      back as a query parameter.
    fixture: type_matrix
    preconditions: []
    steps:
      - "When the driver runs 'MATCH (t:TypeMatrix) RETURN t.pointProp AS p'"
      - "Then p is a native Point value with x=12.34, y=56.78, not a map or string"
      - "When the driver runs 'RETURN $p AS echo' with params={p: <that point>}"
      - "Then echo round-trips without precision loss"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      No Point/spatial type handling exists anywhere in
      BoltStructureMapper or PackStreamWriter (confirmed even though the
      underlying ArcadeDB Cypher engine itself does support point() -
      verified point({x:12.34,y:56.78}) creates/stores correctly via
      HTTP/embedded execution; the gap is Bolt wire serialization only).
    tracking_issue: "#4890"
```

- [ ] **Step 2: Run the validator**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: FAIL; only `errors` and `protocol` remain in `areas with zero scenarios` and `areas missing a #4890-tracked expected-fail scenario`.

---

### Task 11: `errors` and `protocol` area scenarios

**Files:**
- Modify: `bolt/conformance/spec.yaml`

**Interfaces:**
- Consumes: `fixtures.beer` from Task 3.
- Produces: 4 `errors` scenarios + 3 `protocol` scenarios - the last two areas needed for full coverage.

- [ ] **Step 1: Append errors and protocol scenarios**

```yaml
  - id: ERR-001
    area: errors
    title: "Syntax error returns Neo.ClientError.Statement.SyntaxError"
    description: "An invalid Cypher statement returns the correct structured client error code."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs an intentionally malformed query, e.g. 'MATCH (n RETURN n'"
      - "Then the driver receives error code Neo.ClientError.Statement.SyntaxError (BoltErrorCodes.SYNTAX_ERROR)"
    applicable_driver_versions: all
    current_status: passing
  - id: ERR-002
    area: errors
    title: "Semantic error returns Neo.ClientError.Statement.SemanticError"
    description: "A syntactically valid but semantically invalid query (e.g. undefined variable reference) returns the correct code."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver runs a semantically invalid query, e.g. referencing an undeclared variable in RETURN"
      - "Then the driver receives error code Neo.ClientError.Statement.SemanticError (BoltErrorCodes.SEMANTIC_ERROR)"
    applicable_driver_versions: all
    current_status: passing
  - id: ERR-003
    area: errors
    title: "Unauthenticated request returns Neo.ClientError.Security.Forbidden"
    description: "A request attempted without prior successful authentication is rejected with the correct code."
    fixture: beer
    preconditions: []
    steps:
      - "When the driver sends RUN before completing HELLO/LOGON"
      - "Then the driver receives error code Neo.ClientError.Security.Forbidden (BoltErrorCodes.FORBIDDEN_ERROR)"
    applicable_driver_versions: all
    current_status: passing
  - id: ERR-004
    area: errors
    title: "Transient conditions surface Neo.TransientError.* codes"
    description: >
      Conditions that are expected to succeed on retry (lock timeout,
      concurrent modification, leader change during an HA write) should
      surface a Neo.TransientError.* code so driver-side retry policies
      (managed transaction functions) know to retry rather than fail
      permanently.
    fixture: beer
    preconditions: []
    steps:
      - "Given a condition expected to be transient, e.g. two sessions racing to write the same node"
      - "When the losing session's write fails"
      - "Then it should receive a Neo.TransientError.* code, not a ClientError/DatabaseError code"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      BoltErrorCodes.java defines only Neo.ClientError.*/Neo.DatabaseError.*
      codes (7 total) - no Neo.TransientError.* code exists anywhere in the
      Bolt module.
    tracking_issue: "#4890"
  - id: PROTO-001
    area: protocol
    title: "Version negotiation succeeds for Bolt 4.4, 4.0, and 3.0"
    description: "The server correctly negotiates each of its advertised supported versions."
    fixture: beer
    preconditions: []
    steps:
      - "When a driver advertises support for Bolt versions including 4.4, 4.0, or 3.0"
      - "Then the server negotiates the highest mutually supported version among {0x00000404, 0x00000004, 0x00000003} (BoltNetworkExecutor.SUPPORTED_VERSIONS, line 96) and the connection proceeds"
    applicable_driver_versions: all
    current_status: passing
  - id: PROTO-002
    area: protocol
    title: "Version negotiation with a Bolt 5.x-only driver"
    description: >
      A driver that only offers Bolt 5.x versions during handshake should
      either negotiate a documented-supported 5.x version, or receive a
      clean, documented rejection - not an undocumented silent downgrade.
    fixture: beer
    preconditions: []
    steps:
      - "When a driver's handshake proposes only Bolt 5.x version ranges"
      - "Then the server should either negotiate a 5.x version, or the negotiation failure/fallback behavior should be documented and intentional"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      BoltNetworkExecutor.SUPPORTED_VERSIONS (line 96) never advertises
      any Bolt 5.x version; current drivers that support 5.x only work
      today by silently downgrading to 4.4, which is undocumented and
      untested as a deliberate compatibility stance.
    tracking_issue: "#4890"
  - id: PROTO-003
    area: protocol
    title: "RESET returns the connection to a clean state mid-stream"
    description: "A RESET message sent while a result is still streaming aborts it and leaves the connection ready for a new RUN."
    fixture: beer
    preconditions: []
    steps:
      - "Given a query with multiple pending records mid-stream"
      - "When the driver sends RESET"
      - "Then the server discards the pending stream and the connection accepts a new RUN immediately"
    applicable_driver_versions: all
    current_status: unverified
```

- [ ] **Step 2: Run the validator and confirm full pass**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml`
Expected: `OK: 39 scenarios across 9 areas` (exact count depends on final tally - re-count if it differs, but the run must exit 0 with no errors).

---

### Task 12: README.md - consumption model and traceability convention

**Files:**
- Modify: `bolt/conformance/README.md`

**Interfaces:**
- Consumes: the completed `spec.yaml` from Tasks 1-11 (references its structure/fields).
- Produces: the document B-task implementers (#4885-#4889) read before writing any test code.

- [ ] **Step 1: Replace the stub README with full content**

```markdown
# Bolt Conformance Spec

Shared, language-neutral certification scenarios for ArcadeDB's Bolt
protocol implementation, referenced by issue #4883 (part of epic #4882).
`spec.yaml` in this directory is the single source of truth for what
"certified" means across all five official Neo4j driver ecosystems (Java,
JavaScript, Python, C#, Go).

## Consumption model

**No language's test suite parses `spec.yaml` at runtime.** This file is a
reference document: for each scenario, a human hand-writes the equivalent
test in that language's existing idiomatic framework - jest (`e2e-js`),
pytest (`e2e-python`), xUnit (`e2e-csharp`), `go test` (`e2e-go`, new
module), or JUnit (`bolt`/`e2e` for Java). Do not build a YAML-driven test
generator or interpreter for this spec in any language - that is explicitly
out of scope.

The only place `spec.yaml` is ever parsed programmatically is the future CI
aggregation step (issue #4891/#4892), which cross-references JUnit-style
test report output against the scenario list to build the published
compatibility matrix. That is one script, not five.

## Traceability convention

Every hand-written test for a scenario in this spec must embed that
scenario's `id` so coverage can be checked by grepping test names - for
example:

- Java/JUnit: `@DisplayName("[TYPE-011] Duration round-trips as a native Bolt Duration structure")`
- JS/Jest: `test('[TYPE-011] duration round-trip', async () => { ... })`
- Python/pytest: `def test_TYPE_011_duration_roundtrip(): ...`
- C#/xUnit: `[Fact(DisplayName = "TYPE-011: Duration round-trip")]`
- Go/`go test`: `func TestTYPE011_DurationRoundtrip(t *testing.T) { ... }`

## Scenario fields

Each entry in `scenarios:` has:

| Field | Meaning |
|---|---|
| `id` | Stable identifier, `AREA-NNN` |
| `area` | One of the 9 feature-matrix areas (see below) |
| `title` / `description` | What the scenario exercises |
| `fixture` | `beer` or `type_matrix` (see Fixtures below) |
| `preconditions` | Setup required beyond the fixture, if any |
| `steps` | Given/When/Then-style steps a human translates into test code |
| `applicable_driver_versions` | `all`, or a list of `language:band_name` refs from `driver_version_bands` |
| `current_status` | `passing`, `expected-fail`, `not-applicable`, or `unverified` |
| `known_limitation` / `tracking_issue` | Required when `current_status: expected-fail` |

**`current_status` is a starting hint, not a verdict.** `passing` and
`expected-fail` reflect what direct source-code inspection confirmed at
spec-authoring time (2026-07-03); `unverified` scenarios were not directly
confirmed either way and are exactly what each B-task's first implementation
run is meant to determine. If a B-task finds an `unverified` scenario
actually fails and the failure looks like a real gap (not scenario error),
open a new tracking issue (or fold it under `#4890` if related) and update
`tracking_issue`/`known_limitation` here in the same PR.

## Areas

`connection`, `auth`, `transactions`, `causal-consistency`,
`multi-database`, `result-handling`, `type-roundtrip`, `errors`, `protocol` -
verbatim from the epic #4882 feature-matrix table, so spec coverage can be
checked directly against that table.

## Driver version bands

`driver_version_bands` in `spec.yaml` defines symbolic bands per language
(e.g. `latest-6.x`, `lts`) rather than concrete version numbers, so the spec
doesn't go stale between now and whenever each B-task actually starts. Each
B-task resolves its own bands to concrete versions and records them in its
own build manifest (pom.xml / package.json / requirements.txt / .csproj /
go.mod) at implementation time.

## Fixtures

- **`beer`**: the pre-existing OpenBeer dataset, imported remotely at
  container boot via `ArcadeContainerTemplate` - already shared by Java and
  Python e2e today. Covers most scenarios.
- **`type_matrix`**: `fixtures/type-matrix.cypher`, a small local fixture
  supplementing `beer` with rows for the type-round-trip matrix (temporal,
  Duration, Point, nested collections, nulls). **Must be seeded via HTTP
  `/command` (SQL or Cypher) or the embedded API - never via a Bolt
  session** - seeding through Bolt would exercise the very serialization
  path several `type-roundtrip` scenarios exist to test, invalidating the
  result.

## Validating the spec

```bash
cd bolt/conformance
python3 validate_spec.py spec.yaml
```

Checks structural integrity: unique scenario IDs, valid `area`/
`current_status` values, every area has at least one scenario, and the 4
epic-confirmed gaps (single-node-only ROUTE, missing `Neo.TransientError.*`,
no Bolt 5.x negotiation, type-fidelity gaps) each have a `#4890`-tracked
`expected-fail` scenario. Run this after any edit to `spec.yaml`.
```

- [ ] **Step 2: Confirm the README renders sensibly**

Run: `cd bolt/conformance && python3 -c "import sys; content=open('README.md').read(); assert '## Consumption model' in content and '## Traceability convention' in content; print('README sections present')"`
Expected: `README sections present`

---

### Task 13: Live fixture verification against a real ArcadeDB instance

**Files:**
- None created/modified - this task is verification-only, confirming Task 3's fixture actually works end-to-end (beyond the syntax check already spot-verified during planning).

**Interfaces:**
- Consumes: `bolt/conformance/fixtures/type-matrix.cypher` from Task 3.
- Produces: confirmation logged in this task's notes that the fixture is safe for B-task implementers to rely on as-is.

- [ ] **Step 1: Confirm a reachable ArcadeDB instance**

Using the ArcadeDB MCP tools (`list_databases`), confirm at least one database is reachable. If none is available in your environment, start one locally per this repo's standard dev flow (`mvn -pl engine,server -am install -DskipTests -q` then run the packaged server script), or use any disposable local ArcadeDB instance.

Expected: at least one database name returned.

- [ ] **Step 2: Execute the fixture script's statement against a scratch vertex type**

Using the ArcadeDB MCP tools (`execute_command`, `language: cypher`), run the exact contents of `bolt/conformance/fixtures/type-matrix.cypher` against any reachable database.

Expected: the `CREATE` succeeds and returns one record with all 9 properties present (`localDateProp`, `localTimeProp`, `localDateTimeProp`, `offsetDateTimeProp`, `durationProp`, `pointProp`, `nestedListProp`, `nestedMapProp` - `nullProp` legitimately omitted from JSON output since it's null, which is expected HTTP/JSON behavior, not a bug).

This exact check was already run once during planning (2026-07-03) against the `test` database and succeeded - this step re-confirms it against whichever instance the plan is executed in, since environments differ.

- [ ] **Step 3: Query the seeded data back and spot-check types**

Using `query` (`language: cypher`), run `MATCH (t:TypeMatrix) RETURN t.durationProp, t.pointProp, t.localDateProp` and confirm non-null, plausible values are returned.

Expected: values matching what was inserted (duration `P1DT2H30M`, point with x=12.34/y=56.78, date `2026-01-15`).

- [ ] **Step 4: Clean up the scratch data**

Using `execute_command` (`language: sql`), run `DELETE FROM TypeMatrix UNSAFE` and `DROP TYPE TypeMatrix` against whichever database was used for verification, so this step is repeatable and leaves no residue.

Expected: both commands succeed.

- [ ] **Step 5: Final full validator run**

Run: `cd bolt/conformance && python3 validate_spec.py spec.yaml && python3 -m unittest test_validate_spec.py -v`
Expected: validator prints `OK: <N> scenarios across 9 areas` and exits 0; all unit tests still PASS.

- [ ] **Step 6: Stage the new files (no commit)**

Run: `git add bolt/conformance/`
Expected: `git status` shows `bolt/conformance/spec.yaml`, `bolt/conformance/README.md`, `bolt/conformance/validate_spec.py`, `bolt/conformance/test_validate_spec.py`, `bolt/conformance/fixtures/type-matrix.cypher` staged. Per Global Constraints, do not run `git commit` - the user reviews and commits.

---

## Summary of deliverables

- `bolt/conformance/spec.yaml` - ~39 scenarios across all 9 epic feature-matrix areas, with the 4 confirmed protocol gaps explicitly tracked as `expected-fail` (never silently omitted).
- `bolt/conformance/fixtures/type-matrix.cypher` - verified-executable local fixture for type-round-trip scenarios, seeded via HTTP/SQL not Bolt.
- `bolt/conformance/README.md` - consumption model and traceability convention for the five B-task implementers.
- `bolt/conformance/validate_spec.py` + `test_validate_spec.py` - structural-integrity check, runnable by any future spec edit (including by B-tasks if they add `unverified`-resolution updates).
