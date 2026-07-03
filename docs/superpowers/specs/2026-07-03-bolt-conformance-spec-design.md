# Design: Shared Bolt Conformance Spec (issue #4883)

## Context

Part of epic #4882 (Bolt Driver Compatibility Certification). ArcadeDB's `bolt`
module implements the Neo4j Bolt wire protocol (3.0/4.0/4.4) well enough that
`neo4j-java-driver` and `neo4j-driver` (JS) already connect and run queries in
CI, but coverage is shallow and uneven across languages, and there is no
single definition of "certified" to test against.

This design covers **only issue #4883**: authoring the shared, language-neutral
conformance spec that the five per-language e2e suites (#4885-#4889) will be
hand-implemented against. It does not touch protocol code, the server-identity
doc (#4884), or any B/C/D-group issue.

## Current state (grounding, verified against the codebase)

- **Protocol negotiation**: `BoltNetworkExecutor.SUPPORTED_VERSIONS` only
  offers 3.0/4.0/4.4 (`bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java:96`).
  No Bolt 5.x.
- **ROUTE**: `handleRoute` (same file, lines 903-939) always returns the local
  node's own address for WRITE/READ/ROUTE roles - single-node only, no
  HA-aware topology.
- **Type fidelity** (`BoltStructureMapper.toPackStreamValue`, lines 48-158):
  - Native: Node, Relationship, UnboundRelationship, `byte[]`, lists/maps, nulls.
  - `BoltPath` class exists but has zero call sites constructing it - query
    results never actually produce native Path structures today.
  - Temporal types (LocalDate, LocalTime, LocalDateTime, OffsetDateTime,
    ZonedDateTime, OffsetTime, Instant, `java.util.Date`) fall back to ISO
    strings, not native Bolt temporal structures.
  - `java.time.Duration` has no handling at all (not even a string fallback
    branch - falls through to generic `toString()`).
  - Spatial `Point` has no handling anywhere in the mapper.
- **Errors** (`BoltErrorCodes.java`): 7 codes, all `Neo.ClientError.*` /
  `Neo.DatabaseError.*`. Zero `Neo.TransientError.*` codes, so driver-side
  transient-error retry logic has nothing to trigger on.
- **e2e coverage**:
  - Java: `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java` - 4
    tests (connectivity, simple query, beer-dataset query, parameterized query).
  - JS: `e2e-js/src/js-bolt-e2e.test.js`, driver pinned `^6.0.1` - parameterized
    WHERE-clause queries and one VLP regression test (issue #4452).
  - Python (`e2e-python`) and C# (`e2e-csharp`): Postgres-wire only, zero Bolt
    references.
  - Go: no `e2e-go` module exists in the repo at all.
  - No suite anywhere tests transactions, error paths, or type round-trips.
- **Fixture**: the "beer" dataset is not a repo file - `ArcadeContainerTemplate`
  imports a remote gzipped OrientDB export from
  `ArcadeData/arcadedb-datasets` at container boot
  (`-Darcadedb.server.defaultDatabases=beer[root]{import:...OpenBeer.gz}`),
  already shared by Java (Bolt) and Python (Postgres) e2e today.
- **CI**: no Bolt-specific workflow exists; grep for "bolt" across
  `.github/workflows/*.yml` returns zero matches. Bolt module tests only run
  implicitly inside the general `mvn-test.yml` build. No nightly cron touches
  Bolt.

## Decisions

### 1. Spec format: structured YAML

`bolt/conformance/spec.yaml` is the single source of truth, not a Markdown
table. Rationale: the epic's success criteria require comparable, aggregable
results across five drivers feeding a published matrix (#4891/#4892);
structured records with stable IDs support that directly, while Markdown
tables would need re-authoring into structured data later anyway. Readability
is preserved by keeping natural-language `description`/`steps` fields inside
each YAML record (see below) rather than requiring a separate generated view.

**Alternative considered and rejected: Gherkin/BDD** (Cucumber-JVM,
Cucumber.js, `behave`/`pytest-bdd`, Reqnroll, `godog`). Gherkin `.feature`
files would be literally executable and shared verbatim across languages,
which is a genuinely stronger unification than YAML-plus-hand-translation.
Rejected because:
- The epic's own governing principle names the *existing* idiomatic
  frameworks already in use - jest, pytest, xUnit, `go test` - not a BDD
  framework. Adopting Cucumber/behave/Reqnroll/godog means introducing four
  new test-framework dependencies across modules that don't currently have
  them (and running two test runners per language, or migrating existing
  thin suites).
- Step-definition glue code is still per-language, hand-written work; Gherkin
  changes the syntax it's written against, not the amount of work.
- Gherkin's core value (bridging technical/non-technical stakeholders) isn't
  needed here - the audience is driver-conformance engineers.
- It would expand #4883's scope from "author a spec" into "select and justify
  a BDD framework for 5 ecosystems," which belongs to the B-tasks
  (#4885-#4889) if ever revisited, not the A1 keystone.

### 2. Consumption model: spec is a reference, not a runtime dependency

`spec.yaml` is read by a human once per scenario when hand-writing that
language's native test (jest/pytest/xUnit/`go test`) - **no language writes a
YAML parser to consume it.** Each hand-written test embeds the scenario ID
(test name prefix, comment, or tag, e.g. `test_TYPE010_duration_roundtrip`)
for traceability. The only place that ever parses `spec.yaml` programmatically
is the future CI aggregation step in #4891/#4892, which cross-references
JUnit-style report output (already tagged with scenario IDs) against the full
scenario list - one script, not five. This is documented explicitly in
`bolt/conformance/README.md` so no B-task implementer builds an unnecessary
parser.

### 3. Driver version pinning: symbolic bands, not concrete versions

Each language declares named bands (Java/JS: `oldest-supported-4.x`,
`latest-5.x`, `latest-6.x`; Python/C#/Go: `lts`, `current`, `latest`) rather
than hardcoded version numbers. Concrete versions are resolved by each B-task
at implementation time and recorded in that language's own build manifest
(pom.xml / package.json / requirements.txt / .csproj / go.mod) - the same
place dependency versions already live. This avoids the spec drifting stale
between now and whenever each B-task actually starts (no scheduled start
date).

### 4. Fixture: extend locally, don't touch the external dataset repo

The existing `beer` fixture (remote OrientDB import from
`ArcadeData/arcadedb-datasets`) is referenced as-is - zero risk to the
existing Java/Python e2e that already depend on it, no external-repo
coordination required. A **new local fixture**,
`bolt/conformance/fixtures/type-matrix.cypher`, is added to this repo to seed
the rows needed for the type-round-trip matrix that `beer` doesn't cover
(Duration, Point, temporal edge cases, byte arrays, nested lists/maps, nulls).

Important constraint: `type-matrix.cypher` must be seeded via **HTTP
`/command` (SQL) or embedded API, not via Bolt** - since Duration/Point have
zero handling in the Bolt serialization layer under test, seeding through
Bolt would already exercise the broken path independently of the
`TYPE-*` scenarios that are supposed to test it. This is called out explicitly
in the fixture's `seeded_by` field and in the README so B-task implementers
don't get a false negative from seeding through the wrong channel.

## Spec schema

```yaml
# bolt/conformance/spec.yaml
version: 1

driver_version_bands:
  java:
    band_names: [oldest-supported-4.x, latest-5.x, latest-6.x]
    driver_artifact: "org.neo4j.driver:neo4j-java-driver"
    resolution_note: "Resolved by #4889; current repo baseline is 6.2.0."
  javascript:
    band_names: [oldest-supported-4.x, latest-5.x, latest-6.x]
    driver_artifact: "neo4j-driver (npm)"
    resolution_note: "Resolved by #4888; current repo baseline is ^6.0.1."
  python:
    band_names: [lts, current, latest]
    driver_artifact: "neo4j (PyPI)"
    resolution_note: "Resolved by #4885; no existing pin in this repo."
  csharp:
    band_names: [lts, current, latest]
    driver_artifact: "Neo4j.Driver (NuGet)"
    resolution_note: "Resolved by #4886; no existing pin in this repo."
  go:
    band_names: [lts, current, latest]
    driver_artifact: "github.com/neo4j/neo4j-go-driver"
    resolution_note: "Resolved by #4887; new module, no existing pin."

fixtures:
  beer:
    description: >
      Pre-existing OpenBeer dataset (Beer vertex type, name property);
      imported at container boot, already shared by Java (Bolt) and Python
      (Postgres-wire) e2e today.
    seeded_by: ArcadeContainerTemplate
    source: "https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz"
    covers_areas: [connection, auth, transactions, result-handling, multi-database]
  type_matrix:
    description: >
      Supplementary fixture for type-round-trip scenarios not covered by
      beer (temporal edge cases, Duration, Point, ByteArray, nested/null
      values).
    seeded_by: >
      HTTP /command (SQL), NOT Bolt - avoids exercising the under-test
      serialization path while seeding data.
    path: bolt/conformance/fixtures/type-matrix.cypher
    covers_areas: [type-roundtrip]

scenarios:
  - id: TYPE-010
    area: type-roundtrip
    title: "Duration round-trip as query result and bound parameter"
    description: >
      Server returns a java.time.Duration-valued property; client reads it
      back as a native Duration value, then sends it back as a query
      parameter and the server persists/compares it correctly.
    fixture: type_matrix
    preconditions: []
    steps:
      - "RUN: MATCH (t:TypeMatrix) RETURN t.durationProp AS d"
      - "Assert d is a native Duration/isoduration value, not a string"
      - "RUN: RETURN $d AS echo  (params: {d: <duration>})"
      - "Assert echo round-trips without precision loss"
    applicable_driver_versions: all
    current_status: expected-fail
    known_limitation: >
      BoltStructureMapper/PackStreamWriter have no Duration handling; falls
      through to generic toString(), so drivers never receive a native value.
    tracking_issue: "#4890"
  # ... remaining scenarios across all 9 areas
```

**Status taxonomy** (`current_status`):
- `passing` - works today against ArcadeDB's shipped Bolt implementation.
- `expected-fail` - known gap; must carry `known_limitation` (why) and
  `tracking_issue` (`#4890` for the 4 confirmed epic gaps, or a new issue if a
  B-task surfaces something not yet tracked).
- `not-applicable` - intentionally doesn't apply to a given
  driver/version/language combo; explained inline, no tracking issue needed.

**Areas** (verbatim from the epic's feature-matrix table, so acceptance
criterion "covering every row of the Epic's feature matrix table" is directly
checkable by area-name cross-reference): `connection`, `auth`, `transactions`,
`causal-consistency`, `multi-database`, `result-handling`, `type-roundtrip`,
`errors`, `protocol`.

## File layout

```
bolt/conformance/
  spec.yaml              # source of truth: scenarios, driver bands, fixture refs
  README.md              # consumption model, scenario-ID traceability convention,
                          # how B-tasks (#4885-#4889) should use this spec
  fixtures/
    type-matrix.cypher    # supplementary fixture, seeded via HTTP not Bolt
```

## Verification plan

No protocol code changes, so verification targets spec integrity, not runtime
behavior:

1. `spec.yaml` parses as valid YAML.
2. Every one of the epic's 9 feature-matrix areas has >= 1 scenario; every
   scenario's `area` is one of the 9 valid values.
3. The 4 confirmed gaps (temporal-as-string, missing Duration/Point, missing
   `Neo.TransientError.*`, single-node `ROUTE`) each appear as `expected-fail`
   scenarios tagged `tracking_issue: "#4890"`.
4. `type-matrix.cypher` is actually executed against a live ArcadeDB instance
   (via the ArcadeDB MCP tools) to confirm it runs without error before being
   considered done - not just assumed to work.
5. `README.md` is checked for the consumption-model note so no B-task builds
   an unneeded YAML parser.

## Out of scope

- Any protocol code change (Duration/Point/temporal fidelity, TransientError
  codes, HA-aware ROUTE, Bolt 5.x negotiation) - tracked separately under
  #4890, only *referenced* by `expected-fail` scenarios here.
- Implementing any of the five per-language suites (#4885-#4889).
- The server-identity documentation (#4884).
- CI gating/nightly runs (#4891) and the published matrix (#4892).
