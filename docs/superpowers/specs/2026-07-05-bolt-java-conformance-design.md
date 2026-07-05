# Bolt (Java) conformance certification - design (#4889)

Part of epic #4882 (Bolt Driver Compatibility Certification), Group B.
Depends on the A1 shared conformance spec (#4883, `bolt/conformance/spec.yaml`,
already on `main`). Sibling suites #4887 (Go) and #4888 (JS) are merged and are
the direct model for this work.

## Goal

Deepen the Java Bolt certification from 4 smoke tests to the full 39-scenario
A1 conformance spec, driven by the real `neo4j-java-driver`, so the Java column
of the eventual compatibility matrix (#4892) is comparable to the JS/Go columns.

Certification, not remediation: every unsupported cell is proven and documented
as a known limitation tracked by #4890, never silently omitted. No production
Bolt code changes are in scope here (gap fixes belong to #4890).

## Non-goals

- Fixing any protocol/type-fidelity gap (temporal/Duration/Point native
  structures, `Neo.TransientError.*`, HA-aware ROUTE, Bolt 5.x) - those are #4890.
- A YAML-driven test generator. Per the spec README, every scenario is a
  hand-written JUnit test; `spec.yaml` is reference only.
- The full driver-version CI matrix and nightly scheduling - that is #4891.

## Architecture: two certification layers

The issue explicitly invites coverage "directly in `bolt/src/test/java` ...
rather than only at the e2e layer." Java therefore certifies across two layers,
both counted toward the Java column:

1. **e2e layer** - `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java`:
   driver-visible, black-box, against the published `arcadedata/arcadedb:latest`
   testcontainer (the existing `ArcadeContainerTemplate`). This is where the
   bulk of the 39 scenarios live, mirroring the JS suite one-to-one.
2. **bolt-module layer** - `bolt/src/test/java/com/arcadedb/bolt/`: narrower
   ITs/unit tests for scenarios the shared e2e container cannot express
   (TLS modes, raw-socket auth ordering) or that are better asserted at the
   wire level (type serialization). Reuses existing embedded-server harnesses.

### Test organization

`RemoteBoltDatabaseIT` gets `@Nested` classes, one per feature-matrix area
(`Connection`, `Auth`, `Transactions`, `CausalConsistency`, `MultiDatabase`,
`ResultHandling`, `TypeRoundTrip`, `Errors`, `Protocol`). The existing 4 tests
are preserved (folded into the relevant `@Nested` class, keeping their method
bodies), satisfying AC "Existing 4 tests preserved."

### Traceability

Every test carries the scenario id in a `@DisplayName`, per the spec README:
`@DisplayName("[TYPE-011] Duration round-trips as a native Bolt Duration structure")`.
Coverage is then grep-checkable and the #4892 aggregator can key off the id.

## Expected-fail mechanism

JUnit 5 has no `it.failing`. We add a tiny helper mirroring the JS semantics:

```java
// Passes while the known gap reproduces; FAILS loudly the day the gap closes,
// forcing the assertion to be flipped to a real one and spec.yaml updated.
static void assertExpectedFailure(String trackingIssue, ThrowingCallable idealAssertion) {
  try {
    idealAssertion.call();
  } catch (Throwable expected) {
    return; // gap still present - certified as a documented limitation
  }
  fail("Expected-fail scenario now PASSES - close it: flip the assertion and "
     + "update bolt/conformance/spec.yaml current_status (" + trackingIssue + ")");
}
```

- **xfail (via `assertExpectedFailure`)** for driver-visible gaps that reproduce
  through the driver: TYPE-003 (Path), TYPE-011 (Duration), TYPE-012 (Point),
  RESULT-004 (counters), ERR-002 (semantic error code), PROTO-002 (Bolt 5.x),
  and any scenario the first real-image run proves still gapped.
- **`@Disabled("[ID] reason - #4890")`** only for scenarios that cannot be
  driven at all from the e2e layer: CONN-004 (needs a real 3-node HA cluster)
  and ERR-003 (driver never sends RUN before LOGON; requires a raw socket).
  ERR-003 is instead covered at the bolt-module layer if a raw-socket harness
  already exists (`BoltProtocolIT`); otherwise it stays `@Disabled` with a note.

Rationale: this matches JS `it.failing`/`it.skip` semantics exactly, so the two
columns are comparable. `current_status` in `spec.yaml` remains the source of
truth for the matrix; the test representation just reproduces the gap so it
can't rot.

## Verdict reconciliation (critical)

Per the spec README, `current_status` is "a starting hint, not a verdict."
`unverified` scenarios (TX-004, CAUSAL-001, MDB-002, RESULT-002, RESULT-004,
PROTO-003) and even some `expected-fail` ones must be settled by running against
the real `:latest` image. The JS run already discovered surprises worth
mirroring and re-confirming for Java:

- TYPE-007..010 (LocalDate/LocalTime/LocalDateTime/OffsetDateTime) were spec'd
  `expected-fail` but the JS suite asserts them with plain `it()` - i.e. the
  published image returns native temporals. Java must re-verify and assert
  positively if confirmed.
- TX-005/ERR-004 (`Neo.TransientError.*` on write-write race): JS asserts these
  positively. Java re-verifies via a two-writer race helper.
- ERR-002 (semantic error code) was spec'd `passing` but JS found it a gap
  (`it.failing`). Java re-verifies.

Process: implement each scenario asserting the spec's hinted verdict; run
against the real image (`mvn -pl e2e verify` / the failsafe IT); where the
observed verdict differs from `spec.yaml`, flip the test representation AND
update `bolt/conformance/spec.yaml` (`current_status`, and
`known_limitation`/`tracking_issue` if it becomes a gap) in this same PR. This
keeps the shared spec honest and is explicitly sanctioned by the README.

## Fixtures and seeding

- **beer**: already imported at container boot by `ArcadeContainerTemplate`.
- **type_matrix**: `bolt/conformance/fixtures/type-matrix.cypher`, seeded once
  in `@BeforeAll` via HTTP `POST /api/v1/command/beer` (language cypher) - never
  over Bolt, since Bolt serialization is the thing under test. The e2e module
  has no HTTP helper today, so a minimal `httpCommand` helper is added
  (HttpURLConnection, root basic auth), mirroring the JS `httpCommand`.
- **boltscratch** second database (for MDB-002 isolation) created once in
  `@BeforeAll` via HTTP `POST /api/v1/server` `{"command":"create database boltscratch"}`.

The fixture file is read from the repo tree. `RemoteBoltDatabaseIT` resolves it
relative to the module (the `e2e` module can reach `../bolt/conformance/...`);
if that path coupling is undesirable the fixture's few `CREATE` lines are
inlined as a constant. Decision: resolve from the repo path, with a clear
failure message if absent (same posture as JS `TYPE_MATRIX_FIXTURE`).

## TLS scenarios (CONN-002, CONN-005) - bolt-module layer

The shared e2e container runs TLS DISABLED, single Bolt port. Rather than
duplicate the JS derived-image + keytool machinery in `e2e`, we reuse the
existing `bolt/src/test/java/com/arcadedb/bolt/BoltTlsIT` (embedded server +
real `neo4j-java-driver` over TLS, already proving REQUIRED mode):

- Tag the existing REQUIRED-mode test with `[CONN-002]` in its `@DisplayName`.
- Add a `[CONN-005]` test asserting a plaintext `bolt://` client connects when
  `NETWORK_SSL` mode is OPTIONAL (mirror the existing setup with mode OPTIONAL).

This is DRY, uses a harness Java already owns, and keeps CONN-002/005 real.

## Protocol negotiation (PROTO-001) - reuse

PROTO-001 (4.4/4.0/3.0 negotiation) is already unit-covered by
`BoltVersionNegotiationTest`; the e2e `RemoteBoltDatabaseIT` adds a driver-level
assertion (the pinned driver negotiates 4.4 and runs), and the module test is
tagged `[PROTO-001]`. PROTO-002 (5.x) is xfail at the e2e layer.

## Type serialization unit coverage - `BoltTypeRoundTripTest`

New `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest`, mirroring the
existing `BoltStructureTest`/`PackStreamTest` pattern, asserting what
`BoltStructureMapper.toPackStreamValue` / `PackStreamWriter` actually emit for
LocalDate/LocalTime/LocalDateTime/OffsetDateTime/Duration/Point at the wire
level. This pins the current serialization contract (native vs ISO-string
fallback) at the module level, so a #4890 fix is caught precisely where it
lands, independent of which image the e2e layer pulls. Gap cases use the same
`assertExpectedFailure` posture (or characterization with an explicit gap
`@DisplayName`).

## Driver-version range (AC4) - baseline + opt-in 4.4 profile

- **Default**: full suite on the repo baseline `neo4j-java-driver` 6.2.0
  (`${neo4j-driver.version}`), resolving the spec's `latest-6.x` band.
- **`-Pbolt-driver-legacy`**: resolves the `oldest-supported-4.x` band to the
  latest 4.4.x line. Because 4.4 lacks 5.x+ API (`executeWrite`, `lastBookmarks`
  plural, etc.), this profile does NOT recompile the full suite. Instead:
  - `maven-compiler-plugin` `<testExcludes>` (in the profile) excludes the
    6.x-API-only ITs from compilation.
  - A dedicated `RemoteBoltLegacyDriverIT` written against the cross-version
    stable API subset (`GraphDatabase.driver`, `verifyConnectivity`,
    `session.run`, parameterized read, error-code assertion, 4.4 negotiation)
    compiles and runs under both driver majors.
  - `failsafe` under the profile runs only `RemoteBoltLegacyDriverIT`.

This concretely satisfies AC4 ("at least via a second profile/dependency
classifier if full matrix testing isn't practical in one module") without
fighting API drift. The full driver-version matrix and its scheduling remain
#4891. Concrete versions are recorded in `e2e/pom.xml` and noted back into
`spec.yaml`'s `driver_version_bands.java.resolution_note`.

## Scenario placement summary

| Scenario | Layer | Representation |
|---|---|---|
| CONN-001, CONN-003 | e2e | it |
| CONN-002, CONN-005 | bolt-module `BoltTlsIT` | it |
| CONN-004 | e2e | @Disabled (needs 3-node HA) |
| AUTH-001..003 | e2e | it |
| TX-001..004 | e2e | it |
| TX-005 | e2e | it or xfail (real-image verdict) |
| CAUSAL-001 | e2e | it |
| MDB-001 | e2e | it |
| MDB-002 | e2e | it (needs boltscratch db) |
| RESULT-001..003 | e2e | it |
| RESULT-004 | e2e | xfail |
| TYPE-001,002,004,005,006 | e2e | it |
| TYPE-003 | e2e | xfail (Path) |
| TYPE-007..010 | e2e | it (real-image verdict; JS passes) |
| TYPE-011, TYPE-012 | e2e | xfail (Duration, Point) |
| TYPE (wire-level) | bolt-module `BoltTypeRoundTripTest` | it + xfail |
| ERR-001 | e2e | it |
| ERR-002 | e2e | xfail (real-image verdict; JS gap) |
| ERR-003 | bolt-module raw or @Disabled | it / @Disabled |
| ERR-004 | e2e | it or xfail (real-image verdict) |
| PROTO-001 | e2e + `BoltVersionNegotiationTest` | it |
| PROTO-002 | e2e | xfail (Bolt 5.x) |
| PROTO-003 | e2e | it |

## Verification

- `mvn -q -pl bolt -am install -DskipTests` then bolt-module tests:
  `mvn -q -pl bolt test` (BoltTlsIT, BoltTypeRoundTripTest, negotiation).
- e2e ITs (Docker required): `mvn -q -pl e2e -am verify -DskipITs=false`
  running `RemoteBoltDatabaseIT` against `arcadedata/arcadedb:latest`.
- Legacy band: `mvn -q -pl e2e verify -Pbolt-driver-legacy -DskipITs=false`.
- Re-run `bolt/conformance/validate_spec.py` after any `spec.yaml` edit made
  during verdict reconciliation.
- Confirm all 39 ids appear in test `@DisplayName`s (grep) so coverage is
  complete and comparable to JS/Go.

## Files touched

- `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltDatabaseIT.java` (expanded)
- `e2e/src/test/java/com/arcadedb/e2e/RemoteBoltLegacyDriverIT.java` (new)
- `e2e/pom.xml` (`bolt-driver-legacy` profile)
- `bolt/src/test/java/com/arcadedb/bolt/BoltTlsIT.java` (add CONN-005, tag CONN-002)
- `bolt/src/test/java/com/arcadedb/bolt/BoltTypeRoundTripTest.java` (new)
- `bolt/conformance/spec.yaml` (verdict reconciliation, java band resolution) - only if the real-image run changes a status
- `docs/superpowers/specs/2026-07-05-bolt-java-conformance-design.md` (this file)
- `docs/superpowers/plans/2026-07-05-bolt-java-conformance.md` (plan, next step)
