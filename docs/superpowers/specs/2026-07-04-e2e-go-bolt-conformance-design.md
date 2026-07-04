# e2e-go Bolt conformance suite - design (issue #4887)

Part of epic #4882 (Bolt Driver Compatibility Certification), Group B.
Depends on the shared conformance spec `bolt/conformance/spec.yaml` (#4883,
merged to main). Sibling references, both merged to main: `e2e-python`
(#4885) and `e2e-csharp` (#4886).

## Problem

Go is one of the five official Neo4j driver languages and is the only one
with zero footprint in ArcadeDB today: no `e2e-go` directory, no `go.mod`,
no `neo4j-go-driver` reference anywhere in the repo. Every other language
(Java, JS, Python, C#) now has a Bolt conformance suite driven by the shared
spec. This issue closes that gap by building the module from scratch and
wiring a PR-gating CI job.

## Governing constraints (from the epic)

- **Certify depth, not presence.** Every one of the 39 `spec.yaml` scenarios
  is implemented as a discrete, traceable test - a "not supported" outcome is
  a valid, complete answer only when it is asserted as a known gap, never
  silently absent.
- **Official driver only.** `github.com/neo4j/neo4j-go-driver/v5`. No mocks,
  no bespoke Bolt clients.
- **One shared spec, polyglot code.** No new multi-language Bolt module; this
  is an idiomatic `go test` suite driven by the same `spec.yaml` the other
  four suites consume by hand.
- **Mirror the established pattern.** Match e2e-csharp/e2e-python structure,
  fixture strategy, seeding approach, and gap-handling so results are
  comparable across languages.

## Scope

**In scope (issue #4887 acceptance criteria):**
1. New standalone `e2e-go/` module: `go.mod`, testcontainers-go fixture,
   `neo4j-go-driver/v5` dependency.
2. Full coverage of all 39 `spec.yaml` scenarios matching the Python/C#
   suites, with identical `current_status` semantics.
3. New `go-e2e-tests` CI job in `.github/workflows/mvn-test.yml`, PR-gating,
   mirroring the `csharp-e2e-tests`/`python-e2e-tests` job shape.
4. Known-gap and infra-unavailable scenarios documented/asserted, never
   silently absent.

**Out of scope (deferred, owned by other issues):**
- Multi-version driver-band matrix + nightly scheduled run -> #4891.
- Published compatibility matrix / badge -> #4892.
- Any server-side protocol/type-fidelity fix (temporal/Duration/Point,
  `Neo.TransientError.*`, HA-aware ROUTE, Bolt 5.x) -> #4890. This suite only
  *observes and documents* those gaps; it does not fix them.
- 3-node HA cluster orchestration (CONN-004) - skipped here as in the
  Python/C# suites.

## Architecture

Standalone Go module, NOT part of the Maven reactor (identical to how
`e2e-python`, `e2e-js`, `e2e-csharp` sit outside `<modules>` in the root
`pom.xml`). It is invoked directly by its own CI job via `go test`.

```
e2e-go/
  go.mod                      # module + pinned neo4j-go-driver/v5, testcontainers-go, testify
  go.sum
  README.md                   # driver-version bands, run instructions, traceability convention
  fixtures_test.go            # TestMain, shared plain container, HTTP seeding, repo-root + image resolution
  tls_test.go                 # keytool cert-gen, derived-image build, TLS-required/optional containers (lazy)
  knowngap_test.go            # assertStillFails() strict-xfail helper
  bolt_conformance_test.go    # 39 Test_<AREA>_<NNN>_<slug> functions
```

Go has no package-scoped fixtures like pytest, so container lifecycle is
managed in `TestMain(m *testing.M)`:
- Start the shared plain container, wait on `GET /api/v1/ready` (204), create
  the `boltscratch` database and seed `type-matrix.cypher` - both over HTTP,
  never over Bolt (seeding must not exercise the serialization path under
  test), exactly as the Python/C# fixtures do.
- Run `m.Run()`.
- Tear down the plain container and any lazily-started TLS containers.

TLS containers (needed only by CONN-002 and CONN-005) are started lazily
behind a `sync.Once`, cleaned up in `TestMain` after `m.Run()`. This mirrors
the pytest module-scoped TLS fixtures and avoids paying TLS startup cost for
the ~37 non-TLS scenarios.

### Container image and fixture contract

- Image resolution honors `ARCADEDB_DOCKER_IMAGE` with a
  `arcadedata/arcadedb:latest` default (same as `ArcadeDbFixture.cs:40-41`).
  In CI the branch-built image is `docker load`ed under that tag; the env var
  lets the CI job pass the exact `build-and-package` output tag and avoids the
  split-suite trap where the plain container uses the branch image but the TLS
  derived image is built `FROM arcadedata/arcadedb:latest`.
- Base `JAVA_OPTS` (identical to the Python fixture):
  ```
  -Darcadedb.server.rootPassword=playwithdata
  -Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}
  -Darcadedb.server.plugins=BoltProtocolPlugin
  ```
- Exposed ports: 2480 (HTTP, for readiness + seeding) and 7687 (Bolt).
- The type-matrix fixture is read from `../bolt/conformance/fixtures/type-matrix.cypher`;
  repo root is resolved by walking up from the test file's directory until a
  `bolt/conformance` directory is found (Go analog of C#'s `FindRepoRoot()`).

### TLS strategy (CONN-002, CONN-005)

Direct port of the C#/Python approach:
1. Generate a throwaway self-signed keystore (`keystore.p12`) + truststore
   (`truststore.jks`) with the JDK `keytool` binary (present on GitHub
   `ubuntu-latest` runners; the test skips with a clear message if `keytool`
   is absent locally).
2. Build a derived image (`FROM <base image>`, `COPY` the certs into
   `/home/arcadedb/tls_certs/`) via testcontainers-go's `FromDockerfile` so
   the certs resolve inside the container regardless of host/CI bind-mount
   quirks.
3. TLS-required container: `-Darcadedb.bolt.ssl=REQUIRED` + keystore/truststore
   sysprops. Driver connects with the `bolt+ssc://` scheme (self-signed, no CA
   verification).
4. TLS-optional container: `-Darcadedb.bolt.ssl=OPTIONAL`; a plaintext
   `bolt://` driver connects successfully.

### Strict-xfail: the Go equivalent of `KnownGapAssertions`

Go's `testing` package has no `xfail(strict=True)`. We reproduce the exact
property with a helper whose body asserts the *target-correct* (Neo4j)
behavior and returns an error when that behavior is absent:

```go
// assertStillFails runs body, which must return a non-nil error while the
// known gap still reproduces (driver error, or a value that does not match
// the Neo4j-correct expectation). If body returns nil the gap has been
// fixed: fail loudly so whoever fixed it converts this to a normal test and
// updates current_status in bolt/conformance/spec.yaml in the same PR.
func assertStillFails(t *testing.T, reason string, body func() error) {
    t.Helper()
    if err := body(); err != nil {
        t.Logf("known gap still reproduces (expected): %v", err)
        return
    }
    t.Fatalf("XPASS: known gap no longer reproduces - convert to a normal "+
        "test and update current_status in bolt/conformance/spec.yaml. Gap: %s", reason)
}
```

Normal (passing) scenarios use `testify/require` directly - idiomatic Go, no
wrapper. Infra-unavailable scenarios call `t.Skip(reason)` with the same
justification text the Python/C# suites use.

## Scenario mapping (all 39)

`current_status` values are taken verbatim from `spec.yaml`; the Go
implementation must reproduce the same outcome the Python suite does.

| Scenario | status in spec | Go handling |
|---|---|---|
| CONN-001 connect bolt:// | passing | `require`, `VerifyConnectivity` |
| CONN-002 bolt+ssc TLS required | passing | TLS-required container, `bolt+ssc://` |
| CONN-003 neo4j:// routing single-node | passing | `require`, `neo4j://` scheme |
| CONN-004 neo4j:// HA topology | expected-fail | `t.Skip` (needs 3-node cluster) |
| CONN-005 TLS optional plaintext | passing | TLS-optional container, `bolt://` |
| AUTH-001 basic auth valid | passing | `require` |
| AUTH-002 basic auth invalid | passing | expect `Neo.ClientError.Security.Unauthorized` |
| AUTH-003 auth none rejected | passing | expect auth error (intentional rejection) |
| TX-001 autocommit | passing | `require`, 5 rows |
| TX-002 explicit commit persists | passing | `require` |
| TX-003 explicit rollback discards | passing | `require` |
| TX-004 managed executeWrite commits | unverified | assert passing via `ExecuteWrite` |
| TX-005 managed retry on transient | passing | two-writer race, expect `TransientError` |
| CAUSAL-001 bookmark read-after-write | unverified | assert passing via `LastBookmarks` |
| MDB-001 session db selection | passing | `require` |
| MDB-002 cross-db isolation | unverified | assert passing (boltscratch vs beer) |
| RESULT-001 streaming pull | passing | `require`, iterate 10 |
| RESULT-002 partial pull then continue | unverified | `FetchSize: 2`, assert passing |
| RESULT-003 discard abandons remaining | passing | `require`, `Consume` |
| RESULT-004 summary counters reflect writes | expected-fail | `assertStillFails` (empty counters) |
| TYPE-001 node roundtrip | passing | `dbtype.Node` |
| TYPE-002 relationship roundtrip | passing | `dbtype.Relationship` |
| TYPE-003 path roundtrip | expected-fail | `assertStillFails` (no native Path) |
| TYPE-004 bytearray param roundtrip | passing | `[]byte` param echo |
| TYPE-005 nested list/map roundtrip | passing | `require` |
| TYPE-006 null roundtrip | passing | `require`, nil |
| TYPE-007 local date roundtrip | passing | `dbtype.Date` |
| TYPE-008 local time roundtrip | passing | `dbtype.LocalTime`/`Time` |
| TYPE-009 local datetime roundtrip | passing | `dbtype.LocalDateTime` |
| TYPE-010 offset datetime roundtrip | passing | `time.Time` with +02:00 offset |
| TYPE-011 duration roundtrip | expected-fail | `assertStillFails` (comes back as string) |
| TYPE-012 point roundtrip | expected-fail | `assertStillFails` (no Point handling) |
| ERR-001 syntax error | passing | expect `Neo.ClientError.Statement.SyntaxError` |
| ERR-002 semantic error | expected-fail | `assertStillFails` (mapped as SyntaxError) |
| ERR-003 unauthenticated request rejected | not-applicable | `t.Skip` (needs raw socket) |
| ERR-004 transient condition code | passing | two-writer race, expect `TransientError` |
| PROTO-001 negotiation succeeds | passing | `require` |
| PROTO-002 Bolt 5.x negotiation documented | expected-fail | `assertStillFails` (never advertises 5.x) |
| PROTO-003 reset mid-stream | passing | `FetchSize: 2`, consume then re-run |

Notes on temporal type mapping: the neo4j-go-driver maps Cypher temporals to
`dbtype.Date`, `dbtype.LocalTime`, `dbtype.Time`, `dbtype.LocalDateTime`, and
`time.Time` (zoned). The exact `dbtype` used per scenario is confirmed against
the driver during implementation (TDD); the table above states intent, and
the assertion checks the driver-returned concrete type, not a hardcoded guess.

## Driver-version bands

`spec.yaml` assigns Go the bands `[lts, current, latest]`, "resolved by #4887".
The Go driver line is `neo4j-go-driver/v5` (there is no published v6 Go
module). For PR-gating this suite pins a single concrete `v5` version (the
latest v5 patch at implementation time), recorded in `go.mod` and documented
in `e2e-go/README.md`. Exercising all three bands on a schedule is #4891's
responsibility; this issue must not front-run that (neither Python nor C#
did).

## CI job

New `go-e2e-tests` job in `.github/workflows/mvn-test.yml`, modeled on
`csharp-e2e-tests`:
- `runs-on: ubuntu-latest`, `needs: build-and-package`.
- `actions/setup-go` with a pinned Go toolchain version.
- Restore + `docker load` the cached branch image (same cache key the other
  e2e jobs use).
- `working-directory: e2e-go`, run `go test ./... -v`.
- `env: ARCADEDB_DOCKER_IMAGE: ${{ needs.build-and-package.outputs.image-tag }}`.

JUnit/aggregation output is intentionally NOT added here - the Python/C#/JS
jobs don't emit it yet either, and comparable-report aggregation is #4891's
scope. Adding it now would front-run that issue.

## Testing / verification approach

TDD is awkward for a from-scratch e2e module (the container must exist before
any test can run), so the practical sequence is:
1. Scaffold `go.mod` + a single `Test_CONN_001` + the `TestMain` fixture;
   get one green test against the locally-built branch image first. This
   proves the whole harness (container boot, seeding, driver connect) before
   fanning out.
2. Add scenarios area by area (connection -> auth -> transactions -> ... ->
   protocol), each verified against the real container as it is written.
3. Verify the strict-xfail scenarios genuinely reproduce their gap (the test
   passes because `body` errors) - and sanity-check that flipping one to
   expect success makes it fail, proving the XPASS guard works.
4. Full-suite run against a locally-built image
   (`mvn -Pdocker -DskipTests -pl bolt,package -am` to produce
   `arcadedata/arcadedb:latest`, per the "debug against the real image"
   practice) before opening the PR.
5. CI runs the job against the branch-built image; all cells are green
   (passing scenarios pass, expected-fail scenarios reproduce their gap,
   skipped scenarios skip).

No unit tests - this is an integration/e2e suite by nature, matching the
sibling modules.

## Risks / open points

- **Go driver temporal `dbtype` specifics**: resolved empirically during TDD
  against the running container; the design does not hardcode them.
- **keytool availability**: present on `ubuntu-latest`; local runs without a
  JDK skip the two TLS scenarios with a clear message (same as Python).
- **testcontainers-go image build (`FromDockerfile`)**: used for the TLS
  derived image; if it proves flaky in CI, the fallback is a bind mount, but
  the C#/Python COPY-into-derived-image approach is the proven path and is
  preferred.
- **Two-writer race (TX-005, ERR-004)**: inherently timing-sensitive; the
  assertion checks the whole error set (not a single index) for a
  `TransientError`, matching the Python suite's hardening.
```
