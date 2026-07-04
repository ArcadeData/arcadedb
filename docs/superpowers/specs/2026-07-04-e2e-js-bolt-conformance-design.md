# e2e-js Bolt conformance suite - design (issue #4888)

Part of epic #4882 (Bolt Driver Compatibility Certification), Group B.
Depends on the shared conformance spec `bolt/conformance/spec.yaml` (#4883,
merged to main). Sibling references, all merged to main: `e2e-python` (#4885),
`e2e-csharp` (#4886), `e2e-go` (#4887). The Python suite
(`e2e-python/tests/test_bolt.py`) is the authoritative template this design
mirrors.

## Problem

JavaScript is one of the five official Neo4j driver languages. `e2e-js` already
has a Bolt test file (`src/js-bolt-e2e.test.js`), but it covers only connect +
WHERE-filter reads + one variable-length-path regression test (issues
#4452/#4271). No transactions, error paths, result-handling, or type-round-trip
coverage exists. Every other language now has a full conformance suite driven by
the shared spec; JS is the last Group B gap. This issue deepens `e2e-js` to all
39 `spec.yaml` scenarios, at parity with the merged siblings, without disturbing
the existing regression test.

## Governing constraints (from the epic)

- **Certify depth, not presence.** Every one of the 39 `spec.yaml` scenarios is
  implemented as a discrete, traceable test. A "not supported" outcome is a
  valid, complete answer only when asserted as a known gap, never silently
  absent.
- **Official driver only.** `neo4j-driver` (npm), the driver already declared in
  `e2e-js/package.json` (`^6.0.1`). No mocks, no bespoke Bolt clients.
- **One shared spec, polyglot code.** No new multi-language Bolt module; this is
  an idiomatic Jest suite driven by the same `spec.yaml` the other four suites
  consume by hand.
- **Mirror the established pattern.** Match the merged e2e-python/e2e-go/
  e2e-csharp structure, fixture strategy, seeding approach, gap-handling, and
  `current_status` semantics so results are comparable across languages.
- **Advertised identity is fixed.** The server reports "Neo4j/5.26.0
  compatible"; the suite depends on `neo4j-driver` continuing to negotiate
  Bolt 4.4 (silent downgrade from its native 5.x), exactly as the other four
  suites do. A future driver major that drops legacy negotiation would break
  the whole suite, not just PROTO-002.

## Scope

**In scope (issue #4888 acceptance criteria):**
1. New `e2e-js/src/js-bolt-conformance.test.js`, implementing all 39 scenarios,
   with `current_status` outcomes identical to `main`'s reconciled `spec.yaml`.
2. Beer-dataset + `type-matrix.cypher` fixture strategy, seeded over HTTP (never
   over Bolt), matching the Python/Go/C# fixtures.
3. TLS scenarios (CONN-002, CONN-005) via a keytool-generated self-signed
   keystore/truststore baked into a derived Docker image built at test time.
4. Existing `js-bolt-e2e.test.js` VLP regression test preserved byte-for-byte.
5. Known-gap and infra-unavailable scenarios asserted/skipped with a reason and
   a `#4890` reference, never silently absent.
6. `jest-junit` output confirmed produced for the new suite (already wired in
   `package.json`).

**Out of scope (deferred, owned by other issues):**
- Multi-version driver-band matrix + nightly scheduled run -> #4891.
- JUnit-report aggregation / published compatibility matrix / badge -> #4891,
  #4892. This suite emits `jest-junit.xml` (already configured) but does not add
  cross-suite aggregation.
- Any server-side protocol/type-fidelity fix (native Path, Duration, Point,
  `Neo.TransientError.*` beyond what already works, HA-aware ROUTE, Bolt 5.x)
  -> #4890. This suite only *observes and documents* those gaps.
- 3-node HA cluster orchestration (CONN-004) - skipped here as in the Python/
  Go/C# suites.

## Decisions taken (were open questions; defaulted while user was away)

- **Full mirror including TLS.** CONN-002/CONN-005 are implemented, not skipped.
  Parity/comparability across all five languages is the epic's core aim;
  skipping TLS in JS only would leave two cells uncertified relative to the
  other four suites. Risk (derived-image build in testcontainers-js) is
  mitigated below.
- **Existing regression file left fully untouched.** The new conformance file
  honors `ARCADEDB_DOCKER_IMAGE`; `js-bolt-e2e.test.js` keeps its hardcoded
  `arcadedata/arcadedb:latest`, honoring the issue's "preserved unchanged"
  requirement. (In CI the branch image is `docker load`ed and the env var is
  passed to the new file; the old file already works against the loaded tag.)

## Architecture

A second Jest test file in the existing module - no new module, no `pom.xml`
change (`e2e-js` already sits outside the Maven reactor). Jest runs with
`--runInBand` (see `package.json` test script), so the two files' containers
never run concurrently; each file owns its own container lifecycle.

```
e2e-js/
  package.json                     # unchanged deps (neo4j-driver ^6.0.1,
                                   #   testcontainers ^12, jest ^30, jest-junit ^17)
  src/
    js-bolt-e2e.test.js            # UNCHANGED - VLP regression (#4452/#4271)
    js-pg-e2e.test.js              # UNCHANGED - Postgres wire
    js-bolt-conformance.test.js    # NEW - all 39 spec.yaml scenarios
```

All 39 scenarios live in one new file, organized with a `describe()` per spec
area (connection, auth, transactions, causal-consistency, multi-database,
result-handling, type-roundtrip, errors, protocol) and one `it()`/`test()` per
scenario. Per the README traceability convention, every test name is prefixed
with the bracketed scenario id, e.g. `test('[TYPE-011] duration round-trip',
...)`, so `grep -o '\[[A-Z]*-[0-9]*\]'` over the file proves 39/39 coverage.

### Fixture strategy (mirrors the Python fixture exactly)

- **Plain container** (used by ~35 scenarios): `GenericContainer` on the
  resolved image, exposing 2480 (HTTP) + 7687 (Bolt), with base `JAVA_OPTS`:
  ```
  -Darcadedb.server.rootPassword=playwithdata
  -Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}
  -Darcadedb.server.plugins=BoltProtocolPlugin
  ```
  Boot waits on `GET /api/v1/ready` -> 204 (`Wait.forHttp`, already used by the
  existing file). Then, over HTTP only: `create database boltscratch` and seed
  `type-matrix.cypher` into `beer` via `POST /api/v1/command/beer`
  (`{language:"cypher", command:<file>}`). Seeding never touches Bolt so it does
  not exercise the serialization path under test.
- **Image resolution:** `process.env.ARCADEDB_DOCKER_IMAGE || "arcadedata/arcadedb:latest"`,
  matching `ArcadeDbFixture.cs` / the Python fixture. In CI the branch-built
  image tag is passed via that env var (the `js-e2e-tests` job already sets it).
- **type-matrix fixture path:** read from
  `path.resolve(__dirname, "../../bolt/conformance/fixtures/type-matrix.cypher")`
  (`e2e-js/src` -> repo root). A one-time `fs.existsSync` guard fails with a
  clear message if the path is wrong, rather than seeding an empty command.
- **Lifecycle:** container start + seeding in `beforeAll`; a single shared
  `driver` (basic auth root/playwithdata) created in `beforeAll` and closed in
  `afterAll`; container stopped in `afterAll`. `jest.setTimeout(180000)` to
  absorb the beer import + optional TLS image build (Python uses 60-120s waits).
- **Write-collision avoidance:** write-emitting scenarios (TX-002/004,
  CAUSAL-001, MDB-002, RESULT-004) use unique marker property values rather than
  isolated databases and assert on marker-scoped counts, never on absolute
  row/node totals - identical to the Python suite's rationale.

### TLS strategy (CONN-002, CONN-005) - direct port of the Python fixture

1. Generate a throwaway self-signed `keystore.p12` + `truststore.jks` with the
   JDK `keytool` binary (present on `ubuntu-latest`; the two TLS tests skip with
   a clear message if `keytool` is absent locally, matching Python).
2. Build a derived image (`FROM <resolved base image>`, `COPY` the certs into
   `/home/arcadedb/tls_certs/`) via testcontainers-js
   `GenericContainer.fromDockerfile(contextDir).build(tag)`. A `docker build`
   COPY resolves its own build context regardless of host/CI bind-mount quirks -
   the Python fixture documents that GitHub runners have started containers with
   an empty bind-mounted cert dir, so baking the certs in is the proven path.
3. **CONN-002 (REQUIRED):** container with
   `-Darcadedb.bolt.ssl=REQUIRED` + `arcadedb.ssl.keyStore/.trustStore` sysprops;
   driver connects with the `bolt+ssc://` scheme (self-signed, no CA
   verification) and runs a beer query.
4. **CONN-005 (OPTIONAL):** container with `-Darcadedb.bolt.ssl=OPTIONAL`; a
   plaintext `bolt://` driver connects successfully.

TLS containers are started lazily inside the two TLS tests (or a dedicated
nested `describe` with its own `beforeAll`/`afterAll`) so the ~35 non-TLS
scenarios never pay TLS startup cost, and are torn down after. The derived image
is removed in `afterAll`.

### Strict-xfail: the Jest equivalent of `xfail(strict=True)`

Jest ships this natively: **`test.failing()` / `it.failing()`** inverts the
result - the test passes while its body throws (or an assertion fails), and
**fails loudly if the body unexpectedly passes** (XPASS). That is exactly
pytest's `xfail(strict=True)` and Go's `assertStillFails`, with no helper
needed. Each known-gap test is written to assert the *Neo4j-correct* behavior
(the behavior the gap currently prevents), wrapped in `it.failing(...)`, with a
comment naming the gap and `#4890`. When the server gap is fixed the body starts
passing, `it.failing` flips red, and whoever fixed it converts it to a normal
`it()` and updates `current_status` in `spec.yaml` in the same PR.

Passing scenarios use plain `it()` with `expect(...)`. Infra-unavailable
scenarios use `it.skip()` (or `it()` early-returning after a logged skip
reason for the `keytool`-absent case), carrying the same justification text the
Python/Go suites use.

## Scenario mapping (all 39) - target = `main`'s reconciled `spec.yaml`

`current_status` values are taken verbatim from `main`'s `spec.yaml` (already
reconciled by the merged Python/Go/C# PRs). The JS suite must reproduce the same
outcome; where the `neo4j-driver` JS type mapping differs from Python's, the
assertion checks the driver-returned concrete type, not a hardcoded guess (see
notes).

| Scenario | status | Jest handling |
|---|---|---|
| CONN-001 connect bolt:// | passing | `it`, `verifyConnectivity()` |
| CONN-002 bolt+ssc TLS required | passing | TLS-required derived image, `bolt+ssc://` |
| CONN-003 neo4j:// routing single-node | passing | `it`, `neo4j://` scheme |
| CONN-004 neo4j:// HA topology | expected-fail | `it.skip` (needs 3-node cluster, #4890) |
| CONN-005 TLS optional plaintext | passing | TLS-optional derived image, `bolt://` |
| AUTH-001 basic auth valid | passing | `it` |
| AUTH-002 basic auth invalid | passing | expect code `Neo.ClientError.Security.Unauthorized` |
| AUTH-003 auth none rejected | passing | expect auth error (intentional rejection) |
| TX-001 autocommit | passing | `it`, 5 rows |
| TX-002 explicit commit persists | passing | `beginTransaction`/`commit`, marker count |
| TX-003 explicit rollback discards | passing | `beginTransaction`/`rollback`, marker count 0 |
| TX-004 managed executeWrite commits | unverified | assert passing via `executeWrite` |
| TX-005 managed retry on transient | passing | two-writer race, expect `TransientError` |
| CAUSAL-001 bookmark read-after-write | unverified | assert passing via `lastBookmarks()` |
| MDB-001 session db selection | passing | `it` |
| MDB-002 cross-db isolation | unverified | assert passing (boltscratch vs beer) |
| RESULT-001 streaming pull | passing | `it`, iterate 10 |
| RESULT-002 partial pull then continue | unverified | `fetchSize: 2`, assert passing |
| RESULT-003 discard abandons remaining | passing | `it`, `result.consume()` |
| RESULT-004 summary counters reflect writes | expected-fail | `it.failing` (empty counters) |
| TYPE-001 node roundtrip | passing | `neo4j.types.Node` |
| TYPE-002 relationship roundtrip | passing | `neo4j.types.Relationship` |
| TYPE-003 path roundtrip | expected-fail | `it.failing` (no native Path) |
| TYPE-004 bytearray param roundtrip | passing | byte-array param echo (concrete type confirmed in TDD) |
| TYPE-005 nested list/map roundtrip | passing | `it`, deep-equal |
| TYPE-006 null roundtrip | passing | `it`, null prop + null param |
| TYPE-007 local date roundtrip | passing | `neo4j.types.Date` |
| TYPE-008 local time roundtrip | passing | `neo4j.types.Time`/`LocalTime` |
| TYPE-009 local datetime roundtrip | passing | `neo4j.types.LocalDateTime` |
| TYPE-010 offset datetime roundtrip | passing | `neo4j.types.DateTime` w/ +02:00 offset |
| TYPE-011 duration roundtrip | expected-fail | `it.failing` (comes back as string) |
| TYPE-012 point roundtrip | expected-fail | `it.failing` (no Point handling) |
| ERR-001 syntax error | passing | expect `Neo.ClientError.Statement.SyntaxError` |
| ERR-002 semantic error | expected-fail | `it.failing` (mapped as SyntaxError) |
| ERR-003 unauthenticated request rejected | not-applicable | `it.skip` (needs raw socket) |
| ERR-004 transient condition code | passing | two-writer race, expect `TransientError` |
| PROTO-001 negotiation succeeds | passing | `it` |
| PROTO-002 Bolt 5.x negotiation documented | expected-fail | `it.failing` (never advertises 5.x) |
| PROTO-003 reset mid-stream | unverified | `fetchSize: 2`, consume then re-run |

Expected JS tally: **31 passing, 6 `it.failing` gaps** (RESULT-004, TYPE-003,
TYPE-011, TYPE-012, ERR-002, PROTO-002), **2 skips** (CONN-004, ERR-003). Exact
counts confirmed during TDD against the running container.

**Notes on JS driver specifics (resolved empirically during TDD, not
hardcoded):**
- Temporal mapping: `neo4j-driver` returns `neo4j.types.Date`, `Time`/
  `LocalTime`, `LocalDateTime`, `DateTime`. The assertion checks the concrete
  returned type + a round-trip via `$param`, matching the Python suite intent.
- Byte arrays: the JS driver's byte-array representation (e.g. `Int8Array`) is
  confirmed against the running driver in TDD; TYPE-004 echoes a client-bound
  byte array and asserts value equality.
- The two-writer race (TX-005, ERR-004) is timing-sensitive: assert on the whole
  collected error set for a `TransientError` (not a single index), and that no
  non-retryable error appears - the Python suite's hardening.

## Driver-version band

`spec.yaml` assigns JS the bands `[oldest-supported-4.x, latest-5.x,
latest-6.x]`, "resolved by #4888"; current repo baseline is `^6.0.1`. For
PR-gating this suite pins a single concrete `neo4j-driver` 6.x version in
`package.json` (the existing `^6.0.1`, or the latest 6.x patch if bumped during
implementation), documented in `e2e-js/README.md` (added). Exercising all three
bands on a schedule is #4891's responsibility; this issue must not front-run it
(neither Python, Go, nor C# did).

## CI

No new CI job. The existing `js-e2e-tests` job in `.github/workflows/mvn-test.yml`
runs `npm install && npm test`; Jest picks up the new `*.test.js` file
automatically, and the job already passes `ARCADEDB_DOCKER_IMAGE` and loads the
branch image. `jest-junit` (already configured in `package.json`)
writes `reports/jest-junit.xml`. No CI edit is required for PR-gating; artifact
upload/aggregation is deferred to #4891 (matching how the Go suite declined to
front-run it).

## Testing / verification approach

TDD is awkward for a from-scratch e2e file (the container must exist before any
test runs), so the practical sequence is:
1. Scaffold the fixture (`beforeAll` container boot + HTTP seeding) plus a single
   `[CONN-001]` test; get one green run against a locally-built branch image
   first, proving the whole harness (boot, seed, connect) before fanning out.
2. Add scenarios area by area (connection -> auth -> transactions -> ... ->
   protocol), each verified against the real container as written.
3. Verify the six `it.failing` scenarios genuinely reproduce their gap (the test
   is green because the body throws/mismatches) - and sanity-check that
   converting one to a plain `it()` makes it fail, proving the XPASS guard works.
4. Wire and verify the TLS derived-image path locally (with `keytool` on PATH),
   confirming CONN-002/CONN-005 pass; confirm the `keytool`-absent skip path.
5. Full-suite run against a locally-built image
   (`mvn -Pdocker -DskipTests -pl bolt,package -am` to produce
   `arcadedata/arcadedb:latest`, the "debug against the real image" practice)
   before opening the PR: 31 pass, 6 xfail-green, 2 skip, and the untouched
   `js-bolt-e2e.test.js` still green.
6. CI runs the existing job against the branch-built image; all cells behave as
   above.

No unit tests - this is an integration/e2e suite by nature, matching the sibling
modules.

## Risks / open points

- **testcontainers-js `fromDockerfile` build** for the TLS derived image: if it
  proves flaky in CI, the fallback is a runtime bind mount, but the
  COPY-into-derived-image approach is the proven path across Python/Go/C# and is
  preferred. Highest-risk item; validated locally in step 4 before the PR.
- **JS driver temporal / byte-array concrete types**: resolved empirically in
  TDD against the running container; the design does not hardcode them.
- **Two container boots per `js-e2e-tests` job** (existing file + new file, run
  sequentially under `--runInBand`): roughly doubles that job's wall-clock. This
  is acceptable and matches the one-container-per-suite pattern; sharing a
  container across the two files would require touching the "preserved
  unchanged" regression file.
- **Divergence from `spec.yaml`**: baseline target is `main`'s reconciled
  statuses. If the JS driver empirically behaves differently from that (unlikely
  for the shared server behavior), the JS suite asserts actual behavior and the
  same PR reconciles `spec.yaml` - it is the shared source of truth.
