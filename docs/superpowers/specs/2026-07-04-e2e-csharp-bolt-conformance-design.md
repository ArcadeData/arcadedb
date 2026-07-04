# e2e-csharp Bolt conformance suite (design)

Issue #4886, part of epic #4882 (Group B, per the shared spec authored in
#4883: `bolt/conformance/spec.yaml`).

## Context

`e2e-csharp` today has zero Bolt footprint: `ArcadeDB.E2ETests.csproj`
references only `Npgsql`/`Testcontainers`/`xunit`, and `PostgresE2ETests.cs`
exercises the Postgres wire protocol only. This issue adds a `Neo4j.Driver`
NuGet-based Bolt conformance suite implementing every scenario in
`bolt/conformance/spec.yaml`.

Two sibling Group B issues already merged to `main` before this design was
written and materially change the starting picture:

- **#4884** (`bolt/SERVER_IDENTITY.md`) documents the advertised
  `"Neo4j/5.26.0 compatible (ArcadeDB ...)"` identity and known gaps.
- **#4885** (`e2e-python/tests/test_bolt.py`, merged via PR #4920) is a
  complete, working implementation of the same spec against the official
  Python driver. While implementing it, that suite found that several gaps
  spec.yaml originally recorded as `expected-fail` had *already been fixed*
  upstream (native temporal PackStream structs: #4905/#4906/#4907;
  `Neo.TransientError.*` codes: #4908/#4909), and discovered two *new* gaps
  that spec.yaml didn't originally record (`ERR-002` semantic error code is
  dead code; `RESULT-004` write counters are never populated). It updated
  `spec.yaml`'s `current_status` fields accordingly. **This design targets
  that current, corrected `spec.yaml` on `main`, not the version original to
  #4883.**

Remaining genuine gaps (all tracked under `#4890`): `CONN-004` (ROUTE is
single-node only), `TYPE-003` (Path never constructed), `TYPE-011`
(Duration unhandled), `TYPE-012` (Point unhandled), `PROTO-002` (no Bolt 5.x
negotiation), plus the two newly-discovered `ERR-002`/`RESULT-004`.
`ERR-003` is `not-applicable`: no official driver's public API can send RUN
before completing HELLO/LOGON, so it can't be exercised this way in any
language.

**This design closely mirrors `e2e-python/tests/test_bolt.py`'s already-reviewed
architecture**, translated to xUnit/C# idiom, rather than inventing a new
shape. The epic's success criteria explicitly calls for comparable,
aggregatable per-language results, and following an already-merged sibling
suite is the most direct way to get that.

## Components

### 1. `Neo4j.Driver` dependency

Add to `ArcadeDB.E2ETests.csproj`:
```xml
<PackageReference Include="Neo4j.Driver" Version="6.2.1" />
```
`6.2.1` is the latest stable NuGet release (targets net8.0/9.0/10.0,
Apache-2.0), keeping rough parity with the Java suite's pinned
`neo4j-java-driver` 6.2.0 and Python's `neo4j>=6.2.0`. Per spec.yaml's
`driver_version_bands`, this is a single pinned version for the
PR-gating suite; resolving the full `lts`/`current`/`latest` band matrix
is `#4891`'s job (nightly scheduled run), not this issue's.

### 2. Fixtures - three containers, following the Python suite's split

A single shared container can't serve both the default scenarios and the
two TLS modes, because TLS mode (`DISABLED`/`REQUIRED`/`OPTIONAL`) is a
server-startup-time flag. New file `BoltFixtures.cs`:

- **`ArcadeDbBoltFixture`** (collection `"ArcadeDB-Bolt"`): one container,
  `BoltProtocolPlugin` enabled, `beer` imported via
  `-Darcadedb.server.defaultDatabases=beer[root]{import:...OpenBeer.gz}`
  (same recipe as `ArcadeContainerTemplate.java` and the Python fixture),
  plus a `boltscratch` database created via HTTP for `MDB-002`, plus
  `bolt/conformance/fixtures/type-matrix.cypher` seeded via HTTP
  `/api/v1/command/beer` (never over Bolt itself - see spec.yaml's fixture
  note on why). Covers every scenario except the two TLS ones and
  `CONN-004`.
- **`ArcadeDbBoltTlsRequiredFixture`** / **`ArcadeDbBoltTlsOptionalFixture`**
  (own collections): each builds a throwaway image
  (`ImageFromDockerfileBuilder`, `FROM arcadedata/arcadedb:latest` +
  `COPY` a generated PKCS12 keystore/JKS truststore) rather than bind-mounting
  certs at runtime - the Python suite's comment on this is explicit that
  bind-mounts were observed to start with an empty mounted directory on some
  CI runners, while a Dockerfile `COPY` always resolves its own build
  context correctly. Certs are generated once via the JDK `keytool` binary
  (present by default on GitHub's `ubuntu-latest` runners, same assumption
  the Python suite already makes with no extra CI setup step) into a shared
  temp dir, then each fixture sets
  `-Darcadedb.bolt.ssl=REQUIRED|OPTIONAL` +
  `-Darcadedb.ssl.keyStore`/`.trustStore` (+ passwords).

All three fixtures dispose their container in `DisposeAsync`; the shared
keystore/truststore temp directory and throwaway image are cleaned up once
both TLS fixtures are done with them.

### 3. Test files

- **`BoltE2ETests.cs`** (collection `"ArcadeDB-Bolt"`): all non-TLS,
  non-HA scenarios - connection (bolt://, neo4j:// single-node), auth,
  transactions, causal consistency, multi-database, result-handling,
  type-roundtrip, errors, protocol. 37 of the spec's 39 scenarios (all but
  the two TLS ones, `CONN-002`/`CONN-005`).
- **`BoltTlsE2ETests.cs`**: two small test classes, one per TLS-mode
  collection, for `CONN-002` and `CONN-005`.

Every test's `[Fact(DisplayName = "...")]` embeds its spec.yaml scenario id
per `bolt/conformance/README.md`'s traceability convention, e.g.
`[Fact(DisplayName = "TYPE-011: Duration round-trip")]`.

### 4. Representing known-gap scenarios

xUnit has no built-in equivalent of pytest's `xfail(strict=True)` (fails the
build if a marked-expected-to-fail test unexpectedly starts passing, forcing
the marker to be removed). Two categories, matching the Python suite exactly:

- **Truly non-automatable in this harness** (`CONN-004` 3-node HA topology,
  `ERR-003` pre-auth RUN): `[Fact(Skip = "reason, see #4890")]`, direct xUnit
  equivalent of `pytest.mark.skip`.
- **Real product gaps, automatable, currently failing** (`TYPE-003`,
  `TYPE-011`, `TYPE-012`, `PROTO-002`): a small helper,
  `KnownGapAssertions.AssertStillFails(Func<Task> action, string reason)`,
  added to the test project. It writes the *target-correct* assertion
  normally (e.g. "Duration round-trips unchanged"), runs it inside a
  try/catch, and:
  - swallows any exception/assertion failure (the gap still exists, as
    expected) and returns normally, so the test passes today;
  - re-throws a clear `XunitException` if the action *succeeds* - so the
    moment the underlying bug is fixed, this test starts failing the build,
    forcing whoever fixed it to flip it to a normal `[Fact]` and update
    `spec.yaml`'s `current_status` in the same PR (mirroring the
    `README.md` guidance already followed by #4885).

  This achieves the same "fails loudly when fixed, not silently absent"
  property as `xfail(strict=True)` without needing to hand-derive today's
  exact fallback wire shape for Duration/Point/Path in C# - the helper only
  needs to know that the *correct* assertion currently throws, not exactly
  how.

### 5. CI

No workflow changes needed: `csharp-e2e-tests` in `mvn-test.yml` already
runs `dotnet test` against this project and is already PR-gating. `keytool`
requires no new setup step (ubuntu-latest ships a JDK already; the Python
job makes the same assumption with no Java setup step).

## Testing / verification

- `dotnet build` must pass with 0 warnings/errors (matches existing repo
  convention for this project).
- `dotnet test` locally against a live ArcadeDB image
  (`ARCADEDB_DOCKER_IMAGE` or default `arcadedata/arcadedb:latest`) must
  pass: all non-gap scenarios green, all four `AssertStillFails`-wrapped
  scenarios green (confirming the gap still reproduces against this build),
  both `Skip`-marked scenarios reported skipped, not failed.
- No unit tests are meaningful here (this project *is* the E2E suite); the
  existing `PostgresE2ETests.cs` must continue passing unmodified,
  confirming the new Bolt fixtures don't interfere with the Postgres
  collection.

## Non-goals (matching the Python suite's precedent)

- No multi-node HA cluster orchestration for `CONN-004` - out of scope for
  this issue's `M` sizing; `#4890` tracks the underlying ROUTE gap.
- No attempt to trigger `ERR-003` via a bespoke raw-socket client - the
  epic's governing principles restrict suites to official drivers only.
- No full driver-version-band matrix (`lts`/`current`/`latest`) - single
  pin only; `#4891` owns the nightly band matrix.
