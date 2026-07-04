# Design: e2e-python Bolt conformance suite (issue #4885)

## Context

Part of epic #4882 (Bolt Driver Compatibility Certification), Group B ("per-language
implementations"). Issue #4883 authored the shared, language-neutral conformance
spec (`bolt/conformance/spec.yaml`, 39 scenarios across 9 areas) and issue #4884
documented the advertised server identity (`bolt/SERVER_IDENTITY.md`) - both
already merged to `main` (`e14057ed6`, `67f6bedb6`).

`e2e-python` today has zero Bolt/`neo4j`-driver footprint - it tests only
Postgres-wire (`psycopg`, `asyncpg`, `sqlalchemy`) against a shared
`DockerContainer`-based ArcadeDB instance. This design covers hand-implementing
every scenario from `spec.yaml` as native pytest tests in a new
`e2e-python/tests/test_bolt.py`, wired into the already PR-gating
`python-e2e-tests` CI job. No protocol code changes - this is test-only, mirroring
what #4886-#4889 will each do for their own language.

## Grounding (verified against the codebase and `origin/main`)

- `bolt/conformance/spec.yaml`: 39 scenarios - `connection`(5), `auth`(3),
  `transactions`(5), `causal-consistency`(1), `multi-database`(2),
  `result-handling`(4), `type-roundtrip`(12), `errors`(4), `protocol`(3).
  `current_status` values: `passing` (22 scenarios), `expected-fail` (11
  scenarios, all tied to the epic's 4 confirmed gaps and tagged
  `tracking_issue: "#4890"`), or `unverified` (6 scenarios; spec-authoring time
  did not confirm these either way - README explicitly defers resolving them
  to each B-task's first implementation run).
- `bolt/conformance/README.md`: consumption model is "hand-write the test, embed
  the scenario ID for traceability" - no suite parses `spec.yaml` at runtime.
  Python convention given verbatim: `def test_TYPE_011_duration_roundtrip(): ...`.
- `bolt/conformance/fixtures/type-matrix.cypher`: a single `CREATE (:TypeMatrix
  {...})` statement (temporal props, Duration, Point, nested list/map, null) that
  must be seeded via HTTP `/command`, never via Bolt (would exercise the very
  serialization path several `type-roundtrip` scenarios test).
- `e2e-python/pyproject.toml`: `requires-python = ">=3.8"`; deps are all
  `>=` floors (`testcontainers>=4.14.2`, `pytest>=9.1.1`, `pytest-check>=2.8.0`,
  etc.), no `neo4j` driver present. No `conftest.py` anywhere in the module.
- `e2e-python/tests/test_arcadedb.py` (and sibling Postgres-wire files) establish
  the pattern to mirror: a module-level `DockerContainer("arcadedata/arcadedb:latest")`
  with `.with_exposed_ports(...)` / `.with_env("JAVA_OPTS", ...)`, started by a
  `@pytest.fixture(scope="module", autouse=True)`, readiness polled via
  `GET /api/v1/ready` expecting HTTP 204.
- `e2e/src/test/java/.../ArcadeContainerTemplate.java` (Java's shared e2e base)
  and `e2e-js/src/js-bolt-e2e.test.js` confirm the exact plugin/port pattern
  needed: expose port `7687`, add `BoltProtocolPlugin` to
  `-Darcadedb.server.plugins=...`. `GlobalConfiguration` shows Bolt's defaults
  (host `0.0.0.0`, port `7687`, TLS `DISABLED`) need no extra sysprops beyond
  listing the plugin, except for the two TLS scenarios (`arcadedb.bolt.ssl=
  REQUIRED`/`OPTIONAL`).
- Root `pom.xml` pins Java's `neo4j-driver.version` to `6.2.0`; `e2e-js` pins
  `^6.0.1`. Current PyPI `neo4j` driver is `6.2.0` (released 2026-05-04),
  requiring Python `>=3.10`.
- `.github/workflows/mvn-test.yml`'s `python-e2e-tests` job already runs
  `pytest tests/` with no path filtering - a new `test_bolt.py` file needs no
  workflow changes to be picked up.
- Server HTTP command endpoint confirmed via
  `server/.../handler/PostCommandHandler.java`: `POST /api/v1/command/{database}`,
  JSON body `{"language": "...", "command": "..."}`, Basic auth.

## Decisions

### 1. Known-gap scenarios: `pytest.mark.xfail(strict=True)`

The 11 `expected-fail` scenarios are implemented as ordinary tests asserting the
*correct*, Neo4j-compatible behavior (e.g. "Duration comes back as a native
`neo4j.time.Duration`"), decorated `@pytest.mark.xfail(strict=True, reason="...
see #4890")`. This satisfies both of #4885's acceptance criteria at once:
failures are visible in the pytest report with a reason referencing the tracking
issue (not a silent skip), and the run still exits green. `strict=True` means if
the underlying gap is ever closed, the test flips from XFAIL to a hard failure
(XPASS-as-failure) instead of quietly staying green - forcing `spec.yaml`'s
`current_status` to be updated in that future PR. This is the standard pytest
idiom for "known, tracked, not-yet-fixed" and needs no custom tooling.

**Rejected alternative: characterization tests** (assert today's actual broken
output, e.g. "Duration comes back as a string", as a plain passing test). Rejected
because a plain `PASS` in the report is indistinguishable from a real conformance
pass to a future reader or to the CI-aggregation matrix (#4891/#4892), which
misrepresents certification status for exactly the scenarios meant to be flagged.

### 2. Unverified scenarios: implement as real assertions, promote to xfail if they fail

The spec's `unverified` scenarios were not confirmed either way at spec-authoring
time. Each is implemented as a normal assertion of correct behavior. If one fails
during implementation and the failure looks like a genuine gap (not a scenario
authoring error), per `bolt/conformance/README.md`'s own instructions: mark it
`xfail(strict=True)`, and in the same PR update `spec.yaml`'s
`current_status`/`known_limitation`/`tracking_issue` (reusing `#4890` if it fits
the same root cause, otherwise opening a new tracking issue).

### 3. CONN-004 (HA routing topology) is `pytest.mark.skip`, not `xfail`, with a clear reason

CONN-004's precondition is a 3-node HA cluster - standing that up would require
new multi-container HA orchestration code in `e2e-python` that no other suite in
this module has. Unlike the other 10 `expected-fail` scenarios, this one cannot
be meaningfully expressed as an `xfail` against a single-node container: the
"correct" assertion (routing table lists multiple distinct nodes reflecting real
leader/follower topology) would keep failing forever even after
`BoltNetworkExecutor.handleRoute` is fixed, because a single-node deployment
*correctly* has only one node to report - `strict=True`'s self-correcting
property (Decision 1) would never trigger, silently misrepresenting the scenario
as permanently broken. Marking it `pytest.mark.skip(reason="requires a 3-node HA
cluster; e2e-python's single-node harness cannot meaningfully exercise this
scenario - see #4890")` is the honest signal here: it is loud (shown as SKIPPED
with the reason in every test report, unlike an unexplained/absent test) without
falsely implying it's testable in this harness. If a real multi-node HA e2e
harness is ever built (in this module or elsewhere), this scenario should be
revisited to run against it as a proper `xfail`/`pass`.

### 4. Three container fixtures, one test file

- `bolt_container` (module-scoped, TLS disabled): the default container -
  `beer` dataset preloaded (existing pattern), plus port `7687` and
  `BoltProtocolPlugin` added to the plugin list. At setup this fixture also:
  - creates a second database (`boltscratch`) via
    `POST /api/v1/server` (`create database boltscratch`) for MDB-002's
    cross-database isolation scenario;
  - seeds `bolt/conformance/fixtures/type-matrix.cypher` into `beer` via
    `POST /api/v1/command/beer` (`language: cypher`) over HTTP - never Bolt.
  Hosts the remaining 37 scenarios (39 total minus CONN-002 and CONN-005).
- `bolt_container_tls_required` / `bolt_container_tls_optional` (module-scoped,
  fixture-requested only by CONN-002 / CONN-005): dedicated containers started
  with `arcadedb.bolt.ssl=REQUIRED` / `OPTIONAL`. Kept in the same file as
  separate fixtures (pytest only boots a fixture when a test requests it) rather
  than a separate file, since `spec.yaml` traceability is scenario-ID-based, not
  file-based.

No `conftest.py` is introduced - consistent with the rest of `e2e-python` today.

### 5. Dependency: `neo4j>=6.2.0`, bump `requires-python` to `>=3.10`

Adds `neo4j>=6.2.0` to `pyproject.toml` (matching the `>=` floor style of every
existing dependency, and Java's pinned `6.2.0` baseline). The 6.x driver line
requires Python `>=3.10`, so `requires-python` moves from `>=3.8` to `>=3.10`.
Harmless in practice - CI's `python-e2e-tests` job already pins Python `3.13.0`.
Per `spec.yaml`'s `driver_version_bands.python` (`lts`/`current`/`latest`), this
PR resolves the band to a single concrete version (`current`, `6.2.0`) and
records it in `resolution_note` - matching the design decision from #4883 that
each B-task resolves its own bands and records them where dependency versions
already live. Testing across all three bands is out of scope (see below).

### 6. Traceability

Every test named `test_<AREA>_<NNN>_<slug>` per the README's Python convention,
e.g. `test_TYPE_011_duration_roundtrip`. A module-level docstring or comment
notes the scenario `id` maps 1:1 to `spec.yaml`, so a future CI-aggregation
script (#4891/#4892) can cross-reference by name pattern.

## Out of scope

- Any protocol/server code change (tracked under #4890).
- Testing across all three driver-version bands (`lts`/`current`/`latest`) in
  this suite's own CI run - one pinned version now; multi-version matrix testing
  is #4891's nightly-scheduled job, not a per-B-task concern.
- Building real multi-node HA cluster test infrastructure in `e2e-python`
  (CONN-004 is scoped down per Decision 3).
- JUnit XML report generation/aggregation (D-group, #4891/#4892).
- The other four per-language suites (#4886-#4889).

## Verification plan

1. `pytest tests/test_bolt.py -v` run locally (Docker required) against the
   `beer`+`BoltProtocolPlugin` container - confirms every scenario is a genuine
   `PASS`, an `XFAIL` with a reason string, or (CONN-004 only) a `SKIPPED` with
   a reason string - never an unexpected `FAIL` or an unexplained skip.
2. Cross-check scenario coverage: every `id` in `spec.yaml` has exactly one
   corresponding `test_<id>_*` function in `test_bolt.py` (manual grep check,
   documented in the plan's final verification step).
3. Full existing `e2e-python` suite (`pytest tests/`) still passes - confirms no
   regression to the Postgres-wire tests sharing plugin-list/container-fixture
   patterns.
4. `python-e2e-tests` CI job run (via the PR) confirms the suite is green in
   the real pipeline, not just locally.
