# ArcadeDB Go E2E Tests (Bolt conformance)

End-to-end Bolt protocol conformance tests for ArcadeDB using the official
[`neo4j-go-driver`](https://github.com/neo4j/neo4j-go-driver). Part of epic
[#4882](https://github.com/ArcadeData/arcadedb/issues/4882) (issue
[#4887](https://github.com/ArcadeData/arcadedb/issues/4887)).

Every scenario in the shared conformance spec
[`bolt/conformance/spec.yaml`](../bolt/conformance/spec.yaml) (#4883) is
implemented here, so the Go results are directly comparable to the sibling
`e2e-python` (#4885), `e2e-csharp` (#4886), `e2e-js`, and Java suites.

## Traceability convention

Each test function embeds its `spec.yaml` scenario id:
`Test_<AREA>_<NNN>_<slug>`, e.g. `Test_TYPE_011_DurationRoundtrip` maps to
scenario `TYPE-011`. This 1:1 naming is the cross-language traceability
convention documented in
[`bolt/conformance/README.md`](../bolt/conformance/README.md).

Outcomes mirror each scenario's `current_status` in `spec.yaml`:

- `passing` / `unverified` -> a normal green test.
- `expected-fail` -> a strict-xfail via `assertStillFails` (see
  `knowngap_test.go`): the test asserts the Neo4j-correct behavior and passes
  **while the documented gap still reproduces**. If the gap is ever fixed the
  test fails loudly (`XPASS: ...`), forcing whoever fixed it to convert the
  test to a normal assertion and update `spec.yaml` in the same PR. This is the
  Go port of the C# suite's `KnownGapAssertions`.
- HA-cluster / raw-socket scenarios (`CONN-004`, `ERR-003`) `t.Skip` with the
  same justification the Python/C# suites use.

## Driver-version bands

`spec.yaml` assigns Go the bands `[lts, current, latest]` for
`github.com/neo4j/neo4j-go-driver`. There is no published v6 Go module; the
driver line is `neo4j-go-driver/v5`. This suite pins a single concrete `v5`
version (see `go.mod`) for PR-gating. Exercising all three bands on a schedule
against a pinned driver-version matrix is the responsibility of
[#4891](https://github.com/ArcadeData/arcadedb/issues/4891); this module does
not front-run it.

## Running the tests

```bash
cd e2e-go
go test -v ./...
```

Requirements:

- A local ArcadeDB Docker image tagged `arcadedata/arcadedb:latest`, or set
  `ARCADEDB_DOCKER_IMAGE` to the image to test. In CI the branch-built image is
  `docker load`ed under the tag passed via `ARCADEDB_DOCKER_IMAGE`. To build the
  branch image locally: `./mvnw install -Pdocker -DskipTests -pl bolt,package -am`.
- Docker (the suite uses [`testcontainers-go`](https://golang.testcontainers.org/)).
- For the TLS scenarios (`CONN-002`, `CONN-005`): a JDK `keytool` on `PATH` and
  `docker build` (a throwaway self-signed keystore/truststore is generated and
  baked into a derived image). If either is unavailable locally, those two
  scenarios skip with a clear message; they always run in CI.

### Note on `neo4j://` routing (`CONN-003`)

`CONN-003` connects with the `neo4j://` routing scheme. ArcadeDB's ROUTE
response advertises the server's own bound address (the container's Docker
bridge IP). On the Linux CI runner that subnet is routable from the host, so
the scenario passes. On Docker Desktop (macOS/Windows) container IPs are not
host-routable, so `CONN-003` can only be fully verified in CI. All `bolt://`
scenarios are unaffected because they use the mapped host port.

## Layout

| File | Responsibility |
|---|---|
| `fixtures_test.go` | `TestMain`, shared plain container, HTTP seeding, repo-root + image resolution |
| `tls_test.go` | keytool cert-gen, derived-image build, lazy TLS-required/optional containers |
| `knowngap_test.go` | `assertStillFails` strict-xfail helper |
| `bolt_conformance_test.go` | the 39 `Test_<AREA>_<NNN>_<slug>` scenario functions |
