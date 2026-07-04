# e2e-js

End-to-end tests for ArcadeDB's JavaScript-facing wire protocols, run with Jest
against a real ArcadeDB container (testcontainers).

## Suites

- `src/js-bolt-conformance.test.js` - Bolt conformance suite (issue #4888,
  epic #4882). Implements all 39 scenarios in `bolt/conformance/spec.yaml`
  against the official `neo4j-driver`. Each test name is prefixed with its
  spec id (`[AREA-NNN]`) for traceability.
- `src/js-bolt-e2e.test.js` - Bolt variable-length-path regression (#4452/#4271).
- `src/js-pg-e2e.test.js` - PostgreSQL wire-protocol smoke test.

## Driver version band

`spec.yaml` assigns JS the bands `[oldest-supported-4.x, latest-5.x, latest-6.x]`.
This suite PR-gates a single pinned `neo4j-driver` 6.x (`package.json`,
currently `^6.0.1`). Exercising all three bands on a schedule is #4891.

## Known gaps (asserted via `it.failing`, tracked in #4890)

RESULT-004 (write counters), TYPE-003 (native Path), TYPE-011 (Duration),
TYPE-012 (Point), ERR-002 (semantic-error code), PROTO-002 (Bolt 5.x). Each is
written to assert the Neo4j-correct behavior, so `it.failing` stays green while
the gap reproduces and turns red (XPASS) the moment the server is fixed - at
which point convert it to `it()` and flip `current_status` in
`bolt/conformance/spec.yaml` in the same change.

Skipped: CONN-004 (needs a 3-node HA cluster), ERR-003 (needs a raw Bolt socket
the managed driver cannot produce).

## Run

```bash
npm install
ARCADEDB_DOCKER_IMAGE=arcadedata/arcadedb:latest npm test
```

The `ARCADEDB_DOCKER_IMAGE` env var selects the image under test (CI passes the
branch-built image); it defaults to `arcadedata/arcadedb:latest`.

### Local (macOS) note

CONN-003 (`neo4j://` routing) exercises the driver's routing-discovery path:
the server's ROUTE reply advertises its own bind address, which on the Docker
bridge network is only reachable from the host on Linux. On a macOS/Docker
Desktop host that address is not routable, so CONN-003 times out locally; it
passes on the Linux CI runners (same as the Python/Go/C# suites).

### TLS scenarios

CONN-002/CONN-005 need a JDK `keytool` on PATH to mint a throwaway self-signed
keystore/truststore, baked into a derived image. They self-skip with a warning
if `keytool` is absent.
