# Issue #4916 - Bolt ROUTE crashes with ClassCastException under negotiated protocol 4.4

## Problem

`BoltNetworkExecutor.SUPPORTED_VERSIONS` advertises Bolt 4.4 as the highest
negotiable protocol version. Under Bolt 4.4+ the ROUTE message's third field
changed shape from `db::String` (pre-4.4) to `extra::Map{db, imp_user}`.
`BoltMessage.parseRoute` unconditionally cast the third field to `String`,
so any `neo4j://` routing-scheme connection (official Neo4j drivers
negotiate 4.4 by default) threw `ClassCastException: class
java.util.LinkedHashMap cannot be cast to class java.lang.String`, surfaced
to the client as `Neo.DatabaseError.General.UnknownError`. Every
`neo4j://` connection failed outright, even against a trivial single-node
deployment.

## Root cause

`BoltMessage.parseRoute(List<Object> fields)` read `fields.get(2)` and cast
it directly to `String` regardless of the negotiated protocol version.

## Fix

`parseRoute` now branches on the runtime shape of the third field instead of
threading protocol-version state through the parser:
- If it is a `Map`, treat it as Bolt 4.4+'s `extra` map and pull `db` out of
  it (missing/null `db` yields `null`, matching prior behavior for no
  database specified).
- Otherwise, treat it as the pre-4.4 `db::String` field directly.

`imp_user` is not extracted since nothing downstream currently consumes
impersonation, keeping the fix minimal and localized to the parsing gap
described in the issue.

## Tests

- `BoltMessageTest` (new unit tests on `BoltMessage.parse`):
  - `parseRouteMessageWithBolt44ExtraMapDatabase` - 4.4+ shape extracts `db`
  - `parseRouteMessageWithBolt44ExtraMapDatabaseAndImpUser` - `db` extracted, `imp_user` ignored without error
  - `parseRouteMessageWithBolt44ExtraMapNoDb` - map without `db` yields `null`, no exception
  - `parseRouteMessageWithBolt44EmptyExtraMap` - empty map yields `null`, no exception
  - Existing `parseRouteMessage`/`parseRouteMessageMinimalFields` continue to cover the pre-4.4 `db::String` shape and confirm no regression.
- `BoltProtocolIT` (new integration test, real Neo4j Java driver 6.2.0 against a live embedded server):
  - `routingConnection` - connects via `neo4j://localhost:7687` (routing scheme), calls `verifyConnectivity()`, and runs a query.

## Test results

- Confirmed the bug: with the fix reverted, both the new `BoltMessageTest`
  cases and `BoltProtocolIT.routingConnection` failed with the exact
  reported `ClassCastException` (the IT surfaced it wrapped as
  `BoltFailureException` / `ServiceUnavailable` client-side, matching the
  issue's repro).
- With the fix applied: `mvn -pl bolt test` - 228 tests, 0 failures.
  `mvn -pl bolt test -Dtest=BoltProtocolIT` - 77 tests, 0 failures
  (includes the new routing test).

## Impact

Localized to `BoltMessage.parseRoute`; no other ROUTE consumers
(`RouteMessage`, `BoltNetworkExecutor.handleRoute`) needed changes since
they already operate on the parsed `String database` field. Fixes
`neo4j://` routing-scheme connectivity for single-node deployments; ROUTE
remains single-node-only (not HA-cluster-aware), which is a separate,
already-tracked gap in issue #4916's parent scope.

`bolt/conformance/spec.yaml`'s CONN-003 already claims `current_status:
passing`; with this fix that claim becomes accurate for the single-node
case, so no change was needed there. Automated verification of CONN-003
via the `e2e-python` Bolt conformance suite (#4885) was not performed as
part of this fix - that suite does not yet have Bolt/Neo4j driver tests
implemented in this repository.
