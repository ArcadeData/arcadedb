# Issue 4273 - CONNECTION_STRATEGY.STICKY is a no-op

## Summary

`RemoteHttpComponent.CONNECTION_STRATEGY.STICKY` was functionally identical to
`ROUND_ROBIN`. Neither strategy ever pinned requests to the concrete cluster-member
that handled `begin()`, causing "Remote transaction not found or expired" errors in HA
deployments where a load-balancer routes HTTP requests across nodes.

## Root Cause

`RemoteHttpComponent.httpCommand()` only branched on `FIXED`. Both `STICKY` and
`ROUND_ROBIN` fell into the same else-path (round-robin or leader-preferred routing).
Likewise, `RemoteDatabase.begin()` used `currentServer`/`currentPort` (the LB
hostname) and the session-ID-carrying server was never recorded.

## Fix

1. Added `stickyTransactionServer` field to `RemoteHttpComponent` - a
   `Pair<String, Integer>` that records the concrete pod that should service the
   current transaction.

2. `RemoteHttpComponent.getUrl()` checks `stickyTransactionServer` when strategy is
   `STICKY` and serves the pinned URL instead of the LB hostname.

3. `RemoteHttpComponent.httpCommand()` uses `stickyTransactionServer` as the initial
   target and treats it like `FIXED` for retry-count and failover (no round-robin
   during an active transaction).

4. `RemoteDatabase.begin()` picks `leaderServer` (or `currentServer`/`currentPort`
   as fallback) as the sticky target BEFORE issuing the HTTP call, so all three
   phases (begin → command → commit/rollback) reach the same physical node.

5. `RemoteDatabase.setSessionId(null)` clears `stickyTransactionServer` so the pin
   is released when the transaction ends.

## Files Changed

- `network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java`
- `network/src/main/java/com/arcadedb/remote/RemoteDatabase.java`

## Tests

- `RemoteHttpComponentTest` - 4 new unit tests for URL routing under STICKY
- `RemoteDatabaseTest` - 2 new unit tests for session/sticky-server lifecycle

## Verification

```
cd network && mvn test -Dtest="RemoteHttpComponentTest,RemoteDatabaseTest" -q
```
