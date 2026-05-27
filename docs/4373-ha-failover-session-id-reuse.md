# Fix #4373: RemoteDatabase reuses session id across servers on HA failover

## Root Cause

`RemoteHttpComponent.httpCommand()` retries failed HTTP calls against a different cluster
member when `autoReconnect=true` and more retries remain. In the server-switch branch
(not `FIXED`, not `stickyPinned`), the code moves to the next server without checking
whether a transaction session is currently open.

Because `RemoteDatabase.createRequestBuilder()` always injects the `arcadedb-session-id`
header when `getSessionId() != null`, the retry request arrives at the new server carrying
a session id that was issued by the original (now unreachable) server. The new server
either rejects the session or treats the request as session-less, while the client still
believes its transaction is alive - silently corrupting or losing the transaction.

## Affected components

- `network/src/main/java/com/arcadedb/remote/RemoteHttpComponent.java` - `httpCommand()`
  IOException retry branch (the `else` block that switches to a new server)

## Fix

In the `else` block (server-switch path), before calling `reloadClusterConfiguration()`,
check whether `this` is a `RemoteDatabase` with an active session. If so:
1. Clear the (now invalid) session id so the object is left in a consistent state.
2. Throw `TransactionException("Server failover during active transaction")`.

The caller is responsible for starting a new transaction.

## Test

New test in `RemoteDatabaseTest`:
- `haFailoverDuringActiveTransactionThrowsTransactionException` - uses a replica entry
  in `replicaServerList` so `maxRetry = 2`, then manually sets a session id and calls
  `query()`. The first HTTP attempt fails with IOException (no server), the retry path
  enters the server-switch branch, detects the active session, and must throw
  `TransactionException`. Asserts the session is cleared afterwards.

## Verification

```
cd network && mvn test -Dtest=RemoteDatabaseTest -pl .
```

## Status

- [x] Test written (failing before fix)
- [x] Fix implemented
- [x] Tests passing (375 tests, 0 failures, full network module)
- [ ] PR opened
