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

## PR

https://github.com/ArcadeData/arcadedb/pull/4377

## Review cycles

### Cycle 1 - SHA 64f4b6f0c0203010be8e642694bad79e6efa8274

gemini-code-assist reviewed with 3 medium-priority comments (all applied):
1. Pass IOException cause `e` to `TransactionException` constructor for better stack traces.
2. Use `127.0.0.1` instead of `"replica-server"` in both test replica entries to avoid DNS lookups.
3. (Same as #2, second test.)

Follow-up SHA: `76076726b611663477f1df992d177cebee92781f`

### Cycle 2 - SHA 76076726b611663477f1df992d177cebee92781f

No review - gemini-code-assist does not re-review follow-up pushes (known repo behavior).
Loop exited with `timeout` state.

## Final state: timeout (gemini does not re-review follow-up pushes)

## Status

- [x] Test written (failing before fix)
- [x] Fix implemented
- [x] Tests passing (375 tests, 0 failures, full network module)
- [x] PR opened: https://github.com/ArcadeData/arcadedb/pull/4377
- [x] Cycle 1 review addressed
- [ ] PR merged (developer's responsibility)
