# #6756: gRPC server lifecycle, double-terminate, and executeCommand row-cap defects

Three unrelated defects in `ArcadeDbGrpcService`/`GrpcServerPlugin`, grouped in the issue because they share
the same service/plugin.

## 1. `GrpcServerPlugin.startService()` failure leaks the server and reaper thread

`configureServer()` constructs `ArcadeDbGrpcService` (which starts its idle-transaction reaper thread)
*before* `serverBuilder.build().start()` actually binds the port. `ArcadeDBServer.start()` deliberately
never calls `stopService()` on a plugin whose `startService()` threw, so a bind failure (port in use) - or,
in `mode=both`, the xDS server failing after the standard server fully started - left the already-created
service/reaper (and possibly a bound port) running with no teardown path.

**Fix:** `startService()`'s catch block now calls the (idempotent, CAS-guarded) `stopService()` before
rethrowing.

**Test:** `Issue6756StartServiceFailureCleanupTest` - occupies a real port with a raw `ServerSocket`, points
the plugin at that port, asserts `startService()` throws, and asserts the leaked service's idle reaper was
actually shut down (`ArcadeDbGrpcService.isIdleReaperShutdown()`, a new test-support accessor - unlike the
existing `isIdleReaperActive()`, which reflects only the constructor-time decision and never flips back).
Verified to fail without the fix.

## 2. Unary handlers can double-terminate on a concurrent client cancel

Only `executeCommand` guarded its catch block with a `responded` flag (issue #6192) against calling
`onError` after `onNext`+`onCompleted` had already been delivered. A client cancel landing exactly as the
response is delivered can make `onCompleted()` throw; every other unary handler's catch then called
`onError` on an already-closed call, letting an `IllegalStateException` ("call already closed") escape the
handler instead of the call simply completing.

**Fix:** applied the same `responded` guard to `executeQuery`, `createRecord`, `lookupByRid`,
`updateRecord`, `deleteRecord`, `bulkInsert`, `beginTransaction`, `commitTransaction`, `rollbackTransaction`.

**Test:** `Issue6756DoubleTerminateGuardTest` (9 cases, one per handler) - calls each handler directly
against a real `ArcadeDbGrpcService` (no gRPC transport) with a mocked `StreamObserver` whose
`onCompleted()` throws, and asserts `onError` is never invoked afterward. All 9 verified to fail without the
fix.

## 3. `executeCommand` with `return_rows=true` silently drops rows past `max_rows`

Rows beyond `max_rows` (default 1000) were still counted into `affected` but never added to the response,
and nothing signalled the truncation - diverging from `executeQuery`, which fails loudly with
`RESOURCE_EXHAUSTED` when its own row cap is exceeded.

**Fix:** mirrored `executeQuery`: a result exceeding `max_rows` now throws `RESOURCE_EXHAUSTED` instead of
returning a silently truncated result.

**Test:** `Issue6756ExecuteCommandMaxRowsTest` - asserts a 10-row result with `max_rows=5` fails with
`RESOURCE_EXHAUSTED` (and never calls `onNext`), and a 10-row result with `max_rows=20` succeeds normally
with all 10 records. Verified the first case fails without the fix.
