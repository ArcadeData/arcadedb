# Issue #4803 - gRPC unbounded result materialization + busy-wait on the worker thread (DoS)

URL: https://github.com/ArcadeData/arcadedb/issues/4803
Type: bug (`fix`)

## Problem

`ArcadeDbGrpcService` has three unbounded paths that let slow or limitless gRPC
clients pin worker threads / exhaust heap (DoS):

1. **`waitUntilReady`** spins `Thread.sleep(1)` with no deadline while the client
   transport stays not-ready. A slow/abandoned stream pins the gRPC worker thread
   (and the open `ResultSet`/transaction) forever.
2. **`streamMaterialized`** (retrieval mode `MATERIALIZE_ALL`) buffers the entire
   result set into an `ArrayList<GrpcRecord>` before emitting anything - unbounded heap.
3. **Unary `executeQuery`** builds the full result into one response unless
   `request.getLimit() > 0` - unbounded heap.

## Root cause

No upper bound on busy-wait duration, materialized buffer size, or unary result
row count.

## Fix

Three server-scoped `GlobalConfiguration` knobs (each with a DoS-protection
default; `-1` opts back into the legacy unbounded behaviour):

- `arcadedb.server.grpcQueryMaxResultRows` (Integer, default 100000) - caps unary
  `ExecuteQuery` when the request gives no positive `limit`.
- `arcadedb.server.grpcStreamMaxMaterializedRows` (Integer, default 1000000) - caps
  `MATERIALIZE_ALL` buffering; exceeding fails the call with `RESOURCE_EXHAUSTED`
  so the client falls back to `CURSOR`/`PAGED` streaming.
- `arcadedb.server.grpcStreamWriteTimeoutMs` (Long, default 60000) - bounds how long
  `waitUntilReady` blocks for transport readiness before aborting the stream
  (sets the cancelled flag so the surrounding loop rolls back and releases the
  `ResultSet`/transaction).

The busy-wait logic is extracted into a package-private static helper
`awaitTransportReady(BooleanSupplier, AtomicBoolean, long)` so the deadline is
unit-testable without a real stalled transport.

## Tests

- `grpcw` `Issue4803GrpcResultBoundingIT` (extends `BaseGraphServerTest`):
  - unary `ExecuteQuery` with no limit is capped at the configured max rows
  - `MATERIALIZE_ALL` stream over a too-large result fails with `RESOURCE_EXHAUSTED`
- `grpcw` `Issue4803WaitUntilReadyTimeoutTest` (plain unit test):
  - `awaitTransportReady` returns `false` after the deadline when never ready
  - returns `true` immediately when ready
  - returns `false` promptly when already cancelled

## Verification

- `Issue4803WaitUntilReadyTimeoutTest`: 3/3 pass.
- `Issue4803GrpcResultBoundingIT`: 3/3 pass (server log confirms the
  `RESOURCE_EXHAUSTED` cap-breach for `MATERIALIZE_ALL`).
- Regression check on the affected existing gRPC paths - no failures:
  - `GrpcStreamMetricsIT` 1/1, `Issue4197GrpcExecuteQueryResultSetLeakIT` 1/1,
    `ArcadeDbGrpcServiceExtendedTest` 26/26.

Build scoped to `engine` (install) + `grpcw` (test) to avoid the unrelated
pre-existing `ha-raft` `ClusterMonitorTest` test-compile error on main HEAD.

## Files changed

- `engine/.../GlobalConfiguration.java` - three new SERVER-scoped gRPC knobs.
- `grpcw/.../ArcadeDbGrpcService.java` - unary `executeQuery` cap, `streamMaterialized`
  cap (`RESOURCE_EXHAUSTED`), `streamQuery` catch preserves explicit gRPC status,
  `waitUntilReady` bounded by deadline via new `awaitTransportReady` helper.
- `grpcw/.../Issue4803GrpcResultBoundingIT.java` (new), `grpcw/.../Issue4803WaitUntilReadyTimeoutTest.java` (new).
