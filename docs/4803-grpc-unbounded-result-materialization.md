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

## Review cycle 1 (claude + gemini-code-assist)

Both bots reviewed the initial PR head. Applied the gating items; deferred lower-priority notes.

### Applied
- **Unary cap is now a hard ceiling that fails loudly (gemini security-medium #1 + claude #1).**
  The unary `executeQuery` previously let an explicit oversized `limit` bypass the cap, and when no
  limit was given it *silently* truncated at the cap with no signal to the client (inconsistent with
  the `MATERIALIZE_ALL` path and a backward-incompatible silent data loss). It now honors an explicit
  limit only when it is at or below the configured cap, and a result that would exceed the cap fails
  with `RESOURCE_EXHAUSTED` (same status, message style, and "use a LIMIT / CURSOR / PAGED" guidance as
  the stream path). The `RESOURCE_EXHAUSTED` is thrown before any response emission, so no partial
  batch or `ResultSet` leak. The transaction-path catch in `executeQuery` now also preserves an
  explicit `StatusRuntimeException` instead of masking it as `INTERNAL`.
- **Monotonic deadline in `awaitTransportReady` (gemini #2).** Switched from
  `System.currentTimeMillis() + timeoutMs` to a `System.nanoTime()` elapsed-since-start comparison:
  immune to wall-clock adjustments and free of the addition overflow for large timeouts.
- **`0` documented as a second "unlimited" value (claude #3).** Both row-cap config descriptions now
  say "Set to -1 or 0 for unlimited"; the unary description also states the hard-ceiling /
  `RESOURCE_EXHAUSTED` semantics.
- **New tests (claude test gap + gemini #1).** `Issue4803GrpcResultBoundingIT` now asserts that a
  limitless query over a too-large result fails with `RESOURCE_EXHAUSTED`, and adds
  `unaryExecuteQueryWithExplicitLimitLargerThanCapFailsWithResourceExhausted` locking in that an
  oversized explicit limit cannot bypass the ceiling. The smaller-than-cap honored-limit test is kept.

### Deferred / not actioned (with rationale)
- **Explicit client status on the write-timeout path (claude #4).** On deadline expiry
  `waitUntilReady` sets the shared `cancelled` flag, so a server-side timeout and a real client cancel
  are indistinguishable to the stream loop and the client gets no explicit `DEADLINE_EXCEEDED`. Surfacing
  a distinct status requires separating the two signals and is a larger, separable change; deferred as a
  follow-up. The per-sweep WARNING log already gives operators visibility.
- **Off-by-one between the two caps (claude #2).** No longer applies on the unary side - the new unary
  path never materializes beyond the ceiling before failing (it checks `count >= configuredMax &&
  resultSet.hasNext()`). The `MATERIALIZE_ALL` path keeps its existing "max + 1 buffered then throw"
  behavior; both are safe and a one-row buffer difference is not worth a behavior change to the stream path.
- **1 ms poll granularity in `awaitTransportReady` (claude #5).** Unchanged - only the total wait is now
  bounded; the poll cadence matches prior behavior and was flagged for completeness, not as a defect.

### Verification (post-review)
- `Issue4803GrpcResultBoundingIT` 4/4 (includes the two new cap-failure tests; server log shows the
  `RESOURCE_EXHAUSTED` unary cap-breach).
- `Issue4803WaitUntilReadyTimeoutTest` 3/3 (monotonic-deadline change).
- Regression: `GrpcStreamMetricsIT` 1/1, `Issue4197GrpcExecuteQueryResultSetLeakIT` 1/1 - no regressions.
