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

## Review cycle 2 (claude + gemini-code-assist)

Claude's re-review was an approval with nits ("clean, well-tested DoS hardening"); gemini re-posted
its cycle-1 review verbatim.

### Applied
- **Config rationale for the 100k vs 1M asymmetry (claude).** The unary cap description now explains it
  is lower than `grpcStreamMaxMaterializedRows` because the unary response is a single gRPC message
  (also bounded by the max message size), whereas StreamQuery emits incrementally.
- **Disabled-path regression test (claude).** Added `unaryExecuteQueryWithCapDisabledReturnsAllRows`:
  sets the cap to `-1` and asserts the limitless query returns the full result (well past the former
  ceiling) without `RESOURCE_EXHAUSTED`, locking in that the opt-out does not silently regress.

### Not actioned (with rationale)
- **gemini line-1690 `System.currentTimeMillis()` re-flag.** Stale: `awaitTransportReady` was already
  converted to a monotonic `System.nanoTime()` deadline in cycle 1, and gemini's suggested replacement
  is functionally identical to the current code. The only `currentTimeMillis` token left in that method
  is inside an explanatory comment. No change.
- **`count >= configuredMax && resultSet.hasNext()` double-evaluates `hasNext()` (claude minor).**
  Idempotent for `ResultSet`; harmless, not worth restructuring.
- **Write-timeout path conflates server deadline with client cancel (claude).** Confirmed conscious gap,
  already deferred in cycle 1; the per-sweep WARNING gives operators visibility.
- **Timing-based / hardcoded-port test notes (claude).** Bounds are generous and the port matches the
  existing gRPC IT convention; no change.

### BREAKING CHANGE - for release/upgrade notes
The unary `ExecuteQuery` change is **backward-incompatible by design**: a limitless query whose result
exceeds `arcadedb.server.grpcQueryMaxResultRows` (default 100000) now fails with `RESOURCE_EXHAUSTED`
instead of returning everything. This is the correct DoS posture (loud failure over silent truncation)
and is opt-out via `-1`/`0`, but clients running large limitless unary gRPC queries will get hard errors
after upgrade. **This must be surfaced in the release notes / upgrade guide, not only in this doc.**

### Verification (post-cycle-2)
- `Issue4803GrpcResultBoundingIT` 5/5 (adds the disabled-cap opt-out test).
- `Issue4803WaitUntilReadyTimeoutTest` 3/3.

## Review cycle 3 (claude + gemini-code-assist)

Claude re-surfaced the write-timeout client-status gap and asked for it to be resolved before merge
(escalated from a deferred follow-up). Acted on it this cycle.

### Applied
- **Explicit `DEADLINE_EXCEEDED` on a server-induced write timeout (claude #1, previously deferred).**
  A server timeout previously set the shared `cancelled` flag and the stream loop returned with no
  terminal status, leaving a healthy-but-slow client blocked until its own deadline - the exact "stream
  silently stops" symptom a DoS feature should not create. Introduced a per-call `serverTimedOut`
  `AtomicBoolean`, threaded from `streamQuery` through `streamCursor`/`streamMaterialized`/`streamPaged`
  into `waitUntilReady`, which now sets it (alongside `cancelled`) only on the deadline path - never on a
  genuine client cancel. `streamQuery`'s cancelled branch surfaces `Status.DEADLINE_EXCEEDED` (before the
  best-effort rollback, so the client is signaled even if rollback throws) when `serverTimedOut` is set,
  and stays silent for a real client cancel (transport already tearing down). Single terminal preserved:
  the catch block only emits when `!cancelled.get()`, so there is no double-terminal.

### Not actioned (with rationale)
- **gemini line-1690 `currentTimeMillis` (3rd identical post).** Still stale - the method has used a
  monotonic `nanoTime` deadline since cycle 1; gemini re-posts its whole original review each push.
- **`MATERIALIZE_ALL` 1M default may still spike heap (claude #2).** Deliberate tradeoff, "no change
  strictly required"; the default is the operator's tuning call and is documented with its rationale.
- **Boundary-semantics one-row asymmetry, poll granularity, test-config/port nits (claude).** All
  confirmed harmless / convention-matching; not worth a behavior change.

### Known test gap
The `DEADLINE_EXCEEDED` emission is not covered by an end-to-end IT: deterministically forcing
`ServerCallStreamObserver.isReady()` to stay false against a real client transport is not feasible in
this harness (the same reason the original PR unit-tested only the `awaitTransportReady` helper). The
threading and the helper's deadline logic are covered by `Issue4803WaitUntilReadyTimeoutTest` (3/3) plus
the full stream-path regression (no regressions).

### Verification (post-cycle-3)
- `Issue4803GrpcResultBoundingIT` 5/5, `Issue4803WaitUntilReadyTimeoutTest` 3/3.
- Regression: `GrpcStreamMetricsIT` 1/1, `Issue4197GrpcExecuteQueryResultSetLeakIT` 1/1 - no regressions.

## Review cycle 4 (claude + gemini-code-assist) - final

Claude's re-review verified correctness across the board (unary cap boundary, ResultSet lifecycle,
single-terminal, status propagation) and concluded "**Nothing here is blocking**". This is the last cycle
in the `--max-cycles=4` budget.

### Applied
- **Bail out immediately after a write-timeout (claude #2).** `waitUntilReady` flags
  `cancelled`/`serverTimedOut` but does not itself return, so `streamCursor`/`streamPaged`/`streamMaterialized`
  would convert and (no-op) emit one more row/batch before the loop-top `cancelled` check caught it. Added an
  explicit `if (cancelled.get()) return;` right after each `waitUntilReady` call so an aborted stream stops
  promptly without the wasted work - the right posture for a DoS-protection path.

### Not actioned (with rationale)
- **gemini line-1710 `currentTimeMillis` (4th identical post).** Stale - `awaitTransportReady` has used a
  monotonic `nanoTime` deadline since cycle 1; gemini re-posts its entire original review on every push.
- **Hoist `getValueAsLong()` out of the per-row `waitUntilReady` call (claude #4, optional).** It is a cached
  field read (negligible per claude); hoisting would thread `timeoutMs` through three helper signatures for no
  measurable gain. Skipped deliberately.
- **Unit test for the `serverTimedOut` flag wiring via a stub `ServerCallStreamObserver` (claude #3, optional).**
  The deterministic deadline logic already lives in the unit-tested `awaitTransportReady`; the flag flip in
  `waitUntilReady` is a two-line glue. A full stub observer (many abstract methods) is disproportionate to that
  glue. Left as the acknowledged e2e gap.
- **100k unary default / release notes (claude #1).** Default tuning and release-note placement are the
  maintainer's call; the breaking change is documented in the "BREAKING CHANGE" section above.

### Verification (post-cycle-4)
- `Issue4803GrpcResultBoundingIT` 5/5, `Issue4803WaitUntilReadyTimeoutTest` 3/3.
- Regression: `GrpcStreamMetricsIT` 1/1, `Issue4197GrpcExecuteQueryResultSetLeakIT` 1/1 - no regressions.

## Final state

`max-cycles-reached` (4/4) with a clean approval from claude on the final commit and no outstanding blocking
feedback. Remaining gemini item is stale; remaining claude items are optional/deferred with rationale above.
Merge remains the developer's responsibility.
