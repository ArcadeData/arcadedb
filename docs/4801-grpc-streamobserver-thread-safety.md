# Issue #4801 - gRPC streaming terminal calls on a non-thread-safe StreamObserver

## Problem

gRPC `StreamObserver` is not thread-safe. In `ArcadeDbGrpcService`:

- `graphBatchLoad` made terminal/write calls (`onNext`/`onError`/`onCompleted`) directly on the
  raw observer, guarded only by a TOCTOU `if (!cancelled.get())` check. A cancellation racing the
  final terminal call could deliver a write/terminal on an already-closed call.
- `insertBidirectional` dispatched database work to a single-threaded executor and called terminal
  methods from those executor tasks, while the cancel handler ran on the transport thread. A
  duplicate terminal (e.g. a COMMIT followed by a stray message hitting the defensive catch) or a
  cancellation could interleave terminal calls.

Symptoms: `IllegalStateException` ("call already closed"), dropped responses, or client hangs.

## Fix

Added `SynchronizedStreamObserver<T>` (grpcw, package `com.arcadedb.server.grpc`): a thread-safe
wrapper that:

- serializes every call through a single monitor;
- uses an `AtomicBoolean completed` CAS so at most one terminal call (`onError`/`onCompleted`) is
  delegated and any `onNext` after a terminal call is dropped;
- exposes `markTerminated()` for the call's cancel handler to flip the flag without delegating
  (the transport already closed the call);
- swallows the `IllegalStateException` the delegate throws if the call was concurrently
  closed/cancelled, marking itself terminated so nothing further is attempted.

Both `graphBatchLoad` and `insertBidirectional` now wrap their response observer in
`SynchronizedStreamObserver` and route all write/terminal calls through it. Their cancel handlers
(and the bidirectional `onError`/cleanup path) call `markTerminated()`.

The original `ServerCallStreamObserver` reference (`call`) is kept for flow-control
(`disableAutoInboundFlowControl`, `setOnCancelHandler`, `request`).

## Tests

`Issue4801SynchronizedStreamObserverTest` (unit, mock-free) drives the wrapper against a delegate
that emulates gRPC's real contract (throws `IllegalStateException` on a second terminal or on a
write after close):

- `onNextAfterCompletedIsDropped`
- `duplicateTerminalCallsAreCollapsedToOne`
- `markTerminatedDropsSubsequentCallsWithoutDelegating`
- `delegateThrowingOnConcurrentCloseIsSwallowedAndTerminates`
- `concurrentTerminalAndWriteCallsNeverInterleave` (200 iterations x 32 threads racing
  onNext/onCompleted/onError/markTerminated)

Red/green verified: with a naive pass-through wrapper the suite fails with the exact
`IllegalStateException: call already closed` from the issue; with the fix all 5 pass.

Regression: `GrpcServerIT#graphBatchLoadVerticesAndEdges` still passes end-to-end.

## Impact

- Scope limited to grpcw streaming handlers. Unary RPCs were already single-threaded and untouched.
- No new dependencies. Negligible overhead (one monitor + one AtomicBoolean per stream).

## Review cycles

### Cycle 1 (gemini-code-assist + claude)

- gemini: also catch `io.grpc.StatusRuntimeException` (not only `IllegalStateException`) in all
  three guarded delegate calls, since gRPC can throw a `StatusRuntimeException` (e.g. CANCELLED)
  when the transport closes the call. Applied via multi-catch with an explicit import.
- claude: `insertStream` (`ArcadeDbGrpcService.java:1646`) has the identical TOCTOU
  cancel-vs-terminal race and was left untouched. Applied the same `SynchronizedStreamObserver`
  wrapper to `insertStream` for consistency (cancel handler + onError + onCompleted paths).
- claude (test suggestion): added `happyPathDeliversExactlyOneTerminalToDelegate` asserting exactly
  one terminal reaches the delegate on the no-cancel path.
- Verified: `Issue4801SynchronizedStreamObserverTest` 6/6, plus `GrpcServerIT#graphBatchLoadVerticesAndEdges`
  and `Issue4198InsertStreamCommitErrorIT` pass end-to-end.

### Cycle 2 (claude - "looks good to merge")

- Clarity-only: documented why `markTerminated()` is lock-free (cancel handler must not block behind
  an in-flight `onNext`) and why `graphBatchLoad`'s `errorSent` flag is retained.
- executeQuery / streamMaterialized-Cursor-Paged path left out of scope (single producer thread, already
  self-heals via `safeOnNext` catching `StatusRuntimeException`); flagged as a dedicated follow-up.

### Cycle 3 (claude)

- Acted on the one pre-merge item: `onNext` now best-effort delivers `onError` when a write throws, so a
  still-live call that rejects a single message (e.g. `RESOURCE_EXHAUSTED`) does not leave the client
  hanging without a terminal. `compareAndSet` skips it if a concurrent `markTerminated` already fired; an
  already-closed call swallows the no-op `onError`.
- Added `failedWriteOnLiveCallDeliversTerminalSoClientDoesNotHang`. Unit suite now 7/7.
- `insertBidirectional` half-close-without-COMMIT delivering no terminal is pre-existing and left as a
  documented follow-up.
