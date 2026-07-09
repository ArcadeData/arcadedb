# Issue #5041 - gRPC insert-stream transaction lifecycle

## Symptom
`InsertContext` in `ArcadeDbGrpcService` mismanages ArcadeDB's thread-bound transactions across gRPC
callbacks, causing silent data loss, transaction leaks that corrupt unrelated requests, and wrong
half-close semantics.

## Root cause (findings from the 2026-07 gRPC audit)
- **TX-3** `InsertContext.close()` is an empty no-op, so `closeQuietly()` on every error/cancel path
  never rolls back the transaction begun in the ctor. The orphaned, still-active transaction stays
  bound to the pooled gRPC thread; the next unrelated request reusing that thread silently joins it.
- **TX-4** `insertStream` runs its callbacks on shared gRPC pool threads and leaves the live
  transaction parked in the thread-local `DatabaseContext` between callbacks. An unrelated request on
  the same pool thread calls `DatabaseContext.init()`, which rolls back the parked transaction -
  silently discarding the stream's rows.
- **TX-6** `insertBidirectional.onCompleted` (half-close without an explicit COMMIT) calls
  `flushCommit(false)`, which actually COMMITS for `PER_ROW`/`PER_BATCH` (`db.commit(); db.begin()`)
  and leaks the open tx for `PER_STREAM` - contradicting the comment's stated "rollback" intent.
- **CON-4** `insertBidirectional.onNext` submits to `streamExecutor` without guarding against a
  `RejectedExecutionException` thrown when a racing cancel shut the executor down (`onCompleted`
  already guards this).
- **CON-1** `insertBidirectional` submits work to `streamExecutor` without propagating
  `io.grpc.Context`, so header/Bearer-only auth (`USER_CONTEXT_KEY`) is invisible on the worker thread.

## Fix
- **TX-3** `close()` now rebinds the captured transaction to the current thread and rolls it back if
  still active (via the existing `abortTransaction()`), then clears the handle.
- **TX-4** new `unbindFromCurrentThread()` detaches the captured transaction from the thread-local
  `DatabaseContext` (without rolling it back) at the end of each `insertStream.onNext`;
  `bindToCurrentThread()` re-attaches it on the next callback.
- **TX-6** half-close without COMMIT now calls `abortTransaction()` (explicit rollback) for all modes.
- **CON-4** the `onNext` submit is wrapped in the same `try/catch (RejectedExecutionException)` guard
  as `onCompleted`.
- **CON-1** capture `Context.current()` at observer creation and wrap every submitted runnable with
  `grpcContext.wrap(...)`.

## Tests
`grpcw/.../Issue5041InsertStreamTxLifecycleIT`:
- TX-3: stream error mid-way leaves no active transaction bound to the thread.
- TX-4: an unrelated `DatabaseContext.init()` on the same thread cannot roll back the stream's tx.
- TX-6: bidirectional half-close without COMMIT commits nothing.

## Impact
Only `grpcw` `ArcadeDbGrpcService`. No wire/proto changes. Behavior preserved for payload-credential
clients and the existing #4644/#4198/#4806 insert-stream regression tests.
