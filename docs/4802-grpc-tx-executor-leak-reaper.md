# Issue #4802 - gRPC per-transaction executor + open transaction leak (no idle reaper)

## Problem

`ArcadeDbGrpcService.beginTransaction` creates, per call:
- a dedicated single-thread daemon executor (`arcadedb-tx-<id>`),
- an open ArcadeDB transaction (holding locks/WAL),
- a `Database` reference,

all registered in `activeTransactions`. These are released only on an explicit
`commitTransaction` / `rollbackTransaction`, or on server shutdown (`close()`).

There is no association with the gRPC call lifecycle and no idle reaper, so a
client that calls `beginTransaction` and then disconnects (crash, network drop,
forgotten handle) leaks the executor thread, the open transaction and the
database reference indefinitely -> resource exhaustion + lock/WAL retention.

## Root cause

`TransactionContext` carries no notion of when it was created or last used, and
the service never sweeps `activeTransactions`. Nothing ties a registered
transaction to liveness.

## Fix

1. `TransactionContext` now records `createdAtMs` and a `volatile lastAccessMs`,
   with a `touch()` that refreshes `lastAccessMs`. Every lookup of an active
   transaction (`executeCommand`, insert, batch, etc.) goes through a single
   `lookupActiveTransaction(txId)` helper that touches the context, so genuine
   activity keeps a transaction alive.
2. A single daemon `ScheduledExecutorService` ("arcadedb-grpc-tx-reaper") runs
   `reapIdleTransactions()` on a fixed delay. A transaction is reaped when it has
   been idle longer than `maxIdleMs`, or (optionally) older than `maxAgeMs`.
   Reaping atomically removes it from `activeTransactions` (`remove(key, value)`,
   so it never races a concurrent commit/rollback), submits a rollback on its own
   dedicated thread without blocking the reaper (`shutdown()` lets that rollback
   finish before the executor terminates), and releases the executor. Each reaped
   transaction is logged at FINE; one summary WARNING per sweep reports the count
   reclaimed, so a burst of abandoned transactions cannot flood the log.
3. `close()` stops the reaper first, then performs the existing drain.
4. The thresholds are configurable through `GrpcServerPlugin`:
   - `arcadedb.grpc.tx.maxIdleMs`     (default 300000 = 5 min)
   - `arcadedb.grpc.tx.maxAgeMs`      (default 0 = disabled)
   - `arcadedb.grpc.tx.reaperPeriodMs` (default 30000 = 30 s)
   Setting both idle and age to 0 disables the reaper entirely.

## Verification

New integration test `GrpcTransactionReaperIT` (grpcw module):
- configures a short idle timeout + reaper period via system properties,
- begins a transaction and writes inside it (does not commit, simulating an
  abandoned client),
- asserts the active-transaction count drops back to 0 once the reaper runs,
- asserts a later commit on the reaped id reports `committed=false`.

Before the fix the transaction is never reaped (count stays 1, commit succeeds),
so the test fails; after the fix it passes.

Existing `GrpcServerIT` begin/commit/rollback tests confirm no regression to the
normal transaction lifecycle.

## Review cycles

- Cycle 1 (claude + gemini): both flagged the synchronous `.get()` blocking the
  reaper thread and the "throttled WARNING" wording not matching per-transaction
  logging; claude additionally asked for max-age and disabled-reaper test
  coverage and a TOCTOU note. Addressed in commit 66337b09b: async rollback via
  cooperative `shutdown()`, one summary WARNING per sweep (per-tx at FINE),
  TOCTOU comment, plus `ArcadeDbGrpcServiceReaperTest` and
  `GrpcTransactionMaxAgeReaperIT`.
- Cycle 2 (re-review): gemini approved ("ready to go"); claude verified every
  prior item resolved ("ready to merge"), with a single explicitly non-blocking
  observation about the broad catch in the busy-loop test.

## Final state

Clean approval from both reviewers after 2 cycles. No outstanding actionable
feedback.
