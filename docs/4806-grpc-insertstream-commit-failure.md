# Issue #4806 - gRPC insertStream keeps processing after a commit failure and mis-indexes the error

## Problem

`ArcadeDbGrpcService.insertStream` (`onNext`): when a chunk insert throws (e.g. a
`PER_BATCH`/`PER_ROW` mid-stream `flushCommit` failure, such as a deferred unique-index
violation surfacing at `db.commit()`), two defects occur:

1. **Mis-indexed error.** The `catch` records the failure against a single context-wide row
   index (`ctx.received - 1`) with a generic `DB_ERROR` code, even though a transaction-level
   commit failure is not attributable to one specific row.
2. **Keeps processing on a broken transaction.** The `flushCommit` that failed has already
   rolled the transaction back (the post-commit `db.begin()` never ran), but the `finally`
   keeps pulling more chunks (`call.request(1)`) and `onNext` re-binds and inserts into the now
   rolled-back transaction, producing further spurious `DB_ERROR`s and losing rows.

## Root cause

Per-row errors are caught row-by-row inside `insertRows`. Any exception that *escapes*
`insertRows` is therefore a transaction-level/structural failure (mid-stream commit). The
`onNext` handler treated it like a recoverable per-row error: it recorded it at `ctx.received-1`
and let the stream continue against the dead transaction.

## Fix

In `insertStream.onNext`, on an exception escaping `insertRows`:
- mark the stream failed (`AtomicBoolean streamFailed`),
- record the failure at the transaction level (`rowIndex = -1`) with code `CONFLICT`
  (for `DuplicatedKeyException`) or `COMMIT_FAILED` otherwise, instead of `ctx.received-1`/`DB_ERROR`,
- short-circuit subsequent `onNext` chunks (drain inbound to reach `onCompleted` but skip
  inserting), so subsequent rows never touch the rolled-back transaction.

In `onCompleted`, skip the deferred `flushCommit` when the stream already failed mid-stream (the
transaction is gone) and still deliver a structured `InsertSummary` (consistent with #4198/#4214,
which prefer a structured summary over `Status.INTERNAL`).

## Test

`Issue4806InsertStreamMidStreamCommitFailureIT` - PER_BATCH, `server_batch_size = 1`, UNIQUE index,
duplicate row triggers a mid-stream commit failure followed by a would-be-valid row. Asserts a
single transaction-level error (`rowIndex == -1`, code `CONFLICT`) and that the subsequent row is
not processed against the broken transaction.

## Status

- [x] Analysis
- [x] Test (fails before fix)
- [x] Fix
- [x] Verify

## Review cycles

- **Cycle 1 - gemini-code-assist** (transaction leak): a transaction-level failure that is not a
  commit failure (e.g. "options changed mid-stream") leaves the transaction active and bound to the
  pooled gRPC thread. Added `InsertContext.abortTransaction()` (rolls back only if still active on the
  calling thread, then drops the captured handle; never re-binds a dead transaction). Called from the
  `onNext` catch and defensively from `onCompleted` when `streamFailed`. Added the contract-change
  regression test.
- **Cycle 2 - claude**: (1) counted the failing chunk's rows as `received` so the summary can no
  longer report `failed > received`; (2/3) documented the intentional all-or-nothing stance and the
  asymmetry with `recordCommitException`; (4) extracted `commitErrorCode()`/`exceptionMessage()`
  helpers shared by both failure paths; (5) the contract-change test covers the `COMMIT_FAILED` branch.

## Final state

Fix complete. 2 new ITs + 21 existing insertStream ITs (#4198/#4214/#4644/#4656) pass; no regressions.
