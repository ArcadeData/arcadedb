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
