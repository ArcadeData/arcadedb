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
  helpers shared by both failure paths; (5) the contract-change test asserts the `CONTRACT_VIOLATION`
  branch (a pre-insert contract rejection is not a commit failure).
- **Cycle 3 - claude** (post-rebase onto `main`, which now includes #4801's thread-safe `out` observer):
  - **(#1, medium) Fixed summary over-reporting `inserted`/`updated` after a mid-stream rollback.** In
    `PER_STREAM`/`PER_REQUEST` (and `PER_BATCH` when the batch is larger than the chunk) earlier rows stay
    uncommitted; `abortTransaction()` rolls them back but they were already counted. `abortTransaction()`
    now returns whether it actually rolled back an active transaction; when it did, the `onNext` catch
    reclassifies the still-uncommitted `inserted`/`updated` as `failed` (symmetric to
    `recordCommitException`). When nothing was rolled back (`PER_BATCH`/`PER_ROW` whose earlier batches
    already committed) the counts stand. The contract-change test now asserts `inserted == 0`,
    `received == 2`, `failed == 2` (counts balance).
  - **(#5, edge) Closed a transaction leak in the `InsertContext` constructor.** If construction threw
    after `db.begin()` but before the context reached `ctxRef`, the mid-stream catch could not roll the
    begun transaction back (it saw `ctxRef.get() == null`). The constructor now rolls back its own
    just-begun transaction on failure before propagating.
  - **(#2, minor) Not changed:** `commitErrorCode()` keeps `COMMIT_FAILED` as the catch-all even for a
    pre-loop structural failure (e.g. a missing target type from `schema.getType`). The `InsertError.code`
    values are a client-visible contract; renaming the default is itself a compatibility change for a
    cosmetic mislabel, so the dominant-case label stands.
  - **(#3, minor) Error-code set documented** (below).

## InsertError.code values (client-facing contract)

`InsertError.code` is a free-form string; clients may switch on it. The full set currently emitted by the
insert paths:

- `CONFLICT` - a unique-key/`DuplicatedKeyException` violation.
- `CONTRACT_VIOLATION` - a stream contract rejection (`IllegalArgumentException`, e.g. "options changed
  mid-stream"); not a commit failure.
- `COMMIT_FAILED` - any other transaction-level failure escaping `insertRows` (catch-all).
- `DB_ERROR` - a per-row insert/update error caught inside `insertRows`.
- `MISSING_ENDPOINTS` - an edge row missing its required `from`/`to` endpoints.
- `INVALID_KEY_COLUMN` - a key-column reference that does not resolve on the row.

## Final state

Fix complete. 2 new ITs + 21 existing insertStream ITs (#4198/#4214/#4644/#4656) pass; no regressions.
Rebased onto `main` (resolves the #4801 thread-safe-observer overlap in `ArcadeDbGrpcService`).
