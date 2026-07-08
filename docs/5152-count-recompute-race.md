# #5152 - count(*) drift root cause: lazy cached-counter recompute races with concurrent commits

## Symptom
`SELECT count(*)` permanently under/over-reports vs `count()` / a real scan (the drift behind #5149). PR #5150 added a reactive repair (`CHECK ... FIX` invalidates the counter); this fixes the underlying cause.

## Root cause
`count(*)` is served from `LocalBucket.cachedRecordCount`. That counter is repopulated from a scan only on the `cachedRecordCount == -1` branch of `LocalBucket.count()`, via an unconditional `set(total)` after a lock-free page scan. Commits fold their per-bucket deltas at `TransactionContext.commit2ndPhase` (guarded by `> -1`, so a `-1` counter drops the delta). Both `commit()` and `countType()` run under `executeInReadLock` (concurrent holders), and commits additionally hold the per-bucket file lock across `publishPages` + the fold - but the recompute took no file lock, so it was not serialized against commits.

Race (counter at `-1`, concurrent writes): a recompute scans while a commit publishes a record and folds its delta; the fold sees `-1` and drops the delta, and the recompute stores a total that missed the record. The committed record is absent from the counter until the next `-1` recompute -> persistent drift. A seqlock/version probe cannot fix it: the record becomes scan-visible at `publishPages` but the counter is folded later, so a version-only scheme still admits a double-count in that gap.

Vulnerable windows (counter `== -1` with concurrent load): first `count(*)` after a fresh open with no `statistics.json` entry; after an unclean shutdown; after `CHECK ... FIX` (#5150).

## Fix
In `LocalBucket.count()`, the recompute path (scan + `set`) now acquires the bucket's own file lock (via `TransactionManager.tryLockFile`, `COMMIT_LOCK_TIMEOUT`) - the same lock a commit holds across `publishPages` + fold - making the recompute mutually exclusive with commits on that bucket. Details:
- Taken only on the rare recompute path (counter unknown, or no active transaction); the O(1) cached fast-path is untouched.
- Requester mirrors the current transaction's (`getRequester()`) so a recompute invoked while the same transaction already holds the lock re-enters (`ALREADY_ACQUIRED`) instead of self-deadlocking; the lock is released only when this call actually acquired it (`YES`).
- On lock-acquire timeout (`NO`) it falls back to a lock-free scan and returns a best-effort value but does **not** cache it (leaves the counter at `-1`), so a possibly-drifted value is never persisted and a later call recomputes cleanly.
- No deadlock: a recompute locks a single file and does no further lock acquisition, so it cannot form a cycle with the ordered multi-file locks commits take; both acquire the shared DB read lock first.
- The cached fast-path now returns the counter directly whenever it is known (`> -1`), for both transactional and non-transactional calls (adding the transaction delta only when a transaction is active). Previously a call with no active transaction always did a full O(N) scan, contradicting the O(1) contract relied on by cardinality estimation (`StatisticsProvider`) and external-bucket cleanup; the recompute path is now entered only when the counter is genuinely unknown.

## Verification
- New stress regression `Issue5152CountRecomputeRaceTest` (`@Tag("slow")`): large single bucket forced to `-1`, a reader triggers a long recompute while a writer commits concurrently; asserts `count(*) == count(@rid)`. Red before the fix (e.g. 52998 vs 53000), green after (repeated runs).
- Regressions green: `Issue5149CountStarCacheDriftTest`, `CheckDatabaseTest`, `CheckDatabaseExtendedTest`, `CRUDTest`, `ConcurrentWriteTest`, `MVCCTest`, `ReusingSpaceTest`, `TransactionTypeTest`, plus `RandomTestMultiThreadsTest` (32 concurrent workers) and `PolymorphicTest` - no deadlock or perf regression.

## Impact
Eliminates the source of `count(*)` drift. Combined with #5150 (which repairs already-drifted databases and now recomputes race-free), `count(*)` stays consistent under concurrent load. No change to the read fast-path; the file lock is engaged only during a rare full recompute.
