# #5149 - SQL `count(*)` returns stale cached bucket counter

## Symptom
`SELECT count(*) FROM <Type>` returns fewer records than `SELECT count()` / a full scan / an export. Studio (which uses `count(*)`) shows the wrong number. Reported on 26.7.1: `count()`=676 vs `count(*)`=606.

## Root cause
`count(*)` and `count()` take different paths:
- `count(*)` matches `SelectExecutionPlanner.isCountStar()` -> `CountFromTypeStep` -> `LocalDatabase.countType()` -> sums `LocalBucket.count()`, which returns the **cached** `cachedRecordCount` (+ current tx delta) instead of scanning.
- `count()` and `count(<field>)` do a real record scan and are always accurate.

`cachedRecordCount` is maintained incrementally (insert +1, delete -1, folded at commit) and persisted to `<db>/statistics.json`. It is only recomputed when a bucket's value is `-1` or after an unclean shutdown. If it ever drifts it is persisted as-is and `count(*)` stays wrong. `CHECK ... FIX` computed the true active-record count in its report but never wrote it back, so no user-facing command reconciled the counter.

## Fix
In `LocalBucket.check(verboseLevel, fix)`, when `fix` is true, invalidate the cached counter (`cachedRecordCount.set(-1)`) after the page scan. The next `count()`/`count(*)` then performs an authoritative full scan and repopulates the counter with the correct value.

Invalidation (rather than writing a concrete value inside `check()`) is deliberate. `check(fix=true)` may run inside a caller-managed transaction, and at commit `TransactionContext` folds that transaction's accumulated bucket delta into `cachedRecordCount` only when it is `> -1`. Writing a freshly scanned value would let unrelated inserts/deletes in that same transaction be double-counted on top of it; leaving `-1` makes the fold skip and defers to an authoritative recompute. The `-1` sentinel is the existing, correct "recompute on next read" signal, so the repopulated value comes from `count()`'s own scan logic and survives a rollback of the enclosing transaction. (Note `check()`'s own corrupt-record deletions go through `LocalBucket.deleteRecordInternal`, which does not register a bucket delta, so they are not what the guard protects against.)

A narrow caveat remains: no `count()` may run on the bucket between the invalidation and the checker's commit (from this caller or a concurrent transaction), or it would repopulate the counter and let the commit-time fold re-apply the delta. That window is inherent to the incremental counter design; `CHECK ... FIX` is an admin operation.

## Verification
- New regression test `Issue5149CountStarCacheDriftTest`: inserts records, corrupts the cached counter, asserts `count(*)` diverges from `count()` (reproduces the bug), runs `CHECK DATABASE ... FIX`, asserts `count(*)` == `count()` == actual.
- Related suites: `CheckDatabaseExtendedTest`, `CRUDTest`.

## Impact
`CHECK DATABASE FIX` / `CHECK DATABASE TYPE <t> FIX` now repairs a drifted `count(*)`. No hot-path behavior changes; the invalidation only runs under an explicit fix. Operator note: because every in-scope bucket is invalidated (we cannot know which drifted), the first `count(*)` after a FIX triggers a one-time full rescan of those buckets - a noticeable but bounded one-off cost on very large datasets.

## PR and review history
- PR: https://github.com/ArcadeData/arcadedb/pull/5150
- Review cycles (bots: `claude`, `gemini-code-assist`):
  - cycle 1 (`12f63561`): gemini no actionable feedback; claude LGTM + 3 non-blocking suggestions. Applied: persist/reopen round-trip test, `count()`-during-fix caveat comment.
  - cycle 2 (`b7844628`): claude LGTM; gemini did not re-review (known unreliable). Applied: concurrency-window note (real race = `publishPages`->fold window in `TransactionContext`).
  - cycle 3 (`6c18e9d0`): claude LGTM with one substantive catch - the comment's rationale was inaccurate (`deleteRecordInternal` registers no bucket delta; the real reason is deltas from other ops in a caller-managed enclosing tx). Verified in-source and corrected the comment, this doc, and the PR body; marked all 4 tests in the test plan.
  - cycle 4 (`1bc42f6e`): claude verdict "Correct, safe, and well-tested"; confirmed the corrected rationale is accurate. gemini did not re-review.
- Final state: `max-cycles-reached` (converged - every cycle LGTM; only non-blocking polish remained).

## Deferred (non-blocking) follow-ups for the developer
1. Optional: add a test that corrupts a real record so `check(fix=true)` deletes it, then asserts `count(*)` == `count(@rid)` == `(TOTAL - deleted)` - locks in the delete-no-delta -> invalidate -> rescan path directly (currently covered indirectly by `CheckDatabaseTest` et al.).
2. Possible follow-up issue: root-cause why the incremental counter drifts in the first place (dropped delta vs. unclean-shutdown edge) so users need not run FIX repeatedly. This PR is a reactive reconciliation tool by design.
