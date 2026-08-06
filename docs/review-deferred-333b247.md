# Review disposition — PR #5836, cycle 1, commit 333b247

Bot: `claude[bot]` (issue comment). Full text: https://github.com/ArcadeData/arcadedb/pull/5836#issuecomment-5200868489

## Applied

1. **Clamp asymmetry between sampled and CSR-exact paths.** Verified: intentional, not a bug — the
   sampled path clamps to guard against a *sampling* artifact (a pathologically clustered prefix); the
   CSR-exact path has no such bias to guard against, since it is the true population mean. Added a
   Javadoc note on `GraphAnalyticalView.getMeanEdgesPerConnectedPair` making this explicit, and corrected
   the tracking doc to state it.
3. **Doc/code mismatch on Snapshot memoization.** Verified: real mismatch. The design doc originally
   claimed the GAV result is memoized on the `Snapshot`; the committed code has no such memoization (the
   database-scoped `GraphStatisticsCache` in `StatisticsProvider` was the caching layer actually built).
   Corrected the tracking doc to describe the real design and explain why a second memoization layer
   inside `GraphAnalyticalView` was not added.

## Skipped, with rationale

2. **averageDegree invalidation ignores vertex-count changes.** The reviewer itself frames this as "flagging
   only so it is a conscious call," not a request for a code change — and the tracking doc already
   documents this exact tradeoff in the "Database-scoped cache" section (average degree's cache validity
   is keyed on edge-type record count only, not source/target vertex counts, to keep the two cached
   quantities' invalidation semantics identical). No change: this is a deliberate, already-documented
   heuristic-cache tradeoff for a cost-model estimate, not a correctness bug.

4. **Performance: O(E) first-call cost for large edge types.** The reviewer explicitly assesses this as
   "fine" ("exactness is the point... noting it because..."), not an ask. No change.

5. **`MULTIPLICITY_UNKNOWN` is a `long` widening to `double`.** Correct as stated, but the constant is
   shared with `countEdgesBetween` (a `long`-returning method) elsewhere in the same class; introducing a
   second `double`-typed "unknown" sentinel purely for one call site duplicates the same semantic value in
   two places for a marginal readability gain. Left as-is (widening is well-defined and already tested by
   the `isNegative()` assertions).

6. **Orphaned `GraphStatisticsCache` entries on dropped/recreated edge types.** The reviewer explicitly
   assesses this as "a non-issue in practice" and "fine to leave as-is." No change.
