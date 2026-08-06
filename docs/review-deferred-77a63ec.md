# Review disposition — PR #5836, cycle 3, commit 77a63ec

Bot: `claude[bot]` (issue comment, id 5201025946).

## Applied

1. **Plan-quality staleness when a GAV is built after a value is already cached.** Verified: real gap -
   the count-stamp cannot detect a GAV build/drop transition, since neither changes the edge type's
   record count. Implemented the reviewer's suggested cheap fix: `GraphAnalyticalView.build()`,
   `buildAsync()`, and `shutdown()` now call `getGraphStatisticsCache().clear()` (via
   `DatabaseInternal.unwrap`, same pattern `GraphTraversalProviderRegistry` already uses). Added
   `meanEdgesPerConnectedPairPicksUpANewlyBuiltGAVEvenThoughTheEdgeCountDidNotChange` to
   `StatisticsProviderTest`, verified to fail (1000 vs 667) when the invalidation calls are reverted.

2. **Potential NPE when `database != null` but `getGraphStatisticsCache()` returns null.** Verified: real
   latent gap, not hit by any existing test today but a genuine contract mismatch with the constructor's
   own `database != null` guard. Added `graphStatisticsCache != null` to both call sites'
   cache-branch conditions in `getMeanEdgesPerConnectedPair`/`getAverageDegree`. Added
   `getMeanEdgesPerConnectedPairAndAverageDegreeDoNotNpeWhenTheSharedCacheIsUnavailable` (reflectively
   nulling the field to simulate the scenario, since building a full custom `DatabaseInternal` test
   double just for this is disproportionate to the fix).

## Skipped, with rationale

3. **`getAverageDegree` cache keyed on edge count only.** Third time this exact point has been raised
   (cycle 1 point 2, cycle 2 point 1, cycle 3 point 3), each time explicitly framed as "already
   documented," "accepted," or "flagging only for the record" - not a request for a change. No further
   action; the tracking doc already covers this tradeoff in the "Database-scoped cache" section.
