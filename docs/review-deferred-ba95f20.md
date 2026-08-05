# Deferred review items - PR #5832, cycle 1 (head ba95f20f9)

Reviewer: claude[bot] (issue comment, 2026-08-05T21:05:24Z)

Applied this cycle: stale class Javadoc ("O(1)" claim), prefix-sampling-bias comment, removed a
copy-pasted `@author` tag from the new test, added an IN-direction bound-target-hop test.

## Deferred - actionable but needs a design decision

**Point 1 (deeper ask): share/memoize `getMeanEdgesPerConnectedPair` beyond one query's
`StatisticsProvider` instance, or replace sampling with a cheaper O(1) proxy.**

Verbatim: "Can this be memoized on the database (or on a longer-lived stats holder) rather than
recomputed per optimizer instance? ... Consider whether a cheaper proxy is acceptable... falling
back to sampling only when needed."

Not applied because it requires a decision this loop cannot make unattended: where would a
database-lifetime cache for this statistic live, and what invalidates it as edges are
inserted/deleted (a stale multiplicity estimate is a correctness-adjacent regression risk, not just a
performance one)? `averageDegreeCache` has the identical per-query lifetime and the identical
staleness question already - this would be a design change to the class's caching model, not a
local fix. Worth a follow-up issue: give `StatisticsProvider` (or a new holder) a database-scoped
cache with an explicit invalidation trigger, and reconsider whether sampling is even necessary if a
cheap proxy from existing O(1) counts can approximate it.

## Skipped - not blocking per reviewer's own framing, or contradicted by existing convention

**Point 3: thread-safety of `meanEdgesPerConnectedPairCache` (plain `HashMap`).**
Reviewer's own words: "likely single-threaded in practice... not a new bug... Not blocking."
Matches the pre-existing `averageDegreeCache` pattern exactly. No action.

**Point 4: pack the `(RID, RID)` pair into a primitive key instead of `Map.Entry<RID, RID>` to cut
GC pressure.**
Reviewer's own words: "Bounded at 2000 so low priority." `RID` is `bucketId:int` + `position:long`,
so a genuinely primitive combined key for two RIDs needs a real packing scheme (not a single
`long`), which trades a small, bounded, per-planning-call allocation for meaningfully more complex
code. Not worth it for a capped, non-hot-path (query execution) allocation. Revisit only if profiling
ever shows this method as a real GC contributor.

**Point 5b: `DatabaseFactory` in the new test's `setup()` is never explicitly closed.**
Verified against the codebase: every sibling test in this package
(`CypherGAVBoundTargetCardinalityTest`, `CypherBoundTargetExpansionTest`, etc.) uses the identical
`database.drop()`-only teardown with no separate `factory.close()` call. This is the established
project convention here, not a defect introduced by this PR.
