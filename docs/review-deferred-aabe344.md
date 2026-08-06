# Review disposition — PR #5836, cycle 2, commit aabe344

Bot: `claude[bot]` (issue comment, id 5200929180).

## Applied

3. **Doc imprecision: provider lookup location.** Verified: real mismatch, confirmed via
   `git diff origin/main -- .../CypherOptimizer.java` (empty). The tracking doc said
   `CypherOptimizer.estimateMeanEdgesPerConnectedPair` looks up the provider directly; the lookup
   actually lives in `StatisticsProvider.exactMeanFromTraversalProvider`, and `CypherOptimizer.java` is
   untouched by this PR. Corrected the doc.

## Skipped, with rationale

1. **`getAverageDegree` invalidation ignores vertex counts (restated with a concrete scenario).** Same
   already-documented, deliberate tradeoff as cycle 1's point 2 — the reviewer itself calls this
   "documented and accepted," flagging only for visibility. No code change; if a real bad-plan report
   ever traces back to this, folding a vertex-count component into the cache stamp is the documented
   remedy.

2. **Clamp asymmetry — reviewer confirms it now reads as intentional and documented.** No action needed;
   this is acknowledgment of cycle 1's fix, not a new ask.

4. **Non-synchronous GAV can look fresh while topologically stale.** New observation, explicitly filed
   under "same accepted heuristic-cache category... not a correctness bug; noting for completeness." A
   real but out-of-scope refinement (would need per-provider staleness tracking beyond edge count) for a
   cost-model estimate that already documents its heuristic-cache limitations. No action.
