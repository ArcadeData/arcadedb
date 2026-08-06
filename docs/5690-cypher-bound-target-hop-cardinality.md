# #5690: bound-target hop cardinality estimate assumes filtering, but it can multiply

Issue: https://github.com/ArcadeData/arcadedb/issues/5690

## Root cause

`CypherOptimizer.createExpandIntoOperator` estimates the output of a hop whose far end is already
bound (a "bound-target" hop, marked `⭐ BOUND-TARGET` in `EXPLAIN` output) as:

```java
final long outputCardinality = (long) (inputCardinality * DEFAULT_EXPAND_INTO_SELECTIVITY); // 0.1
```

`DEFAULT_EXPAND_INTO_SELECTIVITY = 0.1` encodes "this hop filters": of ten input rows, one survives.
That was correct back when the operator behaved as a semi-join (issue predates #5684/#5663). Since
#5684 the operator is an expansion - it emits one row **per relationship joining the pair** - so on a
multigraph the hop multiplies instead of filtering, and the estimate is wrong by a factor that grows
with the pair's parallel-edge multiplicity. The estimate feeds `JoinOrderRule`, so an under-estimated
multiplying hop can mislead the planner into ordering subsequent operators as if the result were still
small.

## Affected components

- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/CypherOptimizer.java`
  (`createExpandIntoOperator`)
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/statistics/StatisticsProvider.java`
  (new statistic)

## Fix

Added `StatisticsProvider.getMeanEdgesPerConnectedPair(edgeType)`: samples a bounded prefix (2000
edges) of the edge type via `Database.iterateType`, counts how many distinct (out, in) pairs the
sampled edges resolve to, and returns `sampledEdges / distinctPairs` (at least `1.0`, the simple-graph
default). Cached per edge type on the provider instance, matching the existing `getAverageDegree`
cache pattern.

`createExpandIntoOperator` now scales the estimate by the average of this statistic across the hop's
edge types:

```java
final double meanEdgesPerConnectedPair = estimateMeanEdgesPerConnectedPair(edgeTypes);
final long outputCardinality = (long) (inputCardinality * DEFAULT_EXPAND_INTO_SELECTIVITY * meanEdgesPerConnectedPair);
```

An untyped hop (no edge type restriction) cannot be attributed to one type's statistic and keeps
multiplicity `1.0` (today's behaviour, unchanged). `outputCardinality` is shared by the `GAVExpandInto`
branch of the same method, so the CSR-backed path picks up the same corrected estimate automatically.

This matches the issue's proposed formula (`connectivity * meanEdgesPerConnectedPair`) but keeps the
"connectivity" factor as the existing `DEFAULT_EXPAND_INTO_SELECTIVITY` heuristic rather than adding a
new statistic for it - the issue itself notes a sampled multiplicity estimate is "probably enough for a
cost model," and this stays scoped to that.

## Scope decisions vs. the issue's checklist

- [x] Add mean-edges-per-connected-pair per edge type to `StatisticsProvider`, sampled
- [ ] Exact CSR answer via `GraphAnalyticalView` when one covers the type - **not done**. The CSR
      infrastructure (`com.arcadedb.graph.olap`) exists and is production-quality, but wiring it into
      `StatisticsProvider`/the optimizer is new integration work, not a extension of the existing
      sampling method. Sampling alone already fixes the reported estimate defect; exact-when-available
      is a worthwhile follow-up, not a correctness requirement for this issue.
- [x] Use it in `createExpandIntoOperator` instead of the bare `DEFAULT_EXPAND_INTO_SELECTIVITY`
- [x] Check whether `createExpandAllOperator`'s `DEFAULT_AVG_DEGREE` has the mirror-image problem -
      see below. **Not fixed here** - it is a real, pre-existing, differently-shaped defect and fixing
      it is out of scope for this issue's regression test.
- [x] A test that a multigraph pattern's estimated cardinality tracks the real one

## Planning-time cost of the sample

`getMeanEdgesPerConnectedPair` is the one estimator in `StatisticsProvider` that is not O(1): on a
cache miss it loads up to `MULTIPLICITY_SAMPLE_LIMIT` (2000) edge records. This is amortized by
`CypherPlanCache` (`OpenCypherQueryEngine.java:330-343`), which caches the whole `PhysicalPlan` keyed
by query string and skips `CypherOptimizer`/statistics collection entirely on a hit - verified against
the code, not assumed. So the sampling cost is paid once per distinct query **string** that is not
already in the (bounded, LRU-like) plan cache, not once per execution. A high-cardinality or
frequently-evicted set of distinct query strings against a large edge type still re-pays it; tracked as
part of the memoization discussion in #5834, along with skipping the sample entirely on the CSR/GAV
path where an exact count could be had instead (raised again on review cycles 2-4; intentionally left
for #5834 rather than folded into this fix, since it needs the same cache-placement/invalidation design
decision).

## Truncation to zero on small inputs (not fixed here)

`(long) (inputCardinality * DEFAULT_EXPAND_INTO_SELECTIVITY * meanEdgesPerConnectedPair)` still
floors to `0` when `inputCardinality` is small (e.g. `1`) and `meanEdgesPerConnectedPair` is under
`10`. This is pre-existing - the bare `inputCardinality * 0.1` already floored to `0` before this fix
- so it is not a regression, and the multigraph fix does not need to rescue it to close #5690. A
`Math.max(1, ...)` floor (matching `calculateAverageDegree`'s existing clamp style) would prevent a
bound-target hop from ever being estimated at zero rows, which could still mislead `JoinOrderRule`.
Left out of this PR because it changes behaviour for every small-input `ExpandInto` hop, not just the
multigraph case this issue reports - a broader blast radius than this fix's scope, and worth its own
look (possibly alongside the `createExpandAllOperator` follow-up below) rather than bundled in here.

### `createExpandAllOperator` finding (not fixed in this PR)

`createExpandAllOperator` (`CypherOptimizer.java`) estimates `ExpandAll` cardinality with a flat
constant:

```java
final long outputCardinality = inputCardinality * (long) DEFAULT_AVG_DEGREE; // 10.0
```

This ignores `StatisticsProvider.getAverageDegree(relationshipType, sourceLabel, targetLabel)`, which
already exists and is already wired into `JoinOrderRule.estimateAverageDegree` for hop ordering - just
not into the `ExpandAll` operator's own cardinality. So `ExpandAll`'s estimate is disconnected from
real statistics in **both** directions (over- or under-estimating depending on the type's true average
degree), unlike `ExpandInto`'s defect which was specifically backwards on multigraphs. The class-level
`TODO` comment at `CypherOptimizer.java:94` ("replace with runtime statistics once the statistics
provider tracks per-type average degree") is stale - the statistics provider already tracks it; only
the wiring into this one call site is missing. Recommend a follow-up issue rather than folding it into
this PR, since it changes `ExpandAll` cost/cardinality broadly rather than the specific multigraph
bound-target defect reported here.

## Tests

- `StatisticsProviderTest`: three new tests for `getMeanEdgesPerConnectedPair` - simple graph (returns
  1.0), multigraph (5 parallel edges + 1 single edge -> mean 3.0), and fallback cases (unknown type,
  edge type with no edges, non-edge type).
- `CypherExpandIntoMultiplicityCardinalityTest` (new): builds a bound-target cycle
  (`(a:Account)-[:INITIATED]->(t:Txn)-[:SETTLED]->(a)`) where the closing `SETTLED` hop is joined by 5
  parallel edges for its one connected pair, and asserts (via `EXPLAIN`'s `rows=` on the
  `⭐ BOUND-TARGET` operator) that the estimate is `5`, not the pre-fix `1`. Verified this test fails
  (`expected: 5L but was: 1L`) against the pre-fix formula before restoring the fix, per TDD.

## Verification

- `CypherExpandIntoMultiplicityCardinalityTest`, `StatisticsProviderTest`: pass.
- Broader regression sweep: `Cypher*`, `*Optimizer*`, `*ExpandInto*`, `*GAV*`, `PhysicalOperatorTest`,
  `JoinOrderRuleTest`, `CostModelTest`, `ExpandIntoRuleTest` - all pass (`BUILD SUCCESS`).
- No existing test asserted the exact numeric output of `createExpandIntoOperator`'s old formula, so no
  existing assertions needed updating.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5832

## Review cycles

Reviewer: `claude[bot]` (posts as a PR issue comment, not a formal GitHub review). `--max-cycles=4`
was reached; the loop stopped per the max-cycles guard, not a clean approval.

- **Cycle 1** (head `ba95f20f9`): 5 points - planning-time sampling cost, prefix-sampling bias,
  cache thread-safety (non-blocking), GC/primitive-key nit (low priority), test nits (stray
  `@author`, factory-close claim, direction coverage). Applied: qualified the stale "O(1)" class
  Javadoc, added a prefix-bias comment, removed the copy-pasted `@author` tag, added an IN-direction
  hop test. Verified the factory-close claim against sibling tests and found it false (matches
  established convention) - skipped. Deferred the cache-sharing/CSR question to a
  `review-deferred-*.md` notes file. Pushed as `0651306fd`.
- **Cycle 2** (head `0651306fd`): reviewer accepted cycle 1's deferral, flagged that prefix-clustering
  could *overestimate* by orders of magnitude (asymmetric risk vs. the bug being fixed), a minor
  field-alignment nit, and that the `review-deferred-*.md` notes file itself shouldn't be committed.
  Applied: a `1000.0` upper clamp (matching `calculateAverageDegree`'s existing clamp style, with a
  new test proving it), the alignment fix, removed the notes file, and opened follow-up issue
  https://github.com/ArcadeData/arcadedb/issues/5834 for the deferred architectural items instead.
  Pushed as `7b66e3716`.
- **Cycle 3** (head `7b66e3716`): "none blocking" overall. Asked for one-line comments explaining
  why multi-type hops average rather than sum multiplicity, and why `ExpandInto`'s cost intentionally
  stays keyed off `inputCardinality` while cardinality scales; noted a redundant lower `Math.max`
  clamp as dead code; raised (as a question for the maintainers, not a demand) whether an issue-scoped
  design doc under `docs/` is consistent with removing the review-notes file in cycle 2. Applied both
  comments and removed the dead clamp. The doc-policy question is left for the developer - see
  "Handoff notes" below. Pushed as `2cad89465`.
- **Cycle 4** (head `2cad89465`, final cycle): praised the overall thoroughness; asked to confirm
  the `CypherPlanCache` amortization claim against the code (verified true - see "Planning-time cost
  of the sample" above) and document it; called out two missing one-line test cases (union-type
  averaging, untyped-hop fallback) and a wasted double-`save()` in test setup; reiterated the
  CSR-shortcut/memoization ask (already tracked in #5834); floated a `Math.max(1, ...)` floor for the
  pre-existing zero-truncation on small inputs. Applied: the verified doc note, both missing tests,
  the test fix. Documented (without implementing) the zero-truncation floor - see "Truncation to zero
  on small inputs" above. Pushed as `6f6b4d2f5`.

## Final state: max-cycles-reached

Four review cycles ran; each received genuinely new, substantive feedback and each was answered with
either a code change or a documented, reasoned deferral - not a rubber-stamp. Nothing outstanding
blocks correctness of the reported defect. What remains open for the developer's judgment:

1. **Follow-up issue #5834** (database-scoped memoization/invalidation for the sampling cache, and a
   CSR-exact multiplicity path when a `GraphAnalyticalView` covers the type) - raised on cycles 1, 2,
   and 4. The reviewer's cycle-4 framing ("the main thing I would want resolved before merge") is
   stronger than earlier cycles; worth a final read before merging.
2. **The zero-truncation floor** (`Math.max(1, ...)` on `outputCardinality`) - pre-existing, not a
   regression, but flagged twice (cycles 1 implicitly via the `Math.max` clamp precedent, and
   explicitly in cycle 4).
3. **Doc-policy question** (cycle 3): whether `docs/5690-*.md` belongs in the repo at all, given the
   project's convention of purging review-cycle tracking docs (a signal independently corroborated
   by the reviewer's own cycle-2 request to drop `review-deferred-*.md`). This orchestration kept the
   design doc per this skill's own workflow; the developer may want to fold its content into the PR
   description and drop the file before merging, consistent with project convention.

## Handoff notes

**Merge is the developer's responsibility - this orchestration does not merge PRs.**
