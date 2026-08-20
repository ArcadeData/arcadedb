# Issue #6431: Star-join count(*) push-down ignores inline node properties and dynamic labels

## Summary

Follow-up to #6337 (PR #6430), flagged during that PR's review by `claude[bot]`. The claim: the two
node-label decline checks in `tryDetectStarCountStar` (`CypherExecutionPlan.java`) - the central-variable
one from #6322 and the arm-endpoint one added by #6337 - gate only on `node.hasLabels()`, and neither
checks `node.hasProperties()` or `node.hasDynamicLabels()`, so an inline property filter or dynamic label
on the central variable or an arm node would be silently dropped by the `DegreeProductOp` ("CSR degree
product") push-down, the same over-count class #6337 fixed for plain labels.

## Investigation finding: not reproducible on current `main`

Before writing code, I reproduced the issue's own example query against current `main`
(`MATCH (p:Post)<-[:WROTE]-(a:Author {status:'active'}), (p)-[:TAGGED]->(:Topic) RETURN count(*)`) and a
central-variable variant. Both already **decline** the star-join push-down and return the correct answer.

The reason: `tryOptimizeCountStar` (the single entry point that dispatches to `tryDetectStarCountStar` and
its siblings) already has a blanket guard, `hasInlineNodePropertyOrDynamicLabel()`, added by the fix for
issue #5071 (commit `0276115`, 2026-07-06). It walks every node in every `MATCH` path pattern of the
statement and declines **all** count push-down detectors - the star one included - if any node carries
`hasProperties()` or `hasDynamicLabels()`. That fix landed over a month **before** #6337/PR #6430
(2026-08-19), so by the time the arm-endpoint *label* check was added, the property/dynamic-label case was
already covered at the outer level. The PR review's static-code read of `tryDetectStarCountStar` in
isolation missed the outer guard sitting in the caller.

Verified with `EXPLAIN` on both example queries: the physical plan uses the ordinary Cost-Based Query
Optimizer path (`Filter` + `ExpandAll` steps), not `COUNT STAR JOIN`, and the count matches the
materialized pipeline's row count.

## What this PR still does

Even though the exact silent-over-count scenario in the issue isn't reachable today, the issue's suggested
fix - adding `node.hasProperties() || node.hasDynamicLabels()` directly to `tryDetectStarCountStar`'s own
two decline checks - has standalone value as defense-in-depth, matching how the pair-join and chain-hop
detectors in the same file check these conditions at their own level rather than relying solely on the
outer guard. It keeps the star-join detector correct on its own if `hasInlineNodePropertyOrDynamicLabel()`'s
scope or call site ever changes. This PR makes that small, mechanical change:

- Central-variable loop: now declines whenever an occurrence of the central variable carries
  `hasProperties()`/`hasDynamicLabels()`, in addition to the existing label checks.
- Arm-node loop: now declines whenever a non-central node carries `hasProperties()`/`hasDynamicLabels()`,
  in addition to the existing `hasLabels()` check.

## Test plan

Added `CypherStarCountPropertyIssue6431Test` (4 tests), following the pattern of
`CypherStarCountArmLabelIssue6337Test`: cross-checks the declined push-down's `count(*)` answer against the
materialized pipeline's row count as ground truth, for:
- a property filter on an arm endpoint
- a property filter on the central variable
- a dynamic label (`:$($param)`) on an arm endpoint
- a dynamic label on the central variable

Also re-ran the sibling suites to confirm no regression:
- `CypherStarCountArmLabelIssue6337Test` (4 tests)
- `CypherCountPushDownLabelIssue6322Test` (8 tests)
- `GAVEligibilityTest` (43 tests)
- Full `com.arcadedb.query.opencypher.**` package

All green. `mvn -pl engine -am compile` clean.

## Review history

(updated as review cycles run)
