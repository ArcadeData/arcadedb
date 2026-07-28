# 5481 - `shortestPath` ignores relationship inline `WHERE` (and the property map on the expression form)

Issue: https://github.com/ArcadeData/arcadedb/issues/5481

## Problem

`shortestPath()` / `allShortestPaths()` have two independent evaluators in the OpenCypher engine:

| Evaluator | Used by | Property map `{tag: 'ok'}` | Inline `WHERE` |
|---|---|---|---|
| `executor/steps/ShortestPathStep` | `MATCH p = shortestPath(...)` | enforced (#5096) | ignored |
| `ast/ShortestPathExpression` | `RETURN shortestPath(...)` | ignored | ignored |

Three of the four combinations silently dropped the constraint, so a `shortestPath` that reads as
constrained returned a path through relationships the query excluded.

## Root cause

Both evaluators read only `RelationshipPattern.getTypes()` and `getDirection()` from the pattern:

- `ShortestPathStep.edgePropertyFilters()` looked at `getProperties()` only. When it returned `null`
  the step fell through to `SQLFunctionShortestPath`, a vertex-only BFS that cannot see edge
  properties at all. An inline `WHERE` never switched the step onto the edge-aware BFS, and even on
  the edge-aware BFS the predicate was never evaluated.
- `ShortestPathExpression.evaluate()` always called `SQLFunctionShortestPath` directly. It never
  consulted `getProperties()` nor `getWhereExpression()`, so both constraints were dropped.

Both AST builders (`CypherASTBuilder.visitRelationshipPattern` for the `MATCH` form,
`CypherExpressionBuilder.parseRelationshipPattern` for the expression form, the latter since #5460)
already populate `whereExpression` on the AST node, so the parse side needed no change: the
predicate reached the evaluators and was silently ignored.

## Fix

1. Extracted the per-edge constraint into `ShortestPathStep.EdgeConstraint` - a small holder for the
   inline property map plus the inline `WHERE` predicate, with a `matches(Edge)` method. It is built
   once per source/target pair (`EdgeConstraint.from(rel, inputRow)`) and returns `null` when the
   pattern carries neither constraint, so the unconstrained hot path stays exactly as it was
   (`SQLFunctionShortestPath`, CSR-accelerated).
2. `ShortestPathStep` now routes onto the existing edge-aware BFS whenever an `EdgeConstraint` is
   present (previously: only when a property map was present), and the BFS checks the full
   constraint rather than just the property map. This covers both the optimizer and the legacy
   planner paths, since both construct the same `ShortestPathStep`.
3. `ShortestPathExpression` now builds the same `EdgeConstraint` and delegates to the shared
   edge-aware BFS helpers when one is present, keeping `SQLFunctionShortestPath` for the
   unconstrained case.

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/CypherShortestPathInlineWhereTest.java`
covers the full 2x2 matrix (both evaluators x property map / inline `WHERE`), plus
`allShortestPaths`, the combination of both constraints, `$parameter` predicates, and unconstrained
control cases.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/5487

## Review cycles

### Cycle 1 - head `2fcad506`

`claude[bot]` reviewed (no blocking findings). `gemini-code-assist` did not respond inside the
15-minute window.

| Finding | Outcome |
|---|---|
| 1. A relationship property map supplied as a bare parameter (`-[:LINK* $props]->`) is still dropped | Partly applied. Verified: on the `MATCH` form there is no gap - `CypherASTBuilder.validateNoParameterProperties` rejects a parameter map in a `MATCH` pattern at parse time (`InvalidParameterUse`), so it can never be silently dropped. The expression form is not covered by that validation, and there `$props` really was dropped; `EdgeConstraint.from` now resolves it from the query parameters. Both behaviors are pinned by tests. |
| 2. `allShortestPaths()` in expression position returns one path while the `MATCH` form returns all co-shortest paths | No change - intentional. The expression form has always returned a single path; this PR only adds constraint enforcement and deliberately leaves cardinality untouched. Changing it belongs in its own issue. |
| 3. `matchesProperties` duplicates the numeric-coercion rules of `GraphTraverser.matchesPropertyFilter` | Applied. Extracted `GraphTraverser.matchesPropertyFilter(Edge, Map)` as a static helper; the existing protected instance method delegates to it, and `EdgeConstraint` calls it. One definition now serves the variable-length MATCH traversers and both `shortestPath` evaluators. |

### Cycle 2 - head `315b3533`

`claude[bot]` re-reviewed: "Looks good to merge", no blocking findings. `gemini-code-assist` again did
not respond inside the 15-minute window, so the loop ends on a reviewer timeout rather than a clean
two-bot approval.

| Finding | Outcome |
|---|---|
| 1. `*min..max` hop bounds are ignored by every `shortestPath` traversal, so the `*1..3` in the new tests is decorative | Documented. Added the invariant to the `ShortestPathStep` class Javadoc. Pre-existing (`SQLFunctionShortestPath` and the #5096 filtered path behave the same), out of scope here - worth a follow-up issue for actual bound enforcement. |
| 2. `longValue()` coercion means `{w: 1.5}` compares as `1`, so the property map and the inline `WHERE` can disagree on fractional numerics | No change. Carried over verbatim by the extraction, not introduced here. Changing the coercion would alter variable-length MATCH semantics too. |
| 3. `EdgeConstraint` reuses a mutable evaluation row | No change needed - confirmed safe, the contract is documented on the class. |
| 4. Expression-form `allShortestPaths` cardinality | No change - agreed it belongs in a separate issue. |

### Cycle 3 - head `3ea5867e`

`claude[bot]` re-reviewed, no blocking findings. `gemini-code-assist` did not respond in this window
either (three consecutive windows), so the loop terminates on a reviewer timeout.

| Finding | Outcome |
|---|---|
| 1. The `*1..3` in the tests is decorative since bounds are unenforced; add a note so a future reader is not misled | Applied. Note added to the first `*1..3` test. |
| 2. Property map (`longValue()` coercion) and inline `WHERE` (exact numerics) can disagree on fractional values now that they sit side by side | No change - pre-existing, and changing it would move variable-length MATCH semantics. Listed as a follow-up candidate below. |
| 3. `computeFilteredShortestPath` / `computeFilteredAllShortestPaths` are now `public static`, widening the step class surface | No change. Both callers are shortestPath evaluators and they live in different packages; the alternative is duplicating the BFS, which is the defect this PR removes. |
| 4. `EdgeConstraint` mutable-row reuse | No change needed - confirmed safe and documented. |

CI on the final head: `build-and-package`, all five e2e suites, CodeQL and `claude-review` pass.
`Meterian client scan` fails on the pre-existing dependency advisories already present on `main`
(this PR adds no dependencies).

## Follow-up candidates (not filed)

- `shortestPath()` / `allShortestPaths()` ignore the `*min..max` hop bounds on every path.
- `allShortestPaths()` in expression position returns a single path instead of every co-shortest path.
- Inline property map and inline `WHERE` disagree on fractional numerics: the map coerces through
  `longValue()` (so `{w: 1.5}` matches a stored `1`), the predicate compares exactly.

## Final state

`timeout` - `gemini-code-assist` did not review in any of the three windows. `claude[bot]` reviewed on
every head and its final verdict is "looks good to merge". Merge remains the developer's decision.

## Test results

`mvn test -Dtest='*ShortestPath*,*Cypher*,*OpenCypher*,*Traverse*,*Traversal*'` in `engine`:
6646 tests, 0 failures. The 3 errors are `OpenCypherCustomFunctionTest` GraalVM
`polyglot.Engine$ImplHolder` `NoClassDefFound`, verified pre-existing on the base commit with the
change stashed.

## Scope note

Sibling work is fixing relationship inline `WHERE` in other contexts (node inline `WHERE`, #5480).
This change is confined to the two `shortestPath` evaluators and does not touch the shared
`MatchRelationshipStep` / pattern-comprehension paths.
