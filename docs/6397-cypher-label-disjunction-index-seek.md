# Issue #6397 — openCypher label disjunction never uses an index

## Problem

`IndexSelectionRule.createAnchorOperator` built a `NodeByLabelDisjunctionScan` for any label
disjunction anchor (`(n:A|B {id: $x})`) **before** looking at `anchor.useIndex()`, so an indexed
equality predicate could never drive a seek for a disjunction, no matter how selective it was.
`AnchorSelector.evaluateNode` had the matching problem on the costing side: a disjunction anchor
was always costed as the full multi-type scan.

## Fix

All-or-nothing per-root index seek, matching the issue's suggested shape:

1. `StatisticsProvider.getMatchingVertexRootTypes(labels, disjunction)` (new) returns the subset of
   `Labels.matchingVertexTypes` that has no ancestor also in that set — the minimal list of types a
   *polymorphic* index seek has to visit to cover the whole disjunction exactly once. `matches()`
   propagates down the whole subtype chain, so a root's own polymorphic index already reaches every
   listed descendant of that root.
2. `AnchorSelector.tryDisjunctionIndexSeek` (new) looks for a single equality predicate (inline
   property or `WHERE` clause) on the disjunction's own variable, then asks each root type for a
   usable index on it. All-or-nothing: one root with no usable index aborts the whole attempt back
   to the existing full-scan costing.
3. `AnchorSelection` gained an optional `List<DisjunctionIndexSeek>` (type name + `IndexStatistics`
   per root) alongside the existing single-index fields, so a disjunction anchor can carry N
   per-root seeks through the same object the rest of the optimizer already threads.
4. `IndexSelectionRule.createAnchorOperator` builds one `NodeIndexSeek` per root (reusing the
   existing, already-correct seek operator unmodified — including its composite-index prefix
   handling from #5444) and wraps them in a new `NodeByLabelDisjunctionIndexSeek` operator that
   unions the per-root result streams and de-duplicates by RID.
5. `CypherOptimizer.extractTypeNames` collected only `node.getFirstLabel()` for statistics purposes,
   so a disjunction's second and later alternatives never had their indexes collected at all — an
   alternative with a real index read back as "no index". Fixed to collect every label of a
   disjunction node.

### Why de-duplication is required, not merely defensive

A `TypeIndex` is inherently polymorphic — creating an index on a type indexes that type's buckets
*and every subtype's buckets* (`TypeIndexBuilder.create()` uses `type.getBuckets(true)`). So seeking
a root's own index already returns every matching descendant; the roots computed in step 1 are
chosen so their subtrees are disjoint under single inheritance. But ArcadeDB supports multiple
inheritance (`EXTENDS A, B`), so a type that multiply-inherits from two accepted alternatives is
reachable through *both* roots' indexes — the existing test
`CypherDisjunctionCardinalityIssue6363Test.aSubtypeIsCountedOnceAndOnlyThroughTheAlternativesThatAcceptIt`
already exercises exactly this shape for the scan operator. `NodeByLabelDisjunctionIndexSeek` carries
its own RID `Set` across all per-root seeks for the same reason.

### Scope

Only a single equality predicate drives the seek (inline property or `WHERE var.prop = value`),
matching the issue's primary example and its "suggested shape" section. The issue itself flags the
IN-list and range-predicate paths on a disjunction anchor as a related but separate limitation
("worth noting … which a disjunction also never reaches") — not something this fix closes. A mixed
seek/scan union (some roots indexed, some not) is also left alone per the issue's own note that it
"needs the operator to interleave two access paths… worth doing second if at all".

## Tests

New: `engine/src/test/java/com/arcadedb/query/opencypher/CypherDisjunctionIndexSeekIssue6397Test.java`
- inline-property equality on every indexed alternative uses `NodeByLabelDisjunctionIndexSeek`
- `WHERE` clause equality form does too
- one non-indexed alternative falls back to the full scan (all-or-nothing) and still returns correct rows
- a value no alternative has returns nothing
- a type multiply-inheriting from two indexed alternatives (diamond) is returned once, not twice

Regression run (existing + new, all green):
- `CypherDisjunctionIndexSeekIssue6397Test` (new) — 5/5
- `CypherDisjunctionCardinalityIssue6363Test` — 5/5
- `CypherLabelDisjunctionOnExpandedNodeIssue6338Test` — 11/11
- `CypherRelationshipTypeDisjunctionTest` — 13/13
- `Issue5362DuplicatePredicateIndexSeekTest` — 9/9
- `Issue5444CompositeIndexSeekTest` — 10/10
- `StatisticsProviderTest` — 18/18
- `CostModelTest` — 16/16
- `CypherOptimizerIntegrationTest` — 7/7
- `AnchorSelectorTest` — 11/11
- `IndexSelectionRuleTest` — 11/11
- `OpenCypherOptimizerVerificationTest` — 9/9
- `CypherPlannerDifferentialIssue6400Test` — 4/4
- Full `com.arcadedb.query.opencypher.**` package sweep (excluding benchmark/slow/vector lanes) — see PR for final count

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/statistics/StatisticsProvider.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/AnchorSelector.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/plan/AnchorSelection.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/rules/IndexSelectionRule.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/CypherOptimizer.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/executor/operators/NodeByLabelDisjunctionIndexSeek.java` (new)
- `engine/src/test/java/com/arcadedb/query/opencypher/CypherDisjunctionIndexSeekIssue6397Test.java` (new)
