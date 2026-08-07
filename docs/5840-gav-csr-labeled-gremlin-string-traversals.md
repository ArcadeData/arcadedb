# Issue #5840: GAV/CSR acceleration never engages for labeled Gremlin traversals submitted as strings

## Root cause

TinkerPop 3.8.1's `gremlin-lang` parser (the entry point used by the HTTP endpoint, Studio, and the
drivers) builds a `VertexStepPlaceholder` for any hop with a non-empty edge-label array, instead of a
concrete `VertexStep`. `GValueReductionStrategy` resolves those placeholders into real `VertexStep`
instances, and `ArcadeTraversalStrategy.applyGAVOptimization` only recognizes `instanceof VertexStep`.

The ordering between the two strategies was never guaranteed by the code. `ArcadeTraversalStrategy`
declared itself as `TraversalStrategy.OptimizationStrategy`, the same category as
`GValueReductionStrategy`. Every `OptimizationStrategy` inherits a default `applyPost()` that adds
`GValueReductionStrategy.class` (see `TraversalStrategy.OptimizationStrategy#applyPost` in
`gremlin-core`), meaning `GValueReductionStrategy` is guaranteed to run **last** within the
Optimization category, i.e. strictly *after* `ArcadeTraversalStrategy`. So on the string-submission
path, `ArcadeTraversalStrategy` always ran before the placeholders it needed resolved were resolved.

The fluent Java API (`graph.traversal()`) never hits this: `out(String...)` etc. construct a concrete
`VertexStep` directly, no placeholder involved. That's why the existing fluent-path tests
(`ArcadeGAVStepsTest`, `GremlinGAVTest`) didn't catch the defect.

## Fix

Reclassify `ArcadeTraversalStrategy` from `TraversalStrategy.OptimizationStrategy` to
`TraversalStrategy.ProviderOptimizationStrategy`. TinkerPop's own category-ordering documentation
describes `ProviderOptimizationStrategy` as exactly this use case ("graph system/language/driver
providers that want to rewrite a traversal using provider specific steps"), and category ordering is
enforced unconditionally by `TraversalStrategies.sortStrategies` regardless of any strategy's
`applyPrior`/`applyPost` declarations: every strategy in an earlier category is a hard prerequisite for
every strategy in a later category. `ProviderOptimizationStrategy` sorts strictly after
`OptimizationStrategy` (which contains `GValueReductionStrategy`), so by the time
`ArcadeTraversalStrategy.apply()` runs, every `VertexStepPlaceholder` (including ones freshly rebuilt by
`AdjacentToIncidentStrategy`) has already been resolved to a concrete step.

This also makes the ordering deterministic across JVMs — previously `ArcadeTraversalStrategy`'s position
relative to other same-category `OptimizationStrategy` strategies (besides its one explicit
`applyPrior()` entry) depended on `LinkedHashSet`/topo-sort iteration order.

Since category ordering now guarantees `ArcadeTraversalStrategy` runs after every built-in
`OptimizationStrategy`, including `InlineFilterStrategy`, the explicit `applyPrior()` override naming
`InlineFilterStrategy.class` became redundant and was removed.

### Scope note

A top-level `.out().count()` shape is rewritten by TinkerPop's `AdjacentToIncidentStrategy` into
`.outE().count()` (edge-returning), regardless of the ordering fix. `ArcadeGAVVertexStep` only replaces
vertex-returning `VertexStep`s, so this shape is not, and was never, GAV-accelerated by
`ArcadeTraversalStrategy` — that is a distinct, out-of-scope enhancement (an edge-returning GAV step),
not a consequence of the ordering bug this issue reports. Results stay correct via the OLTP fallback
either way.

## Files changed

- `gremlin/src/main/java/com/arcadedb/gremlin/ArcadeTraversalStrategy.java` — category change,
  `applyPrior()` override removed.
- `gremlin/src/test/java/com/arcadedb/gremlin/GremlinStringPathGAVTest.java` — new regression test
  covering the string-submission path.

## Tests

New test class `GremlinStringPathGAVTest` parses queries with the same `GremlinLangScriptEngine` the
`graph.gremlin(String)` entry point uses (`ArcadeGremlin.executeStatement`), without iterating, and
asserts the compiled plan the same way the existing fluent-path tests do (`TraversalPlans`):

- labeled single hop (`out('KNOWS')`, `in('KNOWS')`, `both('KNOWS')`) submitted as a string installs
  `ArcadeGAVVertexStep`.
- labeled two-hop chain submitted as a string fuses into `ArcadeGAVFusedStep`.
- label-less multi-hop (`out().out()`) submitted as a string fuses into `ArcadeGAVFusedStep` (the shape
  the issue calls out as previously uncovered).
- results consistency: the string path and the fluent path return the same rows for a labeled hop
  (guards against the plan assertion passing while behavior silently diverges).

All were confirmed to fail against the pre-fix code (labeled-hop cases) before the fix was applied, per
TDD.
