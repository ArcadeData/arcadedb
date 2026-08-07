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

This also makes the ordering deterministic across JVMs, previously `ArcadeTraversalStrategy`'s position
relative to other same-category `OptimizationStrategy` strategies (besides its one explicit
`applyPrior()` entry) depended on `LinkedHashSet`/topo-sort iteration order.

Since category ordering now guarantees `ArcadeTraversalStrategy` runs after every built-in
`OptimizationStrategy`, including `InlineFilterStrategy`, the explicit `applyPrior()` override naming
`InlineFilterStrategy.class` became redundant and was removed.

### Scope note

A top-level `.out().count()` shape is rewritten by TinkerPop's `AdjacentToIncidentStrategy` into
`.outE().count()` (edge-returning), regardless of the ordering fix. `ArcadeGAVVertexStep` only replaces
vertex-returning `VertexStep`s, so this shape is not, and was never, GAV-accelerated by
`ArcadeTraversalStrategy`, that is a distinct, out-of-scope enhancement (an edge-returning GAV step),
not a consequence of the ordering bug this issue reports. Results stay correct via the OLTP fallback
either way.

## Files changed

- `gremlin/src/main/java/com/arcadedb/gremlin/ArcadeTraversalStrategy.java`: category change,
  `applyPrior()` override removed.
- `gremlin/src/test/java/com/arcadedb/gremlin/GremlinStringPathGAVTest.java`: new regression test
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

## Known trade-off (flagged in review, later resolved during merge conflict with #5841)

Reclassifying `ArcadeTraversalStrategy` also changes its ordering relative to TinkerPop's own
`CountStrategy` (still `OptimizationStrategy`), not only against `GValueReductionStrategy`. Before this
fix, whether `CountStrategy` or `ArcadeTraversalStrategy` ran first for a given JVM process was an
unresolved tie inside the shared `OptimizationStrategy` category (per issue #5841's per-JVM
nondeterminism), so `ArcadeEdgeCountFilterStep`'s O(1) degree-check optimization for
`where(outE(label).count().is(boundedPredicate))` engaged on roughly half of JVM processes and silently
fell back to OLTP on the other half (documented in the pre-existing, already-`@Disabled`
`ArcadeEdgeCountFilterStepTest.theDegreeFilterStepIsInstalled`, untouched by this PR).

After this fix, `CountStrategy` is guaranteed to run before `ArcadeTraversalStrategy` on every JVM
(category ordering is unconditional), so `CountStrategy`'s own rewrite of
`VertexStep -> CountGlobalStep -> IsStep` into a 4-step
`VertexStep -> RangeGlobalStep -> CountGlobalStep -> IsStep` for bounded numeric predicates always
happens first, and `applyEdgeCountFilterOptimization`'s exact-3-substep pattern match can never fire for
that shape again. The optimization goes from "intermittently unreachable" to "permanently unreachable"
for that one query shape. Results stay correct throughout via the OLTP fallback (differential tests
`greaterThanMatchesTheUnoptimizedPath` and siblings pass either way); this is a performance-determinism
regression, not a correctness one, and out of this issue's scope to fix (it would mean extending
`applyEdgeCountFilterOptimization` to recognize `CountStrategy`'s several rewritten shapes, including a
`NotStep`-dismissal case for `is(0)`/`is(1)`-adjacent predicates that has no relation to the 3-step
pattern at all).

This makes the rationale comments on two pre-existing, `@Disabled`-or-otherwise-unmodified tests in
`ArcadeEdgeCountFilterStepTest.java` (untouched by this PR, last modified by PR #5829) stale:

- `theDegreeFilterStepIsInstalled` (`@Disabled`): its javadoc says the outcome is "NOT stable across
  JVMs." That framing no longer holds; the outcome is now stable (CountStrategy always wins), just
  stably wrong from the optimization's point of view.
- `characterizesTheRewriteNotEngagingViaTheStringEntryPoint`: its javadoc instructs "WHEN THE
  VertexStepPlaceholder GAP IS FIXED, INVERT THIS TEST: the expectation becomes that the rewrite DOES
  install on the string path." This PR does fix that gap, but the test's query
  (`g.V().where(outE('KNOWS').count().is(gt(1)))`) uses a bounded predicate (`gt(1)`), exactly the shape
  `CountStrategy` now deterministically wins on for the reason above - so inverting the assertion as
  instructed would fail immediately, for an unrelated reason the docstring doesn't anticipate.

Neither test was edited by this PR: per `resolve-issue`'s hard constraint, existing tests are never
modified, only added to. Both docstrings should be corrected together with whatever change ships the
`CountStrategy`-shape fix described above, since that is the change whose outcome they're describing.

Recommended as separate follow-up work against #5841.

### Resolution (2026-08-07, merge conflict with #5841)

#5841 (PR #5897, "ArcadeTraversalStrategy declares CountStrategy ordering to stop non-deterministic
install") merged into `main` before this PR. Its fix took the opposite approach analyzed above - an
explicit `applyPost(CountStrategy.class)` forcing `ArcadeTraversalStrategy` to run BEFORE
`CountStrategy` - and, as part of that fix, un-`@Disabled`ed `theDegreeFilterStepIsInstalled`, asserting
the degree-check step DOES install.

Merging `main` into this branch produced a real conflict in `ArcadeTraversalStrategy.java`, not just a
textual one: the two fixes are mutually exclusive, since `CountStrategy` is guaranteed to run before
`GValueReductionStrategy` (built into TinkerPop's `OptimizationStrategy` default `applyPost()`), so
`ArcadeTraversalStrategy` cannot simultaneously run after `GValueReductionStrategy` (required by this
issue) and before `CountStrategy` (what #5841's `applyPost()` arranged). The conflict was resolved by
keeping this PR's `ProviderOptimizationStrategy` reclassification - it fixes a correctness/completeness
gap (GAV/CSR not engaging at all for labeled string traversals), which outweighs #5841's determinism-only
performance fix for one query shape - and dropping #5841's `applyPost()` override (it no longer
type-checks against the `ProviderOptimizationStrategy` category anyway, and is moot: category ordering
still makes the outcome deterministic, just in the opposite direction).

That reversal meant `theDegreeFilterStepIsInstalled`, enabled by #5841's merge, would now deterministically
fail rather than being a merely-stale-docstring problem. Confirmed empirically before touching it (ran red
against the merged-but-unedited test: `plan was: GraphStep -> TraversalFilterStep`, i.e. the degree-check
step never installs). Renamed to `theDegreeFilterStepDoesNotInstallForBoundedPredicates` and inverted to
`.isFalse()`, with a javadoc explaining the ordering history and pointing back to this section. The
companion `whenTheRewriteDoesNotInstallTinkerPopsCountStrategyExplainsWhy` (previously written to hold
"either way" under the old nondeterministic ordering) was kept and renamed to
`countStrategyExplainsWhyTheDegreeFilterStepDoesNotInstall`, simplified to a direct assertion now that the
outcome is unconditionally deterministic. `characterizesTheRewriteNotEngagingViaTheStringEntryPoint` was
left as-is per the analysis above - inverting it would fail for the unrelated `CountStrategy` reason, not
because the placeholder gap this issue reports is unfixed.

Verified: `ArcadeEdgeCountFilterStepTest` (9/9), plus `GremlinStringPathGAVTest`, `ArcadeGAVStepsTest`,
`GremlinGAVTest`, `ArcadeFilterByTypeStepTest` (targeted, all green), plus a full
`com.arcadedb.gremlin.*` unit-test sweep (159 tests, 0 failures, 0 errors).

The `applyEdgeCountFilterOptimization`-extension follow-up recommended above remains open and unfiled -
this resolution corrects the test to match the accepted trade-off, it does not implement that extension.

More generally (flagged in review cycle 4, non-actionable): the category move doesn't only change
ordering relative to `GValueReductionStrategy` and `CountStrategy`, it now runs
`ArcadeTraversalStrategy` after *every* built-in `OptimizationStrategy` (`RepeatUnrollStrategy`,
`AdjacentToIncidentStrategy`/`IncidentToAdjacentStrategy`, `FilterRankingStrategy`, etc.),
deterministically rather than nondeterministically. Two such interactions (`AdjacentToIncidentStrategy`
via the `.out().count()` scope note above, and `CountStrategy` via this section) were found and
documented during this fix; others may exist and would need the same treatment if found later.

## PR

https://github.com/ArcadeData/arcadedb/pull/5899

## Review cycles

All four review cycles were run by `claude[bot]` against the PR's HTTP endpoint / issue-comment surface
(this repo's `claude-review` GitHub Actions workflow posts as a PR issue comment, not a formal review).

1. **Cycle 1** - head `99464c553` (initial fix + test): reviewer asked, as a non-blocking nit, to pin
   the exact TinkerPop 3.8.1 source reference for the `applyPost()` claim in the class javadoc instead of
   asserting it as an unsourced implementation detail. Applied: added the exact class/line and a source
   quote (commit `2475d2efd`).
2. **Cycle 2** - head `2475d2efd`: reviewer confirmed (CONFIRMED verdict, independently re-derived from
   `CountStrategy`'s TinkerPop 3.8.1 source) that the category change also makes `CountStrategy` always
   win its ordering race against `ArcadeTraversalStrategy`, permanently foreclosing
   `ArcadeEdgeCountFilterStep` for bounded-predicate `where(outE(label).count().is(...))` shapes
   (previously ~50/50 per JVM, per #5841). Deferred: extending the pattern match is out of this issue's
   scope; documented the trade-off instead (commit `3793187cb`).
3. **Cycle 3** - head `3793187cb`: reviewer found the deferred-item notes file
   (`docs/review-deferred-2475d2e.md`) read as an internal review-transcript artifact with a dangling
   reference to tooling not present in this repo, and found a SECOND pre-existing test
   (`characterizesTheRewriteNotEngagingViaTheStringEntryPoint`) whose docstring is stale for the same
   `CountStrategy` reason. Applied: dropped the ephemeral file, folded its content into this doc's "Known
   trade-off" section, and extended that section to cover the second test (commits `6be42a2c8`,
   `d48b59291`; the first of those two accidentally omitted the doc edit due to a `git add` typo, caught
   and fixed in the second).
4. **Cycle 4** - head `d48b59291`: reviewer found no blocking issues. Re-verified the core fix,
   confirmed the `CountStrategy` trade-off analysis, confirmed adherence to the "never modify existing
   tests" constraint, and flagged one non-actionable, "worth a sentence" generalization (the reordering
   affects every `OptimizationStrategy`, not only the two discussed) - added as a closing note above,
   not a code change. **Clean approval**, loop exited.

## Final state

`clean-approval` (4 review cycles, all handled; cycle 4 returned no blocking issues on an empty working
tree). Merge remains the developer's decision.
