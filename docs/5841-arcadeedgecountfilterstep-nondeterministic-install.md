# Issue #5841 - ArcadeEdgeCountFilterStep installs non-deterministically across JVM runs

## Problem

`ArcadeTraversalStrategy.applyEdgeCountFilterOptimization()` rewrites the pattern
`where(outE('X').count().is(predicate))` into an O(1) degree check
(`ArcadeEdgeCountFilterStep`) by matching an exact 3-substep shape inside the `where`-child:
`VertexStep(edge) -> CountGlobalStep -> IsStep`.

TinkerPop's own `CountStrategy` also rewrites `count().is(predicate)` patterns: for a bounded
predicate such as `is(gt(1))` it computes a `highRangeCandidate` and inserts a
`RangeGlobalStep(0, 2)` ahead of the `CountGlobalStep`, growing the where-child to 4 substeps.
That permanently defeats the exact-3-substep match.

`ArcadeTraversalStrategy.applyPrior()` declared an ordering relationship only with
`InlineFilterStrategy` - nothing about `CountStrategy`. With no declared edge between the two
strategies, TinkerPop's `TraversalStrategies.sortStrategies()` topological sort ties are broken by
iterating a `HashSet<Class<?>>`. `Class` does not override `hashCode()`, so the iteration order
follows the JVM's identity hash codes, which are seeded per-JVM-invocation and not stable across
JVM processes.

`ArcadeGraph` computes and registers the strategy set exactly once in a `static` block, so
whichever order a given JVM process happens to produce is frozen for that JVM's entire lifetime:
deterministic within one Surefire fork, but flips depending on which JVM process (fork) runs the
query - for byte-identical code and unchanged test order.

## Fix

`gremlin/src/main/java/com/arcadedb/gremlin/ArcadeTraversalStrategy.java`

Added an `applyPost()` override declaring `CountStrategy.class` (in addition to the
`OptimizationStrategy` default of `GValueReductionStrategy.class`, which would otherwise be
silently dropped by overriding the method). This forces TinkerPop's topological sort to always
place `ArcadeTraversalStrategy` before `CountStrategy`, regardless of JVM identity-hash iteration
order, so `applyEdgeCountFilterOptimization()` always sees the traversal in its pre-`CountStrategy`
shape before deciding whether the O(1) rewrite applies.

## Tests

`gremlin/src/test/java/com/arcadedb/gremlin/ArcadeEdgeCountFilterStepTest.java`

- Removed the `@Disabled` annotation (and its long explanatory Javadoc, now folded into this
  tracking doc and the fix's Javadoc) from `theDegreeFilterStepIsInstalled`, which pins that
  `ArcadeEdgeCountFilterStep` installs for `g.V().where(outE("KNOWS").count().is(P.gt(1)))`. This
  is the exact bounded-predicate shape that triggered TinkerPop's `CountStrategy` interference.
  Removed the now-unused `Disabled` import.
- Left `whenTheRewriteDoesNotInstallTinkerPopsCountStrategyExplainsWhy` untouched: it is written to
  hold under either outcome (if the rewrite doesn't install, it must be because of
  `CountStrategy`'s `RangeGlobalStep`), so it stays meaningful as a belt-and-suspenders check even
  though the `if (!installed)` branch is now unreachable.

### Verification that the test can actually fail (pre-fix)

Per project practice, proved the un-disabled test both fails against the original code and passes
against the fix, rather than trusting a newly-enabled assertion at face value:

1. Stashed just the production change (`ArcadeTraversalStrategy.java`), keeping the un-disabled
   test. Rebuilt (`./mvnw -pl gremlin install -DskipTests -q`) and ran
   `./mvnw -pl gremlin-it test -Dtest=ArcadeEdgeCountFilterStepTest` across 6 separate JVM forks:
   all 6 failed `theDegreeFilterStepIsInstalled` with
   `plan was: GraphStep -> TraversalFilterStep` / `Expecting value to be true but was false` -
   confirming `CountStrategy` wins the tie-break deterministically in this environment (illustrating
   the "frozen per-JVM, but the JVM you get is arbitrary" nature of the bug - a different developer
   machine or CI runner could just as easily freeze the other way).
2. Restored the fix (`git stash pop`), rebuilt, and re-ran the same test across 3 separate JVM
   forks plus the original single run: all 9 tests in the class passed every time (`Failures: 0,
   Errors: 0`), including `theDegreeFilterStepIsInstalled`.

### Regression coverage

- `ArcadeEdgeCountFilterStepTest` (9 tests) - all pass, including the un-disabled test, across
  multiple JVM forks.
- `ArcadeGAVStepsTest` (12 tests), `ArcadeFilterByTypeStepTest` (13 tests), `GremlinGAVTest` (9
  tests) - all pass, covering other `ArcadeTraversalStrategy` rewrite paths that share the same
  `apply()`/`applyPrior()`/`applyPost()` machinery.
- Full `gremlin-it` unit test module (152 tests total across all classes, run via
  `./mvnw -pl gremlin-it test`) - 0 failures, 0 errors.

Build note: per the module's own test-execution quirk, `gremlin`'s tests are compiled into a
test-jar but only actually executed from `gremlin-it` against the shaded/relocated jar
(`gremlin/pom.xml` sets `skipTests=true` for both surefire and failsafe). Every verification run
above was preceded by `./mvnw -pl gremlin install -DskipTests -q` so `gremlin-it` picked up the
current working-tree code rather than a stale `~/.m2` artifact.

## Impact analysis

Scope is limited to `ArcadeTraversalStrategy`'s ordering relative to `CountStrategy`; no change to
`apply()`'s rewrite logic itself, and no other `OptimizationStrategy` ordering is touched. The fix
makes an existing optimization's applicability deterministic - it does not change result semantics
in either the optimized or unoptimized path (both paths call the same predicate/count logic;
`ArcadeEdgeCountFilterStepTest`'s six `DifferentialTraversal`-based tests confirm the optimized and
unoptimized paths agree).

## Recommendations

- No further action needed for this issue. The companion issues #5838, #5840, #5842 (also found in
  PR #5829, tracked separately) are unrelated defects in different code paths and are out of scope
  here.
- If a similar "silent HashSet-tie-break" class of bug is suspected elsewhere in
  `ArcadeTraversalStrategy` or other custom `TraversalStrategy` implementations, the fix pattern
  (declare the missing `applyPrior()`/`applyPost()` edge against the specific TinkerPop strategy
  class involved) generalizes directly.
