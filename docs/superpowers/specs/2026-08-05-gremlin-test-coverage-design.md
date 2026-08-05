# Gremlin Module Test Coverage: Design

**Date:** 2026-08-05
**Scope:** `gremlin/src/main/java/com/arcadedb/gremlin/`
**Goal:** find real defects in the ArcadeDB-specific Gremlin code. Coverage is a reported side effect, not the target.

## Problem

The `com.arcadedb.gremlin` package sits at **70.2% line coverage (1225/1744) and 57.8% branch coverage (499/864)**, measured across the full existing suite: 1866 integration tests plus roughly 60 unit tests, all passing.

That aggregate hides a sharp split. The full TinkerPop `structure` and `process` conformance suites run against `ArcadeGraph` with zero `@OptOut` annotations, so the TinkerPop contract surface of `ArcadeVertex`, `ArcadeEdge`, `ArcadeProperty`, and `ArcadeGraph` is exercised heavily. Everything ArcadeDB-specific, which no TinkerPop suite knows about, is thin or absent:

| Class | Lines | Branches |
| --- | --- | --- |
| `ArcadeGAVFusedStep` | 0/56 | 0/31 |
| `ArcadeGAVVertexStep` | 0/30 | 0/12 |
| `ArcadeEdgeCountFilterStep` | 0/28 | 0/13 |
| `ArcadeIoRegistry` | 10/24 | 0/11 |
| `ArcadeFilterByIndexStep` | 29/64 | 12/42 |
| `GremlinQueryEngine` | 19/34 | 4/10 |
| `ArcadeTraversalStrategy` | 109/154 | 83/126 |
| `ArcadeCountGlobalStep` | 25/38 | 15/26 |
| `ArcadeFilterByTypeStep` | 49/72 | 25/42 |

### The finding that shapes this design

`GremlinGAVTest` contains 8 tests named `outWithGAV`, `inWithGAV`, `bothWithGAV`, `multiHopWithGAV`, and so on. Each builds a real `GraphAnalyticalView` over the fixture graph and asserts the traversal's results. All 8 pass.

None of them execute the GAV code path.

JaCoCo line data for `ArcadeTraversalStrategy.java` proves it. Line 205, `if (provider != null)`, is covered. Lines 206 through 209, the branch body that constructs and installs `ArcadeGAVVertexStep`, are MISSED. `GraphTraversalProviderRegistry.findProvider(...)` returns null on every call, so the traversal silently runs the OLTP fallback. The tests pass because they assert only on results, and the fallback returns the same results.

Lines 286 through 301 are dead the same way: the entire body of the `where(outE().count().is(...))` rewrite never matched a test traversal.

The defect class this represents is the design driver. A result-only assertion cannot distinguish "the optimization ran and was correct" from "the optimization never ran". Any new test for an optimization must first prove the optimization is present.

### Prior art for the risk

Issue #5746 is the Cypher-side analogue. With a Graph Analytical View active, a multi-hop pattern silently returned zero rows, caused by two independent defects in `GAVFusedChainOperator` and `CypherOptimizer.fuseGAVExpandChain` (PR #5827). Either defect alone still broke the query.

A view is a performance feature. A wrong-and-silent answer that appears only once a view exists is worse than a slow query, because nothing downstream can tell an empty result from a real one. `ArcadeGAVFusedStep` is the Gremlin-side fused chain implementation, it has never been executed by a test, and it carries 31 uncovered branches.

## Approach

Two test-scope utilities form the backbone. No production code changes.

### `TraversalPlans`

Applies the strategy set to a traversal and exposes the resulting step list, so a test can assert which steps the optimizer actually installed.

```java
assertThat(TraversalPlans.stepsOf(g.V().has("name", "Alice").out("KNOWS")))
    .containsStepOfType(ArcadeGAVVertexStep.class);
```

This is the antidote to the `GremlinGAVTest` failure mode. Every test claiming to exercise an optimization asserts plan shape before asserting behavior.

### `DifferentialTraversal`

Runs one traversal twice against the same database: once through the normal `ArcadeGraph` strategy set, once with `ArcadeTraversalStrategy` removed from a cloned `TraversalStrategies`. Asserts the two result sets agree.

```java
DifferentialTraversal.on(graph)
    .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gt(30)));
```

`ArcadeGraph` registers its strategies into `TraversalStrategies.GlobalCache` for `ArcadeGraph.class` (`ArcadeGraph.java:101-105`), so cloning that set and removing one strategy is straightforward.

The optimized and unoptimized paths are two implementations of a single specification, so any disagreement is a defect by construction. No hand-authored oracle is needed, and no expected value has to anticipate the bug. This is where the real finds are expected, particularly in `ArcadeFilterByIndexStep`: its 5 comparison-predicate rewrites (`eq`, `gt`, `gte`, `lt`, `lte`) map onto index cursor calls whose inclusive/exclusive flag is easy to get wrong, and an off-by-one there is invisible to a result-only test whose fixture happens to lack boundary values.

### Direct unit tests for pure functions

For genuinely pure logic, direct unit tests are simpler and better: `ArcadeIoRegistry.newRID` and `isRID`, `RIDSerializer` round-trip, `ArcadeGremlin.getStringObjectMap`. These bypass the strategy, which is fine because there is no strategy involved.

### Rejected: property-based differential fuzzing

Random graphs plus random traversals compared across the optimizer boundary would have the highest raw defect yield, but it is flake-prone, slow inside a suite already running 1866 integration tests, and a useful shrinker is its own project. Cut per YAGNI.

## Phases

Ordered by defect payoff per unit of risk. Each phase is independently reviewable.

### Phase 1: diagnose the GAV provider lookup

Before any GAV test is written, determine why `findProvider` returns null.

`GraphAnalyticalView.registerAsTraversalProvider()` calls `GraphTraversalProviderRegistry.register(database, this)`, so registration exists. `findProvider` returns null when `hasAnyProviders` is false, when the `WeakHashMap` lookup on `unwrap(database)` misses, when no provider `isReady()`, or when `coversEdgeType` rejects the labels.

Leading hypothesis: a database-identity mismatch. The view registers against the database instance it holds; `ArcadeTraversalStrategy` looks up `graph.getDatabase()`. If `unwrap()` does not reconcile a wrapper with its inner `LocalDatabase`, the lookup misses. This repository has documented history of exactly that wrapped-versus-inner database trap.

The answer determines whether Phase 2 can test 114 currently dead lines, or whether the finding is "the GAV optimization never engages through Gremlin in any configuration", which is a significant defect in its own right and one that no test should be written to enshrine.

### Phase 2: optimizer and custom steps

`ArcadeTraversalStrategy` plus the six steps it installs. Plan-shape assertion and differential execution for each rewrite:

- type filter (`ArcadeFilterByTypeStep`), including the multi-label bail-out and the wrong-kind guard for `g.V()` against an edge type and `g.E()` against a vertex type
- index filter (`ArcadeFilterByIndexStep`) across all 5 comparison predicates, with fixtures whose values sit exactly on the range boundaries
- count-global (`ArcadeCountGlobalStep`)
- GAV single-hop (`ArcadeGAVVertexStep`), gated on the Phase 1 outcome
- GAV fused multi-hop (`ArcadeGAVFusedStep`), including the DFS path, the deleted-since-CSR-build skip, and the OLTP fallback when the start vertex has no CSR node id
- edge-count filter (`ArcadeEdgeCountFilterStep`) via `where(outE('X').count().is(...))`

### Phase 3: `ArcadeGremlin` engine and analysis

- engine selection across `auto`, `java`, and `groovy`, including strict-`java` refusal to fall back to Groovy
- the parse-versus-runtime error classification (`isParsingFailure`), which decides HTTP 400 against HTTP 500
- `parse()` and its `OperationType` mapping, consumed by HA follower routing, so a misclassification routes writes wrongly

One concrete defect is already identified for this phase: `ArcadeGremlin.timeout` is declared `private static Long` but assigned by the instance method `setTimeout(long, TimeUnit)`. One graph's timeout silently becomes every graph's timeout process-wide, and `getTimeout()` reads the same static.

### Phase 4: io and integration formats

`ArcadeIoRegistry.newRID` (0/11 branches, every coercion arm untested, including the `IllegalArgumentException` default), `isRID`, `RIDSerializer` round-trip, and GraphSON/GraphML import-export fidelity. Mostly direct unit tests.

### Phase 5: graph lifecycle and factory pool

`ArcadeGraphFactory` pool exhaustion at `maxInstances`, release and dispose semantics, and the `vertices()`/`edges()` id-coercion arms (`RID`, `Vertex`/`Edge` instance, `String`, null, unrecognized).

## Placement and execution

Tests live in `gremlin/src/test/java/`, harness utilities under `.../com/arcadedb/gremlin/support/`.

They do not run from the `gremlin` module. Both surefire and failsafe are configured `<skipTests>true</skipTests>` there, because gremlin's tests need TinkerPop's ANTLR 4.9.1 while the engine needs 4.13.2, and the two cannot coexist on an unshaded test classpath. The tests compile into the test-jar and execute from `arcadedb-gremlin-it` through `dependenciesToScan`, against the shaded jar.

Consequences the implementation must respect:

- The verification command is `./mvnw -pl gremlin-it -DskipITs=false verify`. Running `-pl gremlin` reports success while executing nothing.
- Any new test dependency must be added to **both** poms. `gremlin-it` re-declares every dependency by hand because it consumes the shaded artifact with `<exclusions>*</exclusions>`. A dependency added only to `gremlin` compiles and then fails at runtime.
- `*IT` runs under failsafe, `*Test` under surefire. The plan-shape and differential tests are fast and belong in `*Test`.
- Anything with a multi-second runtime gets `@Tag("slow")` per `CLAUDE.md`.

## Handling discovered defects

The suite stays green.

When a test exposes a defect, it is written so it **would** pass against correct behavior, then annotated `@Disabled` with a comment stating the observed behavior concretely: actual against expected, not "this is broken".

Before anything is disabled, the test is verified to fail for the stated reason and not an unrelated one, so a `@Disabled` marker never quietly encodes a fixture mistake.

Findings collect into a report kept in the **session scratchpad**, not the repository. ArcadeDB does not accept `docs/NNNN-*.md` tracking documents; 65 of them were purged from main on 2026-08-03. The analysis belongs in the pull request body. Each finding records the disabled test that pins it, the suspected cause, and a severity read. Which defects get fixed and filed is the maintainer's call.

One exception: if Phase 1 shows the GAV path cannot engage at all, there is no test to disable. That becomes a report finding with a reproduction, because a test asserting "the optimization does not fire" would cement the bug.

## Verification

- **Baseline:** 70.2% lines, 57.8% branches. Reproduced by merging `gremlin-it/target/jacoco.exec` and `gremlin-it/target/jacoco-it.exec` and reporting against `gremlin/target/classes`:

  ```
  ./mvnw -pl gremlin-it -Pcoverage verify -DskipITs=false
  java -jar ~/.m2/repository/org/jacoco/org.jacoco.cli/0.8.15/org.jacoco.cli-0.8.15-nodeps.jar report \
    gremlin-it/target/jacoco.exec gremlin-it/target/jacoco-it.exec \
    --classfiles gremlin/target/classes --sourcefiles gremlin/src/main/java --csv coverage.csv
  ```

  Neither module's own JaCoCo report shows these numbers, so the merge step is required for any comparison to be meaningful.

- Each phase re-measures and reports the delta. A phase that adds 40 tests and finds 3 defects has succeeded even if the percentage barely moves.
- The full `gremlin-it` suite must stay green at every phase boundary.
- **The differential harness gets a self-test.** It must be shown to detect a deliberately broken rewrite. An assertion utility that cannot fail is worth less than no utility at all, and that is precisely the trap the existing GAV tests fell into.
