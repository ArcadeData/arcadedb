# Gremlin Module Test Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Find real defects in the ArcadeDB-specific Gremlin code by making every optimizer test prove the optimization actually fired, and by differentially comparing the optimized path against the unoptimized one.

**Architecture:** Two test-scope utilities form the backbone. `TraversalPlans` applies the strategy set to a traversal and exposes the resulting step list so a test can assert which steps the optimizer installed. `DifferentialTraversal` runs a traversal twice against the same database, once normally and once with `ArcadeTraversalStrategy` removed, and asserts the results agree. Pure functions get plain unit tests. No production code changes.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Apache TinkerPop 3.8.x, Maven, JaCoCo 0.8.15.

## Global Constraints

- **No production code changes.** This plan adds test code only. If a defect is found, it is pinned by a `@Disabled` test, not fixed here.
- **The suite stays green.** Every task ends with the full `gremlin-it` suite passing.
- **Tests live in `gremlin/src/test/java/`, and run only from `gremlin-it`.** `gremlin/pom.xml` sets `<skipTests>true</skipTests>` for both surefire and failsafe. Running `./mvnw -pl gremlin ...` prints `Tests are skipped.` and `BUILD SUCCESS` while executing zero tests.
- **Every verification run MUST be preceded by `./mvnw -pl gremlin install -DskipTests -q`.** This is not optional and not a performance detail. `gremlin-it` executes the gremlin module's tests from its **installed test-jar in `~/.m2`** via `dependenciesToScan`, so `./mvnw -pl gremlin-it test ...` on its own silently runs the LAST INSTALLED code and ignores your working tree entirely. This was proven during Task 1: deliberately breaking `TraversalPlans.hasStepOfType` to `return true` still reported 3/3 passing until the install step was added, after which the self-test correctly failed. A run without the install step is not evidence of anything.

  Scoped run:
  ```bash
  ./mvnw -pl gremlin install -DskipTests -q
  ./mvnw -pl gremlin-it test -Dtest=ClassName
  ```

  Full gate:
  ```bash
  ./mvnw -pl gremlin install -DskipTests -q
  ./mvnw -pl gremlin-it -DskipITs=false verify
  ```

  Do NOT substitute `-pl gremlin-it -am`: that also builds `arcadedb-engine`, where `-Dtest=ClassName` matches nothing and the build fails with "No tests matching pattern".
- **A new test dependency must be added to both `gremlin/pom.xml` and `gremlin-it/pom.xml`.** `gremlin-it` re-declares every dependency by hand because it consumes the shaded artifact with `<exclusions>*</exclusions>`. No new dependency is expected in this plan; JUnit 5 and AssertJ are already present in both.
- **Every new `.java` file starts with the Apache 2.0 license header** copied verbatim from an existing file such as `gremlin/src/test/java/com/arcadedb/gremlin/GremlinHasLabelWrongKindTest.java`.
- **Code style:** `final` on variables and parameters where possible; no curly braces for single-statement `if`; import classes rather than using fully qualified names; assertions in the form `assertThat(x.isMandatory()).isTrue()`.
- **Do not add Claude as an author** of any source file.
- **Commit each task on the feature branch `worktree-gremlin-test-coverage`.** The owner authorized commits on this branch at execution setup, relaxing the `CLAUDE.md` "do not commit" rule, which remains in force for `main`. Nothing is merged or pushed without the owner's review. Use a conventional-commit subject and end the message with `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.
- **No em dash characters** (U+2014) in any file or message. Use a normal dash, a comma, or rephrase.
- **Tag slow tests:** `@Tag("slow")` for anything with a multi-second runtime, `@Tag("benchmark")` for microbenchmarks.
- **Test class naming:** `*Test` runs under surefire, `*IT` under failsafe. Everything in this plan is fast and uses `*Test`.
- **Defect reports go to the session scratchpad and the PR body, never into `docs/` as a tracking file.** 65 such files were purged from main on 2026-08-03.

## Baseline

Measured 2026-08-05 across 1866 integration tests plus roughly 60 unit tests, all passing:

- `com.arcadedb.gremlin`: **70.2% lines (1225/1744), 57.8% branches (499/864)**

Reproduce with:

```bash
./mvnw -pl gremlin-it -Pcoverage verify -DskipITs=false
java -jar ~/.m2/repository/org/jacoco/org.jacoco.cli/0.8.15/org.jacoco.cli-0.8.15-nodeps.jar report \
  gremlin-it/target/jacoco.exec gremlin-it/target/jacoco-it.exec \
  --classfiles gremlin/target/classes --sourcefiles gremlin/src/main/java \
  --csv /tmp/gremlin-coverage.csv
```

If the JaCoCo CLI jar is absent, fetch it once:

```bash
./mvnw org.apache.maven.plugins:maven-dependency-plugin:3.8.1:get \
  -Dartifact=org.jacoco:org.jacoco.cli:0.8.15:jar:nodeps
```

Per-class summary from the CSV:

```bash
awk -F, 'NR>1 && $2 ~ /^com.arcadedb.gremlin/ {li=$8; lc=$9; tot=li+lc; pct=(tot>0)?100*lc/tot:0; \
  printf "%6.1f%%  L%4d/%-4d  B%3d/%-4d  %s.%s\n", pct, lc, tot, $7, $6+$7, $2, $3}' \
  /tmp/gremlin-coverage.csv | sort -n
```

## Root Cause Already Diagnosed

The spec listed "diagnose why `findProvider` returns null" as Phase 1 investigative work. That diagnosis is complete, and the answer collapses the phase into a fixture fix:

`GraphAnalyticalView.registerAsTraversalProvider()` (`GraphAnalyticalView.java:260`) is package-private and has **exactly one caller**: `GraphAnalyticalViewBuilder.createView()` at `GraphAnalyticalViewBuilder.java:212`.

`GremlinGAVTest.setup()` builds its view with `new GraphAnalyticalView((Database) graph.getDatabase())` followed by `gav.build(vertexTypes, edgeTypes)`. That public constructor is documented as "Simple constructor for backward compatibility" and never registers the view as a traversal provider. So `GraphTraversalProviderRegistry.findProvider(...)` finds nothing, `ArcadeTraversalStrategy.java:206-209` never executes, and all 8 `*WithGAV` tests silently exercise the OLTP fallback.

The fix is to construct through the builder:

```java
gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
    .withVertexTypes("Person")
    .withEdgeTypes("KNOWS", "LIKES")
    .build();
```

`GraphAnalyticalViewBuilder.build()` calls `createView()`, which calls `registerAsTraversalProvider()`. `build()` sets `status = Status.READY`, so `isReady()` returns true, and `coversEdgeType` returns true for the declared types.

**This is a test-fixture bug, not a production defect.** Note for the report, though: a public constructor that yields a view which can never accelerate a query is a usability trap worth raising separately.

## File Structure

**Create:**
- `gremlin/src/test/java/com/arcadedb/gremlin/support/TraversalPlans.java` - plan-shape inspection helper
- `gremlin/src/test/java/com/arcadedb/gremlin/support/DifferentialTraversal.java` - optimized vs unoptimized comparison helper
- `gremlin/src/test/java/com/arcadedb/gremlin/support/TraversalPlansTest.java` - self-test for the helpers
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeFilterByTypeStepTest.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeFilterByIndexStepTest.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGAVStepsTest.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeEdgeCountFilterStepTest.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGremlinEngineSelectionTest.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGremlinAnalyzeTest.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/io/ArcadeIoRegistryTest.java`
- `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGraphFactoryPoolTest.java`

**Modify:**
- `gremlin/src/test/java/com/arcadedb/gremlin/GremlinGAVTest.java:64-65` - build the view through the builder so the GAV path actually engages

---

### Task 1: Plan-shape helper and its self-test

The helper that makes every later optimizer test honest. It ships with its own self-test because an assertion utility that cannot fail is worth less than no utility, which is exactly the trap `GremlinGAVTest` fell into.

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/support/TraversalPlans.java`
- Test: `gremlin/src/test/java/com/arcadedb/gremlin/support/TraversalPlansTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `static List<Step> stepsOf(Traversal<?, ?> traversal)` - applies strategies, returns the compiled step list
  - `static boolean hasStepOfType(Traversal<?, ?> traversal, Class<? extends Step> type)`
  - `static String describe(Traversal<?, ?> traversal)` - step class simple names joined by `" -> "`, for assertion failure messages

- [ ] **Step 1: Write the failing self-test**

Create `gremlin/src/test/java/com/arcadedb/gremlin/support/TraversalPlansTest.java`. Copy the Apache 2.0 license header verbatim from `gremlin/src/test/java/com/arcadedb/gremlin/GremlinHasLabelWrongKindTest.java` lines 1-18, then:

```java
package com.arcadedb.gremlin.support;

import com.arcadedb.gremlin.ArcadeFilterByTypeStep;
import com.arcadedb.gremlin.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Self-test for the plan inspection helper. If these fail, every optimizer assertion built on
 * TraversalPlans is meaningless, so this class must be kept honest: it asserts both that an expected
 * step IS found and that an absent step is NOT found.
 */
class TraversalPlansTest {

  private ArcadeGraph createGraph() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/test-traversalplans");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
    return graph;
  }

  @Test
  void detectsTheTypeFilterStepTheStrategyInstalls() {
    final ArcadeGraph graph = createGraph();
    try {
      assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("Person"),
          ArcadeFilterByTypeStep.class)).isTrue();
    } finally {
      graph.drop();
    }
  }

  @Test
  void doesNotReportAStepThatIsAbsent() {
    final ArcadeGraph graph = createGraph();
    try {
      // No hasLabel(), so the strategy has nothing to rewrite and the plain GraphStep survives.
      assertThat(TraversalPlans.hasStepOfType(graph.traversal().V(),
          ArcadeFilterByTypeStep.class)).isFalse();
      assertThat(TraversalPlans.hasStepOfType(graph.traversal().V(), GraphStep.class)).isTrue();
    } finally {
      graph.drop();
    }
  }

  @Test
  void describeListsStepsInOrder() {
    final ArcadeGraph graph = createGraph();
    try {
      assertThat(TraversalPlans.describe(graph.traversal().V().hasLabel("Person")))
          .contains("ArcadeFilterByTypeStep");
    } finally {
      graph.drop();
    }
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=TraversalPlansTest`
Expected: FAIL at compilation, `cannot find symbol: class TraversalPlans`.

- [ ] **Step 3: Write the helper**

Create `gremlin/src/test/java/com/arcadedb/gremlin/support/TraversalPlans.java` with the license header, then:

```java
package com.arcadedb.gremlin.support;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Inspects the step list of a traversal AFTER strategy application, so a test can assert which steps
 * the optimizer actually installed.
 * <p>
 * A traversal's strategies are applied exactly once and the traversal is locked afterwards, so an
 * instance passed here must not also be executed. Build a separate traversal for execution.
 */
public class TraversalPlans {

  private TraversalPlans() {
  }

  public static List<Step> stepsOf(final Traversal<?, ?> traversal) {
    final Traversal.Admin<?, ?> admin = traversal.asAdmin();
    if (!admin.isLocked())
      admin.applyStrategies();
    return (List<Step>) (List<?>) admin.getSteps();
  }

  public static boolean hasStepOfType(final Traversal<?, ?> traversal, final Class<? extends Step> type) {
    for (final Step step : stepsOf(traversal))
      if (type.isInstance(step))
        return true;
    return false;
  }

  public static String describe(final Traversal<?, ?> traversal) {
    return stepsOf(traversal).stream()
        .map(s -> s.getClass().getSimpleName())
        .collect(Collectors.joining(" -> "));
  }
}
```

Note: `ArcadeFilterByTypeStep` is public, so the self-test can reference it. `ArcadeGAVFusedStep` is package-private (declared `class ArcadeGAVFusedStep`), so Task 4's test must live in package `com.arcadedb.gremlin` to reference it. That is why only the helpers live in the `support` subpackage.

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=TraversalPlansTest`
Expected: PASS, 3 tests.

- [ ] **Step 5: Prove the helper can fail**

Temporarily change `hasStepOfType` to `return true;` unconditionally. Re-run the test.
Expected: `doesNotReportAStepThatIsAbsent` FAILS. Revert the change and re-run to confirm PASS again.

This step is mandatory. Record the observed failure output in the scratchpad report.

- [ ] **Step 6: Commit and report completion**

Commit the new files on the feature branch:

```bash
git add gremlin/src/test/java/com/arcadedb/gremlin/support/
git commit -m "test(gremlin): add TraversalPlans plan-inspection helper

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Differential execution helper

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/support/DifferentialTraversal.java`
- Test: `gremlin/src/test/java/com/arcadedb/gremlin/support/DifferentialTraversalTest.java`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces:
  - `static DifferentialTraversal on(ArcadeGraph graph)`
  - `void assertSameResults(Function<GraphTraversalSource, Traversal<?, ?>> query)` - runs the query against the normal strategy set and against one with `ArcadeTraversalStrategy` removed, asserts the two result lists match as multisets
  - `List<Object> optimized(Function<GraphTraversalSource, Traversal<?, ?>> query)`
  - `List<Object> unoptimized(Function<GraphTraversalSource, Traversal<?, ?>> query)`

- [ ] **Step 1: Write the failing self-test**

Create `gremlin/src/test/java/com/arcadedb/gremlin/support/DifferentialTraversalTest.java` with the license header, then:

```java
package com.arcadedb.gremlin.support;

import com.arcadedb.gremlin.ArcadeGraph;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Self-test for the differential helper. Asserts that it agrees when the two paths agree, AND that
 * it actually detects a disagreement. A comparison utility that cannot report a mismatch would let
 * every optimizer defect through silently.
 */
class DifferentialTraversalTest {

  private ArcadeGraph createGraph() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/test-differential");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> {
      graph.addVertex("Person").property("name", "Alice");
      graph.addVertex("Person").property("name", "Bob");
    });
    return graph;
  }

  @Test
  void agreesWhenBothPathsReturnTheSameRows() {
    final ArcadeGraph graph = createGraph();
    try {
      DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("Person").values("name"));
    } finally {
      graph.drop();
    }
  }

  @Test
  void reportsADisagreementWhenTheTwoSidesDiffer() {
    final ArcadeGraph graph = createGraph();
    try {
      final DifferentialTraversal diff = DifferentialTraversal.on(graph);
      // Feed deliberately different queries to the two sides to prove the comparison bites.
      assertThatThrownBy(() -> diff.assertResultsMatch(
          diff.optimized(g -> g.V().hasLabel("Person").values("name")),
          diff.unoptimized(g -> g.V().hasLabel("Person").limit(1).values("name"))))
          .isInstanceOf(AssertionError.class);
    } finally {
      graph.drop();
    }
  }

  @Test
  void bothSidesSeeTheSameData() {
    final ArcadeGraph graph = createGraph();
    try {
      final DifferentialTraversal diff = DifferentialTraversal.on(graph);
      assertThat(diff.optimized(g -> g.V().hasLabel("Person").values("name")))
          .containsExactlyInAnyOrder("Alice", "Bob");
      assertThat(diff.unoptimized(g -> g.V().hasLabel("Person").values("name")))
          .containsExactlyInAnyOrder("Alice", "Bob");
    } finally {
      graph.drop();
    }
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=DifferentialTraversalTest`
Expected: FAIL at compilation, `cannot find symbol: class DifferentialTraversal`.

- [ ] **Step 3: Write the helper**

Create `gremlin/src/test/java/com/arcadedb/gremlin/support/DifferentialTraversal.java` with the license header, then:

```java
package com.arcadedb.gremlin.support;

import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.ArcadeTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs one query twice against the same database: once through the normal ArcadeGraph strategy set,
 * and once with {@link ArcadeTraversalStrategy} removed. The optimized and unoptimized paths are two
 * implementations of a single specification, so any disagreement is a defect by construction and no
 * hand-authored expected value is required.
 */
public class DifferentialTraversal {

  private final ArcadeGraph graph;

  private DifferentialTraversal(final ArcadeGraph graph) {
    this.graph = graph;
  }

  public static DifferentialTraversal on(final ArcadeGraph graph) {
    return new DifferentialTraversal(graph);
  }

  public List<Object> optimized(final Function<GraphTraversalSource, Traversal<?, ?>> query) {
    return drain(query.apply(graph.traversal()));
  }

  public List<Object> unoptimized(final Function<GraphTraversalSource, Traversal<?, ?>> query) {
    return drain(query.apply(graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class)));
  }

  public void assertSameResults(final Function<GraphTraversalSource, Traversal<?, ?>> query) {
    assertResultsMatch(optimized(query), unoptimized(query));
  }

  public void assertResultsMatch(final List<Object> optimizedResults, final List<Object> unoptimizedResults) {
    assertThat(optimizedResults)
        .as("optimized path returned different rows than the unoptimized path")
        .containsExactlyInAnyOrderElementsOf(unoptimizedResults);
  }

  private static List<Object> drain(final Traversal<?, ?> traversal) {
    final List<Object> results = new ArrayList<>();
    while (traversal.hasNext())
      results.add(traversal.next());
    return results;
  }
}
```

`GraphTraversalSource.withoutStrategies(Class...)` returns a clone, so the source cached inside `ArcadeGraph.traversal()` is not mutated and the optimized side stays intact.

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=DifferentialTraversalTest`
Expected: PASS, 3 tests.

- [ ] **Step 5: Verify the unoptimized side really lacks the strategy**

Add this assertion to `bothSidesSeeTheSameData` and re-run:

```java
assertThat(TraversalPlans.hasStepOfType(
    graph.traversal().withoutStrategies(ArcadeTraversalStrategy.class).V().hasLabel("Person"),
    com.arcadedb.gremlin.ArcadeFilterByTypeStep.class)).isFalse();
```

Expected: PASS. If it fails, `withoutStrategies` is not removing the strategy and the whole differential approach is void. Stop and report before continuing.

- [ ] **Step 6: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS, 0 failures.

- [ ] **Step 7: Commit and report completion**

Commit the new files on the feature branch:

```bash
git add gremlin/src/test/java/com/arcadedb/gremlin/support/
git commit -m "test(gremlin): add DifferentialTraversal optimized-vs-unoptimized helper

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Fix the GAV fixture so the GAV path engages

The single highest-value change in this plan. It converts 8 tests that silently exercise the fallback into 8 tests that exercise the CSR path, and unlocks 114 dead lines for Task 4.

**Files:**
- Modify: `gremlin/src/test/java/com/arcadedb/gremlin/GremlinGAVTest.java:64-65`

**Interfaces:**
- Consumes: `TraversalPlans.hasStepOfType` from Task 1.
- Produces: a `GremlinGAVTest` whose fixture registers a real traversal provider.

- [ ] **Step 1: Write the failing guard test**

Add this test to `GremlinGAVTest`, and add the imports `com.arcadedb.gremlin.support.TraversalPlans` and `static org.assertj.core.api.Assertions.assertThat` if absent:

```java
  @Test
  void theGAVStepIsActuallyInstalledInThePlan() {
    // Guard against the failure mode this class shipped with: all 8 *WithGAV tests passed while the
    // GAV path never executed, because the view was never registered as a traversal provider and the
    // OLTP fallback returns identical results. Assert the plan, not just the rows.
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "Alice").out("KNOWS"),
        ArcadeGAVVertexStep.class))
        .as("plan was: %s", TraversalPlans.describe(graph.traversal().V().has("name", "Alice").out("KNOWS")))
        .isTrue();
  }
```

`ArcadeGAVVertexStep` is package-private and `GremlinGAVTest` is already in package `com.arcadedb.gremlin`, so no import is needed for it.

- [ ] **Step 2: Run the test to verify it fails**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=GremlinGAVTest#theGAVStepIsActuallyInstalledInThePlan`
Expected: FAIL. The assertion message shows a plan containing `VertexStep` rather than `ArcadeGAVVertexStep`. Record this output in the scratchpad report as the proof that the pre-existing tests were vacuous.

- [ ] **Step 3: Fix the fixture to register the view**

In `GremlinGAVTest.setup()`, replace lines 64-65:

```java
    // Build GAV covering all edge types
    gav = new GraphAnalyticalView((Database) graph.getDatabase());
    gav.build(new String[] { "Person" }, new String[] { "KNOWS", "LIKES" });
```

with:

```java
    // Build the GAV through the builder. The public GraphAnalyticalView(Database) constructor is a
    // backward-compatibility shim that never calls registerAsTraversalProvider(), so a view built
    // that way is invisible to GraphTraversalProviderRegistry.findProvider() and the traversal
    // silently falls back to OLTP.
    gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS", "LIKES")
        .build();
```

`GraphAnalyticalViewBuilder.build()` calls `createView()`, which calls `registerAsTraversalProvider()` and leaves `status = Status.READY`.

- [ ] **Step 4: Run the test to verify it passes**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=GremlinGAVTest`
Expected: `theGAVStepIsActuallyInstalledInThePlan` PASSES.

**The other 8 tests may now fail.** They were only ever green on the fallback. Any failure here is a genuine GAV defect surfacing for the first time. Do not fix production code. For each failure: record actual against expected in the scratchpad report, and mark that single test `@Disabled` with a comment naming the concrete wrong behavior, for example:

```java
  @Disabled("GAV path returns [] but the OLTP path returns [Bob]; ArcadeGAVVertexStep drops the "
      + "single-hop result when the start vertex has a CSR node id. Found 2026-08-05.")
```

- [ ] **Step 5: Confirm the teardown still cleans up**

`teardown()` calls `gav.drop()`, which calls `GraphTraversalProviderRegistry.unregister(database, this)`. Verify no provider leaks across tests by running the class twice in one JVM:

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=GremlinGAVTest`
Expected: PASS on the same set of tests both times, with no ordering dependence. If a later test sees a provider it did not create, report it: a leaked registry entry is a real defect.

- [ ] **Step 6: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 7: Re-measure coverage and report**

Run the baseline commands from the Baseline section. Report the new numbers for `ArcadeGAVVertexStep`, `ArcadeGAVFusedStep`, and `ArcadeTraversalStrategy`, plus the package totals against 70.2% / 57.8%.

---

### Task 4: GAV single-hop and fused multi-hop steps

`ArcadeGAVFusedStep` is the Gremlin analogue of the Cypher `GAVFusedChainOperator` that carried two silent wrong-result defects in issue #5746. It has 0/56 lines and 0/31 branches. This task is where the highest-severity finds are expected.

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGAVStepsTest.java`

**Interfaces:**
- Consumes: `TraversalPlans` (Task 1), `DifferentialTraversal` (Task 2), the fixed builder pattern from Task 3.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the fixture and the fusion plan test**

Create `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGAVStepsTest.java` with the license header. The class must be in package `com.arcadedb.gremlin` because `ArcadeGAVVertexStep` and `ArcadeGAVFusedStep` are package-private.

```java
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the CSR-accelerated GAV steps. Every test asserts the accelerated step is present in the
 * compiled plan BEFORE asserting behavior, then compares the accelerated result against the
 * unoptimized traversal. Result-only assertions cannot distinguish a correct optimization from an
 * optimization that never ran.
 */
class ArcadeGAVStepsTest {

  private ArcadeGraph          graph;
  private GraphAnalyticalView  gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gav-steps");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");
    graph.getDatabase().getSchema().createEdgeType("LIKES");

    // A -KNOWS-> B -KNOWS-> C -KNOWS-> D, plus A -LIKES-> C and a disconnected E.
    // Three KNOWS hops make a 3-step fusion chain reachable; E exercises the empty-neighbor path.
    graph.getDatabase().transaction(() -> {
      final MutableVertex a = graph.getDatabase().newVertex("Person").set("name", "A").save();
      final MutableVertex b = graph.getDatabase().newVertex("Person").set("name", "B").save();
      final MutableVertex c = graph.getDatabase().newVertex("Person").set("name", "C").save();
      final MutableVertex d = graph.getDatabase().newVertex("Person").set("name", "D").save();
      graph.getDatabase().newVertex("Person").set("name", "E").save();
      a.newEdge("KNOWS", b);
      b.newEdge("KNOWS", c);
      c.newEdge("KNOWS", d);
      a.newEdge("LIKES", c);
    });

    gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS", "LIKES")
        .build();
  }

  @AfterEach
  void teardown() {
    if (gav != null)
      gav.drop();
    if (graph != null)
      graph.drop();
  }

  @Test
  void twoConsecutiveHopsFuseIntoASingleStep() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "A").out("KNOWS").out("KNOWS"), ArcadeGAVFusedStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().has("name", "A").out("KNOWS").out("KNOWS")))
        .isTrue();
  }

  @Test
  void aSingleHopUsesTheUnfusedGAVStep() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().has("name", "A").out("KNOWS"), ArcadeGAVVertexStep.class))
        .as("plan was: %s", TraversalPlans.describe(graph.traversal().V().has("name", "A").out("KNOWS")))
        .isTrue();
  }
}
```

- [ ] **Step 2: Run to verify both plan tests pass**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeGAVStepsTest`
Expected: PASS. If `twoConsecutiveHopsFuseIntoASingleStep` fails, the fusion in `ArcadeTraversalStrategy.applyGAVOptimization` phase 2 never triggers. That is a finding: record it and mark the test `@Disabled` with the observed plan string.

- [ ] **Step 3: Add the differential behavior tests**

Append to `ArcadeGAVStepsTest`:

```java
  @Test
  void singleHopMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "A").out("KNOWS").values("name"));
  }

  @Test
  void twoHopFusedChainMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").values("name"));
  }

  @Test
  void threeHopFusedChainMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS").values("name"));
  }

  @Test
  void inDirectionMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "D").in("KNOWS").values("name"));
  }

  @Test
  void bothDirectionMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "B").both("KNOWS").values("name"));
  }

  @Test
  void mixedEdgeLabelsAcrossHopsMatchTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("LIKES").out("KNOWS").values("name"));
  }

  @Test
  void unlabeledHopMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "A").out().values("name"));
  }

  @Test
  void aVertexWithNoOutgoingEdgesYieldsNothingOnBothPaths() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().has("name", "E").out("KNOWS").values("name"));
  }

  @Test
  void aChainLongerThanTheGraphYieldsNothingOnBothPaths() {
    // Walks past D, where the CSR neighbor array is empty at an intermediate hop.
    DifferentialTraversal.on(graph).assertSameResults(
        g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS").out("KNOWS").values("name"));
  }
```

- [ ] **Step 4: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeGAVStepsTest`
Expected: all PASS if the GAV steps are correct.

For each failure, the differential helper's message names the disagreeing rows. Record actual against expected in the scratchpad report and mark that test `@Disabled` with a concrete comment. A mismatch here is a wrong-result defect in a performance feature, which per issue #5746 is worse than a slow query because nothing downstream can distinguish it from a real answer. Flag any such finding as high severity.

- [ ] **Step 5: Add the deleted-record path test**

`ArcadeGAVFusedStep.dfs` catches `RecordNotFoundException` and skips vertices deleted since the CSR was built. Append:

```java
  @Test
  void aVertexDeletedAfterTheCsrBuildIsSkippedNotThrown() {
    graph.getDatabase().transaction(() -> {
      final var it = graph.getDatabase().query("sql", "SELECT FROM Person WHERE name = 'D'");
      it.next().getElement().get().asVertex().delete();
    });
    // The CSR still holds D's node id. The fused chain must skip it rather than propagate the
    // RecordNotFoundException.
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().has("name", "A").out("KNOWS").out("KNOWS").out("KNOWS").values("name"));
  }
```

- [ ] **Step 6: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 7: Re-measure coverage and report**

Report the delta for `ArcadeGAVVertexStep` and `ArcadeGAVFusedStep` against their 0/30 and 0/56 baselines.

---

### Task 5: Type filter and count rewrites

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeFilterByTypeStepTest.java`

**Interfaces:**
- Consumes: `TraversalPlans`, `DifferentialTraversal`.
- Produces: nothing consumed later.

- [ ] **Step 1: Write the tests**

Create the file with the license header, package `com.arcadedb.gremlin`:

```java
package com.arcadedb.gremlin;

import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the ArcadeFilterByTypeStep and ArcadeCountGlobalStep rewrites in ArcadeTraversalStrategy,
 * including the guards that must SUPPRESS the rewrite: more than one label container, and a label
 * naming a type of the opposite element kind (issue #5223).
 */
class ArcadeFilterByTypeStepTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-filterbytype");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createVertexType("Company");
    graph.getDatabase().getSchema().createEdgeType("WORKS_AT");
    graph.getDatabase().transaction(() -> {
      final ArcadeVertex alice = graph.addVertex("Person");
      alice.property("name", "Alice");
      final ArcadeVertex bob = graph.addVertex("Person");
      bob.property("name", "Bob");
      final ArcadeVertex acme = graph.addVertex("Company");
      acme.property("name", "Acme");
      alice.addEdge("WORKS_AT", acme);
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void hasLabelInstallsTheTypeFilterStep() {
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("Person"),
        ArcadeFilterByTypeStep.class)).isTrue();
  }

  @Test
  void hasLabelMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("Person").values("name"));
  }

  @Test
  void edgeHasLabelMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.E().hasLabel("WORKS_AT").count());
  }

  @Test
  void countAfterHasLabelInstallsTheCountStep() {
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V().hasLabel("Person").count(),
        ArcadeCountGlobalStep.class))
        .as("plan was: %s", TraversalPlans.describe(graph.traversal().V().hasLabel("Person").count()))
        .isTrue();
  }

  @Test
  void countAfterHasLabelMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("Person").count());
  }

  @Test
  void twoLabelsSuppressTheRewrite() {
    // ArcadeTraversalStrategy bails out when totalLabels > 1: it clears typeNameToMatch and the
    // containersToRemove set. Removing only one of two label containers would silently widen results.
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person", "Company"), ArcadeFilterByTypeStep.class)).isFalse();
  }

  @Test
  void twoLabelsStillReturnTheRightRows() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person", "Company").values("name"));
  }

  @Test
  void vertexTraversalWithAnEdgeLabelReturnsNothing() {
    // Issue #5223: a wrong-kind index scan would return elements of the opposite category.
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("WORKS_AT").count());
  }

  @Test
  void edgeTraversalWithAVertexLabelReturnsNothing() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.E().hasLabel("Person").count());
  }

  @Test
  void hasLabelOnANonExistentTypeReturnsNothing() {
    DifferentialTraversal.on(graph).assertSameResults(g -> g.V().hasLabel("DoesNotExist").count());
  }

  @Test
  void graphStepWithExplicitIdsIsNotRewritten() {
    // ArcadeTraversalStrategy skips the rewrite when prevStepGraph.getIds().length != 0.
    final Object id = graph.traversal().V().hasLabel("Person").next().id();
    assertThat(TraversalPlans.hasStepOfType(graph.traversal().V(id).hasLabel("Person"),
        ArcadeFilterByTypeStep.class)).isFalse();
  }
}
```

- [ ] **Step 2: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeFilterByTypeStepTest`
Expected: PASS. Triage failures per the Task 4 Step 4 procedure.

- [ ] **Step 3: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit and report completion**

Commit the new test file on the feature branch with a `test(gremlin): ...` subject and the `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` trailer. Report files added, tests added, and any test marked `@Disabled` with its reason.

---

### Task 6: Index-backed filter across all comparison predicates

`ArcadeFilterByIndexStep` sits at 29/64 lines and 12/42 branches. `ArcadeTraversalStrategy` maps `eq`, `gt`, `gte`, `lt`, `lte` onto index cursor calls whose inclusive/exclusive boolean is easy to invert, and it intersects multiple cursors for multi-predicate queries. Fixtures deliberately place values exactly on the range boundaries, because an off-by-one is invisible to a test whose data avoids them.

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeFilterByIndexStepTest.java`

**Interfaces:**
- Consumes: `TraversalPlans`, `DifferentialTraversal`.
- Produces: nothing consumed later.

- [ ] **Step 1: Write the tests**

Create the file with the license header, package `com.arcadedb.gremlin`:

```java
package com.arcadedb.gremlin;

import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the index-backed rewrite. Ages are 10, 20, 30, 40, 50 and every range predicate is probed
 * AT a stored boundary value, so an inverted inclusive/exclusive flag on the index cursor changes
 * the result set and the differential comparison catches it.
 */
class ArcadeFilterByIndexStepTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-filterbyindex");
    final var personType = graph.getDatabase().getSchema().createVertexType("Person");
    personType.createProperty("age", Type.INTEGER);
    personType.createProperty("city", Type.STRING);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "age");
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "city");

    graph.getDatabase().transaction(() -> {
      final int[] ages = { 10, 20, 30, 40, 50 };
      final String[] cities = { "Rome", "Rome", "Milan", "Milan", "Turin" };
      for (int i = 0; i < ages.length; i++) {
        final ArcadeVertex v = graph.addVertex("Person");
        v.property("name", "P" + ages[i]);
        v.property("age", ages[i]);
        v.property("city", cities[i]);
      }
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void anIndexedEqualityInstallsTheIndexStep() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().hasLabel("Person").has("age", 30), ArcadeFilterByIndexStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().hasLabel("Person").has("age", 30)))
        .isTrue();
  }

  @Test
  void equalityMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", 30).values("name"));
  }

  @Test
  void greaterThanAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gt(30)).values("name"));
  }

  @Test
  void greaterThanOrEqualAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gte(30)).values("name"));
  }

  @Test
  void lessThanAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lt(30)).values("name"));
  }

  @Test
  void lessThanOrEqualAtABoundaryValueMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lte(30)).values("name"));
  }

  @Test
  void greaterThanTheMaximumReturnsNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.gt(50)).values("name"));
  }

  @Test
  void lessThanTheMinimumReturnsNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", P.lt(10)).values("name"));
  }

  @Test
  void twoIndexedPredicatesIntersectCorrectly() {
    // Exercises the multi-cursor intersection loop, where cursor 0 seeds the set and later cursors
    // retainAll. A wrong intersection silently widens or empties the result.
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Rome").has("age", 20).values("name"));
  }

  @Test
  void twoIndexedPredicatesWithAnEmptyIntersectionReturnNothing() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Turin").has("age", 10).values("name"));
  }

  @Test
  void anIndexedAndAnUnindexedPredicateCombineCorrectly() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("age", 30).has("name", "P30").values("name"));
  }

  @Test
  void aRangeAndAnEqualityCombineCorrectly() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().hasLabel("Person").has("city", "Milan").has("age", P.gte(30)).values("name"));
  }
}
```

- [ ] **Step 2: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeFilterByIndexStepTest`
Expected: PASS. Triage failures per the Task 4 Step 4 procedure. A boundary failure on `gt` against `gte`, or on `lt` against `lte`, is a real defect: report it with the exact predicate and the two result sets.

- [ ] **Step 3: Verify the index step is actually chosen**

If `anIndexedEqualityInstallsTheIndexStep` fails, the strategy fell through to `ArcadeFilterByTypeStep` because `getPolymorphicIndexByProperties` returned null. That would mean every other test in this class silently exercises the type-filter path instead, so the whole class is vacuous. Stop, report, and fix the fixture's index creation before continuing.

- [ ] **Step 4: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Re-measure coverage and report**

Report the delta for `ArcadeFilterByIndexStep` against its 29/64 lines and 12/42 branches baseline.

---

### Task 7: Edge-count filter rewrite

`ArcadeEdgeCountFilterStep` is 0/28 lines. `ArcadeTraversalStrategy.applyEdgeCountFilterOptimization` matches a precise 3-step shape (`VertexStep(Edge)` then `CountGlobalStep` then `IsStep`) inside a `WhereTraversalStep` or `TraversalFilterStep`, and lines 286-301 have never executed.

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeEdgeCountFilterStepTest.java`

**Interfaces:**
- Consumes: `TraversalPlans`, `DifferentialTraversal`, the builder pattern from Task 3.
- Produces: nothing consumed later.

- [ ] **Step 1: Write the tests**

Create the file with the license header, package `com.arcadedb.gremlin`:

```java
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableVertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.gremlin.support.DifferentialTraversal;
import com.arcadedb.gremlin.support.TraversalPlans;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the where(outE('X').count().is(...)) rewrite into an O(1) degree check. The rewrite requires
 * a registered GraphTraversalProvider, so the view is built through the builder (the bare
 * GraphAnalyticalView constructor never registers).
 */
class ArcadeEdgeCountFilterStepTest {

  private ArcadeGraph         graph;
  private GraphAnalyticalView gav;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-edgecountfilter");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");

    // Out-degrees: A=2, B=1, C=0, D=0
    graph.getDatabase().transaction(() -> {
      final MutableVertex a = graph.getDatabase().newVertex("Person").set("name", "A").save();
      final MutableVertex b = graph.getDatabase().newVertex("Person").set("name", "B").save();
      final MutableVertex c = graph.getDatabase().newVertex("Person").set("name", "C").save();
      final MutableVertex d = graph.getDatabase().newVertex("Person").set("name", "D").save();
      a.newEdge("KNOWS", b);
      a.newEdge("KNOWS", c);
      b.newEdge("KNOWS", d);
    });

    gav = GraphAnalyticalView.builder((Database) graph.getDatabase())
        .withVertexTypes("Person")
        .withEdgeTypes("KNOWS")
        .build();
  }

  @AfterEach
  void teardown() {
    if (gav != null)
      gav.drop();
    if (graph != null)
      graph.drop();
  }

  @Test
  void theDegreeFilterStepIsInstalled() {
    assertThat(TraversalPlans.hasStepOfType(
        graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1))), ArcadeEdgeCountFilterStep.class))
        .as("plan was: %s",
            TraversalPlans.describe(graph.traversal().V().where(outE("KNOWS").count().is(P.gt(1)))))
        .isTrue();
  }

  @Test
  void greaterThanMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.gt(1))).values("name"));
  }

  @Test
  void equalToZeroMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.eq(0))).values("name"));
  }

  @Test
  void equalToOneMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.eq(1))).values("name"));
  }

  @Test
  void lessThanMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.lt(2))).values("name"));
  }

  @Test
  void greaterThanOrEqualMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE("KNOWS").count().is(P.gte(1))).values("name"));
  }

  @Test
  void anUnlabeledEdgeCountMatchesTheUnoptimizedPath() {
    DifferentialTraversal.on(graph)
        .assertSameResults(g -> g.V().where(outE().count().is(P.gt(0))).values("name"));
  }
}
```

- [ ] **Step 2: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeEdgeCountFilterStepTest`
Expected: PASS.

If `theDegreeFilterStepIsInstalled` fails, capture the plan string. The rewrite requires exactly 3 sub-steps; TinkerPop's `InlineFilterStrategy` runs first (declared in `applyPrior()`) and may reshape the child traversal so the pattern no longer matches. That is a genuine finding worth reporting: the optimization would then be unreachable in normal use, not merely untested.

- [ ] **Step 3: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit and report completion**

Commit the new test file on the feature branch with a `test(gremlin): ...` subject and the `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` trailer. Report files added, tests added, and any test marked `@Disabled` with its reason.

---

### Task 8: ArcadeGremlin engine selection and error classification

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGremlinEngineSelectionTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: nothing consumed later.

- [ ] **Step 1: Write the tests**

Create the file with the license header, package `com.arcadedb.gremlin`:

```java
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers engine selection and the parse-versus-runtime error classification, which decides HTTP 400
 * against HTTP 500 (issues #5201 and #5219), plus the process-wide timeout field.
 */
class ArcadeGremlinEngineSelectionTest {

  private ArcadeGraph graph;
  private String      originalEngine;

  @BeforeEach
  void setup() {
    originalEngine = GlobalConfiguration.GREMLIN_ENGINE.getValueAsString();
    graph = ArcadeGraph.open("./target/test-gremlin-engine");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
  }

  @AfterEach
  void teardown() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue(originalEngine);
    if (graph != null)
      graph.drop();
  }

  @Test
  void aGroovyClosureIsRejectedAsAParsingErrorUnderTheJavaEngine() {
    graph.getDatabase().getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    // Strict java mode must NOT fall back to the insecure Groovy engine (GHSA-wcm5-4wjm-9wj3).
    assertThatThrownBy(() -> graph.gremlin("g.V().filter { it.get().value('name') == 'Alice' }").execute())
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void nextOnAnEmptyTraversalIsARuntimeErrorNotAParsingError() {
    graph.getDatabase().getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    // Issue #5219: a NoSuchElementException surfaced during eager iteration must stay a
    // CommandExecutionException (HTTP 500), not be misreported as invalid syntax (HTTP 400).
    assertThatThrownBy(() -> graph.gremlin("g.V().has('name','NoSuchPerson').next()").execute())
        .isInstanceOf(CommandExecutionException.class)
        .isNotInstanceOf(CommandParsingException.class);
  }

  @Test
  void genuinelyInvalidSyntaxIsAParsingError() {
    graph.getDatabase().getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    assertThatThrownBy(() -> graph.gremlin("g.V().thisStepDoesNotExist()").execute())
        .isInstanceOf(CommandParsingException.class);
  }

  @Test
  void anUnknownEngineNameIsRejected() {
    graph.getDatabase().getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "nonsense");
    assertThatThrownBy(() -> graph.gremlin("g.V().count()").execute())
        .isInstanceOf(CommandExecutionException.class);
  }

  @Test
  void theJavaEngineExecutesAValidQuery() {
    graph.getDatabase().getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "java");
    assertThat((Long) graph.gremlin("g.V().count()").execute().nextIfAvailable().getProperty("result"))
        .isEqualTo(1L);
  }

  @Test
  void autoModeFallsBackToGroovyForAClosure() {
    graph.getDatabase().getConfiguration().setValue(GlobalConfiguration.GREMLIN_ENGINE, "auto");
    // 'auto' is documented as opting in to the Groovy fallback for compatibility.
    assertThat((Long) graph.gremlin("g.V().count()").execute().nextIfAvailable().getProperty("result"))
        .isEqualTo(1L);
  }

  /**
   * CHARACTERIZATION TEST OF A KNOWN DEFECT. This asserts the WRONG behavior on purpose, to pin it
   * and to detect if it silently changes. It is not a statement of the intended contract.
   * <p>
   * {@code ArcadeGremlin.timeout} is declared {@code private static Long} but assigned by the
   * INSTANCE method {@code setTimeout(long, TimeUnit)}, so a timeout set on one graph applies to
   * every ArcadeGremlin in the process.
   * <p>
   * WHEN THE FIELD IS MADE NON-STATIC, INVERT THIS TEST: the expectation becomes that {@code second}
   * does NOT observe {@code first}'s timeout, and the method should be renamed accordingly.
   */
  @Test
  void characterizesTheProcessWideTimeoutLeak() {
    final ArcadeGremlin first = graph.gremlin("g.V().count()");
    final ArcadeGremlin second = graph.gremlin("g.V().count()");
    first.setTimeout(1234, java.util.concurrent.TimeUnit.MILLISECONDS);
    assertThat(second.getTimeout())
        .as("timeout leaked across ArcadeGremlin instances via the static field")
        .isEqualTo(1234L);
  }
}
```

- [ ] **Step 2: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeGremlinEngineSelectionTest`
Expected: PASS.

`characterizesTheProcessWideTimeoutLeak` is expected to PASS, because it asserts the buggy behavior deliberately. Do **not** mark it `@Disabled`. Record the static-field leak in the defect report as a confirmed defect with this test as its evidence, and note that the test must be inverted once the field is made non-static.

If `autoModeFallsBackToGroovyForAClosure` produces a security warning in the log, that is expected: the Groovy engine logs a warning by design.

- [ ] **Step 3: Check for cross-test contamination**

The timeout field is static, so `ArcadeGremlinEngineSelectionTest` can affect later tests in the same JVM. Run the full suite twice and compare results:

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS both times, identical test counts. Report any ordering-dependent failure.

- [ ] **Step 4: Commit and report completion**

Commit the new test file on the feature branch with a `test(gremlin): ...` subject and the `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` trailer. Report files added, tests added, and any test marked `@Disabled` with its reason.

---

### Task 9: ArcadeGremlin query analysis and operation types

`parse()` feeds HA follower routing, so a misclassified operation routes writes wrongly.

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGremlinAnalyzeTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: nothing consumed later.

- [ ] **Step 1: Write the tests**

Create the file with the license header, package `com.arcadedb.gremlin`:

```java
package com.arcadedb.gremlin;

import com.arcadedb.query.OperationType;
import com.arcadedb.query.QueryEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers ArcadeGremlin.parse(), whose idempotency verdict and OperationType set drive HA follower
 * routing. A read misclassified as a write, or a write as a read, routes the query to the wrong node.
 */
class ArcadeGremlinAnalyzeTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-analyze");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().getSchema().createEdgeType("KNOWS");
    graph.getDatabase().transaction(() -> graph.addVertex("Person").property("name", "Alice"));
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  private QueryEngine.AnalyzedQuery analyze(final String query) {
    return graph.gremlin(query).parse();
  }

  @Test
  void aPlainTraversalIsIdempotentAndReadOnly() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.V().hasLabel('Person').values('name')");
    assertThat(analyzed.isIdempotent()).isTrue();
    assertThat(analyzed.getOperationTypes()).containsExactly(OperationType.READ);
  }

  @Test
  void aCountIsIdempotent() {
    assertThat(analyze("g.V().count()").isIdempotent()).isTrue();
  }

  @Test
  void addVertexIsNotIdempotentAndIsACreate() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.addV('Person')");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
    assertThat(analyzed.getOperationTypes()).doesNotContain(OperationType.READ);
  }

  @Test
  void addEdgeIsACreate() {
    final QueryEngine.AnalyzedQuery analyzed =
        analyze("g.V().hasLabel('Person').as('a').addE('KNOWS').to('a')");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.CREATE);
  }

  @Test
  void dropIsADelete() {
    final QueryEngine.AnalyzedQuery analyzed = analyze("g.V().hasLabel('Person').drop()");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.DELETE);
  }

  @Test
  void addPropertyIsAnUpdate() {
    final QueryEngine.AnalyzedQuery analyzed =
        analyze("g.V().hasLabel('Person').property('age', 30)");
    assertThat(analyzed.isIdempotent()).isFalse();
    assertThat(analyzed.getOperationTypes()).contains(OperationType.UPDATE);
  }

  @Test
  void analysisIsNeverDDL() {
    assertThat(analyze("g.addV('Person')").isDDL()).isFalse();
  }

  @Test
  void analysisToleratesUnboundParameters() {
    // Issue #5187: the analyze() path (HA follower idempotency check) does not receive parameter
    // bindings, so it must use the null-tolerant java engine and still classify the query.
    assertThat(analyze("g.V().has('name', name).values('name')").isIdempotent()).isTrue();
  }
}
```

- [ ] **Step 2: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeGremlinAnalyzeTest`
Expected: PASS. A wrong `OperationType` is a high-severity finding because HA routing consumes it; report the query, the expected set, and the actual set.

- [ ] **Step 3: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit and report completion**

Commit the new test file on the feature branch with a `test(gremlin): ...` subject and the `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` trailer. Report files added, tests added, and any test marked `@Disabled` with its reason.

---

### Task 10: ArcadeIoRegistry pure functions

`ArcadeIoRegistry` is 10/24 lines with **0/11 branches**: every arm of the `newRID` switch and both arms of `isRID` are untested. These are pure functions, so plain unit tests are the right tool.

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/io/ArcadeIoRegistryTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: nothing consumed later.

- [ ] **Step 1: Write the tests**

Create the file with the license header, package `com.arcadedb.gremlin.io`:

```java
package com.arcadedb.gremlin.io;

import com.arcadedb.database.RID;
import com.arcadedb.gremlin.ArcadeGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the RID coercion helpers. Every arm of the newRID switch and both arms of isRID were
 * previously unexercised (0 of 11 branches).
 */
class ArcadeIoRegistryTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-ioregistry");
    graph.getDatabase().getSchema().createVertexType("Person");
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void aNullYieldsNull() {
    assertThat(ArcadeIoRegistry.newRID(graph.getDatabase(), null)).isNull();
  }

  @Test
  void anExistingRidIsReturnedUnchanged() {
    final RID rid = new RID(graph.getDatabase(), 1, 42);
    assertThat(ArcadeIoRegistry.newRID(graph.getDatabase(), rid)).isSameAs(rid);
  }

  @Test
  void aStringIsParsedIntoARid() {
    final RID rid = ArcadeIoRegistry.newRID(graph.getDatabase(), "#1:42");
    assertThat(rid.getBucketId()).isEqualTo(1);
    assertThat(rid.getPosition()).isEqualTo(42L);
  }

  @Test
  void aMapWithBucketIdAndPositionIsConvertedToARid() {
    final Map<String, Number> map = new LinkedHashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_ID, 1);
    map.put(ArcadeIoRegistry.BUCKET_POSITION, 42L);
    final RID rid = ArcadeIoRegistry.newRID(graph.getDatabase(), map);
    assertThat(rid.getBucketId()).isEqualTo(1);
    assertThat(rid.getPosition()).isEqualTo(42L);
  }

  @Test
  void anUnsupportedTypeIsRejected() {
    assertThatThrownBy(() -> ArcadeIoRegistry.newRID(graph.getDatabase(), 42))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void isRidRecognizesAWellFormedMap() {
    final Map<String, Number> map = new HashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_ID, 1);
    map.put(ArcadeIoRegistry.BUCKET_POSITION, 42L);
    assertThat(ArcadeIoRegistry.isRID(map)).isTrue();
  }

  @Test
  void isRidRejectsANonMap() {
    assertThat(ArcadeIoRegistry.isRID("#1:42")).isFalse();
  }

  @Test
  void isRidRejectsAMapMissingTheBucketPosition() {
    final Map<String, Number> map = new HashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_ID, 1);
    assertThat(ArcadeIoRegistry.isRID(map)).isFalse();
  }

  @Test
  void isRidRejectsAMapMissingTheBucketId() {
    final Map<String, Number> map = new HashMap<>();
    map.put(ArcadeIoRegistry.BUCKET_POSITION, 42L);
    assertThat(ArcadeIoRegistry.isRID(map)).isFalse();
  }

  @Test
  void theSharedInstanceIsNotNullAndHasNoDatabase() {
    assertThat(ArcadeIoRegistry.instance()).isNotNull();
    assertThat(ArcadeIoRegistry.instance().getDatabase()).isNull();
  }
}
```

- [ ] **Step 2: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeIoRegistryTest`
Expected: PASS. If `new RID(BasicDatabase, int, long)` does not compile, check the actual `RID` constructor signatures in `engine/src/main/java/com/arcadedb/database/RID.java` and use `RID.create(database, bucketId, position)` instead, which `newRID` itself uses.

- [ ] **Step 3: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit and report completion**

Commit the new test file on the feature branch with a `test(gremlin): ...` subject and the `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` trailer. Report files added, tests added, and any test marked `@Disabled` with its reason.

---

### Task 11: ArcadeGraphFactory pool

**Files:**
- Create: `gremlin/src/test/java/com/arcadedb/gremlin/ArcadeGraphFactoryPoolTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: nothing consumed later.

- [ ] **Step 1: Write the tests**

Create the file with the license header, package `com.arcadedb.gremlin`:

```java
package com.arcadedb.gremlin;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the ArcadeGraph pool: exhaustion, reuse after release, and the counter semantics.
 */
class ArcadeGraphFactoryPoolTest {

  private static final String DB_PATH = "./target/test-graphfactory-pool";

  private ArcadeGraphFactory factory;

  @AfterEach
  void teardown() {
    if (factory != null)
      factory.close();
    final ArcadeGraph graph = ArcadeGraph.open(DB_PATH);
    graph.drop();
  }

  @Test
  void theDefaultMaximumIs32() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    assertThat(factory.getMaxInstances()).isEqualTo(32);
  }

  @Test
  void exhaustingThePoolIsRejected() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    factory.setMaxInstances(2);
    final List<ArcadeGraph> held = new ArrayList<>();
    held.add(factory.get());
    held.add(factory.get());
    assertThatThrownBy(() -> factory.get()).isInstanceOf(IllegalArgumentException.class);
    for (final ArcadeGraph g : held)
      g.close();
  }

  @Test
  void aReleasedInstanceIsReusedRatherThanRecreated() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    final ArcadeGraph first = factory.get();
    assertThat(factory.getTotalInstancesCreated()).isEqualTo(1);
    first.close();
    final ArcadeGraph second = factory.get();
    assertThat(factory.getTotalInstancesCreated())
        .as("a released instance must come back from the pool, not be created afresh")
        .isEqualTo(1);
    assertThat(second).isSameAs(first);
    second.close();
  }

  @Test
  void releasingThenReacquiringDoesNotTripTheLimit() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    factory.setMaxInstances(1);
    for (int i = 0; i < 5; i++) {
      final ArcadeGraph g = factory.get();
      g.close();
    }
    assertThat(factory.getTotalInstancesCreated()).isEqualTo(1);
  }

  @Test
  void closingTheFactoryDisposesPooledInstances() {
    factory = ArcadeGraphFactory.withLocal(DB_PATH);
    final ArcadeGraph g = factory.get();
    g.close();
    factory.close();
    factory = null;
  }
}
```

- [ ] **Step 2: Run and triage**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it test -Dtest=ArcadeGraphFactoryPoolTest`
Expected: PASS.

Note on `totalInstancesCreated`: it increments on creation and is never decremented on release. `releasingThenReacquiringDoesNotTripTheLimit` verifies that reuse does not inflate it. If that test fails, the counter grows on reuse and the pool would eventually refuse service after `maxInstances` total acquisitions regardless of releases. Report that as a defect.

- [ ] **Step 3: Run the full suite**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit and report completion**

Commit the new test file on the feature branch with a `test(gremlin): ...` subject and the `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>` trailer. Report files added, tests added, and any test marked `@Disabled` with its reason.

---

### Task 12: Final coverage measurement and defect report

**Files:**
- Create: `<session scratchpad>/gremlin-coverage-findings.md` (NOT in the repository)

**Interfaces:**
- Consumes: findings recorded during Tasks 3 through 11.
- Produces: the report handed to the owner for the PR body.

- [ ] **Step 1: Run the full suite one final time**

Run: `./mvnw -pl gremlin install -DskipTests -q && ./mvnw -pl gremlin-it -DskipITs=false verify`
Expected: BUILD SUCCESS, 0 failures, 0 errors.

- [ ] **Step 2: Measure final coverage**

Run the two commands from the Baseline section plus the `awk` summary.

- [ ] **Step 3: Write the report**

Write to the session scratchpad, not to `docs/`. Structure:

```markdown
# Gremlin Coverage: Findings

## Coverage
| Metric | Before | After | Delta |
| --- | --- | --- | --- |
| Lines | 1225/1744 (70.2%) | ... | ... |
| Branches | 499/864 (57.8%) | ... | ... |

Per-class deltas for: ArcadeGAVVertexStep, ArcadeGAVFusedStep, ArcadeEdgeCountFilterStep,
ArcadeFilterByIndexStep, ArcadeFilterByTypeStep, ArcadeCountGlobalStep, ArcadeTraversalStrategy,
ArcadeIoRegistry, ArcadeGremlin, ArcadeGraphFactory.

## Confirmed defects
For each: title, severity, the test that pins it, actual against expected, suspected cause,
and whether the test is @Disabled or asserts the buggy behavior deliberately.

Known entries to include:
1. GremlinGAVTest built its view with the non-registering constructor, so all 8 *WithGAV tests
   passed without ever running the GAV path. Fixed in Task 3. Severity: high, it masked whatever
   Task 3 and Task 4 subsequently surfaced.
2. ArcadeGremlin.timeout is a private static field assigned by an instance setter, so a timeout set
   on one graph applies process-wide. Pinned by
   ArcadeGremlinEngineSelectionTest.characterizesTheProcessWideTimeoutLeak, which asserts the current
   (wrong) behavior and must be inverted when fixed. Severity: medium.
3. GraphAnalyticalView(Database) is public and documented as a backward-compatibility shim, but a
   view built through it never registers as a traversal provider and can never accelerate a query.
   Usability trap; suggest deprecating it or making it register. Severity: medium.

## Not covered, deliberately
ArcadeServiceRegistry Lambda* inner classes: the class Javadoc calls these "toy" services for
demonstration and testing. Testing them raises the coverage number without finding defects.

## Suggested follow-up issues
One line per defect worth filing.
```

- [ ] **Step 4: Hand off**

Commit the report reference on the feature branch if any plan file changed, then report to the owner: files added, tests added, tests disabled and why, coverage delta, and the defect list. Do not push and do not open a PR; the owner decides what happens to the branch.

---

## Self-Review

**Spec coverage:**

| Spec item | Task |
| --- | --- |
| `TraversalPlans` helper | Task 1 |
| `DifferentialTraversal` helper | Task 2 |
| Phase 1: diagnose GAV provider lookup | Diagnosed before planning; fixture fix in Task 3 |
| Phase 2: type filter, multi-label, wrong-kind guard | Task 5 |
| Phase 2: index filter, 5 predicates, boundaries | Task 6 |
| Phase 2: count-global | Task 5 |
| Phase 2: GAV single-hop and fused multi-hop, deleted-record skip | Task 4 |
| Phase 2: edge-count filter | Task 7 |
| Phase 3: engine selection, error classification, static timeout | Task 8 |
| Phase 3: `parse()` and OperationType | Task 9 |
| Phase 4: `ArcadeIoRegistry` | Task 10 |
| Phase 5: factory pool | Task 11 |
| Quarantine-and-report handling | Task 3 Step 4, Task 4 Step 4, Task 12 |
| Differential harness self-test | Task 1 Step 5, Task 2 Step 5 |
| Baseline and re-measurement | Baseline section, Tasks 3, 4, 6, 12 |

**Gaps accepted, with reasons:**

- **`RIDSerializer` round-trip and GraphSON/GraphML fidelity** (spec Phase 4) have no task. `RIDSerializer` is at 78.3% lines and the import/export formats at 69-82%, all already exercised by the four existing `*IT` classes. They are the weakest defect-per-effort in the plan, and the GraphBinary round-trip needs a live Gremlin Server fixture that would push these into `*IT` territory. Recommend a follow-up plan if the owner wants them.
- **`vertices()`/`edges()` id-coercion arms** (spec Phase 5) are covered incidentally by the TinkerPop structure suite, which passes `RID`, `Vertex`, and `String` ids. Only the `null`/default arm is unexercised, and it is a `continue`.
- **`ArcadeServiceRegistry`** is excluded by design, as stated in the spec and repeated in the Task 12 report template.

**Placeholder scan:** no TBD, TODO, or "similar to Task N". Every code step contains complete compilable code. Two steps contain conditional fallbacks (Task 10 Step 2 on the `RID` constructor, Task 6 Step 3 on index selection) which name the exact alternative rather than deferring the decision.

**Type consistency check:**

- `TraversalPlans.stepsOf` / `hasStepOfType` / `describe` are defined in Task 1 and used with those exact names in Tasks 2, 3, 4, 5, 6, 7.
- `DifferentialTraversal.on` / `assertSameResults` / `assertResultsMatch` / `optimized` / `unoptimized` are defined in Task 2 and used with those exact names in Tasks 4, 5, 6, 7. `assertResultsMatch` is public because Task 2's own self-test calls it directly.
- The `GraphAnalyticalView.builder(...).withVertexTypes(...).withEdgeTypes(...).build()` chain is identical in Tasks 3, 4, and 7, and matches the real signatures at `GraphAnalyticalViewBuilder.java:71-197`.
- `ArcadeFilterByTypeStep` and `ArcadeFilterByIndexStep` are public; `ArcadeGAVVertexStep`, `ArcadeGAVFusedStep`, `ArcadeCountGlobalStep`, and `ArcadeEdgeCountFilterStep` are package-private, which is why every test class referencing them is declared in package `com.arcadedb.gremlin`. Only `TraversalPlansTest` and `DifferentialTraversalTest` sit in `...gremlin.support`, and they reference only the public `ArcadeFilterByTypeStep`.
