# Cypher Write-Counter Surfacing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface Cypher write counters (`QueryStatistics`) over HTTP `POST /command` and gRPC, aggregate them across top-level `UNION` writes and `CALL { ... }` write subqueries, and close two counter-fidelity correctness bugs plus missing constraint-kind coverage.

**Architecture:** Builds on the `QueryStatistics` / `ResultSet.getStatistics()` infrastructure merged in #5016. A new `QueryStatistics.add()` merge and a `CommandContext.setStatistics()` instance-share close the UNION/CALL aggregation gap in the engine; HTTP and gRPC read the already-attached accumulator off the `ResultSet` and serialize it (camelCase JSON / a new proto message). Three engine fidelity fixes are folded in.

**Tech Stack:** Java 21, Maven multi-module, JUnit 5 + AssertJ, ArcadeDB `com.arcadedb.serializer.json.JSONObject`, protobuf3 (grpc module), neo4j-java-driver (Bolt IT).

## Global Constraints

- Java 21+; match existing code style (final on locals/params; no curly braces for single-statement `if`; import classes, no FQNs).
- Use `com.arcadedb.serializer.json.JSONObject` for JSON, not org.json.
- No new dependencies. Apache-2.0 compatible only.
- Do not add Claude as author. No issue numbers in Javadoc/comments (state behavioral invariants only).
- Every backend change compiles before moving on; run the affected module tests, not the whole suite.
- Test tags: none needed here (these are fast functional tests) except the Bolt/gRPC ITs which already follow the `IT` server-boot pattern.
- Commit after each task's tests pass. Commit message format `feat(#5015): <subject>` (or `test(#5015):`/`fix(#5015):`). End body with the Co-Authored-By trailer only if the repo convention requires it - here, do NOT add Claude as author.

---

### Task 1: `QueryStatistics.add()` merge + camelCase `toJSON()`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/QueryStatistics.java`
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/QueryStatisticsTest.java`

**Interfaces:**
- Produces: `void QueryStatistics.add(QueryStatistics other)` (null `other` is a no-op); `com.arcadedb.serializer.json.JSONObject QueryStatistics.toJSON()` (camelCase keys, always includes all 11 counters + `containsUpdates`).

- [ ] **Step 1: Write the failing test** — append to `QueryStatisticsTest`:

```java
  @Test
  void addMergesCountersFieldWise() {
    final QueryStatistics a = new QueryStatistics();
    a.incNodesCreated();
    a.addPropertiesSet(2);
    final QueryStatistics b = new QueryStatistics();
    b.incNodesCreated();
    b.incRelationshipsCreated();
    b.addPropertiesSet(3);
    a.add(b);
    assertThat(a.getNodesCreated()).isEqualTo(2);
    assertThat(a.getRelationshipsCreated()).isEqualTo(1);
    assertThat(a.getPropertiesSet()).isEqualTo(5);
    assertThat(a.containsUpdates()).isTrue();
  }

  @Test
  void addNullIsNoOp() {
    final QueryStatistics a = new QueryStatistics();
    a.incNodesCreated();
    a.add(null);
    assertThat(a.getNodesCreated()).isEqualTo(1);
  }

  @Test
  void toJSONEmitsAllCamelCaseCountersAndContainsUpdates() {
    final QueryStatistics s = new QueryStatistics();
    s.incNodesCreated();
    s.addPropertiesSet(2);
    final JSONObject json = s.toJSON();
    assertThat(json.getInt("nodesCreated")).isEqualTo(1);
    assertThat(json.getInt("propertiesSet")).isEqualTo(2);
    assertThat(json.getInt("relationshipsCreated")).isEqualTo(0);
    assertThat(json.getBoolean("containsUpdates")).isTrue();
  }
```

Add imports to the test if missing: `import com.arcadedb.serializer.json.JSONObject;` and `import static org.assertj.core.api.Assertions.assertThat;`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && mvn -q -Dtest=QueryStatisticsTest test`
Expected: compile failure / FAIL — `add(...)` and `toJSON()` do not exist.

- [ ] **Step 3: Implement `add()` and `toJSON()`** in `QueryStatistics.java` (add import `com.arcadedb.serializer.json.JSONObject`), after `restore(...)`:

```java
  /**
   * Adds every counter from {@code other} into this accumulator. A {@code null} argument is a no-op.
   * Used to aggregate the statistics of UNION branches and CALL subqueries into the outer result.
   */
  public void add(final QueryStatistics other) {
    if (other == null)
      return;
    nodesCreated += other.nodesCreated;
    nodesDeleted += other.nodesDeleted;
    relationshipsCreated += other.relationshipsCreated;
    relationshipsDeleted += other.relationshipsDeleted;
    propertiesSet += other.propertiesSet;
    labelsAdded += other.labelsAdded;
    labelsRemoved += other.labelsRemoved;
    indexesAdded += other.indexesAdded;
    indexesRemoved += other.indexesRemoved;
    constraintsAdded += other.constraintsAdded;
    constraintsRemoved += other.constraintsRemoved;
  }

  /**
   * Serializes the counters into an ArcadeDB-native camelCase JSON object. All eleven counters are
   * always present (zero when unset) plus a {@code containsUpdates} flag, giving a stable shape.
   */
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("nodesCreated", nodesCreated);
    json.put("nodesDeleted", nodesDeleted);
    json.put("relationshipsCreated", relationshipsCreated);
    json.put("relationshipsDeleted", relationshipsDeleted);
    json.put("propertiesSet", propertiesSet);
    json.put("labelsAdded", labelsAdded);
    json.put("labelsRemoved", labelsRemoved);
    json.put("indexesAdded", indexesAdded);
    json.put("indexesRemoved", indexesRemoved);
    json.put("constraintsAdded", constraintsAdded);
    json.put("constraintsRemoved", constraintsRemoved);
    json.put("containsUpdates", containsUpdates());
    return json;
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd engine && mvn -q -Dtest=QueryStatisticsTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/executor/QueryStatistics.java engine/src/test/java/com/arcadedb/query/sql/executor/QueryStatisticsTest.java
git commit -m "feat(#5015): add QueryStatistics.add() merge and camelCase toJSON()"
```

---

### Task 2: Aggregate counters across CALL subqueries and top-level UNION

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/CommandContext.java` (add `setStatistics`)
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/BasicCommandContext.java` (impl `setStatistics`)
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherExecutionPlan.java` (`executeWithSeedRow` signature + share stats; `executeUnion` write aggregation)
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/SubqueryStep.java` (pass outer context)
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/UnionStep.java` (aggregate branch stats)
- Test: `engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java`

**Interfaces:**
- Consumes: `QueryStatistics.add(...)` from Task 1.
- Produces: `void CommandContext.setStatistics(QueryStatistics)`; `ResultSet CypherExecutionPlan.executeWithSeedRow(Result seedRow, CommandContext outerContext)` (new second param); `QueryStatistics UnionStep.getAggregatedStatistics()`.

- [ ] **Step 1: Write the failing tests** — append to `CypherQueryStatisticsTest`:

```java
  @Test
  void callSubqueryWriteCountsPropagateToOuter() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "UNWIND [1,2] AS x CALL { WITH x CREATE (:CallNode {v:x}) } RETURN x");
      assertThat(s.getNodesCreated()).isEqualTo(2);
      assertThat(s.getPropertiesSet()).isEqualTo(2);
      assertThat(s.getLabelsAdded()).isEqualTo(2);
      assertThat(s.containsUpdates()).isTrue();
    });
  }

  @Test
  void topLevelUnionWriteCountsAreSummed() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "CREATE (:UnionA {n:1}) RETURN 1 AS r UNION ALL CREATE (:UnionB {n:2}) RETURN 2 AS r");
      assertThat(s.getNodesCreated()).isEqualTo(2);
      assertThat(s.getPropertiesSet()).isEqualTo(2);
      assertThat(s.getLabelsAdded()).isEqualTo(2);
      assertThat(s.containsUpdates()).isTrue();
    });
  }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd engine && mvn -q -Dtest=CypherQueryStatisticsTest#callSubqueryWriteCountsPropagateToOuter+topLevelUnionWriteCountsAreSummed test`
Expected: FAIL — counters are 0 (CALL/UNION not aggregated).

- [ ] **Step 3a: Add `setStatistics` to the context interface + impl.**

In `CommandContext.java`, next to `QueryStatistics getStatistics();`:

```java
  void setStatistics(QueryStatistics statistics);
```

In `BasicCommandContext.java`, right after `getStatistics()`:

```java
  @Override
  public void setStatistics(final QueryStatistics statistics) {
    this.statistics = statistics;
  }
```

If any other `CommandContext` implementation exists that does not extend `BasicCommandContext`, add a no-op or field-backed `setStatistics`. Verify with:
`grep -rln "implements CommandContext" engine/src/main/java` — as of this writing only `BasicCommandContext` implements it directly.

- [ ] **Step 3b: Share the outer accumulator into CALL subqueries.**

In `CypherExecutionPlan.java`, change the `executeWithSeedRow` signature and share stats. Replace the method header and the two inner-context creations:

Header (line ~311):
```java
  public ResultSet executeWithSeedRow(final Result seedRow, final CommandContext outerContext) {
```

In the UNION-in-CALL branch, after `setupFunctionResolver(ctx);` (the `ctx` created ~line 323) add:
```java
      if (outerContext != null)
        ctx.setStatistics(outerContext.getStatistics());
```
and change the recursive branch call from `branchPlan.executeWithSeedRow(seedRow)` to:
```java
        final ResultSet rs = branchPlan.executeWithSeedRow(seedRow, outerContext);
```

In the non-UNION path, after `setupFunctionResolver(context);` (the `context` created ~line 349) add:
```java
    if (outerContext != null)
      context.setStatistics(outerContext.getStatistics());
```

Delete the now-stale limitation comment at the top of `executeWithSeedRow` (the `// Limitation: each branch/inner plan runs with its own BasicCommandContext ...` block).

- [ ] **Step 3c: Update the CALL caller to pass the outer context.**

In `SubqueryStep.java` line ~424, change:
```java
    final ResultSet resultSet = innerPlan.executeWithSeedRow(filterSeedRow(outerRow), context);
```

- [ ] **Step 3d: Aggregate branch stats in `UnionStep`.**

In `UnionStep.java`: add a field and a getter, and accumulate when a branch is exhausted. Add near the other fields:
```java
  private final QueryStatistics aggregatedStatistics = new QueryStatistics();
```
Add import `import com.arcadedb.query.sql.executor.QueryStatistics;` if absent.

In `syncPull`, at the point where the current branch's result set is exhausted and the step advances to the next branch (right before incrementing `currentQueryIndex` past a finished `currentResultSet`), fold its stats in exactly once:
```java
        if (currentResultSet != null)
          aggregatedStatistics.add(currentResultSet.getStatistics().orElse(null));
```
Place this so it runs once per branch as that branch completes (i.e. immediately before the code moves `currentResultSet` to the next branch's `execute()`). If the loop structure makes "just exhausted" hard to pinpoint, accumulate right after the `while(currentResultSet.hasNext())` for a branch drains and before loading the next.

Add the getter:
```java
  public QueryStatistics getAggregatedStatistics() {
    return aggregatedStatistics;
  }
```

- [ ] **Step 3e: Attach the aggregate for write UNIONs in `executeUnion`.**

Replace the body of `executeUnion()` (keeping the read path lazy):
```java
  private ResultSet executeUnion() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    context.setInputParameters(parameters);
    setupFunctionResolver(context);

    final UnionStep unionStep = new UnionStep(unionSubqueryPlans, unionRemoveDuplicates, context);

    // Read UNION: stay lazy/streaming, no statistics to surface.
    if (statement.isReadOnly())
      return unionStep.syncPull(context, 100);

    // Write UNION: materialize to force each branch's mutation steps to execute (mirroring the
    // non-union write path), then surface the summed per-branch statistics.
    final ResultSet rs = unionStep.syncPull(context, 100);
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    final IteratorResultSet out = new IteratorResultSet(rows.iterator());
    final QueryStatistics aggregated = unionStep.getAggregatedStatistics();
    if (aggregated.containsUpdates())
      out.setStatistics(aggregated);
    return out;
  }
```
Ensure imports exist in `CypherExecutionPlan.java`: `java.util.List`, `java.util.ArrayList`, `com.arcadedb.query.sql.executor.Result`, `IteratorResultSet`, `QueryStatistics` (most already imported; add any missing). Remove the stale `// Limitation:` comment block at the top of `executeUnion`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd engine && mvn -q -Dtest=CypherQueryStatisticsTest test`
Expected: PASS (new + existing cases).

- [ ] **Step 5: Run the wider Cypher regression subset to catch UNION/CALL behavior changes**

Run: `cd engine && mvn -q -Dtest="com.arcadedb.query.opencypher.**" test`
Expected: PASS. If a UNION/CALL row-count test regresses, the materialization change in `executeUnion` is the suspect — confirm the read path (`statement.isReadOnly()`) still returns the lazy `unionStep.syncPull(...)`.

- [ ] **Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/sql/executor/CommandContext.java engine/src/main/java/com/arcadedb/query/sql/executor/BasicCommandContext.java engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherExecutionPlan.java engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/SubqueryStep.java engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/UnionStep.java engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java
git commit -m "feat(#5015): aggregate write counters across CALL subqueries and UNION"
```

---

### Task 3: Fix constraint-vs-plain-index miscount + constraint-kind coverage

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/query/OpenCypherQueryEngine.java`
- Test: `engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java`

**Interfaces:**
- Produces: internal helper `uniqueIndexExistsOnProperties(Schema, String, String[])`; no public API change.

- [ ] **Step 1: Write the failing tests** — append to `CypherQueryStatisticsTest`:

```java
  @Test
  void notNullConstraintCountsAsConstraintAdded() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:NnLabel {p:1})");
      final QueryStatistics s = statsOf(database,
          "CREATE CONSTRAINT FOR (n:NnLabel) REQUIRE n.p IS NOT NULL");
      assertThat(s.getConstraintsAdded()).isEqualTo(1);
    });
  }

  @Test
  void nodeKeyConstraintCountsAsConstraintAdded() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "CREATE CONSTRAINT FOR (n:KeyLabel) REQUIRE (n.k) IS NODE KEY");
      assertThat(s.getConstraintsAdded()).isEqualTo(1);
    });
  }

  @Test
  void typedConstraintCountsAsConstraintAdded() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "CREATE CONSTRAINT FOR (n:TypedLabel) REQUIRE n.age IS :: INTEGER");
      assertThat(s.getConstraintsAdded()).isEqualTo(1);
    });
  }

  @Test
  void uniqueConstraintOverPlainIndexStillCountsAsConstraintAdded() {
    database.transaction(() -> {
      // A non-unique index already covers the property; adding a UNIQUE constraint is a genuine
      // schema change and must be counted even though an index existed.
      database.command("sql", "CREATE VERTEX TYPE CoveredLabel");
      database.command("sql", "CREATE PROPERTY CoveredLabel.email STRING");
      database.command("sql", "CREATE INDEX ON CoveredLabel (email) NOTUNIQUE");
      final QueryStatistics s = statsOf(database,
          "CREATE CONSTRAINT FOR (n:CoveredLabel) REQUIRE n.email IS UNIQUE");
      assertThat(s.getConstraintsAdded()).isEqualTo(1);
    });
  }
```

Note: verify the exact Cypher DDL surface for NODE KEY / IS TYPED against existing tests in `engine/src/test/java/com/arcadedb/query/opencypher/` (search for `IS NODE KEY`, `IS ::`, `REQUIRE`); adjust the constraint syntax in the tests above to the dialect the parser accepts if it differs. The `constraintsAdded == 1` assertions are the invariant.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd engine && mvn -q -Dtest=CypherQueryStatisticsTest#uniqueConstraintOverPlainIndexStillCountsAsConstraintAdded test`
Expected: FAIL — `constraintsAdded` is 0 because `indexExistsOnProperties` returns true for the plain index. (The NOT_NULL/KEY/TYPED tests may already pass; they are added coverage and guard against regression.)

- [ ] **Step 3: Narrow the pre-existence check to unique indexes.** In `OpenCypherQueryEngine.java`, add a helper next to `indexExistsOnProperties`:

```java
  /**
   * Like {@link #indexExistsOnProperties} but only reports a pre-existing UNIQUE index. A non-unique
   * index over the same properties does not satisfy a UNIQUE/KEY constraint, so adding the constraint
   * is a genuine schema change that must be counted.
   */
  private static boolean uniqueIndexExistsOnProperties(final Schema schema, final String typeName, final String[] propertyNames) {
    if (!schema.existsType(typeName))
      return false;
    final DocumentType type = schema.getType(typeName);
    final com.arcadedb.index.Index own = type.getIndexByProperties(propertyNames);
    if (own != null && own.isUnique())
      return true;
    final com.arcadedb.index.Index poly = type.getPolymorphicIndexByProperties(propertyNames);
    return poly != null && poly.isUnique();
  }
```

(Prefer importing `com.arcadedb.index.Index` at the top and using the short name, matching the file's style; the FQN above is only for clarity. Verify `Index.isUnique()` exists — it is the standard ArcadeDB index API.)

Change the UNIQUE branch (line ~393) and the KEY branch (line ~428) to use the new helper:
```java
      final boolean existedBefore = uniqueIndexExistsOnProperties(schema, typeName, propertyNames);
```
Leave `indexExistsOnProperties` in place for the plain `CREATE INDEX` paths.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd engine && mvn -q -Dtest=CypherQueryStatisticsTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/opencypher/query/OpenCypherQueryEngine.java engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java
git commit -m "fix(#5015): count UNIQUE/KEY constraint added over a pre-existing plain index"
```

---

### Task 4: Fix DETACH DELETE + explicit-edge-delete double-count

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/DeleteStep.java`
- Test: `engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java`

**Interfaces:**
- Produces: `deleteVertex`/`deleteAllEdges`/`deleteEdge` now thread and consult the shared `Set<Object> deleted` set, so a relationship removed via DETACH and via an explicit match is counted once.

- [ ] **Step 1: Write the failing test** — append to `CypherQueryStatisticsTest`:

```java
  @Test
  void detachDeleteWithExplicitEdgeCountsRelationshipOnce() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Dd {n:'a'})-[:LINK]->(b:Dd {n:'b'})");
      final QueryStatistics s = statsOf(database,
          "MATCH (a:Dd {n:'a'})-[r:LINK]->(b:Dd {n:'b'}) DETACH DELETE r, a, b");
      assertThat(s.getRelationshipsDeleted()).isEqualTo(1);
      assertThat(s.getNodesDeleted()).isEqualTo(2);
    });
  }
```

- [ ] **Step 2: Run test to verify it fails (or passes as a guard)**

Run: `cd engine && mvn -q -Dtest=CypherQueryStatisticsTest#detachDeleteWithExplicitEdgeCountsRelationshipOnce test`
Expected: ideally FAIL with `relationshipsDeleted == 2`. If it already passes (the current `RecordNotFoundException` guard happens to mask the double count for this shape), keep the test as a regression guard and still apply Step 3 so the dedup is deterministic rather than exception-timing-dependent. Note the observed result in the commit body.

- [ ] **Step 3: Consult the shared `deleted` set in the vertex/edge delete helpers.**

In `DeleteStep.java`, thread the `deleted` set through the instance delete methods so DETACH edge removal dedups against explicitly-deleted edges:

Change `deleteObject` (line ~433/440) call sites to pass the set:
```java
      try {
        deleteVertex((Vertex) obj, deleted);
      } catch (final RecordNotFoundException e) {
        // Already deleted - skip
      }
```
```java
      try {
        deleteEdge((Edge) obj, deleted);
      } catch (final RecordNotFoundException e) {
        // Already deleted - skip
      }
```

Change `deleteVertex` (line ~479) signature and its `deleteAllEdges` call:
```java
  private void deleteVertex(final Vertex vertex, final Set<Object> deleted) {
    if (deleteClause.isDetach()) {
      deleteAllEdges(vertex, deleted);
    } else {
      if (vertex.getEdges(Vertex.DIRECTION.OUT).iterator().hasNext() ||
          vertex.getEdges(Vertex.DIRECTION.IN).iterator().hasNext())
        throw new CommandExecutionException("DeleteConnectedNode: Cannot delete node " + vertex.getIdentity() +
            " because it still has relationships. To delete this node, you must first delete its relationships, or use DETACH DELETE");
    }

    vertex.delete();
    context.getStatistics().incNodesDeleted();
  }
```

Change `deleteAllEdges` (line ~500) to skip and record edges already in the set:
```java
  private void deleteAllEdges(final Vertex vertex, final Set<Object> deleted) {
    // Collect connected edges in both directions, de-duplicating self-loops (which appear in both
    // OUT and IN) so a self-loop relationship is deleted and counted exactly once.
    final List<Edge> edgesToDelete = new ArrayList<>();
    final Set<RID> seen = new HashSet<>();
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
      if (seen.add(edge.getIdentity()))
        edgesToDelete.add(edge);
    for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.IN))
      if (seen.add(edge.getIdentity()))
        edgesToDelete.add(edge);

    final QueryStatistics stats = context.getStatistics();
    for (final Edge edge : edgesToDelete) {
      // Skip any relationship already removed and counted by an explicit DELETE in the same statement.
      if (deleted.contains(edge))
        continue;
      try {
        edge.delete();
        stats.incRelationshipsDeleted();
        deleted.add(edge);
      } catch (final RecordNotFoundException ignored) {
        deleted.add(edge);
      }
    }
  }
```

Change `deleteEdge` (line ~528) to record the edge in the set:
```java
  private void deleteEdge(final Edge edge, final Set<Object> deleted) {
    edge.delete();
    context.getStatistics().incRelationshipsDeleted();
    deleted.add(edge);
  }
```

Verify no other caller of `deleteVertex`/`deleteEdge`/`deleteAllEdges` remains with the old arity:
`grep -n "deleteVertex(\|deleteEdge(\|deleteAllEdges(" engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/DeleteStep.java`

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd engine && mvn -q -Dtest=CypherQueryStatisticsTest test`
Expected: PASS. Then run the delete regression subset:
Run: `cd engine && mvn -q -Dtest="*Delete*" -pl engine test` (or `-Dtest=CypherDeleteTest` if that is the concrete class name — confirm via `ls engine/src/test/java/com/arcadedb/query/opencypher | grep -i delete`).
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/DeleteStep.java engine/src/test/java/com/arcadedb/query/opencypher/CypherQueryStatisticsTest.java
git commit -m "fix(#5015): dedup relationships-deleted across DETACH DELETE and explicit edge delete"
```

---

### Task 5: Single-source the replace-map removed-property counting rule

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/CypherStatisticsHelper.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/SetStep.java`
- Modify: `engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/MergeStep.java`
- Test: `engine/src/test/java/com/arcadedb/query/opencypher/executor/steps/CypherStatisticsHelperTest.java`

**Interfaces:**
- Produces: `static int CypherStatisticsHelper.countRemovedProperties(Set<String> existingProps, Map<String,Object> replacementMap)` — number of non-internal pre-existing properties not re-set by the replacement map.

- [ ] **Step 1: Write the failing test** — create `CypherStatisticsHelperTest.java`:

```java
package com.arcadedb.query.opencypher.executor.steps;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CypherStatisticsHelperTest {

  @Test
  void countsPreExistingPropertiesNotReSetAndSkipsInternal() {
    final Set<String> existing = new LinkedHashSet<>();
    existing.add("a");
    existing.add("b");
    existing.add("@type"); // internal - never counted
    final Map<String, Object> replacement = new HashMap<>();
    replacement.put("a", 1); // re-set, not a removal
    // b is removed (absent from replacement) -> counts 1
    assertThat(CypherStatisticsHelper.countRemovedProperties(existing, replacement)).isEqualTo(1);
  }

  @Test
  void nullValuedReplacementIsARemoval() {
    final Set<String> existing = new LinkedHashSet<>();
    existing.add("a");
    final Map<String, Object> replacement = new HashMap<>();
    replacement.put("a", null); // map.get("a") == null -> counts as removed
    assertThat(CypherStatisticsHelper.countRemovedProperties(existing, replacement)).isEqualTo(1);
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd engine && mvn -q -Dtest=CypherStatisticsHelperTest test`
Expected: compile failure — `CypherStatisticsHelper` does not exist.

- [ ] **Step 3: Create the helper** `CypherStatisticsHelper.java` (include the standard Apache license header copied from a sibling file in the same package):

```java
package com.arcadedb.query.opencypher.executor.steps;

import java.util.Map;
import java.util.Set;

/**
 * Shared counting rules for Cypher mutation statistics, single-sourced so the subtle semantics do
 * not drift between the SET and MERGE step implementations.
 */
public final class CypherStatisticsHelper {
  private CypherStatisticsHelper() {
  }

  /**
   * Counts pre-existing, non-internal properties that a replace-map (SET n = {..} / MERGE ... = {..})
   * does not re-set with a non-null value. Neo4j counts these removals toward properties-set.
   */
  public static int countRemovedProperties(final Set<String> existingProps, final Map<String, Object> replacementMap) {
    int removed = 0;
    for (final String prop : existingProps)
      if (!prop.startsWith("@") && replacementMap.get(prop) == null)
        removed++;
    return removed;
  }
}
```

- [ ] **Step 4: Route both call sites through the helper.**

In `SetStep.applyReplaceMap` replace the loop (lines ~281-283):
```java
    propertiesSet += CypherStatisticsHelper.countRemovedProperties(existingProps, map);
```

In `MergeStep` REPLACE_MAP branch replace the loop (lines ~1362-1364):
```java
          propertiesSet += CypherStatisticsHelper.countRemovedProperties(existingProps, map);
```

(Both files are in the same package, so no import is needed.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd engine && mvn -q -Dtest="CypherStatisticsHelperTest,CypherQueryStatisticsTest" test`
Expected: PASS (helper unit + existing REPLACE-map stat assertions unchanged).

- [ ] **Step 6: Commit**

```bash
git add engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/CypherStatisticsHelper.java engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/SetStep.java engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/MergeStep.java engine/src/test/java/com/arcadedb/query/opencypher/executor/steps/CypherStatisticsHelperTest.java
git commit -m "refactor(#5015): single-source the replace-map removed-property counting rule"
```

---

### Task 6: Surface write counters over HTTP `POST /command`

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/PostCommandStatisticsIT.java` (new)

**Interfaces:**
- Consumes: `ResultSet.getStatistics()` (already attached by the engine); `QueryStatistics.toJSON()` from Task 1.
- Produces: HTTP `POST /command` JSON response contains a `stats` object (camelCase) for write queries; absent for reads.

- [ ] **Step 1: Write the failing test** — create `PostCommandStatisticsIT.java` (mirror `PostCommandHandlerProfileIT`'s HTTP helpers):

```java
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class PostCommandStatisticsIT extends BaseGraphServerTest {

  @Test
  void writeCommandReturnsCamelCaseStats() throws Exception {
    final JSONObject response = command("opencypher",
        "CREATE (:HttpStat {name:'x'})-[:REL]->(:HttpStat2 {name:'y'})");
    assertThat(response.has("stats")).isTrue();
    final JSONObject stats = response.getJSONObject("stats");
    assertThat(stats.getInt("nodesCreated")).isEqualTo(2);
    assertThat(stats.getInt("relationshipsCreated")).isEqualTo(1);
    assertThat(stats.getInt("propertiesSet")).isEqualTo(2);
    assertThat(stats.getBoolean("containsUpdates")).isTrue();
  }

  @Test
  void readCommandOmitsStats() throws Exception {
    command("opencypher", "CREATE (:HttpStatRead {id:1})");
    final JSONObject response = command("opencypher", "MATCH (n:HttpStatRead) RETURN n");
    assertThat(response.has("stats")).isFalse();
  }

  private JSONObject command(final String language, final String cmd) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/command/graph").toURL().openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    final JSONObject payload = new JSONObject();
    payload.put("language", language);
    payload.put("command", cmd);
    payload.put("serializer", "studio");
    formatPayload(connection, payload);
    connection.connect();
    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd server && mvn -q -Dtest=PostCommandStatisticsIT test`
Expected: FAIL — `stats` is absent on the write response.

- [ ] **Step 3: Emit `stats` in `PostCommandHandler`.**

In `PostCommandHandler.java`, in the non-EXPLAIN `else` block, after `serializeResultSet(database, serializer, limit, response, qResult);` (line ~192) and before the profile/explain block, add:

```java
          if (qResult != null) {
            final var qStats = qResult.getStatistics();
            if (qStats.isPresent() && qStats.get().containsUpdates())
              response.put("stats", qStats.get().toJSON());
          }
```

`getStatistics()` reads the accumulator already attached to the result set during engine execution; it does not require re-iterating and is safe to call after `serializeResultSet` (which consumes rows but leaves the attached statistics intact) and before the `finally` closes the result set. Add an import for `QueryStatistics` only if referenced explicitly; the `var` above avoids it.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd server && mvn -q -Dtest=PostCommandStatisticsIT test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java server/src/test/java/com/arcadedb/server/http/handler/PostCommandStatisticsIT.java
git commit -m "feat(#5015): surface Cypher write counters as camelCase stats over HTTP /command"
```

---

### Task 7: Surface write counters over gRPC

**Files:**
- Modify: `grpc/src/main/proto/arcadedb-server.proto`
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java`
- Test: `grpcw/src/test/java/com/arcadedb/server/grpc/GrpcWriteStatisticsIT.java` (new)

**Interfaces:**
- Consumes: `ResultSet.getStatistics()`.
- Produces: proto message `QueryUpdateStats` + `ExecuteCommandResponse.stats` field, populated for writes, absent for reads.

- [ ] **Step 1: Add the proto message and field.**

In `arcadedb-server.proto`, add the message (near `ExecuteCommandResponse`):
```proto
message QueryUpdateStats {
  int32 nodes_created = 1;
  int32 nodes_deleted = 2;
  int32 relationships_created = 3;
  int32 relationships_deleted = 4;
  int32 properties_set = 5;
  int32 labels_added = 6;
  int32 labels_removed = 7;
  int32 indexes_added = 8;
  int32 indexes_removed = 9;
  int32 constraints_added = 10;
  int32 constraints_removed = 11;
  bool contains_updates = 12;
}
```
Add a field to `ExecuteCommandResponse` (next free number is 6):
```proto
  QueryUpdateStats stats = 6; // present only for write commands that mutated data
```

- [ ] **Step 2: Regenerate + write the failing test.** Regenerate proto stubs by building the grpc module:

Run: `cd grpc && mvn -q -DskipTests install`
Expected: BUILD SUCCESS; `QueryUpdateStats` and `ExecuteCommandResponse.getStats()`/`hasStats()` now exist.

Create `GrpcWriteStatisticsIT.java`. Mirror an existing command-executing IT (e.g. `GrpcServerIT` / `ArcadeDbGrpcServiceExtendedTest`) for channel/stub setup; assert on the response:

```java
// Pseudocode skeleton — copy the concrete stub/channel bootstrap from an existing grpcw IT
// (e.g. GrpcServerIT): build a managed channel to the running gRPC server, obtain the blocking
// stub, then:

ExecuteCommandResponse write = stub.executeCommand(ExecuteCommandRequest.newBuilder()
    .setDatabase(dbName)
    .setLanguage("opencypher")
    .setCommand("CREATE (:GrpcStat {name:'x'})-[:REL]->(:GrpcStat2 {name:'y'})")
    .setCredentials(creds)
    .build());
assertThat(write.hasStats()).isTrue();
assertThat(write.getStats().getNodesCreated()).isEqualTo(2);
assertThat(write.getStats().getRelationshipsCreated()).isEqualTo(1);
assertThat(write.getStats().getPropertiesSet()).isEqualTo(2);
assertThat(write.getStats().getContainsUpdates()).isTrue();

ExecuteCommandResponse read = stub.executeCommand(ExecuteCommandRequest.newBuilder()
    .setDatabase(dbName)
    .setLanguage("opencypher")
    .setCommand("MATCH (n:GrpcStat) RETURN n")
    .setCredentials(creds)
    .build());
assertThat(read.hasStats()).isFalse();
```

Fill the skeleton with the concrete bootstrap used by the sibling IT so it compiles and boots a real server.

- [ ] **Step 3: Run test to verify it fails**

Run: `cd grpcw && mvn -q -Dtest=GrpcWriteStatisticsIT test`
Expected: FAIL — `hasStats()` is false for the write (service never sets it).

- [ ] **Step 4: Populate `stats` in the service.**

In `ArcadeDbGrpcService.executeCommandInternal`, capture the statistics before the try-with-resources closes the `ResultSet`. Add a local before the `try (ResultSet rs = ...)`:
```java
      QueryStatistics writeStats = null;
```
Inside the try, after both drain branches (right before the closing brace of the `if (rs != null)` block, i.e. after line ~531), add:
```java
          writeStats = rs.getStatistics().orElse(null);
```
After `out.setAffectedRecords(affected).setExecutionTimeMs(ms);` (line ~575) and before `out.build()`, add:
```java
      if (writeStats != null && writeStats.containsUpdates())
        out.setStats(QueryUpdateStats.newBuilder()
            .setNodesCreated(writeStats.getNodesCreated())
            .setNodesDeleted(writeStats.getNodesDeleted())
            .setRelationshipsCreated(writeStats.getRelationshipsCreated())
            .setRelationshipsDeleted(writeStats.getRelationshipsDeleted())
            .setPropertiesSet(writeStats.getPropertiesSet())
            .setLabelsAdded(writeStats.getLabelsAdded())
            .setLabelsRemoved(writeStats.getLabelsRemoved())
            .setIndexesAdded(writeStats.getIndexesAdded())
            .setIndexesRemoved(writeStats.getIndexesRemoved())
            .setConstraintsAdded(writeStats.getConstraintsAdded())
            .setConstraintsRemoved(writeStats.getConstraintsRemoved())
            .setContainsUpdates(true)
            .build());
```
Add imports: `import com.arcadedb.query.sql.executor.QueryStatistics;` and the generated `QueryUpdateStats` (same generated package as `ExecuteCommandResponse`, e.g. `import com.arcadedb.server.grpc.QueryUpdateStats;` — confirm the concrete package from an existing generated-type import in the file).

- [ ] **Step 5: Run test to verify it passes**

Run: `cd grpcw && mvn -q -Dtest=GrpcWriteStatisticsIT test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add grpc/src/main/proto/arcadedb-server.proto grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java grpcw/src/test/java/com/arcadedb/server/grpc/GrpcWriteStatisticsIT.java
git commit -m "feat(#5015): carry Cypher write counters in gRPC ExecuteCommandResponse"
```

---

### Task 8: Extend the Bolt IT for CALL / UNION write counters over the wire

**Files:**
- Modify: `bolt/src/test/java/com/arcadedb/bolt/Bolt5000ResultCountersIT.java`

**Interfaces:**
- Consumes: the Task 2 engine aggregation, surfaced through the existing Bolt `stats` path.

- [ ] **Step 1: Add the failing wire tests** — append to `Bolt5000ResultCountersIT`:

```java
  @Test
  void callSubqueryWriteReportsCountersOverBolt() {
    try (final Driver driver = getDriver();
         final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final SummaryCounters counters = session.run(
          "UNWIND [1,2] AS x CALL { WITH x CREATE (:BoltCall {v:x}) } RETURN x").consume().counters();
      assertThat(counters.nodesCreated()).isEqualTo(2);
      assertThat(counters.propertiesSet()).isEqualTo(2);
      assertThat(counters.containsUpdates()).isTrue();
    }
  }

  @Test
  void unionWriteReportsSummedCountersOverBolt() {
    try (final Driver driver = getDriver();
         final Session session = driver.session(SessionConfig.forDatabase(getDatabaseName()))) {
      final SummaryCounters counters = session.run(
          "CREATE (:BoltUnionA {n:1}) RETURN 1 AS r UNION ALL CREATE (:BoltUnionB {n:2}) RETURN 2 AS r")
          .consume().counters();
      assertThat(counters.nodesCreated()).isEqualTo(2);
      assertThat(counters.propertiesSet()).isEqualTo(2);
      assertThat(counters.containsUpdates()).isTrue();
    }
  }
```

- [ ] **Step 2: Run to verify (should pass on top of Task 2)**

Run: `cd bolt && mvn -q -Dtest=Bolt5000ResultCountersIT test`
Expected: PASS (the engine aggregation from Task 2 flows through the unchanged Bolt `stats` path). If these were run before Task 2 they would FAIL with zero counters — Task 2 is the dependency.

- [ ] **Step 3: Commit**

```bash
git add bolt/src/test/java/com/arcadedb/bolt/Bolt5000ResultCountersIT.java
git commit -m "test(#5015): assert CALL and UNION write counters over Bolt wire"
```

---

### Task 9: Full affected-module verification

- [ ] **Step 1: Compile everything touched**

Run: `mvn -q -pl engine,server,grpc,grpcw,bolt -am -DskipTests install`
Expected: BUILD SUCCESS.

- [ ] **Step 2: Run the affected test classes together**

Run:
```bash
cd engine && mvn -q -Dtest="QueryStatisticsTest,CypherQueryStatisticsTest,CypherStatisticsHelperTest" test
cd ../server && mvn -q -Dtest=PostCommandStatisticsIT test
cd ../grpcw && mvn -q -Dtest=GrpcWriteStatisticsIT test
cd ../bolt && mvn -q -Dtest=Bolt5000ResultCountersIT test
```
Expected: all PASS.

- [ ] **Step 3: Run the broader Cypher + delete regression subsets** (guards the executeUnion/DeleteStep changes)

Run: `cd engine && mvn -q -Dtest="com.arcadedb.query.opencypher.**" test`
Expected: PASS.

- [ ] **Step 4: Commit any final fixups, then open the PR** (handled by the outer workflow).

---

## Self-Review

**Spec coverage:**
- Item 1 (HTTP surfacing) -> Task 6. Item 1 (gRPC surfacing) -> Task 7.
- Item 2 (UNION + CALL aggregation) -> Task 2, verified over Bolt in Task 8.
- Constraint-kind coverage + constraint-vs-plain-index miscount -> Task 3.
- DETACH DELETE dedup -> Task 4.
- Single-source replace-map rule -> Task 5.
- `QueryStatistics.add()` + camelCase `toJSON()` prerequisites -> Task 1.
- Testing across engine/HTTP/gRPC/Bolt -> Tasks 1-8, consolidated in Task 9.

**Type consistency:** `QueryStatistics.add(QueryStatistics)`, `QueryStatistics.toJSON()`, `CommandContext.setStatistics(QueryStatistics)`, `CypherExecutionPlan.executeWithSeedRow(Result, CommandContext)`, `UnionStep.getAggregatedStatistics()`, `CypherStatisticsHelper.countRemovedProperties(Set<String>, Map<String,Object>)` — used consistently across tasks. proto `QueryUpdateStats` snake_case fields map to `getNodesCreated()` etc. in generated Java.

**Known implementation-time confirmations flagged inline:** exact Cypher DDL dialect for NODE KEY / IS TYPED (Task 3 Step 1), `Index.isUnique()` name (Task 3 Step 3), the concrete grpcw IT channel bootstrap (Task 7 Step 2), and the generated `QueryUpdateStats` package (Task 7 Step 4). Each has a grep/verify note.
